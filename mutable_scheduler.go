package cronutil

import (
	"context"
	"errors"
	"fmt"
	"git.padmadigital.id/potato/go-logutil"
	"github.com/go-co-op/gocron"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"math"
	"math/rand"
	"sync"
	"time"
)

// MutableScheduler works like Scheduler, but with initialPeriod shared across scheduler and can be modified at will.
// This scheduler can also be paused, which will also pause all instances of the schedulers across all services.
// Because it relies on storing period time in Redis together with pause status and mutex, it must have a very short
// PollingTime. This is set to 5 seconds by default, the lowest resolution we currently support. You must make sure the network
// also supports Redis polling in this short time. Although polling time can actually be set to any values, we
// recommend that it is left at the default 5 seconds.
type MutableScheduler struct {
	// A standard Redis client.
	Client redis.UniversalClient

	// Polling period to try to acquire the lock.
	PollingTime time.Duration

	// Timeout for the action.
	ActionTimeout time.Duration

	// The period of which the action should be executed.
	// Only initial value, as period will be stored in Redis.
	initialPeriod time.Duration

	// initialPeriodJitter defines how much jitter delay needs to be added to the next action run time.
	// It must be >= 0, while by default it is set to 0 (no jitter).
	// In case jitter is set to 0.1, period will be multiplied by 0.1 and a random seconds are picked between 0 and
	// period * period jitter. Chosen random seconds are then added to the next run time, preventing other schedulers
	// to start at the same time.
	initialPeriodJitter float64

	// The poller schedules polling to Redis to acquire lock and execute action.
	poller *gocron.Scheduler

	// The distributed Redis mutex.
	mu *redsync.Mutex

	// A buffered error channel. Must be drained.
	errCh chan error

	// Prefix can be configured to give a unique key to store execution time in Redis.
	// Execution time will be stored as Prefix:id key in the configured Redis database.
	Prefix string

	// The id has to be unique in the entire Redis database.
	id string

	// Error from action is piped into the error channel.
	action func(ctx context.Context) error

	Logger logutil.Logger

	// This ensures that action is only called once although the poller determines to run the action again.
	once *sync.Once

	// Store cancel function to cancel action when we want to stop the scheduler so we don't have to wait.
	cancel   context.CancelFunc
	cancelMu sync.Mutex

	tracer trace.Tracer
}

// NewMutableScheduler creates a new mutable scheduler.
// initialPeriod must be set, to the same values set to all schedulers. This value is used only in the beginning,
// when the first scheduler is started and it initializes the period config in Redis. All other schedulers will
// then follow the period config set in Redis, until one of them call a function to update the period (and period
// jitter).
func NewMutableScheduler(id string, client redis.UniversalClient, initialPeriod time.Duration, action func(context.Context) error) *MutableScheduler {
	s := &MutableScheduler{
		Client:              client,
		PollingTime:         5 * time.Second,
		ActionTimeout:       30 * time.Second,
		initialPeriod:       initialPeriod,
		initialPeriodJitter: 0,
		errCh:               make(chan error, 256),
		Prefix:              defaultPrefix,
		id:                  id,
		action:              action,
		Logger:              logutil.NewStdLogger(false, "cronutil"),
		once:                &sync.Once{},
	}

	tp := otel.GetTracerProvider()
	s.tracer = tp.Tracer("git.padmadigital.id/potato/go-cronutil")

	if s.initialPeriod <= s.PollingTime {
		panic("period must be >= polling time")
	}
	s.poller = gocron.NewScheduler(time.UTC)
	_, _ = s.poller.Every(s.PollingTime).Do(s.pollingFunc)
	return s
}

// key returns the key to the entry that contains the next execution time in Redis.
func (s *MutableScheduler) key() string {
	return fmt.Sprintf("%s:%s", s.Prefix, s.id)
}

// mutexKey returns the key to the entry that contains the mutex lock data in Redis.
func (s *MutableScheduler) mutexKey() string {
	return fmt.Sprintf("%s:mutex:%s", s.Prefix, s.id)
}

// periodKey returns the key to the entry that contains the period configuration time in Redis.
func (s *MutableScheduler) periodKey() string {
	return fmt.Sprintf("%s:period:%s", s.Prefix, s.id)
}

// jitterKey returns the key to the entry that contains the period configuration time in Redis.
func (s *MutableScheduler) jitterKey() string {
	return fmt.Sprintf("%s:jitter:%s", s.Prefix, s.id)
}

// activeKey returns the key to the entry that contains the active status in Redis.
func (s *MutableScheduler) activeKey() string {
	return fmt.Sprintf("%s:active:%s", s.Prefix, s.id)
}

// sendToCh sends an error to the error channel.
func (s *MutableScheduler) sendToCh(err error) {
	select {
	case s.errCh <- err:
	default:
		s.Logger.Warnf("schduler `%s` error channel is full, discarding error", s.id)
	}
}

// pollingFunc is the polling function that is executed by the internal timer.
// It will try to acquire Redis lock and will check if it is time to execute.
// The action is executed inside this function when it is time to execute.
// This will then set the next execution time to current time + initialPeriod.
func (s *MutableScheduler) pollingFunc() {
	s.once.Do(func() {
		defer func() {
			s.once = &sync.Once{}
		}()

		s.runWithLock(func(ctx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						s.sendToCh(fmt.Errorf("action in scheduler encountered panic: %w", err))
					} else {
						s.sendToCh(fmt.Errorf("action in scheduler encountered panic: %v", r))
					}
					return
				}
			}()

			isPaused, err := s.isPaused(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}

			if isPaused {
				if s.PollingTime.Minutes() > 15 {
					s.Logger.Tracef("scheduler `%s` is paused, skipping action", s.id)
				}
				return
			}

			next, err := s.getNextExecTime(ctx)
			if err != nil && !errors.Is(err, redis.Nil) {
				s.sendToCh(err)
				return
			}

			err = nil

			if !time.Now().After(next) {
				if s.PollingTime.Minutes() > 15 {
					s.Logger.Tracef("scheduler `%s` determined that this is not the right time to execute cronutil action, will execute at %s", s.id, next.Format(time.RFC3339))
				}
				return
			}

			err = s.action(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}

			err = s.setNextExecTimeAuto(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}
		})
	})
}

// runWithLock wraps f to be run with a Redis lock and sets its cancellation to cancel.
func (s *MutableScheduler) runWithLock(f func(ctx context.Context)) {
	s.cancelMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), s.ActionTimeout)
	s.cancel = cancel
	s.cancelMu.Unlock()
	ctx, span := s.tracer.Start(ctx, fmt.Sprintf("scheduler.%s.run", s.id))
	defer span.End()

	defer func() {
		s.cancelMu.Lock()
		defer s.cancelMu.Unlock()
		cancel()
		s.cancel = nil
	}()

	err := s.mu.LockContext(ctx)
	if err != nil {
		s.Logger.Tracef("scheduler `%s` cannot acquire lock: %s, skipping to execute action", s.id, err.Error())
		s.sendToCh(err)
		return
	}

	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(ctx, 10*time.Second)
		defer unlockCancel()
		_, err := s.mu.UnlockContext(unlockCtx)
		if err != nil {
			s.Logger.Warnf("scheduler `%s` cannot unlock lock: %s", s.id, err.Error())
			s.sendToCh(err)
		}
	}()

	f(ctx)
}

// Err returns the error channel.
// This error channel should be consumed to avoid getting too many errors in the channel.
func (s *MutableScheduler) Err() <-chan error {
	return s.errCh
}

// Ping pings Redis to check the connection.
func (s *MutableScheduler) Ping(ctx context.Context) error {
	err := s.Client.Ping(ctx).Err()
	if err != nil {
		s.Logger.Warnf("scheduler `%s` cannot ping Redis: %w", s.id, err)
		return err
	}
	return nil
}

// LockCtx manually locks the distributed mutex, effectively pausing this scheduler, until the lock is released again.
// If err is returned, that means the lock cannot be acquired, and the action will continue. The poller will also be stopped,
// but can be rerun with UnlockCtx.
func (s *MutableScheduler) LockCtx(ctx context.Context) error {
	err := s.mu.LockContext(ctx)
	if err != nil {
		if errors.Is(err, redsync.ErrFailed) {
			return ErrMutexLocked
		}
		return err
	}
	s.poller.Stop()
	return nil
}

// UnlockCtx manually releases the distributed mutex.
func (s *MutableScheduler) UnlockCtx(ctx context.Context) error {
	_, err := s.mu.UnlockContext(ctx)
	if !s.poller.IsRunning() {
		s.poller.StartAsync()
	}
	return err
}

// setPeriodConfig updates the period config in Redis.
func (s *MutableScheduler) setPeriodConfig(ctx context.Context, period time.Duration, jitter float64) (err error) {
	err = s.Client.Set(ctx, s.periodKey(), int(period.Seconds()), 0).Err()
	if err != nil {
		return err
	}

	err = s.Client.Set(ctx, s.jitterKey(), jitter, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

// getPeriodConfig retrieves period and period jitter from Redis.
func (s *MutableScheduler) getPeriodConfig(ctx context.Context) (period time.Duration, jitter float64, err error) {
	cmd := s.Client.Get(ctx, s.periodKey())
	periodInt, err := cmd.Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, 0, err
	}

	period = time.Duration(periodInt) * time.Second
	if errors.Is(err, redis.Nil) || periodInt > 24*60 { // Period is invalid (too big)
		period = s.initialPeriod

		err = s.Client.Set(ctx, s.periodKey(), int(s.initialPeriod.Seconds()), 0).Err()
		if err != nil {
			return 0, 0, err
		}
	}

	cmd = s.Client.Get(ctx, s.jitterKey())
	jitter, err = cmd.Float64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, 0, err
	}

	if errors.Is(err, redis.Nil) {
		jitter = s.initialPeriodJitter

		err = s.Client.Set(ctx, s.jitterKey(), s.initialPeriodJitter, 0).Err()
		if err != nil {
			return 0, 0, err
		}
	}

	return period, jitter, nil
}

// isPaused checks if scheduler is paused.
func (s *MutableScheduler) isPaused(ctx context.Context) (isPaused bool, err error) {
	isActive, err := s.Client.Get(ctx, s.activeKey()).Bool()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}

	if errors.Is(err, redis.Nil) {
		return true, nil
	}

	return !isActive, nil
}

// getNextExecTime retrieves the next execution time from Redis.
// It may return redis.Nil error, which indicates that there is no execution time stored in Redis.
func (s *MutableScheduler) getNextExecTime(ctx context.Context) (next time.Time, err error) {
	cmd := s.Client.Get(ctx, s.key())
	result, err := cmd.Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		s.Logger.Warnf("scheduler `%s` cannot retrieve run schedule time from Redis: %s", s.id, err.Error())
		return time.Time{}, err
	}
	if errors.Is(err, redis.Nil) {
		return time.Time{}, redis.Nil
	}

	next = time.Unix(result, 0)
	return next, nil
}

// setNextExecTime sets the next execution time in Redis.
func (s *MutableScheduler) setNextExecTimeAuto(ctx context.Context) (err error) {
	t, jitter, err := s.getPeriodConfig(ctx)
	if err != nil {
		return err
	}

	j := int(math.Ceil(t.Seconds() * jitter))
	delay := 0 * time.Second
	if j > 0 {
		delay = time.Duration(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(j)) * time.Second
	}

	next := time.Now().Add(t).Add(delay)
	err = s.setNextExecTime(ctx, next)
	if err != nil {
		return err
	}

	//if s.PollingTime.Minutes() > 15 {
	s.Logger.Tracef("scheduler `%s` next run time set to %s (with added delay for %s)", s.id, next.Format(time.RFC3339), delay.String())
	//}
	return nil
}

// setNextExecTime updates the next run time in Redis with custom run time.
func (s *MutableScheduler) setNextExecTime(ctx context.Context, next time.Time) (err error) {
	err = s.Client.Set(ctx, s.key(), next.Unix(), 0).Err()
	if err != nil {
		s.Logger.Warnf("scheduler `%s` cannot set initialization time in Redis: %s", s.id, err.Error())
		return err
	}

	return err
}

// init initializes next execution time.
// It first checks Redis if next execution time has been registered in Redis. If it has been registered, it will just
// do nothing, so that the current next execution time is not overwritten. Otherwise, it will set next execution time
// in Redis to (time.Now() + Scheduler.Period).
func (s *MutableScheduler) init(ctx context.Context) error {
	err := s.mu.LockContext(ctx)
	if err != nil {
		et := &redsync.ErrTaken{}
		en := &redsync.ErrNodeTaken{}
		if errors.As(err, &et) || errors.As(err, &en) {
			s.Logger.Warnf("scheduler `%s` redis mutex is locked, initializing anyway", s.id)
			return nil
		}

		s.Logger.Warnf("scheduler `%s` cannot acquire lock: %s", s.id, err.Error())
		return err
	}

	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer unlockCancel()
		_, err := s.mu.UnlockContext(unlockCtx)
		if err != nil {
			s.Logger.Warnf("scheduler `%s` cannot unlock lock: %s", s.id, err.Error())
			return
		}
	}()

	_, err = s.getNextExecTime(ctx)
	if errors.Is(err, redis.Nil) {
		s.Logger.Tracef("scheduler `%s` has no next exec time, setting", s.id)
		err = s.setNextExecTimeAuto(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Start starts the scheduler synchronously.
// You can start it in goroutine to start the Scheduler asynchronously. It will return error immediately if connection
// to Redis cannot be made.
// It will not execute its task immediately. It will wait for Scheduler.PollingTime duration to execute the action
// in the next period.
//
// If Start fails, the error channel will be closed.
func (s *MutableScheduler) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.ActionTimeout)
	defer cancel()

	ctx, span := s.tracer.Start(ctx, fmt.Sprintf("scheduler.%s.start", s.id))
	defer span.End()

	defer close(s.errCh)

	err := s.Ping(ctx)
	if err != nil {
		return err
	}

	s.Logger.Tracef("scheduler `%s` redis connection established for cronutil", s.id)
	if s.initialPeriod.Minutes() <= 30 {
		s.Logger.Tracef("scheduler `%s` is using short action period, logging will be reduced", s.id)
	}

	if s.PollingTime.Minutes() <= 15 {
		s.Logger.Tracef("scheduler `%s` is using short polling time, logging will be reduced", s.id)
	}

	s.mu = redsync.New(goredis.NewPool(s.Client)).NewMutex(s.mutexKey(), redsync.WithExpiry(s.ActionTimeout))

	err = s.init(ctx)
	if err != nil {
		return err
	}

	s.poller.StartBlocking()
	return nil
}

// Stop stops the scheduler and closes the error channel.
func (s *MutableScheduler) Stop() {
	s.cancelMu.Lock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
		s.Logger.Tracef("scheduler `%s` cronutil action cancelled", s.id)
	}
	s.cancelMu.Unlock()

	s.poller.Stop()
	s.Logger.Tracef("scheduler `%s` cronutil poller stopped", s.id)

	unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer unlockCancel()
	_, err := s.mu.UnlockContext(unlockCtx)
	if err != nil {
		s.Logger.Warnf("scheduler `%s` cannot unlock lock: %s, continuing anyway", s.id, err.Error())
	}
}

// Trigger triggers the execution of action immediately, resetting the timer on action completion.
func (s *MutableScheduler) Trigger() {
	s.once.Do(func() {
		defer func() {
			s.once = &sync.Once{}
		}()

		s.runWithLock(func(ctx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						s.sendToCh(fmt.Errorf("action in scheduler encountered panic: %w", err))
					} else {
						s.sendToCh(fmt.Errorf("action in scheduler encountered panic: %v", r))
					}
					return
				}
			}()

			err := s.action(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}

			err = s.setNextExecTimeAuto(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}
		})
	})
}

// UpdatePeriod updates period config, resetting the next run time.
func (s *MutableScheduler) UpdatePeriod(period time.Duration, jitter float64) (err error) {
	if period <= s.ActionTimeout {
		return errors.New("period must be > action timeout")
	}

	if period <= s.PollingTime {
		return errors.New("period must be > polling time")
	}

	s.runWithLock(func(ctx context.Context) {
		err = s.setPeriodConfig(ctx, period, jitter)
		if err != nil {
			return
		}

		j := int(math.Ceil(period.Seconds() * jitter))
		delay := 0 * time.Second
		if j > 0 {
			delay = time.Duration(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(j)) * time.Second
		}

		next := time.Now().Add(period).Add(delay)
		err = s.setNextExecTime(ctx, next)
		if err != nil {
			return
		}

		s.Logger.Tracef("scheduler %s next execution time has been updated to %s following request to update period/jitter to %.2f/%.2f", s.id, next.Format(time.RFC3339), period.Seconds(), jitter)
	})

	return err
}

// Pause sets active status to false.
func (s *MutableScheduler) Pause() (err error) {
	err = s.Client.Set(context.Background(), s.activeKey(), false, 0).Err()
	if err != nil {
		return
	}

	s.Logger.Tracef("schedluer %s is paused", s.id)

	return err
}

// Unpause sets the timer setting to active.
// When unpausing, the timer is like recreated again and so the next run time will be set to next period.
func (s *MutableScheduler) Unpause() (err error) {
	s.runWithLock(func(ctx context.Context) {
		err = s.Client.Set(ctx, s.activeKey(), true, 0).Err()
		if err != nil {
			return
		}

		s.Logger.Tracef("schedluer %s is unpaused, next exec time is reset", s.id)

		err = s.setNextExecTimeAuto(ctx)
		if err != nil {
			return
		}
	})

	return err
}

// Reset manually resets the next execution schedule to current time + initialPeriod.
func (s *MutableScheduler) Reset() {
	s.runWithLock(func(ctx context.Context) {
		err := s.setNextExecTimeAuto(ctx)
		if err != nil {
			s.sendToCh(err)
			return
		}
	})
}
