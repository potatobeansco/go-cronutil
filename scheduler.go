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
	"math"
	"math/rand"
	"sync"
	"time"
)

// Scheduler schedules action to be executed in periodically (determined by Period).
// Due to the polling nature of the Scheduler, it cannot be used for quick jobs (less than a few seconds). Even with
// very low polling time, it could result in too many lock requests which could waste resource. Due to the polling nature
// action is not guaranteed to execute in the exact period time. It will probably miss by a few milliseconds to seconds.
// It is designed for long-running jobs that are not executed too frequently. See also this package documentation.
type Scheduler struct {
	// A standard Redis client.
	Client redis.UniversalClient

	// Polling period to try to acquire the lock.
	PollingTime time.Duration

	// Timeout for the action.
	ActionTimeout time.Duration

	// The period of which the action should be executed.
	Period time.Duration

	// PeriodJitter defines how much jitter delay needs to be added to the next action run time.
	// It must be >= 0, while by default it is set to 0 (no jitter).
	// In case jitter is set to 0.1, period will be multiplied by 0.1 and a random seconds are picked between 0 and
	// period * period jitter. Chosen random seconds are then added to the next run time, preventing other schedulers
	// to start at the same time.
	PeriodJitter float64

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
}

// NewScheduler creates a new scheduler. See also the package documentation and Scheduler type documentation.
// You should store and consume the error channel by calling Scheduler.Err().
// All Schedulers that share the same action must have the exact same period parameter. Otherwise, it will result
// in unpredictable scheduling due to unpredictable next execution time in Redis.
// The "id" is the id of the job, and will have to match the other schedulers that execute the same action.
// period determines the duration between action execution.
// pollingTime determines the period of which the Scheduler polls Redis for lock and execution time. pollingTime is
// typically a few seconds, and can be longer for long-running jobs that are not executed frequently. pollingTime of
// 10 seconds means that the Scheduler will ask Redis if it's the right time to execute the given action every 10 seconds.
// context is given to the action function, which will have a timeout context given to it. The timeout is determined
// by Scheduler.ActionTimeout (default to 30 seconds), which can be changed.
//
// It is obvious that period must be > pollingTime, and will give panic if period is <= pollingTime.
//
// It is possible to give different pollingTime and action. pollingTime can be differentiated for example to reduce
// the possibility of multiple services asking for the same lock at the same time. But as start time of different
// Schedulers are often different, we suggest keeping the pollingTime the same as other Schedulers. action can be
// different for each scheduler, and there is no way to enforce them across multiple Schedulers. However, it is obvious
// that all Schedulers should execute the same kind of actions.
//
// The execution time is stored in Redis with key "<prefix>:<id>" as UNIX epoch seconds.
// Prefix can be changed as you wish, default to defaultPrefix.
func NewScheduler(id string, client redis.UniversalClient, period time.Duration, pollingTime time.Duration, action func(context.Context) error) *Scheduler {
	s := &Scheduler{
		Client:        client,
		PollingTime:   pollingTime,
		ActionTimeout: 30 * time.Second,
		Period:        period,
		PeriodJitter:  0,
		errCh:         make(chan error, 256),
		Prefix:        defaultPrefix,
		id:            id,
		action:        action,
		Logger:        logutil.NewStdLogger(false, "cronutil"),
		once:          &sync.Once{},
	}

	if s.Period <= s.PollingTime {
		panic("period must be >= polling time")
	}
	s.poller = gocron.NewScheduler(time.UTC)
	_, _ = s.poller.Every(s.PollingTime).Do(s.pollingFunc)
	return s
}

// key returns the key to the entry that contains the next execution time in Redis.
func (s *Scheduler) key() string {
	return fmt.Sprintf("%s:%s", s.Prefix, s.id)
}

// mutexKey returns the key to the entry that contains the mutex lock data in Redis.
func (s *Scheduler) mutexKey() string {
	return fmt.Sprintf("%s:mutex:%s", s.Prefix, s.id)
}

// sendToCh sends an error to the error channel.
func (s *Scheduler) sendToCh(err error) {
	select {
	case s.errCh <- err:
	default:
		s.Logger.Warnf("schduler `%s` error channel is full, discarding error", s.id)
	}
}

// pollingFunc is the polling function that is executed by the internal timer.
// It will try to acquire Redis lock and will check if it is time to execute.
// The action is executed inside this function when it is time to execute.
// This will then set the next execution time to current time + Period.
func (s *Scheduler) pollingFunc() {
	s.once.Do(func() {
		defer func() {
			s.once = &sync.Once{}
		}()

		s.runWithLock(func(ctx context.Context) {
			next, err := s.getNextExecTime(ctx)
			if err != nil && !errors.Is(err, redis.Nil) {
				s.sendToCh(err)
				return
			}

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

			err = s.setNextExecTime(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}
		})
	})
}

// runWithLock wraps f to be run with a Redis lock and sets its cancellation to cancel.
func (s *Scheduler) runWithLock(f func(ctx context.Context)) {
	s.cancelMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), s.ActionTimeout)
	s.cancel = cancel
	s.cancelMu.Unlock()
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
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 10*time.Second)
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
func (s *Scheduler) Err() <-chan error {
	return s.errCh
}

// Ping pings Redis to check the connection.
func (s *Scheduler) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.ActionTimeout)
	defer cancel()
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
func (s *Scheduler) LockCtx(ctx context.Context) error {
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
func (s *Scheduler) UnlockCtx(ctx context.Context) error {
	_, err := s.mu.UnlockContext(ctx)
	if !s.poller.IsRunning() {
		s.poller.StartAsync()
	}
	return err
}

// getNextExecTime retrieves the next execution time from Redis.
// It may return redis.Nil error, which indicates that there is no execution time stored in Redis.
func (s *Scheduler) getNextExecTime(ctx context.Context) (next time.Time, err error) {
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
func (s *Scheduler) setNextExecTime(ctx context.Context) (err error) {
	t := s.Period
	j := int(math.Ceil(t.Seconds() * s.PeriodJitter))
	delay := 0 * time.Second
	if j > 0 {
		delay = time.Duration(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(j)) * time.Second
	}

	next := time.Now().Add(s.Period).Add(delay)
	err = s.Client.Set(ctx, s.key(), next.Unix(), 0).Err()
	if err != nil {
		s.Logger.Warnf("scheduler `%s` cannot set initialization time in Redis: %s", s.id, err.Error())
		return err
	}

	if s.PollingTime.Minutes() > 15 {
		s.Logger.Tracef("scheduler `%s` next run time set to %s (with added delay for %s)", s.id, next.Format(time.RFC3339), delay.String())
	}
	return nil
}

// init initializes next execution time.
// It first checks Redis if next execution time has been registered in Redis. If it has been registered, it will just
// do nothing, so that the current next execution time is not overwritten. Otherwise, it will set next execution time
// in Redis to (time.Now() + Scheduler.Period).
func (s *Scheduler) init() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.ActionTimeout)
	defer cancel()

	err := s.mu.LockContext(ctx)
	if err != nil {
		var et redsync.ErrTaken
		var en redsync.ErrNodeTaken
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
		err = s.setNextExecTime(ctx)
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
func (s *Scheduler) Start() error {
	defer close(s.errCh)

	err := s.Ping()
	if err != nil {
		return err
	}

	s.Logger.Tracef("scheduler `%s` redis connection established for cronutil", s.id)
	if s.Period.Minutes() <= 30 {
		s.Logger.Tracef("scheduler `%s` is using short action period, logging will be reduced", s.id)
	}

	if s.PollingTime.Minutes() <= 15 {
		s.Logger.Tracef("scheduler `%s` is using short polling time, logging will be reduced", s.id)
	}

	s.mu = redsync.New(goredis.NewPool(s.Client)).NewMutex(s.mutexKey(), redsync.WithExpiry(s.ActionTimeout))

	err = s.init()
	if err != nil {
		return err
	}

	s.poller.StartBlocking()
	return nil
}

// Stop stops the scheduler and closes the error channel.
func (s *Scheduler) Stop() {
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
func (s *Scheduler) Trigger() {
	s.once.Do(func() {
		defer func() {
			s.once = &sync.Once{}
		}()

		s.runWithLock(func(ctx context.Context) {
			err := s.action(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}

			err = s.setNextExecTime(ctx)
			if err != nil {
				s.sendToCh(err)
				return
			}
		})
	})
}

// Reset manually resets the next execution schedule to current time + Period.
func (s *Scheduler) Reset() {
	s.runWithLock(func(ctx context.Context) {
		err := s.setNextExecTime(ctx)
		if err != nil {
			s.sendToCh(err)
			return
		}
	})
}
