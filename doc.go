// Package cronutil provides an extra light implementation of distributed cron scheduler.
// The Scheduler guarantees that only one Scheduler can execute a given action at a time, and the action should be
// executed periodically.
//
// It relies on Redis to store lock and the time schedule. It works by continuously polling Redis to check if the
// time is up to execute an action and executes it if it is time. After executing, the time stored in Redis will be
// updated to the next execution time. If during execution of the action, another scheduler (call it scheduler B) with the same id requests
// to execute an action, it will hang until the execution by the original scheduler (call it scheduler A) is over.
// Eventually when scheduler A has completed its action, it will update the next execution time and then release the lock,
// causing scheduler B to check the execution time and determine that it is not the time to execute. This means, only
// scheduler A will execute the given action, the other schedulers will execute their given actions when scheduler A
// no longer requests a lock and updates next execution time in Redis (e.g. if scheduler A is down).
//
// See also documentation for NewScheduler.
package cronutil

import "errors"

var ErrMutexLocked = errors.New("mutex is locked")

const defaultPrefix = "cronutil"
