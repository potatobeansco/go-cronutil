# cronutil

This library provides a standard implementation for a distributed cron for use in PotatoBeans standard Go services and
applications.

The distributed cron are used to schedule periodic actions across multiple services. This utilizes Redis to provide a
global lock and the schedule to the action.

TODO: This library has not yet support standard cron notations. However, it has provided schedulers with a distributed
mutex to do actions periodically.

This module is provided for all people to use, including PotatoBeans former clients. It is actively maintained by 
PotatoBeans.

## Scheduler

The go-cronutil primary reason for existence is to provide Scheduler, and then also MutableScheduler. The primary goal
of a scheduler is to run action periodically, but can work in multi-service and replication environment. As it uses
Redis as its backend, services using this Scheduler can be stateless.

The Scheduler works with polling, so it is not recommended for highly-precise applications to do actions at precisely
set time. It can be used for example to do a very long-running batch jobs without having to set stuff outside of the
applications, such as Kubernetes or Nomad jobs. This way, all logic can remain written in the application in the hands
of the developers.

Scheduler works by polling a distributed lock in Redis multiple times. It is defined by the scheduler `pollingTime`. The
action period is configured with `actionPeriod`. So 10 minutes action period can have less than action period polling time,
such as 1 minute. This way, all schedulers configured with the same scheduler ID and polling time will try to poll Redis
at roughly the same time. When it has passed the time where the action is required to be executed, one of the service
winning the lock will run the action. This is analogous to a distributed periodic `sync.Once`.

It depends on the `github.com/redis/go-redis/v9` library and you must have Redis running.

### Example

```go
package main

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/potatobeansco/go-cronutil"
)

func main() {
	rc := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	s := cronutil.NewScheduler("myscheduler", rc, 1*time.Hour, 10*time.Minute, func(ctx context.Context) error {
        fmt.Println("hello world")
		return nil
	})
	
	// Start will start blocking. Better to call start in a goroutine and wait it using sync.WaitGroup
	go func() {
		err := s.Start()
		if err != nil {
			fmt.Printf("unable to start: %s\n", err.Error())
			os.Exit(1)
        }
    }()
	
	...
	
	// Avoid memory leak by calling stop somewhere
	s.Stop()
}
```

The MutableScheduler is a variant of Scheduler where period setting is stored also in Redis and can be modified. The
MutableScheduler can also be paused and unpaused. See Scheduler and MutableScheduler documentation for details.

### Logging and OpenTelemetry

Scheduler and MutableScheduler support OpenTelemetry logging and trace. It will try to receive the default trace provider
as usual. Consult the Golang OpenTelemetry guide to set up OpenTelemetry in your application.

The schedulers have Logger attribute which can be changed. It uses the PotatoBeans `logutil.ContextLogger` interface.
The `logutil.OpenTelemetryLogger` can be used to log into OpenTelemetry collector/services. See potatobeansco/go-logutil module
for details.

## Future Tasks

- Enchancing documentation and example
- Support for high precision timer
- Support for cron notation