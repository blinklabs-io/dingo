// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestScheduler_RegistersAndRunsTask(t *testing.T) {
	t.Parallel()

	var counter atomic.Int32

	// Create a Scheduler with 10ms tick interval
	timer := NewScheduler(10 * time.Millisecond)
	timer.Start()
	defer timer.Stop()

	// Registering task to execute every 3 ticks
	timer.Register(3, func() {
		counter.Add(1)
	}, nil)

	// Wait for task to run at least 2 times (polls instead of fixed sleep)
	require.Eventually(t, func() bool {
		return counter.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"expected task to run at least 2 times",
	)
}

func TestScheduler_ChangeInterval(t *testing.T) {
	t.Parallel()

	var counter atomic.Int32

	// Create a Scheduler with a fast tick interval so the pre-change
	// baseline is established quickly.
	const fastInterval = 20 * time.Millisecond
	timer := NewScheduler(fastInterval)
	timer.Start()
	defer timer.Stop()

	// Registering task to execute for every 1 tick
	timer.Register(1, func() {
		counter.Add(1)
	}, nil)

	// Wait for at least 2 executions before changing interval
	require.Eventually(t, func() bool {
		return counter.Load() >= 2
	}, 2*time.Second, 5*time.Millisecond,
		"expected at least 2 executions before interval change",
	)
	// ChangeInterval queues the interval in updateIntervalChan's one-slot
	// buffer and run() applies it when it next reaches its select, so the
	// change is asynchronous. Poll the scheduler's own interval field,
	// read under the same mutex run() uses to update it, rather than
	// asserting immediately after the call.
	// require.Eventually runs its condition func on a separate goroutine
	// per poll, where require's t.FailNow is unsafe to call -- so check
	// the (always-nil, since slowInterval is a positive constant) error
	// directly rather than through require.NoError.
	const slowInterval = 300 * time.Millisecond
	require.Eventually(t, func() bool {
		if err := timer.ChangeInterval(slowInterval); err != nil {
			t.Logf("ChangeInterval: %v", err)
			return false
		}
		timer.mutex.Lock()
		applied := timer.interval == slowInterval
		timer.mutex.Unlock()
		return applied
	}, 5*time.Second, 5*time.Millisecond,
		"expected interval change to be applied",
	)
	afterChangeReq := counter.Load()

	// Assert the observable property -- the tick rate slowed down --
	// instead of a hard real-time window. Even after the change is
	// confirmed applied, one leftover tick from the pre-change ticker can
	// still land, and CI scheduling delays can bunch ticks. Rather than
	// counting ticks in a narrow fixed window, require that the count
	// never reaches a threshold that is reachable only if the scheduler
	// kept running at the old (fast) interval: over this window the new
	// interval nominally delivers ~4 ticks (plus at most one leftover),
	// while the old interval would reach the threshold in well under a
	// fifth of the window.
	const window = 1200 * time.Millisecond
	const tooManyTicks = 10
	require.Never(t, func() bool {
		return counter.Load()-afterChangeReq >= tooManyTicks
	}, window, 10*time.Millisecond,
		"timer did not respect interval change: ran too frequently",
	)

	// A lower bound on counter is not attributable to the new interval at
	// any anchor. tick() enqueues onto taskQueue and the worker pool
	// drains it asynchronously, so a closure enqueued by the last
	// pre-change tick can increment counter after the reading above was
	// taken -- satisfying "counter grew" on a scheduler that stopped
	// ticking outright at the change.
	//
	// Register a second task instead. Register appends under the same
	// mutex tick() holds, and run() stopped and discarded the old ticker
	// before publishing st.interval, so no tick predating the confirmed
	// change can reach this task: every execution of it is driven by a
	// tick the new ticker delivered.
	var postChange atomic.Int32
	timer.Register(1, func() {
		postChange.Add(1)
	}, nil)
	require.Eventually(t, func() bool {
		return postChange.Load() > 0
	}, 10*time.Second, 10*time.Millisecond,
		"expected ticking to continue after interval change",
	)
}

// TestScheduler_ChangeIntervalDeliveredWhenNotParked verifies that a single,
// non-retried ChangeInterval call is not silently dropped when run() is not
// currently parked in its own select -- dingo#4155. Before Start(), run() has
// not even been launched, which is the most extreme case of "not parked in
// the select": with the old unbuffered updateIntervalChan and its
// non-blocking select/default send, this call took the default branch,
// discarded newInterval, and returned nil, leaving the scheduler on its
// constructor interval forever once started -- exactly the production
// shape, since the real caller in ledger/state.go's era-rollover path makes
// one call and does not retry. The fixed send must queue the interval so it
// is applied once run() starts.
func TestScheduler_ChangeIntervalDeliveredWhenNotParked(t *testing.T) {
	t.Parallel()

	const newInterval = 200 * time.Millisecond
	timer := NewScheduler(50 * time.Millisecond)

	// A single, non-retried call -- what the production caller does.
	require.NoError(t, timer.ChangeInterval(newInterval))

	timer.Start()
	defer timer.Stop()

	require.Eventually(t, func() bool {
		timer.mutex.Lock()
		defer timer.mutex.Unlock()
		return timer.interval == newInterval
	}, 2*time.Second, 5*time.Millisecond,
		"interval change before Start must be delivered, not silently dropped",
	)
}

// TestScheduler_ChangeIntervalDeliveredDuringTick verifies that a single
// ChangeInterval call made while run() is inside tick(), rather than parked
// in its select, is applied once tick() returns. This is the production
// shape of dingo#4155. A task whose previous run still holds its lock makes
// tick() call runFailFunc synchronously on run()'s goroutine; blocking there
// holds run() inside tick() for the duration of the call.
func TestScheduler_ChangeIntervalDeliveredDuringTick(t *testing.T) {
	t.Parallel()

	const newInterval = 200 * time.Millisecond
	timer := NewScheduler(time.Millisecond)

	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	inTick := make(chan struct{})
	var inTickOnce sync.Once
	// The first due tick queues taskFunc, which holds the task lock until
	// release. The next due tick fails TryLock and calls runFailFunc from
	// inside tick().
	timer.Register(1, func() { <-releaseCh }, func() {
		inTickOnce.Do(func() { close(inTick) })
		<-releaseCh
	})
	timer.Start()
	defer timer.Stop()
	// Runs before Stop, which waits for the worker blocked in taskFunc.
	defer release()

	testutil.RequireReceive(
		t, inTick, 2*time.Second, "run() never blocked inside tick()",
	)
	require.NoError(t, timer.ChangeInterval(newInterval))
	release()

	require.Eventually(t, func() bool {
		timer.mutex.Lock()
		defer timer.mutex.Unlock()
		return timer.interval == newInterval
	}, 2*time.Second, 5*time.Millisecond,
		"interval change during a tick must be delivered, not silently dropped",
	)
}

// TestScheduler_ChangeIntervalLatestWins verifies that a second
// ChangeInterval call made before run() drains the first supersedes it
// rather than being discarded because the one-slot buffer is full.
func TestScheduler_ChangeIntervalLatestWins(t *testing.T) {
	t.Parallel()

	const (
		staleInterval = 100 * time.Millisecond
		newInterval   = 200 * time.Millisecond
	)
	timer := NewScheduler(50 * time.Millisecond)

	require.NoError(t, timer.ChangeInterval(staleInterval))
	require.NoError(t, timer.ChangeInterval(newInterval))

	timer.Start()
	defer timer.Stop()

	require.Eventually(t, func() bool {
		timer.mutex.Lock()
		defer timer.mutex.Unlock()
		return timer.interval == newInterval
	}, 2*time.Second, 5*time.Millisecond,
		"the latest interval must replace a pending one, not be dropped",
	)
}

func TestSchedulerRunFailFunc(t *testing.T) {
	t.Parallel()

	var failCounter atomic.Int32

	// Create a Scheduler with 50ms tick interval
	timer := NewScheduler(50 * time.Millisecond)
	timer.Start()
	defer timer.Stop()

	// Registering task to execute every 2 ticks (100ms).
	// The task sleeps 500ms, so multiple tick intervals pass
	// while it holds its lock — generating fail calls even on
	// slow CI runners (macOS/Windows).
	timer.Register(
		2,
		// Task func
		func() {
			time.Sleep(500 * time.Millisecond)
		},
		// Run fail func
		func() {
			failCounter.Add(1)
		},
	)

	// Wait for the fail function to be called at least 3 times
	require.Eventually(t, func() bool {
		return failCounter.Load() >= 3
	}, 10*time.Second, 50*time.Millisecond,
		"expected failure to run task at least 3 times",
	)
}

func TestScheduler_Config(t *testing.T) {
	t.Parallel()

	// Test default configuration
	defaultScheduler := NewScheduler(100 * time.Millisecond)
	if defaultScheduler.workerPoolSize != 10 {
		t.Errorf(
			"Expected default worker pool size 10, got %d",
			defaultScheduler.workerPoolSize,
		)
	}
	if cap(defaultScheduler.taskQueue) != 100 {
		t.Errorf(
			"Expected default task queue size 100, got %d",
			cap(defaultScheduler.taskQueue),
		)
	}

	// Test custom configuration
	config := SchedulerConfig{
		WorkerPoolSize: 5,
		TaskQueueSize:  50,
	}
	customScheduler := NewSchedulerWithConfig(100*time.Millisecond, config)
	if customScheduler.workerPoolSize != 5 {
		t.Errorf(
			"Expected custom worker pool size 5, got %d",
			customScheduler.workerPoolSize,
		)
	}
	if cap(customScheduler.taskQueue) != 50 {
		t.Errorf(
			"Expected custom task queue size 50, got %d",
			cap(customScheduler.taskQueue),
		)
	}

	// Test default config function
	defaultConfig := DefaultSchedulerConfig()
	if defaultConfig.WorkerPoolSize != 10 {
		t.Errorf(
			"Expected default config worker pool size 10, got %d",
			defaultConfig.WorkerPoolSize,
		)
	}
	if defaultConfig.TaskQueueSize != 100 {
		t.Errorf(
			"Expected default config task queue size 100, got %d",
			defaultConfig.TaskQueueSize,
		)
	}

	// Test validation/coercion of invalid values
	// Test zero values
	zeroConfig := SchedulerConfig{
		WorkerPoolSize: 0,
		TaskQueueSize:  0,
	}
	zeroScheduler := NewSchedulerWithConfig(100*time.Millisecond, zeroConfig)
	if zeroScheduler.workerPoolSize != 10 {
		t.Errorf(
			"Expected zero worker pool size to be coerced to 10, got %d",
			zeroScheduler.workerPoolSize,
		)
	}
	if cap(zeroScheduler.taskQueue) != 100 {
		t.Errorf(
			"Expected zero task queue size to be coerced to 100, got %d",
			cap(zeroScheduler.taskQueue),
		)
	}

	// Test negative values
	negativeConfig := SchedulerConfig{
		WorkerPoolSize: -5,
		TaskQueueSize:  -10,
	}
	negativeScheduler := NewSchedulerWithConfig(
		100*time.Millisecond,
		negativeConfig,
	)
	if negativeScheduler.workerPoolSize != 10 {
		t.Errorf(
			"Expected negative worker pool size to be coerced to 10, got %d",
			negativeScheduler.workerPoolSize,
		)
	}
	if cap(negativeScheduler.taskQueue) != 100 {
		t.Errorf(
			"Expected negative task queue size to be coerced to 100, got %d",
			cap(negativeScheduler.taskQueue),
		)
	}

	// Test mixed valid/invalid values
	mixedConfig := SchedulerConfig{
		WorkerPoolSize: 15, // Valid
		TaskQueueSize:  -5, // Invalid
	}
	mixedScheduler := NewSchedulerWithConfig(100*time.Millisecond, mixedConfig)
	if mixedScheduler.workerPoolSize != 15 {
		t.Errorf(
			"Expected valid worker pool size 15 to be preserved, got %d",
			mixedScheduler.workerPoolSize,
		)
	}
	if cap(mixedScheduler.taskQueue) != 100 {
		t.Errorf(
			"Expected invalid task queue size to be coerced to 100, got %d",
			cap(mixedScheduler.taskQueue),
		)
	}
}

func TestScheduler_ChangeInterval_RejectsInvalidDuration(t *testing.T) {
	t.Parallel()

	timer := NewScheduler(50 * time.Millisecond)

	// Validation happens before the channel send, so we do not need
	// the scheduler running to verify that invalid durations are
	// rejected.

	// Zero duration must return an error, not panic
	err := timer.ChangeInterval(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "interval must be positive")

	// Negative duration must return an error, not panic
	err = timer.ChangeInterval(-1 * time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "interval must be positive")

	// Positive duration must not return an error.
	// The scheduler is not started, so nothing ever drains the queued
	// value, but ChangeInterval still queues it (dingo#4155) and returns
	// successfully; validation succeeds independent of whether the
	// scheduler is running.
	err = timer.ChangeInterval(100 * time.Millisecond)
	require.NoError(t, err)
}

// TestScheduler_StopIsIdempotent verifies that calling Stop more than
// once (or on a Scheduler that was never Start-ed) does not panic.
// LedgerState.Close calls Scheduler.Stop unconditionally, and some
// callers (e.g. test cleanup) may also call it directly beforehand --
// Stop must tolerate being called from both without double-closing its
// quit channel.
func TestScheduler_StopIsIdempotent(t *testing.T) {
	t.Parallel()

	timer := NewScheduler(10 * time.Millisecond)
	timer.Start()
	require.NotPanics(t, func() {
		timer.Stop()
		timer.Stop()
	})
}

// TestScheduler_StopWithoutStartIsSafe verifies that Stop on a Scheduler
// that was constructed but never Start-ed does not panic either --
// LedgerState.Close calls Scheduler.Stop whenever ls.Scheduler is
// non-nil, regardless of whether dev-mode forging ever actually started
// ticking.
func TestScheduler_StopWithoutStartIsSafe(t *testing.T) {
	t.Parallel()

	timer := NewScheduler(10 * time.Millisecond)
	require.NotPanics(t, func() {
		timer.Stop()
	})
}

// TestScheduler_StopBeforeStartPreventsLaterStart verifies that a shutdown
// request is durable even if it wins a race with Start. This is the only way
// Stop can guarantee a concurrent Start does not create workers after Stop
// has already returned.
func TestScheduler_StopBeforeStartPreventsLaterStart(t *testing.T) {
	t.Parallel()

	timer := NewScheduler(10 * time.Millisecond)

	timer.Stop()
	timer.Start()

	require.Nil(t, timer.ticker)
	require.Empty(t, timer.workers)
	require.False(t, timer.started)
}

func TestScheduler_ConcurrentStartStopLeavesNoWorkers(t *testing.T) {
	t.Parallel()

	for range 100 {
		timer := NewScheduler(time.Hour)
		var callers sync.WaitGroup
		callers.Add(2)
		go func() {
			defer callers.Done()
			timer.Start()
		}()
		go func() {
			defer callers.Done()
			timer.Stop()
		}()
		callers.Wait()
		timer.Stop()

		timer.lifecycleMutex.Lock()
		require.True(t, timer.stopped)
		timer.lifecycleMutex.Unlock()

		done := make(chan struct{})
		go func() {
			timer.workerWg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("scheduler workers remained after concurrent Start/Stop")
		}
	}
}

// TestScheduler_StopDoesNotRaceChangeInterval guards against a real data
// race: run's interval-update case (driven by ChangeInterval, which a
// running node calls at era/epoch boundaries as slot length changes)
// reassigns st.ticker under st.mutex, but Stop used to read st.ticker
// with no synchronization at all. This never surfaced via -race in
// practice until LedgerState.Close started calling Scheduler.Stop on a
// live, running scheduler -- before that, Stop was only ever called on
// schedulers nothing else was concurrently touching. Run with -race:
// this must never report a race between the ChangeInterval goroutine's
// write and Stop's read of st.ticker.
func TestScheduler_StopDoesNotRaceChangeInterval(t *testing.T) {
	t.Parallel()

	timer := NewScheduler(1 * time.Millisecond)
	timer.Start()

	// ChangeInterval's send is non-blocking (updateIntervalChan has a
	// capacity-1 buffer, coalesced rather than dropped -- dingo#4155), so
	// goroutines spinning as fast as possible maximize the chance run's
	// interval-update case actually wins the race against Stop reading
	// st.ticker concurrently. Several concurrent callers (not just one)
	// raise the odds this reproduces within a single run rather than
	// needing many repeated runs.
	const changers = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(changers)
	for g := range changers {
		go func(n int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				interval := time.Duration(1+(i+n)%3) * time.Millisecond
				_ = timer.ChangeInterval(interval)
			}
		}(g)
	}

	timer.Stop()
	close(stop)
	wg.Wait()
}
