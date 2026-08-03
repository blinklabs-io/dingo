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

	"github.com/stretchr/testify/require"
)

func TestScheduler_RegistersAndRunsTask(t *testing.T) {
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
	var counter atomic.Int32

	// Create a Scheduler with 50ms tick interval
	timer := NewScheduler(50 * time.Millisecond)
	timer.Start()
	defer timer.Stop()

	// Registering task with 50ms tick interval to execute for every 1 tick
	timer.Register(1, func() {
		counter.Add(1)
	}, nil)

	// Wait for at least 2 executions before changing interval
	require.Eventually(t, func() bool {
		return counter.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"expected at least 2 executions before interval change",
	)
	beforeChange := counter.Load()

	// Change interval to 200ms
	err := timer.ChangeInterval(200 * time.Millisecond)
	require.NoError(t, err)

	// Wait for at least 1 execution after interval change
	require.Eventually(t, func() bool {
		return counter.Load()-beforeChange >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"expected at least 1 execution after interval change",
	)

	// Allow enough time for potential additional ticks
	time.Sleep(500 * time.Millisecond)

	secondCount := counter.Load()
	afterChange := secondCount - beforeChange

	if afterChange < 1 || afterChange > 3 {
		t.Errorf(
			"timer did not respect interval change, ran too frequently: %d more ticks",
			afterChange,
		)
	}
}

func TestSchedulerRunFailFunc(t *testing.T) {
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
	// The scheduler is not started, so the send will be dropped by the
	// default case in the select, but the validation itself succeeds.
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
	timer := NewScheduler(10 * time.Millisecond)

	timer.Stop()
	timer.Start()

	require.Nil(t, timer.ticker)
	require.Empty(t, timer.workers)
	require.False(t, timer.started)
}

func TestScheduler_ConcurrentStartStopLeavesNoWorkers(t *testing.T) {
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
	timer := NewScheduler(1 * time.Millisecond)
	timer.Start()

	// updateIntervalChan is unbuffered and ChangeInterval's send is
	// non-blocking (select/default), so goroutines spinning as fast as
	// possible maximize the chance run's interval-update case actually
	// wins the race against Stop reading st.ticker concurrently, rather
	// than every send just being dropped before run gets to it. Several
	// concurrent callers (not just one) raise the odds this reproduces
	// within a single run rather than needing many repeated runs.
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
