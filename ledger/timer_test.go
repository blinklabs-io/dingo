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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScheduler_RegistersAndRunsTask(t *testing.T) {
	var counter int32

	// Create a Scheduler with 10ms tick interval
	timer := NewScheduler(10 * time.Millisecond)
	timer.Start()
	defer timer.Stop()

	// Registering task to execute every 3 ticks
	timer.Register(3, func() {
		atomic.AddInt32(&counter, 1)
	}, nil)

	// Wait for task to run at least 2 times (polls instead of fixed sleep)
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&counter) >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"expected task to run at least 2 times",
	)
}

func TestScheduler_ChangeInterval(t *testing.T) {
	var counter int32

	// Create a Scheduler with 50ms tick interval
	timer := NewScheduler(50 * time.Millisecond)
	timer.Start()
	defer timer.Stop()

	// Registering task with 50ms tick interval to execute for every 1 tick
	timer.Register(1, func() {
		atomic.AddInt32(&counter, 1)
	}, nil)

	// Wait for at least 2 executions before changing interval
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&counter) >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"expected at least 2 executions before interval change",
	)
	beforeChange := atomic.LoadInt32(&counter)

	// Change interval to 200ms
	timer.ChangeInterval(200 * time.Millisecond)

	// Wait for at least 1 execution after interval change
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&counter)-beforeChange >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"expected at least 1 execution after interval change",
	)

	// Allow enough time for potential additional ticks
	time.Sleep(500 * time.Millisecond)

	secondCount := atomic.LoadInt32(&counter)
	afterChange := secondCount - beforeChange

	if afterChange < 1 || afterChange > 3 {
		t.Errorf(
			"timer did not respect interval change, ran too frequently: %d more ticks",
			afterChange,
		)
	}
}

func TestSchedulerRunFailFunc(t *testing.T) {
	var failCounter int32

	// Create a Scheduler with 10ms tick interval
	timer := NewScheduler(10 * time.Millisecond)
	timer.Start()
	defer timer.Stop()

	// Registering task to execute every 3 ticks
	timer.Register(
		3,
		// Task func
		func() {
			time.Sleep(100 * time.Millisecond)
		},
		// Run fail func
		func() {
			atomic.AddInt32(&failCounter, 1)
		},
	)

	// Wait for the fail function to be called at least 3 times
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&failCounter) >= 3
	}, 5*time.Second, 10*time.Millisecond,
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
