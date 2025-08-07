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

	// Sleeping for 100ms to allow multiple ticks to occur
	time.Sleep(100 * time.Millisecond)

	finalCount := atomic.LoadInt32(&counter)
	if finalCount < 2 {
		t.Errorf("Expected task to run at least 2 times, but got %d", finalCount)
	}
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

	// Waiting 120ms to observe task execution at 50ms interval
	time.Sleep(120 * time.Millisecond)
	beforeChange := atomic.LoadInt32(&counter)
	if beforeChange < 2 {
		t.Errorf("Expected at least 2 executions before interval change, got %d", beforeChange)
	}

	// Change interval to 200ms
	timer.ChangeInterval(200 * time.Millisecond)

	// Sleep for 500ms after interval change to observe behavior
	time.Sleep(500 * time.Millisecond)

	secondCount := atomic.LoadInt32(&counter)
	afterChange := secondCount - beforeChange

	if afterChange < 1 || afterChange > 3 {
		t.Errorf("timer did not respect interval change, ran too frequently: %d more ticks", afterChange)
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
			time.Sleep(50 * time.Millisecond)
		},
		// Run fail func
		func() {
			atomic.AddInt32(&failCounter, 1)
		},
	)

	// Sleeping for 200ms to allow multiple ticks to occur
	time.Sleep(200 * time.Millisecond)

	finalCount := atomic.LoadInt32(&failCounter)
	if finalCount < 3 {
		t.Errorf("Expected failure to run task at least 3 times, but got %d", finalCount)
	}
}
