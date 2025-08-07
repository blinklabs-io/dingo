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
	"time"
)

type ScheduledTask struct {
	mutex             sync.Mutex
	interval          int
	ticksSinceLastRun int
	taskFunc          func()
	runFailFunc       func()
}

type Scheduler struct {
	mutex              sync.Mutex
	interval           time.Duration
	ticker             *time.Ticker
	quit               chan struct{}
	updateIntervalChan chan time.Duration
	tasks              []*ScheduledTask
	startOnce          sync.Once
}

func NewScheduler(interval time.Duration) *Scheduler {
	return &Scheduler{
		interval:           interval,
		quit:               make(chan struct{}),
		updateIntervalChan: make(chan time.Duration),
		tasks:              []*ScheduledTask{},
	}
}

// Start the timer (run goroutine once)
func (st *Scheduler) Start() {
	st.startOnce.Do(func() {
		st.ticker = time.NewTicker(st.interval)
		go st.run()
	})
}

// Listens for tick events and interval updates and updating the ticker accordingly.
func (st *Scheduler) run() {
	for {
		select {
		case <-st.ticker.C:
			st.tick()
		case newInterval := <-st.updateIntervalChan:
			st.mutex.Lock()
			st.ticker.Stop()
			st.ticker = time.NewTicker(newInterval)
			st.interval = newInterval
			st.mutex.Unlock()
		case <-st.quit:
			st.ticker.Stop()
			return
		}
	}
}

// Increments per-task tick counters and executes tasks when due
func (st *Scheduler) tick() {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	for _, task := range st.tasks {
		task.ticksSinceLastRun++
		if task.ticksSinceLastRun >= task.interval {
			if task.mutex.TryLock() {
				go func() {
					task.taskFunc()
					task.mutex.Unlock()
				}()
			} else {
				// Run callback on failure to acquire task lock
				// This means a previous instance of the task is still running
				if task.runFailFunc != nil {
					task.runFailFunc()
				}
			}
			task.ticksSinceLastRun = 0
		}
	}
}

// Adds a new task to be scheduler
func (st *Scheduler) Register(interval int, taskFunc func(), runFailFunc func()) {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	st.tasks = append(st.tasks, &ScheduledTask{
		interval:          interval,
		ticksSinceLastRun: 0,
		taskFunc:          taskFunc,
		runFailFunc:       runFailFunc,
	})
}

// ChangeInterval updates the tick interval of the Scheduler at runtime.
func (st *Scheduler) ChangeInterval(newInterval time.Duration) {
	select {
	case st.updateIntervalChan <- newInterval:
	default:
	}
}

// Stop the timer (terminates)
func (st *Scheduler) Stop() {
	close(st.quit)
	if st.ticker != nil {
		st.ticker.Stop()
	}
}
