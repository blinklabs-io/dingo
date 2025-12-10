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

type SchedulerConfig struct {
	WorkerPoolSize int
	TaskQueueSize  int
}

func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		WorkerPoolSize: 10,
		TaskQueueSize:  100,
	}
}

type ScheduledTask struct {
	taskFunc          func()
	runFailFunc       func()
	interval          int
	ticksSinceLastRun int
	mutex             sync.Mutex
}

type Scheduler struct {
	ticker             *time.Ticker
	quit               chan struct{}
	updateIntervalChan chan time.Duration
	tasks              []*ScheduledTask
	interval           time.Duration
	startOnce          sync.Once
	mutex              sync.Mutex
	// Worker pool fields
	workerPoolSize int
	taskQueue      chan func()
	workers        []chan struct{}
	workerWg       sync.WaitGroup
}

func NewScheduler(interval time.Duration) *Scheduler {
	return NewSchedulerWithConfig(interval, DefaultSchedulerConfig())
}

func NewSchedulerWithConfig(
	interval time.Duration,
	config SchedulerConfig,
) *Scheduler {
	// Validate and coerce configuration values
	if config.WorkerPoolSize <= 0 {
		config.WorkerPoolSize = 10 // Default to 10 workers
	}
	if config.TaskQueueSize <= 0 {
		config.TaskQueueSize = 100 // Default to 100 task queue size
	}

	return &Scheduler{
		interval:           interval,
		quit:               make(chan struct{}),
		updateIntervalChan: make(chan time.Duration),
		tasks:              []*ScheduledTask{},
		workerPoolSize:     config.WorkerPoolSize,
		taskQueue:          make(chan func(), config.TaskQueueSize),
		workers:            make([]chan struct{}, 0),
	}
}

// Start the timer (run goroutine once)
func (st *Scheduler) Start() {
	st.startOnce.Do(func() {
		st.ticker = time.NewTicker(st.interval)
		st.startWorkerPool()
		go st.run()
	})
}

// startWorkerPool initializes the worker pool
func (st *Scheduler) startWorkerPool() {
	for i := 0; i < st.workerPoolSize; i++ {
		quit := make(chan struct{})
		st.workers = append(st.workers, quit)
		st.workerWg.Add(1)
		go st.worker(quit)
	}
}

// worker runs tasks from the queue
func (st *Scheduler) worker(quit <-chan struct{}) {
	defer st.workerWg.Done()
	for {
		select {
		case task := <-st.taskQueue:
			task()
		case <-quit:
			return
		}
	}
}

// stopWorkerPool shuts down the worker pool
func (st *Scheduler) stopWorkerPool() {
	for _, quit := range st.workers {
		close(quit)
	}
	st.workerWg.Wait()
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
				// Submit task to worker pool instead of spawning goroutine
				select {
				case st.taskQueue <- func() {
					defer task.mutex.Unlock()
					task.taskFunc()
				}:
				default:
					// Queue is full, unlock and call failure function
					task.mutex.Unlock()
					if task.runFailFunc != nil {
						task.runFailFunc()
					}
				}
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
func (st *Scheduler) Register(
	interval int,
	taskFunc func(),
	runFailFunc func(),
) {
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
	st.stopWorkerPool()
}
