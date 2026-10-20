// Copyright 2026 Blink Labs Software
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

package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func startMemoryProfile(path string) func() error {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var peakAllocated, peakInUse, peakRetained uint64
		for {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			peakAllocated = max(peakAllocated, stats.HeapAlloc)
			peakInUse = max(peakInUse, stats.HeapInuse)
			peakRetained = max(peakRetained, stats.Sys-stats.HeapReleased)
			select {
			case <-stop:
				fmt.Fprintf(os.Stderr,
					"Memory diagnostic (bytes): peak_alloc=%d peak_inuse=%d "+
						"peak_runtime_retained=%d final_alloc=%d "+
						"final_idle=%d final_released=%d gc_cycles=%d\n",
					peakAllocated, peakInUse, peakRetained, stats.HeapAlloc,
					stats.HeapIdle, stats.HeapReleased, stats.NumGC)
				return
			case <-ticker.C:
			}
		}
	}()
	return func() error {
		close(stop)
		<-done
		// Do not force a collection: it changes the workload's retained heap.
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create heap profile: %w", err)
		}
		return errors.Join(pprof.WriteHeapProfile(file), file.Close())
	}
}
