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

package mithril

import "sync"

// inOrderSequencer turns out-of-order completions into in-order
// processing. Parallel producers call Complete(num) as items finish in
// any order; a single background consumer invokes process(num) in strict
// contiguous order 0,1,2,...,total-1, each exactly once. This is the
// reorder buffer that lets the immutable download fetch chunks in
// parallel while the blob-store copy processes them sequentially.
type inOrderSequencer struct {
	total   uint64
	process func(num uint64) error

	mu       sync.Mutex
	cond     *sync.Cond
	done     map[uint64]bool
	next     uint64
	err      error
	finished bool
}

// newInOrderSequencer starts a consumer goroutine that processes the
// contiguous prefix of completed items in order. process is invoked
// outside the lock so producers may keep calling Complete concurrently.
func newInOrderSequencer(
	total uint64,
	process func(num uint64) error,
) *inOrderSequencer {
	s := &inOrderSequencer{
		total:   total,
		process: process,
		done:    make(map[uint64]bool),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.run()
	return s
}

// Complete marks item num as ready. Safe to call concurrently and in any
// order. Indices beyond total or already-completed are tolerated.
func (s *inOrderSequencer) Complete(num uint64) {
	s.mu.Lock()
	s.done[num] = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Cancel stops the sequencer with err, unblocking Wait even if the
// contiguous prefix can never advance (e.g. a producer failed). The
// first cancel/process error wins.
func (s *inOrderSequencer) Cancel(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *inOrderSequencer) run() {
	s.mu.Lock()
	for s.next < s.total && s.err == nil {
		for !s.done[s.next] && s.err == nil {
			s.cond.Wait()
		}
		if s.err != nil {
			break
		}
		num := s.next
		s.mu.Unlock()
		err := s.process(num)
		s.mu.Lock()
		if err != nil {
			if s.err == nil {
				s.err = err
			}
			break
		}
		s.next++
	}
	s.finished = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Wait blocks until every item has been processed in order or the
// sequencer stops early via a process error or Cancel. It returns the
// first error, or nil on full completion.
func (s *inOrderSequencer) Wait() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for !s.finished {
		s.cond.Wait()
	}
	return s.err
}
