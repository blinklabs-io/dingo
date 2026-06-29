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

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Items completed out of order must be processed in strict contiguous
// index order starting at 0, exactly once each.
func TestSequencerProcessesInContiguousOrder(t *testing.T) {
	var mu sync.Mutex
	var order []uint64
	seq := newInOrderSequencer(5, func(num uint64) error {
		mu.Lock()
		order = append(order, num)
		mu.Unlock()
		return nil
	})
	for _, n := range []uint64{3, 1, 0, 4, 2} {
		seq.Complete(n)
	}
	require.NoError(t, seq.Wait())
	assert.Equal(t, []uint64{0, 1, 2, 3, 4}, order)
}

// Concurrent completions from many goroutines must still process in
// strict 0..N-1 order.
func TestSequencerConcurrentCompletions(t *testing.T) {
	const n = uint64(200)
	var mu sync.Mutex
	var order []uint64
	seq := newInOrderSequencer(n, func(num uint64) error {
		mu.Lock()
		order = append(order, num)
		mu.Unlock()
		return nil
	})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(num uint64) {
			defer wg.Done()
			seq.Complete(num)
		}(i)
	}
	wg.Wait()
	require.NoError(t, seq.Wait())
	want := make([]uint64, n)
	for i := range want {
		want[i] = uint64(i)
	}
	assert.Equal(t, want, order)
}

// A processing error stops the sequencer and is returned from Wait;
// later indices are not processed.
func TestSequencerStopsOnProcessError(t *testing.T) {
	boom := errors.New("boom")
	var mu sync.Mutex
	var processed []uint64
	seq := newInOrderSequencer(5, func(num uint64) error {
		mu.Lock()
		processed = append(processed, num)
		mu.Unlock()
		if num == 2 {
			return boom
		}
		return nil
	})
	for _, n := range []uint64{0, 1, 2, 3, 4} {
		seq.Complete(n)
	}
	require.ErrorIs(t, seq.Wait(), boom)
	assert.Equal(t, []uint64{0, 1, 2}, processed)
}

// Cancel unblocks Wait even when not all indices have completed (e.g. a
// fetch worker failed and the contiguous prefix can never advance).
func TestSequencerCancelUnblocksWait(t *testing.T) {
	cancelled := errors.New("cancelled")
	seq := newInOrderSequencer(5, func(uint64) error { return nil })
	seq.Complete(0)
	seq.Complete(1)
	// index 2 never completes; cancel should release Wait.
	seq.Cancel(cancelled)
	require.ErrorIs(t, seq.Wait(), cancelled)
}
