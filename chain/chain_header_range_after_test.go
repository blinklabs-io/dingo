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

package chain_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
)

// queueTestHeaders builds a chain manager whose primary chain has the given
// testBlocks queued as headers (never applied), so HeaderRangeAfter's window
// can be exercised against a known queue.
func queueTestHeaders(t *testing.T, headers []*MockBlock) *chain.Chain {
	t.Helper()
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, header := range headers {
		if err := c.AddBlockHeader(header); err != nil {
			t.Fatalf("unexpected error adding header to chain: %s", err)
		}
	}
	return c
}

// TestChainHeaderRangeAfterSkipZeroMatchesHeaderRange pins skip==0 as
// identical to HeaderRange for every count, since HeaderRangeAfter's window
// is meant to be a strict generalization of HeaderRange rather than a
// different read of the same queue.
func TestChainHeaderRangeAfterSkipZeroMatchesHeaderRange(t *testing.T) {
	t.Parallel()

	for _, count := range []int{1, 2, 3, 1000} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			t.Parallel()

			c := queueTestHeaders(t, testBlocks[3:])
			wantStart, wantEnd := c.HeaderRange(count)

			gotStart, gotEnd, available := c.HeaderRangeAfter(0, count)

			if available == 0 {
				t.Fatalf(
					"expected a non-zero window at skip=0, count=%d",
					count,
				)
			}
			if gotStart.Slot != wantStart.Slot ||
				!bytes.Equal(gotStart.Hash, wantStart.Hash) {
				t.Fatalf(
					"start mismatch at skip=0: HeaderRangeAfter=%#v HeaderRange=%#v",
					gotStart,
					wantStart,
				)
			}
			if gotEnd.Slot != wantEnd.Slot ||
				!bytes.Equal(gotEnd.Hash, wantEnd.Hash) {
				t.Fatalf(
					"end mismatch at skip=0: HeaderRangeAfter=%#v HeaderRange=%#v",
					gotEnd,
					wantEnd,
				)
			}
		})
	}
}

// TestChainHeaderRangeAfterSkipPastEndReportsZeroAvailable covers a skip at or
// beyond the queue's length: the window is empty and callers must be able to
// tell that from available rather than from a zero-value point pair, since a
// genuine skip=0,count=0 call also returns the zero value.
func TestChainHeaderRangeAfterSkipPastEndReportsZeroAvailable(t *testing.T) {
	t.Parallel()

	queued := testBlocks[3:] // 3 queued headers
	for _, skip := range []int{len(queued), len(queued) + 1, len(queued) + 50} {
		t.Run(fmt.Sprintf("skip=%d", skip), func(t *testing.T) {
			t.Parallel()

			c := queueTestHeaders(t, queued)
			start, end, available := c.HeaderRangeAfter(skip, 10)

			if available != 0 {
				t.Fatalf(
					"expected available=0 at skip=%d, got %d",
					skip,
					available,
				)
			}
			if start.Slot != 0 || len(start.Hash) != 0 {
				t.Fatalf("expected zero-value start, got %#v", start)
			}
			if end.Slot != 0 || len(end.Hash) != 0 {
				t.Fatalf("expected zero-value end, got %#v", end)
			}
		})
	}
}

// TestChainHeaderRangeAfterClampsAtQueueTail covers a window whose requested
// count would run past the end of the queue: available must clamp to what
// actually exists rather than index out of range or report more than is
// queued.
func TestChainHeaderRangeAfterClampsAtQueueTail(t *testing.T) {
	t.Parallel()

	queued := testBlocks[3:] // slots 60, 80, 100
	c := queueTestHeaders(t, queued)

	// skip=1 leaves headers at slots 80 and 100 (2 available); count=10 asks
	// for far more than that.
	start, end, available := c.HeaderRangeAfter(1, 10)

	if available != 2 {
		t.Fatalf(
			"expected available=2 (clamped to queue tail), got %d",
			available,
		)
	}
	wantStart := queued[1] // slot 80
	wantEnd := queued[2]   // slot 100
	if start.Slot != wantStart.SlotNumber() ||
		!bytes.Equal(start.Hash, wantStart.Hash().Bytes()) {
		t.Fatalf(
			"start mismatch: got %d.%x, wanted %d.%x",
			start.Slot, start.Hash,
			wantStart.SlotNumber(), wantStart.Hash().Bytes(),
		)
	}
	if end.Slot != wantEnd.SlotNumber() ||
		!bytes.Equal(end.Hash, wantEnd.Hash().Bytes()) {
		t.Fatalf(
			"end mismatch: got %d.%x, wanted %d.%x",
			end.Slot, end.Hash,
			wantEnd.SlotNumber(), wantEnd.Hash().Bytes(),
		)
	}
}

// TestChainHeaderRangeAfterExactWindowNotClamped is the control for the
// clamp above: a window that fits exactly within the queue must report every
// header in it, not one fewer from an off-by-one in the clamp arithmetic.
func TestChainHeaderRangeAfterExactWindowNotClamped(t *testing.T) {
	t.Parallel()

	queued := testBlocks[3:] // slots 60, 80, 100
	c := queueTestHeaders(t, queued)

	start, end, available := c.HeaderRangeAfter(1, 2)

	if available != 2 {
		t.Fatalf(
			"expected available=2 for an exact-fit window, got %d",
			available,
		)
	}
	if start.Slot != queued[1].SlotNumber() {
		t.Fatalf(
			"start mismatch: got slot %d, wanted %d",
			start.Slot,
			queued[1].SlotNumber(),
		)
	}
	if end.Slot != queued[2].SlotNumber() {
		t.Fatalf(
			"end mismatch: got slot %d, wanted %d",
			end.Slot,
			queued[2].SlotNumber(),
		)
	}
}

// TestChainHeaderRangeAfterNonPositiveCount mirrors
// TestChainHeaderRangeNonPositiveCount: a zero or negative count must return
// the zero value and available=0 rather than panicking on an out-of-range
// slice index.
func TestChainHeaderRangeAfterNonPositiveCount(t *testing.T) {
	t.Parallel()

	for _, count := range []int{0, -1, -1000} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			t.Parallel()

			c := queueTestHeaders(t, testBlocks)
			start, end, available := c.HeaderRangeAfter(0, count)

			if available != 0 {
				t.Fatalf(
					"expected available=0 for count=%d, got %d",
					count,
					available,
				)
			}
			if start.Slot != 0 || len(start.Hash) != 0 || end.Slot != 0 ||
				len(end.Hash) != 0 {
				t.Fatalf(
					"expected zero-value points for count=%d, got start=%#v end=%#v",
					count,
					start,
					end,
				)
			}
		})
	}
}

// TestChainHeaderRangeAfterConcurrentWithAppend races HeaderRangeAfter
// against concurrent AddBlockHeader calls under -race, exactly as
// HeaderRange's own RLock is exercised against concurrent mutation elsewhere
// in this package. Nothing here asserts on the observed windows -- headers
// are still being appended while reads land -- the purpose is exclusively to
// let the race detector catch an unsynchronized read of c.headers.
func TestChainHeaderRangeAfterConcurrentWithAppend(t *testing.T) {
	t.Parallel()

	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()

	const headerCount = 200
	headers := make([]*MockBlock, 0, headerCount)
	prevHash := ""
	for i := range headerCount {
		hash := fmt.Sprintf("%s%04d", testHashPrefix, i+1)
		headers = append(headers, &MockBlock{
			MockBlockNumber: uint64(i + 1),
			MockSlot:        uint64((i + 1) * 20),
			MockHash:        hash,
			MockPrevHash:    prevHash,
		})
		prevHash = hash
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, header := range headers {
			if err := c.AddBlockHeader(header); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range headerCount {
			c.HeaderRangeAfter(1, 5)
		}
	}()
	wg.Wait()
}
