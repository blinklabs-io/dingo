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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A sequencer created with a start offset processes only [start..total-1] in
// contiguous order; lower indices are never expected and never block it. This
// is what lets a catch-up download begin at the immutable-import marker instead
// of zero.
func TestSequencerStartOffsetProcessesFromStart(t *testing.T) {
	var mu sync.Mutex
	var order []uint64
	seq := newInOrderSequencerFrom(3, 6, func(num uint64) error {
		mu.Lock()
		order = append(order, num)
		mu.Unlock()
		return nil
	})
	// Complete only the in-range items, out of order.
	for _, n := range []uint64{5, 3, 4} {
		seq.Complete(n)
	}
	require.NoError(t, seq.Wait())
	assert.Equal(t, []uint64{3, 4, 5}, order)
}

// newInOrderSequencer is the start=0 special case and must be unchanged.
func TestSequencerStartOffsetZeroMatchesDefault(t *testing.T) {
	var mu sync.Mutex
	var order []uint64
	seq := newInOrderSequencerFrom(0, 3, func(num uint64) error {
		mu.Lock()
		order = append(order, num)
		mu.Unlock()
		return nil
	})
	for _, n := range []uint64{2, 0, 1} {
		seq.Complete(n)
	}
	require.NoError(t, seq.Wait())
	assert.Equal(t, []uint64{0, 1, 2}, order)
}
