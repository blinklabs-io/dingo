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

package chain

import (
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

// TestHeaderSeqIsTotalAcrossChainsSharingABus pins the counter's owner.
//
// ChainHeaderEventType is one topic on the one event bus ChainManager hands to
// every chain it builds -- the primary chain at load, and both fork
// constructors. A consumer (VoteManager.rollbackProtectedLocked) compares the
// sequence numbers it receives against each other as a single total order. A
// per-Chain counter restarts at 1 on a second publishing chain, so that
// consumer would compare a fresh low number against an older high one and
// protect or prune announcements belonging to the wrong mutation.
//
// Today the primary chain is the only publisher, so this is a latent defect
// rather than a live one; NewChain and NewChainFromIntersect are exported and
// have no non-test callers. Owning the counter at the manager makes the
// guarantee structural instead of relying on that staying true.
func TestHeaderSeqIsTotalAcrossChainsSharingABus(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := NewManager(nil, bus)
	require.NoError(t, err)

	primary := cm.PrimaryChain()
	require.NotNil(t, primary)
	// A fork chain as the manager's constructors build one: same manager,
	// same bus. They are not used here because they need a block store to
	// resolve an intersect point, and the field under test is the counter.
	fork := &Chain{id: 2, manager: cm, eventBus: cm.eventBus}

	var got []uint64
	for _, c := range []*Chain{primary, fork, primary, fork, fork} {
		c.mutex.Lock()
		got = append(got, c.nextHeaderSeqLocked())
		c.mutex.Unlock()
	}
	require.Equal(
		t,
		[]uint64{1, 2, 3, 4, 5},
		got,
		"two chains on one bus must stamp one strictly increasing sequence",
	)
}

// TestHeaderSeqConcurrentStampsAreUnique covers the counter being shared
// across chains that stamp under their own locks rather than the manager's, so
// nothing but the atomic serializes them.
func TestHeaderSeqConcurrentStampsAreUnique(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := NewManager(nil, bus)
	require.NoError(t, err)

	chains := []*Chain{
		cm.PrimaryChain(),
		{id: 2, manager: cm, eventBus: cm.eventBus},
		{id: 3, manager: cm, eventBus: cm.eventBus},
	}
	const perChain = 200

	var wg sync.WaitGroup
	seqs := make([][]uint64, len(chains))
	for i, c := range chains {
		wg.Go(func() {
			out := make([]uint64, 0, perChain)
			for range perChain {
				c.mutex.Lock()
				out = append(out, c.nextHeaderSeqLocked())
				c.mutex.Unlock()
			}
			seqs[i] = out
		})
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, len(chains)*perChain)
	for _, out := range seqs {
		for _, seq := range out {
			require.NotZero(t, seq, "zero means unsequenced to consumers")
			_, dup := seen[seq]
			require.False(t, dup, "sequence %d stamped twice", seq)
			seen[seq] = struct{}{}
		}
	}
	require.Len(t, seen, len(chains)*perChain)
}

// TestHeaderSeqFallsBackForManagerlessChain documents the one case the
// manager-owned counter cannot serve: a Chain built as a struct literal
// without a manager, which only this package's tests do. Such a chain shares
// no bus with any other, so its own counter is already a complete order.
func TestHeaderSeqFallsBackForManagerlessChain(t *testing.T) {
	c := &Chain{}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	require.Equal(t, uint64(1), c.nextHeaderSeqLocked())
	require.Equal(t, uint64(2), c.nextHeaderSeqLocked())
}
