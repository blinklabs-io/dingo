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

package chainselection

import (
	"fmt"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type standoffFixture struct {
	cs          *ChainSelector
	mu          sync.Mutex
	now         time.Time
	disconnects []GenesisDensityDisconnect
}

func newStandoffFixture() *standoffFixture {
	f := &standoffFixture{now: time.Unix(1_700_000_000, 0)}
	f.cs = NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
		// Wider than the slots k blocks span, as 3k/f is on a real network,
		// so the Genesis Density Disconnector cannot yet prove either fork
		// sparser.
		GenesisWindowSlots: 1000,
		OnGenesisDensityDisconnect: func(d GenesisDensityDisconnect) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.disconnects = append(f.disconnects, d)
		},
	})
	f.cs.nowFn = func() time.Time {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.now
	}
	return f
}

// evaluate runs one evaluation after the Genesis density rate limit.
func (f *standoffFixture) evaluate() []GenesisDensityDisconnect {
	f.mu.Lock()
	f.now = f.now.Add(2 * GenesisDensityEvaluationInterval)
	f.disconnects = nil
	f.mu.Unlock()
	f.cs.EvaluateAndSwitch()
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]GenesisDensityDisconnect(nil), f.disconnects...)
}

// standoffBaseSlot keeps every delivered slot far enough past the local tip
// that the selector stays in Genesis mode for the fixture's window.
const standoffBaseSlot = 100_000

func standoffTip(prefix string, block, slot uint64) ochainsync.Tip {
	tip := loeTip(prefix, block)
	tip.Point.Slot = slot
	return tip
}

// feedStandoff delivers blocks from..to, block n at slot(n).
func feedStandoff(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	prefix string,
	from, to uint64,
	slot func(block uint64) uint64,
) {
	for block := from; block <= to; block++ {
		cs.UpdatePeerTip(connId, standoffTip(prefix, block, slot(block)), nil)
	}
}

// sharedSlot places the common prefix, which ends at block 10.
func sharedSlot(block uint64) uint64 { return standoffBaseSlot + block*100 }

// sparseSlot continues a fork at one block per 100 slots.
func sparseSlot(block uint64) uint64 { return standoffBaseSlot + block*100 }

// denseSlot continues a fork after block 10 at one block per slot.
func denseSlot(block uint64) uint64 { return sharedSlot(10) + block - 10 }

// hold pauses connId at the limit for the rest of the test.
func hold(t *testing.T, cs *ChainSelector, connId ouroboros.ConnectionId) {
	t.Helper()
	t.Cleanup(cs.pauseEagerness(connId))
}

// Two forks that both run more than k past their fork point each pause at the
// limit, so neither can show the window-wide density the Genesis Density
// Disconnector needs. The denser fork must win even when the sparse one is the
// incumbent, and the sparse one is removed so the limit can move.
func TestLimitOnEagernessStandoffRemovesSparserFork(t *testing.T) {
	t.Parallel()
	f := newStandoffFixture()
	sparse := newTestConnectionId(1)
	dense := newTestConnectionId(2)
	feedStandoff(f.cs, sparse, "c", 1, 10, sharedSlot)
	feedStandoff(f.cs, sparse, "s", 11, 16, sparseSlot)
	f.evaluate()
	require.Equal(t, sparse, *f.cs.GetBestPeer())

	feedStandoff(f.cs, dense, "c", 1, 10, sharedSlot)
	feedStandoff(f.cs, dense, "d", 11, 16, denseSlot)
	require.Equal(t, uint64(10+loeTestK), f.cs.EagernessLimit().BlockNumber)
	hold(t, f.cs, sparse)
	hold(t, f.cs, dense)

	got := f.evaluate()
	require.Len(t, got, 1, "the standoff must remove exactly one fork")
	assert.Equal(t, sparse, got[0].ConnectionId)
	assert.Equal(t, dense, got[0].DominatingConnectionId)
	assert.True(t, got[0].EagernessStandoff)
	assert.Empty(t, f.evaluate(), "the surviving fork is never removed")
}

// A fork still streaming below the limit may yet be denser, so the other
// fork's pause alone decides nothing.
func TestLimitOnEagernessStandoffWaitsForEveryCandidateToPause(t *testing.T) {
	t.Parallel()
	f := newStandoffFixture()
	sparse := newTestConnectionId(1)
	dense := newTestConnectionId(2)
	feedStandoff(f.cs, sparse, "c", 1, 10, sharedSlot)
	feedStandoff(f.cs, sparse, "s", 11, 16, sparseSlot)
	feedStandoff(f.cs, dense, "c", 1, 10, sharedSlot)
	feedStandoff(f.cs, dense, "d", 11, 14, denseSlot)
	hold(t, f.cs, sparse)

	assert.Empty(t, f.evaluate())
}

// Equal density leaves nothing to decide on but transport, which is the same
// order the selector uses: the incumbent stays.
func TestLimitOnEagernessStandoffTieKeepsIncumbent(t *testing.T) {
	t.Parallel()
	f := newStandoffFixture()
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedStandoff(f.cs, a, "c", 1, 10, sharedSlot)
	feedStandoff(f.cs, a, "a", 11, 16, sparseSlot)
	f.evaluate()
	require.Equal(t, a, *f.cs.GetBestPeer())
	feedStandoff(f.cs, b, "c", 1, 10, sharedSlot)
	feedStandoff(f.cs, b, "b", 11, 16, sparseSlot)
	hold(t, f.cs, a)
	hold(t, f.cs, b)

	got := f.evaluate()
	require.Len(t, got, 1)
	assert.Equal(t, b, got[0].ConnectionId)
}

func TestEagernessPausedReflectsHeldStream(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 10)
	require.False(t, cs.EagernessPaused(a))
	require.False(t, cs.EagernessPaused(newTestConnectionId(9)),
		"an untracked peer is never held")

	release := cs.pauseEagerness(a)
	assert.True(t, cs.EagernessPaused(a))
	release()
	assert.False(t, cs.EagernessPaused(a))
}

// A peer that delivered all of a short dead fork and went idle is a candidate
// that holds the limit at its fork point while an honest peer is still
// streaming a denser chain. The honest peer is held at the limit, not allowed
// to run past it, and is released once the idle peer ages out because only an
// idle peer is ever aged out: a held one is exempt.
func TestLimitOnEagernessIdleDeadForkHoldsHonestPeerUntilStale(t *testing.T) {
	t.Parallel()
	clock := &atomicClock{}
	clock.nanos.Store(time.Now().UnixNano())
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      loeTestK,
		GenesisWindowSlots: 1000,
		StaleTipThreshold:  time.Minute,
	})
	installFakeClock2(cs, clock.Now)
	dead := newTestConnectionId(1)
	honest := newTestConnectionId(2)
	feedStandoff(cs, dead, "c", 1, 10, sharedSlot)
	feedStandoff(cs, dead, "x", 11, 12, sparseSlot)
	feedStandoff(cs, honest, "c", 1, 10, sharedSlot)
	feedStandoff(cs, honest, "h", 11, 15, denseSlot)
	require.Equal(t, uint64(10+loeTestK), cs.EagernessLimit().BlockNumber)

	requireAdmitted(t, awaitAsync(t.Context(), cs, honest, 15))
	done := awaitAsync(t.Context(), cs, honest, 16)
	requireStillBlocked(t, done)
	require.Eventually(t, func() bool {
		return cs.EagernessPaused(honest)
	}, 10*time.Second, time.Millisecond)

	// The honest peer keeps reporting while held up; the dead one is silent.
	clock.Advance(2 * time.Minute)
	cs.UpdatePeerTip(honest, standoffTip("h", 15, denseSlot(15)), nil)
	requireAdmitted(t, done)
}

// BenchmarkEagernessLimit measures the limit computation that runs under the
// selector's write lock on every tip update: fifty peers with full 2k+1
// fragments at mainnet k that agree on all but their last blocks.
func BenchmarkEagernessLimit(b *testing.B) {
	const k = 2160
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: k,
		// Keeps the selector in Genesis mode from the first header.
		GenesisWindowSlots: 1,
	})
	for p := 1; p <= 50; p++ {
		connId := newTestConnectionId(p)
		for block := uint64(1); block <= 2*k+1; block++ {
			prefix := "c"
			if block > 2*k-3 {
				prefix = fmt.Sprintf("p%d-", p)
			}
			cs.UpdatePeerTip(connId, loeTip(prefix, block), nil)
		}
	}
	if l := cs.EagernessLimit(); !l.Active || !l.Intersected {
		b.Fatalf("limit not exercised: %+v", l)
	}
	b.ResetTimer()
	for b.Loop() {
		cs.mutex.Lock()
		cs.computeEagernessLimitLocked(nil, cs.localTip)
		cs.mutex.Unlock()
	}
}
