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
	"math/big"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gddFixture drives a Genesis-mode selector with k=40, a configured 120-slot
// window and a fake clock. Peers share a block at slot 10; the window under
// comparison is therefore (10, 130].
type gddFixture struct {
	cs          *ChainSelector
	mu          sync.Mutex
	now         time.Time
	disconnects []GenesisDensityDisconnect
}

func newGDDFixture(t *testing.T, genesis bool) *gddFixture {
	t.Helper()
	return newGDDFixtureWindow(t, genesis, 120)
}

// newGDDFixtureWindow is newGDDFixture with an explicit GenesisWindowSlots;
// zero leaves it unset.
func newGDDFixtureWindow(
	t *testing.T,
	genesis bool,
	windowSlots uint64,
) *gddFixture {
	t.Helper()
	f := &gddFixture{now: time.Unix(1_700_000_000, 0)}
	f.cs = NewChainSelector(ChainSelectorConfig{
		GenesisMode:        genesis,
		SecurityParam:      40,
		GenesisWindowSlots: windowSlots,
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

func (f *gddFixture) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// evaluate runs one selector evaluation after the rate-limit interval has
// elapsed. Header delivery can itself trigger an evaluation, which would
// otherwise leave the explicit one inside the interval.
func (f *gddFixture) evaluate() {
	f.advance(2 * GenesisDensityEvaluationInterval)
	f.cs.EvaluateAndSwitch()
}

func (f *gddFixture) disconnected() []ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ouroboros.ConnectionId, 0, len(f.disconnects))
	for _, d := range f.disconnects {
		out = append(out, d.ConnectionId)
	}
	return out
}

// deliver feeds one delivered header per slot to a peer. Block numbers count
// up from the peer's previous header so the plausibility checks accept the
// sequence. advertisedSlot is the slot of the tip the peer advertises; 0 means
// a tip far beyond anything delivered, which also keeps the selector from
// leaving Genesis mode on the first headers.
func (f *gddFixture) deliver(
	t *testing.T,
	connId ouroboros.ConnectionId,
	advertisedSlot uint64,
	slots ...uint64,
) {
	t.Helper()
	for _, slot := range slots {
		var block uint64 = 1
		if pt := f.cs.peerTips[connId]; pt != nil {
			block = pt.ObservedTip.BlockNumber + 1
		}
		obs := genesisTip(slot, fmt.Sprintf("h%d", slot), block)
		if advertisedSlot == 0 {
			advertisedSlot = 1_000_000
		}
		adv := genesisTip(advertisedSlot, "adv", block+1)
		require.True(t, f.cs.updatePeerTipObserved(connId, adv, obs, nil))
	}
}

func slotRange(from, to uint64) []uint64 {
	out := make([]uint64, 0, to-from+1)
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

func TestGDDDisconnectsProvablySparserPeer(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	dense, sparse := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
	// Sparse peer: 2 blocks in (10,130], head past the window end, so its
	// window is complete and cannot gain density. Its blocks sit after the
	// dense peer's, so the intersection stays at slot 10.
	f.deliver(t, sparse, 0, 10, 60, 100, 200)

	f.evaluate()

	assert.Equal(t, []ouroboros.ConnectionId{sparse}, f.disconnected())
}

func TestGDDKeepsPeerWithIncompleteWindow(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	dense, slow := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
	// One block in the window so far, but its head (60) is short of the
	// window end (130) and it advertises a tip far ahead: up to 70 more blocks
	// may still arrive, so it is not provably sparser than 40.
	f.deliver(t, slow, 0, 10, 60)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

// A peer whose window is incomplete is only disconnected for a rival offering
// more than k headers after the intersection, as in ouroboros-consensus
// densityDisconnect. Here the slow peer can gain at most 5 more blocks before
// the window end (130), so the dense peer's 40 exceed its upper bound of 6,
// but 40 headers is not more than k=40.
func TestGDDKeepsIncompletePeerAgainstRivalWithinK(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	dense, slow := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
	f.deliver(t, slow, 0, 10, 125)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

// Without a configured window the selector falls back to 3k slots, not 3k/f.
// At k=40 that window averages 6 honest blocks, so a peer on an honest short
// fork can show a complete window of 1 block against a rival's 2. Only a real
// Genesis window makes that comparison meaningful.
func TestGDDInactiveWithoutConfiguredWindow(t *testing.T) {
	t.Parallel()
	f := newGDDFixtureWindow(t, true, 0)
	rival, peer := corrConn(1), corrConn(2)
	f.deliver(t, rival, 0, 10, 11, 12)
	// 1 block in (10,130], head past the window end: complete.
	f.deliver(t, peer, 0, 10, 60, 200)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

// The same 1-versus-2 shape over a full 3k/f window, where the peer's head
// genuinely passes the window end, is upstream's lb0 == ub0 case and still
// disconnects.
func TestGDDDisconnectsCompleteSparsePeerOverGenesisWindow(t *testing.T) {
	t.Parallel()
	window := GenesisWindowSlotsForParams(40, big.NewRat(1, 20))
	require.Equal(t, uint64(2400), window)
	f := newGDDFixtureWindow(t, true, window)
	rival, peer := corrConn(1), corrConn(2)
	f.deliver(t, rival, 0, 10, 11, 12)
	// 1 block in (10,2410], head past the window end: complete.
	f.deliver(t, peer, 0, 10, 60, 2500)

	f.evaluate()

	assert.Equal(t, []ouroboros.ConnectionId{peer}, f.disconnected())
}

// A window derived without k or without a positive f is no Genesis window,
// so a selector configured from it reports nothing even for a complete
// sparse peer that a configured window would disconnect.
func TestGDDInactiveWithoutGenesisParams(t *testing.T) {
	t.Parallel()
	for name, window := range map[string]uint64{
		"zero k":     GenesisWindowSlotsForParams(0, big.NewRat(1, 20)),
		"nil f":      GenesisWindowSlotsForParams(40, nil),
		"zero f":     GenesisWindowSlotsForParams(40, new(big.Rat)),
		"negative f": GenesisWindowSlotsForParams(40, big.NewRat(-1, 20)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newGDDFixtureWindow(t, true, window)
			dense, sparse := corrConn(1), corrConn(2)
			f.deliver(
				t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...,
			)
			// 2 blocks, head past any window up to 6990 slots: complete.
			f.deliver(t, sparse, 0, 10, 60, 100, 7000)

			f.evaluate()

			assert.Empty(t, f.disconnected())
		})
	}
}

func TestGDDNeverDisconnectsLastPeer(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	only := corrConn(1)
	f.deliver(t, only, 0, 10, 60, 100, 200)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

func TestGDDKeepsLaggingPeerOnSameChain(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	ahead, behind := corrConn(1), corrConn(2)
	f.deliver(t, ahead, 0, append([]uint64{1000}, slotRange(1001, 1040)...)...)
	// The lagging peer is a strict prefix of the other chain and sits at its
	// own tip (it advertises its head): it forks nowhere, so although it can
	// gain no further density it must not be treated as sparse.
	f.deliver(t, behind, 1002, 1000, 1001, 1002)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

func TestGDDInactiveOutsideGenesisMode(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, false)
	dense, sparse := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
	f.deliver(t, sparse, 0, 10, 60, 100, 200)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

func TestGDDReportsEachPeerOnce(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	dense, sparse := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
	f.deliver(t, sparse, 0, 10, 60, 100, 200)

	f.evaluate()
	f.evaluate()

	assert.Len(t, f.disconnected(), 1)
}

// Evaluation is rate limited: a peer that becomes provably sparser inside the
// rate-limit interval is only reported once the interval has elapsed.
func TestGDDEvaluationIsRateLimited(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	dense, late := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{10}, slotRange(11, 50)...)...)
	f.deliver(t, late, 0, 10, 60)

	// First evaluation runs (late is undecidable) and starts the interval.
	f.cs.EvaluateAndSwitch()
	require.Empty(t, f.disconnected())

	// late's window completes; the interval has not elapsed.
	f.deliver(t, late, 0, 200)
	f.advance(GenesisDensityEvaluationInterval / 2)
	f.cs.EvaluateAndSwitch()
	assert.Empty(t, f.disconnected(), "evaluated again inside the interval")

	f.advance(GenesisDensityEvaluationInterval)
	f.cs.EvaluateAndSwitch()
	assert.Equal(t, []ouroboros.ConnectionId{late}, f.disconnected())
}

// A peer that has delivered everything up to its advertised tip is not
// thereby complete: its tip can still move forward. Treating it as unable to
// gain density would let any denser fork, honest or fabricated ahead of
// local ledger verification, disconnect honest peers sitting at their own
// tip on a short fork.
func TestGDDKeepsIdlePeerOnShortFork(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	dense, idle := corrConn(1), corrConn(2)
	f.deliver(t, dense, 0, append([]uint64{1000}, slotRange(1001, 1040)...)...)
	// One block in (1000,1120], at its advertised tip (1050): idle, window
	// not complete. Slots start at 1000 so the advertised tip does not end
	// Genesis mode.
	f.deliver(t, idle, 1050, 1000, 1050)

	f.evaluate()

	assert.Empty(t, f.disconnected())
}

// Density is compared at each pair's own intersection, so "sparser than"
// is not transitive: X loses to Y and Z at slot 1000, while Z beats Y at their
// later fork (1010). Every peer then has a rival in the same pass, and a pass
// that let an already-reported peer serve as a rival would disconnect all
// three.
func TestGDDKeepsOnePeerWhenDensityIsCyclic(t *testing.T) {
	t.Parallel()
	f := newGDDFixture(t, true)
	x, y, z := corrConn(1), corrConn(2), corrConn(3)
	shared := slotRange(1001, 1010)
	// (1000,1120]: X 20, Y 21, Z 19. (1010,1130]: Y 11, Z 19.
	f.deliver(t, x, 0, append([]uint64{1000}, slotRange(1101, 1120)...)...)
	yslots := append([]uint64{1000}, shared...)
	yslots = append(yslots, slotRange(1011, 1021)...)
	f.deliver(t, y, 0, append(yslots, 1200)...)
	zslots := append([]uint64{1000}, shared...)
	zslots = append(zslots, slotRange(1031, 1039)...)
	f.deliver(t, z, 0, append(zslots, slotRange(1121, 1130)...)...)

	f.evaluate()

	assert.Len(t, f.disconnected(), 2)
}
