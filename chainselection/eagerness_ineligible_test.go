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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hookEligibility is a ConnectionEligible hook the test can flip, standing in
// for the chainsync ingress admission state.
type hookEligibility struct {
	mu       sync.Mutex
	disabled map[ouroboros.ConnectionId]bool
}

func (h *hookEligibility) set(connId ouroboros.ConnectionId, eligible bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.disabled == nil {
		h.disabled = make(map[ouroboros.ConnectionId]bool)
	}
	h.disabled[connId] = !eligible
}

func (h *hookEligibility) eligible(connId ouroboros.ConnectionId) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.disabled[connId]
}

// newFrozenObserverScenario reproduces a live stall shape at k=5. The applied
// tip is block 30 and the immutable tip block 25, which is also the recorded
// anchor. Four peers share the chain "n" and intersect at block 31; one peer
// is frozen on a fragment (blocks 26..28, above the immutable tip) that shares
// no point with any of them.
func newFrozenObserverScenario(
	t *testing.T,
	hook *hookEligibility,
) (*ChainSelector, ouroboros.ConnectionId) {
	t.Helper()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      loeTestK,
		StaleTipThreshold:  time.Hour,
		ConnectionEligible: hook.eligible,
	})
	frozen := newTestConnectionId(1)
	feedLoEChain(cs, frozen, "f", 26, 28)
	feedLoEChain(cs, newTestConnectionId(2), "n", 31, 31)
	feedLoEChain(cs, newTestConnectionId(3), "n", 27, 33)
	feedLoEChain(cs, newTestConnectionId(4), "n", 30, 31)
	feedLoEChain(cs, newTestConnectionId(5), "n", 30, 31)
	cs.SetLocalTip(loeTip("n", 30))
	cs.setEagernessAnchor(&eagernessAnchor{
		point:       loeTip("n", 25).Point,
		blockNumber: 25,
	})
	return cs, frozen
}

func TestLimitOnEagernessReleasesWhenFrozenPeerIneligible(t *testing.T) {
	t.Parallel()
	hook := &hookEligibility{}
	cs, frozen := newFrozenObserverScenario(t, hook)

	pinned := cs.EagernessLimit()
	require.True(t, pinned.Active, "scenario must stay in Genesis mode")
	require.False(t, pinned.Intersected,
		"a frozen disjoint fragment must make the candidates share no point")
	require.Equal(t, uint64(30), pinned.BlockNumber,
		"the no-overlap fallback holds the limit at the applied tip")

	hook.set(frozen, false)

	released := cs.EagernessLimit()
	require.True(t, released.Active)
	assert.True(t, released.Intersected,
		"an ineligible peer must not be a limit-on-eagerness candidate")
	assert.Equal(t, uint64(31), released.Point.Slot/100)
	assert.Equal(t, uint64(31+loeTestK), released.BlockNumber)
	assert.True(t, cs.withinEagernessLimit(
		newTestConnectionId(3), 31, nil,
	), "the block past the applied tip must be admitted")

	hook.set(frozen, true)
	again := cs.EagernessLimit()
	assert.False(t, again.Intersected,
		"a re-admitted peer with an unchanged fragment is a candidate again")
	assert.Equal(t, uint64(31+loeTestK), again.BlockNumber,
		"re-admission falls back to the anchor recorded while it was out, "+
			"not to the applied tip")
}

// The hook is an additional gate: it never re-enables a peer that the
// peer-governance eligibility map has already excluded.
func TestConnectionEligibleHookDoesNotOverridePeerGovernance(t *testing.T) {
	t.Parallel()
	hook := &hookEligibility{}
	cs, frozen := newFrozenObserverScenario(t, hook)

	cs.SetConnectionEligible(frozen, false)
	hook.set(frozen, true)
	assert.True(t, cs.EagernessLimit().Intersected,
		"a permissive hook must not re-enable a governance-ineligible peer")
	cs.SetConnectionEligible(frozen, true)
	assert.False(t, cs.EagernessLimit().Intersected,
		"an eligible peer the hook admits is a candidate again")
}

// A peer re-admitted after a demotion still carries the fragment it froze on.
// Until it re-intersects it counts again, which is the only window in which it
// can pin the limit; the rollback that follows its promotion (the resync to
// the ledger tip) must drop that fragment from the candidate set.
func TestLimitOnEagernessReadmittedPeerRollbackDropsFrozenFragment(
	t *testing.T,
) {
	t.Parallel()
	hook := &hookEligibility{}
	cs, frozen := newFrozenObserverScenario(t, hook)

	hook.set(frozen, false)
	hook.set(frozen, true)
	require.False(t, cs.EagernessLimit().Intersected,
		"before it re-intersects the re-admitted peer counts with its "+
			"frozen fragment")

	cs.HandlePeerRollbackEvent(event.NewEvent(
		PeerRollbackEventType,
		PeerRollbackEvent{
			ConnectionId: frozen,
			Point:        loeTip("n", 30).Point,
			Tip:          loeTip("n", 33),
		},
	))

	limit := cs.EagernessLimit()
	assert.True(t, limit.Intersected,
		"the re-intersected peer must not keep the frozen fragment as a "+
			"candidate")
	assert.Equal(t, uint64(31+loeTestK), limit.BlockNumber)
}

// An observer keeps feeding verified deliveries and stays a corroboration
// witness: the hook removes it only from the Limit on Eagerness candidates.
func TestConnectionEligibleHookKeepsObserverAsWitness(t *testing.T) {
	t.Parallel()
	hook := &hookEligibility{}
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
		ConnectionEligible:    hook.eligible,
	})
	applicant := corrConn(8)
	observer := corrConn(9)
	hook.set(observer, false)
	tips := []ochainsync.Tip{
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	}
	feedFrontier(cs, applicant, tips...)
	for _, tip := range tips {
		cs.RecordDeliveredPeerTip(PeerTipUpdateEvent{
			ConnectionId: observer,
			Tip:          tip,
			ObservedTip:  tip,
		})
	}
	assert.True(t, cs.ShouldApplyIngress(applicant),
		"an observer's delivered history must still corroborate ingress")
}
