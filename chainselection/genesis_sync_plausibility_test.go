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
	"testing"

	"github.com/stretchr/testify/require"
)

// A node syncing from genesis has applied no block, so its local tip is block
// 0. It must bound a peer's delivered headers as it does at any other local
// tip: a reference it has caught up with is stale and takes the catch-up
// ceiling, and a reference within K ahead parks the connection instead of
// refusing it.
func TestEmptyLocalChainBoundsPeerFrontierLikeAnyLocalTip(t *testing.T) {
	t.Parallel()
	for _, local := range []uint64{0, bandLocal} {
		c := loneFarChain{localBlock: local, k: bandK}
		// Peers advertise the network tip from their first header.
		network := c.tip(4_687_076)
		newSelector := func(t *testing.T, reference uint64) *ChainSelector {
			cs := NewChainSelector(ChainSelectorConfig{
				SecurityParam:             bandK,
				DisableEventSubscriptions: true,
			})
			cs.SetLocalTip(c.tip(local))
			require.True(t, cs.updatePeerTipObserved(
				newTestConnectionId(1), network, c.tip(reference), nil,
			))
			return cs
		}
		t.Run(fmt.Sprintf("local %d stale reference", local), func(t *testing.T) {
			t.Parallel()
			cs := newSelector(t, local)
			require.True(t, cs.updatePeerTipObserved(
				newTestConnectionId(1),
				network,
				c.tip(local+2*bandK),
				nil,
			), "a header batch within the catch-up ceiling is accepted")
		})
		t.Run(fmt.Sprintf("local %d reference in band", local), func(t *testing.T) {
			t.Parallel()
			cs := newSelector(t, local+1)
			observed := c.tip(local + 1 + bandK + 1)
			cs.mutex.RLock()
			held := cs.catchUpBandHoldLocked(
				newTestConnectionId(1), observed, c.tip(local), false,
			)
			cs.mutex.RUnlock()
			require.True(t, held,
				"a header past reference+K waits for the ledger instead of "+
					"being refused")
		})
	}
}

// From an empty local chain the hold must still agree with admission, or a
// parked header would be refused on release.
func TestCatchUpBandHoldMatchesAdmissionOnEmptyLocalChain(t *testing.T) {
	t.Parallel()
	c := loneFarChain{localBlock: 0, k: bandK}
	network := c.tip(4_687_076)
	newSelector := func(t *testing.T, reference uint64) *ChainSelector {
		cs := NewChainSelector(ChainSelectorConfig{
			SecurityParam:             bandK,
			DisableEventSubscriptions: true,
		})
		cs.SetLocalTip(c.tip(0))
		require.True(t, cs.updatePeerTipObserved(
			newTestConnectionId(1), network, c.tip(reference), nil,
		))
		return cs
	}
	conn := newTestConnectionId(2)
	for reference := uint64(0); reference <= 3*bandK; reference++ {
		for observed := uint64(1); observed <= 4*bandK; observed++ {
			t.Run(fmt.Sprintf("ref=%d/observed=%d", reference, observed), func(t *testing.T) {
				t.Parallel()
				disposition := newSelector(t, reference).
					PreparePeerTipAdmissionDisposition(PeerTipUpdateEvent{
						ConnectionId:     conn,
						AdmissionID:      1,
						Tip:              network,
						ObservedTip:      c.tip(observed),
						ObservedPrevHash: c.prevHash(observed),
					})
				cs := newSelector(t, reference)
				cs.mutex.RLock()
				defer cs.mutex.RUnlock()
				require.Equal(t,
					disposition != PeerTipUpdateAccepted,
					cs.catchUpBandHoldLocked(conn, c.tip(observed), c.tip(0), true),
					"a held connection waits until the admission bound passes it")
			})
		}
	}
}
