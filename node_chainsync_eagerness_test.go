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

package dingo

import (
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestNodeEagernessRecoveryUsesProductionCallback(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"retained after callback", "session replacement", "unpublished withdrawal"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			n, points := newLiveLifecycleTestNode(t, 4)
			generation := n.ledgerState.CurrentHeaderCryptoVerificationGeneration()
			generationHooks := n.buildChainSelectorConfig(2, true, 0)
			n.chainSelector = chainselection.NewChainSelector(
				chainselection.ChainSelectorConfig{
					GenesisMode: true, SecurityParam: 2, GenesisWindowSlots: 1000, MinCorroboratingPeers: 1,
					CurrentHeaderCryptoVerificationGeneration: generationHooks.CurrentHeaderCryptoVerificationGeneration,
					HeaderCryptoVerificationGenerationCurrent: generationHooks.HeaderCryptoVerificationGenerationCurrent,
					PinHeaderCryptoVerificationGeneration:     generationHooks.PinHeaderCryptoVerificationGeneration,
				},
			)
			var peers []ouroboros.ConnectionId
			for index := range 4 {
				peer := newNodeTestConnId(uint(9100 + index))
				peers = append(peers, peer)
				u := chainselection.PeerTipUpdateEvent{
					ConnectionId: peer, AdmissionID: uint64(index + 1),
					ObservedTip: ochainsync.Tip{
						Point:       points[1],
						BlockNumber: 2,
					},
					Tip: ochainsync.Tip{
						Point: ocommon.NewPoint(
							1_000_000,
							make([]byte, 32),
						),
						BlockNumber: 500_000,
					},
					HeaderCryptoVerificationGeneration: generation,
				}
				require.True(t, n.chainSelector.PreparePeerTipAdmission(u))
				require.True(t, n.chainSelector.CommitPeerTipAdmission(u))
			}
			n.chainSelector.SetLocalTip(
				ochainsync.Tip{Point: points[3], BlockNumber: 4},
			)
			before := n.chainSelector.EagernessLimit().BlockNumber
			require.EqualValues(
				t,
				4,
				before,
				"the admitted anchor must constrain the pre-repair limit",
			)
			wire := n.newOuroborosConfig(false, 0, 0)
			require.NotNil(t, wire.ChainsyncRecoverDeliveredPeerTip)
			blocks, err := fixtures.GenerateBabbageChainAt(1, 2, 5)
			require.NoError(t, err)
			require.Equal(t, points[3].Hash, blocks[3].Hash().Bytes())
			var replaced atomic.Bool
			for _, peer := range peers {
				wire.ChainsyncRecoverDeliveredPeerTip(
					t.Context(),
					ochainsync.CallbackContext{ConnectionId: peer},
					chainselection.PeerTipUpdateEvent{
						ConnectionId: peer,
						ObservedTip: ochainsync.Tip{
							Point:       points[3],
							BlockNumber: 4,
						},
						HeaderCryptoVerificationGeneration: generation,
					},
					blocks[3].Header(),
					func() bool { return !replaced.Load() },
				)
			}
			require.EqualValues(
				t,
				6,
				n.chainSelector.EagernessLimit().BlockNumber,
				"production wiring must recover a missing path and retain it after callback cancellation",
			)
			switch name {
			case "session replacement":
				replaced.Store(true)
				require.Equal(
					t,
					before,
					n.chainSelector.EagernessLimit().BlockNumber,
					"a new bearer session must not inherit a previous session's proof",
				)
			case "unpublished withdrawal":
				require.NoError(
					t,
					n.chainManager.PrimaryChain().
						AddBlockHeader(t.Context(), blocks[4].Header()),
				)
				n.chainManager.PrimaryChain().ClearHeaders()
				require.Equal(
					t,
					generation,
					n.ledgerState.CurrentHeaderCryptoVerificationGeneration(),
					"the raw withdrawal must precede ledger publication in this regression",
				)
				require.Equal(
					t,
					before,
					n.chainSelector.EagernessLimit().BlockNumber,
					"stored proof reuse must reject a withdrawal without a new ledger generation",
				)
			}
		})
	}
}
