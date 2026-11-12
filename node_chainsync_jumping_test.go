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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The dynamo's headers are staged at the Genesis apply gate until a witness
// vouches for them, and jump agreements are its witnesses. The fragment the
// ChainSync Jumping driver jumps to must therefore include staged headers,
// or a dynamo with no other witness never offers a jump and stays held.
func TestChainsyncJumpingDynamoFragmentIncludesStagedHeaders(t *testing.T) {
	t.Parallel()
	n := &Node{
		chainSelector: chainselection.NewChainSelector(
			chainselection.ChainSelectorConfig{
				GenesisMode:               true,
				SecurityParam:             20,
				GenesisWindowSlots:        1000,
				MinCorroboratingPeers:     1,
				ChainSyncJumping:          true,
				DisableEventSubscriptions: true,
			},
		),
	}
	n.config.genesisChainSyncJumping = true
	cfg := n.chainsyncJumpingConfig()
	require.True(t, cfg.Enabled)
	dynamo := staleObserveConnId()
	tipAt := func(block uint64) ochainsync.Tip {
		return ochainsync.Tip{
			Point: ocommon.NewPoint(
				block*10,
				fmt.Appendf(nil, "block-%d", block),
			),
			BlockNumber: block,
		}
	}
	for block := uint64(1); block <= 3; block++ {
		disposition := n.chainsyncObservePeerTip(
			chainselection.PeerTipUpdateEvent{
				ConnectionId: dynamo,
				AdmissionID:  block,
				Tip:          tipAt(3),
				ObservedTip:  tipAt(block),
			},
		)
		require.Equal(t, chainselection.PeerTipUpdateAccepted, disposition)
	}
	require.False(t, n.chainsyncApplyEligible(dynamo),
		"fixture: the dynamo is held at corroboration")

	points, head, ok := cfg.DynamoFragment(dynamo)
	require.True(t, ok,
		"a dynamo held at corroboration must offer its staged fragment")
	require.Equal(t, uint64(3), head)
	require.Equal(t, tipAt(3).Point, points[len(points)-1])
}
