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

package consensus

import (
	"bytes"
	"fmt"
	"net"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// runConsensusVector replays a consensus-category vector against
// dingo's chain-selection subsystem and verifies the selected chain
// matches expected_output.final_tip.
//
// Scope (v1): drives only chainselection.ChainSelector. The captured
// roll_forward stream gives us per-peer tips; chain selection is the
// observable that answers "given these peer chains, which one would
// Praos pick?" — the question fork_and_select_v1 exists to test.
//
// Out of scope (v1): driving the full chain.Manager + dingo
// chainsync.State + EventBus composition. Doing so requires either
// full block bodies (which the captured trace doesn't carry — only
// headers come through chainsync) or reaching past the public API
// surface (the chainsync roll-forward handler is private to package
// ouroboros). The chain-selection result subsumes the meaningful
// outcome for the current scenario; extending to chain manager is a
// later step once captures include block bodies or the runner gains
// a synthetic-block helper.
func runConsensusVector(
	t *testing.T,
	title string,
	capture *format.ConsensusCapture,
) error {
	t.Helper()
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{})

	// Feed each peer's last roll_forward tip into the chain selector
	// using a synthesized per-peer ConnectionId.
	for _, peer := range capture.Peers {
		tip, ok := lastServedTip(peer.Served)
		if !ok {
			return fmt.Errorf(
				"peer %d: served trace has no roll_forward — "+
					"nothing to feed the chain selector",
				peer.PeerID,
			)
		}
		// vrfOutput=nil: peer B's block count substantially exceeds
		// peer A's in the fork_and_select_v1 scenario, so no
		// tiebreaker is needed. Scenarios where two chains tie at
		// the same block number would need to capture VRF outputs;
		// the format doesn't carry them yet.
		if !cs.UpdatePeerTip(testConnectionID(peer.PeerID), tip, nil) {
			return fmt.Errorf(
				"peer %d: chain selector rejected tip update",
				peer.PeerID,
			)
		}
	}

	// Force a synchronous re-evaluation. The background loop fires
	// every EvaluationInterval (default 200ms); EvaluateAndSwitch
	// runs the same evaluation immediately so the test doesn't have
	// to sleep.
	cs.EvaluateAndSwitch()

	// Tier 1: confirm the selector landed on a peer, and that peer's
	// tip matches expected_output.final_tip.
	best := cs.GetBestPeer()
	if best == nil {
		return fmt.Errorf(
			"%s: chain selector produced no best peer", title,
		)
	}
	bestTip := cs.GetPeerTip(*best)
	if bestTip == nil {
		return fmt.Errorf(
			"%s: chain selector best peer %v has no tip",
			title, *best,
		)
	}
	if err := assertTipMatches(
		bestTip.Tip, capture.ExpectedOutput.FinalTip,
	); err != nil {
		return fmt.Errorf("%s: final_tip: %w", title, err)
	}
	return nil
}

// testConnectionID synthesizes a deterministic ConnectionId from a
// peer id. Distinct peer ids yield distinct conn ids; the same peer
// id always yields the same conn id within a single test run.
//
// peerID is expected to be small (committed scenarios use 0..N where
// N is the peer count, currently ≤ 2). A peerID larger than ~55000
// would overflow the 16-bit TCP port space below; the function would
// still produce *some* ConnectionId via net.ResolveTCPAddr error
// fall-through but the result wouldn't be unique. Add a bounds
// check here if a future scenario starts assigning large peer ids.
func testConnectionID(peerID uint64) ouroboros.ConnectionId {
	local, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	// 10000 + peerID stays in the ephemeral port range and keeps
	// the synthesized address human-readable in test failure output.
	remote, _ := net.ResolveTCPAddr(
		"tcp",
		fmt.Sprintf("127.0.0.1:%d", 10000+peerID),
	)
	return ouroboros.ConnectionId{LocalAddr: local, RemoteAddr: remote}
}

// lastServedTip walks served in reverse and returns the tip of the
// most recent roll_forward. Returns the captured Tip's slot + hash +
// block_number verbatim — the recorder copies all three off the
// gouroboros chainsync.Tip callback argument at capture time.
func lastServedTip(
	served []format.ServedMessage,
) (ochainsync.Tip, bool) {
	for _, m := range slices.Backward(served) {
		if m.MsgType != format.ChainSyncMsgRollForward || m.Tip == nil {
			continue
		}
		return ochainsync.Tip{
			Point: ocommon.Point{
				Slot: m.Tip.Slot,
				Hash: append([]byte(nil), m.Tip.Hash...),
			},
			BlockNumber: m.Tip.BlockNumber,
		}, true
	}
	return ochainsync.Tip{}, false
}

// assertTipMatches compares the selected chain's tip against the
// vector's recorded final_tip on slot + hash + block number. Block
// number comparison catches a vector that records the right hash
// but wrong chain-length count, which would silently desync chain
// selection in any multi-peer scenario where peers tie on slot but
// differ on block count.
func assertTipMatches(got ochainsync.Tip, want format.Tip) error {
	if got.Point.Slot != want.Slot {
		return fmt.Errorf(
			"tip slot mismatch: got %d, want %d",
			got.Point.Slot, want.Slot,
		)
	}
	if !bytes.Equal(got.Point.Hash, want.Hash) {
		return fmt.Errorf(
			"tip hash mismatch: got %x, want %x",
			got.Point.Hash, []byte(want.Hash),
		)
	}
	if got.BlockNumber != want.BlockNumber {
		return fmt.Errorf(
			"tip block_number mismatch: got %d, want %d",
			got.BlockNumber, want.BlockNumber,
		)
	}
	return nil
}
