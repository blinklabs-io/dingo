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

package consensus_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// TestRunVectorRoutesConsensus confirms dispatch routes a
// consensus-category vector to the consensus driver and the driver
// runs to completion. The ledger-category dispatch path is
// exercised end-to-end by TestLedgerConformanceVectorsNewFormat
// against the committed converted corpus.
func TestRunVectorRoutesConsensus(t *testing.T) {
	v := syntheticConsensusVector(t)
	if err := consensus.RunVector(t, v); err != nil {
		t.Fatalf("RunVector(consensus): %v", err)
	}
}

// TestRunVectorRejectsMismatchedPayload guards against vectors that
// claim category=consensus but omit Capture (or vice versa). The
// format decoder catches this at load time, but RunVector also
// double-checks because callers can synthesize TestVectors directly.
func TestRunVectorRejectsMismatchedPayload(t *testing.T) {
	v := format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "broken",
		Category:      format.CategoryConsensus,
		Capture:       nil,
	}
	if err := consensus.RunVector(t, v); err == nil {
		t.Fatal("expected error for consensus vector with no capture")
	}
}

// TestConsensusRunnerDetectsTipMismatch tampers with
// expected_output.final_tip and confirms the runner reports a clear
// tip-mismatch error rather than passing silently.
func TestConsensusRunnerDetectsTipMismatch(t *testing.T) {
	v := syntheticConsensusVector(t)
	// Bump the expected tip's slot by 1 so the runner's selected
	// peer's tip (which equals one of the served traces' tips) no
	// longer matches.
	v.Capture.ExpectedOutput.FinalTip.Slot++
	err := consensus.RunVector(t, v)
	if err == nil {
		t.Fatal("expected tip-slot mismatch")
	}
	if !strings.Contains(err.Error(), "tip slot mismatch") {
		t.Fatalf(
			"expected 'tip slot mismatch' in error, got: %v", err,
		)
	}
}

func TestLoadVectorMissingFile(t *testing.T) {
	_, err := consensus.LoadVector(
		filepath.Join(t.TempDir(), "does-not-exist.json"),
	)
	if err == nil {
		t.Fatal("expected error for missing vector file")
	}
}

// syntheticConsensusVector builds a minimal two-peer consensus
// vector: peer A has one roll_forward, peer B has two, peer B's tip
// is the expected output. The runner's chain-selection logic should
// route to peer B and match the expected tip.
func syntheticConsensusVector(t *testing.T) format.TestVector {
	t.Helper()
	era := uint(6)
	hashA := mustHexBytes(t, "aa11")
	hashB := mustHexBytes(t, "bb22")
	hashBTip := mustHexBytes(t, "bb33")

	peerA := format.PeerInput{
		PeerID: 0,
		Served: []format.ServedMessage{
			{
				Protocol:   format.ProtocolChainSync,
				MsgType:    format.ChainSyncMsgRollForward,
				Era:        &era,
				HeaderCbor: format.HexBytes{0x01},
				Tip: &format.Tip{
					Slot: 10, Hash: hashA, BlockNumber: 1,
				},
			},
		},
	}
	peerB := format.PeerInput{
		PeerID: 1,
		Served: []format.ServedMessage{
			{
				Protocol:   format.ProtocolChainSync,
				MsgType:    format.ChainSyncMsgRollForward,
				Era:        &era,
				HeaderCbor: format.HexBytes{0x02},
				Tip: &format.Tip{
					Slot: 20, Hash: hashB, BlockNumber: 1,
				},
			},
			{
				Protocol:   format.ProtocolChainSync,
				MsgType:    format.ChainSyncMsgRollForward,
				Era:        &era,
				HeaderCbor: format.HexBytes{0x03},
				Tip: &format.Tip{
					Slot: 30, Hash: hashBTip, BlockNumber: 2,
				},
			},
		},
	}
	return format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "synthetic-two-peer",
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: []format.PeerInput{peerA, peerB},
			ExpectedOutput: format.ExpectedOutput{
				DownstreamChainSync: peerB.Served,
				FinalTip: format.Tip{
					Slot: 30, Hash: hashBTip, BlockNumber: 2,
				},
			},
		},
	}
}

func mustHexBytes(t *testing.T, s string) format.HexBytes {
	t.Helper()
	var hb format.HexBytes
	if err := hb.UnmarshalJSON([]byte(`"` + s + `"`)); err != nil {
		t.Fatalf("hex %q: %v", s, err)
	}
	return hb
}
