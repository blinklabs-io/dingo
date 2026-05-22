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

package main

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

func TestComposeMergesPeersAndObservation(t *testing.T) {
	dir := t.TempDir()
	peerA := writeSinglePeerCapture(t, dir, "peer-a", []format.ServedMessage{
		rollBackwardToOrigin(t),
		rollForward(t, 6, "aa", 10, "11aa"),
	})
	peerB := writeSinglePeerCapture(t, dir, "peer-b", []format.ServedMessage{
		rollBackwardToOrigin(t),
		rollForward(t, 6, "bb", 20, "22bb"),
		rollForward(t, 6, "bbcc", 30, "33bb"),
	})
	observation := writeSinglePeerCapture(t, dir, "obs",
		[]format.ServedMessage{
			rollForward(t, 6, "bb", 20, "22bb"),
			rollForward(t, 6, "bbcc", 30, "33bb"),
		},
	)

	vec, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peerA, peerB},
		ObservationCapturePath: observation,
		Title:                  "fork_and_select_v1",
	})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if vec.Title != "fork_and_select_v1" {
		t.Fatalf("title = %q", vec.Title)
	}
	if vec.Capture == nil || len(vec.Capture.Peers) != 2 {
		t.Fatalf("peers = %+v", vec.Capture)
	}
	if vec.Capture.Peers[0].PeerID != 0 ||
		vec.Capture.Peers[1].PeerID != 1 {
		t.Fatalf("peer ids not assigned in order: %+v",
			vec.Capture.Peers,
		)
	}
	if len(vec.Capture.Peers[0].Served) != 2 ||
		len(vec.Capture.Peers[1].Served) != 3 {
		t.Fatalf("served counts: a=%d b=%d",
			len(vec.Capture.Peers[0].Served),
			len(vec.Capture.Peers[1].Served),
		)
	}
	if len(vec.Capture.ExpectedOutput.DownstreamChainSync) != 2 {
		t.Fatalf("downstream length = %d, want 2",
			len(vec.Capture.ExpectedOutput.DownstreamChainSync),
		)
	}
	if vec.Capture.ExpectedOutput.FinalTip.Slot != 30 {
		t.Fatalf("final_tip.slot = %d, want 30 (peer B's tip)",
			vec.Capture.ExpectedOutput.FinalTip.Slot,
		)
	}
}

func TestComposeRejectsLedgerCategoryInput(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "ledger.json")
	if err := consensus.WriteVector(bad, format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "wrong-category",
		Category:      format.CategoryLedger,
		LedgerPhase:   &format.LedgerPhase{},
	}); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}
	if _, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{bad},
		ObservationCapturePath: bad,
	}); err == nil {
		t.Fatal("expected error for ledger-category input")
	}
}

func TestComposeRejectsMultiPeerInput(t *testing.T) {
	dir := t.TempDir()
	multi := filepath.Join(dir, "multi.json")
	if err := consensus.WriteVector(multi, format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "already-merged",
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: []format.PeerInput{
				{PeerID: 0, Served: []format.ServedMessage{
					rollBackwardToOrigin(t),
				}},
				{PeerID: 1, Served: []format.ServedMessage{
					rollBackwardToOrigin(t),
				}},
			},
		},
	}); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}
	if _, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{multi},
		ObservationCapturePath: multi,
	}); err == nil {
		t.Fatal("expected error for multi-peer input")
	}
}

func TestDiffAgainstGoldenStructuralMatch(t *testing.T) {
	dir := t.TempDir()
	peer := writeSinglePeerCapture(t, dir, "peer", []format.ServedMessage{
		rollBackwardToOrigin(t),
		rollForward(t, 6, "aa", 10, "1111"),
	})
	obs := writeSinglePeerCapture(t, dir, "obs", []format.ServedMessage{
		rollForward(t, 6, "aa", 10, "1111"),
	})
	golden, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peer},
		ObservationCapturePath: obs,
		Title:                  "golden",
	})
	if err != nil {
		t.Fatalf("Compose golden: %v", err)
	}
	goldenPath := filepath.Join(dir, "golden.json")
	if err := consensus.WriteVector(goldenPath, golden); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}

	// Fresh capture with different header bytes + tip hash, but same
	// structural shape and same tip slot. Should match.
	peerFresh := writeSinglePeerCapture(t, dir, "peer-fresh",
		[]format.ServedMessage{
			rollBackwardToOrigin(t),
			rollForward(t, 6, "bb", 10, "2222"),
		},
	)
	obsFresh := writeSinglePeerCapture(t, dir, "obs-fresh",
		[]format.ServedMessage{
			rollForward(t, 6, "bb", 10, "2222"),
		},
	)
	fresh, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peerFresh},
		ObservationCapturePath: obsFresh,
		Title:                  "fresh",
	})
	if err != nil {
		t.Fatalf("Compose fresh: %v", err)
	}
	res, err := consensus.DiffAgainstGolden(goldenPath, fresh)
	if err != nil {
		t.Fatalf("DiffAgainstGolden: %v", err)
	}
	if !res.Match {
		t.Fatalf("expected structural match, got differences: %v",
			res.Differences,
		)
	}
}

func TestDiffAgainstGoldenToleratesLengthDifference(t *testing.T) {
	// Per CONSENSUS_W2.md §5 "Golden tolerance", forge nondeterminism
	// means per-run captures can have different message counts. The
	// diff should treat that as a match as long as the structural
	// skeleton holds (peer count, presence of roll_forwards, tip
	// slot within tolerance).
	dir := t.TempDir()
	peer := writeSinglePeerCapture(t, dir, "peer",
		[]format.ServedMessage{
			rollBackwardToOrigin(t),
			rollForward(t, 6, "aa", 10, "1111"),
		},
	)
	obs := writeSinglePeerCapture(t, dir, "obs",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 10, "1111"),
		},
	)
	golden, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peer},
		ObservationCapturePath: obs,
	})
	if err != nil {
		t.Fatalf("Compose golden: %v", err)
	}
	goldenPath := filepath.Join(dir, "golden.json")
	if err := consensus.WriteVector(goldenPath, golden); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}

	// Fresh has an extra roll_forward in peer 0 — should still match
	// because the structural skeleton is intact and the tip stays
	// inside FinalTipSlotTolerance.
	peerFresh := writeSinglePeerCapture(t, dir, "peer-fresh",
		[]format.ServedMessage{
			rollBackwardToOrigin(t),
			rollForward(t, 6, "aa", 10, "1111"),
			rollForward(t, 6, "cc", 11, "1112"),
		},
	)
	obsFresh := writeSinglePeerCapture(t, dir, "obs-fresh",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 11, "1112"),
		},
	)
	fresh, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peerFresh},
		ObservationCapturePath: obsFresh,
	})
	if err != nil {
		t.Fatalf("Compose fresh: %v", err)
	}
	res, err := consensus.DiffAgainstGolden(goldenPath, fresh)
	if err != nil {
		t.Fatalf("DiffAgainstGolden: %v", err)
	}
	if !res.Match {
		t.Fatalf(
			"expected match despite length difference; got: %v",
			res.Differences,
		)
	}
}

func TestDiffAgainstGoldenFlagsPeerCountMismatch(t *testing.T) {
	dir := t.TempDir()
	peerA := writeSinglePeerCapture(t, dir, "a",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 10, "1111"),
		},
	)
	peerB := writeSinglePeerCapture(t, dir, "b",
		[]format.ServedMessage{
			rollForward(t, 6, "bb", 20, "2222"),
		},
	)
	obs := writeSinglePeerCapture(t, dir, "obs",
		[]format.ServedMessage{
			rollForward(t, 6, "bb", 20, "2222"),
		},
	)
	golden, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peerA, peerB},
		ObservationCapturePath: obs,
	})
	if err != nil {
		t.Fatalf("Compose golden: %v", err)
	}
	goldenPath := filepath.Join(dir, "golden.json")
	if err := consensus.WriteVector(goldenPath, golden); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}

	// Fresh dropped a peer — should flag.
	fresh, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peerA},
		ObservationCapturePath: obs,
	})
	if err != nil {
		t.Fatalf("Compose fresh: %v", err)
	}
	res, err := consensus.DiffAgainstGolden(goldenPath, fresh)
	if err != nil {
		t.Fatalf("DiffAgainstGolden: %v", err)
	}
	if res.Match {
		t.Fatal("expected mismatch; fresh has only 1 peer vs golden's 2")
	}
}

func TestDiffAgainstGoldenFlagsFinalTipSlotMismatch(t *testing.T) {
	dir := t.TempDir()
	peer := writeSinglePeerCapture(t, dir, "peer",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 10, "1111"),
		},
	)
	obs := writeSinglePeerCapture(t, dir, "obs",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 10, "1111"),
		},
	)
	golden, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peer},
		ObservationCapturePath: obs,
	})
	if err != nil {
		t.Fatalf("Compose golden: %v", err)
	}
	goldenPath := filepath.Join(dir, "golden.json")
	if err := consensus.WriteVector(goldenPath, golden); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}

	// Fresh observation tip is at a different slot — should flag.
	peerFresh := writeSinglePeerCapture(t, dir, "peer-fresh",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 99, "1111"),
		},
	)
	obsFresh := writeSinglePeerCapture(t, dir, "obs-fresh",
		[]format.ServedMessage{
			rollForward(t, 6, "aa", 99, "1111"),
		},
	)
	fresh, err := consensus.Compose(consensus.ComposeArgs{
		PeerCapturePaths:       []string{peerFresh},
		ObservationCapturePath: obsFresh,
	})
	if err != nil {
		t.Fatalf("Compose fresh: %v", err)
	}
	res, err := consensus.DiffAgainstGolden(goldenPath, fresh)
	if err != nil {
		t.Fatalf("DiffAgainstGolden: %v", err)
	}
	if res.Match {
		t.Fatal("expected mismatch; fresh has a different final_tip slot")
	}
}

// --- helpers ----------------------------------------------------------

func writeSinglePeerCapture(
	t *testing.T,
	dir, name string,
	served []format.ServedMessage,
) string {
	t.Helper()
	v := format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         name,
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: []format.PeerInput{
				{PeerID: 0, Served: served},
			},
			ExpectedOutput: format.ExpectedOutput{
				DownstreamChainSync: served,
				FinalTip:            extractTip(served),
			},
		},
	}
	path := filepath.Join(dir, name+".json")
	if err := consensus.WriteVector(path, v); err != nil {
		t.Fatalf("WriteVector %s: %v", path, err)
	}
	return path
}

func extractTip(served []format.ServedMessage) format.Tip {
	for _, m := range slices.Backward(served) {
		if m.Tip != nil &&
			m.MsgType == format.ChainSyncMsgRollForward {
			return format.Tip{
				Slot: m.Tip.Slot,
				Hash: append(format.HexBytes(nil), m.Tip.Hash...),
			}
		}
	}
	return format.Tip{}
}

func rollBackwardToOrigin(t *testing.T) format.ServedMessage {
	t.Helper()
	return format.ServedMessage{
		Protocol: format.ProtocolChainSync,
		MsgType:  format.ChainSyncMsgRollBackward,
		Point: &format.Point{
			Slot: 0,
			Hash: format.HexBytes{},
		},
		Tip: &format.Tip{
			Slot: 0,
			Hash: format.HexBytes{},
		},
	}
}

func rollForward(
	t *testing.T,
	era uint,
	headerHex string,
	tipSlot uint64,
	tipHashHex string,
) format.ServedMessage {
	t.Helper()
	e := era
	return format.ServedMessage{
		Protocol:   format.ProtocolChainSync,
		MsgType:    format.ChainSyncMsgRollForward,
		Era:        &e,
		HeaderCbor: mustHexBytes(t, headerHex),
		Tip: &format.Tip{
			Slot: tipSlot,
			Hash: mustHexBytes(t, tipHashHex),
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
