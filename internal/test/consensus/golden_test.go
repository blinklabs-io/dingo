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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// TestCapturedGoldensDecode validates each committed vector under
// testdata/captured/ for structural invariants: clean decode, the
// declared peer count, at least one downstream roll_forward, and a
// non-zero final_tip. Catches the case where someone hand-edits a
// committed vector in a way that breaks the format contract.
//
// Runs in plain `go test` — no docker dependency. The assertions
// are static reads of committed JSON.
func TestCapturedGoldensDecode(t *testing.T) {
	dir := filepath.Join("testdata", "captured")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("no testdata/captured directory yet")
		}
		t.Fatalf("read captured: %v", err)
	}
	any := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		any = true
		t.Run(e.Name(), func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			v, err := format.DecodeTestVector(raw)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if v.Category != format.CategoryConsensus {
				t.Fatalf("category = %q, want consensus",
					v.Category,
				)
			}
			if v.Capture == nil || len(v.Capture.Peers) == 0 {
				t.Fatal("capture is missing or has zero peers")
			}
			for i, p := range v.Capture.Peers {
				if len(p.Served) == 0 {
					t.Fatalf("peers[%d].served is empty", i)
				}
			}
			rf := 0
			for _, m := range v.Capture.ExpectedOutput.DownstreamChainSync {
				if m.MsgType == format.ChainSyncMsgRollForward {
					rf++
				}
			}
			if rf == 0 {
				t.Fatal(
					"expected_output.downstream_chainsync has " +
						"no roll_forward",
				)
			}
			tip := v.Capture.ExpectedOutput.FinalTip
			if tip.Slot == 0 || len(tip.Hash) == 0 {
				t.Fatalf("final_tip looks empty: %+v", tip)
			}
		})
	}
	if !any {
		t.Skip("testdata/captured is empty")
	}
}

// TestForkAndSelectV1SharedPrefix asserts the load-bearing structural
// invariant of the fork_and_select_v1 scenario: peer A and peer B
// agree on the first N roll_forwards (the shared prefix forged in
// phase A) before diverging. Without that prefix, the observation
// node's rollback would target genesis itself — defeating the
// scenario's "rollback to a non-genesis intersect" purpose.
func TestForkAndSelectV1SharedPrefix(t *testing.T) {
	path := filepath.Join("testdata", "captured", "fork_and_select_v1.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("fork_and_select_v1.json not regenerated yet")
		}
		t.Fatalf("read: %v", err)
	}
	v, err := format.DecodeTestVector(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v.Capture == nil || len(v.Capture.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d",
			len(v.Capture.Peers),
		)
	}
	a := rollForwardHeaders(v.Capture.Peers[0].Served)
	b := rollForwardHeaders(v.Capture.Peers[1].Served)
	shared := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		if !bytesEqual(a[i], b[i]) {
			break
		}
		shared++
	}
	if shared == 0 {
		t.Fatalf(
			"peer A and peer B share zero roll_forwards — no " +
				"shared prefix; observation rollback target " +
				"would be genesis",
		)
	}
	if shared == len(a) && shared == len(b) {
		t.Fatalf(
			"peer A and peer B chains are identical — no " +
				"divergence",
		)
	}
	if len(b) <= len(a) {
		t.Fatalf(
			"peer B chain (%d) not strictly longer than peer A "+
				"chain (%d); observation would select A, "+
				"contradicting the scenario design",
			len(b), len(a),
		)
	}
}

func rollForwardHeaders(served []format.ServedMessage) [][]byte {
	out := make([][]byte, 0, len(served))
	for _, m := range served {
		if m.MsgType == format.ChainSyncMsgRollForward {
			out = append(out, m.HeaderCbor)
		}
	}
	return out
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
