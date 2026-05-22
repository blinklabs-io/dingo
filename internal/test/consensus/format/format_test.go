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

package format_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// Fixture paths are resolved relative to this test file via package-
// scoped helpers below so the tests run from any working directory go
// test happens to use.
var fixturesDir = filepath.Join("..", "testdata", "fixtures")

func TestRoundTripFixtures(t *testing.T) {
	entries, err := os.ReadDir(fixturesDir)
	if err != nil {
		t.Fatalf("read fixtures dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		t.Fatal("no fixture files found under testdata/fixtures")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(fixturesDir, name)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			v, err := format.DecodeTestVector(raw)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			out, err := format.EncodeTestVector(v)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			// Re-decode the encoder output and require it to match
			// the decoded original. We compare via Decode→Decode
			// rather than byte-for-byte because the on-disk
			// fixture may have whitespace or field-order differences
			// from the encoder's canonical pretty-printing.
			v2, err := format.DecodeTestVector(out)
			if err != nil {
				t.Fatalf("decode round-trip output: %v", err)
			}
			if !reflect.DeepEqual(v, v2) {
				t.Fatalf("round-trip mismatch:\n want: %+v\n got:  %+v",
					v, v2,
				)
			}
		})
	}
}

func TestEncodeIsStableUnderReEncode(t *testing.T) {
	// Encoder output must be a fixed point: re-encoding what we just
	// encoded produces identical bytes. This guards against any
	// silent reformatting drift between releases.
	entries, err := os.ReadDir(fixturesDir)
	if err != nil {
		t.Fatalf("read fixtures dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		t.Run(e.Name(), func(t *testing.T) {
			raw, err := os.ReadFile(
				filepath.Join(fixturesDir, e.Name()),
			)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			v, err := format.DecodeTestVector(raw)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			first, err := format.EncodeTestVector(v)
			if err != nil {
				t.Fatalf("encode first: %v", err)
			}
			v2, err := format.DecodeTestVector(first)
			if err != nil {
				t.Fatalf("decode first: %v", err)
			}
			second, err := format.EncodeTestVector(v2)
			if err != nil {
				t.Fatalf("encode second: %v", err)
			}
			if string(first) != string(second) {
				t.Fatalf("encoder output not stable under re-encode")
			}
		})
	}
}

func TestDecodeRejectsUnknownSchemaVersion(t *testing.T) {
	raw := []byte(
		`{"schema_version": 999, "title": "x", "category": "consensus",
		"capture": {"peers": [], "expected_output":
		{"downstream_chainsync": [], "final_tip": {"slot": 0, "hash": ""}}}}`,
	)
	if _, err := format.DecodeTestVector(raw); err == nil {
		t.Fatal("expected error for unknown schema version")
	}
}

func TestDecodeRejectsUnknownCategory(t *testing.T) {
	raw := []byte(
		`{"schema_version": 1, "title": "x", "category": "bogus"}`,
	)
	if _, err := format.DecodeTestVector(raw); err == nil {
		t.Fatal("expected error for unknown category")
	}
}

func TestDecodeRejectsBothPayloadsSet(t *testing.T) {
	// category=consensus with ledger_phase set should fail validation.
	raw := []byte(`{
		"schema_version": 1, "title": "x", "category": "consensus",
		"capture": {"peers": [], "expected_output":
		{"downstream_chainsync": [], "final_tip": {"slot": 0, "hash": ""}}},
		"ledger_phase": {"config": "", "initial_state": "",
		"final_state": "", "events": []}
	}`)
	if _, err := format.DecodeTestVector(raw); err == nil {
		t.Fatal("expected error when both capture and ledger_phase set")
	}
}

func TestDecodeRejectsUnknownMsgType(t *testing.T) {
	raw := []byte(`{
		"schema_version": 1, "title": "x", "category": "consensus",
		"capture": {
			"peers": [{"peer_id": 0, "served": [
				{"protocol": "chainsync", "msg_type": "nope",
				 "msg_bytes": ""}
			]}],
			"expected_output": {"downstream_chainsync": [],
			"final_tip": {"slot": 0, "hash": ""}}
		}
	}`)
	if _, err := format.DecodeTestVector(raw); err == nil {
		t.Fatal("expected error for unknown chainsync msg_type")
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	raw := []byte(`{
		"schema_version": 1, "title": "x", "category": "consensus",
		"capture": {"peers": [], "expected_output":
		{"downstream_chainsync": [], "final_tip": {"slot": 0, "hash": ""}}},
		"extra_field": 42
	}`)
	if _, err := format.DecodeTestVector(raw); err == nil {
		t.Fatal("expected error for unknown top-level field")
	}
}

func TestEncodeProducesPrettyPrintedJSON(t *testing.T) {
	v := format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "pretty",
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: []format.PeerInput{},
			ExpectedOutput: format.ExpectedOutput{
				DownstreamChainSync: []format.ServedMessage{},
				FinalTip: format.Tip{
					Slot: 0,
					Hash: format.HexBytes{},
				},
			},
		},
	}
	out, err := format.EncodeTestVector(v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "\n  ") {
		t.Fatalf("expected 2-space indentation, got:\n%s", s)
	}
	if !strings.HasSuffix(s, "\n") {
		t.Fatal("expected trailing newline")
	}
}

func TestHexBytesEmptyRoundTrip(t *testing.T) {
	v := format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "empty-hex",
		Category:      format.CategoryLedger,
		LedgerPhase: &format.LedgerPhase{
			Config:       nil,
			InitialState: nil,
			FinalState:   nil,
			Events:       []format.LedgerEvent{},
		},
	}
	out, err := format.EncodeTestVector(v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	v2, err := format.DecodeTestVector(out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v2.LedgerPhase == nil {
		t.Fatal("ledger_phase missing after round-trip")
	}
	if v2.LedgerPhase.Config != nil {
		t.Fatalf("empty hex should decode to nil, got %v",
			v2.LedgerPhase.Config,
		)
	}
}
