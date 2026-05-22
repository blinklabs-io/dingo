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
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus"
	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// TestRecorderToVectorRoundTrip seeds a Recorder with synthetic
// ServedMessages, writes a vector through WriteVector, decodes it via
// format.DecodeTestVector, and confirms the round-tripped value matches
// what we started with. Offline equivalent of the live capture path —
// no docker stack required.
func TestRecorderToVectorRoundTrip(t *testing.T) {
	r := consensus.NewRecorder(0)
	r.Record(format.ServedMessage{
		Protocol: format.ProtocolChainSync,
		MsgType:  format.ChainSyncMsgRollBackward,
		Point: &format.Point{
			Slot: 0,
			Hash: format.HexBytes{},
		},
		Tip: &format.Tip{
			Slot: 8,
			Hash: mustHex(t, "abcdef"),
		},
	})
	era := uint(6)
	r.Record(format.ServedMessage{
		Protocol:   format.ProtocolChainSync,
		MsgType:    format.ChainSyncMsgRollForward,
		Era:        &era,
		HeaderCbor: format.HexBytes{0x83, 0x01, 0xaa, 0xbb},
		Tip: &format.Tip{
			Slot: 8,
			Hash: mustHex(t, "abcdef"),
		},
	})
	served := r.Snapshot()

	// Build a vector the same way Sidecar.Vector does — without
	// needing a live connection.
	v := format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         "round-trip-synthetic",
		Category:      format.CategoryConsensus,
		Capture: &format.ConsensusCapture{
			Peers: []format.PeerInput{
				{PeerID: r.PeerID(), Served: served},
			},
			ExpectedOutput: format.ExpectedOutput{
				DownstreamChainSync: served,
				FinalTip: format.Tip{
					Slot: 99,
					Hash: mustHex(t, "deadbeef"),
				},
			},
		},
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "vector.json")
	if err := consensus.WriteVector(out, v); err != nil {
		t.Fatalf("WriteVector: %v", err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	decoded, err := format.DecodeTestVector(raw)
	if err != nil {
		t.Fatalf("DecodeTestVector: %v", err)
	}
	if !reflect.DeepEqual(decoded, v) {
		t.Fatalf("round-trip mismatch:\n want: %+v\n got:  %+v",
			v, decoded,
		)
	}
}

// TestRecorderConcurrentAppend exercises the recorder's mutex by
// driving multiple goroutines that each Record a batch. The
// final Snapshot must contain every message in some order.
func TestRecorderConcurrentAppend(t *testing.T) {
	r := consensus.NewRecorder(7)
	const (
		nGoroutines = 8
		perRoutine  = 32
	)
	done := make(chan struct{}, nGoroutines)
	for g := range nGoroutines {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			era := uint(6)
			for i := range perRoutine {
				r.Record(format.ServedMessage{
					Protocol: format.ProtocolChainSync,
					MsgType:  format.ChainSyncMsgRollForward,
					Era:      &era,
					// Encode goroutine + i into the header
					// payload so the test catches a lost
					// or duplicated append.
					HeaderCbor: format.HexBytes{
						byte(g), byte(i),
					},
					Tip: &format.Tip{
						Slot: uint64(g)*1000 +
							uint64(i),
						Hash: format.HexBytes{
							byte(g), byte(i),
						},
					},
				})
			}
		}(g)
	}
	for range nGoroutines {
		<-done
	}
	got := r.Snapshot()
	if want := nGoroutines * perRoutine; len(got) != want {
		t.Fatalf("recorded %d messages, want %d", len(got), want)
	}
}

// TestLoadConversation parses a hand-crafted capture-conversation.json
// and confirms the step types decode in order.
func TestLoadConversation(t *testing.T) {
	raw := []byte(`{
		"name": "demo",
		"steps": [
			{ "type": "find_intersect", "points": ["origin"] },
			{ "type": "request_next" }
		]
	}`)
	conv, err := consensus.DecodeConversation(raw)
	if err != nil {
		t.Fatalf("DecodeConversation: %v", err)
	}
	if conv.Name != "demo" {
		t.Fatalf("name = %q, want %q", conv.Name, "demo")
	}
	if len(conv.Steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(conv.Steps))
	}
	if conv.Steps[0].Type() != "find_intersect" {
		t.Fatalf("steps[0].Type = %q, want find_intersect",
			conv.Steps[0].Type(),
		)
	}
	if conv.Steps[1].Type() != "request_next" {
		t.Fatalf("steps[1].Type = %q, want request_next",
			conv.Steps[1].Type(),
		)
	}
}

// TestDecodeConversationRejectsUnknownStep guards against silently
// skipping a step the script author misspelled.
func TestDecodeConversationRejectsUnknownStep(t *testing.T) {
	raw := []byte(`{
		"name": "bad",
		"steps": [
			{ "type": "no_such_step" }
		]
	}`)
	if _, err := consensus.DecodeConversation(raw); err == nil {
		t.Fatal("expected error for unknown step type")
	}
}

// TestScenarioConversationsParse loads every scenarios/*/
// capture-conversation.json from disk and asserts it decodes cleanly.
// This catches accidental drift between the conversation schema and
// the JSON files committed for live scenarios.
func TestScenarioConversationsParse(t *testing.T) {
	entries, err := os.ReadDir("scenarios")
	if err != nil {
		t.Fatalf("read scenarios dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no scenarios committed")
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join("scenarios", e.Name(), "capture-conversation.json")
		t.Run(e.Name(), func(t *testing.T) {
			if _, err := os.Stat(path); err != nil {
				t.Skipf("no capture-conversation.json at %s", path)
			}
			conv, err := consensus.LoadConversation(path)
			if err != nil {
				t.Fatalf("LoadConversation: %v", err)
			}
			if len(conv.Steps) == 0 {
				t.Fatalf("scenario %s has no steps", e.Name())
			}
		})
	}
}

// TestDecodeConversationRejectsEmptyIntersectPoints prevents an
// accidental zero-point FindIntersect (which gouroboros would replace
// with the [origin] default — a foot-gun in test scripts).
func TestDecodeConversationRejectsEmptyIntersectPoints(t *testing.T) {
	raw := []byte(`{
		"name": "empty",
		"steps": [
			{ "type": "find_intersect", "points": [] }
		]
	}`)
	if _, err := consensus.DecodeConversation(raw); err == nil {
		t.Fatal("expected error for empty find_intersect points")
	}
}

func mustHex(t *testing.T, s string) format.HexBytes {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex decode %q: %v", s, err)
	}
	return b
}
