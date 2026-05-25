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
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

// TestLosslessConversion exercises the full corpus (314 vectors at
// time of writing). Runs in ~1s; the cost is well below the budget
// where sampling would matter, and full coverage means a
// per-vector mapping regression can't hide in the unsampled tail.
func TestLosslessConversion(t *testing.T) {
	corpusRoot := resolveCorpusRootForTest(t)
	vectors, err := conformance.CollectVectorFiles(corpusRoot)
	if err != nil {
		t.Fatalf("collect vectors from %s: %v", corpusRoot, err)
	}
	if len(vectors) == 0 {
		t.Fatalf("no vectors under %s", corpusRoot)
	}

	for _, srcPath := range vectors {
		t.Run(filepath.Base(srcPath), func(t *testing.T) {
			src, err := conformance.DecodeTestVector(srcPath)
			if err != nil {
				t.Fatalf("decode source: %v", err)
			}
			dst, err := convertVector(src)
			if err != nil {
				t.Fatalf("convert: %v", err)
			}

			assertEnvelopeLossless(t, src, dst)
			assertEventsLossless(t, src, dst)
			assertRoundTrip(t, dst)
		})
	}
}

// assertEnvelopeLossless checks the four opaque blobs (Config,
// InitialState, FinalState, and per-Transaction TxBytes) survive the
// rewrap byte-for-byte. These are the load-bearing artifacts the
// replay-side ledger driver decodes; any drift here would silently
// produce a different chain state.
func assertEnvelopeLossless(
	t *testing.T,
	src *conformance.TestVector,
	dst format.TestVector,
) {
	t.Helper()
	if dst.Category != format.CategoryLedger {
		t.Fatalf("category = %q, want ledger", dst.Category)
	}
	if dst.LedgerPhase == nil {
		t.Fatal("LedgerPhase nil")
	}
	if dst.Title != src.Title {
		t.Fatalf("title: got %q, want %q", dst.Title, src.Title)
	}
	if !bytes.Equal(dst.LedgerPhase.Config, src.Config) {
		t.Fatalf("config diverged (lengths: src=%d dst=%d)",
			len(src.Config), len(dst.LedgerPhase.Config),
		)
	}
	if !bytes.Equal(dst.LedgerPhase.InitialState, src.InitialState) {
		t.Fatalf("initial_state diverged (lengths: src=%d dst=%d)",
			len(src.InitialState), len(dst.LedgerPhase.InitialState),
		)
	}
	if !bytes.Equal(dst.LedgerPhase.FinalState, src.FinalState) {
		t.Fatalf("final_state diverged (lengths: src=%d dst=%d)",
			len(src.FinalState), len(dst.LedgerPhase.FinalState),
		)
	}
}

// assertEventsLossless checks the event stream after conversion has
// the same length and the same per-event content (event type, numeric
// fields, opaque tx bytes). The new format uses pointer fields for
// "may-be-absent" numeric values; the source's per-type-only fields
// shouldn't leak into the wrong event types after conversion.
func assertEventsLossless(
	t *testing.T,
	src *conformance.TestVector,
	dst format.TestVector,
) {
	t.Helper()
	if len(dst.LedgerPhase.Events) != len(src.Events) {
		t.Fatalf("event count: got %d, want %d",
			len(dst.LedgerPhase.Events), len(src.Events),
		)
	}
	for i, want := range src.Events {
		got := dst.LedgerPhase.Events[i]
		switch want.Type {
		case conformance.EventTypeTransaction:
			if got.Type != format.LedgerEventTransaction {
				t.Fatalf("events[%d] type: %q vs source Transaction",
					i, got.Type,
				)
			}
			if !bytes.Equal(got.TxCbor, want.TxBytes) {
				t.Fatalf("events[%d] tx_cbor diverged", i)
			}
			if got.Success == nil || *got.Success != want.Success {
				t.Fatalf("events[%d] success: got %v, want %v",
					i, got.Success, want.Success,
				)
			}
			if got.Slot == nil || *got.Slot != want.Slot {
				t.Fatalf("events[%d] slot: got %v, want %d",
					i, got.Slot, want.Slot,
				)
			}
		case conformance.EventTypePassTick:
			if got.Type != format.LedgerEventPassTick {
				t.Fatalf("events[%d] type: %q vs source PassTick",
					i, got.Type,
				)
			}
			if got.Slot == nil || *got.Slot != want.TickSlot {
				t.Fatalf("events[%d] slot: got %v, want %d",
					i, got.Slot, want.TickSlot,
				)
			}
		case conformance.EventTypePassEpoch:
			if got.Type != format.LedgerEventPassEpoch {
				t.Fatalf("events[%d] type: %q vs source PassEpoch",
					i, got.Type,
				)
			}
			if got.Epoch == nil || *got.Epoch != want.EpochDelta {
				t.Fatalf("events[%d] epoch: got %v, want %d",
					i, got.Epoch, want.EpochDelta,
				)
			}
		case conformance.EventTypeRollback:
			if got.Type != format.LedgerEventRollback {
				t.Fatalf("events[%d] type: %q vs source Rollback",
					i, got.Type,
				)
			}
			if got.TargetSlot == nil ||
				*got.TargetSlot != want.RollbackSlot {
				t.Fatalf("events[%d] target_slot: got %v, want %d",
					i, got.TargetSlot, want.RollbackSlot,
				)
			}
		default:
			t.Fatalf("events[%d]: source has unrecognized type %d",
				i, want.Type,
			)
		}
	}
}

// assertRoundTrip confirms the converted vector survives an encode →
// decode pass through the new-format codec — catches any field that
// the encoder rejects under the new format's per-event validation
// rules (see format/msg_type.go for the discriminated-union check).
func assertRoundTrip(t *testing.T, dst format.TestVector) {
	t.Helper()
	raw, err := format.EncodeTestVector(dst)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := format.DecodeTestVector(raw); err != nil {
		t.Fatalf("decode round-trip: %v", err)
	}
}

// TestConvertEventRejectsUnknownType guards the "fail loudly on
// unknown event types" decision from the spec — silent drops would
// mask either a corpus drift or a tool bug.
func TestConvertEventRejectsUnknownType(t *testing.T) {
	bogus := conformance.VectorEvent{Type: conformance.EventType(99)}
	if _, err := convertEvent(bogus); err == nil {
		t.Fatal("expected error for unrecognized event type")
	}
}

// resolveCorpusRootForTest finds the Amaru corpus the same way the
// binary does — via `go list -m -json` for the pinned ouroboros-mock
// version — but lifts the helper out of main.go so the test can call
// it directly without exec'ing the binary.
func resolveCorpusRootForTest(t *testing.T) string {
	t.Helper()
	root, err := resolveDefaultInputPath()
	if err != nil {
		t.Skipf("cannot resolve Amaru corpus root: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Skipf("corpus root %s: %v", root, err)
	}
	return root
}
