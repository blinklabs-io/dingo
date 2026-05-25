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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dingoconf "github.com/blinklabs-io/dingo/internal/test/conformance"
	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
	"github.com/blinklabs-io/gouroboros/cbor"
	mockconf "github.com/blinklabs-io/ouroboros-mock/conformance"
)

// runLedgerVector replays a ledger-category vector against dingo's
// DingoStateManager via ouroboros-mock's existing conformance
// Harness.
//
// The harness expects vectors in the Amaru CBOR shape and reads
// protocol-parameter blobs from its TestdataRoot. The driver:
//
//  1. Reconstructs an Amaru-shape *mockconf.TestVector from the
//     new-format LedgerPhase (mechanical inverse of the
//     cmd/convert-amaru-vector tool's rewrap).
//  2. Re-encodes it to CBOR and writes to a temp file the harness
//     can read.
//  3. Resolves the ouroboros-mock corpus root so the harness's
//     pparams-by-hash lookups still work.
//  4. Wires DingoStateManager into a fresh Harness and runs the
//     vector. Harness assertions fail via t.Errorf — the failure
//     propagates back through the testing.T the caller passed in.
//
// Return-value contract — asymmetric with runConsensusVector, which
// returns its assertion failures as errors:
//
//   - **Setup-level failures** (corpus root not resolvable, temp-file
//     write failure, etc.) return a non-nil error. The caller in
//     consensus_test.go converts these into t.Fatalf.
//   - **Assertion failures inside the harness** do NOT come back as
//     an error. The harness reports them via t.Errorf on the *same*
//     testing.T the caller passed in, which marks the subtest as
//     failed and surfaces the per-event diagnostic the harness
//     emitted. The return value is nil in this case.
//
// So a nil return doesn't mean "passed" — it means "the harness
// reached the end of its event loop." Pass/fail is carried by the
// testing.T. The asymmetry exists because the harness API is
// testing.T-driven and rewiring it through an error channel would
// duplicate state. The consensus driver is the odd one out; here
// (and any future driver that wraps the harness) the testing.T
// is the authoritative outcome carrier.
func runLedgerVector(
	t *testing.T,
	title string,
	phase *format.LedgerPhase,
) error {
	t.Helper()
	if phase == nil {
		return errors.New("runLedgerVector: nil LedgerPhase")
	}

	if mockTestdataPath == "" {
		return errors.New(
			"runLedgerVector: PrimeMockTestdata must be called from " +
				"the parent test before any ledger subtest runs " +
				"(extracts the ouroboros-mock corpus the harness " +
				"needs for pparams-by-hash lookups)",
		)
	}
	testdataRoot := mockTestdataPath

	tmpPath, err := writeAmaruShapeVector(t, title, phase)
	if err != nil {
		return fmt.Errorf("re-encode to Amaru shape: %w", err)
	}

	sm, err := dingoconf.NewDingoStateManager()
	if err != nil {
		return fmt.Errorf("NewDingoStateManager: %w", err)
	}
	defer func() { _ = sm.Close() }()

	h := mockconf.NewHarness(sm, mockconf.HarnessConfig{
		TestdataRoot: testdataRoot,
	})
	h.RunVector(t, tmpPath)
	return nil
}

// writeAmaruShapeVector rebuilds the 5-element Amaru CBOR array
// from the new-format LedgerPhase and writes it to a t.TempDir() file
// so the harness can read it back via DecodeTestVector. This is the
// inverse of cmd/convert-amaru-vector's structural rewrap.
//
// The three opaque blobs (config, initial_state, final_state) come
// back as cbor.RawMessage so their bytes splice into the outer array
// verbatim — they were captured as raw CBOR by the converter and
// must remain raw CBOR for the harness's downstream parsers.
//
// Each event becomes its positional tuple again. Transaction's
// TxCbor stays a byte string (matches Amaru's [0, tx_cbor:bytes,
// success:bool, slot:uint64] shape). Other event types are
// fixed-arity arrays.
func writeAmaruShapeVector(
	t *testing.T,
	title string,
	phase *format.LedgerPhase,
) (string, error) {
	t.Helper()
	events := make([]any, 0, len(phase.Events))
	for i, e := range phase.Events {
		tup, err := amaruEventTuple(e)
		if err != nil {
			return "", fmt.Errorf("event[%d]: %w", i, err)
		}
		events = append(events, tup)
	}
	wrapper := []any{
		cbor.RawMessage(phase.Config),
		cbor.RawMessage(phase.InitialState),
		cbor.RawMessage(phase.FinalState),
		events,
		title,
	}
	raw, err := cbor.Encode(wrapper)
	if err != nil {
		return "", fmt.Errorf("cbor.Encode: %w", err)
	}
	dir := t.TempDir()
	// Embed the title in the filename so a harness failure surfaces
	// the originating vector in the t.TempDir path. Sanitize because
	// titles can contain path-hostile characters.
	name := sanitizeTitle(title) + ".amaru.cbor"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// amaruEventTuple converts one new-format LedgerEvent back into its
// Amaru positional CBOR tuple.
func amaruEventTuple(e format.LedgerEvent) ([]any, error) {
	switch e.Type {
	case format.LedgerEventTransaction:
		if e.Success == nil || e.Slot == nil {
			return nil, errors.New(
				"transaction event missing success/slot",
			)
		}
		return []any{
			uint64(0), []byte(e.TxCbor), *e.Success, *e.Slot,
		}, nil
	case format.LedgerEventPassTick:
		if e.Slot == nil {
			return nil, errors.New("pass_tick event missing slot")
		}
		return []any{uint64(1), *e.Slot}, nil
	case format.LedgerEventPassEpoch:
		if e.Epoch == nil {
			return nil, errors.New("pass_epoch event missing epoch")
		}
		return []any{uint64(2), *e.Epoch}, nil
	case format.LedgerEventRollback:
		if e.TargetSlot == nil {
			return nil, errors.New(
				"rollback event missing target_slot",
			)
		}
		return []any{uint64(3), *e.TargetSlot}, nil
	}
	return nil, fmt.Errorf("unknown event type %q", e.Type)
}

// sanitizeTitle replaces characters that would break a filesystem
// path. Used only for temp-file readability — the harness uses the
// title from the CBOR title field for its own assertion messages.
func sanitizeTitle(title string) string {
	if title == "" {
		return "vector"
	}
	r := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "?", "_",
		"*", "_", "\"", "_", "<", "_", ">", "_",
		"|", "_", " ", "_",
	)
	return r.Replace(title)
}

// PrimeMockTestdata extracts ouroboros-mock's conformance testdata
// into a t.TempDir scoped to the supplied testing.T and caches the
// path so subsequent runLedgerVector calls can find it. Call from
// the parent test before iterating ledger subtests; the extracted
// dir is cleaned up by Go's testing framework when t (the parent)
// completes, which is after all its subtests have completed.
//
// Idempotent: each call re-extracts and overwrites the cached path.
// That's deliberate — `go test -count=N` reruns the parent test
// multiple times; a sync.Once-gated cache would leave the cached
// path pointing at an already-cleaned-up TempDir on the second run.
// Re-extraction takes ~1s and only happens once per parent-test
// invocation, so the cost is negligible.
//
// Concurrency: subtests today don't call t.Parallel, so the read of
// mockTestdataPath in runLedgerVector is serialised after the
// parent's write here. If parallel subtests are added later, swap
// for an atomic.Pointer.
func PrimeMockTestdata(t *testing.T) error {
	t.Helper()
	dir := t.TempDir()
	root, err := mockconf.ExtractEmbeddedTestdata(dir)
	if err != nil {
		return fmt.Errorf("ExtractEmbeddedTestdata: %w", err)
	}
	mockTestdataPath = root
	return nil
}

// mockTestdataPath holds the extracted ouroboros-mock testdata root.
// Set by PrimeMockTestdata; read by runLedgerVector. Zero value
// triggers a clear error in runLedgerVector if the parent test
// forgot to prime.
var mockTestdataPath string
