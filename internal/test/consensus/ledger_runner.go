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
	"sync"
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
//     new-format LedgerPhase (mechanical inverse of W4's converter).
//  2. Re-encodes it to CBOR and writes to a temp file the harness
//     can read.
//  3. Resolves the ouroboros-mock corpus root so the harness's
//     pparams-by-hash lookups still work.
//  4. Wires DingoStateManager into a fresh Harness and runs the
//     vector. Harness assertions fail via t.Errorf — the failure
//     propagates back through the testing.T the caller passed in.
//
// Returns nil on success (or harness-internal failure marked via
// t.Errorf — which still fails the test); returns a non-nil error
// only for setup-level problems (corpus root not resolvable,
// temp-file write failure, etc.).
func runLedgerVector(
	t *testing.T,
	title string,
	phase *format.LedgerPhase,
) error {
	t.Helper()
	if phase == nil {
		return errors.New("runLedgerVector: nil LedgerPhase")
	}

	testdataRoot, err := mockTestdataRoot(t)
	if err != nil {
		return fmt.Errorf("extract ouroboros-mock testdata: %w", err)
	}

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

// mockTestdataRoot extracts ouroboros-mock's conformance testdata
// once per test binary invocation and returns the on-disk root. The
// harness needs this for its pparams-by-hash lookups.
//
// One-time extraction is fine: each LedgerPhase converted vector
// references the same set of shared pparams files, so paying for
// the extraction once amortizes across all ledger subtests.
func mockTestdataRoot(t *testing.T) (string, error) {
	t.Helper()
	mockTestdataOnce.Do(func() {
		// Use a process-lifetime temp dir (not t.TempDir) so the
		// extracted corpus survives across subtests. Cleanup
		// happens when the process exits — fine for the ~6MB
		// extracted corpus.
		dir, err := os.MkdirTemp("", "ouroboros-mock-testdata-*")
		if err != nil {
			mockTestdataErr = fmt.Errorf("mkdtemp: %w", err)
			return
		}
		root, err := mockconf.ExtractEmbeddedTestdata(dir)
		if err != nil {
			_ = os.RemoveAll(dir)
			mockTestdataErr = fmt.Errorf(
				"ExtractEmbeddedTestdata: %w", err,
			)
			return
		}
		mockTestdataPath = root
	})
	return mockTestdataPath, mockTestdataErr
}

var (
	mockTestdataOnce sync.Once
	mockTestdataPath string
	mockTestdataErr  error
)
