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
	"fmt"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// LoadVector reads a JSON test vector from disk and decodes it.
func LoadVector(path string) (format.TestVector, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return format.TestVector{}, fmt.Errorf(
			"read vector %s: %w", path, err,
		)
	}
	v, err := format.DecodeTestVector(raw)
	if err != nil {
		return format.TestVector{}, fmt.Errorf(
			"decode vector %s: %w", path, err,
		)
	}
	return v, nil
}

// RunConsensusVector runs a consensus-category vector through the
// chain-selection driver and returns nil on conformance, or an error
// describing the divergence.
func RunConsensusVector(t *testing.T, v format.TestVector) error {
	t.Helper()
	if v.Category != format.CategoryConsensus {
		return fmt.Errorf(
			"vector %q: expected category %q, got %q",
			v.Title, format.CategoryConsensus, v.Category,
		)
	}
	if v.Capture == nil {
		return fmt.Errorf(
			"consensus vector %q has no capture", v.Title,
		)
	}
	return runConsensusVector(t, v.Title, v.Capture)
}

// RunLedgerVector runs a ledger-category vector against
// DingoStateManager via the ouroboros-mock conformance harness.
// pparams blobs the harness needs by hash are committed in-tree
// under testdata/mock-pparams.
func RunLedgerVector(t *testing.T, v format.TestVector) error {
	t.Helper()
	if v.Category != format.CategoryLedger {
		return fmt.Errorf(
			"vector %q: expected category %q, got %q",
			v.Title, format.CategoryLedger, v.Category,
		)
	}
	if v.LedgerPhase == nil {
		return fmt.Errorf(
			"ledger vector %q has no ledger_phase", v.Title,
		)
	}
	return runLedgerVector(t, v.Title, v.LedgerPhase)
}
