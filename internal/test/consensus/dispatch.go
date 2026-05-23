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

// RunVector dispatches v on its category to the appropriate driver
// and returns nil on conformance, or an error describing the
// divergence. Schema-version validation already happened at decode
// time (format.DecodeTestVector rejects unknown versions), so the
// dispatch here is just category routing.
func RunVector(t *testing.T, v format.TestVector) error {
	t.Helper()
	switch v.Category {
	case format.CategoryConsensus:
		if v.Capture == nil {
			return fmt.Errorf(
				"consensus vector %q has no capture", v.Title,
			)
		}
		return runConsensusVector(t, v.Title, v.Capture)
	case format.CategoryLedger:
		if v.LedgerPhase == nil {
			return fmt.Errorf(
				"ledger vector %q has no ledger_phase", v.Title,
			)
		}
		return runLedgerVector(t, v.Title, v.LedgerPhase)
	default:
		return fmt.Errorf(
			"vector %q has unknown category %q",
			v.Title, v.Category,
		)
	}
}
