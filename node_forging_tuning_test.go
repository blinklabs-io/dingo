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

package dingo

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/forging"
)

// TestApplyForgeTuningCarriesTheForgingKnobs covers the hop the binary
// takes between dingo.Config and the forger: internal/node's
// buildDingoConfig fills the former, initBlockForger builds the forger
// from the latter, and nothing else connects them. A field missing here
// reaches the forger as its zero value, which the forger then replaces
// with its own default -- so the operator's yaml, env or CLI setting
// disappears without an error anywhere.
func TestApplyForgeTuningCarriesTheForgingKnobs(t *testing.T) {
	t.Parallel()

	const refs, maxBytes = uint64(4321), uint64(98765)
	cfg := NewConfig(
		WithForgeSyncToleranceSlots(11),
		WithForgeStaleGapThresholdSlots(22),
		WithForgeEBSelectionReserve(750*time.Millisecond),
		WithForgeEBMaxTxRefs(refs),
		WithForgeEBMaxBytes(maxBytes),
	)

	var fc forging.ForgerConfig
	applyForgeTuning(&fc, &cfg)

	if fc.ForgeSyncToleranceSlots != 11 {
		t.Fatalf(
			"forgeSyncToleranceSlots = %d, want 11",
			fc.ForgeSyncToleranceSlots,
		)
	}
	if fc.ForgeStaleGapThresholdSlots != 22 {
		t.Fatalf(
			"forgeStaleGapThresholdSlots = %d, want 22",
			fc.ForgeStaleGapThresholdSlots,
		)
	}
	if fc.ForgeEBSelectionReserve != 750*time.Millisecond {
		t.Fatalf(
			"forgeEbSelectionReserve = %s, want 750ms",
			fc.ForgeEBSelectionReserve,
		)
	}
	if fc.ForgeEBMaxTxRefs == nil || *fc.ForgeEBMaxTxRefs != refs {
		t.Fatalf("forgeEbMaxTxRefs = %v, want %d", fc.ForgeEBMaxTxRefs, refs)
	}
	if fc.ForgeEBMaxBytes == nil || *fc.ForgeEBMaxBytes != maxBytes {
		t.Fatalf("forgeEbMaxBytes = %v, want %d", fc.ForgeEBMaxBytes, maxBytes)
	}
}
