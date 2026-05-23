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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
)

// runLedgerVector replays a ledger-category vector against dingo's
// existing DingoStateManager (from internal/test/conformance/).
//
// Not yet wired end-to-end. The structural pieces are in place — the
// LedgerPhase event stream maps cleanly to DingoStateManager's
// LoadInitialState / ApplyTransaction / ProcessEpochBoundary /
// Rollback methods — but the **final-state comparison** step has no
// public ouroboros-mock entry point yet: the existing comparison
// helpers in ouroboros-mock/conformance/ are harness-private. Until
// either:
//
//   - the ledger-corpus conversion tool exists to populate
//     testdata/converted/ with real ledger-category vectors, or
//   - ouroboros-mock exports a public final-state comparison helper
//     this runner can call,
//
// runLedgerVector returns an explicit "not yet implemented" error.
// The dispatch path is wired so a ledger vector under
// testdata/converted/ surfaces this gap clearly rather than silently
// passing.
func runLedgerVector(
	t *testing.T,
	title string,
	phase *format.LedgerPhase,
) error {
	t.Helper()
	if phase == nil {
		return errors.New("runLedgerVector: nil LedgerPhase")
	}
	return fmt.Errorf(
		"ledger driver not yet wired end-to-end (vector %q): "+
			"awaits either the Amaru-corpus conversion tool to "+
			"produce real ledger vectors under testdata/converted/, "+
			"or an exported final-state comparison helper in "+
			"ouroboros-mock that this runner can call",
		title,
	)
}
