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

package ledger_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/utxoshadow"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// TestShadowMatchesTheLedgerAfterApplyAndRollback follows real block
// application, including an invalid transaction's collateral and collateral
// return, then a rollback across them, from the ledger.utxo stream alone, and
// requires the shadow set to match the metadata store's live set after each.
func TestShadowMatchesTheLedgerAfterApplyAndRollback(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{types.StorageModeCore, types.StorageModeAPI} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Stop)
			var shadow *utxoshadow.Shadow
			scenario := ledger.RunShadowScenarioForTest(
				t,
				mode,
				bus,
				nil,
				func(db *database.Database) {
					s, err := utxoshadow.Start(utxoshadow.Config{
						Dir:      t.TempDir(),
						LedgerDB: dbadapter.New(db),
						EventBus: bus,
					})
					require.NoError(t, err)
					t.Cleanup(func() { _ = s.Stop() })
					require.NoError(t, s.Bootstrap())
					shadow = s
				},
			)
			var rep utxoshadow.VerifyReport
			require.Eventually(t, func() bool {
				var err error
				rep, err = shadow.VerifyAll(t.Context())
				return err == nil && rep.Clean() && shadow.Applied() > 0
			}, 5*time.Second, 10*time.Millisecond, "shadow vs metadata store: %+v", rep)
			require.Positive(t, rep.Checked)
			t.Logf(
				"after apply: %d live outputs, %v changes applied",
				rep.Checked,
				shadow.Applied(),
			)

			// The scenario's first transactions spend inputs no block
			// created, so forward application starts with disagreements; the
			// rollback must add none.
			beforeRollback := shadow.Disagreements()
			// Roll back past the invalid transaction's block and the block
			// that spends its collateral return.
			require.Len(t, scenario.Points, 5)
			require.NoError(t, scenario.Rollback(scenario.Points[1]))
			require.Eventually(t, func() bool {
				var err error
				rep, err = shadow.VerifyAll(t.Context())
				return err == nil && rep.Clean() && shadow.Rollbacks() > 0
			}, 5*time.Second, 10*time.Millisecond, "after rollback: %+v", rep)
			t.Logf("after rollback: %d live outputs", rep.Checked)
			require.InDelta(t, beforeRollback, shadow.Disagreements(), 0,
				"the rollback must remove only live outputs and add only new ones")
		})
	}
}
