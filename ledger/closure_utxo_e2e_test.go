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

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

func certifiedUtxoScenario(
	t *testing.T,
) (ledger.BoundaryEpochStateScenario, *utxoset.Set, ledgerdb.LedgerDB, ledger.CertifiedUtxoSlots) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.CertifyLeios()
	return scenario, set, store, scenario.CertifiedUtxos()
}

func closureEffects(
	t *testing.T,
	set *utxoset.Set,
	after uint64,
) []ledger.ClosureUtxoEffect {
	t.Helper()
	var out []ledger.ClosureUtxoEffect
	require.NoError(t, set.ClosureUtxoEffectsAfter(
		after, func(e ledger.ClosureUtxoEffect) error {
			out = append(out, e)
			return nil
		}))
	return out
}

// A rollback past the closures removes their effects with the rows they
// journal, so no correction outlives the outputs it adjusts.
func TestClosureUtxoEffectsRollBack(t *testing.T) {
	t.Parallel()
	scenario, set, _, _ := certifiedUtxoScenario(t)
	require.Len(t, closureEffects(t, set, 0), 6)

	scenario.RollbackBeforeBoundary()

	require.Empty(t, closureEffects(t, set, 0))
}
