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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func newOpCertExistsHarness(t *testing.T) (
	ledger.BoundaryEpochStateScenario,
	*utxoset.Set,
	ledgerdb.LedgerDB,
) {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	return scenario, set, store
}

// existsAt reads whether any counter was observed at slot through the
// ledger's LedgerDB.
func existsAt(t *testing.T, ldb ledgerdb.LedgerDB, slot uint64) bool {
	t.Helper()
	got, err := ldb.PoolOpCertSequencesExistAtSlot(t.Context(), slot, nil)
	require.NoError(t, err)
	return got
}

// TestOpCertExistenceReadSeesItsTransactionsStagedObservation requires the
// read in a write transaction to see the counter that transaction has staged,
// as the metadata store does.
func TestOpCertExistenceReadSeesItsTransactionsStagedObservation(t *testing.T) {
	t.Parallel()
	scenario, _, _ := newOpCertExistsHarness(t)
	pool := bytes.Repeat([]byte{0x46}, 28)
	require.True(t, scenario.OpCertObservedInsideTxn(pool, 3, 77))
}

// TestOpCertExistenceReadBeforeTheSetsRecordUsesItsBaseline requires a slot
// before the set's record to be answered from the counters the set was
// loaded with, which were last observed after it.
func TestOpCertExistenceReadBeforeTheSetsRecordUsesItsBaseline(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x44}, 28))
	scenario := ledger.NewSeededBoundaryEpochStateScenarioForTest(
		t, set, nil, func(db *database.Database) {
			for _, w := range []struct{ sequence, slot uint64 }{
				{4, 100}, {6, 200},
			} {
				require.NoError(t, dbadapter.New(db).UpdatePoolOpCertSequence(
					t.Context(), pool, w.sequence, w.slot, nil,
				))
			}
		},
	)
	ldb := scenario.LedgerDB()

	ok, err := ldb.PoolOpCertSequencesExistAtSlot(t.Context(), 100, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, existsAt(t, ldb, 200))
	require.False(t, existsAt(t, ldb, 250))
}
