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
// ledger's LedgerDB and requires the answer to be the metadata store's.
func existsAt(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	slot uint64,
) bool {
	t.Helper()
	want, err := store.PoolOpCertSequencesExistAtSlot(t.Context(), slot, nil)
	require.NoError(t, err)
	got, err := ldb.PoolOpCertSequencesExistAtSlot(t.Context(), slot, nil)
	require.NoError(t, err)
	require.Equal(t, want, got, "slot %d", slot)
	return got
}

// TestOpCertExistenceReadAgreesWithTheLiveUtxoSet requires the set's counter
// observations to give the metadata store's answer to whether any counter
// was observed at a slot, at the slots counters were written and the ones
// between, and counts each read compared.
func TestOpCertExistenceReadAgreesWithTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := newOpCertExistsHarness(t)
	slots := scenario.OpCerts()
	ldb := scenario.LedgerDB()
	observed := []uint64{
		slots.Base, slots.Raised, slots.Repeated, slots.Lowered, slots.Batched,
	}
	for _, slot := range observed {
		require.True(t, existsAt(t, ldb, store, slot), "slot %d", slot)
	}
	unobserved := []uint64{
		slots.Base - 1, slots.Base + 1, slots.Raised - 1, slots.Batched + 1,
	}
	for _, slot := range unobserved {
		require.False(t, existsAt(t, ldb, store, slot), "slot %d", slot)
	}

	scenario.RollbackBeforeBoundary()
	require.True(t, existsAt(t, ldb, store, slots.Base))
	require.False(t, existsAt(t, ldb, store, slots.Raised),
		"the rollback undoes the observations after its target")
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
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x44}, 28))
	for _, w := range []struct{ sequence, slot uint64 }{{4, 100}, {6, 200}} {
		require.NoError(t, store.UpdatePoolOpCertSequence(
			t.Context(), pool, w.sequence, w.slot, nil,
		))
	}
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	ldb := scenario.LedgerDB()

	ok, err := ldb.PoolOpCertSequencesExistAtSlot(t.Context(), 100, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, existsAt(t, ldb, store, 200))
	require.False(t, existsAt(t, ldb, store, 250))
}

// TestOpCertExistenceReadUsesSetStateAfterAnUntrackedObservation pins the
// set result when a counter observation exists only in metadata.
func TestOpCertExistenceReadUsesSetStateAfterAnUntrackedObservation(t *testing.T) {
	t.Parallel()
	scenario, _, store := newOpCertExistsHarness(t)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x45}, 28))
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(t.Context(), pool, 9, 10, nil),
	)
	reached, _ := scenario.HideStoreReads("PoolOpCertSequencesExistAtSlot")
	ok, err := scenario.LedgerDB().
		PoolOpCertSequencesExistAtSlot(t.Context(), 10, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, reached(), "op-cert read reached the metadata store")
}
