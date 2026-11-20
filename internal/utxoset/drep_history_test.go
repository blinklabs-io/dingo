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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

func writeDRepHistoryFacts(t *testing.T, meta metadata.MetadataStore) {
	t.Helper()
	store, ok := meta.(metadata.FactRowStore)
	require.True(t, ok)
	byName := map[string]metadata.FactTable{}
	for _, tbl := range metadata.FactTables {
		byName[tbl.Name] = tbl
	}
	txn := meta.Transaction(t.Context())
	{
		require.NoError(t, store.WriteFactRows(
			byName["drep_expiry_history"],
			metadata.FactRows{
				Columns: []string{
					"credential_tag", "credential", "added_slot",
					"last_activity_epoch", "expiry_epoch",
				},
				Rows: [][]any{{
					int64(0), bytes.Repeat([]byte{0x41}, 28), int64(2),
					int64(3), int64(9),
				}},
			}, txn,
		))
		require.NoError(t, store.WriteFactRows(
			byName["drep_dormancy_history"],
			metadata.FactRows{
				Columns: []string{"added_slot", "previous_dormant_epochs"},
				Rows:    [][]any{{int64(3), int64(1)}},
			}, txn,
		))
		require.NoError(t, store.WriteFactRows(
			byName["drep_expiry_epoch_event"],
			metadata.FactRows{
				Columns: []string{"added_slot"}, Rows: [][]any{{int64(3)}},
			}, txn,
		))
		require.NoError(t, store.WriteFactRows(
			byName["account_drep_clear"],
			metadata.FactRows{
				Columns: []string{"credential_tag", "staking_key", "added_slot"},
				Rows: [][]any{{
					int64(0), bytes.Repeat([]byte{0x42}, 28), int64(4),
				}},
			}, txn,
		))
	}
	require.NoError(t, txn.Commit())
}

// The DRep history a set loads from the metadata store keeps the slots it was
// recorded at, takes the rows later commits record, and loses the rows after a
// rollback target as the metadata store's rollback deletes them.
func TestLoadedDRepHistoryFollowsCommitsAndRollbacks(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	writeDRepHistoryFacts(t, db.Metadata())

	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	loaded, err := set.DRepHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	lost, err := set.DRepHistoryLost()
	require.NoError(t, err)
	require.False(t, lost)
	got, err := set.DRepHistory()
	require.NoError(t, err)
	require.Len(t, got.Expiry, 1)
	require.Equal(t, uint64(9), got.Expiry[0].Expiry)
	require.Equal(t, []uint64{3}, got.Events)
	require.Len(t, got.Dormancy, 1)
	require.Len(t, got.Clears, 1)

	cred := bytes.Repeat([]byte{0x43}, 28)
	for _, slot := range []uint64{9, 10} {
		tip := slotPoint(slot)
		require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
			DRepHistory: []ledger.DRepHistoryChange{
				{
					Kind: ledger.DRepHistoryExpiry, Slot: slot, Key: cred,
					Activity: slot, Expiry: slot + 20,
				},
				{Kind: ledger.DRepHistoryEvent, Slot: slot},
				{Kind: ledger.DRepHistoryDormancy, Slot: slot, Previous: slot},
				{Kind: ledger.DRepHistoryClear, Slot: slot, Key: cred},
			},
		}))
	}
	got, err = set.DRepHistory()
	require.NoError(t, err)
	require.Len(t, got.Expiry, 3)
	require.Len(t, got.Events, 3)
	require.Len(t, got.Dormancy, 3)
	require.Len(t, got.Clears, 3)

	_, err = set.Rollback(slotPoint(9))
	require.NoError(t, err)
	got, err = set.DRepHistory()
	require.NoError(t, err)
	require.Len(t, got.Expiry, 2, "the row at the target stays, the later one goes")
	require.Equal(t, []uint64{3, 9}, got.Events)
	require.Len(t, got.Dormancy, 2)
	require.Len(t, got.Clears, 2)
}

// drainedHistoryLedgerDB is a LedgerDB whose DRep history and imported block
// counts are drained, as a store's are once their families are drained after
// the set loaded the rest.
type drainedHistoryLedgerDB struct {
	ledgerdb.LedgerDB
}

func (drainedHistoryLedgerDB) DRepHistoryRows(
	ledgerdb.Txn,
) (ledgerdb.DRepHistoryRows, error) {
	return ledgerdb.DRepHistoryRows{}, database.ErrFamilyDrained
}

func (drainedHistoryLedgerDB) ImportedBlockRows(
	ledgerdb.Txn,
) (map[uint64]ledgerdb.ImportedBlockRows, error) {
	return nil, database.ErrFamilyDrained
}

// A set that loads while the metadata store has already drained its history
// holds none of it: it is marked as having lost it, and so is a set that
// loses the imported block counts the same way, so neither is exported as an
// empty table.
func TestSetLoadedFromADrainedStoreRecordsWhatItLost(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 2)
	db := ledgerAt(t, slotPoint(2))
	require.NoError(t, set.LoadLedgerState(
		t.Context(), drainedHistoryLedgerDB{dbadapter.New(db)},
	))
	lost, err := set.DRepHistoryLost()
	require.NoError(t, err)
	require.True(t, lost)
	lost, err = set.ImportedBlocksLost()
	require.NoError(t, err)
	require.True(t, lost)

	loaded, err := set.DRepHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
}
