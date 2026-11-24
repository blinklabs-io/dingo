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

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func txIndexHash(seed byte) []byte { return bytes.Repeat([]byte{seed}, 32) }

// The transaction index survives a reopen, and a rollback drops the entries
// of the blocks it undoes while keeping the ones at and below its target.
func TestTxIndexSurvivesReopenAndRollsBack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	set, err := utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	apply := func(slot uint64, entries ...ledger.TxIndexEntry) ocommon.Point {
		tip := ocommon.NewPoint(slot, txIndexHash(byte(slot)))
		for i := range entries {
			entries[i].Slot = slot
			entries[i].BlockHash = tip.Hash
		}
		require.NoError(t, set.ApplyLedgerChanges(
			&tip, ledger.LedgerChanges{TxIndex: entries},
		))
		return tip
	}
	first := apply(100,
		ledger.TxIndexEntry{Hash: txIndexHash(0xa1), Type: 6, Valid: true},
		ledger.TxIndexEntry{
			Hash: txIndexHash(0xa2), BlockIndex: 1, Type: 6,
		},
	)
	second := apply(200,
		ledger.TxIndexEntry{Hash: txIndexHash(0xb1), Type: 7, Valid: true},
	)

	require.NoError(t, set.Close())
	set, err = utxoset.Open(utxoset.Config{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	got, err := set.TxIndexEntry(txIndexHash(0xa2))
	require.NoError(t, err)
	require.Equal(t, &ledger.TxIndexEntry{
		Hash: txIndexHash(0xa2), BlockHash: first.Hash, Slot: 100,
		BlockIndex: 1, Type: 6, Valid: false,
	}, got)
	got, err = set.TxIndexEntry(txIndexHash(0xb1))
	require.NoError(t, err)
	require.Equal(t, &ledger.TxIndexEntry{
		Hash: txIndexHash(0xb1), BlockHash: second.Hash, Slot: 200,
		Type: 7, Valid: true,
	}, got)
	absent, err := set.TxIndexEntry(txIndexHash(0xcc))
	require.NoError(t, err)
	require.Nil(t, absent)

	// A read as of the first block does not see the second block's entry,
	// before any rollback runs.
	asOf, err := set.TxIndexEntryAt(txIndexHash(0xb1), 100)
	require.NoError(t, err)
	require.Nil(t, asOf)
	after, err := set.TxHashesAfterSlot(nil, 100)
	require.NoError(t, err)
	require.Equal(t, [][]byte{txIndexHash(0xb1)}, after)

	_, err = set.Rollback(first)
	require.NoError(t, err)
	gone, err := set.TxIndexEntry(txIndexHash(0xb1))
	require.NoError(t, err)
	require.Nil(t, gone, "the rollback keeps an entry of a block it undid")
	kept, err := set.TxIndexEntry(txIndexHash(0xa1))
	require.NoError(t, err)
	require.NotNil(t, kept, "the rollback dropped an entry at its target")
	require.Equal(t, first.Hash, kept.BlockHash)
}

// A set that first loads state from a metadata store indexes every
// transaction the store records as applied, with the block it names, and
// none the store only holds an identity row for.
func TestSetLoadsTheMetadataStoresAppliedTransactions(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := releasedAt(t, slotPoint(8))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	for _, tx := range []struct {
		hash, block []byte
		slot        uint64
		era, index  int
		valid       bool
	}{
		{txIndexHash(0x01), txIndexHash(0xf1), 5, 6, 0, true},
		{txIndexHash(0x02), txIndexHash(0xf1), 5, 6, 1, false},
		{txIndexHash(0x03), txIndexHash(0xf2), 7, 7, 0, true},
	} {
		_, err := testutil.ExecTransactionInsert(raw, `
INSERT INTO "transaction" (
    hash, block_hash, slot, type, valid, block_index
) VALUES (?, ?, ?, ?, ?, ?)`,
			tx.hash, tx.block, tx.slot, tx.era, tx.valid, tx.index,
		)
		require.NoError(t, err)
	}
	// A rolled-back transaction keeps its identity row and no application.
	_, err = raw.Exec(
		`INSERT INTO "transaction" (hash) VALUES (?)`, txIndexHash(0x04),
	)
	require.NoError(t, err)

	loaded, err := set.TxIndexLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	loaded, err = set.TxIndexLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	var got []ledger.TxIndexEntry
	require.NoError(t, set.EachTxIndexEntry(func(e ledger.TxIndexEntry) error {
		got = append(got, e)
		return nil
	}))
	require.Equal(t, []ledger.TxIndexEntry{
		{
			Hash: txIndexHash(0x01), BlockHash: txIndexHash(0xf1), Slot: 5,
			Type: 6, Valid: true,
		},
		{
			Hash: txIndexHash(0x02), BlockHash: txIndexHash(0xf1), Slot: 5,
			BlockIndex: 1, Type: 6,
		},
		{
			Hash: txIndexHash(0x03), BlockHash: txIndexHash(0xf2), Slot: 7,
			Type: 7, Valid: true,
		},
	}, got)

	// The load runs once: a row the store gains later is the ledger's to
	// index, not a second load's.
	_, err = testutil.ExecTransactionInsert(raw, `
INSERT INTO "transaction" (
    hash, block_hash, slot, type, valid, block_index
) VALUES (?, ?, 8, 7, TRUE, 0)`,
		txIndexHash(0x05), txIndexHash(0xf3),
	)
	require.NoError(t, err)
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	late, err := set.TxIndexEntry(txIndexHash(0x05))
	require.NoError(t, err)
	require.Nil(t, late)
}

// An index row has hash-sized hashes. A store row that does not is refused,
// by the load and by a commit alike, and a refused load leaves the index
// unloaded so the next start reads the store again.
func TestTxIndexRefusesHashesOfTheWrongLength(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		hash, block []byte
	}{
		{"short transaction hash", txIndexHash(0x01)[:31], txIndexHash(0xf1)},
		{"long block hash", txIndexHash(0x01), append(txIndexHash(0xf1), 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := openRecoverySet(t, utxoset.Config{})
			applySlots(t, set, 8)
			tip := slotPoint(9)
			require.ErrorContains(t, set.ApplyLedgerChanges(
				&tip,
				ledger.LedgerChanges{TxIndex: []ledger.TxIndexEntry{{
					Hash: tc.hash, BlockHash: tc.block, Slot: 9, Valid: true,
				}}},
			), "transaction index")

			db := releasedAt(t, slotPoint(8))
			raw, err := dbtest.RawSQLiteMetadata(t, db)
			require.NoError(t, err)
			_, err = testutil.ExecTransactionInsert(raw, `
INSERT INTO "transaction" (
    hash, block_hash, slot, type, valid, block_index
) VALUES (?, ?, 5, 6, TRUE, 0)`,
				tc.hash, tc.block,
			)
			require.NoError(t, err)
			require.ErrorContains(
				t,
				set.LoadLedgerState(t.Context(), dbadapter.New(db)),
				"transaction index",
			)
			loaded, err := set.TxIndexLoaded()
			require.NoError(t, err)
			require.False(t, loaded)
		})
	}
}
