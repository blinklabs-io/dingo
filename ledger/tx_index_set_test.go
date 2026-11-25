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

package ledger

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func txIndexTestHash(seed byte) []byte { return bytes.Repeat([]byte{seed}, 32) }

func txIndexTestPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, txIndexTestHash(byte(slot)))
}

// commitTxIndexBlock applies a block at slot whose only effect is indexing
// the transactions with hashes, as the block loop does: the entries and the
// block's point reach the LedgerDB when the transaction commits.
func commitTxIndexBlock(
	t *testing.T,
	ls *LedgerState,
	slot uint64,
	hashes ...[]byte,
) ocommon.Point {
	t.Helper()
	point := txIndexTestPoint(slot)
	require.NoError(t, ls.db.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			for i, hash := range hashes {
				ls.stageTxIndex(
					dbadapter.Txn(txn), hash, point,
					uint32(i), //nolint:gosec // a few test transactions
					6, true,
				)
			}
			ls.stageUtxoSet(dbadapter.Txn(txn), &point)
			return nil
		},
	))
	return point
}

func requireTxIndexed(
	t *testing.T,
	ls *LedgerState,
	txn ledgerdb.Txn,
	hash []byte,
	want *ocommon.Point,
) {
	t.Helper()
	got, err := ls.ledgerDB().GetTransactionByHash(t.Context(), hash, txn)
	require.NoError(t, err)
	if want == nil {
		require.Nil(t, got, "transaction %x is indexed", hash[:2])
		return
	}
	require.NotNil(t, got, "transaction %x is not indexed", hash[:2])
	require.Equal(t, want.Slot, got.Slot)
	require.Equal(t, want.Hash, got.BlockHash)
}

// A transaction lookup inside a write transaction sees the entries that
// transaction staged and not the ones a rollback it staged will drop, while a
// lookup outside it sees only what the LedgerDB committed. The commit then
// leaves the LedgerDB as the transaction saw it.
func TestTxIndexLookupSeesStagedEntriesAndStagedRollback(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{db: newTestDB(t)}
	attachTestLedgerDB(t, ls)
	kept, undone, staged := txIndexTestHash(0xa1), txIndexTestHash(0xa2),
		txIndexTestHash(0xa3)
	first := commitTxIndexBlock(t, ls, 100, kept)
	second := commitTxIndexBlock(t, ls, 150, undone)
	requireTxIndexed(t, ls, nil, kept, &first)
	requireTxIndexed(t, ls, nil, undone, &second)

	replacement := txIndexTestPoint(120)
	require.NoError(t, ls.db.Transaction(context.Background(), true).Do(
		func(dbTxn *database.Txn) error {
			txn := dbadapter.Txn(dbTxn)
			ls.stageUtxoSetRollback(txn, first)
			ls.stageTxIndex(txn, staged, replacement, 0, 6, true)
			ls.stageUtxoSet(txn, &replacement)

			requireTxIndexed(t, ls, txn, kept, &first)
			requireTxIndexed(t, ls, txn, undone, nil)
			requireTxIndexed(t, ls, txn, staged, &replacement)
			both, err := ls.ledgerDB().GetTransactionsByHashes(
				t.Context(), [][]byte{staged, undone, kept, staged}, txn,
			)
			require.NoError(t, err)
			require.Len(t, both, 2)
			require.Equal(t, staged, both[0].Hash)
			require.Equal(t, kept, both[1].Hash)

			// Nothing is committed yet.
			requireTxIndexed(t, ls, nil, undone, &second)
			requireTxIndexed(t, ls, nil, staged, nil)
			return nil
		},
	))
	requireTxIndexed(t, ls, nil, kept, &first)
	requireTxIndexed(t, ls, nil, undone, nil)
	requireTxIndexed(t, ls, nil, staged, &replacement)
}

// A transaction lookup fails rather than report a transaction absent once the
// LedgerDB has stopped following the ledger: an absent transaction is one the
// ledger would apply again.
func TestTxIndexLookupFailsWhenTheLedgerDBStopped(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{db: newTestDB(t)}
	attachTestLedgerDB(t, ls)
	hash := txIndexTestHash(0xb1)
	point := commitTxIndexBlock(t, ls, 100, hash)
	requireTxIndexed(t, ls, nil, hash, &point)

	ls.utxoSetFailed.Store(true)
	_, err := ls.ledgerDB().GetTransactionByHash(t.Context(), hash, nil)
	require.ErrorIs(t, err, errTxIndexUnavailable)
	_, err = ls.ledgerDB().GetTransactionsByHashes(
		t.Context(), [][]byte{hash}, nil,
	)
	require.ErrorIs(t, err, errTxIndexUnavailable)
}

// The transactions a client asks about are committed when the ledger has
// applied them, which the LedgerDB's index records; a rolled-back or unknown
// transaction is not among them.
func TestExistingTransactionHashesReadsTheTransactionIndex(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{db: newTestDB(t)}
	attachTestLedgerDB(t, ls)
	kept, undone, unknown := txIndexTestHash(0xc1), txIndexTestHash(0xc2),
		txIndexTestHash(0xc3)
	first := commitTxIndexBlock(t, ls, 100, kept)
	commitTxIndexBlock(t, ls, 150, undone)

	got, err := ls.ExistingTransactionHashes(
		t.Context(), [][]byte{unknown, undone, kept},
	)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{undone, kept}, got)

	require.NoError(t, ls.db.Transaction(context.Background(), true).Do(
		func(dbTxn *database.Txn) error {
			ls.stageUtxoSetRollback(dbadapter.Txn(dbTxn), first)
			return nil
		},
	))
	got, err = ls.ExistingTransactionHashes(
		t.Context(), [][]byte{unknown, undone, kept},
	)
	require.NoError(t, err)
	require.Equal(t, [][]byte{kept}, got)
}

// Seeding the genesis block indexes each genesis transaction at slot 0 under
// the genesis hash, so a later lookup of a genesis output's producer finds it
// like any other transaction's.
func TestCreateGenesisBlockIndexesGenesisTransactions(t *testing.T) {
	t.Parallel()
	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock(context.Background()))

	index, ok := ls.config.UtxoSet.(TxIndexReader)
	require.True(t, ok)
	var entries []TxIndexEntry
	require.NoError(t, index.EachTxIndexEntry(func(e TxIndexEntry) error {
		entries = append(entries, e)
		return nil
	}))
	require.NotEmpty(t, entries, "no genesis transaction is indexed")
	for _, e := range entries {
		require.Zero(t, e.Slot)
		require.Len(t, e.BlockHash, 32)
		require.True(t, e.Valid)
		_, err := ls.UtxoByRef(e.Hash, 0)
		require.NoError(t, err, "genesis transaction %x has no output", e.Hash)
	}
}
