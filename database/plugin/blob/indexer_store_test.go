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

package blob_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestIndexerStoreKeepsOffsetsApartFromTheLedgers(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	shared := db.Blob()
	view := blob.NewIndexerStore(shared)
	txID, hash := make([]byte, 32), make([]byte, 32)
	txID[0], hash[0] = 1, 2

	wtxn := shared.NewTransaction(true)
	require.NoError(t, shared.SetUtxo(wtxn, txID, 0, []byte("ledger-utxo")))
	require.NoError(t, shared.SetTx(wtxn, hash, []byte("ledger-tx")))
	require.NoError(t, wtxn.Commit())

	// The same identity read through the view finds nothing until the
	// indexer writes its own entry.
	rtxn := view.NewTransaction(false)
	_, err = view.GetUtxo(rtxn, txID, 0)
	require.Error(t, err)
	require.NoError(t, rtxn.Rollback())

	itxn := view.NewTransaction(true)
	require.NoError(t, view.SetUtxo(itxn, txID, 0, []byte("indexer-utxo")))
	require.NoError(t, view.SetTx(itxn, hash, []byte("indexer-tx")))
	require.NoError(t, itxn.Commit())

	check := shared.NewTransaction(false)
	got, err := shared.GetUtxo(check, txID, 0)
	require.NoError(t, err)
	require.Equal(
		t,
		[]byte("ledger-utxo"),
		got,
		"ledger entry must be untouched",
	)
	got, err = shared.GetTx(check, hash)
	require.NoError(t, err)
	require.Equal(t, []byte("ledger-tx"), got)
	require.NoError(t, check.Rollback())

	check = view.NewTransaction(false)
	got, err = view.GetUtxo(check, txID, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("indexer-utxo"), got)
	got, err = view.GetTx(check, hash)
	require.NoError(t, err)
	require.Equal(t, []byte("indexer-tx"), got)
	require.NoError(t, check.Rollback())

	del := view.NewTransaction(true)
	require.NoError(t, view.DeleteUtxo(del, txID, 0))
	require.NoError(t, view.DeleteTx(del, hash))
	require.NoError(t, del.Commit())
	check = shared.NewTransaction(false)
	_, err = shared.GetUtxo(check, txID, 0)
	require.NoError(
		t,
		err,
		"deleting the indexer's offset must leave the ledger's",
	)
	require.NoError(t, check.Rollback())
}

func TestIndexerStoreRefusesLedgerWrites(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	view := blob.NewIndexerStore(db.Blob())
	txn := view.NewTransaction(true)
	t.Cleanup(func() { _ = txn.Rollback() })

	require.ErrorIs(t,
		view.SetBlock(txn, 1, []byte("h"), []byte("c"), 1, 1, 1, nil),
		blob.ErrIndexerReadOnly)
	require.ErrorIs(
		t,
		view.DeleteBlock(txn, 1, []byte("h"), 1),
		blob.ErrIndexerReadOnly,
	)
	require.ErrorIs(
		t,
		view.TombstoneBlock(txn, 1, []byte("h")),
		blob.ErrIndexerReadOnly,
	)
	require.ErrorIs(
		t,
		view.Set(txn, []byte("bp-ledger-key"), nil),
		blob.ErrIndexerReadOnly,
	)
	require.ErrorIs(
		t,
		view.Delete(txn, []byte("bp-ledger-key")),
		blob.ErrIndexerReadOnly,
	)
	require.NoError(
		t,
		view.Set(txn, []byte(blob.IndexerKeyPrefix+"own"), []byte("v")),
	)
}

func TestIndexerStoreCloseLeavesTheSharedStoreOpen(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	view := blob.NewIndexerStore(db.Blob())
	require.NoError(t, view.Close())
	txn := db.Blob().NewTransaction(false)
	_, err = db.Blob().GetUtxo(txn, make([]byte, 32), 0)
	require.ErrorIs(
		t,
		err,
		types.ErrBlobKeyNotFound,
		"a miss, not a closed store",
	)
	require.NoError(t, txn.Rollback())
}
