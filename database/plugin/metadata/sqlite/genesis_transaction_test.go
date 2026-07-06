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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// setupFileTestStore creates a file-backed sqlite store. The file-based
// configuration is what genesis sync uses in production: it enables
// foreign_keys(1) and uses WAL with separate read/write pools. FK enforcement
// is what surfaces the genesis UTxO bug.
func setupFileTestStore(t *testing.T) *MetadataStoreSqlite {
	t.Helper()
	store, err := New(t.TempDir(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

// genesisOutputs builds n unspent genesis UTxO models. The hash-referencing FK
// columns (SpentAtTxId, ReferencedByTxId, CollateralByTxId) are set to
// zero-length, non-nil slices on purpose: this is the state that would trigger
// the FK failure if the column did not serialize empty -> SQL NULL. The
// types.NullableHash driver.Valuer must store these as NULL so the FK to
// transaction(hash) is skipped.
func genesisOutputs(n int) []models.Utxo {
	outputs := make([]models.Utxo, n)
	for i := range outputs {
		txid := make([]byte, 32)
		txid[0] = byte(i + 1)
		outputs[i] = models.Utxo{
			TxId:             txid,
			OutputIdx:        0,
			AddedSlot:        0,
			Amount:           1_000_000,
			SpentAtTxId:      types.NullableHash{},
			ReferencedByTxId: types.NullableHash{},
			CollateralByTxId: types.NullableHash{},
		}
	}
	return outputs
}

// TestSqliteSetGenesisTransactionWithinTxn reproduces the genesis sync FK
// failure: the genesis transaction row and its UTxOs are written inside a
// single explicit transaction (as ledger.createGenesisBlock does). Genesis
// UTxOs are unspent/unreferenced, so the nullable hash FK columns must be
// stored as NULL; an empty blob fails the FK to transaction(hash) with error
// 787. Before the fix this INSERT failed with
// "create genesis utxos: FOREIGN KEY constraint failed (787)".
func TestSqliteSetGenesisTransactionWithinTxn(t *testing.T) {
	t.Parallel()
	store := setupFileTestStore(t)

	hash := bytes.Repeat([]byte{0xaa}, 32)
	blockHash := bytes.Repeat([]byte{0xbb}, 32)
	outputs := genesisOutputs(3)

	txn, err := store.BeginTxn()
	require.NoError(t, err)

	err = store.SetGenesisTransaction(hash, blockHash, outputs, txn)
	require.NoError(t, err, "SetGenesisTransaction within txn")

	require.NoError(t, txn.Commit())

	// All outputs should reference the genesis transaction row.
	var count int64
	require.NoError(
		t,
		store.DB().Model(&models.Utxo{}).
			Where("transaction_id IS NOT NULL").
			Count(&count).Error,
	)
	require.Equal(t, int64(3), count, "all genesis UTxOs link to transaction")

	// The nullable hash FK columns must be stored as NULL, not empty blobs.
	var emptyFKCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Utxo{}).
			Where(
				"spent_at_tx_id IS NOT NULL OR referenced_by_tx_id IS NOT NULL OR collateral_by_tx_id IS NOT NULL",
			).
			Count(&emptyFKCount).Error,
	)
	require.Equal(
		t,
		int64(0),
		emptyFKCount,
		"genesis UTxOs must store NULL hash FKs, not empty blobs",
	)
}

// TestSqliteSetGenesisTransactionIdempotent verifies re-running genesis init
// does not error or duplicate rows.
func TestSqliteSetGenesisTransactionIdempotent(t *testing.T) {
	t.Parallel()
	store := setupFileTestStore(t)

	hash := bytes.Repeat([]byte{0xaa}, 32)
	blockHash := bytes.Repeat([]byte{0xbb}, 32)

	run := func() {
		txn, err := store.BeginTxn()
		require.NoError(t, err)
		err = store.SetGenesisTransaction(
			hash,
			blockHash,
			genesisOutputs(3),
			txn,
		)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
	}

	run()
	run() // re-run must not error or duplicate

	var txCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Transaction{}).Count(&txCount).Error,
	)
	require.Equal(t, int64(1), txCount, "exactly one genesis transaction")

	var utxoCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Utxo{}).Count(&utxoCount).Error,
	)
	require.Equal(t, int64(3), utxoCount, "no duplicate genesis UTxOs")
}
