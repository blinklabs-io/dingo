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

package sqlstore

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

const scriptTestAddress = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

func TestListScriptHashesAndCountScripts(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	for i, hash := range [][]byte{
		credentialHash(0x03), credentialHash(0x01), credentialHash(0x02),
	} {
		_, err := db.Exec(
			`INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, ?, 1)`,
			hash,
			[]byte{0x41, byte(i)},
			i,
		)
		require.NoError(t, err)
	}

	hashes, err := store.ListScriptHashes(0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		credentialHash(0x03), credentialHash(0x01), credentialHash(0x02),
	}, hashes, "scripts list in the order they were first recorded")
	hashes, err = store.ListScriptHashes(2, 1, true, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		[][]byte{credentialHash(0x01), credentialHash(0x03)},
		hashes,
	)
	total, err := store.CountScripts(nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
}

// referenceScriptTx builds a transaction with one output carrying script as a
// reference script and one plain output.
func referenceScriptTx(
	t *testing.T,
	seed byte,
	script lcommon.PlutusV2Script,
) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	scriptRef, err := cbor.Encode(&lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV2,
		Script: script,
	})
	require.NoError(t, err)
	txID := bytes.Repeat([]byte{seed}, 32)
	holder, err := mockledger.NewUtxoBuilder().
		WithTxId(txID).WithIndex(0).
		WithAddress(scriptTestAddress).
		WithLovelace(1_000_000).
		WithScriptRef(scriptRef).
		Build()
	require.NoError(t, err)
	plain, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(scriptTestAddress).WithLovelace(2_000_000).Build()
	require.NoError(t, err)
	// The input must not be the holder itself, which would spend the output
	// the moment it is created.
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(bytes.Repeat([]byte{^seed}, 32)).WithIndex(0).Build()
	require.NoError(t, err)
	tx, err := mockledger.NewTransactionBuilder().
		WithId(txID).
		WithInputs(input).
		WithOutputs(holder.Output, plain).
		WithValid(true).
		Build()
	require.NoError(t, err)
	return tx, ocommon.Point{Slot: 100 + uint64(seed), Hash: txID}
}

// rawDB returns the connection the open transaction holds. The API-mode test
// store has a single connection, so reading through the store's own pool
// would wait forever on the transaction that owns it.
func rawDB(t *testing.T, store *Store, txn types.Txn) queryer {
	t.Helper()
	db, _, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	return db
}

func referenceScriptRows(
	t *testing.T,
	store *Store,
	txn types.Txn,
) map[uint][]byte {
	t.Helper()
	rows, err := rawDB(t, store, txn).QueryContext(
		context.Background(),
		`SELECT utxo.output_idx, utxo_reference_script.script_hash
FROM utxo_reference_script JOIN utxo ON utxo.id = utxo_reference_script.utxo_id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	got := map[uint][]byte{}
	for rows.Next() {
		var (
			index uint
			hash  []byte
		)
		require.NoError(t, rows.Scan(&index, &hash))
		got[index] = hash
	}
	require.NoError(t, rows.Err())
	return got
}

func TestSetTransactionIndexesReferenceScripts(t *testing.T) {
	t.Parallel()
	script := lcommon.PlutusV2Script{0x41, 0x07}

	t.Run("api mode records the holder only", func(t *testing.T) {
		t.Parallel()
		store := newAPIModeSQLiteStore(t, nil)
		txn := store.Transaction(context.Background())
		t.Cleanup(func() { _ = txn.Rollback() })
		tx, point := referenceScriptTx(t, 0x31, script)
		require.NoError(
			t,
			store.SetTransaction(tx, point, 0, nil, true, txn, 0),
		)
		require.Equal(
			t,
			map[uint][]byte{0: script.Hash().Bytes()},
			referenceScriptRows(t, store, txn),
		)

		// Applying the transaction again converges on the same row.
		require.NoError(
			t,
			store.SetTransaction(tx, point, 0, nil, true, txn, 0),
		)
		require.Len(t, referenceScriptRows(t, store, txn), 1)
	})

	t.Run("core mode keeps no script index", func(t *testing.T) {
		t.Parallel()
		store := newMigratedSQLiteStore(t)
		txn := store.Transaction(context.Background())
		t.Cleanup(func() { _ = txn.Rollback() })
		tx, point := referenceScriptTx(t, 0x32, script)
		require.NoError(
			t,
			store.SetTransaction(tx, point, 0, nil, true, txn, 0),
		)
		require.Empty(t, referenceScriptRows(t, store, txn))
	})
}

func TestReferenceScriptRowsGoWithTheirUtxo(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	tx, point := referenceScriptTx(t, 0x33, lcommon.PlutusV2Script{0x41, 0x08})
	require.NoError(t, store.SetTransaction(tx, point, 0, nil, true, txn, 0))
	require.Len(t, referenceScriptRows(t, store, txn), 1)

	_, err := rawDB(t, store, txn).ExecContext(
		context.Background(), `DELETE FROM utxo`,
	)
	require.NoError(t, err)
	require.Empty(t, referenceScriptRows(t, store, txn))
}

func TestGetScriptReferenceUtxos(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	script := lcommon.PlutusV2Script{0x41, 0x09}
	other := lcommon.PlutusV2Script{0x41, 0x0a}
	for _, seed := range []byte{0x41, 0x42, 0x43} {
		s := script
		if seed == 0x43 {
			s = other
		}
		tx, point := referenceScriptTx(t, seed, s)
		require.NoError(
			t,
			store.SetTransaction(tx, point, 0, nil, true, txn, 0),
		)
	}
	// 0x42's holder is spent.
	_, err := rawDB(t, store, txn).ExecContext(
		context.Background(),
		`UPDATE utxo SET deleted_slot = 7 WHERE tx_id = ? AND output_idx = 0`,
		bytes.Repeat([]byte{0x42}, 32),
	)
	require.NoError(t, err)

	utxos, err := store.GetScriptReferenceUtxos(script.Hash(), 0, 0, false, txn)
	require.NoError(t, err)
	require.Len(t, utxos, 1)
	require.Equal(t, bytes.Repeat([]byte{0x41}, 32), utxos[0].TxId)
	require.Equal(t, uint32(0), utxos[0].OutputIdx)
	total, err := store.CountScriptReferenceUtxos(script.Hash(), txn)
	require.NoError(t, err)
	require.Equal(t, 1, total)

	_, err = rawDB(t, store, txn).ExecContext(
		context.Background(), `UPDATE utxo SET deleted_slot = 0`,
	)
	require.NoError(t, err)
	utxos, err = store.GetScriptReferenceUtxos(script.Hash(), 0, 0, false, txn)
	require.NoError(t, err)
	require.Len(t, utxos, 2)
	require.Equal(t, bytes.Repeat([]byte{0x41}, 32), utxos[0].TxId)
	require.Equal(t, bytes.Repeat([]byte{0x42}, 32), utxos[1].TxId)
	utxos, err = store.GetScriptReferenceUtxos(script.Hash(), 1, 0, true, txn)
	require.NoError(t, err)
	require.Len(t, utxos, 1)
	require.Equal(t, bytes.Repeat([]byte{0x42}, 32), utxos[0].TxId)
	utxos, err = store.GetScriptReferenceUtxos(script.Hash(), 5, 2, false, txn)
	require.NoError(t, err)
	require.Empty(t, utxos)
	total, err = store.CountScriptReferenceUtxos(other.Hash(), txn)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	total, err = store.CountScriptReferenceUtxos(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xee}, 28)), txn,
	)
	require.NoError(t, err)
	require.Zero(t, total)
}

type redeemerTxOptions struct {
	seed      byte
	inputs    []lcommon.TransactionInput
	mintHash  lcommon.Blake2b224
	redeemers map[lcommon.RedeemerKey]lcommon.RedeemerValue
}

func redeemerTx(
	t *testing.T,
	opts redeemerTxOptions,
) (lcommon.Transaction, ocommon.Point) {
	t.Helper()
	txID := bytes.Repeat([]byte{opts.seed}, 32)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(scriptTestAddress).WithLovelace(1_000_000).Build()
	require.NoError(t, err)
	tx := mockledger.NewTransactionBuilder()
	if opts.mintHash != (lcommon.Blake2b224{}) {
		mint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
			map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
				opts.mintHash: {cbor.NewByteString([]byte("t")): big.NewInt(1)},
			},
		)
		tx.WithMint(&mint)
	}
	tx.WithId(txID)
	tx.WithInputs(opts.inputs...)
	tx.WithOutputs(output)
	tx.WithValid(true)
	tx.WithWitnesses(
		mockledger.NewMockTransactionWitnessSet().WithRedeemers(
			&conway.ConwayRedeemers{Redeemers: opts.redeemers},
		),
	)
	return tx, ocommon.Point{Slot: 200 + uint64(opts.seed), Hash: txID}
}

func redeemerValue(mem int64) lcommon.RedeemerValue {
	var data lcommon.Datum
	data.SetCbor([]byte{0x01})
	return lcommon.RedeemerValue{
		Data:    data,
		ExUnits: lcommon.ExUnits{Memory: mem, Steps: mem * 2},
	}
}

func TestSetTransactionIndexesRedeemerScripts(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)

	// A UTxO locked by script hash H, and one locked by a key. They are
	// stored before the test's own transaction opens: the API-mode store has
	// a single connection, which an open transaction holds.
	scriptHash := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x77}, 28))
	keyHash := bytes.Repeat([]byte{0x78}, 28)
	lockedTx := bytes.Repeat([]byte{0x51}, 32)
	insertUtxoInTxn(t, store, &models.Utxo{
		Address: []byte("addr"), TxId: lockedTx, OutputIdx: 0, PaymentKey: scriptHash.Bytes(),
		PaymentScript: true, AddedSlot: 1, Amount: 5,
	}, true)
	insertUtxoInTxn(t, store, &models.Utxo{
		Address: []byte("addr"), TxId: lockedTx, OutputIdx: 1, PaymentKey: keyHash,
		AddedSlot: 1, Amount: 5,
	}, true)
	lockedInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(lockedTx).WithIndex(0).Build()
	require.NoError(t, err)
	keyInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(lockedTx).WithIndex(1).Build()
	require.NoError(t, err)
	missingInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(bytes.Repeat([]byte{0x52}, 32)).WithIndex(0).Build()
	require.NoError(t, err)

	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })

	// The inputs are listed in the transaction body's canonical order, which
	// the redeemer's spend index points into.
	inputs := []lcommon.TransactionInput{lockedInput, keyInput, missingInput}
	tx, point := redeemerTx(t, redeemerTxOptions{
		seed:     0x61,
		inputs:   inputs,
		mintHash: scriptHash,
		redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: redeemerValue(10),
			{Tag: lcommon.RedeemerTagSpend, Index: 1}: redeemerValue(11),
			{Tag: lcommon.RedeemerTagSpend, Index: 2}: redeemerValue(12),
			{Tag: lcommon.RedeemerTagMint, Index: 0}:  redeemerValue(13),
		},
	})
	require.NoError(t, store.SetTransaction(tx, point, 3, nil, true, txn, 0))

	refs, err := store.GetScriptRedeemers(scriptHash, 0, 0, false, txn)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Equal(t, bytes.Repeat([]byte{0x61}, 32), refs[0].TxHash)
	require.Equal(t, uint8(lcommon.RedeemerTagSpend), refs[0].Tag)
	require.Equal(t, uint32(0), refs[0].Index)
	require.Equal(t, point.Slot, refs[0].Slot)
	require.Equal(t, uint32(3), refs[0].TxIndex)
	require.Equal(t, uint8(lcommon.RedeemerTagMint), refs[1].Tag)
	require.Equal(t, uint32(0), refs[1].Index)
	total, err := store.CountScriptRedeemers(scriptHash, txn)
	require.NoError(t, err)
	require.Equal(t, 2, total)

	// The key-locked spend names no script, and the input that is not in the
	// index leaves its redeemer unattributed rather than failing the block.
	var rows int
	require.NoError(t, rawDB(t, store, txn).QueryRowContext(
		context.Background(), `SELECT COUNT(*) FROM redeemer_script`,
	).Scan(&rows))
	require.Equal(t, 2, rows)

	// Replaying the transaction rewrites rather than duplicates its rows.
	require.NoError(t, store.SetTransaction(tx, point, 3, nil, true, txn, 0))
	total, err = store.CountScriptRedeemers(scriptHash, txn)
	require.NoError(t, err)
	require.Equal(t, 2, total)

	refs, err = store.GetScriptRedeemers(scriptHash, 1, 0, true, txn)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, uint8(lcommon.RedeemerTagMint), refs[0].Tag)
}

// A spend redeemer's index points into the transaction's inputs sorted by
// (transaction id, output index), not into their order in the body. A body may
// list its inputs in any order.
func TestSetTransactionIndexesRedeemerScriptsUsesSortedInputs(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)

	scriptHash := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x7a}, 28))
	lockedTx := bytes.Repeat([]byte{0x53}, 32)
	insertUtxoInTxn(t, store, &models.Utxo{
		Address: []byte("addr"), TxId: lockedTx, OutputIdx: 0, PaymentKey: scriptHash.Bytes(),
		PaymentScript: true, AddedSlot: 1, Amount: 5,
	}, true)
	insertUtxoInTxn(t, store, &models.Utxo{
		Address: []byte("addr"), TxId: lockedTx, OutputIdx: 1,
		PaymentKey: bytes.Repeat([]byte{0x7b}, 28), AddedSlot: 1, Amount: 5,
	}, true)
	lockedInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(lockedTx).WithIndex(0).Build()
	require.NoError(t, err)
	keyInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(lockedTx).WithIndex(1).Build()
	require.NoError(t, err)

	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })

	tx, point := redeemerTx(t, redeemerTxOptions{
		seed:   0x62,
		inputs: []lcommon.TransactionInput{keyInput, lockedInput},
		redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: redeemerValue(10),
		},
	})
	require.NoError(t, store.SetTransaction(tx, point, 0, nil, true, txn, 0))

	refs, err := store.GetScriptRedeemers(scriptHash, 0, 0, false, txn)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, uint8(lcommon.RedeemerTagSpend), refs[0].Tag)
	require.Equal(t, uint32(0), refs[0].Index)
}

func TestGetScriptRedeemersOrdersByChainPosition(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	hash := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x81}, 28))
	// Inserted out of chain order.
	for _, row := range []struct {
		tx      byte
		slot    int
		txIndex int
		tag     int
		index   int
	}{
		{0xa3, 30, 0, 0, 0},
		{0xa1, 10, 2, 1, 0},
		{0xa1, 10, 2, 0, 1},
		{0xa1, 10, 2, 0, 0},
		{0xa2, 10, 3, 0, 0},
	} {
		res, err := db.Exec(
			`INSERT OR IGNORE INTO "transaction" (hash, block_hash, slot, block_index, valid, fee) VALUES (?, ?, ?, ?, 1, '0')`,
			bytes.Repeat(
				[]byte{row.tx},
				32,
			),
			credentialHash(0xee),
			row.slot,
			row.txIndex,
		)
		require.NoError(t, err)
		_ = res
		var id int64
		require.NoError(t, db.QueryRow(
			`SELECT id FROM "transaction" WHERE hash = ?`,
			bytes.Repeat([]byte{row.tx}, 32),
		).Scan(&id))
		_, err = db.Exec(
			"INSERT INTO redeemer_script (transaction_id, tag, `index`, script_hash, slot, tx_index) VALUES (?, ?, ?, ?, ?, ?)",
			id,
			row.tag,
			row.index,
			hash.Bytes(),
			row.slot,
			row.txIndex,
		)
		require.NoError(t, err)
	}

	order := func(refs []models.ScriptRedeemerRef) []string {
		out := make([]string, 0, len(refs))
		for _, ref := range refs {
			out = append(out, string([]byte{
				'0' + ref.TxHash[0]%16, '/', '0' + ref.Tag, '/', '0' + byte(ref.Index),
			}))
		}
		return out
	}
	refs, err := store.GetScriptRedeemers(hash, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{"1/0/0", "1/0/1", "1/1/0", "2/0/0", "3/0/0"},
		order(refs),
	)
	refs, err = store.GetScriptRedeemers(hash, 2, 1, true, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"2/0/0", "1/1/0"}, order(refs))
	require.Equal(t, uint64(10), refs[0].Slot)
	require.Equal(t, uint32(3), refs[0].TxIndex)
	other := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x82}, 28))
	total, err := store.CountScriptRedeemers(other, nil)
	require.NoError(t, err)
	require.Zero(t, total)
	_ = types.StorageModeAPI
}

// Outputs that reach the store outside block application -- a ledger-state
// snapshot import, a recovered spent input, a single created output -- carry
// their reference script into the index the same way applied outputs do.
func TestReferenceScriptIndexedOnImportAndCreate(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	hash := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x7c}, 28))
	newUtxo := func(seed byte) models.Utxo {
		return models.Utxo{
			Address:             []byte("addr"),
			TxId:                bytes.Repeat([]byte{seed}, 32),
			OutputIdx:           0,
			PaymentKey:          bytes.Repeat([]byte{0x7d}, 28),
			AddedSlot:           1,
			Amount:              5,
			ReferenceScriptHash: hash.Bytes(),
		}
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{newUtxo(0x71)}, nil))
	require.NoError(t, store.ImportUtxosDeferredRewardLiveStakeRefresh(
		[]models.Utxo{newUtxo(0x72)}, nil,
	))
	created := newUtxo(0x73)
	require.NoError(t, store.CreateUtxo(nil, &created))

	utxos, err := store.GetScriptReferenceUtxos(hash, 0, 0, false, nil)
	require.NoError(t, err)
	require.Len(t, utxos, 3)
	for i, seed := range []byte{0x71, 0x72, 0x73} {
		require.Equal(t, bytes.Repeat([]byte{seed}, 32), utxos[i].TxId)
	}
}
