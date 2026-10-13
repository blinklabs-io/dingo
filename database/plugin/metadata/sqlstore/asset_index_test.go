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
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func assetPolicy(b byte) []byte { return bytes.Repeat([]byte{b}, 28) }

func insertMintBurn(
	t *testing.T,
	db *sql.DB,
	txHash []byte,
	policy []byte,
	name string,
	slot uint64,
	txIndex uint32,
	quantity string,
) {
	t.Helper()
	_, err := db.Exec(`
INSERT INTO asset_mint_burn (tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		txHash, policy, []byte(name), []byte("fp"), slot, quantity, txIndex,
	)
	require.NoError(t, err)
}

func TestListMintedAssetsOrdersByFirstEvent(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	p1, p2 := assetPolicy(0x01), assetPolicy(0x02)
	// Insertion order is first-event order: A, B, C. A's later burn must not
	// move it, and the empty name is an asset of its own.
	insertMintBurn(t, db, credentialHash(0x11), p1, "A", 10, 0, "5")
	insertMintBurn(t, db, credentialHash(0x12), p1, "B", 11, 0, "5")
	insertMintBurn(t, db, credentialHash(0x13), p2, "C", 12, 0, "5")
	insertMintBurn(t, db, credentialHash(0x14), p1, "A", 13, 0, "-2")
	insertMintBurn(t, db, credentialHash(0x15), p1, "", 14, 0, "1")

	names := func(assets []models.MintedAsset) []string {
		out := make([]string, 0, len(assets))
		for _, asset := range assets {
			out = append(out, string(asset.PolicyId[:1])+"/"+string(asset.Name))
		}
		return out
	}
	policyTag := func(b byte) string { return string([]byte{b}) }

	all, err := store.ListMintedAssets(nil, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		policyTag(1) + "/A", policyTag(1) + "/B", policyTag(2) + "/C", policyTag(1) + "/",
	}, names(all))

	desc, err := store.ListMintedAssets(nil, 0, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		policyTag(1) + "/", policyTag(2) + "/C", policyTag(1) + "/B", policyTag(1) + "/A",
	}, names(desc))

	page, err := store.ListMintedAssets(nil, 2, 1, false, nil)
	require.NoError(t, err)
	require.Equal(t, []string{policyTag(1) + "/B", policyTag(2) + "/C"}, names(page))

	onlyP1, err := store.ListMintedAssets(p1, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		policyTag(1) + "/A", policyTag(1) + "/B", policyTag(1) + "/",
	}, names(onlyP1))

	total, err := store.CountMintedAssets(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	total, err = store.CountMintedAssets(p1, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	total, err = store.CountMintedAssets(assetPolicy(0x09), nil)
	require.NoError(t, err)
	require.Zero(t, total)
	none, err := store.ListMintedAssets(assetPolicy(0x09), 10, 0, false, nil)
	require.NoError(t, err)
	require.Empty(t, none)
}

func TestGetAssetMintBurnHistoryOrdersByChainPosition(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	policy := assetPolicy(0x21)
	other := assetPolicy(0x22)
	// Inserted out of chain order: the slot and tx_index decide, id breaks
	// remaining ties.
	insertMintBurn(t, db, credentialHash(0x32), policy, "T", 20, 1, "-3")
	insertMintBurn(t, db, credentialHash(0x31), policy, "T", 20, 0, "10")
	insertMintBurn(t, db, credentialHash(0x33), policy, "T", 25, 0, "4")
	insertMintBurn(t, db, credentialHash(0x34), policy, "U", 20, 0, "1")
	insertMintBurn(t, db, credentialHash(0x35), other, "T", 20, 0, "1")

	policyHash := lcommon.NewBlake2b224(policy)
	rows, err := store.GetAssetMintBurnHistory(policyHash, []byte("T"), 0, 0, false, nil)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, credentialHash(0x31), rows[0].TxHash)
	require.Equal(t, "10", rows[0].Quantity)
	require.Equal(t, credentialHash(0x32), rows[1].TxHash)
	require.Equal(t, "-3", rows[1].Quantity)
	require.Equal(t, credentialHash(0x33), rows[2].TxHash)

	rows, err = store.GetAssetMintBurnHistory(policyHash, []byte("T"), 2, 0, true, nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, credentialHash(0x33), rows[0].TxHash)
	require.Equal(t, credentialHash(0x32), rows[1].TxHash)

	rows, err = store.GetAssetMintBurnHistory(policyHash, []byte("T"), 1, 2, false, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, credentialHash(0x33), rows[0].TxHash)

	total, err := store.CountAssetMintBurnHistory(policyHash, []byte("T"), nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	total, err = store.CountAssetMintBurnHistory(policyHash, []byte("absent"), nil)
	require.NoError(t, err)
	require.Zero(t, total)
}

type assetTxFixture struct {
	db     *sql.DB
	policy []byte
	name   []byte
}

func (f assetTxFixture) transaction(
	t *testing.T,
	hash []byte,
	slot uint64,
	blockIndex uint32,
) int64 {
	t.Helper()
	res, err := f.db.Exec(`
INSERT INTO "transaction" (hash, block_hash, slot, block_index, valid, fee)
VALUES (?, ?, ?, ?, 1, '0')`,
		hash, credentialHash(0xee), slot, blockIndex,
	)
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	return id
}

// output creates a UTxO produced by transactionID and holding the fixture
// asset when held is set. spentBy marks it consumed by that transaction.
func (f assetTxFixture) output(
	t *testing.T,
	txHash []byte,
	transactionID int64,
	held bool,
	spentBy []byte,
	collateralReturn bool,
) {
	t.Helper()
	var producer, collateral any
	if collateralReturn {
		collateral = transactionID
	} else {
		producer = transactionID
	}
	res, err := f.db.Exec(`
INSERT INTO utxo (transaction_id, collateral_return_for_tx_id, tx_id, output_idx,
    added_slot, deleted_slot, amount, spent_at_tx_id, credential_tag)
VALUES (?, ?, ?, 0, 1, 0, '1', ?, 0)`,
		producer, collateral, txHash, spentBy,
	)
	require.NoError(t, err)
	if !held {
		return
	}
	utxoID, err := res.LastInsertId()
	require.NoError(t, err)
	_, err = f.db.Exec(`
INSERT INTO asset (name, policy_id, fingerprint, utxo_id, amount)
VALUES (?, ?, ?, ?, '1')`, f.name, f.policy, []byte("fp"), utxoID)
	require.NoError(t, err)
}

func TestGetAssetTransactionsCoversMintCreateAndSpend(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	f := assetTxFixture{db: db, policy: assetPolicy(0x41), name: []byte("N")}
	policyHash := lcommon.NewBlake2b224(f.policy)

	mint, create, spend, unrelated, burn := credentialHash(0x51), credentialHash(0x52),
		credentialHash(0x53), credentialHash(0x54), credentialHash(0x55)
	collateralTx := credentialHash(0x56)
	f.transaction(t, mint, 10, 0)
	createID := f.transaction(t, create, 20, 1)
	f.transaction(t, spend, 30, 0)
	unrelatedID := f.transaction(t, unrelated, 15, 0)
	f.transaction(t, burn, 40, 2)
	collateralID := f.transaction(t, collateralTx, 25, 0)

	insertMintBurn(t, db, mint, f.policy, "N", 10, 0, "5")
	insertMintBurn(t, db, burn, f.policy, "N", 40, 2, "-1")
	// create produces an output holding the asset which spend consumes; the
	// mint transaction's own output is not recorded here.
	f.output(t, create, createID, true, spend, false)
	// A different asset must not match, and neither must an output without it.
	f.output(t, unrelated, unrelatedID, false, nil, false)
	// An asset-bearing collateral return belongs to the failed transaction.
	f.output(t, collateralTx, collateralID, true, nil, true)
	// A second UTxO of the same transaction does not duplicate the row.
	_, err := db.Exec(`
INSERT INTO utxo (transaction_id, tx_id, output_idx, added_slot, deleted_slot, amount, credential_tag)
VALUES (?, ?, 1, 1, 0, '1', 0)`, createID, create)
	require.NoError(t, err)
	var secondUtxo int64
	require.NoError(t, db.QueryRow(
		`SELECT id FROM utxo WHERE tx_id = ? AND output_idx = 1`, create,
	).Scan(&secondUtxo))
	_, err = db.Exec(`
INSERT INTO asset (name, policy_id, fingerprint, utxo_id, amount)
VALUES (?, ?, ?, ?, '1')`, f.name, f.policy, []byte("fp"), secondUtxo)
	require.NoError(t, err)

	hashes := func(txs []models.Transaction) [][]byte {
		out := make([][]byte, 0, len(txs))
		for _, tx := range txs {
			out = append(out, tx.Hash)
		}
		return out
	}

	txs, err := store.GetAssetTransactions(policyHash, f.name, nil, nil, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{mint, create, collateralTx, spend, burn}, hashes(txs))
	require.Equal(t, uint64(20), txs[1].Slot)
	require.Equal(t, uint32(1), txs[1].BlockIndex)
	require.Equal(t, credentialHash(0xee), txs[1].BlockHash)

	txs, err = store.GetAssetTransactions(policyHash, f.name, nil, nil, 2, 1, true, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{spend, collateralTx}, hashes(txs))

	from := &models.AddressTransactionPosition{Slot: 20, TxIndex: 1}
	to := &models.AddressTransactionPosition{Slot: 30, TxIndex: 0}
	txs, err = store.GetAssetTransactions(policyHash, f.name, from, to, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{create, collateralTx, spend}, hashes(txs))
	// Bounds are inclusive on the transaction index within a slot.
	later := &models.AddressTransactionPosition{Slot: 20, TxIndex: 2}
	txs, err = store.GetAssetTransactions(policyHash, f.name, later, nil, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{collateralTx, spend, burn}, hashes(txs))
	earlier := &models.AddressTransactionPosition{Slot: 20, TxIndex: 0}
	txs, err = store.GetAssetTransactions(policyHash, f.name, nil, earlier, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{mint}, hashes(txs))

	total, err := store.CountAssetTransactions(policyHash, f.name, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 5, total)
	total, err = store.CountAssetTransactions(policyHash, f.name, from, to, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	total, err = store.CountAssetTransactions(policyHash, []byte("other"), nil, nil, nil)
	require.NoError(t, err)
	require.Zero(t, total)
}
