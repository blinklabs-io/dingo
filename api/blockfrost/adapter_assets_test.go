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

package blockfrost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

func seedMintBurn(
	t *testing.T,
	raw *sql.DB,
	txHash []byte,
	policy []byte,
	name string,
	slot uint64,
	quantity string,
) {
	t.Helper()
	_, err := raw.Exec(`
INSERT INTO asset_mint_burn (tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index)
VALUES (?, ?, ?, ?, ?, ?, 0)`,
		txHash, policy, []byte(name), []byte("fp"), slot, quantity,
	)
	require.NoError(t, err)
}

func assetUnit(policy []byte, name string) string {
	return hex.EncodeToString(policy) + hex.EncodeToString([]byte(name))
}

func TestNodeAdapterAssetListings(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	p1 := bytes.Repeat([]byte{0xb1}, lcommon.AddressHashSize)
	p2 := bytes.Repeat([]byte{0xb2}, lcommon.AddressHashSize)
	addr, _, _ := testKeyAddress(t, 0xb3, 0)

	seedMintBurn(t, raw, fill32(0x01), p1, "A", 1, "12")
	seedMintBurn(t, raw, fill32(0x02), p1, "B", 2, "5")
	seedMintBurn(t, raw, fill32(0x03), p2, "C", 3, "9")
	seedMintBurn(t, raw, fill32(0x04), p1, "B", 4, "-5")
	// A is held by two live outputs; B was burned; C is held by a spent
	// output only, so it also has no live quantity.
	seedAddressOutput(t, raw, db, addr, fill32(0x11), 5, 1_000_000,
		[]models.Asset{{PolicyId: p1, Name: []byte("A"), Amount: 5}}, nil)
	seedAddressOutput(t, raw, db, addr, fill32(0x12), 6, 1_000_000,
		[]models.Asset{{PolicyId: p1, Name: []byte("A"), Amount: 7}}, nil)
	insertAdapterTransaction(t, raw, &models.Transaction{
		Hash: fill32(0x99), Slot: 7, Valid: true,
	})
	seedAddressOutput(
		t,
		raw,
		db,
		addr,
		fill32(0x13),
		6,
		1_000_000,
		[]models.Asset{
			{PolicyId: p2, Name: []byte("C"), Amount: 9},
		},
		fill32(0x99),
	)

	ctx := context.Background()
	items, total, err := adapter.Assets(
		ctx, PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, []AssetListItemInfo{
		{Asset: assetUnit(p1, "A"), Quantity: "12"},
		{Asset: assetUnit(p1, "B"), Quantity: "0"},
		{Asset: assetUnit(p2, "C"), Quantity: "0"},
	}, items)

	items, total, err = adapter.Assets(
		ctx, PaginationParams{Count: 2, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, []string{assetUnit(p2, "C"), assetUnit(p1, "B")},
		[]string{items[0].Asset, items[1].Asset})

	items, total, err = adapter.Assets(
		ctx, PaginationParams{Count: 2, Page: 2, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Len(t, items, 1)
	require.Equal(t, assetUnit(p2, "C"), items[0].Asset)

	items, total, err = adapter.AssetsByPolicy(
		ctx, hex.EncodeToString(p1),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, items, 2)
	require.Equal(t, "12", items[0].Quantity)

	items, total, err = adapter.AssetsByPolicy(
		ctx, hex.EncodeToString(bytes.Repeat([]byte{0xee}, 28)),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, items)

	// Past the last page and beyond the representable offset are empty pages
	// that still report the total.
	items, total, err = adapter.Assets(
		ctx,
		PaginationParams{
			Count: 100,
			Page:  MaxPaginationPage,
			Order: PaginationOrderAsc,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Empty(t, items)
}

func TestNodeAdapterAssetHistory(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	policy := bytes.Repeat([]byte{0xc1}, lcommon.AddressHashSize)
	addr, _, _ := testKeyAddress(t, 0xc2, 0)
	seedMintBurn(t, raw, fill32(0x01), policy, "T", 1, "10")
	seedMintBurn(t, raw, fill32(0x02), policy, "T", 2, "-3")
	policyHex := hex.EncodeToString(policy)
	ctx := context.Background()

	items, total, err := adapter.AssetHistory(
		ctx, policyHex, []byte("T"),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Equal(t, []AssetHistoryItemInfo{
		{
			TxHash: hex.EncodeToString(fill32(0x01)),
			Action: "minted",
			Amount: "10",
		},
		{
			TxHash: hex.EncodeToString(fill32(0x02)),
			Action: "burned",
			Amount: "3",
		},
	}, items)

	items, _, err = adapter.AssetHistory(
		ctx, policyHex, []byte("T"),
		PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "burned", items[0].Action)

	// An asset that is held but has no recorded mint, as after a snapshot
	// import, has an empty history; an asset nothing knows is not found.
	seedAddressOutput(
		t,
		raw,
		db,
		addr,
		fill32(0x21),
		3,
		1_000_000,
		[]models.Asset{
			{PolicyId: policy, Name: []byte("held"), Amount: 1},
		},
		nil,
	)
	items, total, err = adapter.AssetHistory(
		ctx, policyHex, []byte("held"),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, items)
	_, _, err = adapter.AssetHistory(
		ctx, policyHex, []byte("unknown"),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, ErrAssetNotFound)
}

// testSlotTime maps slot n to n seconds after the Unix epoch.
func testSlotTime(slot uint64) (time.Time, error) {
	return time.Unix(int64(slot), 0), nil // #nosec G115 -- test slots are small
}

func newSlotTimeDBBackedAdapter(
	t *testing.T,
) (*NodeAdapter, *sql.DB, *database.Database) {
	t.Helper()
	adapter, raw, db := newDBBackedAdapter(t)
	adapter.slotTime = testSlotTime
	return adapter, raw, db
}

// seedAssetTransaction stores a transaction in a block of its own at
// height 'height' and returns its row.
func seedAssetTransaction(
	t *testing.T,
	raw *sql.DB,
	db *database.Database,
	txHash []byte,
	height uint64,
	slot uint64,
	blockIndex uint32,
) *models.Transaction {
	t.Helper()
	blockHash := fill32(byte(0xd0 + height))
	require.NoError(t, db.BlockCreate(models.Block{
		Hash: blockHash, Slot: slot, Number: height,
		ID:   height + database.BlockInitialIndex,
		Type: 0, Cbor: []byte{byte(height)},
	}, nil))
	tx := &models.Transaction{
		Hash: txHash, BlockHash: blockHash, Slot: slot,
		BlockIndex: blockIndex, Valid: true,
	}
	insertAdapterTransaction(t, raw, tx)
	return tx
}

func TestNodeAdapterAssetTransactions(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newSlotTimeDBBackedAdapter(t)
	policy := bytes.Repeat([]byte{0xd1}, lcommon.AddressHashSize)
	policyHex := hex.EncodeToString(policy)
	mint, move := fill32(0x01), fill32(0x02)

	seedAssetTransaction(t, raw, db, mint, 1, 10, 0)
	seedAssetTransaction(t, raw, db, move, 3, 30, 1)
	require.NoError(t, db.SetTip(ochainsync.Tip{BlockNumber: 3}, nil))
	seedMintBurn(t, raw, mint, policy, "T", 10, "5")
	addr, _, _ := testKeyAddress(t, 0xd2, 0)
	// produce creates an output holding the asset in block 2, which move
	// consumes.
	produce := fill32(0x03)
	seedAddressOutput(t, raw, db, addr, produce, 20, 1_000_000,
		[]models.Asset{{PolicyId: policy, Name: []byte("T"), Amount: 5}}, move)
	blockHash := fill32(0xd2)
	require.NoError(t, db.BlockCreate(models.Block{
		Hash: blockHash, Slot: 20, Number: 2,
		ID:   2 + database.BlockInitialIndex,
		Type: 0, Cbor: []byte{2},
	}, nil))
	_, err := raw.Exec(
		`UPDATE "transaction" SET block_hash = ?, block_index = 0 WHERE hash = ?`,
		blockHash,
		produce,
	)
	require.NoError(t, err)

	ctx := context.Background()
	params := TransactionRangeParams{
		Pagination: PaginationParams{
			Count: 10,
			Page:  1,
			Order: PaginationOrderAsc,
		},
	}
	rows, total, err := adapter.AssetTransactions(
		ctx,
		policyHex,
		[]byte("T"),
		params,
	)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Len(t, rows, 3)
	require.Equal(t, hex.EncodeToString(mint), rows[0].TxHash)
	require.Equal(t, uint64(1), rows[0].BlockHeight)
	require.Equal(t, hex.EncodeToString(produce), rows[1].TxHash)
	require.Equal(t, uint64(2), rows[1].BlockHeight)
	require.Equal(t, hex.EncodeToString(move), rows[2].TxHash)
	require.Equal(t, uint32(1), rows[2].TxIndex)
	require.Equal(t, uint64(3), rows[2].BlockHeight)
	require.Equal(t, int64(30), rows[2].BlockTime)

	desc := params
	desc.Pagination = PaginationParams{
		Count: 1,
		Page:  2,
		Order: PaginationOrderDesc,
	}
	rows, total, err = adapter.AssetTransactions(
		ctx,
		policyHex,
		[]byte("T"),
		desc,
	)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(produce), rows[0].TxHash)

	params.From = &BlockRangePosition{Block: 3}
	rows, total, err = adapter.AssetTransactions(
		ctx,
		policyHex,
		[]byte("T"),
		params,
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(move), rows[0].TxHash)

	params.From = &BlockRangePosition{Block: 2}
	params.To = &BlockRangePosition{Block: 2}
	rows, total, err = adapter.AssetTransactions(
		ctx,
		policyHex,
		[]byte("T"),
		params,
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Equal(t, hex.EncodeToString(produce), rows[0].TxHash)
	params.To = nil

	params.From = &BlockRangePosition{Block: 50}
	rows, total, err = adapter.AssetTransactions(
		ctx,
		policyHex,
		[]byte("T"),
		params,
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)

	_, _, err = adapter.AssetTransactions(
		ctx,
		policyHex,
		[]byte("unknown"),
		TransactionRangeParams{
			Pagination: PaginationParams{
				Count: 1,
				Page:  1,
				Order: PaginationOrderAsc,
			},
		},
	)
	require.ErrorIs(t, err, ErrAssetNotFound)
}

func TestNodeAdapterAssetUTXOs(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newSlotTimeDBBackedAdapter(t)
	policy := bytes.Repeat([]byte{0xe1}, lcommon.AddressHashSize)
	policyHex := hex.EncodeToString(policy)
	addr, _, _ := testKeyAddress(t, 0xe2, 0)
	held := []models.Asset{{PolicyId: policy, Name: []byte("T"), Amount: 3}}

	// first: an inline-datum output in block 1; second: a plain output in
	// block 3. A spent holding in block 2 is not a current UTxO.
	firstTx := seedAssetTransaction(t, raw, db, fill32(0x01), 1, 10, 0)
	spentTx := seedAssetTransaction(t, raw, db, fill32(0x02), 2, 20, 0)
	secondTx := seedAssetTransaction(t, raw, db, fill32(0x03), 3, 30, 1)
	spender := &models.Transaction{Hash: fill32(0x04), Slot: 40, Valid: true}
	insertAdapterTransaction(t, raw, spender)
	for i, tx := range []*models.Transaction{firstTx, spentTx, secondTx} {
		var spentBy []byte
		if tx == spentTx {
			spentBy = spender.Hash
		}
		insertAdapterUtxo(t, raw, &models.Utxo{
			TransactionID: &tx.ID,
			TxId:          tx.Hash,
			OutputIdx:     0,
			PaymentKey:    addr.PaymentKeyHash().Bytes(),
			AddedSlot:     tx.Slot,
			Amount:        types.Uint64(1_000_000 * uint64(i+1)),
			Assets:        held,
		})
		if spentBy != nil {
			_, err := raw.Exec(
				`UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = 41 WHERE tx_id = ?`,
				spentBy,
				tx.Hash,
			)
			require.NoError(t, err)
		}
	}
	datum := lcommon.Datum{Data: data.NewInteger(big.NewInt(42))}
	datumCbor, err := cbor.Encode(&datum)
	require.NoError(t, err)
	withDatum := decodeBabbageOutputAt(
		t, addr, 1_000_000,
		[]any{1, cbor.Tag{Number: 24, Content: datumCbor}},
	)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), firstTx.Hash, 0, withDatum.Cbor())
	}))
	storePointerOutputCbor(t, db, secondTx.Hash, 0, addr, 3_000_000)
	storePointerOutputCbor(t, db, spentTx.Hash, 0, addr, 2_000_000)

	ctx := context.Background()
	params := TransactionRangeParams{
		Pagination: PaginationParams{
			Count: 10,
			Page:  1,
			Order: PaginationOrderAsc,
		},
	}
	rows, total, err := adapter.AssetUTXOs(ctx, policyHex, []byte("T"), params)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 2)
	require.Equal(t, hex.EncodeToString(firstTx.Hash), rows[0].TxHash)
	require.Equal(t, addr.String(), rows[0].Address)
	require.Equal(t, hex.EncodeToString(firstTx.BlockHash), rows[0].Block)
	require.Equal(t, uint64(1), rows[0].BlockHeight)
	require.Equal(t, int64(10), rows[0].BlockTime)
	require.NotNil(t, rows[0].InlineDatum)
	require.Equal(t, hex.EncodeToString(datumCbor), *rows[0].InlineDatum)
	require.JSONEq(t, `{"int":42}`, string(rows[0].InlineDatumJSON))
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "1000000"},
		{Unit: policyHex + hex.EncodeToString([]byte("T")), Quantity: "3"},
	}, rows[0].Amount)
	require.Equal(t, hex.EncodeToString(secondTx.Hash), rows[1].TxHash)
	require.Nil(t, rows[1].InlineDatum)
	require.Nil(t, rows[1].InlineDatumJSON)
	require.Equal(t, uint64(3), rows[1].BlockHeight)

	desc := params
	desc.Pagination = PaginationParams{
		Count: 1,
		Page:  1,
		Order: PaginationOrderDesc,
	}
	rows, total, err = adapter.AssetUTXOs(ctx, policyHex, []byte("T"), desc)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(secondTx.Hash), rows[0].TxHash)

	// A block range selects on the producing transaction's position.
	require.NoError(t, db.SetTip(ochainsync.Tip{BlockNumber: 3}, nil))
	ranged := params
	ranged.From = &BlockRangePosition{Block: 2}
	rows, total, err = adapter.AssetUTXOs(ctx, policyHex, []byte("T"), ranged)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(secondTx.Hash), rows[0].TxHash)
	require.Equal(t, uint64(3), rows[0].BlockHeight)
	require.Len(t, rows[0].Amount, 2, "assets survive the by-reference fetch")

	ranged.From = nil
	ranged.To = &BlockRangePosition{Block: 1}
	rows, total, err = adapter.AssetUTXOs(ctx, policyHex, []byte("T"), ranged)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Equal(t, hex.EncodeToString(firstTx.Hash), rows[0].TxHash)

	ranged.To = nil
	ranged.From = &BlockRangePosition{Block: 99}
	rows, total, err = adapter.AssetUTXOs(ctx, policyHex, []byte("T"), ranged)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)

	_, _, err = adapter.AssetUTXOs(ctx, policyHex, []byte("other"), params)
	require.ErrorIs(t, err, ErrAssetNotFound)
}

// decodeBabbageOutputAt builds a Babbage-format output (a CBOR map) paying
// addr, with the given datum option.
func decodeBabbageOutputAt(
	t *testing.T,
	addr lcommon.Address,
	amount uint64,
	datumOption any,
) lcommon.TransactionOutput {
	t.Helper()
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	raw, err := cbor.Encode(
		map[int]any{0: addrBytes, 1: amount, 2: datumOption},
	)
	require.NoError(t, err)
	output, err := gledger.NewTransactionOutputFromCbor(raw)
	require.NoError(t, err)
	return output
}
