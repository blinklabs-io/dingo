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
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedStakeCredentialUtxos registers a stake credential's account row, then
// seeds numUtxos live UTxOs against it (each at a distinct payment address
// and slot, so ascending/descending order is unambiguous), returning the
// bech32 stake address and the underlying staking key bytes.
func seedStakeCredentialUtxos(
	t *testing.T,
	adapter *NodeAdapter,
	raw *sql.DB,
	db *database.Database,
	numUtxos int,
) (string, []byte) {
	t.Helper()
	stakeKey := bytes.Repeat([]byte{0x77}, lcommon.AddressHashSize)
	stakeAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		adapter.ledgerState.Database().CreateAccount(
			context.Background(),
			nil,
			&models.Account{StakingKey: stakeKey, Active: true},
		),
	)

	for i := range numUtxos {
		payment := make([]byte, lcommon.AddressHashSize)
		binary.BigEndian.PutUint32(payment, uint32(i)+1)
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
		slot := uint64(i) + 1
		amount := (uint64(i) + 1) * 1_000_000
		insertAdapterTransaction(t, raw, &models.Transaction{
			Hash:       txHash,
			Slot:       slot,
			BlockIndex: 0,
			Outputs: []models.Utxo{{
				TxId:       txHash,
				OutputIdx:  0,
				PaymentKey: payment,
				StakingKey: stakeKey,
				AddedSlot:  slot,
				Amount:     types.Uint64(amount),
			}},
		})
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			lcommon.AddressNetworkTestnet,
			payment,
			stakeKey,
		)
		require.NoError(t, err)
		storePointerOutputCbor(t, db, txHash, 0, addr, amount)
	}
	return stakeAddr.String(), stakeKey
}

// TestNodeAdapterAccountUTXOsLargeResultSetPagination proves AccountUTXOs
// pages a large stake account's UTxOs without materializing more than the
// requested window (see dingo/3520): it exercises a large result set, an
// ascending and a descending page, and an out-of-range page landing past
// the end.
func TestNodeAdapterAccountUTXOsLargeResultSetPagination(t *testing.T) {
	t.Parallel()

	adapter, raw, db := newDBBackedAdapter(t)
	const total = 250
	stakeAddr, _ := seedStakeCredentialUtxos(t, adapter, raw, db, total)

	t.Run("ascending page stops at the requested window", func(t *testing.T) {
		items, gotTotal, err := adapter.AccountUTXOs(
			stakeAddr,
			PaginationParams{Count: 10, Page: 3, Order: PaginationOrderAsc},
		)
		require.NoError(t, err)
		assert.Equal(t, total, gotTotal)
		require.Len(t, items, 10)
		// Page 3 with count 10 is items 21..30 (slot order == insertion
		// order); item 21's amount is (21 * 1_000_000).
		assert.Equal(t, "21000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "30000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run("descending page returns newest first", func(t *testing.T) {
		items, gotTotal, err := adapter.AccountUTXOs(
			stakeAddr,
			PaginationParams{Count: 10, Page: 1, Order: PaginationOrderDesc},
		)
		require.NoError(t, err)
		assert.Equal(t, total, gotTotal)
		require.Len(t, items, 10)
		assert.Equal(t, "250000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "241000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run(
		"a page past the end is empty but reports the real total",
		func(t *testing.T) {
			items, gotTotal, err := adapter.AccountUTXOs(
				stakeAddr,
				PaginationParams{
					Count: 100,
					Page:  4,
					Order: PaginationOrderAsc,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, total, gotTotal)
			assert.Empty(t, items)
		},
	)

	t.Run(
		"a page far beyond the address history is empty, not an error",
		func(t *testing.T) {
			items, gotTotal, err := adapter.AccountUTXOs(
				stakeAddr,
				PaginationParams{
					Count: MaxPaginationCount,
					Page:  MaxPaginationPage,
					Order: PaginationOrderAsc,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, total, gotTotal)
			assert.Empty(t, items)
		},
	)
}

// TestNodeAdapterAccountUTXOsEmpty proves a registered stake credential with
// no live UTxOs returns an empty page and a zero total rather than an error.
func TestNodeAdapterAccountUTXOsEmpty(t *testing.T) {
	t.Parallel()

	adapter, raw, db := newDBBackedAdapter(t)
	stakeAddr, _ := seedStakeCredentialUtxos(t, adapter, raw, db, 0)

	items, total, err := adapter.AccountUTXOs(
		stakeAddr,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Zero(t, total)
	assert.Empty(t, items)
}

// TestNodeAdapterAddressUTXOsLargeResultSetPagination seeds one address with
// a large number of live UTxOs and proves ascending and descending
// pagination both return the correct window. It exercises the windowed
// reverse in NodeAdapter.AddressUTXOs (adapter.go): descending pagination
// used to swap-reverse the address's entire UTxO history before slicing out
// a page; reversing only the requested window must produce the identical
// ordering for a large result set (see dingo/3520).
func TestNodeAdapterAddressUTXOsLargeResultSetPagination(t *testing.T) {
	t.Parallel()

	adapter, raw, db := newDBBackedAdapter(t)

	payment := bytes.Repeat([]byte{0x99}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)

	const total = 250
	for i := range total {
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
		slot := uint64(i) + 1
		amount := (uint64(i) + 1) * 1_000_000
		insertAdapterTransaction(t, raw, &models.Transaction{
			Hash:       txHash,
			Slot:       slot,
			BlockIndex: 0,
			Outputs: []models.Utxo{{
				TxId:       txHash,
				OutputIdx:  0,
				PaymentKey: payment,
				AddedSlot:  slot,
				Amount:     types.Uint64(amount),
			}},
		})
		storePointerOutputCbor(t, db, txHash, 0, addr, amount)
	}

	t.Run("ascending page stops at the requested window", func(t *testing.T) {
		items, total, err := adapter.AddressUTXOs(
			addr.String(),
			PaginationParams{Count: 10, Page: 3, Order: PaginationOrderAsc},
		)
		require.NoError(t, err)
		assert.Equal(t, 250, total)
		require.Len(t, items, 10)
		assert.Equal(t, "21000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "30000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run("descending page matches a full-history reverse", func(t *testing.T) {
		items, total, err := adapter.AddressUTXOs(
			addr.String(),
			PaginationParams{Count: 10, Page: 5, Order: PaginationOrderDesc},
		)
		require.NoError(t, err)
		assert.Equal(t, 250, total)
		require.Len(t, items, 10)
		// Descending page 5 (count 10) is the 41st-newest through the
		// 50th-newest UTxO: amounts 210000000 down to 201000000.
		assert.Equal(t, "210000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "201000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run("descending page past the end is empty", func(t *testing.T) {
		items, total, err := adapter.AddressUTXOs(
			addr.String(),
			PaginationParams{Count: 10, Page: 26, Order: PaginationOrderDesc},
		)
		require.NoError(t, err)
		assert.Equal(t, 250, total)
		assert.Empty(t, items)
	})
}

// TestNodeAdapterAddressUTXOsAssetsSurviveRefFetch proves native assets
// still attach to the returned page: AddressUTXOs now resolves its total
// via a reference-only scan (no assets loaded) and fetches full rows for
// just the requested page via UtxosByRefs, a different path than before
// (see dingo/3520) that must not drop asset data along the way.
func TestNodeAdapterAddressUTXOsAssetsSurviveRefFetch(t *testing.T) {
	t.Parallel()

	adapter, store, db := newDBBackedAdapter(t)

	payment := bytes.Repeat([]byte{0x33}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	policyID := bytes.Repeat([]byte{0x44}, lcommon.AddressHashSize)
	assetName := []byte("TOKEN")

	txHash := fill32(0x50)
	insertAdapterTransaction(t, store, &models.Transaction{
		Hash: txHash,
		Slot: 1,
		Outputs: []models.Utxo{{
			TxId:       txHash,
			OutputIdx:  0,
			PaymentKey: payment,
			AddedSlot:  1,
			Amount:     types.Uint64(1_000_000),
			Assets: []models.Asset{{
				PolicyId: policyID,
				Name:     assetName,
				Amount:   types.Uint64(9),
			}},
		}},
	})
	storePointerOutputCbor(t, db, txHash, 0, addr, 1_000_000)

	items, total, err := adapter.AddressUTXOs(
		addr.String(),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, items, 1)
	require.Len(t, items[0].Amount, 2)
	assert.Equal(t, "lovelace", items[0].Amount[0].Unit)
	assert.Equal(
		t,
		hex.EncodeToString(policyID)+hex.EncodeToString(assetName),
		items[0].Amount[1].Unit,
	)
	assert.Equal(t, "9", items[0].Amount[1].Quantity)
}

// newDBBackedAdapter builds a NodeAdapter over a real, in-package LedgerState
// backed by an on-disk (temp-dir) database, plus the sqlite metadata store so
// tests can insert transaction/block rows directly. A CardanoNodeConfig is
// optional; callers supply one when exercising network-specific behavior.
func newDBBackedAdapter(
	t *testing.T,
	nodeConfig ...*cardano.CardanoNodeConfig,
) (*NodeAdapter, *sql.DB, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)

	lsConfig := ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	if len(nodeConfig) > 0 {
		lsConfig.CardanoNodeConfig = nodeConfig[0]
	}
	ls, err := ledger.NewLedgerState(lsConfig)
	require.NoError(t, err)

	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)

	return adapter, raw, db
}

func insertAdapterUtxo(
	t *testing.T,
	raw *sql.DB,
	utxo *models.Utxo,
) {
	t.Helper()
	result, err := raw.Exec(`
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, added_slot, deleted_slot, amount, output_idx,
    payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		utxo.TransactionID,
		utxo.CollateralReturnForTxID,
		utxo.TxId,
		utxo.PaymentKey,
		utxo.StakingKey,
		utxo.CredentialTag,
		utxo.AddedSlot,
		utxo.DeletedSlot,
		strconv.FormatUint(uint64(utxo.Amount), 10),
		utxo.OutputIdx,
		utxo.PaymentScript,
	)
	require.NoError(t, err)
	utxoID, err := result.LastInsertId()
	require.NoError(t, err)
	for i := range utxo.Assets {
		asset := &utxo.Assets[i]
		_, err = raw.Exec(`
INSERT INTO asset (name, policy_id, fingerprint, utxo_id, amount)
VALUES (?, ?, ?, ?, ?)`,
			asset.Name,
			asset.PolicyId,
			asset.Fingerprint,
			utxoID,
			strconv.FormatUint(uint64(asset.Amount), 10),
		)
		require.NoError(t, err)
	}
}

func insertAdapterTransaction(
	t *testing.T,
	raw *sql.DB,
	tx *models.Transaction,
) {
	t.Helper()
	var id any
	if tx.ID != 0 {
		id = tx.ID
	}
	result, err := raw.Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, metadata, slot, type, fee, collateral_fee,
    ttl, block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		tx.Hash,
		tx.BlockHash,
		tx.Metadata,
		tx.Slot,
		tx.Type,
		strconv.FormatUint(uint64(tx.Fee), 10),
		strconv.FormatUint(uint64(tx.CollateralFee), 10),
		strconv.FormatUint(uint64(tx.TTL), 10),
		tx.BlockIndex,
		tx.Valid,
	)
	require.NoError(t, err)
	if tx.ID == 0 {
		lastID, err := result.LastInsertId()
		require.NoError(t, err)
		tx.ID = uint(lastID)
	}
	for i := range tx.Outputs {
		tx.Outputs[i].TransactionID = &tx.ID
		insertAdapterUtxo(t, raw, &tx.Outputs[i])
	}
	if tx.CollateralReturn != nil {
		tx.CollateralReturn.CollateralReturnForTxID = &tx.ID
		insertAdapterUtxo(t, raw, tx.CollateralReturn)
	}
}

// fill32 returns a 32-byte slice filled with b, used for distinct hash/ID
// values in tests.
func fill32(b byte) []byte {
	return bytes.Repeat([]byte{b}, 32)
}

func testPointerAddress(
	t *testing.T,
	paymentHash []byte,
	pointer byte,
) lcommon.Address {
	t.Helper()
	addrBytes := []byte{
		(lcommon.AddressTypeKeyPointer << 4) |
			lcommon.AddressNetworkTestnet,
	}
	addrBytes = append(addrBytes, paymentHash...)
	addrBytes = append(addrBytes, pointer, 0x00, 0x00)
	addr, err := lcommon.NewAddressFromBytes(addrBytes)
	require.NoError(t, err)
	return addr
}

func storePointerOutputCbor(
	t *testing.T,
	db *database.Database,
	txID []byte,
	outputIdx uint32,
	addr lcommon.Address,
	amount uint64,
) {
	t.Helper()
	raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  amount,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txID, outputIdx, raw)
	}))
}

func TestNodeAdapterAddressPointerIncludesSnapshotUtxos(t *testing.T) {
	t.Parallel()

	adapter, store, db := newDBBackedAdapter(t)

	paymentHash := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	wantAddr := testPointerAddress(t, paymentHash, 0x01)
	otherAddr := testPointerAddress(t, paymentHash, 0x02)
	policyID := bytes.Repeat([]byte{0xcd}, lcommon.AddressHashSize)
	assetName := []byte("TOKEN")

	transactionID := uint(1)
	insertAdapterTransaction(t, store, &models.Transaction{
		ID:         transactionID,
		Hash:       fill32(0x10),
		Slot:       10,
		BlockIndex: 2,
	})

	rows := []models.Utxo{
		{
			TransactionID: &transactionID,
			TxId:          fill32(0x10),
			OutputIdx:     0,
			PaymentKey:    paymentHash,
			AddedSlot:     10,
			Amount:        types.Uint64(1_000_000),
			Assets: []models.Asset{{
				PolicyId: policyID,
				Name:     assetName,
				Amount:   types.Uint64(2),
			}},
		},
		{
			// Mithril snapshot imports intentionally have no TransactionID.
			TxId:       fill32(0x20),
			OutputIdx:  0,
			PaymentKey: paymentHash,
			AddedSlot:  20,
			Amount:     types.Uint64(2_000_000),
			Assets: []models.Asset{{
				PolicyId: policyID,
				Name:     assetName,
				Amount:   types.Uint64(3),
			}},
		},
		{
			// A different pointer payload sharing the payment credential must
			// remain excluded by the full decoded-address comparison.
			TxId:       fill32(0x30),
			OutputIdx:  0,
			PaymentKey: paymentHash,
			AddedSlot:  20,
			Amount:     types.Uint64(4_000_000),
			Assets: []models.Asset{{
				PolicyId: policyID,
				Name:     assetName,
				Amount:   types.Uint64(7),
			}},
		},
	}
	for i := range rows {
		insertAdapterUtxo(t, store, &rows[i])
	}
	storePointerOutputCbor(
		t, db, rows[0].TxId, rows[0].OutputIdx, wantAddr, 1_000_000,
	)
	storePointerOutputCbor(
		t, db, rows[1].TxId, rows[1].OutputIdx, wantAddr, 2_000_000,
	)
	storePointerOutputCbor(
		t, db, rows[2].TxId, rows[2].OutputIdx, otherAddr, 4_000_000,
	)

	info, err := adapter.Address(wantAddr.String())
	require.NoError(t, err)
	require.Len(t, info.Amount, 2)
	assert.Equal(t, "lovelace", info.Amount[0].Unit)
	assert.Equal(t, "3000000", info.Amount[0].Quantity)
	assert.Equal(
		t,
		hex.EncodeToString(policyID)+hex.EncodeToString(assetName),
		info.Amount[1].Unit,
	)
	assert.Equal(t, "5", info.Amount[1].Quantity)
}

func TestNodeAdapterAddressPointerRejectsMissingCandidateCbor(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	paymentHash := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	addr := testPointerAddress(t, paymentHash, 0x01)
	insertAdapterUtxo(t, store, &models.Utxo{
		TxId:       fill32(0x40),
		OutputIdx:  0,
		PaymentKey: paymentHash,
		AddedSlot:  20,
		Amount:     types.Uint64(2_000_000),
	})

	_, err := adapter.Address(addr.String())
	require.ErrorContains(t, err, "utxo cbor unavailable")
}

func TestNodeAdapterEnterpriseAddressExcludesPointerUtxos(t *testing.T) {
	t.Parallel()

	adapter, store, db := newDBBackedAdapter(t)

	paymentHash := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash,
		nil,
	)
	require.NoError(t, err)
	pointer := testPointerAddress(t, paymentHash, 0x01)
	addresses := []lcommon.Address{enterprise, pointer}

	for i := range addresses {
		txID := uint(i + 1)
		txHash := fill32(byte(i + 1))
		insertAdapterTransaction(t, store, &models.Transaction{
			ID:         txID,
			Hash:       txHash,
			BlockHash:  fill32(0xf0),
			Slot:       uint64(i + 1),
			BlockIndex: uint32(i),
		})
		insertAdapterUtxo(t, store, &models.Utxo{
			TransactionID: &txID,
			TxId:          txHash,
			OutputIdx:     0,
			PaymentKey:    paymentHash,
			AddedSlot:     uint64(i + 1),
			Amount:        types.Uint64((i + 1) * 1_000_000),
		})
		raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: addresses[i],
			OutputAmount:  uint64((i + 1) * 1_000_000),
		})
		require.NoError(t, err)
		require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
			return db.Blob().SetUtxo(txn.Blob(), txHash, 0, raw)
		}))
	}

	info, err := adapter.Address(enterprise.String())
	require.NoError(t, err)
	require.Len(t, info.Amount, 1)
	assert.Equal(t, "1000000", info.Amount[0].Quantity)

	utxos, total, err := adapter.AddressUTXOs(
		enterprise.String(),
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Len(t, utxos, 1)
	assert.Equal(t, 1, total)
	assert.Equal(t, hex.EncodeToString(fill32(0x01)), utxos[0].TxHash)
}

// TestNodeAdapterBlockOutputAndFees exercises the real DB aggregation path,
// including the phase-2 invalid transaction branch where the collateral return
// (not the discarded outputs) counts toward block output.
func TestNodeAdapterBlockOutputAndFees(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	blockHash := fill32(0xab)

	// Valid transaction: fee 100, outputs 1000 + 2000 (both count).
	validTx := &models.Transaction{
		Hash:       fill32(0x01),
		BlockHash:  blockHash,
		BlockIndex: 0,
		Valid:      true,
		Fee:        types.Uint64(100),
		Outputs: []models.Utxo{
			{TxId: fill32(0x01), OutputIdx: 0, Amount: types.Uint64(1000)},
			{TxId: fill32(0x01), OutputIdx: 1, Amount: types.Uint64(2000)},
		},
	}
	insertAdapterTransaction(t, store, validTx)

	// Invalid transaction: fee 50, its outputs (9999) are discarded and the
	// collateral return (500) is what actually reaches the chain.
	invalidTx := &models.Transaction{
		Hash:       fill32(0x02),
		BlockHash:  blockHash,
		BlockIndex: 1,
		Valid:      false,
		Fee:        types.Uint64(50),
		Outputs: []models.Utxo{
			{TxId: fill32(0x02), OutputIdx: 0, Amount: types.Uint64(9999)},
		},
		CollateralReturn: &models.Utxo{
			TxId:      fill32(0x03),
			OutputIdx: 0,
			Amount:    types.Uint64(500),
		},
	}
	insertAdapterTransaction(t, store, invalidTx)

	output, fees, err := adapter.blockOutputAndFees(blockHash)
	require.NoError(t, err)
	// output = 1000 + 2000 (valid) + 500 (collateral return, NOT 9999) = 3500
	assert.Equal(t, "3500", output)
	// fees = 100 + 50 = 150
	assert.Equal(t, "150", fees)
}

// TestNodeAdapterBlockOutputAndFeesEmpty verifies a block with no transactions
// aggregates to zero rather than erroring.
func TestNodeAdapterBlockOutputAndFeesEmpty(t *testing.T) {
	t.Parallel()

	adapter, _, _ := newDBBackedAdapter(t)

	output, fees, err := adapter.blockOutputAndFees(fill32(0xcd))
	require.NoError(t, err)
	assert.Equal(t, "0", output)
	assert.Equal(t, "0", fees)
}

// TestNodeAdapterBlockOutputAndFeesNoOverflow guards the big.Int accumulation:
// summing amounts/fees whose total exceeds uint64 must produce the true total
// rather than silently wrapping.
func TestNodeAdapterBlockOutputAndFeesNoOverflow(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	blockHash := fill32(0xef)

	maxU64 := uint64(math.MaxUint64)

	// Two transactions, each with the maximum fee and a maximum-value output.
	// The per-field values are valid uint64, but their sums are not.
	for i, b := range []byte{0x21, 0x22} {
		tx := &models.Transaction{
			Hash:       fill32(b),
			BlockHash:  blockHash,
			BlockIndex: uint32(i),
			Valid:      true,
			Fee:        types.Uint64(maxU64),
			Outputs: []models.Utxo{
				{TxId: fill32(b), OutputIdx: 0, Amount: types.Uint64(maxU64)},
			},
		}
		insertAdapterTransaction(t, store, tx)
	}

	// Expected total = 2 * MaxUint64 for both output and fees.
	want := new(big.Int).Mul(
		new(big.Int).SetUint64(maxU64),
		big.NewInt(2),
	).String()

	output, fees, err := adapter.blockOutputAndFees(blockHash)
	require.NoError(t, err)
	assert.Equal(t, want, output)
	assert.Equal(t, want, fees)
}

// TestNodeAdapterNextBlockHash covers the successor lookup against real block
// index entries: a middle block resolves to its successor's hash, the tip block
// resolves to nil (short-circuit), and a block whose successor index is absent
// resolves to nil via ErrBlockNotFound.
func TestNodeAdapterNextBlockHash(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)

	// Three consecutive blocks at Cardano heights 0, 1, 2. Dingo's blob index
	// is 1-based (BlockInitialIndex), so height H lives at index H+1.
	hashes := map[uint64][]byte{
		0: fill32(0x10),
		1: fill32(0x11),
		2: fill32(0x12),
	}
	for height := uint64(0); height <= 2; height++ {
		require.NoError(t, db.BlockCreate(models.Block{
			Hash:   hashes[height],
			Slot:   height * 10,
			Number: height,
			ID:     height + database.BlockInitialIndex,
			Type:   0,
			Cbor:   []byte{byte(height)},
		}, nil))
	}

	const tipHeight = uint64(2)

	// Middle block (height 0) -> successor at height 1.
	next, err := adapter.nextBlockHash(0, tipHeight)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, hex.EncodeToString(hashes[1]), *next)

	// Height 1 -> successor at height 2.
	next, err = adapter.nextBlockHash(1, tipHeight)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, hex.EncodeToString(hashes[2]), *next)

	// Tip block (height == tipHeight) has no successor: short-circuit to nil
	// without a DB lookup.
	next, err = adapter.nextBlockHash(tipHeight, tipHeight)
	require.NoError(t, err)
	assert.Nil(t, next)

	// A non-tip height whose successor index is absent (gap in storage) must
	// resolve to nil via the ErrBlockNotFound path, not surface an error.
	next, err = adapter.nextBlockHash(50, 100)
	require.NoError(t, err)
	assert.Nil(t, next)
}

// TestNodeAdapterPoolMetadataOffchainStoreError guards the error path at the
// PoolMetadata boundary: a failing off-chain metadata store query must
// propagate as an error rather than degrade into a successful URL/hash-only
// response that hides the store failure.
func TestNodeAdapterPoolMetadataOffchainStoreError(t *testing.T) {
	t.Parallel()

	adapter, store, _ := newDBBackedAdapter(t)

	poolKeyHash := bytes.Repeat([]byte{0x0a}, 28)
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		Registration: []models.PoolRegistration{
			{
				PoolKeyHash:  poolKeyHash,
				MetadataUrl:  "https://example.com/pool.json",
				MetadataHash: fill32(0x0b),
				AddedSlot:    1,
			},
		},
	}
	result, err := store.Exec(`
INSERT INTO pool (pool_key_hash) VALUES (?)`,
		pool.PoolKeyHash,
	)
	require.NoError(t, err)
	poolIDValue, err := result.LastInsertId()
	require.NoError(t, err)
	_, err = store.Exec(`
INSERT INTO pool_registration (
    pool_id, pool_key_hash, metadata_url, metadata_hash, added_slot
) VALUES (?, ?, ?, ?, ?)`,
		poolIDValue,
		pool.Registration[0].PoolKeyHash,
		pool.Registration[0].MetadataUrl,
		pool.Registration[0].MetadataHash,
		pool.Registration[0].AddedSlot,
	)
	require.NoError(t, err)

	poolID := hex.EncodeToString(poolKeyHash)

	// Sanity check: with an intact store and no cached document, the lookup
	// succeeds as a URL/hash-only partial response.
	info, err := adapter.PoolMetadata(poolID)
	require.NoError(t, err)
	require.NotNil(t, info.URL)
	assert.Equal(t, "https://example.com/pool.json", *info.URL)
	assert.Nil(t, info.Name)

	// Break the store so GetOffchainMetadata fails; the failure must surface
	// instead of producing a successful partial response.
	_, err = store.Exec("DROP TABLE offchain_metadata")
	require.NoError(t, err)
	_, err = adapter.PoolMetadata(poolID)
	require.ErrorContains(t, err, "get offchain metadata")
}

func newRewardHistoryStakeAddress(
	t *testing.T,
	stakingKeyHash []byte,
) string {
	t.Helper()
	address, err := stakeAddressFromCredential(
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(stakingKeyHash),
		},
		lcommon.AddressNetworkTestnet,
	)
	require.NoError(t, err)
	return address
}

func TestAccountRewardHistoryExcludesNonSpendableReward(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)
	stakingKey := bytes.Repeat([]byte{0x07}, 28)
	poolKey := bytes.Repeat([]byte{0xff}, 28)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			CredentialTag: 0,
			StakingKey:    stakingKey,
			Active:        true,
		}),
	)
	require.NoError(
		t,
		db.Metadata().SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         10,
				CredentialTag: 0,
				StakingKey:    stakingKey,
				PoolKeyHash:   poolKey,
				RewardType:    "member",
				Amount:        1_000_000,
				Spendable:     true,
			},
			{
				Epoch:         11,
				CredentialTag: 0,
				StakingKey:    stakingKey,
				PoolKeyHash:   poolKey,
				RewardType:    "member",
				Amount:        9_999_999,
				Spendable:     false,
			},
		}, nil),
	)

	rows, total, err := adapter.AccountRewardHistory(
		newRewardHistoryStakeAddress(t, stakingKey),
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, "1000000", rows[0].Amount)
}

func TestAccountRewardHistoryExcludesGuardedReward(t *testing.T) {
	t.Parallel()

	adapter, _, db := newDBBackedAdapter(t)
	stakingKey := bytes.Repeat([]byte{0x08}, 28)
	poolKey := bytes.Repeat([]byte{0xfe}, 28)
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			CredentialTag: 0,
			StakingKey:    stakingKey,
			Active:        true,
		}),
	)
	require.NoError(
		t,
		db.Metadata().SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         10,
				CredentialTag: 0,
				StakingKey:    stakingKey,
				PoolKeyHash:   poolKey,
				RewardType:    "member",
				Amount:        1_000_000,
				Spendable:     true,
			},
			{
				Epoch:         11,
				CredentialTag: 0,
				StakingKey:    stakingKey,
				PoolKeyHash:   poolKey,
				RewardType:    "leader",
				Amount:        9_999_999,
				Spendable:     true,
				Guarded:       true,
			},
		}, nil),
	)

	rows, total, err := adapter.AccountRewardHistory(
		newRewardHistoryStakeAddress(t, stakingKey),
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, "1000000", rows[0].Amount)
}

const testDatumAddr = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

// decodeBabbageOutput builds a Babbage (map-form) transaction output CBOR with
// the optional datum option and reference script, then decodes it through the
// same path the adapter uses (NewTransactionOutputFromCbor). This mirrors both
// live sync and API-mode historical backfill, where inline datum and reference
// script are recovered from output CBOR rather than persisted metadata columns.
func decodeBabbageOutput(
	t *testing.T,
	datumOption any,
	scriptRef *lcommon.ScriptRef,
) lcommon.TransactionOutput {
	t.Helper()
	addr, err := lcommon.NewAddress(testDatumAddr)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)

	outputMap := map[int]any{
		0: addrBytes,
		1: uint64(1_000_000),
	}
	if datumOption != nil {
		outputMap[2] = datumOption
	}
	if scriptRef != nil {
		outputMap[3] = scriptRef
	}
	cborBytes, err := cbor.Encode(outputMap)
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionOutputFromCbor(cborBytes)
	require.NoError(t, err)
	return decoded
}

func TestUtxoDatumAndScriptRefInlineAndScript(t *testing.T) {
	t.Parallel()

	// Inline datum option is [1, 24(<datum cbor>)] per CIP-0032.
	datum := lcommon.Datum{Data: data.NewInteger(big.NewInt(42))}
	datumCbor, err := cbor.Encode(&datum)
	require.NoError(t, err)
	datumOption := []any{
		1,
		cbor.Tag{Number: 24, Content: datumCbor},
	}

	script := lcommon.PlutusV2Script([]byte{0x01, 0x02, 0x03, 0x04})
	scriptRef := &lcommon.ScriptRef{
		Type:   lcommon.ScriptRefTypePlutusV2,
		Script: script,
	}

	output := decodeBabbageOutput(t, datumOption, scriptRef)

	inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
	require.NotNil(t, inlineDatum)
	assert.Equal(t, hex.EncodeToString(datumCbor), *inlineDatum)
	require.NotNil(t, referenceScriptHash)
	assert.Equal(
		t,
		hex.EncodeToString(script.Hash().Bytes()),
		*referenceScriptHash,
	)
}

func TestUtxoDatumAndScriptRefDatumHashOnly(t *testing.T) {
	t.Parallel()

	// A datum-hash-only output (option [0, <hash>]) carries no inline datum and
	// no reference script, so both Blockfrost fields must be nil (data_hash is
	// populated elsewhere from the persisted column).
	datumHash := lcommon.NewBlake2b256(make([]byte, 32))
	datumOption := []any{0, datumHash}

	output := decodeBabbageOutput(t, datumOption, nil)

	inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
	assert.Nil(t, inlineDatum)
	assert.Nil(t, referenceScriptHash)
}

func TestUtxoDatumAndScriptRefNone(t *testing.T) {
	t.Parallel()

	output := decodeBabbageOutput(t, nil, nil)
	inlineDatum, referenceScriptHash := utxoDatumAndScriptRef(output)
	assert.Nil(t, inlineDatum)
	assert.Nil(t, referenceScriptHash)
}
