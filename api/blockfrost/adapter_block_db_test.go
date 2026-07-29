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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"bytes"
	"encoding/hex"
	"io"
	"log/slog"
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDBBackedAdapter builds a NodeAdapter over a real, in-package LedgerState
// backed by an on-disk (temp-dir) database, plus the sqlite metadata store so
// tests can insert transaction/block rows directly. No CardanoNodeConfig is
// supplied because the exercised paths (blockOutputAndFees, nextBlockHash) do
// not perform slot/epoch/time math, and NewLedgerState tolerates a nil config.
func newDBBackedAdapter(
	t *testing.T,
) (*NodeAdapter, *sqliteplugin.MetadataStoreSqlite, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)

	return adapter, store, db
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
	adapter, store, db := newDBBackedAdapter(t)

	paymentHash := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	wantAddr := testPointerAddress(t, paymentHash, 0x01)
	otherAddr := testPointerAddress(t, paymentHash, 0x02)
	policyID := bytes.Repeat([]byte{0xcd}, lcommon.AddressHashSize)
	assetName := []byte("TOKEN")

	transactionID := uint(1)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID:         transactionID,
		Hash:       fill32(0x10),
		Slot:       10,
		BlockIndex: 2,
	}).Error)

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
		require.NoError(t, store.DB().Create(&rows[i]).Error)
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
	adapter, store, _ := newDBBackedAdapter(t)

	paymentHash := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	addr := testPointerAddress(t, paymentHash, 0x01)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       fill32(0x40),
		OutputIdx:  0,
		PaymentKey: paymentHash,
		AddedSlot:  20,
		Amount:     types.Uint64(2_000_000),
	}).Error)

	_, err := adapter.Address(addr.String())
	require.ErrorContains(t, err, "utxo cbor unavailable")
}

func TestNodeAdapterEnterpriseAddressExcludesPointerUtxos(t *testing.T) {
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
		require.NoError(t, store.DB().Create(&models.Transaction{
			ID:         txID,
			Hash:       txHash,
			BlockHash:  fill32(0xf0),
			Slot:       uint64(i + 1),
			BlockIndex: uint32(i),
		}).Error)
		require.NoError(t, store.DB().Create(&models.Utxo{
			TransactionID: &txID,
			TxId:          txHash,
			OutputIdx:     0,
			PaymentKey:    paymentHash,
			AddedSlot:     uint64(i + 1),
			Amount:        types.Uint64((i + 1) * 1_000_000),
		}).Error)
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
	require.NoError(t, store.DB().Create(validTx).Error)

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
	require.NoError(t, store.DB().Create(invalidTx).Error)

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
		require.NoError(t, store.DB().Create(tx).Error)
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
	require.NoError(t, store.DB().Create(pool).Error)

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
	require.NoError(
		t, store.DB().Exec("DROP TABLE offchain_metadata").Error,
	)
	_, err = adapter.PoolMetadata(poolID)
	require.ErrorContains(t, err, "get offchain metadata")
}
