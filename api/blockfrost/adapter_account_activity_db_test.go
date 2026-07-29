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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDBBackedAccountAdapter builds a NodeAdapter like newDBBackedAdapter
// (adapter_block_db_test.go), but additionally configures a minimal Shelley
// genesis so slot-0 transactions/withdrawals resolve their block_time via
// SlotToTime's genesis short-circuit, without needing a populated epoch
// cache.
func newDBBackedAccountAdapter(
	t *testing.T,
) (*NodeAdapter, *sqliteplugin.MetadataStoreSqlite, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(t, nodeConfig.LoadShelleyGenesisFromReader(
		strings.NewReader(`{"systemStart":"2022-10-25T00:00:00Z"}`),
	))

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		CardanoNodeConfig: nodeConfig,
	})
	require.NoError(t, err)

	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)

	return adapter, store, db
}

// testStakeCredential builds a bech32 reward/stake address and returns it
// alongside the raw 28-byte key-hash staking credential it wraps.
func testStakeCredential(t *testing.T, fill byte) (string, []byte) {
	t.Helper()
	stakeKey := bytes.Repeat([]byte{fill}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	return addr.String(), stakeKey
}

func createTestBlock(
	t *testing.T,
	db *database.Database,
	id uint64,
	hash []byte,
	number uint64,
) {
	t.Helper()
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     id,
		Hash:   hash,
		Slot:   number,
		Number: number,
		Type:   0,
		Cbor:   []byte{byte(number)},
	}, nil))
}

// --- AccountUTXOs ---

func TestNodeAdapterAccountUTXOs(t *testing.T) {
	adapter, store, db := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0xAB)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	addr1, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)
	addr2, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x02}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)

	txID1 := uint(1)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID1, Hash: fill32(0x01), BlockHash: fill32(0xf1), Slot: 0,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TransactionID: &txID1,
		TxId:          fill32(0x01),
		OutputIdx:     0,
		PaymentKey:    addr1.PaymentKeyHash().Bytes(),
		StakingKey:    stakeKey,
		CredentialTag: 0,
		AddedSlot:     0,
		Amount:        types.Uint64(1_000_000),
	}).Error)
	storePointerOutputCbor(t, db, fill32(0x01), 0, addr1, 1_000_000)

	txID2 := uint(2)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID2, Hash: fill32(0x02), BlockHash: fill32(0xf2), Slot: 0,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TransactionID: &txID2,
		TxId:          fill32(0x02),
		OutputIdx:     1,
		PaymentKey:    addr2.PaymentKeyHash().Bytes(),
		StakingKey:    stakeKey,
		CredentialTag: 0,
		AddedSlot:     0,
		Amount:        types.Uint64(2_000_000),
		DatumHash:     fill32(0x99),
	}).Error)
	storePointerOutputCbor(t, db, fill32(0x02), 1, addr2, 2_000_000)

	// A UTxO under a different stake credential must never appear.
	otherStakeKey := bytes.Repeat([]byte{0xCD}, lcommon.AddressHashSize)
	otherAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x03}, lcommon.AddressHashSize),
		otherStakeKey,
	)
	require.NoError(t, err)
	txID3 := uint(3)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID3, Hash: fill32(0x03), BlockHash: fill32(0xf3), Slot: 0,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TransactionID: &txID3,
		TxId:          fill32(0x03),
		OutputIdx:     0,
		PaymentKey:    otherAddr.PaymentKeyHash().Bytes(),
		StakingKey:    otherStakeKey,
		CredentialTag: 0,
		AddedSlot:     0,
		Amount:        types.Uint64(9_999_999),
	}).Error)
	storePointerOutputCbor(t, db, fill32(0x03), 0, otherAddr, 9_999_999)

	utxos, total, err := adapter.AccountUTXOs(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, utxos, 2)

	byTxHash := map[string]AccountUTXOInfo{}
	for _, u := range utxos {
		byTxHash[u.TxHash] = u
	}
	u1 := byTxHash[hex.EncodeToString(fill32(0x01))]
	assert.Equal(t, addr1.String(), u1.Address)
	assert.Equal(t, "1000000", u1.Amount[0].Quantity)
	assert.Equal(t, hex.EncodeToString(fill32(0xf1)), u1.Block)
	assert.Nil(t, u1.DataHash)

	u2 := byTxHash[hex.EncodeToString(fill32(0x02))]
	assert.Equal(t, addr2.String(), u2.Address)
	assert.Equal(t, "2000000", u2.Amount[0].Quantity)
	require.NotNil(t, u2.DataHash)
	assert.Equal(t, hex.EncodeToString(fill32(0x99)), *u2.DataHash)

	// Pagination + reversed order.
	page, total, err := adapter.AccountUTXOs(
		stakeAddress,
		PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, page, 1)
}

func TestNodeAdapterAccountUTXOsEmpty(t *testing.T) {
	adapter, store, _ := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0xEE)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	utxos, total, err := adapter.AccountUTXOs(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, utxos)
}

func TestNodeAdapterAccountUTXOsInvalidStakeAddress(t *testing.T) {
	adapter, _, _ := newDBBackedAccountAdapter(t)

	_, _, err := adapter.AccountUTXOs(
		"not-a-stake-address",
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, ErrInvalidStakeAddress)
}

func TestNodeAdapterAccountUTXOsNotFound(t *testing.T) {
	adapter, _, _ := newDBBackedAccountAdapter(t)

	stakeAddress, _ := testStakeCredential(t, 0x11)
	_, _, err := adapter.AccountUTXOs(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
}

func TestNodeAdapterAccountUTXOsQueryFailure(t *testing.T) {
	adapter, store, _ := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x22)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	require.NoError(t, store.DB().Exec("DROP TABLE utxo").Error)

	_, _, err := adapter.AccountUTXOs(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.Error(t, err)
}

// --- AccountWithdrawals ---

func TestNodeAdapterAccountWithdrawals(t *testing.T) {
	adapter, store, db := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x33)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	createTestBlock(t, db, 1, fill32(0xb1), 100)
	createTestBlock(t, db, 2, fill32(0xb2), 200)

	require.NoError(t, store.DB().Create(&models.Transaction{
		Hash: fill32(0x01), BlockHash: fill32(0xb1), Slot: 0, BlockIndex: 0,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Transaction{
		Hash: fill32(0x02), BlockHash: fill32(0xb2), Slot: 0, BlockIndex: 1,
	}).Error)

	require.NoError(t, store.DB().Create(&models.AccountRewardDelta{
		StakingKey: stakeKey, CredentialTag: 0,
		TxHash: fill32(0x01), Amount: types.Uint64(1000),
		Withdrawal: true, AddedSlot: 0,
	}).Error)
	require.NoError(t, store.DB().Create(&models.AccountRewardDelta{
		StakingKey: stakeKey, CredentialTag: 0,
		TxHash: fill32(0x02), Amount: types.Uint64(2000),
		Withdrawal: true, AddedSlot: 0,
	}).Error)
	// A non-withdrawal delta (credit) for the same credential must be
	// excluded.
	require.NoError(t, store.DB().Create(&models.AccountRewardDelta{
		StakingKey: stakeKey, CredentialTag: 0,
		TxHash: fill32(0x03), Amount: types.Uint64(555),
		Withdrawal: false, AddedSlot: 0,
	}).Error)

	rows, total, err := adapter.AccountWithdrawals(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, rows, 2)
	assert.Equal(t, hex.EncodeToString(fill32(0x01)), rows[0].TxHash)
	assert.Equal(t, "1000", rows[0].Amount)
	assert.Equal(t, int64(100), rows[0].BlockHeight)
	assert.Equal(t, hex.EncodeToString(fill32(0x02)), rows[1].TxHash)
	assert.Equal(t, "2000", rows[1].Amount)
	assert.Equal(t, int64(200), rows[1].BlockHeight)

	// Descending order reverses the two rows.
	desc, total, err := adapter.AccountWithdrawals(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, desc, 2)
	assert.Equal(t, hex.EncodeToString(fill32(0x02)), desc[0].TxHash)

	// Pagination.
	paged, total, err := adapter.AccountWithdrawals(
		stakeAddress,
		PaginationParams{Count: 1, Page: 2, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, paged, 1)
	assert.Equal(t, hex.EncodeToString(fill32(0x02)), paged[0].TxHash)
}

func TestNodeAdapterAccountWithdrawalsEmpty(t *testing.T) {
	adapter, store, _ := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x44)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	rows, total, err := adapter.AccountWithdrawals(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)
}

func TestNodeAdapterAccountWithdrawalsNotFound(t *testing.T) {
	adapter, _, _ := newDBBackedAccountAdapter(t)

	stakeAddress, _ := testStakeCredential(t, 0x55)
	_, _, err := adapter.AccountWithdrawals(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
}

func TestNodeAdapterAccountWithdrawalsQueryFailure(t *testing.T) {
	adapter, store, _ := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x66)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	require.NoError(t, store.DB().Exec("DROP TABLE account_reward_delta").Error)

	_, _, err := adapter.AccountWithdrawals(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.Error(t, err)
}

// --- AccountTransactions ---

func TestNodeAdapterAccountTransactions(t *testing.T) {
	adapter, store, db := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x77)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	createTestBlock(t, db, 1, fill32(0xc1), 100)
	createTestBlock(t, db, 2, fill32(0xc2), 101)
	createTestBlock(t, db, 3, fill32(0xc3), 105)

	addr1, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)
	addr2, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x02}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)

	// tx1 in block 100, output under addr1.
	txID1 := uint(1)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID1, Hash: fill32(0x01), BlockHash: fill32(0xc1),
		Slot: 0, BlockIndex: 0,
		Outputs: []models.Utxo{{
			TxId: fill32(0x01), OutputIdx: 0,
			PaymentKey: addr1.PaymentKeyHash().Bytes(),
			StakingKey: stakeKey, CredentialTag: 0,
			Amount: types.Uint64(1_000_000),
		}},
	}).Error)

	// tx2 in block 101, two outputs under addr1 and addr2 (must yield two
	// distinct rows, not a duplicate of the same association).
	txID2 := uint(2)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID2, Hash: fill32(0x02), BlockHash: fill32(0xc2),
		Slot: 0, BlockIndex: 2,
		Outputs: []models.Utxo{
			{
				TxId: fill32(0x02), OutputIdx: 0,
				PaymentKey: addr1.PaymentKeyHash().Bytes(),
				StakingKey: stakeKey, CredentialTag: 0,
				Amount: types.Uint64(500_000),
			},
			{
				TxId: fill32(0x02), OutputIdx: 1,
				PaymentKey: addr2.PaymentKeyHash().Bytes(),
				StakingKey: stakeKey, CredentialTag: 0,
				Amount: types.Uint64(700_000),
			},
			// A second output to addr1 in the same tx must not duplicate
			// the (address, tx) association already captured above.
			{
				TxId: fill32(0x02), OutputIdx: 2,
				PaymentKey: addr1.PaymentKeyHash().Bytes(),
				StakingKey: stakeKey, CredentialTag: 0,
				Amount: types.Uint64(1),
			},
		},
	}).Error)

	// tx3 in block 105 under a different stake credential; must never
	// appear in results.
	otherStakeKey := bytes.Repeat([]byte{0x88}, lcommon.AddressHashSize)
	txID3 := uint(3)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID3, Hash: fill32(0x03), BlockHash: fill32(0xc3),
		Slot: 0, BlockIndex: 0,
		Outputs: []models.Utxo{{
			TxId: fill32(0x03), OutputIdx: 0,
			PaymentKey: bytes.Repeat([]byte{0x09}, lcommon.AddressHashSize),
			StakingKey: otherStakeKey, CredentialTag: 0,
			Amount: types.Uint64(1),
		}},
	}).Error)
	// Also index the address associations, mirroring what the real
	// indexing path populates for GetTransactionsByAddressKeys/
	// CountTransactionsByAddressKeys to select from.
	for _, at := range []models.AddressTransaction{
		{
			PaymentKey: addr1.PaymentKeyHash().Bytes(), StakingKey: stakeKey,
			CredentialTag: 0, TransactionID: txID1, Slot: 0, TxIndex: 0,
		},
		{
			PaymentKey: addr1.PaymentKeyHash().Bytes(), StakingKey: stakeKey,
			CredentialTag: 0, TransactionID: txID2, Slot: 0, TxIndex: 2,
		},
		{
			PaymentKey: addr2.PaymentKeyHash().Bytes(), StakingKey: stakeKey,
			CredentialTag: 0, TransactionID: txID2, Slot: 0, TxIndex: 2,
		},
		{
			PaymentKey:    bytes.Repeat([]byte{0x09}, lcommon.AddressHashSize),
			StakingKey:    otherStakeKey,
			CredentialTag: 0, TransactionID: txID3, Slot: 0, TxIndex: 0,
		},
	} {
		require.NoError(t, store.DB().Create(&at).Error)
	}

	rows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, rows, 3)

	// Ascending chain order: tx1 (block 100) first, then the two addr1/addr2
	// rows for tx2 (block 101), sorted deterministically by address.
	assert.Equal(t, hex.EncodeToString(fill32(0x01)), rows[0].TxHash)
	assert.Equal(t, addr1.String(), rows[0].Address)
	assert.Equal(t, uint64(100), rows[0].BlockHeight)

	tx2Rows := rows[1:3]
	gotAddrs := []string{tx2Rows[0].Address, tx2Rows[1].Address}
	assert.ElementsMatch(t, []string{addr1.String(), addr2.String()}, gotAddrs)
	for _, r := range tx2Rows {
		assert.Equal(t, hex.EncodeToString(fill32(0x02)), r.TxHash)
		assert.Equal(t, uint64(101), r.BlockHeight)
		assert.Equal(t, uint32(2), r.TxIndex)
	}

	// from=101 excludes tx1 (block 100).
	fromRows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
			From: &BlockRangePosition{Block: 101},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	for _, r := range fromRows {
		assert.NotEqual(t, hex.EncodeToString(fill32(0x01)), r.TxHash)
	}

	// to=100 keeps only tx1.
	toRows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
			To: &BlockRangePosition{Block: 100},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, toRows, 1)
	assert.Equal(t, hex.EncodeToString(fill32(0x01)), toRows[0].TxHash)

	// from=101:3 (past tx2's index of 2) excludes tx2 as well, leaving
	// nothing (tx1 is in block 100, before 101).
	idx3 := uint32(3)
	noneRows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
			From: &BlockRangePosition{Block: 101, Index: &idx3},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, noneRows)

	// Pagination over the expanded (address, tx) row set.
	paged, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 1, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, paged, 1)
}

func TestNodeAdapterAccountTransactionsEmpty(t *testing.T) {
	adapter, store, _ := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x99)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	rows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)
}

func TestNodeAdapterAccountTransactionsNotFound(t *testing.T) {
	adapter, _, _ := newDBBackedAccountAdapter(t)

	stakeAddress, _ := testStakeCredential(t, 0xA1)
	_, _, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
}

func TestNodeAdapterAccountTransactionsQueryFailure(t *testing.T) {
	adapter, store, _ := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0xA2)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	require.NoError(t, store.DB().Exec("DROP TABLE address_transaction").Error)

	_, _, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.Error(t, err)
}
