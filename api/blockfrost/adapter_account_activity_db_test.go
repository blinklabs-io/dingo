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
// genesis and a single wide-covering epoch (0..10,000,000 slots) so
// SlotToTime resolves any slot used by these tests, not just slot 0 (the
// genesis short-circuit), without needing the full epoch-boundary/nonce
// machinery a real epoch cache would otherwise require. ls.PrepareEpochCacheForStartup
// only works before LedgerState.Start(), which is never called here.
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

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)
	require.NoError(t, store.DB().Create(&models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1000,
		LengthInSlots: 10_000_000,
	}).Error)
	require.NoError(t, ls.PrepareEpochCacheForStartup())

	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)

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

// createTestBlock creates a block at the given Cardano height (Number).
// The blob-store index ID must be height + database.BlockInitialIndex to
// match how BlockByIndex/BlockAtOrAfterIndex resolve a Blockfrost block
// number to a block (see resolveBlockRangeBound in
// adapter_account_activity.go and nextBlockHash in adapter.go).
func createTestBlock(
	t *testing.T,
	db *database.Database,
	hash []byte,
	number uint64,
) {
	t.Helper()
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     number + database.BlockInitialIndex,
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

	createTestBlock(t, db, fill32(0xb1), 100)
	createTestBlock(t, db, fill32(0xb2), 200)

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

// createAccountTransactionFixture creates a Transaction row (with an
// explicit ID) and its corresponding AddressTransaction association row
// under the given stake credential and payment key, mirroring what the
// real indexer populates for one (payment address, transaction) pair.
// slot/txIndex are shared by both rows exactly as production indexing
// does, since the account-transactions query filters and orders using
// address_transaction's own (slot, tx_index) columns.
func createAccountTransactionFixture(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	txID uint,
	txHash []byte,
	blockHash []byte,
	slot uint64,
	txIndex uint32,
	paymentKey []byte,
	stakeKey []byte,
	credentialTag uint8,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: txID, Hash: txHash, BlockHash: blockHash,
		Slot: slot, BlockIndex: txIndex,
	}).Error)
	addAccountTransactionAssociation(
		t, store, txID, slot, txIndex, paymentKey, stakeKey, credentialTag,
	)
}

// addAccountTransactionAssociation creates one more AddressTransaction
// association row for a transaction that already exists (e.g. a second
// output of the same transaction paying a different address under the
// same stake credential), without re-creating the Transaction row (whose
// hash is unique).
func addAccountTransactionAssociation(
	t *testing.T,
	store *sqliteplugin.MetadataStoreSqlite,
	txID uint,
	slot uint64,
	txIndex uint32,
	paymentKey []byte,
	stakeKey []byte,
	credentialTag uint8,
) {
	t.Helper()
	require.NoError(t, store.DB().Create(&models.AddressTransaction{
		PaymentKey: paymentKey, StakingKey: stakeKey,
		CredentialTag: credentialTag, TransactionID: txID,
		Slot: slot, TxIndex: txIndex,
	}).Error)
}

func TestNodeAdapterAccountTransactions(t *testing.T) {
	adapter, store, db := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x77)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	// Blocks at heights 100, 101, and 105; slot mirrors height for
	// readability (only their relative order matters here).
	createTestBlock(t, db, fill32(0xc1), 100)
	createTestBlock(t, db, fill32(0xc2), 101)
	createTestBlock(t, db, fill32(0xc3), 105)

	addr1Payment := bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize)
	addr2Payment := bytes.Repeat([]byte{0x02}, lcommon.AddressHashSize)
	addr1, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		addr1Payment,
		stakeKey,
	)
	require.NoError(t, err)
	addr2, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		addr2Payment,
		stakeKey,
	)
	require.NoError(t, err)

	// A UTxO row per payment key backs the GetUtxoPaymentScriptByCredential
	// lookup the adapter uses to reconstruct each row's exact address;
	// both are plain key-hash payment credentials here (PaymentScript
	// defaults false).
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: fill32(0x01), OutputIdx: 0,
		PaymentKey: addr1Payment, StakingKey: stakeKey, CredentialTag: 0,
		Amount: types.Uint64(1_000_000),
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: fill32(0x02), OutputIdx: 1,
		PaymentKey: addr2Payment, StakingKey: stakeKey, CredentialTag: 0,
		Amount: types.Uint64(700_000),
	}).Error)

	// tx1 in block 100 (slot 100), one association with addr1.
	createAccountTransactionFixture(
		t, store, 1, fill32(0x01), fill32(0xc1), 100, 0,
		addr1Payment, stakeKey, 0,
	)
	// tx2 in block 101 (slot 101), associations with both addr1 and addr2
	// (two outputs sharing the credential in one tx must yield two rows,
	// not a duplicate of the same association -- and a second output to
	// addr1 in the same tx, represented by inserting the addr1 row only
	// once here, must not create a third row for the same pair).
	createAccountTransactionFixture(
		t, store, 2, fill32(0x02), fill32(0xc2), 101, 2,
		addr1Payment, stakeKey, 0,
	)
	addAccountTransactionAssociation(
		t, store, 2, 101, 2, addr2Payment, stakeKey, 0,
	)

	// tx3 in block 105 under a different stake credential; must never
	// appear in results.
	otherStakeKey := bytes.Repeat([]byte{0x88}, lcommon.AddressHashSize)
	createAccountTransactionFixture(
		t, store, 4, fill32(0x03), fill32(0xc3), 105, 0,
		bytes.Repeat([]byte{0x09}, lcommon.AddressHashSize),
		otherStakeKey, 0,
	)

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
	// rows for tx2 (block 101), sorted deterministically by payment key.
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

	// from=101 excludes tx1 (block 100, slot 100 < resolved from-slot 101).
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
	require.Len(t, fromRows, 2)
	for _, r := range fromRows {
		assert.NotEqual(t, hex.EncodeToString(fill32(0x01)), r.TxHash)
	}

	// to=100 keeps only tx1 (slot 100 <= resolved to-slot 100; tx2's slot
	// 101 is excluded).
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

	// from=101:2 (exactly tx2's index) still includes tx2: the range is
	// inclusive at the boundary.
	idx2 := uint32(2)
	exactRows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
			From: &BlockRangePosition{Block: 101, Index: &idx2},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, exactRows, 2)

	// to=101:1 (before tx2's index of 2) excludes tx2, leaving only tx1.
	idx1 := uint32(1)
	beforeIdxRows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
			To: &BlockRangePosition{Block: 101, Index: &idx1},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, beforeIdxRows, 1)
	assert.Equal(t, hex.EncodeToString(fill32(0x01)), beforeIdxRows[0].TxHash)

	// An inverted-looking from/to still resolves each side independently
	// (the handler rejects a genuinely inverted range before this is ever
	// called; a from beyond every known block is unsatisfiable on its own).
	fromBeyondTip, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
			From: &BlockRangePosition{Block: 999},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, fromBeyondTip)

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

// TestNodeAdapterAccountTransactionsScriptPaymentCredential verifies that a
// script payment credential (as opposed to the default key-hash
// assumption AccountAssociatedAddresses makes) is reconstructed correctly,
// using GetUtxoPaymentScriptByCredential rather than decoding UTxO CBOR.
func TestNodeAdapterAccountTransactionsScriptPaymentCredential(t *testing.T) {
	adapter, store, db := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x66)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)
	createTestBlock(t, db, fill32(0xd1), 50)

	scriptPayment := bytes.Repeat([]byte{0x0a}, lcommon.AddressHashSize)
	wantAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptKey,
		lcommon.AddressNetworkTestnet,
		scriptPayment,
		stakeKey,
	)
	require.NoError(t, err)

	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: fill32(0x10), OutputIdx: 0,
		PaymentKey: scriptPayment, StakingKey: stakeKey, CredentialTag: 0,
		PaymentScript: true,
		Amount:        types.Uint64(1),
	}).Error)
	createAccountTransactionFixture(
		t, store, 1, fill32(0x10), fill32(0xd1), 50, 0,
		scriptPayment, stakeKey, 0,
	)

	rows, total, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 100, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, rows, 1)
	assert.Equal(t, wantAddr.String(), rows[0].Address)
}

// TestNodeAdapterAccountTransactionsBounded is the regression test for the
// unbounded-history bug: per-request work must be bounded by the requested
// page size, not by the credential's full transaction history. It creates
// five transactions, each in its own block, but only creates a Block row
// for the one transaction that will actually appear on a count=1 page. If
// the implementation resolved block height for every matching transaction
// up front (the original bug), it would fail looking up one of the four
// missing blocks before pagination ever discarded them; a correctly
// bounded implementation only resolves the page's own transaction's block
// and succeeds.
func TestNodeAdapterAccountTransactionsBounded(t *testing.T) {
	adapter, store, db := newDBBackedAccountAdapter(t)

	stakeAddress, stakeKey := testStakeCredential(t, 0x55)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
	}).Error)

	const total = 5
	// Only the first (oldest, ascending-order-first) transaction's block
	// is ever created; the other four blocks are deliberately absent.
	createTestBlock(t, db, fill32(0xe0), 0)

	for i := range total {
		paymentKey := bytes.Repeat([]byte{byte(0x20 + i)}, lcommon.AddressHashSize)
		blockHash := fill32(byte(0xe0 + i))
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId: fill32(byte(i)), OutputIdx: 0,
			PaymentKey: paymentKey, StakingKey: stakeKey, CredentialTag: 0,
			Amount: types.Uint64(1),
		}).Error)
		createAccountTransactionFixture(
			t, store, uint(i+1), fill32(byte(i)), blockHash,
			uint64(i), 0, paymentKey, stakeKey, 0,
		)
	}

	rows, gotTotal, err := adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 1, Page: 1, Order: PaginationOrderAsc,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, total, gotTotal)
	require.Len(t, rows, 1)
	assert.Equal(t, hex.EncodeToString(fill32(0)), rows[0].TxHash)

	// A later page whose transaction's block is also missing must fail:
	// this confirms the test's missing-block setup would actually have
	// caught the original bug (it only "passes" the count=1/page=1 case
	// above because that happens to be the one block that exists).
	_, _, err = adapter.AccountTransactions(
		stakeAddress,
		AccountTransactionsParams{
			Pagination: PaginationParams{
				Count: 1, Page: 2, Order: PaginationOrderAsc,
			},
		},
	)
	require.Error(t, err)
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
