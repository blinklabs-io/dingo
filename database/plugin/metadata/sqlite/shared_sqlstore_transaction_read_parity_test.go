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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type transactionReadStore interface {
	GetTransactionByHash([]byte, types.Txn) (*models.Transaction, error)
	GetTransactionSlotByHash([]byte, types.Txn) (uint64, bool, error)
	GetTransactionIDByHash([]byte, types.Txn) (uint, bool, error)
	GetTransactionMetadataByHash([]byte, types.Txn) ([]byte, error)
	SumTransactionFeesInSlotRange(uint64, uint64, types.Txn) (uint64, error)
	GetTransactionsByBlockHash([]byte, types.Txn) ([]models.Transaction, error)
	GetTransactionsByHashes([][]byte, types.Txn) ([]models.Transaction, error)
	GetTransactionHashesAfterSlot(uint64, types.Txn) ([][]byte, error)
	GetTransactionsByAddress(
		[]byte,
		uint8,
		[]byte,
		int,
		int,
		string,
		types.Txn,
	) ([]models.Transaction, error)
	CountTransactionsByAddress(
		[]byte,
		uint8,
		[]byte,
		types.Txn,
	) (int, error)
	CountTransactionsByPaymentCred([]byte, types.Txn) (int, error)
	GetTransactionsByMetadataLabel(
		uint64,
		int,
		int,
		bool,
		types.Txn,
	) ([]models.Transaction, error)
	CountTransactionsByMetadataLabel(uint64, types.Txn) (int, error)
	GetAddressesByCredential(
		uint8,
		[]byte,
		int,
		int,
		string,
		types.Txn,
	) ([]models.AddressTransaction, error)
	CountAddressesByCredential(uint8, []byte, types.Txn) (int, error)
	DeleteAddressTransactionsAfterSlot(uint64, types.Txn) error
	DeleteTransactionMetadataLabelsAfterSlot(uint64, types.Txn) error
}

type transactionReadState struct {
	ByHash            *models.Transaction
	Missing           *models.Transaction
	Slot              uint64
	SlotFound         bool
	ID                uint
	IDFound           bool
	Metadata          []byte
	FeeSum            uint64
	ByBlock           []models.Transaction
	ByHashes          []models.Transaction
	HashesAfter       [][]byte
	ByAddress         []models.Transaction
	AddressCount      int
	PaymentCount      int
	ByLabel           []models.Transaction
	LabelCount        int
	Addresses         []models.AddressTransaction
	AddressesCount    int
	AddressCountAfter int
	LabelCountAfter   int
}

func TestSharedSQLStoreTransactionReadParity(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	seedTransactionReadFixture := func(exec func(string, ...any) error) {
		t.Helper()
		require.NoError(t, exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, metadata, slot, type, fee, collateral_fee,
    ttl, block_index, valid
) VALUES
    (1, ?, ?, ?, 10, 1, '5', '0', '20', 0, TRUE),
    (2, ?, ?, NULL, 11, 2, '9', '7', '21', 1, FALSE),
    (3, ?, ?, ?, 12, 3, '4', '0', '22', 0, TRUE)`,
			[]byte("tx-a"), []byte("block-a"), []byte("meta-a"),
			[]byte("tx-b"), []byte("block-a"),
			[]byte("tx-c"), []byte("block-b"), []byte("meta-c"),
		))
		require.NoError(t, exec(`
INSERT INTO address_transaction (
    id, payment_key, staking_key, credential_tag, transaction_id, slot, tx_index
) VALUES
    (1, ?, ?, 0, 1, 10, 0),
    (2, ?, ?, 0, 2, 11, 1),
    (3, ?, ?, 1, 3, 12, 0)`,
			[]byte("pay-a"), []byte("stake-a"),
			[]byte("pay-a"), []byte("stake-a"),
			[]byte("pay-b"), []byte("stake-a"),
		))
		require.NoError(t, exec(`
INSERT INTO transaction_metadata_label (
    id, transaction_id, label, slot, cbor_value, json_value
) VALUES
    (1, 1, '42', 10, X'01', '{}'),
    (2, 2, '42', 11, X'02', '{}'),
    (3, 3, '99', 12, X'03', '{}')`))
	}
	seedTransactionReadFixture(func(query string, args ...any) error {
		_, err := raw.Exec(query, args...)
		return err
	})

	_ = exerciseTransactionReadStore(t, store)
}

func exerciseTransactionReadStore(
	t *testing.T,
	store transactionReadStore,
) transactionReadState {
	t.Helper()
	var ret transactionReadState
	var err error
	ret.ByHash, err = store.GetTransactionByHash([]byte("tx-a"), nil)
	require.NoError(t, err)
	ret.Missing, err = store.GetTransactionByHash([]byte("missing"), nil)
	require.NoError(t, err)
	ret.Slot, ret.SlotFound, err = store.GetTransactionSlotByHash(
		[]byte("tx-b"),
		nil,
	)
	require.NoError(t, err)
	ret.ID, ret.IDFound, err = store.GetTransactionIDByHash(
		[]byte("tx-c"),
		nil,
	)
	require.NoError(t, err)
	ret.Metadata, err = store.GetTransactionMetadataByHash(
		[]byte("tx-a"),
		nil,
	)
	require.NoError(t, err)
	ret.FeeSum, err = store.SumTransactionFeesInSlotRange(10, 12, nil)
	require.NoError(t, err)
	ret.ByBlock, err = store.GetTransactionsByBlockHash(
		[]byte("block-a"),
		nil,
	)
	require.NoError(t, err)
	ret.ByHashes, err = store.GetTransactionsByHashes(
		[][]byte{[]byte("tx-c"), []byte("tx-a")},
		nil,
	)
	require.NoError(t, err)
	ret.HashesAfter, err = store.GetTransactionHashesAfterSlot(10, nil)
	require.NoError(t, err)
	ret.ByAddress, err = store.GetTransactionsByAddress(
		[]byte("pay-a"),
		0,
		[]byte("stake-a"),
		1,
		0,
		"desc",
		nil,
	)
	require.NoError(t, err)
	ret.AddressCount, err = store.CountTransactionsByAddress(
		[]byte("pay-a"),
		0,
		[]byte("stake-a"),
		nil,
	)
	require.NoError(t, err)
	ret.PaymentCount, err = store.CountTransactionsByPaymentCred(
		[]byte("pay-a"),
		nil,
	)
	require.NoError(t, err)
	ret.ByLabel, err = store.GetTransactionsByMetadataLabel(
		42,
		5,
		0,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.LabelCount, err = store.CountTransactionsByMetadataLabel(42, nil)
	require.NoError(t, err)
	ret.Addresses, err = store.GetAddressesByCredential(
		0,
		[]byte("stake-a"),
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	ret.AddressesCount, err = store.CountAddressesByCredential(
		0,
		[]byte("stake-a"),
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAddressTransactionsAfterSlot(10, nil))
	require.NoError(
		t,
		store.DeleteTransactionMetadataLabelsAfterSlot(10, nil),
	)
	ret.AddressCountAfter, err = store.CountTransactionsByPaymentCred(
		[]byte("pay-a"),
		nil,
	)
	require.NoError(t, err)
	ret.LabelCountAfter, err = store.CountTransactionsByMetadataLabel(42, nil)
	require.NoError(t, err)
	return ret
}
