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

package database

import (
	"bytes"
	"database/sql"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addressTransactionCountCase is one expected exact-address count.
type addressTransactionCountCase struct {
	name     string
	addr     lcommon.Address
	from, to *models.AddressTransactionPosition
	want     int
}

// TestCountTransactionsByAddressUsesIndexedAddresses seeds an enterprise
// address whose outputs are produced, spent, referenced, used as collateral
// and returned as collateral, beside a pointer and a base address sharing its
// payment key. The decoding scan and the utxo_address count must agree, and
// once the index is complete the count must not read output CBOR.
func TestCountTransactionsByAddressUsesIndexedAddresses(t *testing.T) {
	t.Parallel()

	config := &Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
		Role:        RoleIndexer,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	raw := rawSQLiteMetadataFixture(t, db)
	payment := bytes.Repeat([]byte{0x61}, lcommon.AddressHashSize)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, lcommon.AddressNetworkTestnet, payment, nil,
	)
	require.NoError(t, err)
	pointer := exactAddressTestPointer(t, payment, 0x01)
	base, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet, payment,
		bytes.Repeat([]byte{0x62}, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	other, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x63}, lcommon.AddressHashSize), nil,
	)
	require.NoError(t, err)

	produced := seedExactAddressUtxo(t, db, raw, enterprise, 1, 0x01)
	for slot := uint64(2); slot <= 4; slot++ {
		seedExactAddressUtxo(t, db, raw, enterprise, slot, byte(slot))
	}
	seedExactAddressUtxo(t, db, raw, pointer, 5, 0x05)
	seedExactAddressUtxo(t, db, raw, pointer, 6, 0x06)
	seedExactAddressUtxo(t, db, raw, base, 7, 0x07)
	// Transactions 8-11 pay the unrelated address and touch the enterprise
	// or pointer address only through an input, a reference input or
	// collateral, as the ledger records them.
	for slot := uint64(8); slot <= 11; slot++ {
		seedExactAddressUtxo(t, db, raw, other, slot, byte(slot))
	}
	spendInput(t, raw, 1, 8, payment)
	addInputEdge(t, raw, "utxo_reference_edge", 2, 9, payment)
	addInputEdge(t, raw, "utxo_collateral_edge", 3, 10, payment)
	spendInput(t, raw, 5, 11, payment)
	// Transaction 12's only enterprise output is its collateral return.
	seedExactAddressUtxo(t, db, raw, enterprise, 12, 0x0c)
	_, err = raw.Exec(
		`UPDATE utxo SET collateral_return_for_tx_id = 12 WHERE tx_id = 12`,
	)
	require.NoError(t, err)

	cases := []addressTransactionCountCase{
		{name: "enterprise", addr: enterprise, want: 8},
		{name: "pointer", addr: pointer, want: 3},
		{name: "base", addr: base, want: 1},
		{
			name: "enterprise in range",
			addr: enterprise,
			from: &models.AddressTransactionPosition{Slot: 3},
			to:   &models.AddressTransactionPosition{Slot: 9},
			want: 4,
		},
	}
	requireCounts := func(t *testing.T, db *Database) {
		t.Helper()
		for _, tc := range cases {
			got, err := db.CountTransactionsByAddress(
				t.Context(), tc.addr, tc.from, tc.to, nil,
			)
			require.NoError(t, err, tc.name)
			assert.Equal(t, tc.want, got, tc.name)
		}
	}

	// Opening an empty database completes the backfill, so the rows seeded
	// since then have no utxo_address row. Reopen the backfill to count by
	// decoding first.
	require.NoError(t, db.SetSyncState(utxoAddressBackfillSyncKey, "", nil))
	requireCounts(t, db)
	require.NoError(t, db.backfillUtxoAddresses(t.Context()))
	requireCounts(t, db)

	// Reopen so no cache holds the output CBOR the scan would decode, then
	// remove one output's CBOR.
	require.NoError(t, closeTestDatabase(db))
	db, err = newTestDatabase(t, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeTestDatabase(db)) })
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		return db.Blob().DeleteUtxo(
			txn.Blob(), produced.TxId, produced.OutputIdx,
		)
	}))
	requireCounts(t, db)
	require.NoError(t, db.SetSyncState(utxoAddressBackfillSyncKey, "", nil))
	_, err = db.CountTransactionsByAddress(t.Context(), enterprise, nil, nil, nil)
	require.Error(t, err, "the decoding scan reads every candidate's outputs")
}

// spendInput marks the output of producer spent by spender and records the
// spender in the payment key's address history.
func spendInput(
	t *testing.T,
	raw *sql.DB,
	producer, spender uint,
	payment []byte,
) {
	t.Helper()
	_, err := raw.Exec(
		`UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = ? WHERE tx_id = ?`,
		spender, spender, producer,
	)
	require.NoError(t, err)
	addAddressTransaction(t, raw, spender, payment)
}

// addInputEdge links the output of producer to consumer through a reference
// or collateral edge table.
func addInputEdge(
	t *testing.T,
	raw *sql.DB,
	table string,
	producer, consumer uint,
	payment []byte,
) {
	t.Helper()
	_, err := raw.Exec(
		`INSERT INTO `+table+` (utxo_id, tx_id)
SELECT id, ? FROM utxo WHERE tx_id = ?`,
		consumer, producer,
	)
	require.NoError(t, err)
	addAddressTransaction(t, raw, consumer, payment)
}

func addAddressTransaction(
	t *testing.T,
	raw *sql.DB,
	txID uint,
	payment []byte,
) {
	t.Helper()
	_, err := raw.Exec(`
INSERT INTO address_transaction (
    payment_key, payment_script, staking_key, credential_tag,
    transaction_id, slot, tx_index
) VALUES (?, FALSE, NULL, 0, ?, ?, 0)`,
		payment, txID, txID,
	)
	require.NoError(t, err)
}
