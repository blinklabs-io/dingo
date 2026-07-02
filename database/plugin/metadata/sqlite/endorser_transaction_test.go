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

func ebHash(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

// TestEndorserTransactionAddAndFilter records endorser-transaction provenance
// and reads it back through FilterEndorserTransactions, including idempotent
// re-recording.
func TestEndorserTransactionAddAndFilter(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	h1, h2, h3 := ebHash(0x11), ebHash(0x22), ebHash(0x33)

	require.NoError(t, store.AddEndorserTransactions([][]byte{h1, h2}, 100, nil))
	// Re-recording an existing hash is a no-op (ON CONFLICT DO NOTHING).
	require.NoError(t, store.AddEndorserTransactions([][]byte{h1}, 100, nil))

	got, err := store.FilterEndorserTransactions([][]byte{h1, h2, h3}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{h1, h2}, got)

	var count int64
	require.NoError(t, store.DB().Model(&models.EndorserTransaction{}).
		Count(&count).Error)
	require.Equal(t, int64(2), count)
}

// TestUtxoSpenders returns the distinct spenders of outputs produced by the
// given producer transactions, and nothing for producers with unspent outputs.
func TestUtxoSpenders(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	producer := ebHash(0xAA)
	producer2 := ebHash(0xBB)
	spender1, spender2 := ebHash(0xC1), ebHash(0xC2)

	// Spenders must exist as transactions for the spent_at_tx_id FK.
	for _, h := range [][]byte{spender1, spender2} {
		require.NoError(t, store.DB().Create(&models.Transaction{
			Hash: h, Slot: 200,
		}).Error)
	}
	// Two outputs of producer, spent by two different transactions.
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: producer, OutputIdx: 0, AddedSlot: 100,
		DeletedSlot: 200, SpentAtTxId: types.NullableHash(spender1),
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: producer, OutputIdx: 1, AddedSlot: 100,
		DeletedSlot: 200, SpentAtTxId: types.NullableHash(spender2),
	}).Error)
	// An unspent output of producer2.
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: producer2, OutputIdx: 0, AddedSlot: 100,
	}).Error)

	got, err := store.UtxoSpenders([][]byte{producer}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{spender1, spender2}, got)

	got2, err := store.UtxoSpenders([][]byte{producer2}, nil)
	require.NoError(t, err)
	require.Empty(t, got2)
}

// TestRevokeEndorserTransactionsRestoresInputAndDeletesOutputs is the core
// revoke test: an endorser transaction that spent an input and produced an
// output is revoked, restoring the input to unspent and deleting the output,
// the transaction row, its address-index and metadata-label rows, collateral
// and reference-input links, and the provenance row.
func TestRevokeEndorserTransactionsRestoresInputAndDeletesOutputs(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	eb := ebHash(0xEE)         // the endorser transaction's hash
	src := ebHash(0x50)        // the transaction that produced the input eb spends
	collateral := ebHash(0x51) // collateral referenced by eb
	reference := ebHash(0x52)  // reference input referenced by eb

	// The endorser transaction row (referenced by spent_at_tx_id and by the
	// address-index row).
	ebTx := models.Transaction{Hash: eb, BlockHash: ebHash(0x01), Slot: 200}
	require.NoError(t, store.DB().Create(&ebTx).Error)

	// The input eb spent: produced by src, now tombstoned as spent by eb.
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: src, OutputIdx: 0, AddedSlot: 100,
		DeletedSlot: 200, SpentAtTxId: types.NullableHash(eb),
	}).Error)
	// The output eb produced (unspent).
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: eb, OutputIdx: 0, AddedSlot: 200,
		TransactionID: &ebTx.ID,
	}).Error)
	// UTxOs linked to eb as collateral and reference input.
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: collateral, OutputIdx: 0, AddedSlot: 100,
		CollateralByTxId: types.NullableHash(eb),
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: reference, OutputIdx: 0, AddedSlot: 100,
		ReferencedByTxId: types.NullableHash(eb),
	}).Error)
	// An address-index row for the endorser transaction.
	require.NoError(t, store.DB().Create(&models.AddressTransaction{
		TransactionID: ebTx.ID, Slot: 200,
		PaymentKey: bytes.Repeat([]byte{0x77}, 28),
	}).Error)
	require.NoError(t, store.DB().Create(&models.TransactionMetadataLabel{
		TransactionID: ebTx.ID,
		Label:         types.Uint64(721),
		Slot:          200,
		CborValue:     []byte{0xA0},
	}).Error)
	require.NoError(t, store.AddEndorserTransactions([][]byte{eb}, 200, nil))

	// Revoke it.
	require.NoError(t, store.RevokeEndorserTransactions([][]byte{eb}, nil))

	// The input is restored to unspent.
	input, err := store.GetUtxoIncludingSpent(src, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, input)
	require.Equal(t, uint64(0), input.DeletedSlot)
	require.Empty(t, input.SpentAtTxId)

	// The produced output is gone.
	out, err := store.GetUtxoIncludingSpent(eb, 0, nil)
	require.NoError(t, err)
	require.Nil(t, out)

	// The collateral and reference links are cleared.
	collateralUtxo, err := store.GetUtxoIncludingSpent(collateral, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, collateralUtxo)
	require.Empty(t, collateralUtxo.CollateralByTxId)
	referenceUtxo, err := store.GetUtxoIncludingSpent(reference, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, referenceUtxo)
	require.Empty(t, referenceUtxo.ReferencedByTxId)

	// The transaction row, address-index row, metadata-label row, and
	// provenance row are gone.
	var txCount, addrCount, labelCount int64
	require.NoError(t, store.DB().Model(&models.Transaction{}).
		Where("hash = ?", eb).Count(&txCount).Error)
	require.Equal(t, int64(0), txCount)
	require.NoError(t, store.DB().Model(&models.AddressTransaction{}).
		Where("transaction_id = ?", ebTx.ID).Count(&addrCount).Error)
	require.Equal(t, int64(0), addrCount)
	require.NoError(t, store.DB().Model(&models.TransactionMetadataLabel{}).
		Where("transaction_id = ?", ebTx.ID).Count(&labelCount).Error)
	require.Equal(t, int64(0), labelCount)
	remaining, err := store.FilterEndorserTransactions([][]byte{eb}, nil)
	require.NoError(t, err)
	require.Empty(t, remaining)
}

// TestDeleteEndorserTransactionsAfterSlot prunes only provenance rows whose
// referencing ranking block is after the rollback slot.
func TestDeleteEndorserTransactionsAfterSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	early, late := ebHash(0x0E), ebHash(0x0F)
	require.NoError(t, store.AddEndorserTransactions([][]byte{early}, 100, nil))
	require.NoError(t, store.AddEndorserTransactions([][]byte{late}, 300, nil))

	require.NoError(t, store.DeleteEndorserTransactionsAfterSlot(200, nil))

	got, err := store.FilterEndorserTransactions([][]byte{early, late}, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{early}, got)
}
