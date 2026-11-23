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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestAddressTransactionsByCredentialBreakTiesByPaymentKeyHash pins the
// documented (slot, tx_index, payment_key) order. The payment key that sorts
// first by hash is given the later dictionary id, so ordering by the id
// instead of the hash reverses the two rows of the shared transaction.
func TestAddressTransactionsByCredentialBreakTiesByPaymentKeyHash(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	stake := dictionaryHash(0x51)
	low, high := dictionaryHash(0x11), dictionaryHash(0xee)
	highID := fixtureCredentialID(t, store, high)
	lowID := fixtureCredentialID(t, store, low)
	require.Less(t, highID, lowID)
	stakeID := fixtureCredentialID(t, store, stake)
	txID := testutil.InsertAppliedTransaction(t, store.writeDB,
		testutil.AppliedTransaction{
			Hash:      bytes.Repeat([]byte{0x77}, 32),
			BlockHash: bytes.Repeat([]byte{0x78}, 32),
			Slot:      500,
		})
	for _, paymentID := range []any{highID, lowID} {
		_, err := store.writeDB.ExecContext(t.Context(), `
INSERT INTO address_transaction
    (payment_key_id, staking_key_id, credential_tag, transaction_id, slot,
     tx_index)
VALUES (?, ?, 0, ?, 500, 0)`, paymentID, stakeID, txID)
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		order string
		want  [][]byte
	}{
		{"asc", [][]byte{low, high}},
		{"desc", [][]byte{high, low}},
	} {
		rows, err := store.GetAddressTransactionsByCredential(
			0, stake, 10, 0, tc.order, nil, nil, nil,
		)
		require.NoError(t, err)
		got := make([][]byte, len(rows))
		for i, row := range rows {
			got[i] = row.PaymentKey
		}
		require.Equal(t, tc.want, got, "order %s", tc.order)
	}
}
