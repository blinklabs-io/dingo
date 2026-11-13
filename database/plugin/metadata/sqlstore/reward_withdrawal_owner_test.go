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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// A withdrawal is owned by a transaction row that something else inserted. A
// hash with no row is refused instead of getting a placeholder row: the real
// insert's conflict handling keeps a pre-existing row's type, fee, ttl,
// metadata and valid columns, so a placeholder would stay incomplete and
// invisible to readers that filter on valid.
func TestApplyAccountRewardWithdrawalRequiresTransactionRow(t *testing.T) {
	t.Parallel()
	store, raw := newManagementTestStoreWithRawDB(t)
	key := bytesRepeat(0x71, 28)
	hash := bytesRepeat(0x72, 32)
	rollbackAccount(t, store, key, 40)

	err := store.ApplyAccountRewardWithdrawal(0, key, 40, 200, hash, nil)
	require.ErrorIs(t, err, models.ErrWithdrawalTransactionNotFound)

	var transactions, withdrawals int
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM "transaction"`,
	).Scan(&transactions))
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM reward_withdrawal`,
	).Scan(&withdrawals))
	require.Zero(t, transactions, "no placeholder transaction row")
	require.Zero(t, withdrawals)
	require.Equal(t, uint64(40), rewardOf(t, store, key),
		"a refused withdrawal leaves the balance alone")

	// The hash is inserted for real afterwards; the retried withdrawal binds
	// to that row, which carries every column the real insert sets.
	require.NoError(t, store.SetGenesisTransaction(hash, hash, nil, nil))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, key, 40, 200, hash, nil,
	))
	require.Equal(t, uint64(0), rewardOf(t, store, key))
	var incomplete, owned int
	require.NoError(t, raw.QueryRow(`
SELECT COUNT(*) FROM applied_transaction
WHERE type IS NULL OR fee IS NULL OR valid IS NULL OR block_hash IS NULL`,
	).Scan(&incomplete))
	require.NoError(t, raw.QueryRow(`
SELECT COUNT(*) FROM reward_withdrawal w
JOIN "transaction" t ON t.id = w.tx_id AND t.hash = ?`, hash,
	).Scan(&owned))
	require.Zero(t, incomplete)
	require.Equal(t, 1, owned)
}

// An identity row with no application row names a transaction the store has
// only seen referenced, such as the producer of an imported output. A
// withdrawal is refused against it: the withdrawing transaction is applied.
func TestApplyAccountRewardWithdrawalRequiresAppliedTransaction(t *testing.T) {
	t.Parallel()
	store, raw := newManagementTestStoreWithRawDB(t)
	key := bytesRepeat(0x73, 28)
	hash := bytesRepeat(0x74, 32)
	rollbackAccount(t, store, key, 40)
	_, err := raw.Exec(`INSERT INTO "transaction" (hash) VALUES (?)`, hash)
	require.NoError(t, err)

	err = store.ApplyAccountRewardWithdrawal(0, key, 40, 200, hash, nil)
	require.ErrorIs(t, err, models.ErrWithdrawalTransactionNotFound)

	var withdrawals int
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM reward_withdrawal`,
	).Scan(&withdrawals))
	require.Zero(t, withdrawals)
	require.Equal(t, uint64(40), rewardOf(t, store, key))
}
