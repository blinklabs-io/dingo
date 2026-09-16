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
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// legacySumCredentialUtxoStake reproduces refreshRewardLiveStakeAggregate's
// pre-fix approach: fetch every live UTxO amount for the credential and sum
// it in Go via the generic sumUint64Rows helper. Kept here as the
// correctness and benchmark comparison point for sumCredentialUtxoStake's
// single-aggregate rewrite.
func legacySumCredentialUtxoStake(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
) (uint64, error) {
	return sumUint64Rows(ctx, db, `
SELECT amount
FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
		ref.Tag, ref.Key)
}

// seedCredentialUtxos inserts one live (or, for entries marked deleted, spent)
// UTxO per amount, all under the same stake credential. A distinct tx_id per
// row satisfies the utxo table's uniqueness expectations without colliding
// with any other seeded credential in the same test.
func seedCredentialUtxos(
	tb testing.TB,
	store *Store,
	group int,
	ref models.StakeCredentialRef,
	amounts []uint64,
	deleted []bool,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(tb, err)
	for i, amount := range amounts {
		txID := make([]byte, 32)
		txID[0] = byte(group >> 24)
		txID[1] = byte(group >> 16)
		txID[2] = byte(group >> 8)
		txID[3] = byte(group)
		txID[4] = byte(i >> 24)
		txID[5] = byte(i >> 16)
		txID[6] = byte(i >> 8)
		txID[7] = byte(i)
		deletedSlot := int64(0)
		if deleted != nil && deleted[i] {
			deletedSlot = 100
		}
		_, err := stmt.Exec(
			txID,
			0,
			ref.Key,
			int64(ref.Tag),
			int64(1),
			deletedSlot,
			decimalUint64(types.Uint64(amount)),
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// TestSumCredentialUtxoStakeMatchesLegacyRowSum proves the single-aggregate
// rewrite returns exactly what the old per-row Go summation did, across an
// empty credential, a single UTxO, a mix of live and already-spent UTxOs
// (the deleted ones must be excluded from both), and amounts spanning small
// balances up to a total near the real lovelace supply.
func TestSumCredentialUtxoStakeMatchesLegacyRowSum(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		amounts []uint64
		deleted []bool
	}{
		{name: "no utxos"},
		{name: "single utxo", amounts: []uint64{5_000_000}},
		{
			name:    "many utxos, some spent",
			amounts: []uint64{1, 2, 3, 1_000_000, 45_000_000_000_000_000},
			deleted: []bool{false, true, false, true, false},
		},
		{
			name: "large realistic totals",
			amounts: []uint64{
				44_999_999_000_000_000,
				999_000_000,
				1,
			},
		},
	}

	for i, tc := range cases {
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
		if len(tc.amounts) > 0 {
			seedCredentialUtxos(t, store, i, ref, tc.amounts, tc.deleted)
		}

		legacy, err := legacySumCredentialUtxoStake(ctx, store.writeDB, ref)
		require.NoError(t, err, tc.name)
		got, err := sumCredentialUtxoStake(ctx, store.writeDB, ref)
		require.NoError(t, err, tc.name)
		require.Equal(t, legacy, got, tc.name)

		var want uint64
		for j, amount := range tc.amounts {
			if tc.deleted != nil && tc.deleted[j] {
				continue
			}
			want += amount
		}
		require.Equal(t, want, got, tc.name)
	}
}

func credentialKeyForIndex(i int) []byte {
	key := make([]byte, 28)
	key[0] = byte(i >> 16)
	key[1] = byte(i >> 8)
	key[2] = byte(i)
	return key
}

// BenchmarkSumCredentialUtxoStake is the timing counterpart: a stake
// credential with many live UTxOs (a heavily used address; profiling a
// synced node found one holding 6,794) forces refreshRewardLiveStakeAggregate
// to fetch every one of them into Go and sum there on every touch. n scales
// with how many live UTxOs a credential has accumulated by the time it is
// next touched.
func BenchmarkSumCredentialUtxoStake(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100, 1_000, 7_000} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/legacy_row_sum", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := legacySumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/sql_aggregate", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := sumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
