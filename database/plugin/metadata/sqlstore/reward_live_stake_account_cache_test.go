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
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// seedRewardLiveStakeAccount inserts one registered `account` row directly
// against the real migrated schema, giving refreshRewardLiveStakeAggregate's
// account lookup (rewardLiveStakeAccountQuery) a row to actually match, so
// tests exercise the reward-carrying, registered path rather than always
// hitting sql.ErrNoRows.
func seedRewardLiveStakeAccount(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
	pool []byte,
	reward uint64,
) {
	tb.Helper()
	_, err := store.writeDB.Exec(
		`INSERT INTO account (staking_key, credential_tag, pool, reward, active, added_slot)
VALUES (?, ?, ?, ?, TRUE, ?)`,
		ref.Key,
		int64(ref.Tag),
		pool,
		decimalUint64(types.Uint64(reward)),
		int64(1),
	)
	require.NoError(tb, err)
}

// oneShotRefreshRewardLiveStakeAggregate reproduces refreshRewardLiveStakeAggregate
// exactly, except its account lookup and upsert are issued as plain one-shot
// QueryRowContext/ExecContext calls instead of going through Store's cached
// statements. It is the direct before/after benchmark and correctness
// comparison point for the rewardLiveStakeAccountQuery/rewardLiveStakeUpsertQuery
// cache entries, mirroring oneShotSumCredentialUtxoStake.
func oneShotRefreshRewardLiveStakeAggregate(
	ctx context.Context,
	s *Store,
	db queryer,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	if len(ref.Key) == 0 {
		return nil
	}
	var reward sql.NullString
	var pool []byte
	var active sql.NullBool
	var addedSlot sql.NullInt64
	accountErr := db.QueryRowContext(ctx, rewardLiveStakeAccountQuery,
		ref.Tag, ref.Key,
	).Scan(&reward, &pool, &active, &addedSlot)
	if accountErr != nil && !isNoRows(accountErr) {
		return fmt.Errorf("query reward live stake account: %w", accountErr)
	}
	utxoStake, err := s.sumCredentialUtxoStake(ctx, db, ref)
	if err != nil {
		return fmt.Errorf("sum reward live stake UTxOs: %w", err)
	}
	rewardStake := uint64(0)
	registered := false
	if accountErr == nil {
		registered = active.Bool
		if reward.Valid {
			value, err := parseUint64("reward live stake reward", reward.String)
			if err != nil {
				return err
			}
			rewardStake = value
		}
	}
	total := utxoStake + rewardStake
	if isNoRows(accountErr) && total == 0 {
		_, err := db.ExecContext(ctx,
			`DELETE FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
			ref.Tag, ref.Key,
		)
		return err
	}
	if !registered {
		pool = nil
	}
	delegationSlot := int64(0)
	if registered && len(pool) > 0 {
		delegationSlot = addedSlot.Int64
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, rewardLiveStakeUpsertQuery,
		ref.Tag,
		ref.Key,
		pool,
		decimalUint64(types.Uint64(utxoStake)),
		decimalUint64(types.Uint64(rewardStake)),
		decimalUint64(types.Uint64(total)),
		registered,
		delegationSlot,
		slotValue,
		models.RewardStakeCalculationVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert reward live stake: %w", err)
	}
	return nil
}

func isNoRows(err error) bool {
	return err == sql.ErrNoRows
}

// readRewardLiveStake fetches the row refreshRewardLiveStakeAggregate wrote,
// for asserting on its computed totals.
func readRewardLiveStake(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
) (utxoStake, rewardStake, totalStake uint64, registered bool) {
	tb.Helper()
	var utxo, rwd, tot string
	row := store.writeDB.QueryRow(
		`SELECT utxo_stake, reward_stake, total_stake, registered
FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag, ref.Key,
	)
	require.NoError(tb, row.Scan(&utxo, &rwd, &tot, &registered))
	u, err := parseUint64("utxo_stake", utxo)
	require.NoError(tb, err)
	r, err := parseUint64("reward_stake", rwd)
	require.NoError(tb, err)
	t, err := parseUint64("total_stake", tot)
	require.NoError(tb, err)
	return u, r, t, registered
}

// requireNoRewardLiveStakeRow asserts no reward_live_stake row exists for
// ref, the expected outcome of refreshRewardLiveStakeAggregate's early
// DELETE-and-return path (never registered, zero total).
func requireNoRewardLiveStakeRow(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
) {
	tb.Helper()
	var exists bool
	err := store.writeDB.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?)`,
		ref.Tag, ref.Key,
	).Scan(&exists)
	require.NoError(tb, err)
	require.False(tb, exists, "expected no reward_live_stake row")
}

// TestRefreshRewardLiveStakeAggregateCachesAccountAndUpsertStatements proves
// Start eagerly caches both new per-touch queries (not just
// sumCredentialUtxoStakeQuery) and that repeated lookups return the same
// *sql.Stmt. This is the regression check for the caching change itself:
// against the prior version of this function, lookupCachedStmt for either
// query returned ok=false, since neither was in hotStatements.
func TestRefreshRewardLiveStakeAggregateCachesAccountAndUpsertStatements(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	accountStmt, ok := store.lookupCachedStmt(rewardLiveStakeAccountQuery)
	require.True(t, ok, "expected the account lookup to be cached")
	require.NotNil(t, accountStmt)

	upsertStmt, ok := store.lookupCachedStmt(rewardLiveStakeUpsertQuery)
	require.True(t, ok, "expected the upsert to be cached")
	require.NotNil(t, upsertStmt)

	require.NotSame(
		t,
		accountStmt,
		upsertStmt,
		"the two queries must not share a cache slot",
	)
}

// TestRefreshRewardLiveStakeAggregateMatchesOneShot proves the cached path
// and the one-shot path compute identical reward_live_stake rows across an
// unregistered credential (no account row), a registered credential with a
// reward and delegated pool, and a credential whose UTxO stake changes
// across repeated touches -- the same access pattern real block processing
// uses (repeated calls through separate write transactions).
func TestRefreshRewardLiveStakeAggregateMatchesOneShot(t *testing.T) {
	t.Parallel()

	pool := make([]byte, 28)
	pool[0] = 0xAA

	cases := []struct {
		name          string
		seedAccount   bool
		reward        uint64
		utxoAmounts   []uint64
		wantUtxoStake uint64
		wantReward    uint64
	}{
		{
			name:          "no account, no utxos",
			wantUtxoStake: 0,
			wantReward:    0,
		},
		{
			name:          "registered account with reward, no utxos",
			seedAccount:   true,
			reward:        1_000_000,
			wantUtxoStake: 0,
			wantReward:    1_000_000,
		},
		{
			name:          "registered account with reward and utxos",
			seedAccount:   true,
			reward:        2_500_000,
			utxoAmounts:   []uint64{5_000_000, 7},
			wantUtxoStake: 5_000_007,
			wantReward:    2_500_000,
		},
	}

	for i, tc := range cases {
		i, tc := i, tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cachedStore := newMigratedSQLiteStore(t)
			oneShotStore := newMigratedSQLiteStore(t)

			ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
			for _, store := range []*Store{cachedStore, oneShotStore} {
				if tc.seedAccount {
					seedRewardLiveStakeAccount(t, store, ref, pool, tc.reward)
				}
				if len(tc.utxoAmounts) > 0 {
					seedCredentialUtxos(t, store, i, ref, tc.utxoAmounts, nil)
				}
			}

			require.NoError(t, cachedStore.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return cachedStore.refreshRewardLiveStakeAggregate(ctx, db, ref, 100)
				},
			))
			require.NoError(t, oneShotStore.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return oneShotRefreshRewardLiveStakeAggregate(ctx, oneShotStore, db, ref, 100)
				},
			))

			if !tc.seedAccount && len(tc.utxoAmounts) == 0 {
				// Both variants take the early DELETE-and-return path (no
				// account, zero total) and never reach the upsert, so no
				// row is ever created for either.
				requireNoRewardLiveStakeRow(t, cachedStore, ref)
				requireNoRewardLiveStakeRow(t, oneShotStore, ref)
				return
			}

			cachedUtxo, cachedReward, cachedTotal, cachedRegistered := readRewardLiveStake(t, cachedStore, ref)
			oneShotUtxo, oneShotReward, oneShotTotal, oneShotRegistered := readRewardLiveStake(t, oneShotStore, ref)

			require.Equal(t, tc.wantUtxoStake, cachedUtxo)
			require.Equal(t, tc.wantReward, cachedReward)
			require.Equal(t, oneShotUtxo, cachedUtxo)
			require.Equal(t, oneShotReward, cachedReward)
			require.Equal(t, oneShotTotal, cachedTotal)
			require.Equal(t, oneShotRegistered, cachedRegistered)
			require.Equal(t, tc.seedAccount, cachedRegistered)
		})
	}
}

// TestRefreshRewardLiveStakeAggregateReusesCachedStatementsAcrossTransactions
// proves both new cached statements are actually reused across independent
// write transactions -- the real access pattern (one write transaction per
// block/UTxO touch) -- the way
// TestSumCredentialUtxoStakeReusesCachedStatementAcrossTransactions already
// proves for sumCredentialUtxoStakeQuery.
func TestRefreshRewardLiveStakeAggregateReusesCachedStatementsAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	touch := func(slot uint64) {
		err := store.withWriteTransaction(
			nil,
			func(db queryer, ctx context.Context) error {
				return store.refreshRewardLiveStakeAggregate(ctx, db, ref, slot)
			},
		)
		require.NoError(t, err)
	}

	touch(1)

	store.stmtMu.Lock()
	firstAccountStmt := store.stmts[rewardLiveStakeAccountQuery]
	firstUpsertStmt := store.stmts[rewardLiveStakeUpsertQuery]
	store.stmtMu.Unlock()
	require.NotNil(t, firstAccountStmt)
	require.NotNil(t, firstUpsertStmt)

	seedCredentialUtxos(t, store, 1, ref, []uint64{9_000_000}, nil)
	touch(2)

	store.stmtMu.Lock()
	secondAccountStmt := store.stmts[rewardLiveStakeAccountQuery]
	secondUpsertStmt := store.stmts[rewardLiveStakeUpsertQuery]
	store.stmtMu.Unlock()

	require.Same(t, firstAccountStmt, secondAccountStmt,
		"expected the account lookup statement to be reused across transactions")
	require.Same(t, firstUpsertStmt, secondUpsertStmt,
		"expected the upsert statement to be reused across transactions")

	utxoStake, _, _, _ := readRewardLiveStake(t, store, ref)
	require.Equal(t, uint64(9_000_000), utxoStake)
}

// BenchmarkRefreshRewardLiveStakeAggregateAccountAndUpsert is the timing
// counterpart to BenchmarkSumCredentialUtxoStake: it isolates just the two
// newly cached queries (a registered credential with no live UTxOs, so
// sumCredentialUtxoStake's own cost is negligible and constant across both
// variants) to measure the one-shot-vs-cached parse-cost delta in isolation.
func BenchmarkRefreshRewardLiveStakeAggregateAccountAndUpsert(b *testing.B) {
	pool := make([]byte, 28)
	pool[0] = 0xBB

	b.Run("one_shot", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		seedRewardLiveStakeAccount(b, store, ref, pool, 1_000_000)
		for n := 0; b.Loop(); n++ {
			err := store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return oneShotRefreshRewardLiveStakeAggregate(
						ctx, store, db, ref, uint64(n+1),
					)
				},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		seedRewardLiveStakeAccount(b, store, ref, pool, 1_000_000)
		for n := 0; b.Loop(); n++ {
			err := store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return store.refreshRewardLiveStakeAggregate(
						ctx, db, ref, uint64(n+1),
					)
				},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
