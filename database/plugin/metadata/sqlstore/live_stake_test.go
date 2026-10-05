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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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

// readUtxoStake reads a credential's stored reward_live_stake.utxo_stake
// column, the running total refreshRewardLiveStakeAggregateDelta maintains.
func readUtxoStake(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
) uint64 {
	tb.Helper()
	var raw string
	err := store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(ref.Tag), ref.Key,
	).Scan(&raw)
	require.NoError(tb, err)
	value, err := parseUint64("test utxo stake", raw)
	require.NoError(tb, err)
	return value
}

// establishRunningTotal seeds a credential's reward_live_stake row via the
// authoritative full-scan path, the way a credential's real first touch
// would -- refreshRewardLiveStakeAggregateDelta's tests build on this
// baseline rather than starting from an empty table, so they exercise the
// warm incremental path instead of its cold-start fallback.
func establishRunningTotal(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
	slot uint64,
) {
	tb.Helper()
	require.NoError(tb, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return store.refreshRewardLiveStakeAggregate(ctx, db, ref, slot)
		},
	))
}

// insertLiveUtxoTx inserts one additional live UTxO row for ref through db
// (the caller's write-transaction handle), so it participates in the same
// transaction as a refreshRewardLiveStakeAggregateDelta call issued
// alongside it -- mirroring how a real gain and its delta application share
// one write transaction. txSeed must be distinct per call within a test to
// avoid tx_id collisions with other rows the test seeds.
func insertLiveUtxoTx(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	txSeed byte,
	amount uint64,
) error {
	txID := make([]byte, 32)
	txID[0] = txSeed
	_, err := db.ExecContext(ctx, `
INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot, amount)
VALUES (?, 0, ?, ?, 1, 0, ?)`,
		txID, ref.Key, int64(ref.Tag), decimalUint64(types.Uint64(amount)),
	)
	return err
}

// markUtxoDeletedTx marks the one live UTxO of the given amount deleted
// through db, failing if that does not match exactly one row -- a test
// asserting a specific loss delta needs to know precisely which row it
// removed.
func markUtxoDeletedTx(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	amount uint64,
	deletedSlot int64,
) error {
	result, err := db.ExecContext(
		ctx,
		`
UPDATE utxo SET deleted_slot = ?
WHERE credential_tag = ? AND staking_key = ? AND amount = ? AND deleted_slot = 0`,
		deletedSlot,
		int64(ref.Tag),
		ref.Key,
		decimalUint64(types.Uint64(amount)),
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf(
			"expected exactly one live UTxO of amount %d, affected %d",
			amount,
			affected,
		)
	}
	return nil
}

// TestRefreshRewardLiveStakeAggregateDeltaGain proves a single UTxO gain
// applied through the incremental path produces the same stored total a
// fresh authoritative sumCredentialUtxoStake scan would, and that it does so
// without falling back to that scan.
func TestRefreshRewardLiveStakeAggregateDeltaGain(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{1_000_000, 2_000_000, 3_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(6_000_000), readUtxoStake(t, store, ref))

	const gain = 500_000
	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			if err := insertLiveUtxoTx(ctx, db, ref, 0x10, gain); err != nil {
				return err
			}
			return store.refreshRewardLiveStakeAggregateDelta(
				ctx, db, ref, 2, gain,
			)
		},
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, uint64(6_500_000), got)
}

// TestRefreshRewardLiveStakeAggregateDeltaLoss is
// TestRefreshRewardLiveStakeAggregateDeltaGain's counterpart for a spend.
func TestRefreshRewardLiveStakeAggregateDeltaLoss(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{1_000_000, 2_000_000, 3_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(6_000_000), readUtxoStake(t, store, ref))

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			if err := markUtxoDeletedTx(ctx, db, ref, 2_000_000, 2); err != nil {
				return err
			}
			return store.refreshRewardLiveStakeAggregateDelta(
				ctx, db, ref, 2, -2_000_000,
			)
		},
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, uint64(4_000_000), got)
}

// TestRefreshRewardLiveStakeAggregateDeltaRapidSequenceWithinOneBlock applies
// several gains and losses for the same credential inside one write
// transaction -- the shape a block with several transactions touching the
// same address produces, all sharing one *sql.Tx -- and proves the final
// running total matches a fresh authoritative scan of the resulting UTxO
// set, with no fallback scan anywhere in the sequence.
func TestRefreshRewardLiveStakeAggregateDeltaRapidSequenceWithinOneBlock(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	// Baseline: A=10,000,000 (survives every step), C=1,000,000 and
	// E=3,000,000 (each spent by one of the steps below).
	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{10_000_000, 1_000_000, 3_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(14_000_000), readUtxoStake(t, store, ref))

	type step struct {
		delta  int64
		mutate func(db queryer, ctx context.Context) error
	}
	steps := []step{
		{2_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x20, 2_000_000) // +B
		}},
		{-1_000_000, func(db queryer, ctx context.Context) error {
			return markUtxoDeletedTx(ctx, db, ref, 1_000_000, 2) // -C
		}},
		{5_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x21, 5_000_000) // +D
		}},
		{-3_000_000, func(db queryer, ctx context.Context) error {
			return markUtxoDeletedTx(ctx, db, ref, 3_000_000, 2) // -E
		}},
		{1_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x22, 1_000_000) // +F
		}},
	}

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			for _, st := range steps {
				if err := st.mutate(db, ctx); err != nil {
					return err
				}
				if err := store.refreshRewardLiveStakeAggregateDelta(
					ctx, db, ref, 2, st.delta,
				); err != nil {
					return err
				}
			}
			return nil
		},
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must never fall back to a full scan mid-sequence",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	// 10,000,000 (A) + 2,000,000 (B) + 5,000,000 (D) + 1,000,000 (F); C and E
	// were spent along the way.
	require.Equal(t, uint64(18_000_000), got)
}

// TestRefreshRewardLiveStakeAggregateDeltaAcrossMultipleTransactions proves
// the running total is correctly persisted and re-read across separate,
// independently committed write transactions -- the shape several blocks
// touching the same credential over time produce -- rather than depending on
// any in-memory state carried between calls.
func TestRefreshRewardLiveStakeAggregateDeltaAcrossMultipleTransactions(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(t, store, 0, ref, []uint64{10_000_000}, nil)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(10_000_000), readUtxoStake(t, store, ref))

	type step struct {
		delta  int64
		mutate func(db queryer, ctx context.Context) error
	}
	steps := []step{
		{2_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x30, 2_000_000)
		}},
		{-2_000_000, func(db queryer, ctx context.Context) error {
			return markUtxoDeletedTx(ctx, db, ref, 2_000_000, 3)
		}},
		{7_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x31, 7_000_000)
		}},
	}

	before := store.sumCredentialUtxoStakeCalls.Load()
	for i, st := range steps {
		slot := uint64(2 + i)
		require.NoError(t, store.withWriteTransaction(
			nil,
			func(db queryer, ctx context.Context) error {
				if err := st.mutate(db, ctx); err != nil {
					return err
				}
				return store.refreshRewardLiveStakeAggregateDelta(
					ctx, db, ref, slot, st.delta,
				)
			},
		))
	}
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must never fall back to a full scan across "+
			"separate transactions",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, uint64(17_000_000), got)
}

// TestApplyUtxoStakeDeltaOverflowAndUnderflow covers applyUtxoStakeDelta's
// negative cases directly: a delta whose magnitude the current total cannot
// absorb must fail closed rather than wrap silently, in both directions.
func TestApplyUtxoStakeDeltaOverflowAndUnderflow(t *testing.T) {
	t.Parallel()
	ref := models.NewStakeCredentialRef(0, []byte("credential"))

	t.Run("ordinary increase", func(t *testing.T) {
		t.Parallel()
		got, err := applyUtxoStakeDelta(100, 50, ref)
		require.NoError(t, err)
		require.Equal(t, uint64(150), got)
	})

	t.Run("ordinary decrease", func(t *testing.T) {
		t.Parallel()
		got, err := applyUtxoStakeDelta(100, -40, ref)
		require.NoError(t, err)
		require.Equal(t, uint64(60), got)
	})

	t.Run("decrease to exactly zero", func(t *testing.T) {
		t.Parallel()
		got, err := applyUtxoStakeDelta(100, -100, ref)
		require.NoError(t, err)
		require.Equal(t, uint64(0), got)
	})

	t.Run("underflow", func(t *testing.T) {
		t.Parallel()
		_, err := applyUtxoStakeDelta(100, -101, ref)
		require.Error(t, err)
		require.ErrorContains(t, err, "underflow")
	})

	t.Run("overflow", func(t *testing.T) {
		t.Parallel()
		_, err := applyUtxoStakeDelta(^uint64(0), 1, ref)
		require.Error(t, err)
		require.ErrorContains(t, err, "overflow")
	})
}

// TestRewardLiveStakeNeedsBackfillHealsCorruptedRunningTotal is the
// load-bearing test for the whole design: the incremental
// running total this change introduces trades away sumCredentialUtxoStake's
// self-healing full-scan property, so it depends entirely on
// RewardLiveStakeNeedsBackfill (run at every node startup, before block
// application resumes -- see Node.backfillRewardLiveStake) actually
// detecting a corrupted running total and RebuildRewardLiveStake actually
// correcting it. This proves both halves against a corruption that exactly
// simulates a missed or double-applied delta: the stored total is wrong, and
// nothing about the incremental path itself would ever notice.
func TestRewardLiveStakeNeedsBackfillHealsCorruptedRunningTotal(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(
		t, store, 0, ref, []uint64{10_000_000, 5_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(15_000_000), readUtxoStake(t, store, ref))

	// Simulate a missed/double-applied delta by corrupting only the stored
	// utxo_stake column directly, without touching the utxo table it is
	// supposed to track and without touching total_stake. This is
	// deliberately not a call through any production code path: it stands in
	// for a bug in that path, and leaving total_stake at its previous
	// (correct) value isolates RewardLiveStakeNeedsBackfill's utxo_stake
	// comparison specifically -- total_stake alone would still agree with
	// the authoritative recomputation here (reward_stake is 0 for this
	// unregistered credential), so this corruption is only caught if the
	// utxo_stake check runs.
	const corrupted = "999999999"
	_, err := store.writeDB.ExecContext(ctx, `
UPDATE reward_live_stake SET utxo_stake = ?
WHERE credential_tag = ? AND staking_key = ?`,
		corrupted, int64(ref.Tag), ref.Key,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(999_999_999),
		readUtxoStake(t, store, ref),
		"corruption must actually take hold before reconciliation can be "+
			"asked to fix it",
	)

	authoritative, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.NotEqual(
		t,
		authoritative,
		readUtxoStake(t, store, ref),
		"the corrupted value must actually disagree with the authoritative "+
			"scan, or this test proves nothing",
	)

	needed, err := store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(
		t,
		needed,
		"expected the corrupted running total to be detected as drift",
	)

	require.NoError(t, store.RebuildRewardLiveStake(2, nil))

	require.Equal(
		t,
		authoritative,
		readUtxoStake(t, store, ref),
		"expected the corrupted running total healed back to the "+
			"authoritative scan",
	)

	stillNeeded, err := store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.False(
		t,
		stillNeeded,
		"expected no further drift to be reported after the rebuild",
	)
}

// TestRebuildRewardLiveStakeFromRunningTotalsLatestDelegationSelection runs
// the same shapes through the Mithril finalization path, which reaches the
// same ranked query with the running-total join attached.
func TestRebuildRewardLiveStakeFromRunningTotalsLatestDelegationSelection(
	t *testing.T,
) {
	t.Parallel()
	authoritative := newMigratedSQLiteStore(t)
	runningTotals := newMigratedSQLiteStore(t)
	for _, store := range []*Store{authoritative, runningTotals} {
		populateRewardLiveStakeRebuildFixture(t, store)
		insertLatestDelegationRow(
			t, store, 0, []byte{0x10, 0x11}, []byte{0xa0, 0xa1}, 12, 3, 4,
		)
		insertLatestDelegationRow(
			t, store, 0, []byte{0x10, 0x11}, []byte{0xc0, 0xc1}, 13, 4, 5,
		)
		insertLatestDelegationRow(
			t, store, 1, []byte{0x30, 0x31}, []byte{0xb0, 0xb1}, 32, 6, 7,
		)
	}
	require.NoError(t, authoritative.RebuildRewardLiveStake(100, nil))
	require.NoError(
		t,
		runningTotals.RebuildRewardLiveStakeFromRunningTotals(100, nil),
	)
	require.Equal(
		t,
		readRewardLiveStakeSnapshot(t, authoritative),
		readRewardLiveStakeSnapshot(t, runningTotals),
	)
	snapshot := readRewardLiveStakeSnapshot(t, runningTotals)
	require.Equal(t, int64(32), snapshotRow(t, snapshot, 1, []byte{0x30, 0x31}).delegationSlot)
	require.Equal(t, int64(10), snapshotRow(t, snapshot, 0, []byte{0x10, 0x11}).delegationSlot)
}

func populateRewardLiveStakeRebuildFixture(t testing.TB, store *Store) {
	t.Helper()
	delegated := models.NewStakeCredentialRef(0, []byte{0x10, 0x11})
	accountOnly := models.NewStakeCredentialRef(0, []byte{0x20, 0x21})
	scriptDelegated := models.NewStakeCredentialRef(1, []byte{0x30, 0x31})
	for _, account := range []*models.Account{
		{
			StakingKey: delegated.Key, CredentialTag: delegated.Tag,
			Pool: []byte{0xa0, 0xa1}, AddedSlot: 10, CreatedSlot: 10,
			Reward: types.Uint64(100), Active: true,
		},
		{
			StakingKey: accountOnly.Key, CredentialTag: accountOnly.Tag,
			AddedSlot: 20, CreatedSlot: 20, Reward: types.Uint64(7), Active: true,
		},
		{
			StakingKey: scriptDelegated.Key, CredentialTag: scriptDelegated.Tag,
			Pool: []byte{0xb0, 0xb1}, AddedSlot: 30, CreatedSlot: 30,
			Reward: types.Uint64(11), Active: true,
		},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	utxos := []models.Utxo{
		{
			TxId: bytesForRebuildTest(0x41), StakingKey: delegated.Key,
			CredentialTag: delegated.Tag, AddedSlot: 11, Amount: types.Uint64(50),
		},
		{
			TxId: bytesForRebuildTest(0x42), StakingKey: delegated.Key,
			CredentialTag: delegated.Tag, AddedSlot: 12, Amount: types.Uint64(75),
		},
		{
			TxId: bytesForRebuildTest(0x43), StakingKey: scriptDelegated.Key,
			CredentialTag: scriptDelegated.Tag, AddedSlot: 31, Amount: types.Uint64(9),
		},
	}
	require.NoError(t, store.ImportUtxos(utxos, nil))
}

func readRewardLiveStakeSnapshot(
	t testing.TB,
	store *Store,
) map[string]rewardLiveStakeSnapshotRow {
	t.Helper()
	rows, err := store.writeDB.Query(`
SELECT credential_tag, staking_key, pool_key_hash,
       utxo_stake, reward_stake, total_stake, registered,
       pool_delegation_slot, pool_delegation_block_index,
       pool_delegation_cert_index, updated_slot, calculation_version
FROM reward_live_stake ORDER BY credential_tag, staking_key`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	ret := make(map[string]rewardLiveStakeSnapshotRow)
	for rows.Next() {
		var row rewardLiveStakeSnapshotRow
		var tag int64
		var key, pool []byte
		require.NoError(t, rows.Scan(
			&tag, &key, &pool, &row.utxoStake, &row.rewardStake,
			&row.totalStake, &row.registered, &row.delegationSlot,
			&row.delegationBlock, &row.delegationCert, &row.updatedSlot,
			&row.calculationVersion,
		))
		row.tag = fmt.Sprintf("%d", tag)
		row.key = string(key)
		row.pool = string(pool)
		ret[row.tag+":"+row.key] = row
	}
	require.NoError(t, rows.Err())
	return ret
}

// TestRebuildRewardLiveStakeFromRunningTotalsMatchesAuthoritativeRebuild
// compares every aggregate column on one fixture, including an account with
// no UTxO and key/script credentials delegated to pools.
func TestRebuildRewardLiveStakeFromRunningTotalsMatchesAuthoritativeRebuild(
	t *testing.T,
) {
	t.Parallel()
	authoritative := newMigratedSQLiteStore(t)
	runningTotals := newMigratedSQLiteStore(t)
	populateRewardLiveStakeRebuildFixture(t, authoritative)
	populateRewardLiveStakeRebuildFixture(t, runningTotals)
	_, err := runningTotals.writeDB.Exec(`
INSERT INTO reward_live_stake
    (credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
     registered, updated_slot, calculation_version)
VALUES (0, ?, '999', '999', '1998', false, 1, 0)`, []byte{0xee, 0xef})
	require.NoError(t, err)
	require.NoError(t, authoritative.RebuildRewardLiveStake(100, nil))
	require.NoError(t, runningTotals.RebuildRewardLiveStakeFromRunningTotals(100, nil))
	require.Equal(
		t,
		readRewardLiveStakeSnapshot(t, authoritative),
		readRewardLiveStakeSnapshot(t, runningTotals),
	)
}

func TestRebuildRewardLiveStakeFromRunningTotalsRejectsMissingUtxoTotal(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, []byte{0x51, 0x52})
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:          bytesForRebuildTest(0x53),
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     11,
		Amount:        types.Uint64(50),
	}}, nil))
	_, err := store.writeDB.Exec(
		`DELETE FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag,
		ref.Key,
	)
	require.NoError(t, err)
	err = store.RebuildRewardLiveStakeFromRunningTotals(100, nil)
	require.ErrorContains(t, err, "missing reward live stake running total")
}

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
		got, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
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

// TestSumCredentialUtxoStakeConcurrentReuse drives many goroutines through
// the cached statement concurrently, each against a distinct credential, and
// must be run with -race. It exercises both cachedStmt's first-use race
// (goroutines racing to populate the cache) and repeated concurrent use of
// the same *sql.Stmt once installed -- both documented safe by
// database/sql, but real regressions in that area are exactly the kind of
// bug a scalar-looking cache like this one can hide without a concurrent
// test.
func TestSumCredentialUtxoStakeConcurrentReuse(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	const goroutines = 8
	const iterations = 25
	refs := make([]models.StakeCredentialRef, goroutines)
	want := make([]uint64, goroutines)
	for i := range refs {
		refs[i] = models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
		amounts := []uint64{uint64(1_000_000 + i), uint64(2_000_000 + i)}
		seedCredentialUtxos(t, store, i, refs[i], amounts, nil)
		want[i] = amounts[0] + amounts[1]
	}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for g := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for range iterations {
				got, err := store.sumCredentialUtxoStake(
					ctx,
					store.writeDB,
					refs[idx],
				)
				if err != nil {
					errCh <- err
					return
				}
				if got != want[idx] {
					errCh <- fmt.Errorf(
						"credential %d: got %d want %d",
						idx,
						got,
						want[idx],
					)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	store.stmtMu.Lock()
	entries := len(store.stmts)
	store.stmtMu.Unlock()
	// hotStatements now has more than just sumCredentialUtxoStakeQuery (see
	// prepared_stmt.go); this asserts concurrent first use created no
	// spurious extra entry beyond the fixed set Start prepares eagerly.
	require.Equal(
		t,
		len(hotStatements),
		entries,
		"expected concurrent first use to converge on the fixed set of cached statements",
	)
}

// TestStaleConsensusStakeSnapshotsExistFailsClosed covers the fail-closed
// gate itself: every prior test writes the symbolic
// current version, so none of them exercise a literal old
// calculation_version tripping the gate. It also covers finding 2: a
// non-authoritative (fallback) Mark reward_snapshot row must fail the gate
// on its own, not by relying on authoritativeMarkRewardSnapshotExists
// rejecting the version mismatch separately.
func TestStaleConsensusStakeSnapshotsExistFailsClosed(t *testing.T) {
	t.Parallel()

	t.Run("current version only reports not stale", func(t *testing.T) {
		t.Parallel()
		store := newManagementTestStore(t)
		poolKey := bytes.Repeat([]byte{0x44}, 28)
		require.NoError(
			t,
			store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
				Epoch:              20,
				SnapshotType:       models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:        poolKey,
				CalculationVersion: models.RewardStakeCalculationVersion,
			}, nil),
		)
		require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:              20,
			SnapshotType:       models.PoolStakeSnapshotTypeMark,
			Authoritative:      true,
			CalculationVersion: models.RewardStakeCalculationVersion,
		}, nil))
		stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
		require.NoError(t, err)
		require.False(t, stale)
	})

	t.Run(
		"literal old pool_stake_snapshot version trips the gate",
		func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			poolKey := bytes.Repeat([]byte{0x55}, 28)
			require.NoError(
				t,
				store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
					Epoch:              21,
					SnapshotType:       models.PoolStakeSnapshotTypeMark,
					PoolKeyHash:        poolKey,
					CalculationVersion: 1,
				}, nil),
			)
			stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
			require.NoError(t, err)
			require.True(t, stale)
			epochs, err := store.StaleConsensusStakeSnapshotEpochs(nil)
			require.NoError(t, err)
			require.Equal(t, []uint64{21}, epochs)
		},
	)

	t.Run(
		"literal old authoritative reward_snapshot version trips the gate",
		func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:              22,
				SnapshotType:       models.PoolStakeSnapshotTypeMark,
				Authoritative:      true,
				CalculationVersion: 1,
			}, nil))
			stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
			require.NoError(t, err)
			require.True(t, stale)
		},
	)

	t.Run(
		"literal old non-authoritative fallback reward_snapshot version trips the gate",
		func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:              23,
				SnapshotType:       models.PoolStakeSnapshotTypeMark,
				Authoritative:      false,
				CalculationVersion: 1,
			}, nil))
			stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
			require.NoError(t, err)
			require.True(t, stale)
			epochs, err := store.StaleConsensusStakeSnapshotEpochs(nil)
			require.NoError(t, err)
			require.Equal(t, []uint64{23}, epochs)
		},
	)
}

func (fakePrepareOnlyQueryer) ExecContext(
	context.Context,
	string,
	...any,
) (sql.Result, error) {
	return nil, errors.New("fakePrepareOnlyQueryer: ExecContext not implemented")
}

func (fakePrepareOnlyQueryer) QueryContext(
	context.Context,
	string,
	...any,
) (*sql.Rows, error) {
	return nil, errors.New("fakePrepareOnlyQueryer: QueryContext not implemented")
}

func (fakePrepareOnlyQueryer) QueryRowContext(
	context.Context,
	string,
	...any,
) *sql.Row {
	return nil
}

func (f fakePrepareOnlyQueryer) PrepareContext(
	context.Context,
	string,
) (*sql.Stmt, error) {
	return nil, f.prepareErr
}
