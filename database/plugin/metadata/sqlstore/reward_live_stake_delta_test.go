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
// without falling back to that scan (the whole point of dingo #4421).
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

// TestSetTransactionIncrementalDeltaMatchesFullScan drives the production
// entry point (SetTransaction, not the internal helper directly) for a
// credential whose running total is already warm -- established the way an
// earlier block's touch would -- so this transaction's own certificate,
// consumed input, and produced output all take
// refreshRewardLiveStakeAggregateDelta's incremental path together, exactly
// as setTransactionWithAccumulator wires them. It proves the result matches
// a fresh authoritative scan and that no full scan ran.
func TestSetTransactionIncrementalDeltaMatchesFullScan(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x07)
	seedConsumedUtxo(t, store, fx)
	establishRunningTotal(t, store, fx.ref, 1)
	require.Equal(
		t,
		fx.consumedAmount,
		readUtxoStake(t, store, fx.ref),
		"baseline must include the UTxO this transaction is about to spend",
	)

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil,
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan through "+
			"SetTransaction",
	)

	got := readUtxoStake(t, store, fx.ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, fx.producedAmount, got)
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

// TestMergeStakeCredentialDeltasSumsPerCredential proves
// mergeStakeCredentialDeltas adds every occurrence's delta for a credential
// (unlike mergeStakeCredentialRefs, which keeps only the first), and that
// order follows first occurrence.
func TestMergeStakeCredentialDeltasSumsPerCredential(t *testing.T) {
	t.Parallel()

	a := models.NewStakeCredentialRef(0, []byte("credential-a"))
	b := models.NewStakeCredentialRef(0, []byte("credential-b"))

	t.Run("all empty", func(t *testing.T) {
		t.Parallel()
		got := mergeStakeCredentialDeltas(nil, []stakeCredentialDelta{}, nil)
		require.Empty(t, got)
	})

	t.Run(
		"sums overlapping deltas across and within slices",
		func(t *testing.T) {
			t.Parallel()
			got := mergeStakeCredentialDeltas(
				[]stakeCredentialDelta{{ref: a, delta: 5}, {ref: b, delta: -2}},
				[]stakeCredentialDelta{{ref: a, delta: -3}},
				[]stakeCredentialDelta{{ref: a, delta: 10}, {ref: b, delta: 1}},
			)
			byKey := make(map[string]int64, len(got))
			for _, d := range got {
				byKey[d.ref.MapKey()] = d.delta
			}
			require.Equal(t, int64(12), byKey[a.MapKey()]) // 5 - 3 + 10
			require.Equal(t, int64(-1), byKey[b.MapKey()]) // -2 + 1
			require.Len(t, got, 2)
		},
	)
}

// TestQueryUtxoStakeConsumedDeltasNegatesAmounts proves
// queryUtxoStakeConsumedDeltas reports the exact negative delta for each
// spent input's credential, grouping and summing when several consumed
// inputs share a credential.
func TestQueryUtxoStakeConsumedDeltasNegatesAmounts(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	refA := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
	refB := models.NewStakeCredentialRef(0, credentialKeyForIndex(1))

	seedCredentialUtxos(t, store, 0, refA, []uint64{4_000_000, 6_000_000}, nil)
	seedCredentialUtxos(t, store, 1, refB, []uint64{9_000_000}, nil)

	ids := []models.UtxoId{
		{Hash: utxoTxIDForGroup(0, 0), Idx: 0},
		{Hash: utxoTxIDForGroup(0, 1), Idx: 0},
		{Hash: utxoTxIDForGroup(1, 0), Idx: 0},
	}
	deltas, err := queryUtxoStakeConsumedDeltas(ctx, store.writeDB, ids)
	require.NoError(t, err)

	byKey := make(map[string]int64, len(deltas))
	for _, d := range deltas {
		byKey[d.ref.MapKey()] = d.delta
	}
	require.Equal(t, int64(-10_000_000), byKey[refA.MapKey()])
	require.Equal(t, int64(-9_000_000), byKey[refB.MapKey()])
	require.Len(t, deltas, 2)
}

// utxoTxIDForGroup reproduces seedCredentialUtxos' tx_id derivation from
// (group, index) so a test can look its seeded rows back up by UtxoId.
func utxoTxIDForGroup(group, index int) []byte {
	txID := make([]byte, 32)
	txID[0] = byte(group >> 24)
	txID[1] = byte(group >> 16)
	txID[2] = byte(group >> 8)
	txID[3] = byte(group)
	txID[4] = byte(index >> 24)
	txID[5] = byte(index >> 16)
	txID[6] = byte(index >> 8)
	txID[7] = byte(index)
	return txID
}

// TestRewardLiveStakeNeedsBackfillHealsCorruptedRunningTotal is the
// load-bearing test for the whole design (dingo #4421): the incremental
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

// BenchmarkRefreshRewardLiveStakeAggregateDeltaAtScale is the direct
// before/after comparison for dingo #4421: refreshRewardLiveStakeAggregate's
// full sumCredentialUtxoStake rescan against
// refreshRewardLiveStakeAggregateDelta's O(1) running-total update, for a
// credential holding as many live UTxOs as the 20,003-UTxO case the issue
// measured on a live node. Both benchmarks touch the same warmed-up
// credential; only the incremental one is expected to stay flat as n grows.
func BenchmarkRefreshRewardLiveStakeAggregateDeltaAtScale(b *testing.B) {
	for _, n := range []int{100, 1_000, 20_003} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/full_scan", n), func(b *testing.B) {
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregate(
							ctx, db, ref, 1,
						)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/incremental_delta", n), func(b *testing.B) {
			establishRunningTotal(b, store, ref, 1)
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregateDelta(
							ctx, db, ref, 1, 0,
						)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
