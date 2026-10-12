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

package snapshot

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestCaptureEpochBoundaryUsesSnapPointStake proves the authoritative capture
// persists the stake read at the SNAP point rather than whatever the live
// aggregate holds when the snapshot row is finally written.
//
// cardano-ledger runs SNAP before POOLREAP and before governance enactment, so
// the mark snapshot must not contain the reward-account credits those later rules
// apply at the boundary slot. Here the live aggregate is mutated between the two
// phases, standing in for exactly those credits; the persisted snapshot must
// still show the SNAP-point value.
func TestCaptureEpochBoundaryUsesSnapPointStake(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolSNAP_1234567890123456789")
	stakingKey := bytes.Repeat([]byte{0x5a}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432_000,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 8,
		SnapshotSlot:    431_999,
	}

	txn := db.Transaction(context.Background(), true)
	// SNAP point: stake read before any post-SNAP boundary rule runs.
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), txn, evt,
	))
	// Post-SNAP boundary credit (POOLREAP refund / MIR / treasury withdrawal /
	// proposal refund), applied inside the same rollover transaction. It raises
	// the live reward aggregate at the boundary slot, which is what the capture
	// used to absorb.
	require.NoError(
		t,
		db.AddPostSnapshotAccountRewardByCredential(
			context.Background(),
			0,
			stakingKey,
			1_000_000,
			evt.BoundarySlot,
			bytes.Repeat([]byte{0xc1}, 32),
			txn,
		),
	)
	// End of rollover: persist, now that the new epoch row would exist.
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn, evt,
	))
	require.NoError(t, txn.Commit())

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(
		t,
		uint64(40_000_000),
		uint64(poolSnapshot.TotalStake),
		"mark snapshot must hold SNAP-point stake, not post-SNAP boundary credits",
	)

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	require.True(t, rewardSnapshot.Authoritative)
	require.Equal(
		t,
		uint64(40_000_000),
		uint64(rewardSnapshot.TotalActiveStake),
		"reward basis must hold SNAP-point stake too",
	)

	inputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(40_000_000), uint64(inputs[0].Stake))
}

// TestCaptureEpochBoundaryMissingSnapHookUsesHistoricalStake proves that the
// persist fallback cannot use the live aggregate merely because the database
// tip is still at/before the snapshot slot. A missing SNAP hook is exactly the
// failure mode where the fallback runs after a post-SNAP boundary credit.
func TestCaptureEpochBoundaryMissingSnapHookUsesHistoricalStake(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolFALLBACK_123456789012345")
	stakingKey := bytes.Repeat([]byte{0x5d}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432_000,
		EpochNonce:      []byte{0x0c, 0x0d},
		ProtocolVersion: 8,
		SnapshotSlot:    431_999,
	}

	txn := db.Transaction(context.Background(), true)
	require.NoError(
		t,
		db.AddPostSnapshotAccountRewardByCredential(
			context.Background(),
			0,
			stakingKey,
			1_000_000,
			evt.BoundarySlot,
			bytes.Repeat([]byte{0xc3}, 32),
			txn,
		),
	)
	// Do not call ComputeEpochBoundarySnapshot: this simulates a missing or
	// failed SNAP read. The fallback runs after the credit in this transaction.
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn, evt,
	))
	require.NoError(t, txn.Commit())

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(
		t,
		uint64(40_000_000),
		uint64(poolSnapshot.TotalStake),
		"missing SNAP hook must reconstruct pre-credit stake, not read live aggregate",
	)
}

// TestCaptureEpochBoundaryIgnoresStaleSnapPointStake proves the SNAP-point
// handoff is bound to the transaction that produced it: a distribution left
// behind by a rolled-back rollover is discarded even when the retry has the
// same boundary identity, and the capture reconstructs the historical SNAP
// value rather than attaching the stale live aggregate.
func TestCaptureEpochBoundaryIgnoresStaleSnapPointStake(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 1, StartSlot: 432000, LengthInSlots: 432000},
	})

	poolHash := []byte("poolSTALE_123456789012345678")
	stakingKey := bytes.Repeat([]byte{0x5b}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)
	// Commit a post-SNAP credit before the abandoned transaction. The live
	// aggregate is now 55M, while boundary-aware reconstruction must subtract
	// the credit and recover the 40M SNAP value.
	creditTxn := db.Transaction(context.Background(), true)
	require.NoError(
		t,
		db.AddPostSnapshotAccountRewardByCredential(
			context.Background(),
			0,
			stakingKey,
			15_000_000,
			432_000,
			bytes.Repeat([]byte{0xc2}, 32),
			creditTxn,
		),
	)
	require.NoError(t, creditTxn.Commit())

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	abandoned := event.EpochTransitionEvent{
		PreviousEpoch: 0,
		NewEpoch:      1,
		BoundarySlot:  432_000,
		SnapshotSlot:  431_999,
	}
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), txn, abandoned,
	))
	require.NoError(t, txn.Rollback())

	// Retry the exact same boundary. Matching only the event fields would
	// incorrectly reuse the abandoned 55M live read here.
	next := abandoned
	txn = db.Transaction(context.Background(), true)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn, next,
	))
	require.NoError(t, txn.Commit())

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(
		t,
		uint64(40_000_000),
		uint64(poolSnapshot.TotalStake),
		"fallback must reconstruct the boundary rather than reuse a rolled-back SNAP read",
	)
}

// TestCalculateStakeDistributionDedupesCredentialAcrossPools proves the
// duplicate-credential collapse actually reaches the snapshot aggregates.
//
// reward_live_stake is unique on (credential_tag, staking_key), so a credential
// cannot legitimately hold stake under two pools. Rows seeded before that index
// was unique can, and such a duplicate previously made the per-credential reward
// inputs disagree with the per-pool aggregate and crashed reward application at an
// epoch rollover. The dedupe existed, but only inside a throwaway validation copy,
// so PoolStakes, TotalStake, DelegatorCount — and from them PoolStakeSnapshot and
// EpochSummary — still double-counted the credential.
func TestCalculateStakeDistributionDedupesCredentialAcrossPools(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolA := []byte("poolDUPA_1234567890123456789")
	poolB := []byte("poolDUPB_1234567890123456789")
	stakingKey := bytes.Repeat([]byte{0x5c}, 28)
	seedPoolAndDelegations(t, db, poolA, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{10}},
	}, 100)
	seedPoolAndDelegations(t, db, poolB, nil, 100)

	raw := snapshotSQLDB(t, db)
	// Reproduce a pre-unique-index database: drop the constraint, then add the
	// duplicate credential row it would now reject.
	_, err := raw.Exec("DROP INDEX IF EXISTS idx_reward_live_stake_cred")
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO reward_live_stake
 (credential_tag, staking_key, pool_key_hash, utxo_stake, reward_stake,
  total_stake, registered, pool_delegation_slot, updated_slot, calculation_version)
 VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?, 0)`, 0, stakingKey, poolB, "10", "0", "10", 100, 100)
	require.NoError(t, err)

	var rows int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM reward_live_stake WHERE staking_key = ?",
		stakingKey,
	).Scan(&rows))
	require.Equal(t, 2, rows, "fixture must hold the duplicate rows")

	calc := NewCalculator(db)
	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Commit() }()
	dist, err := calc.calculateStakeDistributionInTxn(
		context.Background(), txn, 100, 0, 0,
	)
	require.NoError(t, err)

	var keyA, keyB lcommon.PoolKeyHash
	copy(keyA[:], poolA)
	copy(keyB[:], poolB)
	require.Equal(t, uint64(10), dist.TotalStake,
		"a duplicated credential must contribute its stake exactly once")
	require.Len(t, dist.StakeInputs, 1)
	require.Equal(
		t,
		uint64(1),
		dist.DelegatorCount[keyA]+dist.DelegatorCount[keyB],
		"a duplicated credential must be counted as one delegator",
	)
	require.Equal(t, uint64(10), dist.PoolStakes[keyA]+dist.PoolStakes[keyB])
	require.Contains(t, dist.PoolStakes, keyA)
	require.Contains(t, dist.PoolStakes, keyB)
	require.Zero(t, dist.PoolStakes[keyA])
	require.Equal(t, uint64(10), dist.PoolStakes[keyB],
		"only the retained assignment may hold the credential's stake")
}

// TestCalculateEpochBoundaryFallbackHalvesAgree covers the fallback capture
// mixing sources. When live state has advanced past the boundary the fallback
// reconstructs the leader-election pool totals historically; the per-credential
// reward basis must come from that same reconstruction rather than from the live
// reward aggregate, which has no slot predicate. Otherwise one mark snapshot
// carries a boundary-accurate pool total against post-boundary per-credential
// stake, and nothing compares the two.
func TestCalculateEpochBoundaryFallbackHalvesAgree(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name             string
		expiryEpoch      uint64
		inactivityPeriod uint64
	}{
		{name: "CIP-0163 gate off"},
		{
			name:             "CIP-0163 gate on",
			expiryEpoch:      3,
			inactivityPeriod: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := setupTestDB(t)
			seedEpochs(t, db, []models.Epoch{
				{EpochId: 0, StartSlot: 0, LengthInSlots: 100},
				{EpochId: 1, StartSlot: 100, LengthInSlots: 100},
				{EpochId: 2, StartSlot: 200, LengthInSlots: 100},
				{EpochId: 3, StartSlot: 300, LengthInSlots: 100},
			})

			poolHash := bytes.Repeat([]byte{0xb1}, 28)
			stakingKey := bytes.Repeat([]byte{0x5d}, 28)
			seedPoolAndDelegations(t, db, poolHash, []struct {
				stakingKey  []byte
				utxoAmounts []types.Uint64
			}{
				{stakingKey: stakingKey, utxoAmounts: []types.Uint64{50}},
			}, 100)

			// Make the live aggregate disagree with the historical
			// reconstruction, standing in for stake that moved after the
			// boundary.
			raw := snapshotSQLDB(t, db)
			_, err := raw.Exec(
				"UPDATE reward_live_stake SET total_stake = ? WHERE staking_key = ?",
				"75",
				stakingKey,
			)
			require.NoError(t, err)
			// Tip past the snapshot slot selects the historical fallback.
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 150,
					Hash: bytes.Repeat([]byte{0x0f}, 32),
				},
				BlockNumber: 2,
			}, nil))

			calc := NewCalculator(db)
			txn := db.Transaction(context.Background(), false)
			defer func() { _ = txn.Commit() }()
			dist, err := calc.calculateBoundaryStakeDistributionInTxn(
				context.Background(),
				txn,
				100,
				101,
				test.expiryEpoch,
				test.inactivityPeriod,
			)
			require.NoError(t, err)

			var pool lcommon.PoolKeyHash
			copy(pool[:], poolHash)
			var inputSum uint64
			for _, input := range dist.StakeInputs {
				require.Equal(t, poolHash, input.PoolKeyHash)
				inputSum += input.Stake
			}
			require.Equal(t, uint64(50), dist.PoolStakes[pool],
				"leader-election total stays slot-accurate")
			require.Equal(t, dist.PoolStakes[pool], inputSum,
				"the reward basis must sum to the leader-election pool total")
		})
	}
}

// TestCaptureEpochBoundaryIncludesPriorBoundaryPostSnapshotCreditOnce covers
// the snapshot-capture half of the ordering depends
// on: a post-snapshot boundary credit (POOLREAP refund, enacted treasury
// withdrawal, or governance proposal-deposit refund) applied at epoch N's
// boundary must be reflected exactly once in epoch N+1's mark snapshot, not
// zero or twice. This passes both before and after the fix -- the SNAP
// read/write split it exercises was already correct; the defect was in when
// ledger/governance/epoch.go applied a proposal-deposit refund credit in the
// first place (one epoch too early), which
// TestProcessEpochDropsExpiredProposalAndRefundsDepositNextEpoch in
// ledger/governance/epoch_test.go pins as the fail-before/pass-after
// regression test.
func TestCaptureEpochBoundaryIncludesPriorBoundaryPostSnapshotCreditOnce(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 1, StartSlot: 432000, LengthInSlots: 432000},
	})

	poolHash := []byte("poolSNAP_1234567890123456789")
	stakingKey := bytes.Repeat([]byte{0x5a}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)

	// Epoch 0 -> 1 boundary: SNAP, then a post-SNAP boundary credit (e.g. a
	// governance proposal-deposit refund), matching proposal
	// refund at the epoch677 boundary.
	evt1 := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432_000,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 8,
		SnapshotSlot:    431_999,
	}
	txn1 := db.Transaction(context.Background(), true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), txn1, evt1,
	))
	require.NoError(
		t,
		db.AddPostSnapshotAccountRewardByCredential(
			context.Background(),
			0,
			stakingKey,
			1_000_000,
			evt1.BoundarySlot,
			bytes.Repeat([]byte{0xc1}, 32),
			txn1,
		),
	)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn1, evt1,
	))
	require.NoError(t, txn1.Commit())

	// Sanity: mark[1] must still exclude the credit (existing invariant).
	mark1, err := db.Metadata().GetPoolStakeSnapshot(1, "mark", poolHash, nil)
	require.NoError(t, err)
	require.NotNil(t, mark1)
	require.Equal(t, uint64(40_000_000), uint64(mark1.TotalStake))

	// Epoch 1 -> 2 boundary: a full epoch later, with no further credits.
	// The proposal-deposit refund from the previous boundary is now ordinary
	// pre-SNAP history and must be counted exactly once.
	evt2 := event.EpochTransitionEvent{
		PreviousEpoch:   1,
		NewEpoch:        2,
		BoundarySlot:    864_000,
		EpochNonce:      []byte{0x0c, 0x0d},
		ProtocolVersion: 8,
		SnapshotSlot:    863_999,
	}
	txn2 := db.Transaction(context.Background(), true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), txn2, evt2,
	))
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn2, evt2,
	))
	require.NoError(t, txn2.Commit())

	mark2, err := db.Metadata().GetPoolStakeSnapshot(2, "mark", poolHash, nil)
	require.NoError(t, err)
	require.NotNil(t, mark2)
	require.Equal(
		t,
		uint64(41_000_000),
		uint64(mark2.TotalStake),
		"mark[N+1] must count a prior boundary's post-snapshot credit exactly once",
	)
}

// TestCurrentBoundarySPOStakeRows_FallsBackToHistoricalReconstruction covers
// governance's same-boundary SPO read when no
// ComputeEpochBoundarySnapshot stash exists for this boundary (the hook was
// never installed, or its fast path failed): it must still return the
// correct rows via the same historical reconstruction the persisted write
// itself falls back to, with the CIP-1694 reward-account auto-vote
// resolved.
func TestCurrentBoundarySPOStakeRows_FallsBackToHistoricalReconstruction(
	t *testing.T,
) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("gbfallback_pool_1234567890AB")
	stakingKey := bytes.Repeat([]byte{0xf1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{75_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch: 0,
		NewEpoch:      1,
		BoundarySlot:  432000,
		SnapshotSlot:  431999,
	}

	txn := db.Transaction(context.Background(), true)
	rows, err := mgr.CurrentBoundarySPOStakeRows(
		context.Background(), txn, evt,
	)
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	require.Len(t, rows, 1)
	require.Equal(t, poolHash, rows[0].PoolKeyHash)
	require.Equal(t, uint64(75_000_000), uint64(rows[0].TotalStake))
	require.Equal(t, uint64(1), rows[0].Epoch)
	require.Equal(t, "mark", rows[0].SnapshotType)
	require.True(t, rows[0].RewardAccountAutoVoteResolved,
		"a pool row with no reward account is a confirmed None outcome")
	require.Equal(
		t,
		models.PoolRewardAccountAutoVoteNone,
		rows[0].RewardAccountAutoVote,
	)

	// The fallback path must not have written anything: the durable
	// pool_stake_snapshot row is still CaptureEpochBoundarySnapshot's job,
	// which runs later in the same rollover transaction.
	persisted, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(1, "mark", nil)
	require.NoError(t, err)
	require.Empty(t, persisted,
		"CurrentBoundarySPOStakeRows must not persist anything")
}

// TestCurrentBoundarySPOStakeRows_PeeksWithoutConsumingComputedStash proves
// governance's same-boundary read and the later authoritative persist both
// see the exact SNAP-point distribution ComputeEpochBoundarySnapshot stashed
// earlier in the same rollover transaction -- the peek must not disturb the
// later take, and neither read may fall back to recomputing it from scratch.
//
// The stashed distribution's pool stake is overwritten in place with a
// sentinel value after stashing (a white-box mutation only this package can
// make) so that both requirements are verifiable by an identical assertion:
// a call that reads via take-then-clear, or a call that fell back to a fresh
// recomputation, would see the real seeded stake (75_000_000) instead of the
// sentinel.
func TestCurrentBoundarySPOStakeRows_PeeksWithoutConsumingComputedStash(
	t *testing.T,
) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("gbpeek_pool_1234567890ABCDEF")
	stakingKey := bytes.Repeat([]byte{0xf2}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{75_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	txn := db.Transaction(context.Background(), true)

	// Step 3 of the real rollover sequence: stash the SNAP-point read.
	require.NoError(
		t,
		mgr.ComputeEpochBoundarySnapshot(context.Background(), txn, evt),
	)

	// White-box: overwrite the stashed distribution's stake with a sentinel
	// distinct from both the real seeded stake (75_000_000) and zero, so a
	// caller that recomputed instead of reading the stash is caught by
	// comparing against this exact value rather than merely "nonzero".
	const sentinelStake = uint64(999_000_111)
	mgr.mu.Lock()
	require.NotNil(t, mgr.pendingBoundary, "stash must exist after Compute")
	for k := range mgr.pendingBoundary.distribution.PoolStakes {
		mgr.pendingBoundary.distribution.PoolStakes[k] = sentinelStake
	}
	mgr.pendingBoundary.distribution.TotalStake = sentinelStake
	mgr.mu.Unlock()

	// governance's same-boundary read, earlier than the authoritative
	// persist -- must see the mutated stash via peek, not take it, and must
	// not recompute from the (unmutated) live/historical state.
	rows, err := mgr.CurrentBoundarySPOStakeRows(
		context.Background(), txn, evt,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, sentinelStake, uint64(rows[0].TotalStake),
		"must read the stashed distribution, not recompute it live")

	// The authoritative persist, at the end of the same rollover
	// transaction, must still find the SAME stash (not cleared by the peek
	// above) and persist the sentinel, not a fresh recomputation.
	require.NoError(
		t,
		mgr.CaptureEpochBoundarySnapshot(context.Background(), txn, evt),
	)
	require.NoError(t, txn.Commit())

	persisted, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(1, "mark", nil)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, sentinelStake, uint64(persisted[0].TotalStake),
		"the persisted row must come from the same stash CurrentBoundary"+
			"SPOStakeRows read, proving the peek did not consume it")
}

type markerDelegation = struct {
	stakingKey  []byte
	utxoAmounts []types.Uint64
}

// TestCaptureEpochBoundarySnapshotMarksAuthoritative verifies the authoritative
// (epoch-rollover) capture writes reward_snapshot.authoritative = true.
func TestCaptureEpochBoundarySnapshotMarksAuthoritative(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("poolA_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xa1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{50_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	txn := db.Transaction(context.Background(), true)
	require.NoError(
		t,
		mgr.CaptureEpochBoundarySnapshot(context.Background(), txn, evt),
	)
	require.NoError(t, txn.Commit())

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.True(t, snap.Authoritative,
		"epoch-rollover capture must mark the snapshot authoritative")
}

// TestFallbackCaptureMarksNonAuthoritative verifies the event-driven fallback
// capture writes reward_snapshot.authoritative = false.
func TestFallbackCaptureMarksNonAuthoritative(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("poolB_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xb1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{20_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      nil,
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), evt))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.False(t, snap.Authoritative,
		"fallback capture must leave the snapshot non-authoritative")
}

// TestFallbackCaptureReplacesProvisionalFallback verifies that a later fallback
// capture (carrying the real epoch nonce) refreshes an earlier provisional
// fallback row in place, exercising the ClaimFallbackSnapshot replace branch,
// and that it stays non-authoritative.
func TestFallbackCaptureReplacesProvisionalFallback(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := []byte("poolC_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xc1}, 28)
	seedPoolAndDelegations(t, db, poolHash, []markerDelegation{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{30_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)

	// First (slot-clock) fallback: no nonce yet.
	provisional := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      nil,
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}
	require.NoError(
		t,
		mgr.captureMarkSnapshot(context.Background(), provisional),
	)
	first, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.False(t, first.Authoritative)
	require.Empty(t, first.EpochNonce)

	// Second (block-based) fallback: carries the real nonce, replaces the row.
	withNonce := provisional
	withNonce.EpochNonce = []byte{0x0a, 0x0b, 0x0c}
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), withNonce))
	second, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.False(t, second.Authoritative,
		"replacing a provisional fallback row must stay non-authoritative")
	require.Equal(t, []byte{0x0a, 0x0b, 0x0c}, second.EpochNonce,
		"the refreshed fallback row must carry the real epoch nonce")
}

// TestClaimFallbackRewardSnapshotSkipsAuthoritative verifies the metadata-level
// claim refuses to overwrite an existing authoritative row and forces the
// authoritative flag off for a fresh fallback claim.
func TestClaimFallbackRewardSnapshotSkipsAuthoritative(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	meta := db.Metadata()

	authoritative := &models.RewardSnapshot{
		Epoch:            5,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(100),
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     10,
		BoundarySlot:     11,
		EpochNonce:       []byte{0xde, 0xad},
		ProtocolVersion:  8,
		Authoritative:    true,
	}
	require.NoError(t, meta.SaveRewardSnapshot(authoritative, nil))

	// A fallback claim (note Authoritative: true is set by the caller but must be
	// forced false by the claim) must be refused and must not mutate the row.
	fallback := &models.RewardSnapshot{
		Epoch:            5,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(999),
		TotalPoolCount:   9,
		TotalDelegators:  9,
		CapturedSlot:     99,
		BoundarySlot:     99,
		ProtocolVersion:  8,
		Authoritative:    true,
	}
	proceed, err := meta.ClaimFallbackRewardSnapshot(fallback, nil)
	require.NoError(t, err)
	require.False(t, proceed,
		"fallback claim must be refused when an authoritative row exists")

	got, err := meta.GetRewardSnapshot(5, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Authoritative)
	require.Equal(t, uint64(100), uint64(got.TotalActiveStake),
		"authoritative row must be untouched by the refused claim")
}

// TestClaimFallbackRewardSnapshotFreshAndReplace verifies a fresh claim writes a
// non-authoritative row and a second fallback claim replaces it in place.
func TestClaimFallbackRewardSnapshotFreshAndReplace(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	meta := db.Metadata()

	first := &models.RewardSnapshot{
		Epoch:            6,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(100),
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     10,
		BoundarySlot:     11,
		ProtocolVersion:  8,
		Authoritative:    true, // must be forced to false by the claim
	}
	proceed, err := meta.ClaimFallbackRewardSnapshot(first, nil)
	require.NoError(t, err)
	require.True(t, proceed)
	got, err := meta.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Authoritative,
		"fresh fallback claim must be non-authoritative")
	require.Equal(t, uint64(100), uint64(got.TotalActiveStake))

	second := &models.RewardSnapshot{
		Epoch:            6,
		SnapshotType:     "mark",
		TotalActiveStake: types.Uint64(200),
		TotalPoolCount:   2,
		TotalDelegators:  2,
		CapturedSlot:     20,
		BoundarySlot:     21,
		ProtocolVersion:  8,
	}
	proceed, err = meta.ClaimFallbackRewardSnapshot(second, nil)
	require.NoError(t, err)
	require.True(t, proceed)
	got, err = meta.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.Authoritative)
	require.Equal(t, uint64(200), uint64(got.TotalActiveStake),
		"second fallback claim must replace the provisional row in place")
}

func TestFallbackRewardSnapshotGuardTemporaryRow(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	meta := db.Metadata()
	txn := db.Transaction(context.Background(), true)

	proceed, guardID, err := meta.ClaimFallbackRewardSnapshotGuard(
		7,
		"mark",
		txn.Metadata(),
	)
	require.NoError(t, err)
	require.True(t, proceed)
	require.NotZero(t, guardID)

	guard, err := meta.GetRewardSnapshot(7, "mark", txn.Metadata())
	require.NoError(t, err)
	require.NotNil(t, guard)
	require.False(t, guard.Authoritative)

	require.NoError(t, meta.ReleaseFallbackRewardSnapshotGuard(
		guardID,
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())

	guard, err = meta.GetRewardSnapshot(7, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, guard,
		"the temporary serialization row must not survive commit")
}

func TestFallbackRewardSnapshotGuardRefusesAuthoritative(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:         8,
		SnapshotType:  "mark",
		CapturedSlot:  80,
		BoundarySlot:  81,
		Authoritative: true,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	proceed, guardID, err := meta.ClaimFallbackRewardSnapshotGuard(
		8,
		"mark",
		txn.Metadata(),
	)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Zero(t, guardID)
	require.NoError(t, txn.Rollback())

	got, err := meta.GetRewardSnapshot(8, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Authoritative)
	require.Equal(t, uint64(80), got.CapturedSlot)
}

func TestFallbackRewardSnapshotGuardRequiresTransaction(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	meta := db.Metadata()

	_, _, err := meta.ClaimFallbackRewardSnapshotGuard(9, "mark", nil)
	require.ErrorContains(t, err, "transaction is required")
	require.ErrorContains(
		t,
		meta.ReleaseFallbackRewardSnapshotGuard(1, nil),
		"transaction is required",
	)
}

// seedPointerStakeFixture builds a pool, a credential registered and
// delegated entirely through certificate history (so pointer resolution has
// real stake_registration/stake_delegation rows to join against, exactly as
// the reference ledger's saPtrs lookup does), a base-address UTxO for that
// credential, and a pointer-address UTxO naming the registration's own
// position. It returns the credential's staking key so a caller can inspect
// reward_live_stake directly.
//
// This is the / shape: a pool whose stake is understated
// because part of one delegator's stake sits at a pointer address.
func seedPointerStakeFixture(
	t *testing.T,
	db *database.Database,
	poolHash []byte,
) []byte {
	t.Helper()
	require.NoError(t, db.ImportPool(context.Background(), nil, &models.Pool{
		PoolKeyHash: poolHash,
		VrfKeyHash:  make([]byte, 32),
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
	}, &models.PoolRegistration{
		PoolKeyHash: poolHash,
		AddedSlot:   50,
		Pledge:      1_000_000,
		Cost:        340_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:  make([]byte, 32),
	}), "import pool")

	raw := snapshotSQLDB(t, db)
	stakeKey := bytes.Repeat([]byte{0x9c}, 28)
	regCertID := seedCertificate(
		t, raw, 100, 0, 0, lcommon.CertificateTypeStakeRegistration,
	)
	seedStakeRegistration(t, raw, models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: regCertID,
	})
	delCertID := seedCertificate(
		t, raw, 100, 0, 1, lcommon.CertificateTypeStakeDelegation,
	)
	seedStakeDelegation(t, raw, models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolHash,
		AddedSlot:     100,
		CertificateID: delCertID,
	})

	// Account first, then UTxOs: CreateAccount and CreateUtxo each refresh
	// reward_live_stake for the ref they touch by re-summing from the utxo
	// table as it stands at call time, so the account row (which carries
	// registered=true and the delegated pool) must exist before the UTxOs are
	// written for the final refresh to see the correct registration state.
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: stakeKey,
			Pool:       poolHash,
			AddedSlot:  100,
			Active:     true,
		}),
		"create account",
	)
	require.NoError(t, db.CreateUtxo(context.Background(), nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x01}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     700,
		AddedSlot:  150,
	}), "create base-address utxo")
	// The pointer-address utxo names the registration's own position
	// (100, 0, 0). Its StakingKey stays empty by design (see
	// database/models/utxo.go); only utxo_pointer records the position.
	require.NoError(t, db.CreateUtxo(context.Background(), nil, &models.Utxo{
		TxId:      bytes.Repeat([]byte{0x02}, 32),
		OutputIdx: 0,
		Amount:    600,
		AddedSlot: 200,
		Pointer:   &models.UtxoPointer{Slot: 100, TxIndex: 0, CertIndex: 0},
	}), "create pointer-address utxo")

	return stakeKey
}

// TestCaptureEpochBoundaryAgreesOnPointerStake is the review's
// blocking finding: ComputeEpochBoundarySnapshot (the SNAP-point hook a
// normally operating node installs) reads only the live aggregate, while the
// event-driven fallback (no stashed SNAP-point distribution) reconstructs
// historically -- and only the historical route resolved pointer stake. Two
// nodes on the same chain, or one node across a restart that lost the
// SNAP-point read, would persist different Mark stake for a pool holding
// pointer stake.
//
// Both routes must now report the same pool total for the same epoch.
func TestCaptureEpochBoundaryAgreesOnPointerStake(t *testing.T) {
	for _, tc := range []struct {
		name        string
		computeSnap bool
	}{
		{
			name:        "authoritative SNAP-point path (live aggregate + pointer overlay)",
			computeSnap: true,
		},
		{
			name:        "event-driven fallback (historical reconstruction)",
			computeSnap: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			seedEpochs(t, db, []models.Epoch{
				{EpochId: 0, StartSlot: 0, LengthInSlots: 432_000},
			})
			poolHash := bytes.Repeat([]byte{0xb2}, 28)
			seedPointerStakeFixture(t, db, poolHash)

			mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
			evt := event.EpochTransitionEvent{
				PreviousEpoch:   0,
				NewEpoch:        1,
				BoundarySlot:    432_000,
				EpochNonce:      []byte{0x0a, 0x0b},
				ProtocolVersion: 8,
				SnapshotSlot:    431_999,
			}

			txn := db.Transaction(context.Background(), true)
			if tc.computeSnap {
				// Authoritative path: SNAP-point read, then persist reuses
				// the stashed distribution.
				require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
					context.Background(), txn, evt,
				))
			}
			// Without the compute call, this is the "missing/failed SNAP
			// hook" shape: persist has nothing stashed and reconstructs
			// historically.
			require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			))
			require.NoError(t, txn.Commit())

			poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
				1, "mark", poolHash, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, poolSnapshot)
			require.Equal(
				t,
				uint64(1_300),
				uint64(poolSnapshot.TotalStake),
				"both capture routes must attribute the pointer-address "+
					"stake identically for the same epoch",
			)
		})
	}
}

// TestRewardLiveStakeRebuildAgreesWithIncrementalOnPointerAddresses covers the
// constraint the PR's own package doc states: reward_live_stake never carries
// pointer-derived UTxO stake, because attribution depends on certificate
// history at the slot being evaluated rather than on anything the live,
// tip-keyed aggregate can express. That has to hold identically whichever way
// reward_live_stake was populated -- a full RebuildRewardLiveStake pass or the
// normal incremental per-write refresh -- or a node that rebuilds would
// silently start disagreeing with one that never has.
func TestRewardLiveStakeRebuildAgreesWithIncrementalOnPointerAddresses(
	t *testing.T,
) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432_000},
	})
	poolHash := bytes.Repeat([]byte{0xb4}, 28)
	stakeKey := seedPointerStakeFixture(t, db, poolHash)

	raw := snapshotSQLDB(t, db)
	incremental := rewardLiveStakeTotalStake(t, raw, stakeKey)

	require.NoError(
		t,
		db.RebuildRewardLiveStake(context.Background(), 1_000, nil),
	)

	rebuilt := rewardLiveStakeTotalStake(t, raw, stakeKey)

	require.Equal(
		t,
		incremental,
		rebuilt,
		"RebuildRewardLiveStake must not diverge from incremental "+
			"maintenance for a credential holding pointer-address stake",
	)
	require.Equal(
		t,
		uint64(700),
		incremental,
		"reward_live_stake must exclude pointer-address stake on both "+
			"maintenance paths -- the live snapshot path adds it back "+
			"separately, from utxo_pointer, not from this table",
	)
}

// rewardLiveStakeTotalStake reads reward_live_stake.total_stake for a
// credential directly, bypassing GetLiveStakeInputsForPools's pool filter so
// the read is unaffected by which pool the credential is delegated to.
func rewardLiveStakeTotalStake(
	t *testing.T,
	raw *sql.DB,
	stakingKey []byte,
) uint64 {
	t.Helper()
	var total string
	require.NoError(t, raw.QueryRow(
		"SELECT total_stake FROM reward_live_stake WHERE staking_key = ?",
		stakingKey,
	).Scan(&total))
	value, err := strconv.ParseUint(total, 10, 64)
	require.NoError(t, err)
	return value
}

// seedEpochRow writes one epoch row with an explicit era, inside the caller's
// transaction when one is supplied. seedEpochs hardcodes Shelley and always
// commits on its own, neither of which can express an era cutover reached part
// way through a rollover transaction.
func seedEpochRow(
	t *testing.T,
	db *database.Database,
	txn *database.Txn,
	startSlot uint64,
	epochID uint64,
	lengthInSlots uint,
	eraID uint,
) {
	t.Helper()
	require.NoError(t, db.SetEpoch(
		startSlot,
		epochID,
		nil, nil, nil, nil,
		eraID,
		1,
		lengthInSlots,
		txn,
	), "seed epoch %d", epochID)
}

// TestCaptureEpochBoundaryAgreesOnPointerStakeAcrossTheEraCutover pins the
// capture routes against each other at the one boundary the era gate exists
// for.
//
// cardano-ledger's hard-fork combinator translates the ledger state into the
// incoming era in extendToSlot before ticking into that era's first slot
// (ouroboros-consensus HardFork/Combinator/Ledger.hs,
// applyChainTickLedgerResult), and the Babbage->Conway translation rebuilds the
// instant stake as `ConwayInstantStake . sisCredentialStake`
// (Conway/Translation.hs), dropping sisPtrStake. SNAP runs inside that Conway
// TICK, so the mark snapshot taken at a Babbage->Conway boundary carries no
// pointer-address stake.
//
// The two routes reach that boundary at different points of the rollover:
// processEpochRollover's SNAP read (ComputeEpochBoundarySnapshot) runs at step 3
// of its documented ordering, while the incoming epoch's row is written near the
// end of the same transaction -- so a gate that resolves the incoming era from
// the epoch table sees only the outgoing epoch's row on the authoritative route
// and both rows on the persist-time route. Both must still report the incoming
// era's answer.
func TestCaptureEpochBoundaryAgreesOnPointerStakeAcrossTheEraCutover(
	t *testing.T,
) {
	for _, tc := range []struct {
		name        string
		computeSnap bool
	}{
		{
			name:        "authoritative SNAP-point path",
			computeSnap: true,
		},
		{
			name:        "event-driven fallback",
			computeSnap: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			// Only the outgoing Babbage epoch exists when the SNAP read runs.
			seedEpochRow(t, db, nil, 0, 0, 300, eras.BabbageEraDesc.Id)
			poolHash := bytes.Repeat([]byte{0xb6}, 28)
			seedPointerStakeFixture(t, db, poolHash)

			mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
			evt := event.EpochTransitionEvent{
				PreviousEpoch:   0,
				NewEpoch:        1,
				BoundarySlot:    300,
				EpochNonce:      []byte{0x0a, 0x0b},
				ProtocolVersion: 9,
				SnapshotSlot:    299,
			}

			txn := db.Transaction(context.Background(), true)
			if tc.computeSnap {
				require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
					context.Background(), txn, evt,
				))
			}
			// The rollover writes the incoming epoch's row after the SNAP read
			// and before the persist half.
			seedEpochRow(t, db, txn, 300, 1, 300, eras.ConwayEraDesc.Id)
			require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
				context.Background(), txn, evt,
			))
			require.NoError(t, txn.Commit())

			poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
				1, "mark", poolHash, nil,
			)
			require.NoError(t, err)
			require.NotNil(t, poolSnapshot)
			require.Equal(
				t,
				uint64(700),
				uint64(poolSnapshot.TotalStake),
				"the mark snapshot at a Babbage->Conway boundary is produced "+
					"under ConwayInstantStake, which carries no pointer stake",
			)
		})
	}
}

// TestMergePointerStakeInputsAttachesToTheSurvivingLiveRow covers a legacy
// database carrying duplicate reward_live_stake rows for one credential --
// the shape dedupeStakeInputs exists for, and which did occur before
// idx_reward_live_stake_cred was unique.
//
// mergePointerStakeInputs must add the overlay to the row dedupeStakeInputs
// will keep. Attaching it to any other duplicate silently drops the pointer
// stake at aggregation, reinstating on exactly those nodes.
func TestMergePointerStakeInputsAttachesToTheSurvivingLiveRow(t *testing.T) {
	credential := bytes.Repeat([]byte{0x9c}, 28)
	lowPool := bytes.Repeat([]byte{0x01}, 28)
	highPool := bytes.Repeat([]byte{0x02}, 28)

	// dedupeStakeInputs orders duplicates by pool before stake, so the
	// highPool row survives whatever either row's stake is. The lowPool row is
	// last here, which is the row a last-wins index would select.
	rawInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash: highPool, CredentialTag: 0, StakingKey: credential,
			Stake: 200, Registered: true,
		},
		{
			PoolKeyHash: lowPool, CredentialTag: 0, StakingKey: credential,
			Stake: 100, Registered: true,
		},
	}
	pointerInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash: highPool, CredentialTag: 0, StakingKey: credential,
			Stake: 600, Registered: true,
		},
	}

	merged, err := mergePointerStakeInputs(rawInputs, pointerInputs)
	require.NoError(t, err)
	inputs, err := rewardStakeInputsFromRows(merged)
	require.NoError(t, err)
	require.Len(t, inputs, 1, "one credential survives deduplication")
	require.Equal(t, uint64(800), inputs[0].Stake,
		"the pointer overlay must survive deduplication of duplicate "+
			"reward_live_stake rows")
	require.Equal(t, highPool, inputs[0].PoolKeyHash)
}

// retirePool marks a pool retired as of the given epoch, the state POOLREAP
// leaves behind and the state GetActivePoolKeyHashesAtSlot excludes.
func retirePool(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	epoch uint64,
	addedSlot uint64,
) {
	t.Helper()
	_, err := snapshotSQLDB(t, db).Exec(`
INSERT INTO pool_retirement (pool_key_hash, certificate_id, pool_id, epoch, added_slot)
SELECT ?, 0, id, ?, ? FROM pool WHERE pool_key_hash = ?`,
		poolKeyHash, epoch, addedSlot, poolKeyHash,
	)
	require.NoError(t, err)
}

// TestStakeDistributionKeepsRetiredPoolStakeInDenominator pins the sigma_a
// denominator against the defect behind.
//
// cardano-ledger's ssTotalActiveStake sums every registered credential holding
// a delegation and never consults the stake-pool set, so a credential still
// delegated to a pool that has left the active set counts towards the
// denominator even though the pool earns nothing. Enumerating the snapshot
// from the active pool set alone drops that stake, which raises sigma_a for
// every surviving pool and under-credits every reward on the node by the
// dropped share -- uniformly, regardless of pool size or saturation, and
// invisibly to a check that compares the pool rows against the total, because
// the omission shortens both sides equally.
func TestStakeDistributionKeepsRetiredPoolStakeInDenominator(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedSnapshotEpoch(t, db)

	activePool := bytes.Repeat([]byte{0xa1}, 28)
	retiredPool := bytes.Repeat([]byte{0xb2}, 28)
	seedPoolAndDelegations(t, db, activePool, nil, 100)
	seedPoolAndDelegations(t, db, retiredPool, nil, 100)
	// seedSnapshotEpoch puts slot 1000 in epoch 10, so a retirement recorded
	// for epoch 10 is already in effect at the snapshot slot.
	retirePool(t, db, retiredPool, 10, 100)

	seedRewardLiveStake(t, snapshotSQLDB(t, db), []models.RewardLiveStake{
		{
			CredentialTag:      0,
			StakingKey:         bytes.Repeat([]byte{0x01}, 28),
			PoolKeyHash:        activePool,
			TotalStake:         types.Uint64(700),
			Registered:         true,
			PoolDelegationSlot: 100,
		},
		{
			CredentialTag:      0,
			StakingKey:         bytes.Repeat([]byte{0x02}, 28),
			PoolKeyHash:        retiredPool,
			TotalStake:         types.Uint64(300),
			Registered:         true,
			PoolDelegationSlot: 100,
		},
	})

	calc := NewCalculator(db)
	txn := db.Transaction(context.Background(), false)
	defer func() { _ = txn.Commit() }()
	dist, err := calc.calculateStakeDistributionInTxn(
		context.Background(), txn, 1000, 0, 0,
	)
	require.NoError(t, err)

	var active, retired lcommon.PoolKeyHash
	copy(active[:], activePool)
	copy(retired[:], retiredPool)

	// The retired pool earns nothing, so it gets no bucket and no reward input.
	require.NotContains(t, dist.PoolStakes, retired)
	require.Equal(t, uint64(700), dist.PoolStakes[active])
	require.Equal(t, uint64(700), dist.TotalStake)
	require.Len(t, dist.StakeInputs, 1)

	// Its delegator's stake still counts towards the denominator.
	require.Equal(t, uint64(1000), dist.TotalActiveStake)

	// And the reward-stake view carries that denominator rather than
	// re-deriving it from the pool buckets it does not contain.
	reward, err := rewardStakeDistribution(dist)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), rewardTotalActiveStake(reward))
	require.Equal(
		t,
		uint64(300),
		rewardTotalActiveStake(reward)-sumPoolStakes(reward.PoolStakes),
	)
}

// TestRewardTotalActiveStakeFallsBackToBuckets covers a distribution built
// outside the calculator, or restored from a capture predating the
// credential-first total: the pool-bucket sum is the floor, never zero.
func TestRewardTotalActiveStakeFallsBackToBuckets(t *testing.T) {
	t.Parallel()

	var pool lcommon.PoolKeyHash
	pool[0] = 0x01
	dist := &StakeDistribution{
		PoolStakes: map[lcommon.PoolKeyHash]uint64{pool: 500},
	}
	require.Equal(t, uint64(500), rewardTotalActiveStake(dist))

	dist.TotalActiveStake = 900
	require.Equal(t, uint64(900), rewardTotalActiveStake(dist))
}

// TestRewardSnapshotActiveStakeKeepsDegradedPoolStake pins the sigma_a
// denominator written by buildRewardStateInputs.
//
// cardano-ledger derives ssTotalActiveStake from every registered credential
// that carries a delegation, independently of which pools are present in
// ssStakePoolsSnapShot (Cardano.Ledger.State.SnapShots.mkSnapShot over
// Cardano.Ledger.State.Stake.resolveInstantStake, whose own comment reads
// "active stake means any stake credential that is registered and delegated to
// a stake pool"). A pool whose registration cannot be resolved therefore earns
// nothing while its delegators keep contributing to that denominator.
//
// Deriving RewardSnapshot.TotalActiveStake from the post-exclusion pool set
// instead shrinks the denominator, which raises sigma_a for every surviving
// pool, lowers its apparent performance, and under-credits every member and
// leader reward the node reconstructs. TotalPoolCount and TotalDelegators
// describe the reward_pool_input rows actually written and stay reduced.
func TestRewardSnapshotActiveStakeKeepsDegradedPoolStake(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	goodPoolHash := bytes.Repeat([]byte{0x11}, 28)
	goodStakeKey := bytes.Repeat([]byte{0x21}, 28)
	rewardAccount := bytes.Repeat([]byte{0x41}, 28)
	require.NoError(t, db.ImportPool(
		context.Background(),
		nil,
		&models.Pool{PoolKeyHash: goodPoolHash},
		&models.PoolRegistration{
			PoolKeyHash:   goodPoolHash,
			AddedSlot:     50,
			RewardAccount: rewardAccount,
			Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
		},
	))

	// Degraded pool: delegated to, but with no resolvable registration.
	degradedPoolHash := bytes.Repeat([]byte{0x22}, 28)
	degradedStakeKey := bytes.Repeat([]byte{0x32}, 28)

	var goodPoolKey, degradedPoolKey lcommon.PoolKeyHash
	copy(goodPoolKey[:], goodPoolHash)
	copy(degradedPoolKey[:], degradedPoolHash)

	const goodStake = uint64(7_000_000_000)
	const degradedStake = uint64(3_000_000_000)

	distribution := &StakeDistribution{
		Slot: 100,
		PoolStakes: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     goodStake,
			degradedPoolKey: degradedStake,
		},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     1,
			degradedPoolKey: 1,
		},
		TotalStake: goodStake + degradedStake,
		TotalPools: 2,
		StakeInputs: []StakeInput{
			{
				PoolKeyHash: goodPoolHash,
				StakingKey:  goodStakeKey,
				Stake:       goodStake,
				Registered:  true,
			},
			{
				PoolKeyHash: degradedPoolHash,
				StakingKey:  degradedStakeKey,
				Stake:       degradedStake,
				Registered:  true,
			},
		},
	}

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	bundle, err := mgr.buildRewardStateInputs(
		1,
		models.PoolStakeSnapshotTypeMark,
		distribution,
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, bundle)

	// The degraded pool earns nothing: no reward_pool_input row, and no
	// reward_stake_input row for its delegator.
	require.Len(t, bundle.poolInputs, 1)
	require.Equal(t, goodPoolHash, bundle.poolInputs[0].PoolKeyHash)
	require.Len(t, bundle.stakeInputs, 1)
	require.Equal(t, goodStakeKey, bundle.stakeInputs[0].StakingKey)
	require.Equal(t, uint64(1), bundle.snapshot.TotalPoolCount)
	require.Equal(t, uint64(1), bundle.snapshot.TotalDelegators)

	// Its delegator's stake nonetheless stays in the sigma_a denominator.
	require.Equal(
		t,
		goodStake+degradedStake,
		uint64(bundle.snapshot.TotalActiveStake),
	)

	// The excluded pool's stake is tracked explicitly, not just
	// implied by the gap between TotalActiveStake and the surviving pool set,
	// so reward calculation can check reward_pool_input sums to that gap
	// exactly instead of merely no more than TotalActiveStake.
	require.NotNil(t, bundle.snapshot.ExcludedActiveStake)
	require.Equal(t, degradedStake, uint64(*bundle.snapshot.ExcludedActiveStake))
}

// TestRewardSnapshotActiveStakeTracksNoExclusion is the no-degraded-pool
// companion to TestRewardSnapshotActiveStakeKeepsDegradedPoolStake: when
// nothing was excluded, ExcludedActiveStake must still be a tracked zero, not
// left nil. Nil means "unknown, predates tracking"; a fresh
// capture always knows the answer, even when that answer is zero.
func TestRewardSnapshotActiveStakeTracksNoExclusion(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	goodPoolHash := bytes.Repeat([]byte{0x15}, 28)
	goodStakeKey := bytes.Repeat([]byte{0x25}, 28)
	rewardAccount := bytes.Repeat([]byte{0x45}, 28)
	require.NoError(t, db.ImportPool(
		context.Background(),
		nil,
		&models.Pool{PoolKeyHash: goodPoolHash},
		&models.PoolRegistration{
			PoolKeyHash:   goodPoolHash,
			AddedSlot:     50,
			RewardAccount: rewardAccount,
			Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
		},
	))

	var goodPoolKey lcommon.PoolKeyHash
	copy(goodPoolKey[:], goodPoolHash)

	const goodStake = uint64(7_000_000_000)

	distribution := &StakeDistribution{
		Slot: 100,
		PoolStakes: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey: goodStake,
		},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey: 1,
		},
		TotalStake: goodStake,
		TotalPools: 1,
		StakeInputs: []StakeInput{
			{
				PoolKeyHash: goodPoolHash,
				StakingKey:  goodStakeKey,
				Stake:       goodStake,
				Registered:  true,
			},
		},
	}

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	bundle, err := mgr.buildRewardStateInputs(
		1,
		models.PoolStakeSnapshotTypeMark,
		distribution,
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, bundle)

	require.NotNil(t, bundle.snapshot.ExcludedActiveStake)
	require.Zero(t, uint64(*bundle.snapshot.ExcludedActiveStake))
}

// TestSaveSnapshotKeepsDegradedPoolStakeInPoolAndEpochRows is the
// pool_stake_snapshot/epoch_summary companion to
// TestRewardSnapshotActiveStakeKeepsDegradedPoolStake, and pins the
// DATABASE.md claim for those tables: saveSnapshotInTxn builds
// pool_stake_snapshot and epoch_summary from the caller's unmodified
// distribution (rotation.go's loop over distribution.PoolStakes and the
// EpochSummary literal that follows it), not from the post-exclusion copy
// buildRewardStateInputs consumes internally to populate reward_pool_input.
// A pool excluded from reward_pool_input for degraded registration data
// therefore still gets its own pool_stake_snapshot row carrying its full
// stake, and epoch_summary's totals still count it.
func TestSaveSnapshotKeepsDegradedPoolStakeInPoolAndEpochRows(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	goodPoolHash := bytes.Repeat([]byte{0x13}, 28)
	goodStakeKey := bytes.Repeat([]byte{0x23}, 28)
	rewardAccount := bytes.Repeat([]byte{0x43}, 28)
	require.NoError(t, db.ImportPool(
		context.Background(),
		nil,
		&models.Pool{PoolKeyHash: goodPoolHash},
		&models.PoolRegistration{
			PoolKeyHash:   goodPoolHash,
			AddedSlot:     50,
			RewardAccount: rewardAccount,
			Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
		},
	))

	// Degraded pool: delegated to, but with no resolvable registration.
	degradedPoolHash := bytes.Repeat([]byte{0x24}, 28)
	degradedStakeKey := bytes.Repeat([]byte{0x34}, 28)

	var goodPoolKey, degradedPoolKey lcommon.PoolKeyHash
	copy(goodPoolKey[:], goodPoolHash)
	copy(degradedPoolKey[:], degradedPoolHash)

	const goodStake = uint64(7_000_000_000)
	const degradedStake = uint64(3_000_000_000)

	distribution := &StakeDistribution{
		Slot: 100,
		PoolStakes: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     goodStake,
			degradedPoolKey: degradedStake,
		},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     1,
			degradedPoolKey: 1,
		},
		TotalStake: goodStake + degradedStake,
		TotalPools: 2,
		StakeInputs: []StakeInput{
			{
				PoolKeyHash: goodPoolHash,
				StakingKey:  goodStakeKey,
				Stake:       goodStake,
				Registered:  true,
			},
			{
				PoolKeyHash: degradedPoolHash,
				StakingKey:  degradedStakeKey,
				Stake:       degradedStake,
				Registered:  true,
			},
		},
	}

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	saved, err := mgr.saveSnapshot(
		context.Background(),
		1,
		models.PoolStakeSnapshotTypeMark,
		distribution,
		event.EpochTransitionEvent{BoundarySlot: 200},
		false,
		true,
		false,
	)
	require.NoError(t, err)
	require.True(t, saved)

	// reward_pool_input excludes the degraded pool.
	poolInputs, err := db.Metadata().GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.Equal(t, goodPoolHash, poolInputs[0].PoolKeyHash)

	// pool_stake_snapshot still carries the degraded pool's own row with its
	// full stake.
	degraded, err := db.Metadata().GetPoolStakeSnapshot(
		1, models.PoolStakeSnapshotTypeMark, degradedPoolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, degraded)
	require.Equal(t, degradedStake, uint64(degraded.TotalStake))

	// epoch_summary's totals describe the full boundary, not just the
	// surviving pool set reward_pool_input holds.
	summary, err := db.Metadata().GetEpochSummary(1, nil)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, goodStake+degradedStake, uint64(summary.TotalActiveStake))
	require.Equal(t, uint64(2), summary.TotalPoolCount)
	require.Equal(t, uint64(2), summary.TotalDelegators)

	// ExcludedActiveStake round-trips through the database exactly (dingo
	// ): a reward calculation reading this row back after a restart or
	// replay -- not just the in-memory bundle buildRewardStateInputs
	// returned -- must still be able to reconstruct the excluded amount.
	reloaded, err := db.Metadata().
		GetRewardSnapshot(1, models.PoolStakeSnapshotTypeMark, nil)
	require.NoError(t, err)
	require.NotNil(t, reloaded)
	require.NotNil(t, reloaded.ExcludedActiveStake)
	require.Equal(t, degradedStake, uint64(*reloaded.ExcludedActiveStake))
}

func TestDijkstraBoundarySnapshotIncludesEnactmentCredits(t *testing.T) {
	for _, compute := range []bool{false, true} {
		t.Run(strconv.FormatBool(compute), func(t *testing.T) {
			db := setupTestDB(t)
			seedEpochs(t, db, []models.Epoch{
				{
					EpochId:       0,
					StartSlot:     0,
					LengthInSlots: 432000,
					EraId:         eras.ConwayEraDesc.Id,
				},
				{
					EpochId:       1,
					StartSlot:     432000,
					LengthInSlots: 432000,
					EraId:         eras.DijkstraEraDesc.Id,
				},
			})
			require.NoError(
				t,
				db.SetEpoch(
					0,
					0,
					nil,
					nil,
					nil,
					nil,
					eras.ConwayEraDesc.Id,
					1,
					432000,
					nil,
				),
			)
			require.NoError(
				t,
				db.SetEpoch(
					432000,
					1,
					nil,
					nil,
					nil,
					nil,
					eras.DijkstraEraDesc.Id,
					1,
					432000,
					nil,
				),
			)

			poolHash := bytes.Repeat([]byte{0x91}, 28)
			stakingKey := bytes.Repeat([]byte{0x92}, 28)
			seedPoolAndDelegations(t, db, poolHash, []struct {
				stakingKey  []byte
				utxoAmounts []types.Uint64
			}{
				{
					stakingKey:  stakingKey,
					utxoAmounts: []types.Uint64{40_000_000},
				},
			}, 500)
			mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
			evt := event.EpochTransitionEvent{
				PreviousEpoch:   0,
				NewEpoch:        1,
				BoundarySlot:    432000,
				SnapshotSlot:    431999,
				ProtocolVersion: lcommon.ProtocolVersionDijkstra,
			}
			txn := db.Transaction(t.Context(), true)
			// An obsolete Conway-point capture must not survive the era boundary.
			if compute {
				preEnactment := evt
				preEnactment.ProtocolVersion = lcommon.ProtocolVersionConway
				require.NoError(
					t,
					mgr.ComputeEpochBoundarySnapshot(
						context.Background(),
						txn,
						preEnactment,
					),
				)
			}
			require.NoError(
				t,
				db.AddPostSnapshotAccountRewardByCredential(
					t.Context(),
					0,
					stakingKey,
					1_000_000,
					evt.BoundarySlot,
					bytes.Repeat([]byte{0x93}, 32),
					txn,
				),
			)
			require.NoError(
				t,
				mgr.CaptureEpochBoundarySnapshot(
					context.Background(),
					txn,
					evt,
				),
			)
			require.NoError(t, txn.Commit())
			snapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
			require.NoError(t, err)
			require.Equal(
				t,
				uint64(41_000_000),
				uint64(snapshot.TotalActiveStake),
			)
			inputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
			require.NoError(t, err)
			require.Len(t, inputs, 1)
			require.Equal(t, uint64(41_000_000), uint64(inputs[0].Stake))
		})
	}
}
