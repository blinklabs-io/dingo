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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

// TestCurrentBoundarySPOStakeRows_FallsBackToHistoricalReconstruction covers
// governance's dingo#4441 same-boundary SPO read when no
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

	txn := db.Transaction(true)
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

	txn := db.Transaction(true)

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
