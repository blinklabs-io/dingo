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

package ledger

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPendingRewardInputsRetainReconstructionHistory(t *testing.T) {
	t.Parallel()
	fixture := seedRetentionReconstructionFixture(t)
	meta := fixture.db.Metadata()
	ctx := context.Background()
	require.NoError(
		t,
		markRewardStakeInputsPending(
			meta,
			nil,
			retentionRewardSnapshotEpoch,
			100,
		),
	)
	fixture.ls.rewardInputRollbackActive.Store(1)
	require.NoError(t, fixture.ls.writeDeferredRewardStakeInputs(ctx, retentionRewardSnapshotEpoch, 100, fixture.capturedStakeInputs, fixture.ls.rewardInputGeneration.Load()))
	fixture.ls.rewardInputRollbackActive.Store(0)
	partial, err := meta.GetRewardStakeInputs(retentionRewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Empty(t, partial)
	require.NoError(
		t,
		fixture.db.MarkUtxosDeletedAtSlot(
			ctx,
			nil,
			[]types.UtxoKey{
				{TxId: bytes.Repeat([]byte{0x11}, 32), OutputIdx: 0},
			},
			150,
		),
	)
	pruned, floor, err := fixture.ls.pruneConsumedUtxosUpTo(ctx, 200, 50, 1000)
	require.NoError(t, err)
	require.Zero(t, pruned)
	require.Equal(t, uint64(99), floor)
	require.Equal(t, uint64(99), fixture.ls.announcedUtxoPruneFloor())
	tx := fixture.db.Transaction(ctx, true)
	require.NoError(
		t,
		fixture.ls.ensureRewardStakeInputsReady(
			tx,
			retentionRewardSnapshotEpoch,
		),
	)
	require.NoError(t, tx.Commit())
	restored, err := meta.GetRewardStakeInputs(
		retentionRewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, restored, len(fixture.capturedStakeInputs))
	for _, want := range fixture.capturedStakeInputs {
		found := false
		for _, got := range restored {
			if bytes.Equal(want.StakingKey, got.StakingKey) {
				require.Equal(t, want.Stake, got.Stake)
				require.Equal(t, want.Owner, got.Owner)
				found = true
			}
		}
		require.True(t, found)
	}
	pruned, floor, err = fixture.ls.pruneConsumedUtxosUpTo(ctx, 200, 50, 1000)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	require.Equal(t, uint64(200), floor)
}

func TestPendingRewardInputsCorruptStatePreventsPruneAnnouncement(
	t *testing.T,
) {
	t.Parallel()
	for _, raw := range []string{"{", "null", "{}", `{"entries":{}}`, `{"entries":[],"unknown":1}`, `{"entries":[{"epoch":1,"boundary_slot":100,"unknown":1}]}`} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			fixture := seedRetentionReconstructionFixture(t)
			require.NoError(
				t,
				fixture.db.Metadata().
					SetSyncState(rewardStakeInputsPendingKey, raw, nil),
			)
			pruned, _, err := fixture.ls.pruneConsumedUtxosUpTo(
				context.Background(),
				200,
				50,
				1000,
			)
			require.Error(t, err)
			require.Zero(t, pruned)
			require.Zero(t, fixture.ls.announcedUtxoPruneFloor())
		})
	}
}

func TestPendingRewardInputsIgnoreRolledBackSnapshot(t *testing.T) {
	t.Parallel()
	fixture := seedRetentionReconstructionFixture(t)
	meta := fixture.db.Metadata()
	require.NoError(t, markRewardStakeInputsPending(meta, nil, 99, 999))
	floor, protected, err := pendingRewardStakeInputHistoryFloor(meta, nil)
	require.NoError(t, err)
	require.False(t, protected)
	require.Zero(t, floor)
}

func TestPendingRewardInputsDisableTrustedBatchPruning(t *testing.T) {
	t.Parallel()
	h := newExternalIngestHarness(t, false)
	meta := h.db.Metadata()
	require.NoError(
		t,
		meta.SaveRewardSnapshot(
			&models.RewardSnapshot{
				Epoch:         1,
				SnapshotType:  "mark",
				CapturedSlot:  0,
				BoundarySlot:  1,
				Authoritative: true,
			},
			nil,
		),
	)
	require.NoError(t, markRewardStakeInputsPending(meta, nil, 1, 1))
	overlays := h.countOverlays()
	blocks, names := externalIngestBlocks(t, 0)
	tip, err := h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, blockPoint(blocks[len(blocks)-1]), tip)
	require.Zero(t, overlays.Load())
	require.True(t, utxoRowExists(t, h.db, names["register"].hash, 0))
	require.True(t, utxoRowExists(t, h.db, names["invalid"].hash, 1))
	floor, err := h.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Zero(t, floor)
	require.NoError(t, clearRewardStakeInputsPending(meta, nil, 1))
	pruned, _, err := h.ls.pruneConsumedUtxosUpTo(
		t.Context(),
		tip.Slot,
		0,
		1000,
	)
	require.NoError(t, err)
	require.Positive(t, pruned)
	require.False(t, utxoRowExists(t, h.db, names["register"].hash, 0))
}
