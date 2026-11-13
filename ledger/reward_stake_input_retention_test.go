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
	"encoding/json"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"

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
			dbadapter.New(fixture.db),
			nil,
			retentionRewardSnapshotEpoch,
			100,
		),
	)
	fixture.ls.rewardInputRollbackActive.Store(1)
	require.NoError(
		t,
		fixture.ls.writeDeferredRewardStakeInputs(
			ctx,
			retentionRewardSnapshotEpoch,
			100,
			fixture.capturedStakeInputs,
			fixture.ls.rewardInputGeneration.Load(),
		),
	)
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
	durableFloor, err := fixture.db.ConsumedUtxoPruneFloor(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(99), durableFloor)
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
	require.NoError(
		t,
		markRewardStakeInputsPending(dbadapter.New(fixture.db), nil, 99, 999),
	)
	txn := dbadapter.New(fixture.db).BeginLedgerOnly(t.Context(), false)
	defer func() { _ = txn.Rollback() }()
	floor, protected, err := pendingRewardStakeInputHistoryFloor(
		dbadapter.New(fixture.db),
		txn,
	)
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
	require.NoError(
		t,
		markRewardStakeInputsPending(dbadapter.New(h.db), nil, 1, 1),
	)
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
	require.NoError(
		t,
		clearRewardStakeInputsPending(dbadapter.New(h.db), nil, 1),
	)
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

type rewardRetentionHandoffStore struct {
	ledgerdb.LedgerDB
	handoff func()
}

func (s *rewardRetentionHandoffStore) GetSyncState(
	key string,
	txn ledgerdb.Txn,
) (string, error) {
	raw, err := s.LedgerDB.GetSyncState(key, txn)
	if key == rewardStakeInputsPendingKey && s.handoff != nil {
		handoff := s.handoff
		s.handoff = nil
		handoff()
	}
	return raw, err
}

func TestPendingRewardRetentionSnapshotHandoff(t *testing.T) {
	t.Parallel()
	f := seedRetentionReconstructionFixture(t)
	meta := f.db.Metadata()
	epoch, err := meta.GetEpoch(1, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		meta.SetEpoch(100, 1, nil, nil, nil, nil, epoch.EraId, 1, 100, nil),
	)
	ctx := t.Context()
	snapshot, err := meta.GetRewardSnapshot(
		retentionRewardSnapshotEpoch,
		"mark",
		nil,
	)
	require.NoError(t, err)
	pools, err := meta.GetRewardPoolInputs(retentionRewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.NoError(t, meta.DeleteRewardStateAfterSlot(98, nil))
	record, _ := json.Marshal(
		pendingRatificationRecord{
			Epoch:        1,
			BoundarySlot: 100,
			ID:           1,
			Snapshot:     true,
		},
	)
	require.NoError(
		t,
		meta.SetSyncState(pendingRatificationSyncKey, string(record), nil),
	)
	require.NoError(
		t,
		f.db.MarkUtxosDeletedAtSlot(
			ctx,
			nil,
			[]types.UtxoKey{
				{TxId: bytes.Repeat([]byte{0x11}, 32), OutputIdx: 0},
			},
			150,
		),
	)
	pruned, floor, err := f.ls.pruneConsumedUtxosUpTo(ctx, 200, 50, 1000)
	require.NoError(t, err)
	require.Zero(t, pruned)
	require.Equal(t, uint64(99), floor)
	handoff := func() {
		tx := f.db.Transaction(ctx, true)
		require.NoError(t, meta.SaveRewardSnapshot(snapshot, tx.Metadata()))
		require.NoError(t, meta.SaveRewardPoolInputs(pools, tx.Metadata()))
		require.NoError(
			t,
			markRewardStakeInputsPending(dbadapter.New(f.db), tx, 1, 100),
		)
		require.NoError(
			t,
			meta.DeleteSyncState(pendingRatificationSyncKey, tx.Metadata()),
		)
		require.NoError(t, tx.Commit())
	}
	readTxn := dbadapter.New(f.db).BeginLedgerOnly(ctx, false)
	spy := &rewardRetentionHandoffStore{
		LedgerDB: dbadapter.New(f.db),
		handoff:  handoff,
	}
	floor, protected, err := pendingRewardStakeInputHistoryFloor(spy, readTxn)
	require.NoError(t, err)
	require.True(t, protected)
	require.Equal(t, uint64(99), floor)
	require.NoError(t, readTxn.Rollback())
	pruned, floor, err = f.ls.pruneConsumedUtxosUpTo(ctx, 200, 50, 1000)
	require.NoError(t, err)
	require.Zero(t, pruned)
	require.Equal(t, uint64(99), floor)
	tx := f.db.Transaction(ctx, true)
	require.NoError(t, f.ls.ensureRewardStakeInputsReady(tx, 1))
	require.NoError(t, tx.Commit())
	rows, err := meta.GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, rows, len(f.capturedStakeInputs))
	pruned, _, err = f.ls.pruneConsumedUtxosUpTo(ctx, 200, 50, 1000)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
}

func TestPendingRewardRetentionRatificationRecordValidation(t *testing.T) {
	t.Parallel()
	f := seedRetentionReconstructionFixture(t)
	meta := f.db.Metadata()
	for _, raw := range []string{"null", "{}", `{"epoch":1,"boundary_slot":100,"id":1,"snapshot":null}`, `{"epoch":1,"boundary_slot":100,"id":1,"snapshot":true,"unknown":1}`, `{"epoch":1,"boundary_slot":101,"id":1,"snapshot":true}`, `{"epoch":99,"boundary_slot":100,"id":1,"snapshot":true}`} {
		require.NoError(
			t,
			meta.SetSyncState(pendingRatificationSyncKey, raw, nil),
		)
		_, _, err := f.ls.pruneConsumedUtxosUpTo(t.Context(), 200, 50, 1000)
		require.Error(t, err, raw)
		require.Zero(t, f.ls.announcedUtxoPruneFloor())
	}
	require.NoError(
		t,
		meta.SetSyncState(
			pendingRatificationSyncKey,
			`{"epoch":99,"boundary_slot":100,"id":1}`,
			nil,
		),
	)
	_, floor, err := f.ls.pruneConsumedUtxosUpTo(t.Context(), 200, 50, 1000)
	require.NoError(t, err)
	require.Equal(t, uint64(200), floor)
}

func TestPendingRewardRetentionAcceptsNativeEmptyState(t *testing.T) {
	t.Parallel()
	f := seedRetentionReconstructionFixture(t)
	meta := f.db.Metadata()
	require.NoError(
		t,
		saveRewardStakeInputsPending(
			dbadapter.New(f.db),
			nil,
			&rewardStakeInputsPending{},
		),
	)
	raw, err := meta.GetSyncState(rewardStakeInputsPendingKey, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"entries":null}`, raw)
	require.NoError(
		t,
		f.db.MarkUtxosDeletedAtSlot(
			t.Context(),
			nil,
			[]types.UtxoKey{
				{TxId: bytes.Repeat([]byte{0x11}, 32), OutputIdx: 0},
			},
			150,
		),
	)
	pruned, floor, err := f.ls.pruneConsumedUtxosUpTo(
		t.Context(),
		200,
		50,
		1000,
	)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	require.Equal(t, uint64(200), floor)
}
