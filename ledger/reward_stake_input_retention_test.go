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

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestPendingRewardInputsRetainReconstructionHistory(t *testing.T) {
	t.Parallel()
	fixture := seedRetentionReconstructionFixture(t)
	ctx := context.Background()
	require.NoError(
		t,
		markRewardStakeInputsPending(
			seedDB(t, fixture.db),
			nil,
			retentionRewardSnapshotEpoch,
			100,
		),
	)
	partial, err := seedDB(t, fixture.db).GetRewardStakeInputs(retentionRewardSnapshotEpoch, nil)
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
	restored, err := seedDB(t, fixture.db).GetRewardStakeInputs(
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

func TestPendingRewardInputsIgnoreRolledBackSnapshot(t *testing.T) {
	t.Parallel()
	fixture := seedRetentionReconstructionFixture(t)
	require.NoError(
		t,
		markRewardStakeInputsPending(seedDB(t, fixture.db), nil, 99, 999),
	)
	txn := seedDB(t, fixture.db).BeginLedgerOnly(t.Context(), false)
	defer func() { _ = txn.Rollback() }()
	floor, protected, err := pendingRewardStakeInputHistoryFloor(
		seedDB(t, fixture.db),
		txn,
	)
	require.NoError(t, err)
	require.False(t, protected)
	require.Zero(t, floor)
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
	epoch, err := seedDB(t, f.db).GetEpoch(1, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		seedDB(t, f.db).SetEpoch(100, 1, nil, nil, nil, nil, epoch.EraId, 1, 100, nil),
	)
	ctx := t.Context()
	snapshot, err := seedDB(t, f.db).GetRewardSnapshot(
		retentionRewardSnapshotEpoch,
		"mark",
		nil,
	)
	require.NoError(t, err)
	pools, err := seedDB(t, f.db).GetRewardPoolInputs(retentionRewardSnapshotEpoch, nil)
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
		seedDB(t, f.db).SetSyncState(pendingRatificationSyncKey, string(record), nil),
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
		require.NoError(t, f.ls.ledgerDB().SaveRewardSnapshot(snapshot, tx))
		require.NoError(t, f.ls.ledgerDB().SaveRewardPoolInputs(pools, tx))
		require.NoError(
			t,
			markRewardStakeInputsPending(f.ls.ledgerDB(), tx, 1, 100),
		)
		require.NoError(
			t,
			f.ls.ledgerDB().DeleteSyncState(pendingRatificationSyncKey, tx),
		)
		require.NoError(t, tx.Commit())
	}
	readTxn := f.ls.ledgerDB().BeginLedgerOnly(ctx, false)
	spy := &rewardRetentionHandoffStore{
		LedgerDB: f.ls.ledgerDB(),
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
	rows, err := seedDB(t, f.db).GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, rows, len(f.capturedStakeInputs))
	pruned, _, err = f.ls.pruneConsumedUtxosUpTo(ctx, 200, 50, 1000)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
}

func TestPendingRewardRetentionRatificationRecordValidation(t *testing.T) {
	t.Parallel()
	f := seedRetentionReconstructionFixture(t)
	for _, raw := range []string{"null", "{}", `{"epoch":1,"boundary_slot":100,"id":1,"snapshot":null}`, `{"epoch":1,"boundary_slot":100,"id":1,"snapshot":true,"unknown":1}`, `{"epoch":1,"boundary_slot":101,"id":1,"snapshot":true}`, `{"epoch":99,"boundary_slot":100,"id":1,"snapshot":true}`} {
		require.NoError(
			t,
			seedDB(t, f.db).SetSyncState(pendingRatificationSyncKey, raw, nil),
		)
		_, _, err := f.ls.pruneConsumedUtxosUpTo(t.Context(), 200, 50, 1000)
		require.Error(t, err, raw)
		require.Zero(t, f.ls.announcedUtxoPruneFloor())
	}
	require.NoError(
		t,
		seedDB(t, f.db).SetSyncState(
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
	require.NoError(
		t,
		saveRewardStakeInputsPending(
			seedDB(t, f.db),
			nil,
			&rewardStakeInputsPending{},
		),
	)
	raw, err := seedDB(t, f.db).GetSyncState(rewardStakeInputsPendingKey, nil)
	require.NoError(t, err)
	require.Empty(t, raw)
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

// announcedUtxoPruneFloor returns the highest floor spent-UTxO pruning has
// announced. Rows spent before it may be gone.
func (ls *LedgerState) announcedUtxoPruneFloor() uint64 {
	p := &ls.acquiredPins
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.utxoFloorSlot
}

func TestPendingRewardInputWriteRejectsMalformedRecord(t *testing.T) {
	t.Parallel()
	ls, _ := newRewardCalculationTestLedger(t)
	ldb := ls.ledgerDB()
	valid, err := FormatRewardStakeInputsPending(
		[]RewardStakeInputsPendingEntry{{Epoch: 1, BoundarySlot: 100}},
	)
	require.NoError(t, err)
	for _, invalid := range []string{"{", "null", "{}", `{"entries":{}}`, `{"entries":[],"unknown":1}`} {
		t.Run(invalid, func(t *testing.T) {
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(
				t,
				ldb.SetSyncState(rewardStakeInputsPendingKey, valid, txn),
			)
			require.Error(
				t,
				ldb.SetSyncState(rewardStakeInputsPendingKey, invalid, txn),
			)
			got, err := ldb.GetSyncState(rewardStakeInputsPendingKey, txn)
			require.NoError(t, err)
			require.Equal(
				t,
				valid,
				got,
				"rejected pending record must not replace the staged one",
			)
		})
	}
}
