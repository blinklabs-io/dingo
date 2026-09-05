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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// newSyntheticCostModelTestDatabase builds a DB with a persisted epoch
// table (epochs 0-9, 100 slots each: E0 = [0,100), E1 = [100,200), ...)
// so EpochBySlot -- which RecomputeSyntheticV2CostModelMarkerAfterTruncate
// depends on -- can resolve any rollback slot this file seeds.
func newSyntheticCostModelTestDatabase(t *testing.T) *Database {
	t.Helper()
	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	for i := range uint64(10) {
		require.NoError(t, db.SetEpoch(
			i*100, i,
			nil, nil, nil, nil,
			1, 1000, 100,
			nil,
		))
	}
	return db
}

// TestRecomputeSyntheticV2CostModelMarkerAfterTruncate_UndoesClearWhenRollbackCrossesBack
// covers blinklabs-io/dingo#3825's PR review (wolf31o2): a rollback to
// before the epoch that confirmed real PlutusV2 cost-model data must undo
// that confirmation, since the surviving chain (possibly a fork that never
// re-enacts it) can no longer prove the write happened.
func TestRecomputeSyntheticV2CostModelMarkerAfterTruncate_UndoesClearWhenRollbackCrossesBack(
	t *testing.T,
) {
	db := newSyntheticCostModelTestDatabase(t)

	// Simulate real PlutusV2 cost-model data confirmed at epoch 5 (slot 500).
	require.NoError(
		t,
		db.SetSyncState(SyntheticV2CostModelSyncKey, "false", nil),
	)
	require.NoError(t, SetSyntheticV2CostModelClearedEpoch(db, nil, 5))

	// Roll back to slot 250 (epoch 2) -- before the confirming epoch.
	require.NoError(
		t,
		RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 250),
	)

	value, err := db.GetSyncState(SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, value,
		"the boolean marker must be cleared (not forced to \"true\"), so a"+
			" later read falls back to comparing the live PlutusV2 cost"+
			" model directly -- correct even for a chain whose real model"+
			" predates this marker entirely")

	_, cleared, err := SyntheticV2CostModelClearedEpoch(db, nil)
	require.NoError(t, err)
	require.False(t, cleared,
		"the cleared-epoch marker must be removed so a later re-sync"+
			" re-derives synthetic status instead of trusting a stale"+
			" confirmation")
}

// TestRecomputeSyntheticV2CostModelMarkerAfterTruncate_KeepsClearWhenRollbackDoesNotCrossBack
// covers the other direction: a rollback that does not cross back before
// the confirming epoch must leave the marker exactly as it was.
func TestRecomputeSyntheticV2CostModelMarkerAfterTruncate_KeepsClearWhenRollbackDoesNotCrossBack(
	t *testing.T,
) {
	db := newSyntheticCostModelTestDatabase(t)

	require.NoError(
		t,
		db.SetSyncState(SyntheticV2CostModelSyncKey, "false", nil),
	)
	require.NoError(t, SetSyntheticV2CostModelClearedEpoch(db, nil, 5))

	// Roll back to slot 650 (epoch 6) -- at/after the confirming epoch.
	require.NoError(
		t,
		RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 650),
	)

	value, err := db.GetSyncState(SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "false", value,
		"the boolean marker must remain real: this rollback never"+
			" crossed back before the confirming epoch")

	clearedEpoch, cleared, err := SyntheticV2CostModelClearedEpoch(db, nil)
	require.NoError(t, err)
	require.True(t, cleared)
	require.Equal(t, uint64(5), clearedEpoch)
}

// TestRecomputeSyntheticV2CostModelMarkerAfterTruncate_NoOpWhenNeverCleared
// covers the common case: a chain that has never confirmed real PlutusV2
// cost-model data has no cleared-epoch marker to reset, so any rollback is
// a no-op here regardless of depth.
func TestRecomputeSyntheticV2CostModelMarkerAfterTruncate_NoOpWhenNeverCleared(
	t *testing.T,
) {
	db := newSyntheticCostModelTestDatabase(t)

	require.NoError(
		t,
		RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 50),
	)

	value, err := db.GetSyncState(SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, value)
	_, cleared, err := SyntheticV2CostModelClearedEpoch(db, nil)
	require.NoError(t, err)
	require.False(t, cleared)
}
