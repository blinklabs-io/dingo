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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"

	"github.com/stretchr/testify/require"
)

const legacyDeferredHeaderMarkerPrefix = "deferred_header_validation:"

func TestDeferredHeaderFloorIsKeptInTheBlockStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)

	_, found, err := db.DeferredHeaderFloor()
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, db.SetDeferredHeaderFloor(42))
	floor, found, err := db.DeferredHeaderFloor()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(42), floor)

	require.Nil(t, db.Metadata(), "the ledger's database keeps no sync_state")

	require.ErrorIs(
		t,
		db.SetSyncState(DeferredHeaderFloorKey, "7", nil),
		ErrBlockStoreChainState,
	)
	_, err = db.GetSyncState(DeferredHeaderFloorKey, nil)
	require.ErrorIs(t, err, ErrBlockStoreChainState)
	require.ErrorIs(
		t,
		db.DeleteSyncState(DeferredHeaderFloorKey, nil),
		ErrBlockStoreChainState,
	)
	require.ErrorIs(
		t,
		db.SetSyncState(legacyDeferredHeaderMarkerPrefix+"1:ab", "true", nil),
		ErrBlockStoreChainState,
	)

	require.NoError(t, closeTestDatabase(db))
	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	floor, found, err = reopened.DeferredHeaderFloor()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(42), floor)
}

func TestOpenMovesDeferredHeaderStateOutOfSyncState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	// The rows a v0.94.2 store holds: the eviction floor, and markers a
	// release before the blob marker wrote.
	require.NoError(t, db.Metadata().SetSyncState(
		DeferredHeaderFloorKey, "900", nil,
	))
	require.NoError(t, db.Metadata().SetSyncState(
		legacyDeferredHeaderMarkerPrefix+"1000:aa", "true", nil,
	))
	require.NoError(t, db.Metadata().SetSyncState(
		legacyDeferredHeaderMarkerPrefix+"1001:bb", "true", nil,
	))
	// A marker both forms hold keeps its blob payload.
	attributed := []byte{2, 9, 9}
	require.NoError(
		t,
		db.SetDeferredHeaderMarkerWithValue("1001:bb", attributed),
	)
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)

	floor, found, err := reopened.DeferredHeaderFloor()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(900), floor)

	markers, err := reopened.ListDeferredHeaderMarkerValues()
	require.NoError(t, err)
	require.Equal(t, []DeferredHeaderMarker{
		{Key: "1000:aa", Value: []byte{1}},
		{Key: "1001:bb", Value: attributed},
	}, markers)

	// The move copies: the v0.94.2 rows stay.
	row, err := reopened.Metadata().GetSyncState(DeferredHeaderFloorKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, row)
	rows, err := reopened.Metadata().ListSyncStateKeysByPrefix(
		legacyDeferredHeaderMarkerPrefix, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
}

func TestOpenKeepsTheHigherDeferredHeaderFloor(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, db.SetDeferredHeaderFloor(950))
	// A stop between the block-store write and the row's removal leaves both.
	require.NoError(t, db.Metadata().SetSyncState(
		DeferredHeaderFloorKey, "900", nil,
	))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	floor, found, err := reopened.DeferredHeaderFloor()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(950), floor)
}

func TestOpenRefusesAnUnreadableLegacyDeferredHeaderMarker(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetSyncState(
		legacyDeferredHeaderMarkerPrefix+"1000:aa", "true|peer", nil,
	))
	require.NoError(t, closeTestDatabase(db))

	_, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.ErrorContains(t, err, "invalid legacy value")
}

func TestOpenMovesTheHistoryExpiryCursorOutOfSyncState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetSyncState(
		HistoryExpiryCursorKey, "8640", nil,
	))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	cursor, err := reopened.GetChainState(HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "8640", cursor)
	row, err := reopened.Metadata().GetSyncState(HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "8640", row, "the move copies and keeps the row")

	require.ErrorIs(
		t,
		reopened.SetSyncState(HistoryExpiryCursorKey, "1", nil),
		ErrBlockStoreChainState,
	)
	_, err = reopened.GetSyncState(HistoryExpiryCursorKey, nil)
	require.ErrorIs(t, err, ErrBlockStoreChainState)
}

// A rollback that commits lowers the cursor with it, and one that is
// abandoned leaves the cursor where it was.
func TestHistoryExpiryCursorSharesTheRollbackTransaction(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.SetChainState(HistoryExpiryCursorKey, "2000", nil))

	txn := db.Transaction(t.Context(), true)
	require.NoError(t, db.lowerHistoryExpiryCursor(1500, txn))
	require.NoError(t, txn.Rollback())
	cursor, err := db.GetChainState(HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "2000", cursor)

	txn = db.Transaction(t.Context(), true)
	require.NoError(t, db.lowerHistoryExpiryCursor(1500, txn))
	require.NoError(t, txn.Commit())
	cursor, err = db.GetChainState(HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "1500", cursor)
}

// The pruner saves the cursor from its own transaction while a rollback is
// open. The rollback's commit must not fail on that write.
func TestRollbackCommitsWhileThePrunerSavesTheCursor(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.SetChainState(HistoryExpiryCursorKey, "2000", nil))

	txn := db.Transaction(t.Context(), true)
	require.NoError(t, db.lowerHistoryExpiryCursor(1500, txn))
	require.NoError(t, db.SetChainState(HistoryExpiryCursorKey, "2100", nil))
	require.NoError(t, txn.Commit())

	cursor, err := db.GetChainState(HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "1500", cursor)
}

func TestForgeFenceIsKeptInTheBlockStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	const name = types.ForgeFenceChainStatePrefix + "a1"

	require.NoError(t, db.SetChainStateSynced(name, "fence"))
	got, err := db.ChainStateValue(name)
	require.NoError(t, err)
	require.Equal(t, "fence", got)
	require.Nil(t, db.Metadata(), "the ledger's database keeps no sync_state")

	require.ErrorIs(
		t, db.SetSyncState(name, "x", nil), ErrBlockStoreChainState,
	)
	_, err = db.GetSyncState(name, nil)
	require.ErrorIs(t, err, ErrBlockStoreChainState)
	require.ErrorIs(
		t, db.DeleteSyncState(name, nil), ErrBlockStoreChainState,
	)
}

func TestOpenMovesForgeFencesOutOfSyncState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	const (
		poolA = types.ForgeFenceChainStatePrefix + "a1"
		poolB = types.ForgeFenceChainStatePrefix + "b2"
	)
	// The rows a v0.94.2 block producer holds, one per pool it forged for.
	require.NoError(t, db.Metadata().SetSyncState(poolA, "fence-a", nil))
	require.NoError(t, db.Metadata().SetSyncState(poolB, "fence-b", nil))
	require.NoError(t, closeTestDatabase(db))

	for range 2 {
		db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
		require.NoError(t, err)
		for name, want := range map[string]string{
			poolA: "fence-a", poolB: "fence-b",
		} {
			got, err := db.ChainStateValue(name)
			require.NoError(t, err)
			require.Equal(t, want, got)
			row, err := db.Metadata().GetSyncState(name, nil)
			require.NoError(t, err)
			require.Equal(t, want, row, "the move copies and keeps the row")
		}
		require.NoError(t, closeTestDatabase(db))
	}
}

// A v0.94.2 block producer keeps its persisted leader schedules in
// sync_state; the open moves them beside the blocks, where the schedule store
// reads them.
func TestOpenMovesLeaderSchedulesOutOfSyncState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	const name = types.LeaderScheduleChainStatePrefix + "a1:7"
	require.NoError(t, db.Metadata().SetSyncState(name, "schedule", nil))
	require.NoError(t, closeTestDatabase(db))

	db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	got, err := db.ChainStateValue(name)
	require.NoError(t, err)
	require.Equal(t, "schedule", got)
	row, err := db.Metadata().GetSyncState(name, nil)
	require.NoError(t, err)
	require.Equal(t, "schedule", row, "the move copies and keeps the row")
	require.NoError(t, closeTestDatabase(db))
}

// The row stays after its copy. The fence may then advance before the next
// open, which must not put the older row back.
func TestOpenKeepsTheBlockStoreForgeFenceOverALeftoverRow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	const name = types.ForgeFenceChainStatePrefix + "a1"
	require.NoError(t, db.SetChainStateSynced(name, "newer"))
	require.NoError(t, db.Metadata().SetSyncState(name, "older", nil))
	require.NoError(t, closeTestDatabase(db))

	db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	got, err := db.ChainStateValue(name)
	require.NoError(t, err)
	require.Equal(t, "newer", got)
	row, err := db.Metadata().GetSyncState(name, nil)
	require.NoError(t, err)
	require.Equal(t, "older", row, "the move copies and keeps the row")
}

// A metadata store opened with a blob store it was not created with keeps
// its rows: copying them would put them where its own blob store never
// sees them.
func TestOpenLeavesChainStateRowsOfAnotherBlobStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetSyncState(
		RollbackHighWaterKey, "7", nil,
	))
	require.NoError(t, closeTestDatabase(db))

	stores := storesWithFreshBlob(t, dir)
	_, err = New(t.Context(), &Config{DataDir: dir, Role: RoleReleased}, stores)
	require.Error(t, err)
	row, err := stores.Metadata.GetSyncState(RollbackHighWaterKey, nil)
	require.NoError(t, err)
	require.Equal(t, "7", row)
}

// movedSyncStateValues are the single values a v0.94.2 database holds in
// sync_state and this release keeps in the block store.
var movedSyncStateValues = []string{
	ConsensusProviderIdentityKey,
	ImmutableUtxoOffsetsTipKey,
	RollbackHighWaterKey,
	PendingTruncateKey,
}

func TestOpenMovesSingleValuesOutOfSyncState(t *testing.T) {
	t.Parallel()

	for _, name := range movedSyncStateValues {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
			require.NoError(t, err)
			require.NoError(
				t, db.Metadata().SetSyncState(name, "released", nil),
			)
			require.NoError(t, closeTestDatabase(db))

			for range 2 {
				db, err = newTestDatabaseAt(
					t, dir, dir, &Config{DataDir: dir, Role: RoleReleased},
				)
				require.NoError(t, err)
				got, err := db.GetChainState(name, nil)
				require.NoError(t, err)
				require.Equal(t, "released", got)
				row, err := db.Metadata().GetSyncState(name, nil)
				require.NoError(t, err)
				require.Equal(
					t, "released", row, "the move copies and keeps the row",
				)
				require.NoError(t, closeTestDatabase(db))
			}

			// A value written after the move is not replaced by a row
			// an interrupted move left behind.
			db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
			require.NoError(t, err)
			require.NoError(t, db.SetChainState(name, "newer", nil))
			require.NoError(
				t, db.Metadata().SetSyncState(name, "released", nil),
			)
			require.NoError(t, closeTestDatabase(db))
			db, err = newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
			require.NoError(t, err)
			got, err := db.GetChainState(name, nil)
			require.NoError(t, err)
			require.Equal(t, "newer", got)

			require.ErrorIs(
				t,
				db.SetSyncState(name, "x", nil),
				ErrBlockStoreChainState,
			)
			_, err = db.GetSyncState(name, nil)
			require.ErrorIs(t, err, ErrBlockStoreChainState)
			require.ErrorIs(
				t, db.DeleteSyncState(name, nil), ErrBlockStoreChainState,
			)
		})
	}
}

// Each copy of a v0.94.2 chain-state row is flushed to the block store before
// the move goes on, as every other chain-state value is.
func TestMovedChainStateIsFlushed(t *testing.T) {
	t.Parallel()
	real, err := newTestDatabase(
		t, &Config{DataDir: t.TempDir(), Role: RoleReleased},
	)
	require.NoError(t, err)
	counting := &syncCountingBlob{BlobStore: real.Blob()}
	db := &Database{
		config:   &Config{},
		logger:   real.logger,
		metadata: real.Metadata(),
		blobRef:  newBlobStoreRef(counting),
	}
	for key, value := range map[string]string{
		HistoryExpiryCursorKey:                    "8640",
		DeferredHeaderFloorKey:                    "900",
		legacyDeferredHeaderMarkerPrefix + "1:ab": "true",
	} {
		require.NoError(t, db.Metadata().SetSyncState(key, value, nil))
	}
	for name, move := range map[string]func() error{
		"history-expiry cursor": func() error {
			return db.moveSyncStateValue(HistoryExpiryCursorKey)
		},
		"deferred-header floor":   db.moveDeferredHeaderFloor,
		"deferred-header markers": db.moveLegacyDeferredHeaderMarkers,
	} {
		before := counting.syncs.Load()
		require.NoError(t, move(), name)
		require.Greater(
			t, counting.syncs.Load(), before, "%s copy was not flushed", name,
		)
	}
}

// A rollback drops the held blocks above its point, in the rollback's own
// transaction, and keeps the ones at or below it.
func TestHistoryExpiryHeldLowersWithTheRollback(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	hash := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	require.NoError(t, db.SetHistoryExpiryHeld([]HeldBlock{
		{Slot: 10, Hash: hash(1)}, {Slot: 20, Hash: hash(2)},
		{Slot: 30, Hash: hash(3)},
	}))

	txn := db.Transaction(t.Context(), true)
	require.NoError(t, db.lowerHistoryExpiryHeld(20, txn))
	require.NoError(t, txn.Rollback())
	held, err := db.HistoryExpiryHeld()
	require.NoError(t, err)
	require.Len(t, held, 3, "an abandoned rollback leaves the held blocks")

	txn = db.Transaction(t.Context(), true)
	require.NoError(t, db.lowerHistoryExpiryHeld(20, txn))
	require.NoError(t, txn.Commit())
	held, err = db.HistoryExpiryHeld()
	require.NoError(t, err)
	require.Equal(t, []HeldBlock{
		{Slot: 10, Hash: hash(1)}, {Slot: 20, Hash: hash(2)},
	}, held)
}
