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
	"encoding/hex"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestImportProgressIsKeptInTheBlockStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)

	for _, key := range []string{
		SyncStatusKey,
		ImportedLedgerStateSlotKey,
		ImportedLedgerStateHashKey,
		"mithril_immutable_max",
		"mithril_pinned_artifact",
		"mithril_reward_repair_pending",
		LedgerReimportSyncKey,
		LedgerReimportSwappedSyncKey,
	} {
		require.ErrorIs(
			t, db.SetSyncState(key, "1", nil), ErrBlockStoreChainState, key,
		)
		_, err := db.GetSyncState(key, nil)
		require.ErrorIs(t, err, ErrBlockStoreChainState, key)
		require.ErrorIs(
			t, db.DeleteSyncState(key, nil), ErrBlockStoreChainState, key,
		)
	}

	hash := bytes.Repeat([]byte{0xab}, 32)
	require.NoError(t, db.SetImportedLedgerStateBoundary(
		ocommon.NewPoint(77, hash), nil,
	))
	slot, err := db.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(77), slot)
	gotHash, err := db.ImportedLedgerStateBoundaryHash()
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(hash), gotHash)
	require.Nil(t, db.Metadata(), "the ledger's database keeps no sync_state")

	// A boundary without a hash drops the hash of the point it replaces.
	require.NoError(t, db.SetImportedLedgerStateBoundary(
		ocommon.NewPoint(90, nil), nil,
	))
	gotHash, err = db.ImportedLedgerStateBoundaryHash()
	require.NoError(t, err)
	require.Empty(t, gotHash)

	require.Error(t, db.SetImportedLedgerStateBoundary(
		ocommon.NewPoint(91, bytes.Repeat([]byte{1}, 33)), nil,
	))
	slot, err = db.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(90), slot, "a refused boundary was stored")
}

func TestOpenMovesImportProgressOutOfSyncState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	hash := hex.EncodeToString(bytes.Repeat([]byte{0xcd}, 32))
	// The rows a v0.94.2 store can hold.
	rows := map[string]string{
		SyncStatusKey:                     "in_progress",
		ImportedLedgerStateSlotKey:        "500",
		ImportedLedgerStateHashKey:        hash,
		"mithril_immutable_max":           "12",
		"mithril_pinned_artifact":         `{"digest":"aa"}`,
		"mithril_snapshot_restore_active": "blocks",
		"mithril_reward_repair_pending":   "1",
		LedgerReimportSyncKey:             "1",
	}
	for key, value := range rows {
		require.NoError(t, db.Metadata().SetSyncState(key, value, nil))
	}
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	for key, value := range rows {
		got, err := reopened.GetChainState(key, nil)
		require.NoError(t, err)
		require.Equal(t, value, got, key)
		row, err := reopened.Metadata().GetSyncState(key, nil)
		require.NoError(t, err)
		if key == ImportedLedgerStateSlotKey ||
			key == ImportedLedgerStateHashKey {
			// The metadata store's block counts read the boundary row.
			require.Equal(t, value, row, key)
			continue
		}
		require.Empty(t, row, "%s row was left in sync_state", key)
	}
	slot, err := reopened.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(500), slot)

	// The boundary is written only to the block store from here on, so the
	// row left behind never replaces a newer one.
	require.NoError(t, reopened.SetImportedLedgerStateBoundary(
		ocommon.NewPoint(900, nil), nil,
	))
	require.NoError(t, closeTestDatabase(reopened))
	again, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	slot, err = again.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(900), slot)
	gotHash, err := again.ImportedLedgerStateBoundaryHash()
	require.NoError(t, err)
	require.Empty(t, gotHash, "the replaced boundary's hash came back")
}

func TestOpenLeavesABackfillStatusRowInPlace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetSyncState(
		SyncStatusKey, "backfill", nil,
	))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	status, err := reopened.GetChainState(SyncStatusKey, nil)
	require.NoError(t, err)
	require.Equal(t, "backfill", status)
	row, err := reopened.Metadata().GetSyncState(SyncStatusKey, nil)
	require.NoError(t, err)
	require.Equal(
		t, "backfill", row,
		"the row adoption refuses the database by was removed",
	)
}

func TestOpenKeepsAnEmptyBoundaryRowMalformed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	slot, err := db.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Zero(t, slot, "no boundary is recorded")

	require.NoError(t, db.Metadata().SetSyncState(
		ImportedLedgerStateSlotKey, "", nil,
	))
	require.NoError(t, closeTestDatabase(db))

	reopened, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir, Role: RoleReleased})
	require.NoError(t, err)
	_, err = reopened.ImportedLedgerStateBoundarySlotStrict(nil)
	require.ErrorContains(t, err, "empty value")
	require.Zero(t, reopened.ImportedLedgerStateBoundarySlot(nil))
}

func TestIndexerDBKeepsItsOwnBoundary(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, db.SetImportedLedgerStateBoundary(
		ocommon.NewPoint(300, bytes.Repeat([]byte{7}, 32)), nil,
	))

	idx := &Database{
		config:   &Config{Role: RoleIndexer},
		logger:   db.logger,
		metadata: storesWithFreshBlob(t, t.TempDir()).Metadata,
		blobRef:  newBlobStoreRef(db.Blob()),
	}
	// The IndexerDB reads the ledger's block store, and the boundary there
	// is the ledger's: its own is recorded once its replay reaches it.
	slot, err := idx.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Zero(t, slot)

	require.NoError(t, idx.SetSyncState(
		ImportedLedgerStateSlotKey, "120", nil,
	))
	slot, err = idx.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(120), slot)
	slot, err = db.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(300), slot)
}

// A checkpoint answers for the import that wrote it and for no other, and a
// later import's checkpoint replaces it.
func TestImportCheckpointAnswersOnlyForItsImport(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := newTestDatabaseAt(t, dir, dir, &Config{DataDir: dir})
	require.NoError(t, err)
	phase, err := db.ImportCheckpoint("aa:10")
	require.NoError(t, err)
	require.Empty(t, phase)

	require.NoError(t, db.SetImportCheckpoint("aa:10", "utxo"))
	require.NoError(t, db.SetImportCheckpoint("aa:10", "certstate"))
	phase, err = db.ImportCheckpoint("aa:10")
	require.NoError(t, err)
	require.Equal(t, "certstate", phase)
	phase, err = db.ImportCheckpoint("bb:20")
	require.NoError(t, err)
	require.Empty(t, phase)

	require.NoError(t, db.SetImportCheckpoint("bb:20", "utxo"))
	phase, err = db.ImportCheckpoint("aa:10")
	require.NoError(t, err)
	require.Empty(t, phase)
}
