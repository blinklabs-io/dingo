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

package mithril

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func newSyncModeTestDB(t *testing.T) *database.Database {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

// TestDetermineSyncMode pins the state-only dispatch used by Sync to decide
// whether a run is a fresh bootstrap, a resume of an interrupted/backfilling
// sync, or a catch-up of an already-complete database.
func TestDetermineSyncMode(t *testing.T) {
	t.Parallel()

	t.Run("empty database is bootstrap", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, syncModeBootstrap, mode)
	})

	t.Run("in_progress is resume", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		require.NoError(
			t, db.SetSyncState("sync_status", syncStatusInProgress, nil),
		)
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, syncModeResume, mode)
	})

	t.Run("backfill is resume", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		require.NoError(
			t, db.SetSyncState("sync_status", syncStatusBackfill, nil),
		)
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, syncModeResume, mode)
	})

	t.Run("unknown non-empty status is resume", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     42,
			Hash:     bytes.Repeat([]byte{0xaa}, 32),
			PrevHash: bytes.Repeat([]byte{0xbb}, 32),
			Cbor:     []byte{0x80},
			Number:   7,
			Type:     6,
		}, nil))
		require.NoError(
			t, db.SetSyncState("sync_status", "unknown_interrupted_phase", nil),
		)
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, syncModeResume, mode)
	})

	t.Run("complete database with blocks is catch-up", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		block := models.Block{
			Slot:     42,
			Hash:     bytes.Repeat([]byte{0xaa}, 32),
			PrevHash: bytes.Repeat([]byte{0xbb}, 32),
			Cbor:     []byte{0x80},
			Number:   7,
			Type:     6,
		}
		require.NoError(t, db.BlockCreate(block, nil))
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, syncModeCatchUp, mode)
	})
}

func TestRepairCatchUpDecisionForcesReconciliation(t *testing.T) {
	t.Parallel()

	decision := repairCatchUpDecision(
		catchUpDecision{upToDate: true}, true, 1234, true,
	)
	require.True(t, decision.engage)
	require.False(t, decision.upToDate)
	require.EqualValues(t, 1234, decision.start)

	unchanged := catchUpDecision{upToDate: true}
	require.Equal(
		t, unchanged,
		repairCatchUpDecision(unchanged, false, 1234, true),
	)
	active := catchUpDecision{engage: true, start: 20}
	require.Equal(
		t, active,
		repairCatchUpDecision(active, true, 1234, true),
	)
}

func testStoragePlugins() StoragePlugins {
	return StoragePlugins{
		Blob: plugin.Selection{
			Provider: "badger",
			Config:   map[string]any{},
		},
		Metadata: plugin.Selection{
			Provider: "sqlite",
			Config:   map[string]any{},
		},
	}
}

// TestNeedsSyncReflectsSyncStatus drives the public NeedsSync against a real
// on-disk database to pin its contract: a fresh database (empty sync_status,
// no chain data) needs a sync, an "in_progress" status needs a sync, and a
// "backfill" status is servable without a full resync. NeedsSync opens the
// database by DataDir, so each sync-state write goes through its own handle
// that is closed before the next NeedsSync call, leaving the database lock
// free for NeedsSync to re-open it.
func TestNeedsSyncReflectsSyncStatus(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	cfg := SyncConfig{
		DataDir: dataDir,
		Logger:  logger,
	}

	// setSyncStatus opens the database at dataDir, writes sync_status, and
	// closes it so the subsequent NeedsSync call can acquire the lock.
	setSyncStatus := func(status string) {
		t.Helper()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir: dataDir,
			Logger:  logger,
		})
		require.NoError(t, err)
		require.NoError(t, db.SetSyncState("sync_status", status, nil))
		require.NoError(t, dbtest.CloseDatabase(db))
	}

	// Fresh database: empty sync_status and no chain data → needs sync.
	need, err := NeedsSync(cfg)
	require.NoError(t, err)
	require.True(t, need, "fresh database should need a sync")

	// in_progress → needs sync.
	setSyncStatus(syncStatusInProgress)
	need, err = NeedsSync(cfg)
	require.NoError(t, err)
	require.True(t, need, "in_progress should need a sync")

	// backfill → servable, no full resync.
	setSyncStatus(syncStatusBackfill)
	need, err = NeedsSync(cfg)
	require.NoError(t, err)
	require.False(t, need, "backfill should not need a full resync")

	// Unknown non-empty statuses are treated as incomplete.
	setSyncStatus("unknown_interrupted_phase")
	need, err = NeedsSync(cfg)
	require.NoError(t, err)
	require.True(t, need, "unknown sync_status should need a sync")
}
