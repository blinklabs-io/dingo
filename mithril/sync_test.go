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
//
// The steps run in order against one database: opening a migrated database
// dominates the cost of each check, and every step only reads the state the
// previous steps left.
func TestDetermineSyncMode(t *testing.T) {
	// Not t.Parallel: the steps share one database and depend on its state.
	db := newSyncModeTestDB(t)
	expectMode := func(t *testing.T, want syncMode) {
		t.Helper()
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, want, mode)
	}

	t.Run("empty database is bootstrap", func(t *testing.T) {
		expectMode(t, syncModeBootstrap)
	})

	t.Run("in_progress is resume", func(t *testing.T) {
		require.NoError(
			t, db.SetSyncState("sync_status", syncStatusInProgress, nil),
		)
		expectMode(t, syncModeResume)
	})

	t.Run("backfill is resume", func(t *testing.T) {
		require.NoError(
			t, db.SetSyncState("sync_status", syncStatusBackfill, nil),
		)
		expectMode(t, syncModeResume)
	})

	t.Run("unknown non-empty status is resume", func(t *testing.T) {
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
		expectMode(t, syncModeResume)
	})

	t.Run("complete database with blocks is catch-up", func(t *testing.T) {
		require.NoError(t, db.DeleteSyncState("sync_status", nil))
		expectMode(t, syncModeCatchUp)
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
	for _, tc := range []struct {
		name   string
		status string // "" leaves the database fresh
		want   bool
	}{
		{"fresh database needs a sync", "", true},
		{"in_progress needs a sync", syncStatusInProgress, true},
		{"backfill is servable without a full resync", syncStatusBackfill, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			if tc.status != "" {
				// Close the handle before NeedsSync reopens by DataDir.
				db, err := dbtest.NewDatabase(t, &database.Config{
					DataDir: dataDir,
					Logger:  logger,
				})
				require.NoError(t, err)
				require.NoError(t, db.SetSyncState("sync_status", tc.status, nil))
				require.NoError(t, dbtest.CloseDatabase(db))
			}
			need, err := NeedsSync(SyncConfig{DataDir: dataDir, Logger: logger})
			require.NoError(t, err)
			require.Equal(t, tc.want, need)
		})
	}
}
