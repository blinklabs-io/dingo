package mithril

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func testStoragePlugins() StoragePlugins {
	return StoragePlugins{
		Blob:     plugin.Selection{Provider: "badger", Config: map[string]any{}},
		Metadata: plugin.Selection{Provider: "sqlite", Config: map[string]any{}},
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	cfg := SyncConfig{
		DataDir:        dataDir,
		Logger:         logger,
		StoragePlugins: testStoragePlugins(),
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
