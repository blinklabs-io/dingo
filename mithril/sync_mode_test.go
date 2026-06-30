package mithril

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func newSyncModeTestDB(t *testing.T) *database.Database {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := database.New(&database.Config{
		DataDir:        t.TempDir(),
		Logger:         logger,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestDetermineSyncMode pins the state-only dispatch used by Sync to decide
// whether a run is a fresh bootstrap, a resume of an interrupted/backfilling
// sync, or a catch-up of an already-complete database.
func TestDetermineSyncMode(t *testing.T) {
	t.Run("empty database is bootstrap", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		mode, err := determineSyncMode(db)
		require.NoError(t, err)
		require.Equal(t, syncModeBootstrap, mode)
	})

	t.Run("in_progress is resume", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		require.NoError(
			t, db.SetSyncState("sync_status", syncStatusInProgress, nil),
		)
		mode, err := determineSyncMode(db)
		require.NoError(t, err)
		require.Equal(t, syncModeResume, mode)
	})

	t.Run("backfill is resume", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		require.NoError(
			t, db.SetSyncState("sync_status", syncStatusBackfill, nil),
		)
		mode, err := determineSyncMode(db)
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
		mode, err := determineSyncMode(db)
		require.NoError(t, err)
		require.Equal(t, syncModeCatchUp, mode)
	})
}
