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

package lifecycle_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestSnapshotRestoreRoundTrip verifies that a snapshotted database
// restores into a fresh directory with the same blocks and tip.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, src.BlockCreate(testBlock(2, 0x02), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	snapMan, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	restoreMan, err := lifecycle.Restore(
		context.Background(), snapshotDir, targetDir,
	)
	require.NoError(t, err)
	require.Equal(t, snapMan.CommitTimestamp, restoreMan.CommitTimestamp)
	require.Equal(t, snapMan.TipSlot, restoreMan.TipSlot)

	// Reopen the restored data dir like a normal node startup would and
	// confirm the blocks survived the round trip.
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", targetDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", targetDir,
	))
	restored, err := database.New(&database.Config{
		DataDir:        targetDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	defer restored.Close()

	block1, err := restored.BlockByIndex(1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block1.ID)
	block2, err := restored.BlockByIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block2.ID)
}

// TestRestoreRefusesNonEmptyTargetDirectory verifies that Restore errors
// when the target directory already contains a file.
func TestRestoreRefusesNonEmptyTargetDirectory(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)

	// Target dir already has a file in it.
	targetDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(targetDir, "existing.txt"), []byte("data"), 0o644,
	))

	_, err = lifecycle.Restore(context.Background(), snapshotDir, targetDir)
	require.Error(t, err)
}

// TestRestoreRejectsPluginMismatch verifies that a manifest's recorded
// plugins must match the target plugins via CheckPluginMatch.
func TestRestoreRejectsPluginMismatch(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	m, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)
	require.NoError(t, m.CheckPluginMatch("badger", "sqlite"))
	require.Error(t, m.CheckPluginMatch("gcs", "sqlite"))
}
