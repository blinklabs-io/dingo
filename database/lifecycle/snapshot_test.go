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
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestSnapshotWritesManifestAndBackupFiles verifies that Snapshot writes
// a manifest plus blob/metadata backup files with matching commit timestamps.
func TestSnapshotWritesManifestAndBackupFiles(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	dir := filepath.Join(t.TempDir(), "snap1")
	m, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version",
	)
	require.NoError(t, err)
	require.Equal(t, "badger", m.BlobPlugin)
	require.Equal(t, "sqlite", m.MetadataPlugin)
	require.NotZero(t, m.BlobBytes)
	require.NotZero(t, m.MetadataBytes)

	got, err := lifecycle.ReadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, m.CommitTimestamp, got.CommitTimestamp)

	require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
	require.FileExists(t, filepath.Join(dir, lifecycle.MetadataBackupFileName))
}

// TestSnapshotRefusesExistingDirectory verifies that Snapshot errors when
// the target directory already exists.
func TestSnapshotRefusesExistingDirectory(t *testing.T) {
	db := newTestDB(t)
	dir := t.TempDir() // already exists

	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version",
	)
	require.Error(t, err)
}

// TestSnapshotConsistentUnderConcurrentWrites is the end-to-end regression
// test for the blob/metadata drift bug: Snapshot backs up blob then
// metadata sequentially, so without Database.PauseCommits bracketing both
// calls, a dual-store commit landing in between writes its commit
// timestamp to one store's backup but not the other's, and the restored
// copy fails database.New's cross-store consistency check. It fires a
// bounded burst of real dual-store commits concurrently with Snapshot and
// confirms the restored copy still opens cleanly; the count is fixed
// rather than tied to Snapshot's own completion, so this test's worst-case
// runtime doesn't depend on how long the snapshot happens to take.
func TestSnapshotConsistentUnderConcurrentWrites(t *testing.T) {
	const concurrentCommits = 200

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := uint64(2); id < concurrentCommits+2; id++ {
			txn := db.Transaction(true)
			if err := db.BlockCreate(testBlock(id, byte(id)), txn); err != nil {
				txn.Rollback() //nolint:errcheck
				return
			}
			tip := ochainsync.Tip{
				Point:       ocommon.Point{Slot: id * 10},
				BlockNumber: id,
			}
			if err := db.SetTip(tip, txn); err != nil {
				txn.Rollback() //nolint:errcheck
				return
			}
			if err := txn.Commit(); err != nil {
				return
			}
		}
	}()

	dir := filepath.Join(t.TempDir(), "snap-concurrent")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version",
	)
	wg.Wait()
	require.NoError(t, err)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(context.Background(), dir, restoredDir)
	require.NoError(t, err)

	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", restoredDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", restoredDir,
	))
	restored, err := database.New(&database.Config{
		DataDir:        restoredDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	defer restored.Close()
}

// TestSnapshotCleansUpOnFailure verifies that a Snapshot failure (here, a
// pre-cancelled context) removes the partially-created directory.
func TestSnapshotCleansUpOnFailure(t *testing.T) {
	db := newTestDB(t)
	dir := filepath.Join(t.TempDir(), "snap-fail")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := lifecycle.Snapshot(
		ctx, db, dir, lifecycle.TriggerManual, "test-version",
	)
	require.Error(t, err)
	require.NoDirExists(t, dir)
}
