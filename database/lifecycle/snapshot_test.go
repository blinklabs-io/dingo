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
	"runtime"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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
	t.Parallel()

	const concurrentCommits = 200

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	var wg sync.WaitGroup
	wg.Go(func() {
		for id := uint64(2); id < concurrentCommits+2; id++ {
			txn := db.Transaction(context.Background(), true)
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
	})

	dir := snapshotTestDir(t, "snap-concurrent")
	_, err := lifecycle.Snapshot(
		context.Background(),
		db,
		dir,
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
	)
	wg.Wait()
	require.NoError(t, err)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(),
		newTestStorageHost(t),
		nil,
		dir,
		restoredDir,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)

	_, err = dbtest.NewDatabase(t, &database.Config{DataDir: restoredDir})
	require.NoError(t, err)
}

// Snapshot only reads the source database, so every case that takes one shares
// a database holding block 1.
func TestSnapshot(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	snapshot := func(dir string, opts ...lifecycle.ManifestOption) (lifecycle.Manifest, error) {
		return lifecycle.Snapshot(
			context.Background(), db, dir, lifecycle.TriggerManual,
			"test-version", "badger", "sqlite", opts...,
		)
	}

	t.Run("writes manifest and backup files", func(t *testing.T) {
		dir := snapshotTestDir(t, "snap1")
		m, err := snapshot(dir)
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
	})

	t.Run("records payload digests", func(t *testing.T) {
		key := []byte("operator-trust-root-0123456789ab")
		dir := snapshotTestDir(t, "snap-digests")
		m, err := snapshot(dir, lifecycle.WithManifestKey(key))
		require.NoError(t, err)
		blob, err := os.ReadFile(filepath.Join(dir, lifecycle.BlobBackupFileName))
		require.NoError(t, err)
		metadata, err := os.ReadFile(filepath.Join(dir, lifecycle.MetadataBackupFileName))
		require.NoError(t, err)
		require.Equal(t, sha256Hex(blob), m.BlobSHA256)
		require.Equal(t, sha256Hex(metadata), m.MetadataSHA256)
		require.NoError(t, m.Authenticate(lifecycle.WithManifestKey(key)))
	})

	t.Run("creates a private directory", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Windows does not expose Unix permission bits")
		}
		parentDir := t.TempDir()
		require.NoError(t, os.Chmod(parentDir, 0o700))
		dir := filepath.Join(parentDir, "snap-private")
		_, err := snapshot(dir)
		require.NoError(t, err)

		info, err := os.Stat(dir)
		require.NoError(t, err)
		require.Zero(
			t, info.Mode().Perm()&0o077,
			"snapshot directory must not be accessible by group or other users",
		)
		for _, name := range []string{
			lifecycle.BlobBackupFileName,
			lifecycle.MetadataBackupFileName,
			lifecycle.ManifestFileName,
		} {
			info, err := os.Stat(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Zero(
				t, info.Mode().Perm()&0o077,
				"snapshot file %q must not be accessible by group or other users",
				name,
			)
		}
	})

	t.Run("refuses an existing directory and keeps its contents", func(t *testing.T) {
		dir := snapshotTestDir(t, "snap-contested")
		require.NoError(t, os.MkdirAll(dir, 0o755))
		winnerFile := filepath.Join(dir, "blob.bak")
		require.NoError(t, os.WriteFile(winnerFile, []byte("winner's data"), 0o644))

		_, err := snapshot(dir)
		require.Error(t, err)
		require.DirExists(t, dir)
		data, readErr := os.ReadFile(winnerFile)
		require.NoError(t, readErr)
		require.Equal(t, "winner's data", string(data))
	})

	t.Run("cleans up on failure", func(t *testing.T) {
		dir := snapshotTestDir(t, "snap-fail")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := lifecycle.Snapshot(
			ctx, db, dir, lifecycle.TriggerManual, "test-version",
			"badger", "sqlite",
		)
		require.Error(t, err)
		require.NoDirExists(t, dir)
	})

	t.Run("concurrent calls to one directory leave the winner intact", func(t *testing.T) {
		const attempts = 8
		dir := snapshotTestDir(t, "snap-race")
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, attempts)
		wg.Add(attempts)
		for i := range attempts {
			go func(i int) {
				defer wg.Done()
				<-start
				_, errs[i] = snapshot(dir)
			}(i)
		}
		close(start)
		wg.Wait()

		successes := 0
		for _, err := range errs {
			if err == nil {
				successes++
			}
		}
		require.Equal(
			t, 1, successes,
			"exactly one concurrent Snapshot call to the same dir must succeed",
		)
		require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
		require.FileExists(t, filepath.Join(dir, lifecycle.MetadataBackupFileName))
		m, err := lifecycle.ReadManifest(dir)
		require.NoError(
			t, err,
			"the winner's manifest must be intact, not deleted by a losing caller",
		)
		require.NotZero(t, m.BlobBytes)
		require.NotZero(t, m.MetadataBytes)
	})
}
