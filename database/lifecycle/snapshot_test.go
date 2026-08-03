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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
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
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version", "badger", "sqlite",
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
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version", "badger", "sqlite",
	)
	require.Error(t, err)
}

// TestSnapshotRefusingExistingDirectoryDoesNotDeleteItsContents guards
// against a real data-loss bug: two concurrent Snapshot
// calls racing to the same dir used to both pass an initial
// os.Stat-then-MkdirAll check (MkdirAll doesn't error on an existing
// directory, and the Stat-then-MkdirAll gap is a TOCTOU race even if it
// did), so the loser would proceed to open blob.bak with O_EXCL, fail
// with "file exists", and then run its failure-cleanup os.RemoveAll(dir)
// — deleting the winner's in-progress or already-completed backup files
// out from under it. This simulates the loser's view directly: a dir
// that already contains another caller's file when Snapshot is invoked
// must be refused without that file being removed.
func TestSnapshotRefusingExistingDirectoryDoesNotDeleteItsContents(t *testing.T) {
	db := newTestDB(t)

	dir := filepath.Join(t.TempDir(), "snap-contested")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	winnerFile := filepath.Join(dir, "blob.bak")
	require.NoError(t, os.WriteFile(winnerFile, []byte("winner's data"), 0o644))

	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version", "badger", "sqlite",
	)
	require.Error(t, err)

	require.DirExists(t, dir)
	require.FileExists(t, winnerFile)
	data, readErr := os.ReadFile(winnerFile)
	require.NoError(t, readErr)
	require.Equal(t, "winner's data", string(data))
}

// TestSnapshotConcurrentCallsToSameDirLeaveWinnersFilesIntact is the
// direct concurrency regression test for the same data-loss bug above: several Snapshot
// calls racing to create the exact same, not-yet-existing dir used to be
// able to all pass an os.Stat-then-MkdirAll check (MkdirAll doesn't error
// on an existing directory, and Stat-then-MkdirAll has a TOCTOU gap
// between the two calls regardless), so more than one caller could
// believe it owned dir; the loser(s) would then fail opening blob.bak
// with O_EXCL and run their failure-cleanup os.RemoveAll(dir) — deleting
// the winner's in-progress or already-completed backup files. Firing many
// callers at the same dir simultaneously (released off a shared barrier
// to maximize interleaving) must now yield exactly one success whose
// output is a complete, valid, undamaged snapshot, with every other
// caller failing immediately without touching dir's contents at all.
func TestSnapshotConcurrentCallsToSameDirLeaveWinnersFilesIntact(t *testing.T) {
	const attempts = 8

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	dir := filepath.Join(t.TempDir(), "snap-race")

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, attempts)
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := lifecycle.Snapshot(
				context.Background(), db, dir, lifecycle.TriggerManual, "test-version", "badger", "sqlite",
			)
			errs[i] = err
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
	require.Equal(t, 1, successes, "exactly one concurrent Snapshot call to the same dir must succeed")

	require.DirExists(t, dir)
	require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
	require.FileExists(t, filepath.Join(dir, lifecycle.MetadataBackupFileName))
	m, err := lifecycle.ReadManifest(dir)
	require.NoError(t, err, "the winner's manifest must be intact, not deleted by a losing caller")
	require.NotZero(t, m.BlobBytes)
	require.NotZero(t, m.MetadataBytes)
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
	wg.Go(func() {
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
	})

	dir := filepath.Join(t.TempDir(), "snap-concurrent")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version", "badger", "sqlite",
	)
	wg.Wait()
	require.NoError(t, err)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(context.Background(), newTestStorageHost(t), nil, dir, restoredDir,
		lifecycle.RestoreStorageConfig{},
	)
	require.NoError(t, err)

	_, err = dbtest.NewDatabase(t, &database.Config{DataDir: restoredDir})
	require.NoError(t, err)
}

// TestSnapshotCleansUpOnFailure verifies that a Snapshot failure (here, a
// pre-cancelled context) removes the partially-created directory.
func TestSnapshotCleansUpOnFailure(t *testing.T) {
	db := newTestDB(t)
	dir := filepath.Join(t.TempDir(), "snap-fail")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := lifecycle.Snapshot(
		ctx, db, dir, lifecycle.TriggerManual, "test-version", "badger", "sqlite",
	)
	require.Error(t, err)
	require.NoDirExists(t, dir)
}
