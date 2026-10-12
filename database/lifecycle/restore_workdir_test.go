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
	"time"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestCleanStaleRestoreWorkDirs(t *testing.T) {
	parent := t.TempDir()
	staleNames := []string{
		".dingo-restore-payloads-stale",
		".dingo-cloud-snapshot-stale",
		".dingo-verify-snapshot-stale",
	}
	for _, name := range staleNames {
		path := filepath.Join(parent, name)
		require.NoError(t, os.Mkdir(path, 0o700))
		old := time.Now().Add(-8 * 24 * time.Hour)
		require.NoError(t, os.Chtimes(path, old, old))
	}
	recent := filepath.Join(parent, ".dingo-restore-payloads-active")
	require.NoError(t, os.Mkdir(recent, 0o700))

	require.NoError(t, lifecycle.CleanStaleRestoreWorkDirs(parent))
	for _, name := range staleNames {
		_, err := os.Stat(filepath.Join(parent, name))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	_, err := os.Stat(recent)
	require.NoError(t, err)
}

// unusableSystemTempDir points the system temp directory at a path that does
// not exist, so any snapshot-sized copy made there fails the restore. The
// test's own t.TempDir root must already exist before this is called.
func unusableSystemTempDir(t *testing.T) {
	t.Helper()
	missing := filepath.Join(t.TempDir(), "no-such-tmp")
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, missing)
	}
}

// requireOnlyTarget asserts the restore left nothing beside its target.
func requireOnlyTarget(t *testing.T, target string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(target))
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Equal(t, []string{filepath.Base(target)}, names)
}

// A local restore copies its payloads beside the target, never into the
// system temp directory, and removes the copies once done.
func TestRestoreLocalPayloadCopiesStayBesideTarget(t *testing.T) {
	// Not t.Parallel: t.Setenv.
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := lifecycle.Snapshot(
		context.Background(), db, snapshotDir,
		lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)
	workParent := t.TempDir()
	target := filepath.Join(workParent, "restored")
	for _, name := range []string{
		".dingo-restore-payloads-stale",
		".dingo-cloud-snapshot-stale",
		".dingo-verify-snapshot-stale",
	} {
		stale := filepath.Join(workParent, name)
		require.NoError(t, os.Mkdir(stale, 0o700))
		old := time.Now().Add(-8 * 24 * time.Hour)
		require.NoError(t, os.Chtimes(stale, old, old))
	}
	unusableSystemTempDir(t)

	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), nil, snapshotDir, target,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
	)
	require.NoError(t, err)
	requireOnlyTarget(t, target)
}

// A cloud restore downloads the manifest and payloads beside the target,
// never into the system temp directory, and removes them once done.
func TestRestoreFromCloudDownloadsBesideTarget(t *testing.T) {
	// Not t.Parallel: the fake cloud fixture is process-global, and t.Setenv.
	uri, _, _ := cloudSnapshot(t, testTrustKey)
	unusableSystemTempDir(t)

	target, err := restoreFrom(t, uri, testTrustKey)
	require.NoError(t, err)
	requireOnlyTarget(t, target)
}
