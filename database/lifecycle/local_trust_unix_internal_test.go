//go:build !windows

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

package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// privateSnapshotBase returns a directory whose mode does not depend on the
// process umask, so a group-writable mode in a test is always the one the
// test set.
func privateSnapshotBase(t *testing.T) string {
	t.Helper()
	return testutil.OwnerOnlyTempDir(t)
}

// trustSnapshots holds one unkeyed and one keyed snapshot of a database with a
// single block, each built once per test binary. The trust tests only need a
// valid snapshot in a particular place with particular permissions, so each
// gets a copy instead of opening a database and taking a snapshot of its own.
var trustSnapshots struct {
	mu   sync.Mutex
	dirs map[bool]string // keyed -> parent of the "snap" directory
}

func init() {
	RegisterSharedFixtureCleanup(func() {
		for _, dir := range trustSnapshots.dirs {
			_ = os.RemoveAll(dir)
		}
	})
}

func sharedTrustSnapshot(t *testing.T, keyed bool) string {
	t.Helper()
	trustSnapshots.mu.Lock()
	defer trustSnapshots.mu.Unlock()
	if dir, ok := trustSnapshots.dirs[keyed]; ok {
		return filepath.Join(dir, "snap")
	}
	if trustSnapshots.dirs == nil {
		trustSnapshots.dirs = map[bool]string{}
	}
	parent, err := os.MkdirTemp("", "dingo-lifecycle-trust-")
	require.NoError(t, err)
	trustSnapshots.dirs[keyed] = parent
	var opts []ManifestOption
	if keyed {
		opts = append(opts, WithManifestKey(catalogTestTrustKey))
	}
	dir := filepath.Join(parent, "snap")
	buildTrustSnapshot(t, dir, opts...)
	return dir
}

// buildTrustSnapshot takes a real snapshot of a one-block database at dir.
func buildTrustSnapshot(t *testing.T, dir string, opts ...ManifestOption) {
	t.Helper()
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))
	_, err := Snapshot(
		context.Background(), db, dir,
		TriggerManual, "test", "badger", "sqlite",
		opts...,
	)
	require.NoError(t, err)
}

// snapshotForTrustTest places a snapshot named "snap" at dir, signed when a
// manifest key option is supplied. The snapshot ID is bound to the directory
// name, so dir must end in "snap".
func snapshotForTrustTest(
	t *testing.T,
	dir string,
	opts ...ManifestOption,
) {
	t.Helper()
	require.Equal(t, "snap", filepath.Base(dir))
	src := sharedTrustSnapshot(t, len(opts) > 0)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, entry.Name()), data, 0o600,
		))
	}
}

func restoreForTrustTest(
	t *testing.T,
	snapshotDir string,
	opts ...ManifestOption,
) (string, error) {
	t.Helper()
	target := filepath.Join(t.TempDir(), "restored")
	_, err := Restore(
		context.Background(),
		newRestoreInternalTestHost(t),
		nil,
		snapshotDir,
		target,
		RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
		opts...,
	)
	return target, err
}

func TestUnkeyedLocalRestoreRequiresPrivateSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		widen  func(base, snapshotDir string) string
		refuse bool
	}{
		{name: "private", widen: func(string, string) string { return "" }},
		{
			name:   "group-writable directory",
			widen:  func(_, dir string) string { return dir },
			refuse: true,
		},
		{
			name: "group-writable payload",
			widen: func(_, dir string) string {
				return filepath.Join(dir, BlobBackupFileName)
			},
			refuse: true,
		},
		{
			name: "group-writable manifest",
			widen: func(_, dir string) string {
				return filepath.Join(dir, ManifestFileName)
			},
			refuse: true,
		},
		{
			name:   "group-writable ancestor without sticky bit",
			widen:  func(base, _ string) string { return filepath.Join(base, "outer") },
			refuse: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := privateSnapshotBase(t)
			dir := filepath.Join(base, "outer", "snap")
			snapshotForTrustTest(t, dir)
			if path := tc.widen(base, dir); path != "" {
				info, err := os.Stat(path)
				require.NoError(t, err)
				require.NoError(t, os.Chmod(path, info.Mode().Perm()|0o020))
			}
			target, err := restoreForTrustTest(t, dir)
			if !tc.refuse {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrSnapshotTrustRequired)
			require.NoDirExists(t, target)
		})
	}
}

func TestUnkeyedLocalRestoreAcceptsStickyAncestor(t *testing.T) {
	t.Parallel()
	base := privateSnapshotBase(t)
	shared := filepath.Join(base, "shared")
	require.NoError(t, os.Mkdir(shared, 0o755))
	require.NoError(t, os.Chmod(shared, 0o777|os.ModeSticky))
	dir := filepath.Join(shared, "snap")
	snapshotForTrustTest(t, dir)
	_, err := restoreForTrustTest(t, dir)
	require.NoError(t, err)
}

func TestUnkeyedLocalRestoreChecksSymlinkTargetAncestors(t *testing.T) {
	t.Parallel()
	base := privateSnapshotBase(t)
	shared := filepath.Join(base, "shared")
	require.NoError(t, os.Mkdir(shared, 0o755))
	require.NoError(t, os.Chmod(shared, 0o775))
	real := filepath.Join(shared, "real")
	require.NoError(t, os.Mkdir(real, 0o755))
	link := filepath.Join(base, "link")
	require.NoError(t, os.Symlink(real, link))
	require.NoError(t, os.Chmod(shared, 0o755))
	dir := filepath.Join(link, "snap")
	snapshotForTrustTest(t, dir)
	require.NoError(t, os.Chmod(shared, 0o775))

	target, err := restoreForTrustTest(t, dir)
	require.ErrorIs(t, err, ErrSnapshotTrustRequired)
	require.NoDirExists(t, target)
}

func TestKeyedLocalRestoreIgnoresSnapshotPermissions(t *testing.T) {
	t.Parallel()
	base := privateSnapshotBase(t)
	dir := filepath.Join(base, "snap")
	snapshotForTrustTest(t, dir, WithManifestKey(catalogTestTrustKey))
	require.NoError(t, os.Chmod(base, 0o775))
	require.NoError(t, os.Chmod(dir, 0o775))
	require.NoError(t, os.Chmod(filepath.Join(dir, BlobBackupFileName), 0o664))

	_, err := restoreForTrustTest(t, dir, WithManifestKey(catalogTestTrustKey))
	require.NoError(t, err)
}

func TestSnapshotRefusesGroupWritableParent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		parent func(base string) string
	}{
		{name: "existing parent", parent: func(base string) string { return base }},
		{
			name:   "parent created below writable ancestor",
			parent: func(base string) string { return filepath.Join(base, "new") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := privateSnapshotBase(t)
			require.NoError(t, os.Chmod(base, 0o775))
			parent := tc.parent(base)
			db := newRestoreInternalTestDB(t)
			require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))
			_, err := Snapshot(
				context.Background(), db, filepath.Join(parent, "snap"),
				TriggerManual, "test", "badger", "sqlite",
			)
			require.ErrorContains(t, err, "writable by group or other without the sticky bit")
			require.NoDirExists(t, filepath.Join(parent, "snap"))
			if parent != base {
				require.NoDirExists(t, parent)
			}
		})
	}
}

func TestSnapshotStagesMetadataBesideSnapshot(t *testing.T) {
	// Not t.Parallel: swaps the package-level syncSnapshotFile seam.
	base := privateSnapshotBase(t)
	dir := filepath.Join(base, "snap")
	original := syncSnapshotFile
	t.Cleanup(func() { syncSnapshotFile = original })
	var staged []string
	syncSnapshotFile = func(file *os.File) error {
		if file.Name() == filepath.Join(dir, BlobBackupFileName) {
			matches, err := filepath.Glob(
				filepath.Join(base, ".dingo-snapshot-metadata-*", MetadataBackupFileName),
			)
			require.NoError(t, err)
			staged = matches
		}
		return original(file)
	}
	buildTrustSnapshot(t, dir)
	require.Len(t, staged, 1, "metadata backup was not staged beside the snapshot")
	entries, err := os.ReadDir(base)
	require.NoError(t, err)
	require.Len(t, entries, 1, "metadata staging directory was left behind")
	_, err = ReadManifest(dir)
	require.NoError(t, err)
}

func TestSnapshotDirectoryReplacedLeavesReplacementUntouched(t *testing.T) {
	// Not t.Parallel: swaps the package-level syncSnapshotFile seam.
	base := privateSnapshotBase(t)
	dir := filepath.Join(base, "snap")
	moved := dir + ".moved"
	sentinel := filepath.Join(dir, "sentinel")
	original := syncSnapshotFile
	t.Cleanup(func() { syncSnapshotFile = original })
	syncSnapshotFile = func(file *os.File) error {
		if strings.HasSuffix(file.Name(), BlobBackupFileName) {
			require.NoError(t, os.Rename(dir, moved))
			require.NoError(t, os.Mkdir(dir, 0o755))
			require.NoError(t, os.WriteFile(sentinel, []byte("x"), 0o600))
		}
		return original(file)
	}
	db := newRestoreInternalTestDB(t)
	require.NoError(t, db.BlockCreate(newRestoreInternalTestBlock(), nil))
	_, err := Snapshot(
		context.Background(), db, dir,
		TriggerManual, "test", "badger", "sqlite",
	)
	require.ErrorIs(t, err, ErrSnapshotDirectoryReplaced)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "replacement directory was modified")
	require.Equal(t, "sentinel", entries[0].Name())
	movedEntries, err := os.ReadDir(moved)
	require.NoError(t, err)
	require.Empty(t, movedEntries, "opened snapshot directory was not emptied")
}
