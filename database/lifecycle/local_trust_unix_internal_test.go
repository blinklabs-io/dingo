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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// privateSnapshotBase returns a directory whose mode does not depend on the
// process umask, so a group-writable mode in a test is always the one the
// test set.
func privateSnapshotBase(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	require.NoError(t, os.Chmod(base, 0o755))
	return base
}

func snapshotForTrustTest(
	t *testing.T,
	dir string,
	opts ...ManifestOption,
) {
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
	snapshotForTrustTest(t, dir)
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
