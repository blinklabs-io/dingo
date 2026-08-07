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

package sqlstore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublishBackupFileRoundTrip validates the basic happy path: the
// write callback's output ends up at dstPath, including creating any
// missing parent directories.
func TestPublishBackupFileRoundTrip(t *testing.T) {
	dstDir := filepath.Join(t.TempDir(), "nested")
	dst := filepath.Join(dstDir, "backup.bin")
	err := PublishBackupFile(dst, func(stagedPath string) error {
		return os.WriteFile(stagedPath, []byte("hello"), 0o600)
	})
	require.NoError(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

// TestPublishBackupFileRejectsExistingDestination validates that
// PublishBackupFile refuses to run the write callback at all when
// dstPath already exists, leaving the existing file untouched.
func TestPublishBackupFileRejectsExistingDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, os.WriteFile(dst, []byte("existing"), 0o600))
	err := PublishBackupFile(dst, func(stagedPath string) error {
		return os.WriteFile(stagedPath, []byte("new"), 0o600)
	})
	require.Error(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("existing"), data)
}

// TestSyncFile validates that syncFile durably persists a real file's
// contents without error, and reports a clean error for a path that
// doesn't exist -- PublishBackupFile relies on the latter to fail loudly
// if a write callback ever claims success without actually producing a
// file at stagedPath.
func TestSyncFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "staged.bin")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
	require.NoError(t, syncFile(path))

	require.Error(t, syncFile(filepath.Join(t.TempDir(), "missing.bin")))
}

// TestPublishBackupFileSyncsStagedFileEvenWhenWriteCallbackDoesNot guards
// a real gap: PublishBackupFile assumed its write callback already
// fsynced the staged file, which holds for sqlite's VACUUM INTO but not
// for pg_dump/mysqldump, neither of which fsyncs its own output --
// without an explicit sync here, a crash right after publish could leave
// a durable directory entry pointing at a file whose contents were never
// actually flushed to disk. Every existing test callback in this file
// already writes via a plain os.WriteFile (no explicit sync of its own),
// so a successful round trip here is already exercising the callback-
// doesn't-sync path; this test additionally confirms the published file's
// content survived intact through that path.
func TestPublishBackupFileSyncsStagedFileEvenWhenWriteCallbackDoesNot(
	t *testing.T,
) {
	dst := filepath.Join(t.TempDir(), "backup.bin")
	err := PublishBackupFile(dst, func(stagedPath string) error {
		// Deliberately unsynced write, mirroring pg_dump/mysqldump's own
		// behavior of never fsyncing the file they produce.
		return os.WriteFile(stagedPath, []byte("unsynced-by-caller"), 0o600)
	})
	require.NoError(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("unsynced-by-caller"), data)
}

// TestCreateDirDurable validates the basic happy path (a multi-level
// nested directory that doesn't exist yet is created successfully) --
// its actual crash-durability guarantee (each created level's directory
// entry fsynced before relying on it) isn't observable from a single-
// process test, but this at least guards the plain MkdirAll-equivalent
// behavior every caller depends on.
func TestCreateDirDurable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "dir")
	require.NoError(t, CreateDirDurable(dir))
	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// Calling it again against an already-existing directory (nothing left
	// to create) must be a harmless no-op, not an error.
	require.NoError(t, CreateDirDurable(dir))
}

// TestPublishBackupFileFailureDoesNotClobberConcurrentDestination guards the
// TOCTOU property PublishBackupFile exists for: a failed write must not
// touch dstPath even if something else created it concurrently, in the
// window between the initial existence check and the failure.
func TestPublishBackupFileFailureDoesNotClobberConcurrentDestination(
	t *testing.T,
) {
	dst := filepath.Join(t.TempDir(), "backup.bin")
	err := PublishBackupFile(dst, func(stagedPath string) error {
		require.NoError(t, os.WriteFile(stagedPath, []byte("partial"), 0o600))
		require.NoError(t, os.WriteFile(dst, []byte("concurrent"), 0o600))
		return errors.New("simulated failure")
	})
	require.Error(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("concurrent"), data)
}

// TestPublishBackupFileCleansUpDestinationOnLateFailure guards a real gap:
// once os.Link publishes dstPath, every remaining step (the directory
// syncs, removing the staging directory) was already just making that
// publish more durable, not deciding whether it happened -- but a failure
// in any of them still made this function report the whole call as
// failed, while leaving dstPath behind to permanently fail the
// "destination already exists" check at the top of any retry, forcing an
// operator to notice and delete it by hand. Simulates a late failure by
// making the staging directory itself undeletable (chmod 0o500, no write
// permission) right after the file is written into it -- os.Link into
// dstDir still succeeds since that's a different directory, but the later
// os.RemoveAll(tmpDir) then fails.
func TestPublishBackupFileCleansUpDestinationOnLateFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permission bits")
	}
	dst := filepath.Join(t.TempDir(), "backup.bin")
	var tmpDir string
	err := PublishBackupFile(dst, func(stagedPath string) error {
		tmpDir = filepath.Dir(stagedPath)
		if err := os.WriteFile(stagedPath, []byte("hello"), 0o600); err != nil {
			return err
		}
		return os.Chmod(tmpDir, 0o500)
	})
	t.Cleanup(func() { _ = os.Chmod(tmpDir, 0o700) })
	require.Error(t, err)
	_, statErr := os.Stat(dst)
	require.True(
		t, errors.Is(statErr, fs.ErrNotExist),
		"a late failure must clean up the already-published destination "+
			"too, so a retry isn't permanently blocked",
	)
}

// TestPublishBackupFileFailsWhenPublishedDestinationVanishes guards a real
// gap: the verification lstat right after os.Link used to be consulted
// only to decide whether the deferred cleanup should run, not treated as
// a failure of the publish itself -- so if dstPath disappeared (or became
// unreadable) in the instant after a successful Link, this function could
// still reach the end of its happy path and return nil, reporting success
// with no durable backup file actually in place.
func TestPublishBackupFileFailsWhenPublishedDestinationVanishes(t *testing.T) {
	original := lstatFile
	t.Cleanup(func() { lstatFile = original })
	lstatFile = func(string) (os.FileInfo, error) {
		return nil, errors.New("simulated lstat failure right after publish")
	}

	dst := filepath.Join(t.TempDir(), "backup.bin")
	err := PublishBackupFile(dst, func(stagedPath string) error {
		return os.WriteFile(stagedPath, []byte("hello"), 0o600)
	})
	require.Error(
		t, err,
		"a failed post-publish verification must fail PublishBackupFile, "+
			"not be silently swallowed while the function still reports success",
	)
	_, statErr := os.Stat(dst)
	require.True(
		t, errors.Is(statErr, fs.ErrNotExist),
		"a failed post-publish verification must still attempt the same "+
			"cleanup every other late failure does (falling back to the "+
			"still-staged file's identity, since there is no publishedInfo "+
			"to compare against), not strand dstPath with no cleanup "+
			"attempt at all",
	)
}
