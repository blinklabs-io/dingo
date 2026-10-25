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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeRestoreDirectory struct {
	renames      []string
	cleanupErr   error
	cleanupCalls int
}

func (d *fakeRestoreDirectory) rename(name, _ string) error {
	d.renames = append(d.renames, name)
	return nil
}

func (d *fakeRestoreDirectory) cleanup() error {
	d.cleanupCalls++
	return d.cleanupErr
}

func (d *fakeRestoreDirectory) disposeEmpty() error {
	return d.cleanup()
}

func TestRestoreDirCleanupKeepsSubstitutedDirectory(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const name = "staging"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(path, "ours"), []byte("ours"), 0o600,
	))
	dir, err := openRestoreDir(parent, name, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.closeRoot() })

	moved := filepath.Join(parentPath, "moved")
	if !replaceOpenRestorePath(t, path, moved) {
		return
	}
	require.NoError(t, os.Mkdir(path, 0o700))
	replacement := filepath.Join(path, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	err = dir.cleanup()
	require.ErrorIs(t, err, errRestoreDirReplaced)
	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	_, err = os.Stat(filepath.Join(moved, "ours"))
	require.True(t, errors.Is(err, os.ErrNotExist))
}

func TestCleanStaleRestoreEntryKeepsSubstitutedDirectory(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const name = ".dingo-restore-staging-old"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	expected, err := os.Lstat(path)
	require.NoError(t, err)
	require.NoError(t, os.Rename(path, filepath.Join(parentPath, "moved")))
	require.NoError(t, os.Mkdir(path, 0o700))
	replacement := filepath.Join(path, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	err = cleanStaleRestoreEntry(
		parent, parentPath, name, expected, time.Now().Add(time.Hour),
	)
	require.ErrorIs(t, err, errRestoreDirReplaced)
	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

func TestCleanStaleRestoreEntryAllowsConcurrentRemoval(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const name = ".dingo-restore-staging-old"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	expected, err := os.Lstat(path)
	require.NoError(t, err)
	if !removeOpenRestorePath(t, path) {
		return
	}

	require.NoError(t, cleanStaleRestoreEntry(
		parent, parentPath, name, expected, time.Now().Add(time.Hour),
	))
}

func TestCleanupStaleRestoreDirAllowsRemovalAfterOpen(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const name = ".dingo-restore-staging-old"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	dir, err := openRestoreDir(parent, name, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.closeRoot() })
	if !removeOpenRestorePath(t, path) {
		return
	}

	require.NoError(t, cleanupStaleRestoreDir(dir, path))
}

func TestRestoreDirCleanupAllowsRemovedEmptyShell(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })
	const name = "staging"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	dir, err := openRestoreDir(parent, name, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.closeRoot() })
	if !removeOpenRestorePath(t, path) {
		return
	}
	require.NoError(t, dir.cleanup())
}

func TestRestoreDirCleanupAllowsRemovalAfterIdentityCheck(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })
	const name = "staging"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	dir, err := openRestoreDir(parent, name, path)
	require.NoError(t, err)
	dir.removeName = func(string) error { return fs.ErrNotExist }

	require.NoError(t, dir.cleanup())
}

func TestOwnedRestoreDirCleanupKeepsSubstitutedDirectory(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	owned, err := createOwnedRestoreDir(parentPath, ".dingo-owned-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = owned.close() })
	originalPath := owned.dir.path
	movedPath := originalPath + "-moved"
	if !replaceOpenRestorePath(t, originalPath, movedPath) {
		return
	}
	require.NoError(t, os.Mkdir(originalPath, 0o700))
	replacement := filepath.Join(originalPath, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	err = owned.cleanup()
	require.ErrorIs(t, err, errRestoreDirReplaced)
	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

func TestRemoveCreatedRestoreDirKeepsSubstitution(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })
	const name = "created"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	created, err := parent.Lstat(name)
	require.NoError(t, err)
	require.NoError(t, os.Rename(path, path+"-moved"))
	require.NoError(t, os.WriteFile(path, []byte("replacement"), 0o600))

	err = removeCreatedRestoreDir(parent, name, created)
	require.ErrorIs(t, err, errRestoreDirReplaced)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), data)
}

func TestRetainCreatedRestoreDirRejectsSubstitution(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	const name = "created"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	created, err := parent.Lstat(name)
	require.NoError(t, err)
	require.NoError(t, os.Rename(path, path+"-moved"))
	require.NoError(t, os.Mkdir(path, 0o700))
	replacement := filepath.Join(path, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	_, err = retainCreatedRestoreDir(parent, nil, created, name, path)
	require.ErrorIs(t, err, errRestoreDirReplaced)
	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

func TestRestoreDirDisposeEmptyPreservesConcurrentContent(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })
	const name = "target"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	dir, err := openRestoreDir(parent, name, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.closeRoot() })
	marker := filepath.Join(path, "concurrent")
	require.NoError(t, os.WriteFile(marker, []byte("keep"), 0o600))

	require.Error(t, dir.disposeEmpty())
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

func TestRestoreRollbackDoesNotReacquireAfterCleanupFailure(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	owned, err := createOwnedRestoreDir(parentPath, ".dingo-rollback-")
	require.NoError(t, err)
	path := owned.dir.path
	moved := path + "-moved"
	if !replaceOpenRestorePath(t, path, moved) {
		return
	}
	require.NoError(t, os.Mkdir(path, 0o700))
	first := filepath.Join(path, "first")
	require.NoError(t, os.WriteFile(first, []byte("keep"), 0o600))

	rollback := restoreRollback{dir: path, dirHandle: owned}
	firstErr := rollback.removeDir()
	require.ErrorIs(t, firstErr, errRestoreDirReplaced)
	require.NoError(t, os.RemoveAll(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	victim := filepath.Join(path, "victim")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))

	require.ErrorIs(t, rollback.removeDir(), errRestoreDirReplaced)
	data, err := os.ReadFile(victim)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

func TestRestoreDirRenameDoesNotPublishSubstitutedSource(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const name = "staging"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	dir, err := openRestoreDir(parent, name, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.closeRoot() })

	if !replaceOpenRestorePath(
		t, path, filepath.Join(parentPath, "moved"),
	) {
		return
	}
	require.NoError(t, os.Mkdir(path, 0o700))
	replacement := filepath.Join(path, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))
	target := filepath.Join(parentPath, "target")

	err = dir.rename("target", target)
	require.ErrorIs(t, err, errRestoreDirReplaced)
	require.NoFileExists(t, filepath.Join(target, "replacement"))
	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

func TestRestoreDirRenameDoesNotReplaceDestination(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const sourceName = "staging"
	sourcePath := filepath.Join(parentPath, sourceName)
	require.NoError(t, os.Mkdir(sourcePath, 0o700))
	dir, err := openRestoreDir(parent, sourceName, sourcePath)
	require.NoError(t, err)

	const targetName = "target"
	targetPath := filepath.Join(parentPath, targetName)
	require.NoError(t, os.Mkdir(targetPath, 0o700))
	marker := filepath.Join(targetPath, "replacement")
	require.NoError(t, os.WriteFile(marker, []byte("keep"), 0o600))

	require.Error(t, dir.rename(targetName, targetPath))
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	require.NoError(t, dir.checkNamed())
}

func TestRestoreDirRenameReportsRollbackFailure(t *testing.T) {
	parentPath := t.TempDir()
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parent.Close()) })

	const sourceName = "staging"
	sourcePath := filepath.Join(parentPath, sourceName)
	require.NoError(t, os.Mkdir(sourcePath, 0o700))
	dir, err := openRestoreDir(parent, sourceName, sourcePath)
	require.NoError(t, err)

	const targetName = "target"
	targetPath := filepath.Join(parentPath, targetName)
	require.NoError(t, os.Mkdir(targetPath, 0o700))
	originalRename := dir.renameNoReplace
	call := 0
	dir.renameNoReplace = func(root *os.Root, oldName, newName string) error {
		call++
		err := originalRename(root, oldName, newName)
		if call == 1 && err == nil {
			require.NoError(t, root.Mkdir(sourceName, 0o700))
		}
		return err
	}

	err = dir.rename(targetName, targetPath)
	require.ErrorContains(t, err, "restore directory rename rollback")
	require.Equal(t, 3, call)
	require.True(t, strings.HasPrefix(dir.name, ".dingo-restore-move-"))
	require.NoError(t, dir.checkNamed())
}

func TestActivateRestoreDirsDisposesOriginalBeforePublishing(t *testing.T) {
	t.Parallel()

	cleanupErr := errors.New("cleanup failed")
	original := &fakeRestoreDirectory{cleanupErr: cleanupErr}
	staging := &fakeRestoreDirectory{}

	activated, err := activateRestoreDirs(
		staging,
		original,
		t.TempDir(),
		"target",
		filepath.Join(t.TempDir(), "target"),
	)
	require.False(t, activated)
	require.ErrorIs(t, err, cleanupErr)
	require.Equal(t, 1, original.cleanupCalls)
	require.Len(t, original.renames, 2)
	require.True(t, strings.HasPrefix(
		original.renames[0],
		".dingo-restore-target-",
	))
	require.Equal(t, "target", original.renames[1])
	require.Empty(t, staging.renames)
}
