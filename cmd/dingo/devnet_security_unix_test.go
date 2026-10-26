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

//go:build unix

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/devnetkeys"
	"github.com/stretchr/testify/require"
)

func TestPrepareDevnetStateRejectsSymlinkedConfig(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	start := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	require.NoError(t, prepareDevnetStateAtPath(runDir, true, false, start))

	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(target, []byte("unchanged"), 0o600))
	configPath := filepath.Join(runDir, "dingo.yaml")
	require.NoError(t, os.Remove(configPath))
	require.NoError(t, os.Symlink(target, configPath))

	err := prepareDevnetStateAtPath(runDir, true, false, start)
	require.ErrorContains(t, err, "is not a regular file")
	data, readErr := os.ReadFile(target)
	require.NoError(t, readErr)
	require.Equal(t, "unchanged", string(data))
}

func TestAcquireDevnetStateLockRejectsSymlink(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "real"), nil, 0o600))
	require.NoError(t, os.Symlink("real", filepath.Join(runDir, devnetStateLock)))
	root, err := os.OpenRoot(runDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })

	release, err := acquireDevnetStateLock(root, runDir)
	require.Nil(t, release)
	require.ErrorContains(t, err, "is a symlink")
}

func TestPrepareDevnetStateResetUsesPinnedRoot(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	runDir := filepath.Join(parent, "state")
	start := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	require.NoError(t, prepareDevnetStateAtPath(runDir, true, false, start))
	root, err := openDevnetStateRoot(runDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })

	pinnedDir := filepath.Join(parent, "pinned")
	require.NoError(t, os.Rename(runDir, pinnedDir))
	victimDir := filepath.Join(parent, "victim")
	require.NoError(t, os.MkdirAll(filepath.Join(victimDir, "data"), 0o700))
	victim := filepath.Join(victimDir, "data", "keep")
	require.NoError(t, os.WriteFile(victim, []byte("unchanged"), 0o600))
	require.NoError(t, os.Symlink(victimDir, runDir))

	require.NoError(t, prepareDevnetState(root, runDir, true, true, start))
	data, err := os.ReadFile(victim)
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(data))
	require.FileExists(t, filepath.Join(pinnedDir, "dingo.yaml"))
	require.DirExists(t, filepath.Join(pinnedDir, "data"))
}

func TestWriteLocalDevnetConfigReplacesSymlink(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	root, err := openDevnetStateRoot(runDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(target, []byte("unchanged"), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(runDir, "dingo.yaml")))

	require.NoError(t, writeLocalDevnetConfig(
		root,
		"dingo.yaml",
		filepath.Join(runDir, "cardano"),
		filepath.Join(runDir, "data"),
	))
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(data))
	info, err := os.Lstat(filepath.Join(runDir, "dingo.yaml"))
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
}

func TestDevnetRunDirectoryResolvesSymlinkedDataDir(t *testing.T) {
	t.Parallel()

	realDir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(realDir, link))
	want, err := filepath.EvalSymlinks(realDir)
	require.NoError(t, err)

	got, err := devnetRunDirectory(link)
	require.NoError(t, err)
	require.Equal(t, want, got)
	root, err := openDevnetStateRoot(got)
	require.NoError(t, err)
	require.NoError(t, root.Close())
}

func TestDevnetRunDirectoryResolvesSymlinkedAncestorOfNewDir(t *testing.T) {
	t.Parallel()

	realParent := t.TempDir()
	linkParent := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(realParent, linkParent))
	wantParent, err := filepath.EvalSymlinks(realParent)
	require.NoError(t, err)

	got, err := devnetRunDirectory(filepath.Join(linkParent, "a", "devnet"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(wantParent, "a", "devnet"), got)

	release, err := prepareLockedDevnetState(got, true, false, time.Now())
	require.NoError(t, err)
	require.NoError(t, release())
	info, err := os.Stat(filepath.Join(realParent, "a", "devnet"))
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestDevnetRunDirectoryTemporaryUnderSymlinkedTempDir(t *testing.T) {
	realTmp := t.TempDir()
	linkTmp := filepath.Join(t.TempDir(), "tmp")
	require.NoError(t, os.Symlink(realTmp, linkTmp))
	t.Setenv("TMPDIR", linkTmp)
	wantParent, err := filepath.EvalSymlinks(realTmp)
	require.NoError(t, err)

	got, err := devnetRunDirectory("")
	require.NoError(t, err)
	require.Equal(t, wantParent, filepath.Dir(got))

	release, err := prepareLockedDevnetState(got, false, false, time.Now())
	require.NoError(t, err)
	require.NoError(t, release())
}

// resolvedTempDir returns a test directory whose own path has no symlinked
// component, as on platforms whose temporary root is itself a symlink.
func resolvedTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return dir
}

func TestPrepareLockedDevnetStateRejectsComponentSwappedAfterResolution(
	t *testing.T,
) {
	t.Parallel()

	base := resolvedTempDir(t)
	victim := resolvedTempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(base, "a"), 0o700))
	got, err := devnetRunDirectory(filepath.Join(base, "a", "b", "state"))
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(base, "a")))
	require.NoError(t, os.Symlink(victim, filepath.Join(base, "a")))

	release, err := prepareLockedDevnetState(got, true, false, time.Now())
	if release != nil {
		require.NoError(t, release())
	}
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(victim, "b"))
}

func TestPrepareLockedDevnetStateRejectsSymlinkedAncestor(t *testing.T) {
	t.Parallel()

	parent := resolvedTempDir(t)
	victim := resolvedTempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(victim, "existing"), 0o700))
	redirect := filepath.Join(parent, "redirect")
	require.NoError(t, os.Symlink(victim, redirect))
	runDir := filepath.Join(redirect, "existing", "state")

	release, err := prepareLockedDevnetState(
		runDir,
		true,
		false,
		time.Date(2026, time.October, 25, 12, 0, 0, 0, time.UTC),
	)
	if release != nil {
		require.NoError(t, release())
	}
	require.ErrorContains(t, err, "is not a directory")
	require.NoDirExists(t, filepath.Join(victim, "existing", "state"))
}

func TestPrepareLockedDevnetStateRestrictsExistingDirectory(t *testing.T) {
	t.Parallel()

	runDir := resolvedTempDir(t)
	require.NoError(t, os.Chmod(runDir, 0o777))

	release, err := prepareLockedDevnetState(
		runDir,
		true,
		false,
		time.Date(2026, time.October, 25, 12, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)
	require.NoError(t, release())

	info, err := os.Stat(runDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestPrepareLockedDevnetStateDoesNotRestrictUnrecognizedDirectory(
	t *testing.T,
) {
	t.Parallel()

	runDir := resolvedTempDir(t)
	require.NoError(t, os.Chmod(runDir, 0o777))
	require.NoError(t, os.WriteFile(
		filepath.Join(runDir, "keep"),
		[]byte("user data"),
		0o600,
	))

	release, err := prepareLockedDevnetState(
		runDir,
		true,
		false,
		time.Date(2026, time.October, 25, 12, 0, 0, 0, time.UTC),
	)
	require.Nil(t, release)
	require.ErrorContains(t, err, "without a Dingo devnet marker")

	info, err := os.Stat(runDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o777), info.Mode().Perm())
}

func TestInstallLocalTestFaucetInRootRejectsSymlinkedIntermediate(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(runDir, "other", "keys"), 0o700))
	require.NoError(t, os.Symlink("other", filepath.Join(runDir, "cardano")))
	root, err := openDevnetStateRoot(runDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })

	err = devnetkeys.InstallLocalTestFaucetInRoot(
		root,
		filepath.Join("cardano", "keys"),
	)
	require.ErrorContains(t, err, "is not a directory")
	require.NoFileExists(t, filepath.Join(runDir, "other", "keys", "faucet.skey"))
}
