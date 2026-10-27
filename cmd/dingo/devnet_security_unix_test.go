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

func TestOpenDevnetStateRootRestrictsExistingDirectory(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	require.NoError(t, os.Chmod(runDir, 0o777))

	root, err := openDevnetStateRoot(runDir)
	require.NoError(t, err)
	require.NoError(t, root.Close())

	info, err := os.Stat(runDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
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
