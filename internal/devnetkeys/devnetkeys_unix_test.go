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

//go:build !windows

package devnetkeys

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstallLocalTestKeysRestrictsExistingPaths(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.Chmod(dir, 0o777))

	oldFiles := make(map[string]os.FileInfo)
	for _, name := range []string{"vrf.skey", "kes.skey", "opcert.cert"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
		require.NoError(t, os.Chmod(path, 0o666))
		info, err := os.Stat(path)
		require.NoError(t, err)
		oldFiles[name] = info
	}
	hardlink := filepath.Join(t.TempDir(), "vrf.skey")
	require.NoError(t, os.Link(filepath.Join(dir, "vrf.skey"), hardlink))

	require.NoError(t, InstallLocalTestKeys(dir))

	dirInfo, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	for _, name := range []string{"vrf.skey", "kes.skey", "opcert.cert"} {
		want, err := localKeys.ReadFile("keys/" + name)
		require.NoError(t, err)
		path := filepath.Join(dir, name)
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got)
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		require.False(t, os.SameFile(oldFiles[name], info))
	}
	hardlinkData, err := os.ReadFile(hardlink)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), hardlinkData)
}

func TestInstallLocalTestKeysReplacesSymlinkWithoutChangingTarget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Mkdir(dir, 0o700))
	target := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(target, []byte("unchanged"), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "vrf.skey")))

	require.NoError(t, InstallLocalTestKeys(dir))

	targetData, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("unchanged"), targetData)
	want, err := localKeys.ReadFile("keys/vrf.skey")
	require.NoError(t, err)
	got, err := os.ReadFile(filepath.Join(dir, "vrf.skey"))
	require.NoError(t, err)
	require.Equal(t, want, got)
	info, err := os.Lstat(filepath.Join(dir, "vrf.skey"))
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestInstallLocalTestKeysRejectsSymlinkDirectory(t *testing.T) {
	target := t.TempDir()
	dir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Symlink(target, dir))

	err := InstallLocalTestKeys(dir)
	require.ErrorContains(t, err, "is not a directory")
	entries, readErr := os.ReadDir(target)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestInstallLocalTestKeysCleansTemporaryFileOnRenameError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "vrf.skey"), 0o700))

	err := InstallLocalTestKeys(dir)
	require.ErrorContains(t, err, `installing local DevNet key "vrf.skey"`)
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Len(t, entries, 1)
	require.Equal(t, "vrf.skey", entries[0].Name())
	require.True(t, entries[0].IsDir())
}

func TestOpenVerifiedLocalTestKeyRootRejectsDirectoryReplacement(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Mkdir(dir, 0o700))
	expected, err := os.Lstat(dir)
	require.NoError(t, err)
	require.NoError(t, os.Rename(dir, dir+".moved"))
	require.NoError(t, os.Mkdir(dir, 0o700))

	root, err := openVerifiedLocalTestKeyRoot(dir, expected)
	require.ErrorContains(t, err, "changed while it was opened")
	require.Nil(t, root)
}

func TestLocalTestKeyRootStaysBoundAfterPathReplacement(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "keys")
	moved := filepath.Join(base, "keys.moved")
	target := filepath.Join(base, "target")
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.Mkdir(target, 0o700))
	expected, err := os.Lstat(dir)
	require.NoError(t, err)
	root, err := openVerifiedLocalTestKeyRoot(dir, expected)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	require.NoError(t, os.Rename(dir, moved))
	require.NoError(t, os.Symlink(target, dir))
	require.NoError(t, installLocalTestKey(root, "vrf.skey", []byte("key")))

	data, err := os.ReadFile(filepath.Join(moved, "vrf.skey"))
	require.NoError(t, err)
	require.Equal(t, []byte("key"), data)
	entries, err := os.ReadDir(target)
	require.NoError(t, err)
	require.Empty(t, entries)
}
