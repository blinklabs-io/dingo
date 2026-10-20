//go:build unix

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

package dblifecycle

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/keystore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestOptionsRejectsUnsafeTrustKeyPaths(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		target := filepath.Join(dir, "target.key")
		path := filepath.Join(dir, "trust.key")
		require.NoError(t, os.WriteFile(target, []byte("0123456789abcdef"), 0o600))
		require.NoError(t, os.Symlink(target, path))

		_, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: path,
		})
		require.ErrorIs(t, err, keystore.ErrNotRegularFile)
	})

	t.Run("group-readable mode", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "trust.key")
		require.NoError(t, os.WriteFile(path, []byte("0123456789abcdef"), 0o600))
		require.NoError(t, os.Chmod(path, 0o640))

		_, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: path,
		})
		require.Error(t, err)
	})

	t.Run("foreign owner", func(t *testing.T) {
		t.Parallel()
		if os.Geteuid() != 0 {
			t.Skip("changing file ownership requires root")
		}
		path := filepath.Join(t.TempDir(), "trust.key")
		require.NoError(t, os.WriteFile(path, []byte("0123456789abcdef"), 0o600))
		require.NoError(t, os.Chown(path, 1, -1))

		_, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: path,
		})
		require.ErrorIs(t, err, keystore.ErrInsecureFileOwner)
	})

	t.Run("device", func(t *testing.T) {
		t.Parallel()
		_, err := ManifestOptions(config.DatabaseLifecycleConfig{
			SnapshotTrustKeyFile: "/dev/null",
		})
		require.ErrorIs(t, err, keystore.ErrNotRegularFile)
	})

	t.Run("fifo without writer", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "trust.fifo")
		require.NoError(t, syscall.Mkfifo(path, 0o600))

		result := make(chan error, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := ManifestOptions(config.DatabaseLifecycleConfig{
				SnapshotTrustKeyFile: path,
			})
			result <- err
		}()
		t.Cleanup(func() {
			select {
			case <-done:
				return
			default:
			}
			writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
			if err == nil {
				_ = writer.Close()
			}
			testutil.RequireReceive(t, done, testutil.AsyncWait, "blocked FIFO loader cleanup")
		})

		err := testutil.RequireReceive(
			t, result, testutil.AsyncWait, "nonblocking FIFO rejection",
		)
		require.Error(t, err)
	})
}

func TestSnapshotTrustKeyReadKeepsValidatedHandleAcrossReplacement(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "trust.key")
	original := []byte("0123456789abcdef")
	replacement := []byte("fedcba9876543210")
	require.NoError(t, os.WriteFile(path, original, 0o600))

	f, err := keystore.OpenRegularFileNoFollow(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	require.NoError(t, keystore.CheckOpenFileOwnerAndPermissions(f))
	require.NoError(t, os.Rename(path, filepath.Join(dir, "original.key")))
	require.NoError(t, os.WriteFile(path, replacement, 0o600))

	raw, err := readOpenSnapshotTrustKeyFile(f, path)
	require.NoError(t, err)
	assert.Equal(t, original, raw)
}
