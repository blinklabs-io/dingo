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
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestManifestAuthentication(t *testing.T) {
	t.Parallel()
	key := []byte("operator-trust-root-0123456789ab")
	other := []byte("some-other-trust-root-0123456789")

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, lifecycle.WriteManifest(
			dir, lifecycle.Manifest{Network: "preview"},
			lifecycle.WithManifestKey(key),
		))
		got, err := lifecycle.ReadManifest(dir, lifecycle.WithManifestKey(key))
		require.NoError(t, err)
		require.Equal(t, "preview", got.Network)
		require.NotEmpty(t, got.Authentication)
		// Reading without a key is not an authentication failure.
		_, err = lifecycle.ReadManifest(dir)
		require.NoError(t, err)
	})
	t.Run("wrong key", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, lifecycle.WriteManifest(
			dir, lifecycle.Manifest{Network: "preview"},
			lifecycle.WithManifestKey(key),
		))
		_, err := lifecycle.ReadManifest(dir, lifecycle.WithManifestKey(other))
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
	})
	t.Run("unauthenticated manifest", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, lifecycle.WriteManifest(
			dir, lifecycle.Manifest{Network: "preview"},
		))
		_, err := lifecycle.ReadManifest(dir, lifecycle.WithManifestKey(key))
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
	})
	t.Run("edited fields with a recomputed checksum", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		require.NoError(t, lifecycle.WriteManifest(
			dir, lifecycle.Manifest{Network: "preview"},
			lifecycle.WithManifestKey(key),
		))
		m, err := lifecycle.ReadManifest(dir)
		require.NoError(t, err)
		m.MetadataSHA256 = sha256Hex([]byte("forged"))
		// The checksum is unkeyed, so rewriting it needs no secret; the tag
		// must not survive the edit.
		forged := t.TempDir()
		require.NoError(t, lifecycle.WriteManifest(forged, m))
		_, err = lifecycle.ReadManifest(forged, lifecycle.WithManifestKey(key))
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
	})
}

func TestSnapshotRecordsPayloadDigests(t *testing.T) {
	t.Parallel()
	key := []byte("operator-trust-root-0123456789ab")

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dir := filepath.Join(t.TempDir(), "snap")
	m, err := lifecycle.Snapshot(
		context.Background(), db, dir, lifecycle.TriggerManual, "test",
		"badger", "sqlite", lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)

	blob, err := os.ReadFile(filepath.Join(dir, lifecycle.BlobBackupFileName))
	require.NoError(t, err)
	metadata, err := os.ReadFile(filepath.Join(dir, lifecycle.MetadataBackupFileName))
	require.NoError(t, err)
	require.Equal(t, sha256Hex(blob), m.BlobSHA256)
	require.Equal(t, sha256Hex(metadata), m.MetadataSHA256)
	require.NoError(t, m.Authenticate(lifecycle.WithManifestKey(key)))
}
