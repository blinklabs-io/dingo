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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func copyFlatDir(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dst, 0o700))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(dst, entry.Name()), data, 0o600,
		))
	}
}

// writeFormat1Manifest writes m as a format version 1 manifest: no ID, tagged
// with the HMAC and checksum a pre-ID build produced.
func writeFormat1Manifest(
	t *testing.T,
	dir string,
	m lifecycle.Manifest,
	key []byte,
) {
	t.Helper()
	m.FormatVersion = 1
	m.ID = ""
	m.Checksum = ""
	m.Authentication = ""
	data, err := json.Marshal(m)
	require.NoError(t, err)
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	m.Authentication = hex.EncodeToString(mac.Sum(nil))
	data, err = json.Marshal(m)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	m.Checksum = hex.EncodeToString(sum[:])
	data, err = json.MarshalIndent(m, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, lifecycle.ManifestFileName), data, 0o600,
	))
}

func TestManifestSignedID(t *testing.T) {
	t.Parallel()
	key := []byte("operator-trust-root-0123456789ab")

	signedSnapshot := func(t *testing.T, id string) (root string, dir string) {
		t.Helper()
		root = t.TempDir()
		dir = filepath.Join(root, id)
		require.NoError(t, os.Mkdir(dir, 0o700))
		require.NoError(t, lifecycle.WriteManifest(
			dir, lifecycle.Manifest{Network: "preview"},
			lifecycle.WithManifestKey(key),
		))
		return root, dir
	}

	t.Run("manifest records its snapshot ID", func(t *testing.T) {
		t.Parallel()
		_, dir := signedSnapshot(t, "older")
		got, err := lifecycle.ReadManifest(dir, lifecycle.WithManifestKey(key))
		require.NoError(t, err)
		require.Equal(t, "older", got.ID)
		require.Equal(t, 2, got.FormatVersion)
	})

	t.Run("older snapshot copied under a newer ID is rejected", func(t *testing.T) {
		t.Parallel()
		root, older := signedSnapshot(t, "older")
		copyFlatDir(t, older, filepath.Join(root, "newer"))

		_, err := lifecycle.ReadManifest(
			filepath.Join(root, "newer"), lifecycle.WithManifestKey(key),
		)
		require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)

		entries, err := lifecycle.ListSnapshotsContext(
			t.Context(), root, lifecycle.WithManifestKey(key),
		)
		require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
		require.Len(t, entries, 1)
		require.Equal(t, "older", entries[0].ID)
	})

	t.Run("signed ID tampered with a recomputed checksum", func(t *testing.T) {
		t.Parallel()
		_, dir := signedSnapshot(t, "older")
		m, err := lifecycle.ReadManifest(dir)
		require.NoError(t, err)
		forgedDir := filepath.Join(t.TempDir(), "newer")
		require.NoError(t, os.Mkdir(forgedDir, 0o700))
		m.ID = "newer"
		m.Checksum = ""
		data, err := json.Marshal(m)
		require.NoError(t, err)
		sum := sha256.Sum256(data)
		m.Checksum = hex.EncodeToString(sum[:])
		data, err = json.MarshalIndent(m, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(
			filepath.Join(forgedDir, lifecycle.ManifestFileName), data, 0o600,
		))

		// The directory name matches the forged ID, so only the tag can
		// reject it.
		_, err = lifecycle.ReadManifest(forgedDir, lifecycle.WithManifestKey(key))
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
		require.NotErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
	})

	t.Run("format 1 manifest is rejected under a key", func(t *testing.T) {
		t.Parallel()
		dir := filepath.Join(t.TempDir(), "legacy")
		require.NoError(t, os.Mkdir(dir, 0o700))
		writeFormat1Manifest(t, dir, lifecycle.Manifest{Network: "preview"}, key)

		_, err := lifecycle.ReadManifest(dir, lifecycle.WithManifestKey(key))
		require.ErrorIs(t, err, lifecycle.ErrManifestLegacyFormat)
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
		require.ErrorContains(t, err, "must be re-created")

		// Without a key there is nothing to verify against, as before.
		got, err := lifecycle.ReadManifest(dir)
		require.NoError(t, err)
		require.Equal(t, 1, got.FormatVersion)
	})
}

func TestRestoreRejectsSnapshotCopiedUnderNewerID(t *testing.T) {
	t.Parallel()
	key := []byte("operator-trust-root-0123456789ab")
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	root := snapshotTestParent(t)
	older := filepath.Join(root, "older")
	_, err := lifecycle.Snapshot(
		context.Background(), db, older, lifecycle.TriggerManual, "test",
		"badger", "sqlite", lifecycle.WithManifestKey(key),
	)
	require.NoError(t, err)
	newer := filepath.Join(root, "newer")
	copyFlatDir(t, older, newer)

	restore := func(from string) error {
		_, err := lifecycle.Restore(
			context.Background(), newTestStorageHost(t),
			testDestinationRegistry, from,
			filepath.Join(t.TempDir(), "restored"),
			lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
			lifecycle.WithManifestKey(key),
		)
		return err
	}
	require.NoError(t, restore(older))
	require.ErrorIs(t, restore(newer), lifecycle.ErrManifestIDMismatch)
}

func TestCloudRejectsSnapshotCopiedUnderNewerID(t *testing.T) {
	// Not t.Parallel: the fake cloud fixture is process-global.
	uri, objects, _ := cloudSnapshot(t, testTrustKey)
	newerURI := "faketest://bucket/prefix/newer"
	copyFlatDir(t, objects, filepath.Join(filepath.Dir(objects), "newer"))

	_, err := restoreFrom(t, uri, testTrustKey)
	require.NoError(t, err)

	_, err = restoreFrom(t, newerURI, testTrustKey)
	require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)

	_, err = lifecycle.PeekManifest(
		context.Background(), testDestinationRegistry, newerURI,
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)

	_, ok, err := lifecycle.FetchCloudManifest(
		context.Background(), testDestinationRegistry, newerURI,
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.True(t, ok)
	require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)

	m, ok, err := lifecycle.FetchCloudManifest(
		context.Background(), testDestinationRegistry, uri,
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "snap", m.ID)
}
