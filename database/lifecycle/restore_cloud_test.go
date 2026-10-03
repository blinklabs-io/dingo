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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

var testTrustKey = []byte("operator-trust-root-0123456789ab")

// cloudSnapshot snapshots a small database to the fake cloud under a fresh
// backing directory and returns the snapshot URI, the object directory the
// fake serves it from, and the manifest. key, when set, authenticates it.
func cloudSnapshot(
	t *testing.T,
	key []byte,
) (uri string, objects string, manifest lifecycle.Manifest) {
	t.Helper()
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, src.BlockCreate(testBlock(2, 0x02), nil))

	backing := t.TempDir()
	setFakeCloudBackingDir(t, backing)
	var opts []lifecycle.ManifestOption
	if key != nil {
		opts = append(opts, lifecycle.WithManifestKey(key))
	}
	snapshotDir := filepath.Join(t.TempDir(), "snap")
	m, err := lifecycle.SnapshotToCloud(
		context.Background(), testDestinationRegistry, src, snapshotDir,
		lifecycle.TriggerManual, "test", "badger", "sqlite",
		"faketest://bucket/prefix", "", "", opts...,
	)
	require.NoError(t, err)
	return "faketest://bucket/prefix/snap",
		filepath.Join(backing, "prefix", "snap"), m
}

func restoreFrom(
	t *testing.T,
	uri string,
	key []byte,
) (string, error) {
	t.Helper()
	var opts []lifecycle.ManifestOption
	if key != nil {
		opts = append(opts, lifecycle.WithManifestKey(key))
	}
	target := filepath.Join(t.TempDir(), "restored")
	_, err := lifecycle.Restore(
		context.Background(), newTestStorageHost(t), testDestinationRegistry,
		uri, target,
		lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
		opts...,
	)
	return target, err
}

// A restore fetches the manifest, then only the two backup files it declares,
// each bounded by the size the manifest declares; other objects under the
// prefix are never requested.
func TestRestoreFromCloudFetchesOnlyDeclaredObjects(t *testing.T) {
	// Not t.Parallel: the fake cloud fixture is process-global.
	uri, objects, m := cloudSnapshot(t, nil)
	require.NoError(t, os.WriteFile(
		filepath.Join(objects, "extra.bin"), make([]byte, 1<<20), 0o600,
	))

	_, err := restoreFrom(t, uri, nil)
	require.NoError(t, err)

	fakeCloudMu.Lock()
	fetched := append([]lifecycle.DownloadFile(nil), fakeCloudFetched...)
	fakeCloudMu.Unlock()
	var names []string
	sizes := map[string]int64{}
	for _, f := range fetched {
		names = append(names, f.Name)
		sizes[f.Name] = f.MaxBytes
	}
	require.Equal(t, []string{
		lifecycle.ManifestFileName,
		lifecycle.BlobBackupFileName,
		lifecycle.MetadataBackupFileName,
	}, names)
	require.Equal(t, m.BlobBytes, sizes[lifecycle.BlobBackupFileName])
	require.Equal(t, m.MetadataBytes, sizes[lifecycle.MetadataBackupFileName])
}

// A payload edited in place, or cut short, is refused before the target is
// touched, whether or not a trust key is configured.
func TestRestoreRejectsTamperedOrTruncatedPayload(t *testing.T) {
	// Not t.Parallel: the fake cloud fixture is process-global.
	for _, tc := range []struct {
		name   string
		file   string
		mutate func([]byte) []byte
		want   string
	}{
		{
			name: "tampered metadata",
			file: lifecycle.MetadataBackupFileName,
			mutate: func(b []byte) []byte {
				b[len(b)/2] ^= 0xFF
				return b
			},
			want: "digest",
		},
		{
			name:   "truncated blob",
			file:   lifecycle.BlobBackupFileName,
			mutate: func(b []byte) []byte { return b[:len(b)-1] },
			want:   "bytes, manifest declares",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uri, objects, _ := cloudSnapshot(t, nil)
			path := filepath.Join(objects, tc.file)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, tc.mutate(data), 0o600))

			target, err := restoreFrom(t, uri, nil)
			require.ErrorIs(t, err, lifecycle.ErrSnapshotPayloadMismatch)
			require.ErrorContains(t, err, tc.want)
			require.NoDirExists(t, target)
		})
	}
}

// Replacing a payload and rewriting the manifest to match passes the unkeyed
// checksum and the digests, so only the trust key refuses it.
func TestRestoreRejectsManifestReplacementUnderTrustKey(t *testing.T) {
	// Not t.Parallel: the fake cloud fixture is process-global.
	uri, objects, m := cloudSnapshot(t, testTrustKey)

	metadata := filepath.Join(objects, lifecycle.MetadataBackupFileName)
	forged := []byte("forged sql payload")
	require.NoError(t, os.WriteFile(metadata, forged, 0o600))
	m.MetadataBytes = int64(len(forged))
	m.MetadataSHA256 = sha256Hex(forged)
	// An attacker without the key can only write an unauthenticated manifest.
	require.NoError(t, lifecycle.WriteManifest(objects, m))

	target, err := restoreFrom(t, uri, testTrustKey)
	require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
	require.NoDirExists(t, target)
}

// A genuine snapshot restores under the key it was authenticated with, and a
// different key refuses it.
func TestRestoreFromCloudChecksTrustKey(t *testing.T) {
	// Not t.Parallel: the fake cloud fixture is process-global.
	uri, _, _ := cloudSnapshot(t, testTrustKey)

	_, err := restoreFrom(t, uri, []byte("some-other-trust-root-0123456789"))
	require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)

	_, err = restoreFrom(t, uri, testTrustKey)
	require.NoError(t, err)
}
