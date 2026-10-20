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
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// signedManifestFor writes a keyed manifest into a directory named id and
// returns it as read back.
func signedManifestFor(
	t *testing.T,
	id string,
	created int64,
) lifecycle.Manifest {
	t.Helper()
	dir := filepath.Join(t.TempDir(), id)
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, lifecycle.WriteManifest(dir, lifecycle.Manifest{
		CreatedAt: time.Unix(created, 0).UTC(),
		Trigger:   lifecycle.TriggerManual,
	}, lifecycle.WithManifestKey(testTrustKey)))
	m, err := lifecycle.ReadManifest(
		dir,
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.NoError(t, err)
	return m
}

// A cloud URI whose path the provider cleans before building object keys
// must be checked against the prefix those keys actually read.
func TestCloudIDCheckUsesCleanedObjectPrefix(t *testing.T) {
	t.Parallel()
	_, objects, _ := cloudSnapshot(t, testTrustKey)
	copyFlatDir(t, objects, filepath.Join(filepath.Dir(objects), "newer"))

	for _, uri := range []string{
		"faketest://bucket/prefix/newer/.",
		"faketest://bucket/prefix/newer/",
		"faketest://bucket/prefix/newer/x/..",
	} {
		_, err := restoreFrom(t, uri, testTrustKey)
		require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch, uri)

		_, err = lifecycle.PeekManifest(
			context.Background(), testDestinationRegistry, uri,
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch, uri)
	}
}

// replayedManifestDestination serves one manifest at every path and, like
// the S3 and GCS fetchers, authenticates it with only the options it is
// given, so the expected snapshot ID comes from FetchCloudManifest alone.
type replayedManifestDestination struct {
	manifest lifecycle.Manifest
}

func (*replayedManifestDestination) UploadDir(context.Context, string) error {
	return nil
}

func (*replayedManifestDestination) DownloadFiles(
	context.Context,
	string,
	[]lifecycle.DownloadFile,
) error {
	return nil
}

func (d *replayedManifestDestination) FetchManifestWithOptions(
	_ context.Context,
	opts ...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	return d.manifest, d.manifest.Authenticate(opts...)
}

func TestFetchCloudManifestBindsCanonicalSnapshotID(t *testing.T) {
	t.Parallel()
	older := signedManifestFor(t, "older", 1)
	registry := lifecycle.NewDestinationRegistry()
	registry.Register(
		"replayedfetch",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return &replayedManifestDestination{manifest: older}, nil
		},
	)
	fetch := func(uri string) error {
		_, ok, err := lifecycle.FetchCloudManifest(
			context.Background(), registry, uri,
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.True(t, ok, uri)
		return err
	}

	require.NoError(t, fetch("replayedfetch://bucket/prefix/older"))
	require.NoError(t, fetch("replayedfetch://bucket/prefix/older/"))
	// The ID is the segment the destination reads, not the raw last one.
	require.NoError(t, fetch("replayedfetch://bucket/prefix/older/x/.."))
	for _, uri := range []string{
		// The destination reads the cleaned path, prefix/newer.
		"replayedfetch://bucket/prefix/newer/.",
		"replayedfetch://bucket/prefix/newer/x/..",
		// Nothing is written at the bucket root, so it names no snapshot.
		"replayedfetch://bucket",
		"replayedfetch://bucket/",
		"replayedfetch://bucket/.",
	} {
		err := fetch(uri)
		require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch, uri)
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated, uri)
	}
}

func TestListCloudSnapshotsOmitsEntryUnderAnotherID(t *testing.T) {
	t.Parallel()
	older := signedManifestFor(t, "older", 1)
	destination := &unauthenticatedManifestDestination{
		entries: []lifecycle.SnapshotEntry{
			{ID: "newer", Manifest: older},
			{ID: "older", Manifest: older},
		},
	}
	registry := lifecycle.NewDestinationRegistry()
	registry.Register(
		"replayedlist",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return destination, nil
		},
	)

	entries, ok, err := lifecycle.ListCloudSnapshots(
		context.Background(), registry, "replayedlist://bucket",
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.True(t, ok)
	require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
	require.Len(t, entries, 1)
	require.Equal(t, "older", entries[0].ID)
}

func TestCatalogCloudRowUnderAnotherIDIsReported(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	require.NoError(t, lifecycle.EnsureSnapshotCatalog(
		base, lifecycle.WithManifestKey(testTrustKey),
	))
	older := signedManifestFor(t, "older", 1)
	// The rebuild trusts its caller to have authenticated the entries, so
	// it stands in for a catalog file altered on disk.
	require.NoError(t, lifecycle.RebuildCloudSnapshotCatalogContext(
		t.Context(), base, "s3://bucket/snapshots", []lifecycle.SnapshotEntry{
			{ID: "newer", Manifest: older},
			{ID: "older", Manifest: older},
		},
	))

	entries, _, err := lifecycle.ListAvailableSnapshotPageContext(
		t.Context(), base, "s3://bucket/snapshots", 10, nil,
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.ErrorIs(t, err, lifecycle.ErrManifestIDMismatch)
	require.Len(t, entries, 1)
	require.Equal(t, "older", entries[0].Entry.ID)
}

func TestMirrorToCloudRefusesUnboundSnapshot(t *testing.T) {
	t.Parallel()
	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)
	root := t.TempDir()
	older := filepath.Join(root, "older")
	require.NoError(t, os.Mkdir(older, 0o700))
	require.NoError(t, lifecycle.WriteManifest(
		older, lifecycle.Manifest{Network: "preview"},
		lifecycle.WithManifestKey(testTrustKey),
	))
	newer := filepath.Join(root, "newer")
	copyFlatDir(t, older, newer)
	legacy := filepath.Join(root, "legacy")
	require.NoError(t, os.Mkdir(legacy, 0o700))
	writeFormat1Manifest(
		t, legacy, lifecycle.Manifest{Network: "preview"}, testTrustKey,
	)

	for dir, want := range map[string]error{
		newer:  lifecycle.ErrManifestIDMismatch,
		legacy: lifecycle.ErrManifestLegacyFormat,
	} {
		err := lifecycle.MirrorToCloud(
			context.Background(), testDestinationRegistry, dir,
			"faketest://bucket/prefix",
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.ErrorIs(t, err, want, dir)
		require.NoDirExists(
			t, filepath.Join(backingDir, "prefix", filepath.Base(dir)),
		)
	}
}
