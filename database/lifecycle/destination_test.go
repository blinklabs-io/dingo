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
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// fakeCloudDestination simulates a cloud object store by mirroring
// UploadDir/DownloadDir onto another local directory, so tests can exercise
// the registry/parsing/round-trip/listing logic in database/lifecycle
// without any real network calls or cloud SDKs (those are exercised only
// by the build-tag-gated destination_s3.go/destination_gcs.go, which need
// real credentials to test against a live bucket).
//
// Unlike a flat single-directory fake, this resolves each parsed URI's
// path to its own subdirectory under the shared backing directory — the
// same way a real S3/GCS destination's bucket+key would differ between
// "faketest://bucket/prefix" (a base destination) and
// "faketest://bucket/prefix/<snapshotID>" (one specific snapshot under
// it) — so tests actually exercise the nested-per-snapshot layout
// SnapshotToCloud/ListCloudSnapshots depend on, rather than accidentally
// passing regardless of it.
type fakeCloudDestination struct {
	dir string
}

func (d *fakeCloudDestination) UploadDir(_ context.Context, localDir string) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(localDir, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(d.dir, entry.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

func (d *fakeCloudDestination) DownloadDir(_ context.Context, localDir string) error {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(d.dir, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(localDir, entry.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// ListSnapshots delegates straight to the real lifecycle.ListSnapshots:
// the fake's "cloud" storage is just an ordinary local directory, so
// scanning it for per-snapshot subdirectories with a valid manifest.json
// is exactly the same operation as the local catalog uses.
func (d *fakeCloudDestination) ListSnapshots(_ context.Context) ([]lifecycle.SnapshotEntry, error) {
	return lifecycle.ListSnapshots(d.dir)
}

// FetchManifest/Delete similarly delegate straight to real local-file
// operations, since d.dir already resolves to this specific snapshot's
// own directory when parsed from a per-snapshot URI (base + snapshot ID).
func (d *fakeCloudDestination) FetchManifest(_ context.Context) (lifecycle.Manifest, error) {
	return lifecycle.ReadManifest(d.dir)
}

func (d *fakeCloudDestination) Delete(_ context.Context) error {
	return os.RemoveAll(d.dir)
}

var (
	_ lifecycle.SnapshotLister       = &fakeCloudDestination{}
	_ lifecycle.CloudManifestFetcher = &fakeCloudDestination{}
	_ lifecycle.CloudDeleter         = &fakeCloudDestination{}
)

// fakeCloudBackingDir is set by TestMain-less package init below to the
// directory the fake scheme's factory should resolve paths under; tests
// each get their own by resetting this under a lock before use, since
// RegisterCloudDestinationScheme's factory signature carries no per-call
// test context.
var (
	fakeCloudMu  sync.Mutex
	fakeCloudDir string
)

func init() {
	lifecycle.RegisterCloudDestinationScheme(
		"faketest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			fakeCloudMu.Lock()
			base := fakeCloudDir
			fakeCloudMu.Unlock()
			return &fakeCloudDestination{
				dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
			}, nil
		},
	)
}

func setFakeCloudBackingDir(t *testing.T, dir string) {
	t.Helper()
	fakeCloudMu.Lock()
	fakeCloudDir = dir
	fakeCloudMu.Unlock()
	t.Cleanup(func() {
		fakeCloudMu.Lock()
		fakeCloudDir = ""
		fakeCloudMu.Unlock()
	})
}

// TestFakeCloudBackingDirResetsBetweenTests guards against a leaked global:
// setFakeCloudBackingDir used to set fakeCloudDir with no corresponding
// reset, so whichever test happened to set it last left that directory in
// place for every subsequent test in the package — a test that forgot to
// call setFakeCloudBackingDir (or was reordered/shuffled ahead of the one
// that used to set it up) could silently resolve the "faketest://" scheme
// against a leftover directory from an unrelated test instead of failing
// loudly. The subtest's t.Cleanup (registered by setFakeCloudBackingDir)
// runs synchronously before t.Run returns, so fakeCloudDir must already be
// reset by the time this checks it.
func TestFakeCloudBackingDirResetsBetweenTests(t *testing.T) {
	t.Run("sets it", func(t *testing.T) {
		setFakeCloudBackingDir(t, t.TempDir())
	})

	fakeCloudMu.Lock()
	got := fakeCloudDir
	fakeCloudMu.Unlock()
	require.Empty(
		t, got,
		"fakeCloudDir must be reset via t.Cleanup once the test that set it finishes",
	)
}

// TestParseCloudDestinationUnknownScheme verifies that a URI whose scheme
// has no registered factory returns an error.
func TestParseCloudDestinationUnknownScheme(t *testing.T) {
	_, err := lifecycle.ParseCloudDestination("s3unknown://bucket/prefix")
	require.Error(t, err)
}

// TestParseCloudDestinationMissingHost verifies that a URI with no host
// (bucket) segment returns an error even for a registered scheme, and that
// the error's example URIs name schemes dingo actually registers: "s3" and
// "gcs" (destination_gcs.go registers "gcs", not "gs" — see its own doc
// comment on why — so an error telling an operator to try "gs://..." would
// send them to a scheme ParseCloudDestination itself rejects).
func TestParseCloudDestinationMissingHost(t *testing.T) {
	_, err := lifecycle.ParseCloudDestination("faketest:///prefix")
	require.Error(t, err)
	require.Contains(t, err.Error(), "s3://bucket/prefix")
	require.Contains(t, err.Error(), "gcs://bucket/prefix")
	require.NotContains(t, err.Error(), "gs://bucket/prefix")
}

// TestParseCloudDestinationMalformed verifies that an unparseable URI
// string returns an error rather than panicking.
func TestParseCloudDestinationMalformed(t *testing.T) {
	_, err := lifecycle.ParseCloudDestination("not a uri at all ://")
	require.Error(t, err)
}

// TestParseCloudDestinationRegisteredScheme verifies that a valid URI for
// a registered scheme resolves to a usable CloudDestination.
func TestParseCloudDestinationRegisteredScheme(t *testing.T) {
	setFakeCloudBackingDir(t, t.TempDir())
	dest, err := lifecycle.ParseCloudDestination("faketest://bucket/prefix")
	require.NoError(t, err)
	require.NotNil(t, dest)
}

// TestSnapshotToCloudEmptyDestinationIsLocalOnly verifies that an empty
// cloudDest skips the upload and only writes the local snapshot.
func TestSnapshotToCloudEmptyDestinationIsLocalOnly(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	dir := filepath.Join(t.TempDir(), "snap-local-only")
	m, err := lifecycle.SnapshotToCloud(
		context.Background(), db, dir, lifecycle.TriggerManual, "test-version", "",
	)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
	require.FileExists(t, filepath.Join(dir, lifecycle.MetadataBackupFileName))
	require.NotZero(t, m.BlobBytes)
}

// TestSnapshotToCloudUploadsUnderPerSnapshotSubPath verifies that the
// cloud copy lands under cloudDest/<snapshotID>, keeping the local copy too.
func TestSnapshotToCloudUploadsUnderPerSnapshotSubPath(t *testing.T) {
	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	dir := filepath.Join(t.TempDir(), "snap-cloud")
	_, err := lifecycle.SnapshotToCloud(
		context.Background(),
		db,
		dir,
		lifecycle.TriggerManual,
		"test-version",
		"faketest://bucket/prefix",
	)
	require.NoError(t, err)

	// Local copy must still exist — cloud is a mirror, not a replacement.
	require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
	require.FileExists(t, filepath.Join(dir, lifecycle.MetadataBackupFileName))
	require.FileExists(t, filepath.Join(dir, lifecycle.ManifestFileName))

	// The cloud copy must land under prefix/<snapshotID> (snapshotID =
	// filepath.Base(dir) = "snap-cloud"), not flat under prefix — flat
	// would silently collide with every other snapshot ever uploaded to
	// the same configured destination.
	snapshotDir := filepath.Join(backingDir, "prefix", "snap-cloud")
	require.FileExists(t, filepath.Join(snapshotDir, lifecycle.BlobBackupFileName))
	require.FileExists(t, filepath.Join(snapshotDir, lifecycle.MetadataBackupFileName))
	require.FileExists(t, filepath.Join(snapshotDir, lifecycle.ManifestFileName))

	// And nothing must have landed flat directly under prefix/.
	require.NoFileExists(t, filepath.Join(backingDir, "prefix", lifecycle.ManifestFileName))
}

// TestSnapshotToCloudInvalidDestinationStillErrorsButKeepsLocal verifies
// that an unsupported cloud scheme reports an error but the local copy survives.
func TestSnapshotToCloudInvalidDestinationStillErrorsButKeepsLocal(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	dir := filepath.Join(t.TempDir(), "snap-bad-cloud")
	_, err := lifecycle.SnapshotToCloud(
		context.Background(),
		db,
		dir,
		lifecycle.TriggerManual,
		"test-version",
		"unsupported-scheme://bucket/prefix",
	)
	require.Error(t, err)
	// Local snapshot must still be valid even though the operation as a
	// whole reports an error (operator asked for both copies).
	require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
}

// TestRestoreAcceptsCloudURI verifies that Restore can take a per-snapshot
// cloud URI directly, downloading it before restoring as normal.
func TestRestoreAcceptsCloudURI(t *testing.T) {
	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	localDir := filepath.Join(t.TempDir(), "snap-src")
	_, err := lifecycle.SnapshotToCloud(
		context.Background(),
		db,
		localDir,
		lifecycle.TriggerManual,
		"test-version",
		"faketest://bucket/prefix",
	)
	require.NoError(t, err)

	// Restoring from the cloud requires the full per-snapshot URI (base
	// destination + snapshot ID), the same one SnapshotToCloud actually
	// uploaded to — the bare base destination is a catalog of possibly
	// many snapshots, not one restorable directory.
	restoredDir := filepath.Join(t.TempDir(), "restored")
	m, err := lifecycle.Restore(
		context.Background(), "faketest://bucket/prefix/snap-src", restoredDir,
	)
	require.NoError(t, err)
	require.Equal(t, "badger", m.BlobPlugin)
}

// TestListCloudSnapshotsReturnsEveryUploadedSnapshot verifies that every
// snapshot previously uploaded to a cloud destination is listed back.
func TestListCloudSnapshotsReturnsEveryUploadedSnapshot(t *testing.T) {
	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)
	const cloudDest = "faketest://bucket/prefix"

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	for _, name := range []string{"snap-a", "snap-b"} {
		_, err := lifecycle.SnapshotToCloud(
			context.Background(),
			db,
			filepath.Join(t.TempDir(), name),
			lifecycle.TriggerManual,
			"test-version",
			cloudDest,
		)
		require.NoError(t, err)
	}

	entries, ok, err := lifecycle.ListCloudSnapshots(context.Background(), cloudDest)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, entries, 2)
	ids := []string{entries[0].ID, entries[1].ID}
	require.ElementsMatch(t, []string{"snap-a", "snap-b"}, ids)
}

// TestListCloudSnapshotsEmptyDestReturnsNotOK verifies that an empty
// cloudDest returns ok=false and no error, not a failure.
func TestListCloudSnapshotsEmptyDestReturnsNotOK(t *testing.T) {
	entries, ok, err := lifecycle.ListCloudSnapshots(context.Background(), "")
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, entries)
}

// TestListCloudSnapshotsInvalidDestReturnsError verifies that an
// unsupported cloud scheme returns a real error, not ok=false.
func TestListCloudSnapshotsInvalidDestReturnsError(t *testing.T) {
	_, ok, err := lifecycle.ListCloudSnapshots(
		context.Background(), "unsupported-scheme://bucket/prefix",
	)
	require.Error(t, err)
	require.False(t, ok)
}
