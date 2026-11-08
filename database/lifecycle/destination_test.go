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
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// fakeCloudDestination simulates a cloud object store by mirroring
// UploadDir/DownloadFiles onto another local directory, so tests can exercise
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

type unauthenticatedManifestDestination struct {
	entries []lifecycle.SnapshotEntry
}

func (*unauthenticatedManifestDestination) UploadDir(context.Context, string) error {
	return nil
}

func (*unauthenticatedManifestDestination) DownloadFiles(
	context.Context,
	string,
	[]lifecycle.DownloadFile,
) error {
	return nil
}

func (d *unauthenticatedManifestDestination) ListSnapshots(
	context.Context,
	...lifecycle.ManifestOption,
) ([]lifecycle.SnapshotEntry, error) {
	if d.entries != nil {
		return d.entries, nil
	}
	return []lifecycle.SnapshotEntry{{ID: "forged"}}, nil
}

func (*unauthenticatedManifestDestination) FetchManifestWithOptions(
	context.Context,
	...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	return lifecycle.Manifest{}, nil
}

func (d *fakeCloudDestination) UploadDir(
	_ context.Context,
	localDir string,
) error {
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

// DownloadFiles copies only the named files, rejecting one larger than its
// MaxBytes, and records every name it was asked for in fetched.
func (d *fakeCloudDestination) DownloadFiles(
	_ context.Context,
	localDir string,
	files []lifecycle.DownloadFile,
) error {
	fakeCloudMu.Lock()
	fakeCloudFetched = append(fakeCloudFetched, files...)
	fakeCloudMu.Unlock()
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(d.dir, file.Name))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf(
					"%s: %w",
					file.Name,
					lifecycle.ErrCloudSnapshotNotFound,
				)
			}
			return err
		}
		if int64(len(data)) > file.MaxBytes {
			return fmt.Errorf(
				"%s: %w",
				file.Name,
				lifecycle.ErrDownloadTooLarge,
			)
		}
		if err := os.WriteFile(filepath.Join(localDir, file.Name), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// ListSnapshots delegates straight to the real lifecycle.ListSnapshots:
// the fake's "cloud" storage is just an ordinary local directory, so
// scanning it for per-snapshot subdirectories with a valid manifest.json
// is exactly the same operation as the local catalog uses.
func (d *fakeCloudDestination) ListSnapshots(
	_ context.Context,
	opts ...lifecycle.ManifestOption,
) ([]lifecycle.SnapshotEntry, error) {
	return lifecycle.ListSnapshots(d.dir, opts...)
}

func (d *fakeCloudDestination) ListSnapshotCatalog(
	ctx context.Context,
	_ lifecycle.SnapshotCatalogScanBudget,
) ([]lifecycle.SnapshotEntry, error) {
	return lifecycle.ListSnapshotsContext(ctx, d.dir)
}

// FetchManifest/Delete similarly delegate straight to real local-file
// operations, since d.dir already resolves to this specific snapshot's
// own directory when parsed from a per-snapshot URI (base + snapshot ID).
func (d *fakeCloudDestination) FetchManifest(
	_ context.Context,
) (lifecycle.Manifest, error) {
	return lifecycle.ReadManifest(d.dir)
}

func (d *fakeCloudDestination) FetchManifestWithOptions(
	_ context.Context,
	opts ...lifecycle.ManifestOption,
) (lifecycle.Manifest, error) {
	return lifecycle.ReadManifest(d.dir, opts...)
}

func (d *fakeCloudDestination) Delete(_ context.Context) error {
	return os.RemoveAll(d.dir)
}

var (
	_ lifecycle.SnapshotLister                   = &fakeCloudDestination{}
	_ lifecycle.CloudManifestFetcher             = &fakeCloudDestination{}
	_ lifecycle.ConfigurableCloudManifestFetcher = &fakeCloudDestination{}
	_ lifecycle.CloudDeleter                     = &fakeCloudDestination{}
)

// fakeCloudBackingDir is set below to the directory the fake scheme's
// factory should resolve paths under; tests each get their own by
// resetting this under a lock before use, since the registered factory's
// signature carries no per-call test context.
var (
	fakeCloudMu  sync.Mutex
	fakeCloudDir string
	// fakeCloudFetched records every DownloadFile any fake destination was
	// asked for since the current test took the fixture.
	fakeCloudFetched []lifecycle.DownloadFile
	// fakeCloudFixtureMu serializes use of the whole fixture above (not
	// just each individual read/write of fakeCloudDir, which fakeCloudMu
	// already does) across an entire test's lifetime: setFakeCloudBackingDir
	// locks it and only releases via t.Cleanup, once that test is done
	// with the fixture. Without this, two tests using it concurrently
	// (e.g. if either called t.Parallel(), which none currently do, but
	// nothing stops a future test from adding it) could have one test's
	// t.Cleanup reset fakeCloudDir to "" — or set it to a *different*
	// directory once reused for a later test — while the other was still
	// mid-test and relying on its own value still being in effect,
	// silently resolving "faketest://" against the wrong directory
	// instead of either test's own configuration.
	fakeCloudFixtureMu sync.Mutex
)

// testDestinationRegistry is this test package's own instance-owned
// registry (mirroring what composition code builds at startup — see
// DestinationRegistry's doc comment), with the "faketest" scheme
// registered on it once at package load, instead of the package-global
// process registry this used to be built against.
var testDestinationRegistry = func() *lifecycle.DestinationRegistry {
	r := lifecycle.NewDestinationRegistry()
	r.Register(
		"faketest",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			fakeCloudMu.Lock()
			base := fakeCloudDir
			fakeCloudMu.Unlock()
			if base == "" {
				// A resolution with no backing directory set would
				// filepath.Join against "" and write the URI path
				// relative to the package directory. Fail instead: the
				// only way to get here is a resolution that outlived the
				// test that set the fixture, and that should error,
				// not leave files in the checkout.
				return nil, errors.New(
					"faketest: no backing directory set for this test",
				)
			}
			return &fakeCloudDestination{
				dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
			}, nil
		},
	)
	return r
}()

// setFakeCloudBackingDir configures the "faketest://" scheme's backing
// directory for the calling test, and reserves exclusive use of the
// fixture until that test finishes (see fakeCloudFixtureMu) — a second,
// concurrent caller (e.g. a parallel test) blocks here until the first
// one's t.Cleanup releases it, rather than racing it.
func setFakeCloudBackingDir(t *testing.T, dir string) {
	t.Helper()
	fakeCloudFixtureMu.Lock()
	fakeCloudMu.Lock()
	fakeCloudDir = dir
	fakeCloudFetched = nil
	fakeCloudMu.Unlock()
	t.Cleanup(func() {
		fakeCloudMu.Lock()
		fakeCloudDir = ""
		fakeCloudFetched = nil
		fakeCloudMu.Unlock()
		fakeCloudFixtureMu.Unlock()
	})
}

// TestParseCloudDestinationUnknownScheme verifies that a URI whose scheme
// has no registered factory returns an error.
func TestParseCloudDestinationUnknownScheme(t *testing.T) {
	t.Parallel()

	_, err := lifecycle.ParseCloudDestination(
		testDestinationRegistry,
		"s3unknown://bucket/prefix",
	)
	require.Error(t, err)
}

// TestParseCloudDestinationMissingHost verifies that a URI with no host
// (bucket) segment returns an error even for a registered scheme, and that
// the error's example URIs name schemes dingo actually registers: "s3" and
// "gcs" (destination_gcs.go registers "gcs", not "gs" — see its own doc
// comment on why — so an error telling an operator to try "gs://..." would
// send them to a scheme ParseCloudDestination itself rejects).
func TestParseCloudDestinationMissingHost(t *testing.T) {
	t.Parallel()

	_, err := lifecycle.ParseCloudDestination(
		testDestinationRegistry,
		"faketest:///prefix",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "s3://bucket/prefix")
	require.Contains(t, err.Error(), "gcs://bucket/prefix")
	require.NotContains(t, err.Error(), "gs://bucket/prefix")
}

// TestParseCloudDestinationMalformed verifies that an unparseable URI
// string returns an error rather than panicking.
func TestParseCloudDestinationMalformed(t *testing.T) {
	t.Parallel()

	_, err := lifecycle.ParseCloudDestination(
		testDestinationRegistry,
		"not a uri at all ://",
	)
	require.Error(t, err)
}

// TestListCloudSnapshotsEmptyDestReturnsNotOK verifies that an empty
// cloudDest returns ok=false and no error, not a failure.
func TestListCloudSnapshotsEmptyDestReturnsNotOK(t *testing.T) {
	t.Parallel()

	entries, ok, err := lifecycle.ListCloudSnapshots(
		context.Background(),
		nil,
		"",
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, entries)
}

// TestListCloudSnapshotsInvalidDestReturnsError verifies that an
// unsupported cloud scheme returns a real error, not ok=false.
func TestListCloudSnapshotsInvalidDestReturnsError(t *testing.T) {
	t.Parallel()

	_, ok, err := lifecycle.ListCloudSnapshots(
		context.Background(),
		testDestinationRegistry,
		"unsupported-scheme://bucket/prefix",
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.Error(t, err)
	require.False(t, ok)
}

func TestCloudManifestHelpersAuthenticateDestinationResults(t *testing.T) {
	t.Parallel()
	manifestDir := filepath.Join(t.TempDir(), "trusted")
	require.NoError(t, os.Mkdir(manifestDir, 0o700))
	require.NoError(t, lifecycle.WriteManifest(
		manifestDir,
		lifecycle.Manifest{Network: "preview"},
		lifecycle.WithManifestKey(testTrustKey),
	))
	validManifest, err := lifecycle.ReadManifest(
		manifestDir, lifecycle.WithManifestKey(testTrustKey),
	)
	require.NoError(t, err)
	destination := &unauthenticatedManifestDestination{
		entries: []lifecycle.SnapshotEntry{
			{ID: "forged"},
			{ID: "trusted", Manifest: validManifest},
		},
	}
	registry := lifecycle.NewDestinationRegistry()
	registry.Register(
		"unauthenticated",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return destination, nil
		},
	)
	opts := []lifecycle.ManifestOption{
		lifecycle.WithManifestKey(testTrustKey),
	}

	entries, ok, err := lifecycle.ListCloudSnapshots(
		context.Background(), registry, "unauthenticated://bucket", opts...,
	)
	require.True(t, ok)
	require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
	require.Len(t, entries, 1)
	require.Equal(t, []string{"trusted"}, []string{entries[0].ID})

	_, ok, err = lifecycle.FetchCloudManifest(
		context.Background(), registry, "unauthenticated://bucket/snap", opts...,
	)
	require.True(t, ok)
	require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
}

// SnapshotToCloud only reads the source database, so the local-only,
// rejected and uploaded cases, and the restore and listing that follow an
// upload, all share one.
func TestSnapshotToCloud(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	snapshotToCloud := func(dir, dest string, opts ...lifecycle.ManifestOption) (lifecycle.Manifest, error) {
		return lifecycle.SnapshotToCloud(
			context.Background(), testDestinationRegistry, db, dir,
			lifecycle.TriggerManual, "test-version", "badger", "sqlite",
			dest, "", "", opts...,
		)
	}

	t.Run("empty destination is local only", func(t *testing.T) {
		dir := snapshotTestDir(t, "snap-local-only")
		m, err := snapshotToCloud(dir, "")
		require.NoError(t, err)
		require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
		require.FileExists(t, filepath.Join(dir, lifecycle.MetadataBackupFileName))
		require.NotZero(t, m.BlobBytes)
	})

	t.Run("cloud upload requires a trust key", func(t *testing.T) {
		setFakeCloudBackingDir(t, t.TempDir())
		dir := snapshotTestDir(t, "snap-cloud")
		_, err := snapshotToCloud(dir, "faketest://bucket/prefix")
		require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
		require.NoDirExists(t, dir)
	})

	t.Run("invalid destination errors but keeps the local snapshot", func(t *testing.T) {
		dir := snapshotTestDir(t, "snap-bad-cloud")
		_, err := snapshotToCloud(
			dir, "unsupported-scheme://bucket/prefix",
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.Error(t, err)
		require.FileExists(t, filepath.Join(dir, lifecycle.BlobBackupFileName))
	})

	t.Run("uploads under a per-snapshot sub path", func(t *testing.T) {
		backingDir := t.TempDir()
		setFakeCloudBackingDir(t, backingDir)
		dir := snapshotTestDir(t, "snap-cloud")
		_, err := snapshotToCloud(
			dir, "faketest://bucket/prefix",
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.NoError(t, err)
		for _, name := range []string{
			lifecycle.BlobBackupFileName,
			lifecycle.MetadataBackupFileName,
			lifecycle.ManifestFileName,
		} {
			require.FileExists(t, filepath.Join(dir, name))
			require.FileExists(
				t, filepath.Join(backingDir, "prefix", "snap-cloud", name),
			)
		}
		require.NoFileExists(
			t, filepath.Join(backingDir, "prefix", lifecycle.ManifestFileName),
		)
	})

	t.Run("restore accepts a cloud URI", func(t *testing.T) {
		setFakeCloudBackingDir(t, t.TempDir())
		_, err := snapshotToCloud(
			snapshotTestDir(t, "snap-src"), "faketest://bucket/prefix",
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.NoError(t, err)
		m, err := lifecycle.Restore(
			context.Background(), newTestStorageHost(t), testDestinationRegistry,
			"faketest://bucket/prefix/snap-src",
			filepath.Join(t.TempDir(), "restored"),
			lifecycle.RestoreStorageConfig{Blob: testutil.BadgerBlobConfig()},
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.NoError(t, err)
		require.Equal(t, "badger", m.BlobPlugin)
	})

	t.Run("listing returns every uploaded snapshot", func(t *testing.T) {
		setFakeCloudBackingDir(t, t.TempDir())
		const cloudDest = "faketest://bucket/prefix"
		for _, name := range []string{"snap-a", "snap-b"} {
			_, err := snapshotToCloud(
				snapshotTestDir(t, name), cloudDest,
				lifecycle.WithManifestKey(testTrustKey),
			)
			require.NoError(t, err)
		}
		entries, ok, err := lifecycle.ListCloudSnapshots(
			context.Background(), testDestinationRegistry, cloudDest,
			lifecycle.WithManifestKey(testTrustKey),
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, entries, 2)
		require.ElementsMatch(
			t, []string{"snap-a", "snap-b"},
			[]string{entries[0].ID, entries[1].ID},
		)
	})
}
