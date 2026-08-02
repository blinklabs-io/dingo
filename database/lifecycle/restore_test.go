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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// TestSnapshotRestoreRoundTrip verifies that a snapshotted database
// restores into a fresh directory with the same blocks and tip.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, src.BlockCreate(testBlock(2, 0x02), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	snapMan, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	restoreMan, err := lifecycle.Restore(
		context.Background(), newTestStorageHost(t), testDestinationRegistry, snapshotDir, targetDir,
		lifecycle.RestoreStorageConfig{},
	)
	require.NoError(t, err)
	require.Equal(t, snapMan.CommitTimestamp, restoreMan.CommitTimestamp)
	require.Equal(t, snapMan.TipSlot, restoreMan.TipSlot)

	// Reopen the restored data dir like a normal node startup would and
	// confirm the blocks survived the round trip.
	restored, err := dbtest.NewDatabase(t, &database.Config{DataDir: targetDir})
	require.NoError(t, err)

	block1, err := restored.BlockByIndex(1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block1.ID)
	block2, err := restored.BlockByIndex(2, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block2.ID)
}

// TestRestoreRefusesNonEmptyTargetDirectory verifies that Restore errors
// when the target directory already contains a file.
func TestRestoreRefusesNonEmptyTargetDirectory(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	// Target dir already has a file in it.
	targetDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(targetDir, "existing.txt"), []byte("data"), 0o644,
	))

	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), testDestinationRegistry, snapshotDir, targetDir,
		lifecycle.RestoreStorageConfig{},
	)
	require.Error(t, err)
}

// TestRestoreRejectsConfiguredDataDirOverrideWithoutTouchingTarget guards a
// real gap: restore always resolved its blob/metadata plugins with a nil
// provider config, silently ignoring a caller's configured per-plugin
// "dataDir" override (plugin.Selection.Config) -- meaning a restore could
// write into targetDataDir's own staging directory while a real subsequent
// startup, which does honor that override, opens a completely different
// directory and sees the old or empty database there instead. Now that
// storageConfig is propagated, such an override must be refused outright
// (not silently honored either, since doing so would write outside the
// staging directory this package's atomic-rename interruption safety
// depends on) -- and, like every other RestoreValidated rejection, before
// targetDataDir is touched at all.
func TestRestoreRejectsConfiguredDataDirOverrideWithoutTouchingTarget(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), testDestinationRegistry, snapshotDir, targetDir,
		lifecycle.RestoreStorageConfig{
			Blob: map[string]any{"dataDir": "/some/other/configured/path"},
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dataDir override")

	_, statErr := os.Stat(targetDir)
	require.True(
		t, os.IsNotExist(statErr),
		"targetDataDir must be untouched when a dataDir override is rejected",
	)
}

// TestManifestCheckPluginMatch verifies Manifest.CheckPluginMatch itself:
// it accepts the plugins a snapshot was actually taken with, and rejects
// any other combination. This is a unit test of the check in isolation —
// see TestRestoreValidatedRejectsPluginMismatchWithoutTouchingTarget for
// the real call site (internal/dblifecycle.Service.Restore's validate
// hook, via lifecycle.RestoreValidated) that actually enforces it during a
// restore.
func TestManifestCheckPluginMatch(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	m, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)
	require.NoError(t, m.CheckPluginMatch("badger", "sqlite"))
	require.Error(t, m.CheckPluginMatch("gcs", "sqlite"))
}

// TestRestoreValidatedRejectsPluginMismatchWithoutTouchingTarget exercises
// the actual restore call site a plugin mismatch is meant to protect:
// internal/dblifecycle.Service.Restore passes a validate func (calling
// CheckPluginMatch/CheckCompatibility) into lifecycle.RestoreValidated,
// which — per RestoreValidated's own doc comment — must run that check
// before targetDataDir is touched in any way, "not even the empty/absent
// check". Unlike the old, misleadingly-named version of this test (which
// called Manifest.CheckPluginMatch directly and never invoked Restore or
// RestoreValidated at all), this proves the mismatch actually aborts a
// restore attempt, and that it does so before creating targetDir.
func TestRestoreValidatedRejectsPluginMismatchWithoutTouchingTarget(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	_, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.RestoreValidated(
		context.Background(), newTestStorageHost(t), testDestinationRegistry, snapshotDir, targetDir,
		func(m lifecycle.Manifest) error {
			return m.CheckPluginMatch("gcs", "sqlite")
		},
		lifecycle.RestoreStorageConfig{},
	)
	require.Error(t, err)

	_, statErr := os.Stat(targetDir)
	require.Truef(
		t, os.IsNotExist(statErr),
		"a rejected validate hook must run before targetDir is created, "+
			"got stat error: %v", statErr,
	)
}

// TestRestoreRejectsMismatchedTipBlockNumber verifies that
// validateRestoredDatabase's post-restore tip check compares
// TipBlockNumber, not just slot/hash, against the restored database's
// actual tip. The manifest's checksum is recomputed over the tampered
// content (via WriteManifest), so this is not caught as a corrupted file —
// only comparing block number as well as slot/hash catches a restored
// database whose recorded chain height disagrees with its own tip point.
func TestRestoreRejectsMismatchedTipBlockNumber(t *testing.T) {
	src := newTestDB(t)
	require.NoError(t, src.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, src.BlockCreate(testBlock(2, 0x02), nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap1")
	m, err := lifecycle.Snapshot(
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	m.TipBlockNumber++
	require.NoError(t, lifecycle.WriteManifest(snapshotDir, m))

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.Restore(
		context.Background(), newTestStorageHost(t), testDestinationRegistry, snapshotDir, targetDir,
		lifecycle.RestoreStorageConfig{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match manifest tip")
}

// manifestOnlyCloudDestination implements CloudManifestFetcher but fails
// UploadDir/DownloadDir outright -- used to prove a caller went through
// the lightweight FetchManifest path and never attempted a full
// directory download at all, rather than merely happening to succeed
// either way.
type manifestOnlyCloudDestination struct {
	manifest lifecycle.Manifest
}

func (d *manifestOnlyCloudDestination) UploadDir(context.Context, string) error {
	return errors.New("manifestOnlyCloudDestination: UploadDir must never be called")
}

func (d *manifestOnlyCloudDestination) DownloadDir(context.Context, string) error {
	return errors.New("manifestOnlyCloudDestination: DownloadDir must never be called")
}

func (d *manifestOnlyCloudDestination) FetchManifest(context.Context) (lifecycle.Manifest, error) {
	return d.manifest, nil
}

var _ lifecycle.CloudManifestFetcher = &manifestOnlyCloudDestination{}

// Registered directly on the package's shared testDestinationRegistry
// (defined in destination_test.go) — package-level var initializers all
// complete before any init() runs, regardless of which file they're in, so
// referencing it here is safe.
func init() {
	testDestinationRegistry.Register(
		"faketest-manifestonly",
		func(*url.URL) (lifecycle.CloudDestination, error) {
			return &manifestOnlyCloudDestination{manifest: manifestOnlyFixture}, nil
		},
	)
}

var manifestOnlyFixture = lifecycle.Manifest{
	BlobPlugin:     "badger",
	MetadataPlugin: "sqlite",
}

// TestPeekManifestUsesLightweightCloudFetchWithoutDownloading guards
// against a real gap: PeekManifest used to always go
// through the full download-based resolveManifest path, even for a
// cloud snapshotDir whose destination type supports fetching just the
// one manifest.json object via CloudManifestFetcher -- downloading the
// (possibly very large) blob/metadata backups alongside it just to read
// its manifest. This uses a destination whose UploadDir/DownloadDir both
// fail outright, so this test only passes if PeekManifest actually took
// the lightweight FetchCloudManifest path and never called DownloadDir
// at all.
func TestPeekManifestUsesLightweightCloudFetchWithoutDownloading(t *testing.T) {
	m, err := lifecycle.PeekManifest(context.Background(), testDestinationRegistry, "faketest-manifestonly://bucket/prefix")
	require.NoError(t, err)
	require.Equal(t, manifestOnlyFixture, m)
}

// noManifestFetcherCloudDestination forwards to a real fakeCloudDestination
// for UploadDir/DownloadDir but deliberately does not embed it or expose a
// FetchManifest method of its own -- unlike this package's "faketest"
// scheme, whose fakeCloudDestination DOES implement CloudManifestFetcher.
// A test resolving a destination through THIS wrapper's scheme instead
// therefore cannot silently take PeekManifest's lightweight
// FetchCloudManifest path (see TestPeekManifestUsesLightweightCloudFetch
// WithoutDownloading): the type assertion for CloudManifestFetcher
// genuinely fails, forcing the full-download resolveManifest fallback the
// test below claims to cover.
type noManifestFetcherCloudDestination struct {
	inner *fakeCloudDestination
}

func (d *noManifestFetcherCloudDestination) UploadDir(ctx context.Context, localDir string) error {
	return d.inner.UploadDir(ctx, localDir)
}

func (d *noManifestFetcherCloudDestination) DownloadDir(ctx context.Context, localDir string) error {
	return d.inner.DownloadDir(ctx, localDir)
}

var _ lifecycle.CloudDestination = &noManifestFetcherCloudDestination{}

// Registered directly on the package's shared testDestinationRegistry, the
// same way manifestOnlyCloudDestination's scheme is above -- resolved
// under the same fakeCloudDir backing directory as "faketest" itself, so
// setFakeCloudBackingDir still applies.
func init() {
	testDestinationRegistry.Register(
		"faketest-nomanifestfetcher",
		func(uri *url.URL) (lifecycle.CloudDestination, error) {
			fakeCloudMu.Lock()
			base := fakeCloudDir
			fakeCloudMu.Unlock()
			return &noManifestFetcherCloudDestination{
				inner: &fakeCloudDestination{
					dir: filepath.Join(base, strings.TrimPrefix(uri.Path, "/")),
				},
			}, nil
		},
	)
}

// TestPeekManifestFallsBackToDownloadWhenCloudDestinationLacksManifestFetcher
// verifies PeekManifest still works correctly against a cloud destination
// type that does NOT implement CloudManifestFetcher, using
// "faketest-nomanifestfetcher" (a wrapper that deliberately omits
// FetchManifest — see its doc comment) rather than this package's plain
// "faketest" scheme, whose fakeCloudDestination DOES implement
// CloudManifestFetcher and would therefore take the lightweight
// FetchCloudManifest path instead of genuinely exercising this fallback.
func TestPeekManifestFallsBackToDownloadWhenCloudDestinationLacksManifestFetcher(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)

	snapshotDir := filepath.Join(t.TempDir(), "snap-peek")
	m, err := lifecycle.SnapshotToCloud(
		context.Background(), testDestinationRegistry, db, snapshotDir,
		lifecycle.TriggerManual, "test-version", "badger", "sqlite",
		"faketest-nomanifestfetcher://bucket/prefix",
		"", "",
	)
	require.NoError(t, err)

	peeked, err := lifecycle.PeekManifest(
		context.Background(), testDestinationRegistry,
		"faketest-nomanifestfetcher://bucket/prefix/snap-peek",
	)
	require.NoError(t, err)
	require.Equal(t, m.CommitTimestamp, peeked.CommitTimestamp)
}
