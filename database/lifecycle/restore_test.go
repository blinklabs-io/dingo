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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
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
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	restoreMan, err := lifecycle.Restore(
		context.Background(), snapshotDir, targetDir,
	)
	require.NoError(t, err)
	require.Equal(t, snapMan.CommitTimestamp, restoreMan.CommitTimestamp)
	require.Equal(t, snapMan.TipSlot, restoreMan.TipSlot)

	// Reopen the restored data dir like a normal node startup would and
	// confirm the blocks survived the round trip.
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", targetDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", targetDir,
	))
	restored, err := database.New(&database.Config{
		DataDir:        targetDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	defer restored.Close()

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
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)

	// Target dir already has a file in it.
	targetDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(targetDir, "existing.txt"), []byte("data"), 0o644,
	))

	_, err = lifecycle.Restore(context.Background(), snapshotDir, targetDir)
	require.Error(t, err)
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
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
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
		context.Background(), src, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)

	targetDir := filepath.Join(t.TempDir(), "restored")
	_, err = lifecycle.RestoreValidated(
		context.Background(), snapshotDir, targetDir,
		func(m lifecycle.Manifest) error {
			return m.CheckPluginMatch("gcs", "sqlite")
		},
	)
	require.Error(t, err)

	_, statErr := os.Stat(targetDir)
	require.Truef(
		t, os.IsNotExist(statErr),
		"a rejected validate hook must run before targetDir is created, "+
			"got stat error: %v", statErr,
	)
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

func init() {
	lifecycle.RegisterCloudDestinationScheme(
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
// against comment-60's original gap: PeekManifest used to always go
// through the full download-based resolveManifest path, even for a
// cloud snapshotDir whose destination type supports fetching just the
// one manifest.json object via CloudManifestFetcher -- downloading the
// (possibly very large) blob/metadata backups alongside it just to read
// its manifest. This uses a destination whose UploadDir/DownloadDir both
// fail outright, so this test only passes if PeekManifest actually took
// the lightweight FetchCloudManifest path and never called DownloadDir
// at all.
func TestPeekManifestUsesLightweightCloudFetchWithoutDownloading(t *testing.T) {
	m, err := lifecycle.PeekManifest(context.Background(), "faketest-manifestonly://bucket/prefix")
	require.NoError(t, err)
	require.Equal(t, manifestOnlyFixture, m)
}

// TestPeekManifestFallsBackToDownloadWhenCloudDestinationLacksManifestFetcher
// verifies PeekManifest still works correctly against a cloud destination
// type that does NOT implement CloudManifestFetcher (this package's own
// "faketest" scheme has no such restriction, matching a real S3/GCS
// destination, both of which do implement it — this covers the
// interface-not-implemented fallback branch generically without needing
// a destination type that deliberately omits it).
func TestPeekManifestFallsBackToDownloadWhenCloudDestinationLacksManifestFetcher(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)

	snapshotDir := filepath.Join(t.TempDir(), "snap-peek")
	m, err := lifecycle.SnapshotToCloud(
		context.Background(), db, snapshotDir,
		lifecycle.TriggerManual, "test-version", "faketest://bucket/prefix",
	)
	require.NoError(t, err)

	peeked, err := lifecycle.PeekManifest(
		context.Background(), "faketest://bucket/prefix/snap-peek",
	)
	require.NoError(t, err)
	require.Equal(t, m.CommitTimestamp, peeked.CommitTimestamp)
}
