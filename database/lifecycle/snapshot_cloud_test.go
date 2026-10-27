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

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// TestSnapshotToCloudLabelsBeforeMirroring verifies that a name/description
// passed to SnapshotToCloud are already on the manifest MirrorToCloud
// uploads, not applied to the local copy only after the upload already
// happened. Labeling only the local manifest post-upload would leave the
// remote copy permanently unlabeled: the upload already completed and the
// cloud-mirrored marker already records that destination as done, so
// nothing would ever retry the upload to pick up a label applied later.
func TestSnapshotToCloudLabelsBeforeMirroring(t *testing.T) {
	t.Parallel()

	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	dir := filepath.Join(t.TempDir(), "snap-labeled")
	m, err := lifecycle.SnapshotToCloud(
		context.Background(),
		testDestinationRegistry,
		db,
		dir,
		lifecycle.TriggerManual,
		"test-version",
		"badger",
		"sqlite",
		"faketest://bucket/prefix",
		"my-label",
		"my-description",
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.NoError(t, err)
	require.Equal(t, "my-label", m.Name)
	require.Equal(t, "my-description", m.Description)

	local, err := lifecycle.ReadManifest(
		dir, lifecycle.WithManifestKey(testTrustKey),
	)
	require.NoError(t, err)
	require.Equal(t, "my-label", local.Name)
	require.Equal(t, "my-description", local.Description)

	cloudDir := filepath.Join(backingDir, "prefix", "snap-labeled")
	cloudManifest, err := lifecycle.ReadManifest(
		cloudDir, lifecycle.WithManifestKey(testTrustKey),
	)
	require.NoError(t, err)
	require.Equal(t, "my-label", cloudManifest.Name)
	require.Equal(t, "my-description", cloudManifest.Description)
}

func TestMirrorToCloudRejectsIncompleteLegacySnapshot(t *testing.T) {
	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)
	dir := filepath.Join(t.TempDir(), "unsigned")
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, lifecycle.WriteManifest(dir, lifecycle.Manifest{}))

	err := lifecycle.MirrorToCloud(
		context.Background(),
		testDestinationRegistry,
		dir,
		"faketest://bucket/prefix",
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(backingDir, "prefix", "unsigned"))
}

func TestMirrorToCloudRejectsLegacySnapshot(t *testing.T) {
	t.Parallel()

	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dir := filepath.Join(t.TempDir(), "epoch-legacy")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir,
		lifecycle.TriggerEpochBoundary, "test", "badger", "sqlite",
	)
	require.NoError(t, err)

	manifest, err := lifecycle.ReadManifest(dir)
	require.NoError(t, err)
	manifest.BlobSHA256 = ""
	manifest.MetadataSHA256 = ""
	require.NoError(t, lifecycle.WriteManifest(dir, manifest))

	err = lifecycle.MirrorToCloud(
		context.Background(), testDestinationRegistry, dir,
		"faketest://bucket/prefix",
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.ErrorIs(t, err, lifecycle.ErrManifestUnauthenticated)
	require.NoDirExists(t, filepath.Join(backingDir, "prefix", "epoch-legacy"))
}

// TestIsCloudMirroredToDetectsChangedDestination guards the gap a bare
// marker-presence check (IsCloudMirrored) has: the marker only proves a
// mirror happened to *some* destination, not that it matches whatever
// destination is configured right now. If an operator repoints
// SnapshotCloudDestination at a new bucket, a marker left over from the
// old one must not be mistaken for "already mirrored to the destination
// configured now" -- that destination has never actually received this
// snapshot.
func TestIsCloudMirroredToDetectsChangedDestination(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	backingDir := t.TempDir()
	setFakeCloudBackingDir(t, backingDir)
	const cloudDest = "faketest://user:secret@bucket/prefix?token=private#fragment"
	blob := []byte("blob")
	metadata := []byte("metadata")
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, lifecycle.BlobBackupFileName), blob, 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, lifecycle.MetadataBackupFileName), metadata, 0o600,
	))
	require.NoError(t, lifecycle.WriteManifest(dir, lifecycle.Manifest{
		BlobBytes:      int64(len(blob)),
		MetadataBytes:  int64(len(metadata)),
		BlobSHA256:     sha256Hex(blob),
		MetadataSHA256: sha256Hex(metadata),
	}, lifecycle.WithManifestKey(testTrustKey)))

	require.False(
		t, lifecycle.IsCloudMirroredTo(dir, cloudDest),
		"no marker written yet",
	)

	require.NoError(t, lifecycle.MirrorToCloud(
		t.Context(), testDestinationRegistry, dir, cloudDest,
		lifecycle.WithManifestKey(testTrustKey),
	))
	marker, err := os.ReadFile(lifecycle.CloudMirrorMarkerPath(dir))
	require.NoError(t, err)
	require.Contains(t, string(marker), "v1:sha256:")
	for _, secret := range []string{"user", "secret", "private", "fragment"} {
		require.NotContains(t, string(marker), secret)
	}

	require.True(
		t,
		lifecycle.IsCloudMirroredTo(dir, cloudDest),
		"marker matches the destination it actually names",
	)
	for _, changed := range []string{
		"faketest://other:secret@bucket/prefix?token=private#fragment",
		"faketest://user:secret@bucket/prefix?token=changed#fragment",
		"faketest://user:secret@bucket/prefix?token=private#changed",
		"faketest://user:secret@bucket/other?token=private#fragment",
	} {
		require.False(t, lifecycle.IsCloudMirroredTo(dir, changed))
	}
	require.True(
		t,
		lifecycle.IsCloudMirrored(dir),
		"a stale marker is still a marker -- IsCloudMirrored's plain presence check is unaffected",
	)

	require.NoError(t, os.WriteFile(
		lifecycle.CloudMirrorMarkerPath(dir), []byte(cloudDest+"\n"), 0o600,
	))
	require.False(t, lifecycle.IsCloudMirroredTo(dir, cloudDest),
		"legacy raw URI markers force a safe retry")

	// Trailing whitespace would survive TrimSpace and match, so only the
	// size bound rejects this marker.
	oversized := string(marker) + strings.Repeat(" ", 4096)
	require.NoError(t, os.WriteFile(
		lifecycle.CloudMirrorMarkerPath(dir), []byte(oversized), 0o600,
	))
	require.False(t, lifecycle.IsCloudMirroredTo(dir, cloudDest),
		"an oversized marker is rejected")
}

// swappingUploadDirDestination implements only CloudDestination.UploadDir.
// Before reading localDir it replaces the source snapshot's blob payload,
// standing in for a writer that races the upload after verification.
type swappingUploadDirDestination struct {
	sourceDir string
	uploaded  map[string][]byte
}

func (d *swappingUploadDirDestination) UploadDir(
	_ context.Context,
	localDir string,
) error {
	if err := os.WriteFile(
		filepath.Join(d.sourceDir, lifecycle.BlobBackupFileName),
		[]byte("replaced after verification"),
		0o600,
	); err != nil {
		return err
	}
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(localDir, entry.Name()))
		if err != nil {
			return err
		}
		d.uploaded[entry.Name()] = data
	}
	return nil
}

func (*swappingUploadDirDestination) DownloadFiles(
	context.Context,
	string,
	[]lifecycle.DownloadFile,
) error {
	return errors.New("not implemented")
}

// TestMirrorToCloudUploadDirReceivesVerifiedBytes covers a destination that
// only implements UploadDir: replacing a payload in the snapshot directory
// after verification must not change what that destination uploads.
func TestMirrorToCloudUploadDirReceivesVerifiedBytes(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	dir := filepath.Join(t.TempDir(), "epoch-swap")
	_, err := lifecycle.Snapshot(
		context.Background(), db, dir,
		lifecycle.TriggerEpochBoundary, "test", "badger", "sqlite",
		lifecycle.WithManifestKey(testTrustKey),
	)
	require.NoError(t, err)
	wantBlob, err := os.ReadFile(
		filepath.Join(dir, lifecycle.BlobBackupFileName),
	)
	require.NoError(t, err)
	wantMetadata, err := os.ReadFile(
		filepath.Join(dir, lifecycle.MetadataBackupFileName),
	)
	require.NoError(t, err)
	wantManifest, err := os.ReadFile(
		filepath.Join(dir, lifecycle.ManifestFileName),
	)
	require.NoError(t, err)

	dest := &swappingUploadDirDestination{
		sourceDir: dir,
		uploaded:  map[string][]byte{},
	}
	registry := lifecycle.NewDestinationRegistry()
	registry.Register(
		"swaptest",
		func(*url.URL) (lifecycle.CloudDestination, error) { return dest, nil },
	)
	require.NoError(t, lifecycle.MirrorToCloud(
		context.Background(), registry, dir, "swaptest://bucket/prefix",
		lifecycle.WithManifestKey(testTrustKey),
	))

	require.Equal(t, map[string][]byte{
		lifecycle.ManifestFileName:       wantManifest,
		lifecycle.BlobBackupFileName:     wantBlob,
		lifecycle.MetadataBackupFileName: wantMetadata,
	}, dest.uploaded)
	siblings, err := os.ReadDir(filepath.Dir(dir))
	require.NoError(t, err)
	require.Len(t, siblings, 1, "staging directory must be removed")
}
