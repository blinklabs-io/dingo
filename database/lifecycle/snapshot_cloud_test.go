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
	)
	require.NoError(t, err)
	require.Equal(t, "my-label", m.Name)
	require.Equal(t, "my-description", m.Description)

	local, err := lifecycle.ReadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, "my-label", local.Name)
	require.Equal(t, "my-description", local.Description)

	cloudDir := filepath.Join(backingDir, "prefix", "snap-labeled")
	cloudManifest, err := lifecycle.ReadManifest(cloudDir)
	require.NoError(t, err)
	require.Equal(t, "my-label", cloudManifest.Name)
	require.Equal(t, "my-description", cloudManifest.Description)
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
	require.NoError(t, lifecycle.WriteManifest(dir, lifecycle.Manifest{}))

	require.False(
		t, lifecycle.IsCloudMirroredTo(dir, cloudDest),
		"no marker written yet",
	)

	require.NoError(t, lifecycle.MirrorToCloud(
		t.Context(), testDestinationRegistry, dir, cloudDest,
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
}
