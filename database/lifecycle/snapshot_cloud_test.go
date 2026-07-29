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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

// TestIsCloudMirroredToDetectsChangedDestination guards the gap a bare
// marker-presence check (IsCloudMirrored) has: the marker only proves a
// mirror happened to *some* destination, not that it matches whatever
// destination is configured right now. If an operator repoints
// SnapshotCloudDestination at a new bucket, a marker left over from the
// old one must not be mistaken for "already mirrored to the destination
// configured now" -- that destination has never actually received this
// snapshot.
func TestIsCloudMirroredToDetectsChangedDestination(t *testing.T) {
	dir := t.TempDir()

	require.False(
		t, lifecycle.IsCloudMirroredTo(dir, "managerfaketest://bucket/prefix"),
		"no marker written yet",
	)

	require.NoError(t, os.WriteFile(
		lifecycle.CloudMirrorMarkerPath(dir),
		[]byte("managerfaketest://old-bucket/prefix/"+filepath.Base(dir)+"\n"),
		0o600,
	))

	require.True(
		t,
		lifecycle.IsCloudMirroredTo(dir, "managerfaketest://old-bucket/prefix"),
		"marker matches the destination it actually names",
	)
	require.False(
		t,
		lifecycle.IsCloudMirroredTo(dir, "managerfaketest://new-bucket/prefix"),
		"marker names a since-abandoned destination, not the one configured now",
	)
	require.True(
		t, lifecycle.IsCloudMirrored(dir),
		"a stale marker is still a marker -- IsCloudMirrored's plain presence check is unaffected",
	)
}
