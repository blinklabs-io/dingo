//go:build !windows

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
	"path/filepath"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestIsCloudMirroredToRejectsFIFOMarker(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "epoch-marker-fifo")
	writeMirrorFixtureSnapshot(t, dir)
	require.NoError(t, syscall.Mkfifo(lifecycle.CloudMirrorMarkerPath(dir), 0o600))

	require.False(t, lifecycle.IsCloudMirroredTo(dir, "faketest://bucket/prefix"))
	require.False(t, lifecycle.IsCloudMirrored(dir))
}
