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
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestIsCloudMirroredToRejectsFIFOMarker(t *testing.T) {
	t.Parallel()

	setFakeCloudBackingDir(t, t.TempDir())
	const cloudDest = "faketest://bucket/prefix"
	dir := filepath.Join(t.TempDir(), "epoch-marker-fifo")
	writeMirrorFixtureSnapshot(t, dir)
	require.NoError(t, lifecycle.MirrorToCloud(
		t.Context(), testDestinationRegistry, dir, cloudDest,
		lifecycle.WithManifestKey(testTrustKey),
	))
	marker := lifecycle.CloudMirrorMarkerPath(dir)
	digest, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.NoError(t, os.Remove(marker))
	require.NoError(t, syscall.Mkfifo(marker, 0o600))

	// A blocking open of a FIFO's read end waits for a writer, so a reader
	// that does not refuse special files would hang rather than fail. Supply
	// a writer that publishes a valid digest and then closes, so such a
	// reader sees the digest and EOF and the assertions below fail promptly.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w, err := os.OpenFile(marker, os.O_WRONLY, 0)
		if err != nil {
			return
		}
		_, _ = w.Write(digest)
		_ = w.Close()
	}()
	t.Cleanup(func() {
		// Release the writer if nothing opened the read end: a
		// non-blocking read open completes its blocked open.
		r, err := os.OpenFile(marker, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		<-writerDone
		if err == nil {
			_ = r.Close()
		}
	})

	require.False(t, lifecycle.IsCloudMirroredTo(dir, cloudDest))
	require.False(t, lifecycle.IsCloudMirrored(dir))
}
