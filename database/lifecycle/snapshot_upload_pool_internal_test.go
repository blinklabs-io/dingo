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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// writeManyChunkSnapshot writes a snapshot whose immutable index declares
// chunkCount chunk trios, three payload files each.
func writeManyChunkSnapshot(t *testing.T, dir string, chunkCount int) {
	t.Helper()
	idx := immutableIndex{Version: immutableIndexVersion, ChunkSlots: 1000}
	for i := range chunkCount {
		for _, ext := range []string{"chunk", "primary", "secondary"} {
			name := fmt.Sprintf("%05d.%s", i, ext)
			data := []byte(name)
			require.NoError(t, os.WriteFile(
				filepath.Join(dir, immutableFilePrefix+name), data, 0o600,
			))
			idx.Files = append(idx.Files, immutableIndexFile{
				Name: name, Size: int64(len(data)), SHA256: sha256Hex(data),
			})
		}
	}
	indexData, err := json.Marshal(idx)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, ImmutableIndexFileName), indexData, 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, BlobBackupFileName), []byte("blob"), 0o600,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, MetadataBackupFileName), []byte("metadata"), 0o600,
	))
	require.NoError(t, WriteManifest(dir, Manifest{
		BlobBytes:            4,
		MetadataBytes:        8,
		BlobSHA256:           sha256Hex([]byte("blob")),
		MetadataSHA256:       sha256Hex([]byte("metadata")),
		ImmutableIndexBytes:  int64(len(indexData)),
		ImmutableIndexSHA256: sha256Hex(indexData),
	}))
}

func TestSnapshotUploadDeferredChunkRejectsTampering(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "epoch-1")
	require.NoError(t, os.Mkdir(dir, 0o700))
	writeManyChunkSnapshot(t, dir, 2)
	ctx := context.Background()
	files, _, root, err := openSnapshotUploadFiles(ctx, dir, MaxManifestBytes)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, closeSnapshotUploadFiles(files))
		require.NoError(t, root.Close())
	})
	tampered := filepath.Join(dir, immutableFilePrefix+"00001.primary")
	require.NoError(t, os.WriteFile(tampered, []byte("00001.primarX"), 0o600))
	got := forEachSnapshotUploadFile(
		ctx, root, files, 1,
		func(context.Context, snapshotUploadFile, *os.File) error {
			return nil
		},
	)
	require.ErrorIs(t, got, ErrSnapshotPayloadMismatch)
	require.Contains(t, got.Error(), "00001.primary")
}

// TestSnapshotUploadChunksRunConcurrentlyWithinBound holds each chunk callback
// until the pool is full, so a serial implementation never releases, and
// records the most callbacks, each holding one open file, in flight at once.
func TestSnapshotUploadChunksRunConcurrentlyWithinBound(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "epoch-1")
	require.NoError(t, os.Mkdir(dir, 0o700))
	writeManyChunkSnapshot(t, dir, 20)
	ctx := t.Context()
	files, _, root, err := openSnapshotUploadFiles(ctx, dir, MaxManifestBytes)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, closeSnapshotUploadFiles(files))
		require.NoError(t, root.Close())
	})

	const workers = 4
	var inFlight, peak atomic.Int64
	full := make(chan struct{})
	var once atomic.Bool
	var timedOut atomic.Bool
	var manifestAfterChunks atomic.Bool
	var chunksDone atomic.Int64
	err = forEachSnapshotUploadFile(
		ctx, root, files, workers,
		func(_ context.Context, file snapshotUploadFile, _ *os.File) error {
			if file.name == ManifestFileName {
				manifestAfterChunks.Store(chunksDone.Load() == 60)
				return nil
			}
			if file.declared.name == "" {
				return nil
			}
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			if n == workers && once.CompareAndSwap(false, true) {
				close(full)
			}
			select {
			case <-full:
			case <-time.After(10 * time.Second):
				timedOut.Store(true)
				return errors.New("pool never filled: chunks are not concurrent")
			}
			inFlight.Add(-1)
			chunksDone.Add(1)
			return nil
		},
	)
	require.False(t, timedOut.Load(), "chunk files were not processed concurrently")
	require.NoError(t, err)
	require.Equal(t, int64(workers), peak.Load())
	require.True(t, manifestAfterChunks.Load())
}
