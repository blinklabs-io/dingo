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

package mithril

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createChunkArchive creates a zstd-compressed tar archive that
// contains files mimicking an ImmutableDB structure.
func createChunkArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	// Create immutable directory entry
	err = tw.WriteHeader(&tar.Header{
		Name:     "immutable/",
		Typeflag: tar.TypeDir,
		Mode:     0o750,
	})
	require.NoError(t, err)

	// Create chunk files
	chunkFiles := map[string]string{
		"immutable/00000.chunk":     "chunk0 data",
		"immutable/00000.primary":   "primary0 data",
		"immutable/00000.secondary": "secondary0 data",
	}
	for name, content := range chunkFiles {
		err = tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o640,
			Size: int64(len(content)),
		})
		require.NoError(t, err)
		_, err = tw.Write([]byte(content))
		require.NoError(t, err)
	}

	err = tw.Close()
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

func TestBootstrap(t *testing.T) {
	archiveData := createChunkArchive(t)

	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "abc123def456789012345678",
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
				CertificateHash:      "cert123",
				Size:                 int64(len(archiveData)),
				CreatedAt:            "2026-02-10T00:24:56.094721055Z",
				Locations:            []string{}, // Will be set below
				CompressionAlgorithm: "zstandard",
			},
		},
	}

	mux := http.NewServeMux()

	// Snapshot list endpoint
	mux.HandleFunc(
		"/artifact/snapshots",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(snapshots); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		},
	)

	// Download endpoint
	mux.HandleFunc(
		"/download/snapshot.tar.zst",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(archiveData)
		},
	)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Set the download URL to point to our test server
	snapshots[0].Locations = []string{
		server.URL + "/download/snapshot.tar.zst",
	}

	downloadDir := t.TempDir()

	var progressCalled atomic.Int32
	result, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:          "preprod",
		AggregatorURL:    server.URL,
		DownloadDir:      downloadDir,
		CleanupAfterLoad: true,
		OnProgress: func(p DownloadProgress) {
			progressCalled.Add(1)
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "abc123def456789012345678", result.Snapshot.Digest)
	require.NotEmpty(t, result.ImmutableDir)
	require.NotEmpty(t, result.ArchivePath)
	assert.Greater(
		t,
		int(progressCalled.Load()),
		0,
		"OnProgress should have been called",
	)

	// Verify the immutable directory contains chunk files
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapCertVerifyNoCertHash(t *testing.T) {
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:          "abc123",
				Network:         "preprod",
				Locations:       []string{"http://example.com/s"},
				CertificateHash: "",
			},
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(snapshots)
		}),
	)
	t.Cleanup(server.Close)

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:                "preprod",
		AggregatorURL:          server.URL,
		VerifyCertificateChain: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no certificate hash")
}

func TestBootstrapNoSnapshots(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
		}),
	)
	defer server.Close()

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:       "preprod",
		AggregatorURL: server.URL,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no snapshots available")
}

func TestBootstrapNoLocations(t *testing.T) {
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:    "abc123",
				Network:   "preprod",
				Locations: []string{},
			},
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(snapshots); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}),
	)
	defer server.Close()

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:       "preprod",
		AggregatorURL: server.URL,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no download locations")
}

func TestBootstrapUnknownNetwork(t *testing.T) {
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network: "unknown_network",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolving aggregator URL")
}

func TestBootstrapResultCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, []byte("data"), 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extract")
	require.NoError(t, os.MkdirAll(extractDir, 0o750))

	ancillaryDir := filepath.Join(tmpDir, "ancillary")
	require.NoError(t, os.MkdirAll(ancillaryDir, 0o750))

	result := &BootstrapResult{
		ArchivePath:  archivePath,
		ExtractDir:   extractDir,
		AncillaryDir: ancillaryDir,
	}

	result.Cleanup(nil)

	// Individual paths should be removed
	_, err = os.Stat(archivePath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(extractDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(ancillaryDir)
	require.True(t, os.IsNotExist(err))

	// Parent directory should NOT be removed
	_, err = os.Stat(tmpDir)
	require.NoError(t, err)
}

func TestFindImmutableDir(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, baseDir string)
		expected string // relative to baseDir, or "" for not found
	}{
		{
			name: "chunks in root",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				err := os.WriteFile(
					filepath.Join(baseDir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "ROOT",
		},
		{
			name: "chunks in immutable subdir",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(baseDir, "immutable")
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "immutable",
		},
		{
			name: "chunks in db/immutable subdir",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(baseDir, "db", "immutable")
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "db/immutable",
		},
		{
			name: "single top-level dir with immutable inside",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(
					baseDir, "snapshot-data", "immutable",
				)
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "snapshot-data/immutable",
		},
		{
			name: "single top-level dir with db/immutable",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(
					baseDir,
					"snapshot-data",
					"db",
					"immutable",
				)
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "snapshot-data/db/immutable",
		},
		{
			name:     "empty directory",
			setup:    func(t *testing.T, baseDir string) {},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir := t.TempDir()
			tt.setup(t, baseDir)

			result := findImmutableDir(baseDir)
			switch tt.expected {
			case "":
				require.Empty(t, result)
			case "ROOT":
				require.Equal(t, baseDir, result)
			default:
				expected := filepath.Join(baseDir, tt.expected)
				require.Equal(t, expected, result)
			}
		})
	}
}
