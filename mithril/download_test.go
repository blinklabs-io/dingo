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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadSnapshot(t *testing.T) {
	content := []byte("fake-snapshot-archive-data-for-testing")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			_, _ = w.Write(content)
		}),
	)
	defer server.Close()

	destDir := t.TempDir()
	var progressCalled atomic.Int32

	path, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:      server.URL + "/snapshot.tar.zst",
		DestDir:  destDir,
		Filename: "test-snapshot.tar.zst",
		OnProgress: func(p DownloadProgress) {
			progressCalled.Add(1)
			// Use assert (not require) because this callback
			// may run outside the main test goroutine in the
			// future; require calls t.FailNow which panics
			// from non-test goroutines.
			assert.GreaterOrEqual(t, p.BytesDownloaded, int64(0))
		},
	})
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(destDir, "test-snapshot.tar.zst"),
		path,
	)

	assert.Greater(
		t,
		int(progressCalled.Load()),
		0,
		"OnProgress should have been called",
	)

	// Verify the file was written correctly
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestDownloadSnapshotResume(t *testing.T) {
	// Full content: "AAABBB"
	fullContent := []byte("AAABBB")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "bytes=3-" {
				w.Header().
					Set("Content-Range", "bytes 3-5/6")
				w.Header().
					Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent[3:]) // "BBB"
			} else {
				w.Header().
					Set("Content-Length", "6")
				_, _ = w.Write(fullContent)
			}
		}),
	)
	defer server.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "resume-test.tar.zst")

	// Write partial content first
	err := os.WriteFile(destPath, []byte("AAA"), 0o640)
	require.NoError(t, err)

	path, err := DownloadSnapshot(
		context.Background(),
		DownloadConfig{
			URL:      server.URL + "/snapshot.tar.zst",
			DestDir:  destDir,
			Filename: "resume-test.tar.zst",
		},
	)
	require.NoError(t, err)
	require.Equal(t, destPath, path)

	// Verify the full content
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

func TestDownloadSnapshotContextCancel(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate a slow response that never completes
			w.Header().Set("Content-Length", "1000000")
			w.WriteHeader(http.StatusOK)
			// Write nothing, just hang
		}),
	)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	destDir := t.TempDir()
	_, err := DownloadSnapshot(ctx, DownloadConfig{
		URL:     server.URL + "/snapshot.tar.zst",
		DestDir: destDir,
	})
	require.Error(t, err)
}

func TestDownloadSnapshotServerError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)
		}),
	)
	defer server.Close()

	destDir := t.TempDir()
	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:     server.URL + "/snapshot.tar.zst",
		DestDir: destDir,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestDownloadSnapshotSizeVerification(t *testing.T) {
	content := []byte("exact-size-content")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(content)
		}),
	)
	defer server.Close()

	destDir := t.TempDir()

	// Matching size should succeed
	path, err := DownloadSnapshot(
		context.Background(),
		DownloadConfig{
			URL:          server.URL + "/snapshot.tar.zst",
			DestDir:      destDir,
			Filename:     "good.tar.zst",
			ExpectedSize: int64(len(content)),
		},
	)
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestDownloadSnapshotSizeMismatch(t *testing.T) {
	content := []byte("short-content")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(content)
		}),
	)
	defer server.Close()

	destDir := t.TempDir()

	// Wrong expected size should fail
	_, err := DownloadSnapshot(
		context.Background(),
		DownloadConfig{
			URL:          server.URL + "/snapshot.tar.zst",
			DestDir:      destDir,
			Filename:     "bad.tar.zst",
			ExpectedSize: 99999,
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "download size mismatch")
}

func TestDownloadSnapshotDefaultFilename(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("data"))
		}),
	)
	defer server.Close()

	destDir := t.TempDir()
	path, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:     server.URL + "/snapshot.tar.zst",
		DestDir: destDir,
		// Filename is empty, should default to "snapshot.tar.zst"
	})
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(destDir, "snapshot.tar.zst"),
		path,
	)
}

func TestParseContentRangeStart(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   int64
	}{
		{
			name:   "valid range",
			header: "bytes 1024-2047/4096",
			want:   1024,
		},
		{
			name:   "start at zero",
			header: "bytes 0-999/1000",
			want:   0,
		},
		{
			name:   "large offset",
			header: "bytes 1073741824-2147483647/3221225472",
			want:   1073741824,
		},
		{
			name:   "unknown total",
			header: "bytes 512-1023/*",
			want:   512,
		},
		{
			name:   "empty header",
			header: "",
			want:   -1,
		},
		{
			name:   "missing bytes prefix",
			header: "1024-2047/4096",
			want:   -1,
		},
		{
			name:   "no dash",
			header: "bytes 1024",
			want:   -1,
		},
		{
			name:   "non-numeric start",
			header: "bytes abc-2047/4096",
			want:   -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseContentRangeStart(tt.header)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDownloadSnapshotResumeContentRangeMismatch(t *testing.T) {
	// Server returns 206 but with the wrong Content-Range start
	// offset. The downloader should detect this and restart from
	// scratch.
	fullContent := []byte("XXXYYYZZZ")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				// Return 206 with a mismatched start
				// offset (0 instead of requested 3).
				w.Header().Set(
					"Content-Range",
					fmt.Sprintf(
						"bytes 0-%d/%d",
						len(fullContent)-1,
						len(fullContent),
					),
				)
				w.Header().Set(
					"Content-Length",
					fmt.Sprintf("%d", len(fullContent)),
				)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent)
				return
			}
			// Full download on retry
			w.Header().Set(
				"Content-Length",
				fmt.Sprintf("%d", len(fullContent)),
			)
			_, _ = w.Write(fullContent)
		}),
	)
	defer server.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "mismatch-test.tar.zst")

	// Write partial content that does not match what the server
	// returns in Content-Range
	err := os.WriteFile(destPath, []byte("XXX"), 0o640)
	require.NoError(t, err)

	path, err := DownloadSnapshot(
		context.Background(),
		DownloadConfig{
			URL:      server.URL + "/snapshot.tar.zst",
			DestDir:  destDir,
			Filename: "mismatch-test.tar.zst",
		},
	)
	require.NoError(t, err)
	require.Equal(t, destPath, path)

	// The file should contain the full content from the restart,
	// not a corrupted partial+append.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

func TestDownloadSnapshotResumeMissingContentRange(t *testing.T) {
	// Server returns 206 without a Content-Range header. The
	// downloader should treat this as a mismatch (since
	// parseContentRangeStart returns -1) and restart from scratch.
	fullContent := []byte("ABCDEFGH")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				// Return 206 without Content-Range header
				w.Header().Set(
					"Content-Length",
					fmt.Sprintf("%d", len(fullContent)),
				)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent)
				return
			}
			// Full download on retry
			w.Header().Set(
				"Content-Length",
				fmt.Sprintf("%d", len(fullContent)),
			)
			_, _ = w.Write(fullContent)
		}),
	)
	defer server.Close()

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "no-range.tar.zst")

	// Write partial content
	err := os.WriteFile(destPath, []byte("ABCD"), 0o640)
	require.NoError(t, err)

	path, err := DownloadSnapshot(
		context.Background(),
		DownloadConfig{
			URL:      server.URL + "/snapshot.tar.zst",
			DestDir:  destDir,
			Filename: "no-range.tar.zst",
		},
	)
	require.NoError(t, err)
	require.Equal(t, destPath, path)

	// Should be the full content from the restarted download
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}
