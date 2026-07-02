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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/klauspost/compress/zstd"
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
	t.Cleanup(server.Close)

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

func TestNewPooledDownloadTransportUsesHTTP1Connections(t *testing.T) {
	transport := newPooledDownloadTransport(4)

	require.False(t, transport.DisableKeepAlives)
	require.False(t, transport.ForceAttemptHTTP2)
	require.NotNil(t, transport.TLSNextProto)
	require.Empty(t, transport.TLSNextProto)
	require.NotNil(t, transport.TLSClientConfig)
	require.Equal(t, []string{"http/1.1"}, transport.TLSClientConfig.NextProtos)
	require.Equal(t, 8, transport.MaxIdleConns)
	require.Equal(t, 4, transport.MaxIdleConnsPerHost)
	require.Equal(t, 4, transport.MaxConnsPerHost)
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
	t.Cleanup(server.Close)

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

func TestDownloadSnapshotIdleTimeoutRetriesAndResumes(t *testing.T) {
	fullContent := []byte("AAABBB")
	var requestCount atomic.Int32
	resumeRangeCh := make(chan string, 1)

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch requestCount.Add(1) {
			case 1:
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fullContent)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(fullContent[:3])
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
			default:
				select {
				case resumeRangeCh <- r.Header.Get("Range"):
				default:
				}
				w.Header().Set("Content-Range", "bytes 3-5/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent[3:])
			}
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	cfg := DownloadConfig{
		URL:            server.URL + "/snapshot.tar.zst",
		DestDir:        destDir,
		Filename:       "idle-retry.tar.zst",
		ExpectedSize:   int64(len(fullContent)),
		IdleTimeout:    50 * time.Millisecond,
		MaxIdleRetries: 1,
	}
	timeout := cfg.IdleTimeout*time.Duration(cfg.MaxIdleRetries+1) +
		500*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	path, err := DownloadSnapshot(
		ctx,
		cfg,
	)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(destDir, "idle-retry.tar.zst"), path)
	require.Equal(t, int32(2), requestCount.Load())
	require.Equal(
		t,
		"bytes=3-",
		testutil.RequireReceive(
			t,
			resumeRangeCh,
			time.Second,
			"retry resume range",
		),
	)
	testutil.RequireNoReceive(
		t,
		resumeRangeCh,
		100*time.Millisecond,
		"no extra resume retries",
	)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

func TestDownloadSnapshotIdleRetriesResetAfterProgress(t *testing.T) {
	fullContent := []byte("AAABBBCCC")
	var requestCount atomic.Int32
	rangeCh := make(chan string, 2)

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch requestCount.Add(1) {
			case 1:
				w.Header().Set(
					"Content-Length",
					fmt.Sprintf("%d", len(fullContent)),
				)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(fullContent[:3])
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
			case 2:
				select {
				case rangeCh <- r.Header.Get("Range"):
				default:
				}
				w.Header().Set("Content-Range", "bytes 3-8/9")
				w.Header().Set("Content-Length", "6")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent[3:6])
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
			default:
				select {
				case rangeCh <- r.Header.Get("Range"):
				default:
				}
				w.Header().Set("Content-Range", "bytes 6-8/9")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent[6:])
			}
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	cfg := DownloadConfig{
		URL:            server.URL + "/snapshot.tar.zst",
		DestDir:        destDir,
		Filename:       "idle-progress-reset.tar.zst",
		ExpectedSize:   int64(len(fullContent)),
		IdleTimeout:    50 * time.Millisecond,
		MaxIdleRetries: 1,
	}
	timeout := 3*cfg.IdleTimeout + 500*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	path, err := DownloadSnapshot(ctx, cfg)
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(destDir, "idle-progress-reset.tar.zst"),
		path,
	)
	require.Equal(t, int32(3), requestCount.Load())
	require.Equal(
		t,
		"bytes=3-",
		testutil.RequireReceive(
			t,
			rangeCh,
			time.Second,
			"first retry resume range",
		),
	)
	require.Equal(
		t,
		"bytes=6-",
		testutil.RequireReceive(
			t,
			rangeCh,
			time.Second,
			"second retry resume range",
		),
	)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

func TestDownloadSnapshotRejectsNegativeMaxIdleRetries(t *testing.T) {
	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:            "http://example.invalid/snapshot.tar.zst",
		DestDir:        t.TempDir(),
		MaxIdleRetries: -1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MaxIdleRetries")
}

func TestIdleTimeoutReaderStopsTimerBetweenReads(t *testing.T) {
	idleCh := make(chan struct{}, 1)
	reader := newIdleTimeoutReader(
		bytes.NewReader([]byte("abc")),
		20*time.Millisecond,
		func() {
			select {
			case idleCh <- struct{}{}:
			default:
			}
		},
	)

	buf := make([]byte, 1)
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte("a"), buf)

	testutil.RequireNoReceive(
		t,
		idleCh,
		50*time.Millisecond,
		"idle timer should be stopped between reads",
	)
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
	t.Cleanup(server.Close)

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
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:                 server.URL + "/snapshot.tar.zst",
		DestDir:             destDir,
		MaxTransientRetries: -1, // disable retries so the test stays fast
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestDownloadSnapshotTransientRetrySucceeds(t *testing.T) {
	content := []byte("ok-after-transient")
	var requestCount atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requestCount.Add(1) == 1 {
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			_, _ = w.Write(content)
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	path, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:                 server.URL + "/snapshot.tar.zst",
		DestDir:             destDir,
		Filename:            "transient-retry.tar.zst",
		MaxTransientRetries: 2,
	})
	require.NoError(t, err)
	require.Equal(t, int32(2), requestCount.Load(), "expected one retry after transient 503")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestDownloadSnapshotTransientRetryExhausted(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			http.Error(w, "rate limited", http.StatusTooManyRequests)
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:                 server.URL + "/snapshot.tar.zst",
		DestDir:             destDir,
		MaxTransientRetries: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "429")
	// 1 original attempt + 2 retries = 3 total requests
	require.Equal(t, int32(3), requestCount.Load(), "expected 1 attempt + 2 retries")
}

func TestIsTransientDownloadError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{
			name:      "nil",
			err:       nil,
			transient: false,
		},
		{
			name:      "errDownloadTransient sentinel",
			err:       fmt.Errorf("wrapped: %w", errDownloadTransient),
			transient: true,
		},
		{
			name:      "unexpected EOF",
			err:       fmt.Errorf("writing snapshot data: %w", io.ErrUnexpectedEOF),
			transient: true,
		},
		{
			name:      "connection reset",
			err:       fmt.Errorf("downloading snapshot: %w", syscall.ECONNRESET),
			transient: true,
		},
		{
			name:      "non-transient error",
			err:       fmt.Errorf("download size mismatch: got 1, want 2"),
			transient: false,
		},
		{
			name:      "idle timeout is not transient",
			err:       fmt.Errorf("stalled: %w", errDownloadIdleTimeout),
			transient: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.transient, isTransientDownloadError(tc.err))
		})
	}
}

func TestDownloadSnapshotSizeVerification(t *testing.T) {
	content := []byte("exact-size-content")

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(content)
		}),
	)
	t.Cleanup(server.Close)

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
	t.Cleanup(server.Close)

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
	t.Cleanup(server.Close)

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
	t.Cleanup(server.Close)

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
	t.Cleanup(server.Close)

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

// createTestArchive creates a zstd-compressed tar archive in memory
// containing the specified files.
func createTestArchive(
	t *testing.T,
	files map[string]string,
) []byte {
	t.Helper()
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Mode: 0o640,
			Size: int64(len(content)),
		}
		err := tw.WriteHeader(hdr)
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

func TestExtractArchive(t *testing.T) {
	files := map[string]string{
		"immutable/00000.chunk":     "chunk0 data",
		"immutable/00000.primary":   "primary0 data",
		"immutable/00000.secondary": "secondary0 data",
		"immutable/00001.chunk":     "chunk1 data",
	}

	archiveData := createTestArchive(t, files)

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, archiveData, 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extracted")
	result, err := ExtractArchive(context.Background(), archivePath, extractDir, nil)
	require.NoError(t, err)
	require.Equal(t, extractDir, result)

	// Verify extracted files
	for name, content := range files {
		data, err := os.ReadFile(
			filepath.Join(extractDir, name),
		)
		require.NoError(t, err)
		require.Equal(t, content, string(data))
	}
}

func TestExtractArchiveDirectoryTraversal(t *testing.T) {
	// Create an archive with a path traversal attempt
	archiveData := createTestArchive(t, map[string]string{
		"../../../etc/passwd": "evil",
	})

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "evil.tar.zst")
	err := os.WriteFile(archivePath, archiveData, 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extracted")
	_, err = ExtractArchive(context.Background(), archivePath, extractDir, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid path")
}

func TestExtractArchiveWithDirectories(t *testing.T) {
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	// Add a directory entry
	err = tw.WriteHeader(&tar.Header{
		Name:     "db/",
		Typeflag: tar.TypeDir,
		Mode:     0o750,
	})
	require.NoError(t, err)

	// Add a file in the directory
	content := "file content"
	err = tw.WriteHeader(&tar.Header{
		Name: "db/test.txt",
		Mode: 0o640,
		Size: int64(len(content)),
	})
	require.NoError(t, err)
	_, err = tw.Write([]byte(content))
	require.NoError(t, err)

	err = tw.Close()
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "dirs.tar.zst")
	err = os.WriteFile(archivePath, buf.Bytes(), 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extracted")
	_, err = ExtractArchive(context.Background(), archivePath, extractDir, nil)
	require.NoError(t, err)

	data, err := os.ReadFile(
		filepath.Join(extractDir, "db", "test.txt"),
	)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}

func TestValidRelPath(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		valid bool
	}{
		{"normal file", "immutable/00000.chunk", true},
		{"nested path", "db/immutable/00000.chunk", true},
		{"parent traversal", "../etc/passwd", false},
		{"embedded traversal", "foo/../bar", false},
		{"standalone dotdot", "..", false},
		{"trailing dotdot", "foo/..", false},
		{"absolute path", "/etc/passwd", false},
		{"backslash", `foo\bar`, false},
		{"empty", "", false},
		{"dot", ".", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.valid, validRelPath(tt.path))
		})
	}
}
