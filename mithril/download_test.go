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
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureSlogHandler struct {
	records []slog.Record
}

type trackingDownloadBody struct {
	reader bytes.Reader
	read   atomic.Int64
}

func (b *trackingDownloadBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read.Add(int64(n))
	return n, err
}

func (b *trackingDownloadBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func (h *captureSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureSlogHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *captureSlogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *captureSlogHandler) WithGroup(string) slog.Handler {
	return h
}

func TestDownloadSnapshot(t *testing.T) {
	t.Parallel()

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
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
		Filename:          "test-snapshot.tar.zst",
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

func TestDownloadSnapshotRoutineLogsAtDebug(t *testing.T) {
	t.Parallel()

	content := []byte("snapshot")
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
			_, _ = w.Write(content)
		}),
	)
	t.Cleanup(server.Close)

	handler := &captureSlogHandler{}
	logger := slog.New(handler)
	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           t.TempDir(),
		Filename:          "snapshot.tar.zst",
		ExpectedSize:      int64(len(content)),
		Logger:            logger,
	})
	require.NoError(t, err)

	for _, message := range []string{
		"downloading snapshot",
		"download complete",
		"download size verified",
	} {
		found := false
		for _, record := range handler.records {
			if record.Message != message {
				continue
			}
			assert.Equal(t, slog.LevelDebug, record.Level, message)
			found = true
			break
		}
		require.True(t, found, "missing log record %q", message)
	}
}

func TestNewPooledDownloadTransportUsesHTTP1Connections(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
			URL:               server.URL + "/snapshot.tar.zst",
			AllowInsecureHTTP: true,
			DestDir:           destDir,
			Filename:          "resume-test.tar.zst",
		},
	)
	require.NoError(t, err)
	require.Equal(t, destPath, path)

	// Verify the full content
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

// injectedIdle drives a download's idle detection from the test instead of
// from the wall clock. Production reports progress after each body write, so
// a test names the byte offsets it wants a stall at; the injector then fires
// the moment the download next arms its body watchdog, which is where a real
// timer would have fired had the transfer genuinely stopped there.
//
// Only idleTimeoutReader.Read resets a watchdog, so an injected idle is
// always delivered around a body read and never to the watchdog covering the
// request headers, whose expiry would abort an attempt before it read
// anything and turn a resume into a restart.
type injectedIdle struct {
	mu      sync.Mutex
	at      map[int64]struct{}
	pending bool
	count   int
}

func newInjectedIdle(offsets ...int64) *injectedIdle {
	at := make(map[int64]struct{}, len(offsets))
	for _, offset := range offsets {
		at[offset] = struct{}{}
	}
	return &injectedIdle{at: at}
}

// progress is a DownloadConfig.OnProgress callback. It arms an idle timeout
// when a report lands on a requested offset.
func (i *injectedIdle) progress(p DownloadProgress) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.at[p.BytesDownloaded]; !ok {
		return
	}
	delete(i.at, p.BytesDownloaded)
	i.pending = true
}

// watchdog is a DownloadConfig.idleWatchdogs factory. The configured timeout
// is deliberately ignored: this watchdog expires when the test says so.
func (i *injectedIdle) watchdog(_ time.Duration, onIdle func()) idleWatchdog {
	return &injectedIdleWatchdog{injector: i, onIdle: onIdle}
}

// arm requests an idle timeout that no progress report can trigger, for a
// stall before the attempt reads anything. Call it before the response is
// written: only a body read consumes the request, so arming early is what
// keeps the delivery point fixed.
func (i *injectedIdle) arm() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.pending = true
}

func (i *injectedIdle) take() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.pending {
		return false
	}
	i.pending = false
	i.count++
	return true
}

// delivered reports how many injected idle timeouts the download consumed.
func (i *injectedIdle) delivered() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.count
}

// remaining reports how many requested offsets were never reached.
func (i *injectedIdle) remaining() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.at)
}

type injectedIdleWatchdog struct {
	injector *injectedIdle
	onIdle   func()
}

func (w *injectedIdleWatchdog) Reset() {
	if w.injector.take() {
		w.onIdle()
	}
}

func (w *injectedIdleWatchdog) Stop() {}

func TestDownloadSnapshotIdleTimeoutRetriesAndResumes(t *testing.T) {
	t.Parallel()

	fullContent := []byte("AAABBB")
	var requestCount atomic.Int32
	resumeRangeCh := make(chan string, 1)

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch n := requestCount.Add(1); n {
			case 1:
				w.Header().
					Set("Content-Length", fmt.Sprintf("%d", len(fullContent)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(fullContent[:3])
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
			case 2:
				select {
				case resumeRangeCh <- r.Header.Get("Range"):
				default:
				}
				w.Header().Set("Content-Range", "bytes 3-5/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent[3:])
			default:
				// A request the script does not cover means the download
				// retried without the test injecting an idle timeout.
				// Report that here instead of serving a range the client
				// did not ask for, which would resurface later as an
				// unrelated Content-Range restart failure.
				t.Errorf(
					"unexpected download request %d with Range %q",
					n,
					r.Header.Get("Range"),
				)
				http.Error(
					w,
					"unexpected request",
					http.StatusConflict,
				)
			}
		}),
	)
	t.Cleanup(server.Close)

	// Stall the transfer once, after the first three bytes have landed.
	idle := newInjectedIdle(3)
	destDir := t.TempDir()
	cfg := DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
		Filename:          "idle-retry.tar.zst",
		ExpectedSize:      int64(len(fullContent)),
		// Any positive value enables idle detection; the injector, not
		// this duration, decides when the transfer counts as idle.
		IdleTimeout:    time.Minute,
		MaxIdleRetries: 1,
		OnProgress:     idle.progress,
		idleWatchdogs:  idle.watchdog,
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		testutil.AsyncWait,
	)
	defer cancel()

	path, err := DownloadSnapshot(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(destDir, "idle-retry.tar.zst"), path)
	require.Equal(t, int32(2), requestCount.Load())
	require.Equal(
		t,
		1,
		idle.delivered(),
		"download did not act on the injected idle timeout",
	)
	require.Zero(t, idle.remaining(), "injected stall was never reached")
	require.Equal(
		t,
		"bytes=3-",
		testutil.RequireReceive(
			t,
			resumeRangeCh,
			testutil.AsyncWait,
			"retry resume range",
		),
	)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

func TestDownloadSnapshotIdleRetriesResetAfterProgress(t *testing.T) {
	t.Parallel()

	// Three stalls against a budget of one, arranged so the download only
	// survives them if progress restores the budget: the first and third
	// attempts read nothing and each spend a retry, and the second reads
	// three bytes. Spending two retries either side of that progress is
	// what distinguishes a budget that resets from one that merely is not
	// charged for a productive attempt.
	fullContent := []byte("AAABBB")
	var requestCount atomic.Int32
	rangeCh := make(chan string, 4)
	idle := newInjectedIdle(3)

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case rangeCh <- r.Header.Get("Range"):
			default:
			}
			switch n := requestCount.Add(1); n {
			case 1:
				// Stall before a single byte arrives.
				idle.arm()
				w.Header().Set(
					"Content-Length",
					fmt.Sprintf("%d", len(fullContent)),
				)
				w.WriteHeader(http.StatusOK)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
			case 2:
				// Make progress, then stall at three bytes.
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
			case 3:
				// Stall again without adding a byte.
				idle.arm()
				w.Header().Set("Content-Range", "bytes 3-5/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				<-r.Context().Done()
			case 4:
				w.Header().Set("Content-Range", "bytes 3-5/6")
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(fullContent[3:])
			default:
				// See TestDownloadSnapshotIdleTimeoutRetriesAndResumes:
				// an unscripted request is a defect in the retry path,
				// not a range to serve.
				t.Errorf(
					"unexpected download request %d with Range %q",
					n,
					r.Header.Get("Range"),
				)
				http.Error(
					w,
					"unexpected request",
					http.StatusConflict,
				)
			}
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	cfg := DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
		Filename:          "idle-progress-reset.tar.zst",
		ExpectedSize:      int64(len(fullContent)),
		// Any positive value enables idle detection; the injector, not
		// this duration, decides when the transfer counts as idle.
		IdleTimeout:    time.Minute,
		MaxIdleRetries: 1,
		OnProgress:     idle.progress,
		idleWatchdogs:  idle.watchdog,
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		testutil.AsyncWait,
	)
	defer cancel()

	path, err := DownloadSnapshot(ctx, cfg)
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(destDir, "idle-progress-reset.tar.zst"),
		path,
	)
	require.Equal(t, int32(4), requestCount.Load())
	require.Equal(
		t,
		3,
		idle.delivered(),
		"download did not act on all three injected idle timeouts",
	)
	require.Zero(t, idle.remaining(), "an injected stall was never reached")
	for i, want := range []string{"", "", "bytes=3-", "bytes=3-"} {
		require.Equal(
			t,
			want,
			testutil.RequireReceive(
				t,
				rangeCh,
				testutil.AsyncWait,
				"request range",
			),
			"request %d range",
			i+1,
		)
	}

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fullContent, data)
}

func TestDownloadSnapshotRejectsNegativeMaxIdleRetries(t *testing.T) {
	t.Parallel()

	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:            "http://example.invalid/snapshot.tar.zst",
		DestDir:        t.TempDir(),
		MaxIdleRetries: -1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MaxIdleRetries")
}

// TestDownloadSnapshotRejectsPlainHTTPByDefault proves a plain-HTTP
// artifact URL is rejected before any request is attempted
// (DSA-2026-04-24-03): the aggregator-supplied download location is
// untrusted, so the scheme is checked up front rather than relying on
// httpsOnlyRedirect, which only governs where a redirect may lead.
func TestDownloadSnapshotRejectsPlainHTTPByDefault(t *testing.T) {
	t.Parallel()

	called := false
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}),
	)
	t.Cleanup(server.Close)

	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:     server.URL + "/snapshot.tar.zst",
		DestDir: t.TempDir(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "must use https")
	require.False(
		t,
		called,
		"no request should reach a rejected plain-HTTP URL",
	)
}

// TestDownloadSnapshotAllowsPlainHTTPWithEscapeHatch proves
// DownloadConfig.AllowInsecureHTTP is a working, explicit escape hatch
// (used throughout this package's own httptest-based tests).
func TestDownloadSnapshotAllowsPlainHTTPWithEscapeHatch(t *testing.T) {
	t.Parallel()

	content := []byte("fake-snapshot-data")
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(content)
		}),
	)
	t.Cleanup(server.Close)

	path, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           t.TempDir(),
		Filename:          "insecure-ok.tar.zst",
	})
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

// TestDownloadSnapshotAcceptsHTTPSByDefault proves a well-formed HTTPS
// artifact URL passes scheme validation (the connection itself fails
// against the loopback TLS test server's self-signed certificate, which
// is expected — the point is that it isn't rejected for its scheme).
func TestDownloadSnapshotAcceptsHTTPSByDefault(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)
	t.Cleanup(server.Close)

	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:                 server.URL + "/snapshot.tar.zst",
		DestDir:             t.TempDir(),
		MaxTransientRetries: -1,
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "must use https")
}

// armed reports whether an idle period is currently running. Tests assert on
// it so they can read the timer's state at once, instead of waiting out a
// wall-clock interval and inferring the state from what did not happen.
func (t *idleTimer) armed() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.current != nil
}

// TestIdleTimeoutReaderStopsTimerBetweenReads proves the reader disarms its
// watchdog once a read returns, so a consumer that is slow to ask for the
// next chunk is not mistaken for a stalled transfer. The timeout is long and
// never waited on: the assertion reads the timer's armed state, so the test
// neither pays for the interval nor races it.
func TestIdleTimeoutReaderStopsTimerBetweenReads(t *testing.T) {
	t.Parallel()

	var idleCount atomic.Int32
	timer := newIdleTimer(time.Hour, func() {
		idleCount.Add(1)
	})
	reader := newIdleTimeoutReader(bytes.NewReader([]byte("abc")), timer)
	require.True(t, timer.armed(), "constructor should arm the timer")

	buf := make([]byte, 1)
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte("a"), buf)

	require.False(
		t,
		timer.armed(),
		"idle timer should be stopped between reads",
	)
	require.Zero(t, idleCount.Load())
}

func TestDownloadSnapshotContextCancel(t *testing.T) {
	t.Parallel()

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
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
	})
	require.Error(t, err)
}

func TestDownloadSnapshotServerError(t *testing.T) {
	t.Parallel()

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
		AllowInsecureHTTP:   true,
		DestDir:             destDir,
		MaxTransientRetries: -1, // disable retries so the test stays fast
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestDownloadSnapshotTransientRetrySucceeds(t *testing.T) {
	t.Parallel()

	content := []byte("ok-after-transient")
	var requestCount atomic.Int32

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requestCount.Add(1) == 1 {
				http.Error(
					w,
					"temporarily unavailable",
					http.StatusServiceUnavailable,
				)
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
		AllowInsecureHTTP:   true,
		DestDir:             destDir,
		Filename:            "transient-retry.tar.zst",
		MaxTransientRetries: 2,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		int32(2),
		requestCount.Load(),
		"expected one retry after transient 503",
	)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestDownloadSnapshotTransientRetryExhausted(t *testing.T) {
	t.Parallel()

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
		AllowInsecureHTTP:   true,
		DestDir:             destDir,
		MaxTransientRetries: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "429")
	// 1 original attempt + 2 retries = 3 total requests
	require.Equal(
		t,
		int32(3),
		requestCount.Load(),
		"expected 1 attempt + 2 retries",
	)
}

func TestIsTransientDownloadError(t *testing.T) {
	t.Parallel()

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
			name: "unexpected EOF",
			err: fmt.Errorf(
				"writing snapshot data: %w",
				io.ErrUnexpectedEOF,
			),
			transient: true,
		},
		{
			name: "connection reset",
			err: fmt.Errorf(
				"downloading snapshot: %w",
				syscall.ECONNRESET,
			),
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
	t.Parallel()

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
			URL:               server.URL + "/snapshot.tar.zst",
			AllowInsecureHTTP: true,
			DestDir:           destDir,
			Filename:          "good.tar.zst",
			ExpectedSize:      int64(len(content)),
		},
	)
	require.NoError(t, err)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestDownloadSnapshotSizeMismatch(t *testing.T) {
	t.Parallel()

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
			URL:               server.URL + "/snapshot.tar.zst",
			AllowInsecureHTTP: true,
			DestDir:           destDir,
			Filename:          "bad.tar.zst",
			ExpectedSize:      99999,
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "download size mismatch")
}

func TestDownloadSnapshotBoundsResponseRead(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		expect  int64
		wantErr bool
	}{
		{name: "exact boundary", body: "abc", expect: 3},
		{name: "over boundary", body: "abcdefgh", expect: 3, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responseBody := &trackingDownloadBody{}
			responseBody.reader = *bytes.NewReader([]byte(tc.body))
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       responseBody,
					Header:     make(http.Header),
				}, nil
			})}
			path, err := DownloadSnapshot(context.Background(), DownloadConfig{
				URL:                 "https://example.test/snapshot.tar.zst",
				DestDir:             t.TempDir(),
				Filename:            "bounded.tar.zst",
				ExpectedSize:        tc.expect,
				HTTPClient:          client,
				MaxTransientRetries: -1,
			})
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrDownloadTooLarge)
				require.Contains(t, err.Error(), "exceeds expected size")
				require.Empty(t, path)
				require.LessOrEqual(t, responseBody.read.Load(), tc.expect+1)
				return
			}
			require.NoError(t, err)
			data, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, tc.body, string(data))
			require.Equal(t, tc.expect, responseBody.read.Load())
		})
	}
}

func TestDownloadSnapshotMaxExpectedSizeDoesNotOverflow(t *testing.T) {
	body := &trackingDownloadBody{}
	body.reader = *bytes.NewReader([]byte("x"))
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})}
	path, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:          "https://example.test/snapshot.tar.zst",
		DestDir:      t.TempDir(),
		Filename:     "max.tar.zst",
		ExpectedSize: math.MaxInt64,
		MaxBytes:     math.MaxInt64,
		HTTPClient:   client,
	})
	require.ErrorContains(t, err, "download size mismatch")
	require.Equal(t, int64(1), body.read.Load())
	require.Empty(t, path)
}

func TestDownloadSnapshotConfiguredByteLimit(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, body, contentRange string
		status                           int
		contentLength, wantRead          int64
		wantError, removed               bool
	}{
		{name: "below", body: "ab", status: 200, contentLength: -1, wantRead: 2},
		{name: "exact", body: "abc", status: 200, contentLength: -1, wantRead: 3},
		{name: "stream oversized", body: "abcdefgh", status: 200, contentLength: -1, wantRead: 4, wantError: true, removed: true},
		{name: "header oversized", body: "abcdefgh", status: 200, contentLength: 8, wantError: true},
		{name: "resume exact", prefix: "a", body: "bc", status: 206, contentRange: "bytes 1-2/3", contentLength: -1, wantRead: 2},
		{name: "resume oversized", prefix: "a", body: "bcdefgh", status: 206, contentRange: "bytes 1-7/8", contentLength: -1, wantRead: 3, wantError: true, removed: true},
		{name: "resume header oversized", prefix: "a", body: "bcdefgh", status: 206, contentRange: "bytes 1-7/8", contentLength: math.MaxInt64, wantError: true},
		{name: "range ignored", prefix: "ab", body: "abc", status: 200, contentLength: -1, wantRead: 3},
		{name: "range ignored oversized", prefix: "ab", body: "abcdefgh", status: 200, contentLength: -1, wantRead: 4, wantError: true, removed: true},
		{name: "already complete", prefix: "abc", status: 416, contentRange: "bytes */3", contentLength: -1},
		{name: "existing oversized", prefix: "abcd", status: 416, contentRange: "bytes */4", contentLength: -1, wantError: true, removed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dest := filepath.Join(dir, "archive")
			if tc.prefix != "" {
				require.NoError(t, os.WriteFile(dest, []byte(tc.prefix), 0o600))
			}
			body := &trackingDownloadBody{reader: *bytes.NewReader([]byte(tc.body))}
			requests := 0
			cfg := DownloadConfig{
				URL: "https://example.test/archive", DestDir: dir, Filename: "archive",
				MaxBytes: 3,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					requests++
					if tc.prefix != "" {
						require.Equal(t, fmt.Sprintf("bytes=%d-", len(tc.prefix)), r.Header.Get("Range"))
					}
					return &http.Response{StatusCode: tc.status, Body: body,
						ContentLength: tc.contentLength, Header: http.Header{"Content-Range": {tc.contentRange}}}, nil
				})},
			}
			path, err := DownloadSnapshot(context.Background(), cfg)
			require.Equal(t, tc.wantRead, body.read.Load(), "response read must stay within remaining file budget plus one probe byte")
			if tc.wantError {
				require.ErrorIs(t, err, ErrDownloadTooLarge)
				require.Empty(t, path)
				if tc.removed {
					require.NoFileExists(t, dest)
				}
			} else {
				require.NoError(t, err)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				want := tc.body
				if tc.status != 200 {
					want = tc.prefix + tc.body
				}
				require.Equal(t, want, string(data))
			}
			wantRequests := 1
			if len(tc.prefix) > 3 {
				wantRequests = 0
			}
			require.Equal(t, wantRequests, requests, "size errors must not retry")
		})
	}
}

func TestDownloadSnapshotRestartByteLimit(t *testing.T) {
	for _, advertised := range []bool{false, true} {
		for _, content := range []string{"abc", "abcdefgh"} {
			t.Run(fmt.Sprintf("advertised=%t/length=%d", advertised, len(content)), func(t *testing.T) {
				dir := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(dir, "archive"), []byte("a"), 0o600))
				body := &trackingDownloadBody{reader: *bytes.NewReader([]byte(content))}
				requests := 0
				path, err := DownloadSnapshot(context.Background(), DownloadConfig{
					URL: "https://example.test/archive", DestDir: dir, Filename: "archive", MaxBytes: 3,
					HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
						requests++
						if requests == 1 {
							return &http.Response{StatusCode: 206, Body: io.NopCloser(strings.NewReader("ignored")), Header: http.Header{"Content-Range": {"bytes 2-3/4"}}}, nil
						}
						require.Empty(t, r.Header.Get("Range"))
						length := int64(-1)
						if advertised {
							length = int64(len(content))
						}
						return &http.Response{StatusCode: 200, Body: body, ContentLength: length, Header: make(http.Header)}, nil
					})},
				})
				require.Equal(t, 2, requests)
				if len(content) > 3 {
					require.ErrorIs(t, err, ErrDownloadTooLarge)
					wantRead := int64(4)
					if advertised {
						wantRead = 0
					}
					require.Equal(t, wantRead, body.read.Load())
				} else {
					require.NoError(t, err)
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					require.Equal(t, content, string(data))
				}
			})
		}
	}
}

func TestDownloadConfigByteLimit(t *testing.T) {
	require.Equal(t, DefaultMaxDownloadBytes, (DownloadConfig{}).sizeLimit())
	require.Error(t, (DownloadConfig{MaxBytes: -1}).Validate())
	require.ErrorIs(t, (DownloadConfig{MaxBytes: 3, ExpectedSize: 4}).Validate(), ErrDownloadTooLarge)
	require.NoError(t, (DownloadConfig{MaxBytes: math.MaxInt64, ExpectedSize: math.MaxInt64}).Validate())
	require.Equal(t, int64(2), (DownloadConfig{MaxBytes: 3, ExpectedSize: 2}).sizeLimit())
}

// TestDownloadSnapshotRejectsPreexistingSymlinkEscape proves the TOCTOU
// fix for issue #3147: a symlink placed at the download destination
// path *before* the download starts, pointing outside DestDir, must
// not be followed. Before routing file creation through os.Root, a
// plain os.OpenFile(destPath, ...) would re-resolve the path through
// the OS and follow such a symlink, writing attacker-controlled data
// outside DestDir.
func TestDownloadSnapshotRejectsPreexistingSymlinkEscape(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks requires elevated privileges on windows")
	}

	content := []byte("attacker-controlled")
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(content)
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	outsideDir := t.TempDir()
	outsideTarget := filepath.Join(outsideDir, "pwned.txt")

	require.NoError(
		t,
		os.Symlink(outsideTarget, filepath.Join(destDir, "snapshot.tar.zst")),
	)

	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
	})
	require.Error(t, err)
	_, statErr := os.Stat(outsideTarget)
	require.True(
		t,
		os.IsNotExist(statErr),
		"download must not write through the symlink outside DestDir",
	)
}

// TestDownloadSnapshotRefusesSymlinkedDestDir proves DownloadSnapshot
// refuses to create/open its destination through a pre-existing symlink
// at DestDir itself, as opposed to the file it writes there (the
// preceding test). A bare os.MkdirAll(cfg.DestDir)+os.OpenRoot(cfg.DestDir)
// would silently succeed and write through such a symlink, because
// neither call inspects what it is binding to before using it.
func TestDownloadSnapshotRefusesSymlinkedDestDir(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("data"))
		}),
	)
	t.Cleanup(server.Close)

	parent := t.TempDir()
	outside := t.TempDir()
	destDir := filepath.Join(parent, "dest")
	requireSymlinkSupport(t, outside, destDir)

	_, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
	})
	require.Error(t, err)

	entries, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	assert.Empty(
		t, entries,
		"download must not write through the symlinked DestDir",
	)
}

// TestOsRootRejectsFinalSymlinkWithTrailingSlash directly exercises the
// exact shape of GO-2026-4970 (CVE-2026-39822) against the os.Root API
// this fix depends on: on Unix, os.Root.Open("symlink/") — the final
// path component is a symlink and the name ends in a trailing slash —
// escaped the root before Go 1.26.5, opening the symlink's target
// instead of refusing it. dingo's own call sites (ExtractArchive,
// DownloadSnapshot) never reach this exact shape themselves, because
// path.Clean/filepath.Base already strip trailing slashes before a name
// is ever passed to a Root method; this test instead pins the toolchain
// guarantee directly, independent of that incidental normalization, per
// the issue's 2026-08-21 runtime-prerequisite note. A regression here
// would also be caught by the govulncheck release gate (see Makefile,
// publish.yml), but this proves the behavior rather than just the
// absence of a CVE identifier.
func TestOsRootRejectsFinalSymlinkWithTrailingSlash(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("creating symlinks requires elevated privileges on windows")
	}

	destDir := t.TempDir()
	outsideDir := t.TempDir()
	require.NoError(
		t,
		os.Symlink(outsideDir, filepath.Join(destDir, "escape")),
	)

	root, err := os.OpenRoot(destDir)
	require.NoError(t, err)
	defer root.Close()

	f, openErr := root.Open("escape/")
	if f != nil {
		f.Close()
	}
	require.Error(
		t,
		openErr,
		"os.Root must refuse a final symlink with a trailing slash, not follow it outside the root",
	)
}

func TestDownloadSnapshotDefaultFilename(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("data"))
		}),
	)
	t.Cleanup(server.Close)

	destDir := t.TempDir()
	path, err := DownloadSnapshot(context.Background(), DownloadConfig{
		URL:               server.URL + "/snapshot.tar.zst",
		AllowInsecureHTTP: true,
		DestDir:           destDir,
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
	t.Parallel()

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
	t.Parallel()

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
			URL:               server.URL + "/snapshot.tar.zst",
			AllowInsecureHTTP: true,
			DestDir:           destDir,
			Filename:          "mismatch-test.tar.zst",
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
	t.Parallel()

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
			URL:               server.URL + "/snapshot.tar.zst",
			AllowInsecureHTTP: true,
			DestDir:           destDir,
			Filename:          "no-range.tar.zst",
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

func TestExtractArchiveZstdLimits(t *testing.T) {
	archive := createTestArchive(t, map[string]string{"payload": strings.Repeat("x", 4096)})
	var frame zstd.Header
	require.NoError(t, frame.Decode(archive))
	require.True(t, frame.SingleSegment)
	require.Greater(t, frame.FrameContentSize, uint64(1024))
	require.Less(t, frame.FrameContentSize, uint64(1<<20))
	archivePath := filepath.Join(t.TempDir(), "archive.tar.zst")
	require.NoError(t, os.WriteFile(archivePath, archive, 0o600))
	for _, tc := range []struct {
		name           string
		window, memory uint64
		wantError      bool
	}{
		{name: "valid", window: 1 << 20, memory: 1 << 20},
		{name: "window exceeded", window: 1024, memory: 1 << 20, wantError: true},
		{name: "memory exceeded", window: 1 << 20, memory: 1024, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "extracted")
			_, err := ExtractArchive(context.Background(), archivePath, dir, nil, WithZstdLimits(tc.window, tc.memory))
			if tc.wantError {
				// For a single-segment frame, the pinned decoder derives the
				// window from FCS. Both the memory check in framedec.reset
				// and the async window check return ErrDecoderSizeExceeded.
				require.ErrorIs(t, err, zstd.ErrDecoderSizeExceeded)
				require.NoDirExists(t, dir)
			} else {
				require.NoError(t, err)
				data, err := os.ReadFile(filepath.Join(dir, "payload"))
				require.NoError(t, err)
				require.Equal(t, strings.Repeat("x", 4096), string(data))
			}
		})
	}
}

func TestExtractArchive(t *testing.T) {
	t.Parallel()

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
	result, err := ExtractArchive(
		context.Background(),
		archivePath,
		extractDir,
		nil,
	)
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
