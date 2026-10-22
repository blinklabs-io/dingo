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

package bark

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/stretchr/testify/require"
)

const progressStreamTestFingerprintHeader = "X-Test-Client-Fingerprint"

type progressStreamDeadlineRecorder struct {
	mu        sync.Mutex
	deadlines []time.Time
}

type progressStreamDeadlineResponseWriter struct {
	header   http.Header
	deadline time.Time
}

func (w *progressStreamDeadlineResponseWriter) Header() http.Header {
	return w.header
}

func (*progressStreamDeadlineResponseWriter) Write(body []byte) (int, error) {
	return len(body), nil
}

func (*progressStreamDeadlineResponseWriter) WriteHeader(int) {}

func (w *progressStreamDeadlineResponseWriter) SetWriteDeadline(
	deadline time.Time,
) error {
	w.deadline = deadline
	return nil
}

func (r *progressStreamDeadlineRecorder) set(deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadlines = append(r.deadlines, deadline)
	return nil
}

func (r *progressStreamDeadlineRecorder) snapshot() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.deadlines...)
}

func newProgressStreamTestHandler(
	lifetime time.Duration,
) *databaseServiceHandler {
	now := time.Now()
	return &databaseServiceHandler{
		bark: &Bark{config: BarkConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}},
		operations: map[string]*operation{
			"running": {
				id:        "running",
				status:    databasev1alpha1.OperationStatus_OPERATION_STATUS_RUNNING,
				startedAt: now,
				updatedAt: now,
			},
		},
		progressStreamLifetime: lifetime,
	}
}

func newProgressStreamTestClient(
	t *testing.T,
	lifetime time.Duration,
) (
	databaseconnect.DatabaseServiceClient,
	*databaseServiceHandler,
	*progressStreamDeadlineRecorder,
) {
	t.Helper()
	h := newProgressStreamTestHandler(lifetime)
	deadlines := &progressStreamDeadlineRecorder{}
	path, handler := databaseconnect.NewDatabaseServiceHandler(h)
	mux := http.NewServeMux()
	mux.Handle(
		path,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fingerprint := r.Header.Get(progressStreamTestFingerprintHeader)
			ctx := withPeerIdentity(r.Context(), peerIdentity{
				Verified:    true,
				Fingerprint: fingerprint,
			})
			ctx = withResponseWriteDeadlineSetter(ctx, deadlines.set)
			handler.ServeHTTP(w, r.WithContext(ctx))
		}),
	)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return databaseconnect.NewDatabaseServiceClient(
		server.Client(),
		server.URL,
	), h, deadlines
}

func progressStreamTestCounts(
	h *databaseServiceHandler,
	fingerprint string,
) (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activeProgressStreams, h.progressStreamsByFingerprint[fingerprint]
}

func TestResponseControllerContextMiddlewareInstallsSetter(t *testing.T) {
	t.Parallel()
	want := time.Now().Add(time.Minute)
	called := false
	handler := responseControllerContextMiddleware(http.HandlerFunc(
		func(_ http.ResponseWriter, r *http.Request) {
			setter, ok := responseWriteDeadlineSetterFromContext(r.Context())
			require.True(t, ok)
			require.NoError(t, setter(want))
			called = true
		},
	))
	w := &progressStreamDeadlineResponseWriter{header: make(http.Header)}
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", nil))
	require.True(t, called)
	require.Equal(t, want, w.deadline)
}

func TestProgressStreamRequiresWriteDeadlineSetter(t *testing.T) {
	t.Parallel()
	handler := newProgressStreamTestHandler(streamProgressMaxLifetime)
	ctx := withPeerIdentity(t.Context(), peerIdentity{
		Verified:    true,
		Fingerprint: "client-a",
	})
	err := handler.StreamOperationProgress(
		ctx,
		connect.NewRequest(
			&databasev1alpha1.StreamOperationProgressRequest{
				OperationId: "running",
			},
		),
		nil,
	)
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	global, clientActive := progressStreamTestCounts(handler, "client-a")
	require.Zero(t, global)
	require.Zero(t, clientActive)
}

func TestProgressStreamRejectsUnsupportedWriteDeadline(t *testing.T) {
	t.Parallel()
	handler := newProgressStreamTestHandler(streamProgressMaxLifetime)
	ctx := withPeerIdentity(t.Context(), peerIdentity{
		Verified:    true,
		Fingerprint: "client-a",
	})
	ctx = withResponseWriteDeadlineSetter(ctx, func(time.Time) error {
		return http.ErrNotSupported
	})
	err := handler.StreamOperationProgress(
		ctx,
		connect.NewRequest(
			&databasev1alpha1.StreamOperationProgressRequest{
				OperationId: "running",
			},
		),
		nil,
	)
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	global, clientActive := progressStreamTestCounts(handler, "client-a")
	require.Zero(t, global)
	require.Zero(t, clientActive)
}

func TestProgressStreamAdmissionCapsEachClient(t *testing.T) {
	t.Parallel()
	client, handler, _ := newProgressStreamTestClient(
		t,
		streamProgressMaxLifetime,
	)
	cleanups := make([]func(), 0, maxClientProgressStreams)
	for range maxClientProgressStreams {
		ctx, cancel := context.WithCancel(t.Context())
		req := connect.NewRequest(
			&databasev1alpha1.StreamOperationProgressRequest{
				OperationId: "running",
			},
		)
		req.Header().Set(progressStreamTestFingerprintHeader, "client-a")
		stream, err := client.StreamOperationProgress(ctx, req)
		require.NoError(t, err)
		require.True(t, stream.Receive())
		cleanups = append(cleanups, func() {
			cancel()
			require.False(t, stream.Receive())
			require.Equal(t, connect.CodeCanceled, connect.CodeOf(stream.Err()))
		})
	}

	req := connect.NewRequest(
		&databasev1alpha1.StreamOperationProgressRequest{
			OperationId: "running",
		},
	)
	req.Header().Set(progressStreamTestFingerprintHeader, "client-a")
	stream, err := client.StreamOperationProgress(t.Context(), req)
	require.NoError(t, err)
	require.False(t, stream.Receive())
	require.Equal(
		t,
		connect.CodeResourceExhausted,
		connect.CodeOf(stream.Err()),
	)

	for _, cleanup := range cleanups {
		cleanup()
	}
	require.Eventually(t, func() bool {
		global, clientActive := progressStreamTestCounts(handler, "client-a")
		return global == 0 && clientActive == 0
	}, time.Second, 10*time.Millisecond)
}

func TestProgressStreamAdmissionCapsGlobalStreams(t *testing.T) {
	t.Parallel()
	handler := newProgressStreamTestHandler(streamProgressMaxLifetime)
	releases := make([]func(), 0, maxProgressStreams)
	for i := range maxProgressStreams {
		ctx := withPeerIdentity(t.Context(), peerIdentity{
			Verified:    true,
			Fingerprint: string(rune(i + 1)),
		})
		release, err := handler.admitProgressStream(ctx)
		require.NoError(t, err)
		releases = append(releases, release)
	}
	ctx := withPeerIdentity(t.Context(), peerIdentity{
		Verified:    true,
		Fingerprint: "overflow",
	})
	_, err := handler.admitProgressStream(ctx)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	for _, release := range releases {
		release()
	}
	global, _ := progressStreamTestCounts(handler, "overflow")
	require.Zero(t, global)
}

func TestProgressStreamLifetimeIsBounded(t *testing.T) {
	t.Parallel()
	const lifetime = 2 * time.Second
	client, handler, deadlines := newProgressStreamTestClient(
		t,
		lifetime,
	)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req := connect.NewRequest(
		&databasev1alpha1.StreamOperationProgressRequest{
			OperationId: "running",
		},
	)
	req.Header().Set(progressStreamTestFingerprintHeader, "client-a")
	stream, err := client.StreamOperationProgress(ctx, req)
	require.NoError(t, err)
	require.True(t, stream.Receive())
	installed := deadlines.snapshot()
	require.NotEmpty(t, installed)
	require.False(t, installed[0].IsZero())
	remaining := time.Until(installed[0])
	require.Positive(t, remaining)
	require.LessOrEqual(t, remaining, lifetime)
	for stream.Receive() {
	}
	require.Equal(
		t,
		connect.CodeDeadlineExceeded,
		connect.CodeOf(stream.Err()),
	)
	require.Eventually(t, func() bool {
		global, clientActive := progressStreamTestCounts(handler, "client-a")
		return global == 0 && clientActive == 0
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		installed := deadlines.snapshot()
		return len(installed) >= 2 && installed[len(installed)-1].IsZero()
	}, time.Second, 10*time.Millisecond)
}
