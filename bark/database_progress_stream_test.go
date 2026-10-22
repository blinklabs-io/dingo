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
	"testing"
	"time"

	"connectrpc.com/connect"
	databasev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/database"
	databaseconnect "github.com/blinklabs-io/bark/proto/v1alpha1/database/databasev1alpha1connect"
	"github.com/stretchr/testify/require"
)

const progressStreamTestFingerprintHeader = "X-Test-Client-Fingerprint"

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
) {
	t.Helper()
	h := newProgressStreamTestHandler(lifetime)
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
			handler.ServeHTTP(w, r.WithContext(ctx))
		}),
	)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return databaseconnect.NewDatabaseServiceClient(
		server.Client(),
		server.URL,
	), h
}

func progressStreamTestCounts(
	h *databaseServiceHandler,
	fingerprint string,
) (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.activeProgressStreams, h.progressStreamsByFingerprint[fingerprint]
}

func TestProgressStreamAdmissionCapsEachClient(t *testing.T) {
	t.Parallel()
	client, handler := newProgressStreamTestClient(
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
	client, handler := newProgressStreamTestClient(t, 25*time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
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
	require.False(t, stream.Receive())
	require.Equal(
		t,
		connect.CodeDeadlineExceeded,
		connect.CodeOf(stream.Err()),
	)
	require.Eventually(t, func() bool {
		global, clientActive := progressStreamTestCounts(handler, "client-a")
		return global == 0 && clientActive == 0
	}, time.Second, 10*time.Millisecond)
}
