// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

func TestRewriteVersionHandler(t *testing.T) {
	alphaPath := "/utxorpc.v1alpha.query.QueryService/ReadParams"
	betaPath := "/utxorpc.v1beta.query.QueryService/ReadParams"
	calledPath := ""
	alphaHandler := http.HandlerFunc(
		func(_ http.ResponseWriter, r *http.Request) {
			calledPath = r.URL.Path
		},
	)
	handler := rewriteVersionHandler(alphaHandler, betaPath, alphaPath)

	req := httptest.NewRequest(http.MethodPost, betaPath, nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	require.Equal(t, alphaPath, calledPath)
}

func TestUtxorpc_StartStop(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     "127.0.0.1",
	})
	// NewUtxorpc defaults Port 0 to 9090 for runtime config. Force the
	// test instance back to an ephemeral port to avoid local port conflicts.
	u.config.Port = 0

	startCtx := t.Context()
	err := u.Start(startCtx)
	require.NoError(t, err, "failed to start utxorpc")

	// Server is already listening after Start returns successfully
	// (startServer waits internally for startup or error)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = u.Stop(ctx)
	require.NoError(t, err, "failed to stop utxorpc")
}

// publishServer registers server on u's listener the way Start does, for tests
// that drive Stop against a hand-built server rather than a real Start. The
// bind is marked settled immediately because the caller serves the listener
// itself, so there is none in flight for Stop to wait on.
func publishServer(t *testing.T, u *Utxorpc, server *http.Server) {
	t.Helper()
	published, bindDone, err := u.listener.Publish(
		func() *http.Server { return server },
	)
	require.NoError(t, err)
	require.Same(t, server, published)
	close(bindDone)
}

// TestUtxorpc_StopForcesCloseOnUnboundedStream covers Stop's escalation to
// a hard Close when a client keeps a connection open, standing in for a
// WatchTx/WatchMempool stream that never returns.
func TestUtxorpc_StopForcesCloseOnUnboundedStream(t *testing.T) {
	var logBuf bytes.Buffer
	u := NewUtxorpc(UtxorpcConfig{
		Logger:          slog.New(slog.NewJSONHandler(&logBuf, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		ShutdownTimeout: 50 * time.Millisecond,
	})

	requestReceived := make(chan struct{})
	blockHandler := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		close(requestReceived)
		<-blockHandler
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: mux} //nolint:gosec // test server
	publishServer(t, u, server)
	go server.Serve(ln) //nolint:errcheck // test server
	t.Cleanup(func() { close(blockHandler) })

	client := &http.Client{}
	go func() {
		resp, getErr := client.Get(
			"http://" + ln.Addr().String() + "/",
		) //nolint:noctx
		if getErr == nil {
			resp.Body.Close()
		}
	}()
	<-requestReceived

	stopDone := make(chan error, 1)
	go func() { stopDone <- u.Stop(context.Background()) }()

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not force-close the server after ShutdownTimeout")
	}
	assert.Contains(t, logBuf.String(), "forcing close")
}

// TestUtxorpc_StopObservesCtxCancellation ensures cancellation interrupts a
// slow shutdown instead of waiting for the configured shutdown timeout.
func TestUtxorpc_StopObservesCtxCancellation(t *testing.T) {
	var logBuf bytes.Buffer
	u := NewUtxorpc(UtxorpcConfig{
		Logger:          slog.New(slog.NewJSONHandler(&logBuf, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		ShutdownTimeout: 30 * time.Second,
	})

	requestReceived := make(chan struct{})
	blockHandler := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		close(requestReceived)
		<-blockHandler
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: mux} //nolint:gosec // test server
	publishServer(t, u, server)
	go server.Serve(ln) //nolint:errcheck // test server
	t.Cleanup(func() { close(blockHandler) })

	client := &http.Client{}
	go func() {
		resp, getErr := client.Get(
			"http://" + ln.Addr().String() + "/",
		) //nolint:noctx
		if getErr == nil {
			resp.Body.Close()
		}
	}()
	<-requestReceived

	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- u.Stop(stopCtx) }()

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return promptly after ctx cancellation")
	}
	assert.Contains(t, logBuf.String(), "forcing close")
	assert.Contains(t, logBuf.String(), "cancelled by caller context")
}
