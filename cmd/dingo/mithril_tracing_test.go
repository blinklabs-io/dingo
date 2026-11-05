// Copyright 2024 Blink Labs Software
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

package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// Not t.Parallel: replaces the process-wide tracer provider.
func TestStandaloneTracingCoversMithrilBootstrap(t *testing.T) {
	paths := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			paths <- r.URL.Path
			w.WriteHeader(http.StatusOK)
		},
	))
	t.Cleanup(server.Close)
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	off := &config.Config{}
	startStandaloneTracing(context.Background(), off, logger)()
	require.Same(t, previous, otel.GetTracerProvider())

	on := &config.Config{
		Tracing:            true,
		TracingEndpoint:    server.URL,
		TracingSampleRatio: 1,
	}
	stop := startStandaloneTracing(context.Background(), on, logger)
	_, span := tracing.Start(context.Background(), "mithril.bootstrap")
	span.End()
	stop()

	select {
	case path := <-paths:
		require.Equal(t, "/v1/traces", path)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("Mithril bootstrap span was not exported")
	}
}
