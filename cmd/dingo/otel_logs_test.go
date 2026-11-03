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
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Not t.Parallel: these tests set OTEL_LOGS_EXPORTER.

func TestLogExportOffByDefault(t *testing.T) {
	t.Setenv("OTEL_LOGS_EXPORTER", "")
	base := slog.New(slog.NewTextHandler(io.Discard, nil))
	var warn bytes.Buffer
	got, shutdown := withOTLPLogs(
		context.Background(), base, failResource(t), &warn,
	)
	require.Same(t, base, got)
	require.NoError(t, shutdown(context.Background()))
	require.Empty(t, warn.String())
}

func TestLogExportFansOut(t *testing.T) {
	bodies := make(chan []byte, 4)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			bodies <- b
			w.WriteHeader(http.StatusOK)
		},
	))
	t.Cleanup(server.Close)
	t.Setenv("OTEL_LOGS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)

	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))
	var warn bytes.Buffer
	logger, shutdown := withOTLPLogs(
		context.Background(), base, emptyResource, &warn,
	)
	logger.Info("fanout-probe")
	logger.Debug("below-level")
	require.NoError(t, shutdown(context.Background()))

	require.Empty(t, warn.String())
	require.Contains(t, buf.String(), "fanout-probe")
	select {
	case body := <-bodies:
		require.Contains(t, string(body), "fanout-probe")
		require.NotContains(t, string(body), "below-level")
	case <-time.After(testutil.AsyncWait):
		t.Fatal("no log export reached the collector")
	}
}

func TestLogExportInvalidExporterKeepsStdout(t *testing.T) {
	t.Setenv("OTEL_LOGS_EXPORTER", "bogus")
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))
	var warn bytes.Buffer
	logger, shutdown := withOTLPLogs(
		context.Background(), base, emptyResource, &warn,
	)
	logger.Info("still-here")
	require.NoError(t, shutdown(context.Background()))
	require.Contains(t, buf.String(), "still-here")
	require.Contains(t, warn.String(), "bogus")
}

func emptyResource(context.Context) (*resource.Resource, error) {
	return resource.Empty(), nil
}

// failResource fails the test if log export builds a resource it does not
// need.
func failResource(
	t *testing.T,
) func(context.Context) (*resource.Resource, error) {
	return func(context.Context) (*resource.Resource, error) {
		t.Error("resource built while log export is off")
		return resource.Empty(), nil
	}
}

func TestLogExportToleratesPartialResource(t *testing.T) {
	t.Setenv("OTEL_LOGS_EXPORTER", "console")
	var warn bytes.Buffer
	base := slog.New(slog.NewTextHandler(io.Discard, nil))
	logger, shutdown := withOTLPLogs(
		context.Background(),
		base,
		func(context.Context) (*resource.Resource, error) {
			return resource.Empty(), resource.ErrPartialResource
		},
		&warn,
	)
	require.NoError(t, shutdown(context.Background()))
	require.NotSame(t, base, logger)
	require.Contains(t, warn.String(), "OTEL_RESOURCE_ATTRIBUTES")
}
