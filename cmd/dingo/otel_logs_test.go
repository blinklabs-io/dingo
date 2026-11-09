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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Not t.Parallel: these tests set OTEL_LOGS_EXPORTER.

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
