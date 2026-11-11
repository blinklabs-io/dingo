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

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

func TestMithrilMetricsUsesDedicatedLoopbackBind(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"", "127.0.0.1"} {
		t.Run(host, func(t *testing.T) {
			t.Parallel()
			cfg := &config.Config{
				BindAddr:        "192.0.2.1",
				MetricsBindAddr: host,
				MetricsPort:     0,
			}
			server, err := startMithrilMetricsServer(
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				cfg,
				http.NotFoundHandler(),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					time.Second,
				)
				defer cancel()
				require.NoError(t, server.Shutdown(ctx))
			})
			actual, _, err := net.SplitHostPort(server.addr)
			require.NoError(t, err)
			require.Equal(t, "127.0.0.1", actual)
		})
	}
}
