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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/stretchr/testify/require"
)

// freeHealthPort returns a port nothing is listening on. The probe listener
// cannot take a kernel-assigned 0 the way the metrics listener does, because
// healthPort 0 is the operator's opt-out.
func freeHealthPort(t *testing.T) uint {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, ln.Close())
	require.NoError(t, err)
	parsed, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	return uint(parsed)
}

// TestStartHealthProbeServerServesLiveAndNotReady pins the classification a
// bootstrap has to report: the process is up, so liveness answers 200 and the
// container survives, while readiness answers 503 because nothing here is
// following the chain.
//
// Getting this backwards is what the listener exists to prevent. A bootstrap
// answering /readyz 200 would be put into a load balancer with no data, and
// one answering /health 503 -- or refusing the connection, as a bootstrap with
// no listener does -- is replaced by Swarm or ECS partway through a download
// that takes hours.
func TestStartHealthProbeServerServesLiveAndNotReady(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		BindAddr:   "127.0.0.1",
		HealthPort: freeHealthPort(t),
	}
	server, err := startHealthProbeServer(
		slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, "mithril",
	)
	require.NoError(t, err)
	require.NotNil(t, server)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
	})

	client := http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get("http://" + server.addr + health.PathHealth)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var status health.Status
	require.NoError(t, json.Unmarshal(body, &status))
	require.True(t, status.Live)
	require.False(
		t, status.Ready,
		"a bootstrap follows no chain and must not report ready",
	)
	require.Nil(t, status.TipGapSlots)

	readyResp, err := client.Get("http://" + server.addr + health.PathReady)
	require.NoError(t, err)
	defer readyResp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, readyResp.StatusCode)
}

// TestStartHealthProbeServerDisabledOnZeroPort covers the operator opt-out the
// image's HEALTHCHECK also reads: healthPort 0 means no listener, and the
// probe is skipped rather than bound on an arbitrary port.
func TestStartHealthProbeServerDisabledOnZeroPort(t *testing.T) {
	t.Parallel()

	server, err := startHealthProbeServer(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&config.Config{BindAddr: "127.0.0.1", HealthPort: 0},
		"mithril",
	)
	require.NoError(t, err)
	require.Nil(t, server)
}

// TestMithrilSyncServesHealthProbe is the wiring assertion: the bootstrap
// operation has to start the probe listener, not merely be able to.
//
// `dingo mithril sync` runs as its own process ahead of serve, and both the
// Dockerfile's HEALTHCHECK and docker-compose.yml's healthcheck probe
// DINGO_HEALTH_PORT for the whole time it runs. A bootstrap that starts only
// the metrics and pprof listeners leaves that probe refused, and after the
// start period and its retries the container is replaced mid-download.
//
// Driven to the first local failure -- an unsupported backend, rejected
// without any aggregator, network or database access -- so what is asserted is
// the listener startup that precedes it.
func TestMithrilSyncServesHealthProbe(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	cfg := &config.Config{
		BindAddr:   "127.0.0.1",
		HealthPort: freeHealthPort(t),
	}
	cfg.Mithril.Backend = "not-a-backend"

	err := runMithrilSync(context.Background(), cfg, logger, "preview")
	require.ErrorContains(t, err, "unsupported Mithril backend")
	require.Contains(
		t, logs.String(), "serving health probes on",
		"mithril sync must serve the health probe while it bootstraps",
	)
}
