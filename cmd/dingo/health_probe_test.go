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

// reservedHealthListener binds a loopback port and returns the live listener
// with the port it bound. The listener is never released: the caller hands it
// to the probe, which serves on the socket the test already owns.
//
// Returning a port number from a listener that has been closed again would be
// a race, not a reservation. healthPort 0 is the operator's opt-out rather
// than "pick one", so the probe cannot take a kernel-assigned port the way
// the metrics listener does, and in the gap between releasing the number and
// rebinding it anything asking the kernel for an arbitrary port takes it --
// including the mithril metrics listener started moments later in this same
// process, which is exactly what happens when MetricsPort is 0.
func reservedHealthListener(t *testing.T) (net.Listener, uint) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	parsed, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	return ln, uint(parsed)
}

// TestServeHealthProbeServesLiveAndNotReady pins the classification a
// bootstrap has to report: the process is up, so liveness answers 200 and the
// container survives, while readiness answers 503 because nothing here is
// following the chain.
//
// Getting this backwards is what the listener exists to prevent. A bootstrap
// answering /readyz 200 would be put into a load balancer with no data, and
// one answering /health 503 -- or refusing the connection, as a bootstrap with
// no listener does -- is replaced by Swarm or ECS partway through a download
// that takes hours.
func TestServeHealthProbeServesLiveAndNotReady(t *testing.T) {
	t.Parallel()

	listener, port := reservedHealthListener(t)
	cfg := &config.Config{
		BindAddr:   "127.0.0.1",
		HealthPort: port,
	}
	server, err := serveHealthProbe(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg,
		"mithril",
		listener,
	)
	require.NoError(t, err)
	require.NotNil(t, server)
	require.Equal(t, listener.Addr().String(), server.addr)
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

// TestServeHealthProbeDisabledOnZeroPort covers the operator opt-out the
// image's HEALTHCHECK also reads: healthPort 0 means no listener, and the
// probe is skipped rather than bound on an arbitrary port.
func TestServeHealthProbeDisabledOnZeroPort(t *testing.T) {
	t.Parallel()

	server, err := serveHealthProbe(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&config.Config{BindAddr: "127.0.0.1", HealthPort: 0},
		"mithril",
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, server)
}

// TestServeHealthProbeClosesSuppliedListenerWhenDisabled covers the one way
// handing a live listener over could leak one: healthPort 0 still disables
// the probe, so the socket the caller bound has to be released rather than
// left bound for the life of the process.
func TestServeHealthProbeClosesSuppliedListenerWhenDisabled(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server, err := serveHealthProbe(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&config.Config{BindAddr: "127.0.0.1", HealthPort: 0},
		"mithril",
		listener,
	)
	require.NoError(t, err)
	require.Nil(t, server)

	_, acceptErr := listener.Accept()
	require.ErrorIs(t, acceptErr, net.ErrClosed)
}

// TestServeHealthProbeReportsBindFailure is the operator's collision
// case: the configured health port is already taken, so the probe cannot
// come up and the caller is told which address failed rather than being left
// to infer it.
//
// It also pins that the no-listener path binds cfg's address and not some
// other one -- the port in the error is the port the test occupied.
func TestServeHealthProbeReportsBindFailure(t *testing.T) {
	t.Parallel()

	occupied, port := reservedHealthListener(t)

	server, err := serveHealthProbe(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		&config.Config{BindAddr: "127.0.0.1", HealthPort: port},
		"mithril",
		nil,
	)
	require.Nil(t, server)
	require.ErrorContains(
		t, err, "starting health listener on "+occupied.Addr().String(),
	)
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
	listener, port := reservedHealthListener(t)
	cfg := &config.Config{
		BindAddr:   "127.0.0.1",
		HealthPort: port,
	}
	cfg.Mithril.Backend = "not-a-backend"

	err := runMithrilSync(
		context.Background(), cfg, logger, "preview", listener,
	)
	require.ErrorContains(t, err, "unsupported Mithril backend")
	require.Contains(
		t, logs.String(),
		"serving health probes on "+listener.Addr().String(),
		"mithril sync must serve the health probe while it bootstraps",
	)
}
