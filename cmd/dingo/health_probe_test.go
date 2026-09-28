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
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/stretchr/testify/require"
)

// heldHealthProbe binds a loopback port and returns a probe on the live
// listener, which is never released: serveHealthProbe answers on the socket
// the test already owns, so no port number is ever handed back to the kernel
// and rebound.
//
// Returning a port number from a listener that has been closed again would
// be a race, not a reservation: in the gap anything asking the kernel for an
// arbitrary port takes it -- including the mithril metrics listener started
// moments later in this same process, which is what happens when MetricsPort
// is 0.
func heldHealthProbe(t *testing.T, cfg *config.Config) *boundHealthProbe {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	server := node.NewHealthServer(cfg, nil)
	require.NotNil(t, server)
	return &boundHealthProbe{server: server, listener: ln}
}

// bindProbeOnFreePort has bindHealthProbe bind a loopback port nothing holds,
// and reports the port it was asked for alongside the probe.
//
// A port number is not a reservation -- the premise of this whole change --
// so the pick can be taken before bindHealthProbe binds it, by anything in
// the process asking the kernel for an arbitrary port. Testing the bind
// against a configured port needs a candidate all the same, so the loss is
// retried rather than treated as a failure of the code under test; a bind
// that fails for any other reason is reported on the last attempt.
func bindProbeOnFreePort(t *testing.T) (*boundHealthProbe, uint) {
	t.Helper()

	const attempts = 20
	var lastErr error
	for range attempts {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		_, port, err := net.SplitHostPort(ln.Addr().String())
		require.NoError(t, err)
		require.NoError(t, ln.Close())
		parsed, err := strconv.ParseUint(port, 10, 16)
		require.NoError(t, err)

		probe, err := bindHealthProbe(
			&config.Config{BindAddr: "127.0.0.1", HealthPort: uint(parsed)},
		)
		if err == nil {
			require.NotNil(t, probe)
			t.Cleanup(func() { _ = probe.listener.Close() })
			return probe, uint(parsed)
		}
		lastErr = err
	}
	require.NoErrorf(
		t, lastErr,
		"no free loopback port survived the pick in %d attempts", attempts,
	)
	return nil, 0
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

	cfg := &config.Config{BindAddr: "127.0.0.1", HealthPort: 12799}
	probe := heldHealthProbe(t, cfg)
	server := serveHealthProbe(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"mithril",
		probe,
	)
	require.NotNil(t, server)
	require.Equal(t, probe.listener.Addr().String(), server.addr)
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

// TestServeHealthProbeSkipsNilProbe is the disabled and failed-bind case
// arriving at the serving half: there is no socket, so nothing is served and
// the caller is handed no server to shut down.
func TestServeHealthProbeSkipsNilProbe(t *testing.T) {
	t.Parallel()

	require.Nil(t, serveHealthProbe(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"mithril",
		nil,
	))
}

// TestBindHealthProbeBindsConfiguredPort is the path `dingo mithril sync`
// takes: the configured port is bound by the command, and the socket it
// returns is the one the probe will serve on.
func TestBindHealthProbeBindsConfiguredPort(t *testing.T) {
	t.Parallel()

	probe, port := bindProbeOnFreePort(t)
	require.Equal(
		t,
		net.JoinHostPort("127.0.0.1", strconv.FormatUint(uint64(port), 10)),
		probe.listener.Addr().String(),
	)
}

// TestBindHealthProbeDisabledOnZeroPort covers the operator opt-out the
// image's HEALTHCHECK also reads: healthPort 0 means no listener, and the
// probe is skipped rather than bound on an arbitrary port.
func TestBindHealthProbeDisabledOnZeroPort(t *testing.T) {
	t.Parallel()

	probe, err := bindHealthProbe(
		&config.Config{BindAddr: "127.0.0.1", HealthPort: 0},
	)
	require.NoError(t, err)
	require.Nil(t, probe)
}

// TestBindHealthProbeReportsBindFailure is the operator's collision case: the
// configured health port is already taken, so the probe cannot come up and
// the caller is told which address failed rather than being left to infer it.
//
// It also pins that the bind uses cfg's address and not some other one -- the
// port in the error is the port the test occupied.
func TestBindHealthProbeReportsBindFailure(t *testing.T) {
	t.Parallel()

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })
	_, port, err := net.SplitHostPort(occupied.Addr().String())
	require.NoError(t, err)
	parsed, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)

	probe, err := bindHealthProbe(
		&config.Config{BindAddr: "127.0.0.1", HealthPort: uint(parsed)},
	)
	require.Nil(t, probe)
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
	cfg := &config.Config{BindAddr: "127.0.0.1", HealthPort: 12799}
	cfg.Mithril.Backend = "not-a-backend"
	probe := heldHealthProbe(t, cfg)

	err := runMithrilSync(
		context.Background(), cfg, logger, "preview", probe,
	)
	require.ErrorContains(t, err, "unsupported Mithril backend")
	require.Contains(
		t, logs.String(),
		"serving health probes on "+probe.listener.Addr().String(),
		"mithril sync must serve the health probe while it bootstraps",
	)
}
