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

package node

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

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/health"
)

// healthProbeResponse mirrors health.Status as it arrives over the wire, so
// these tests assert the served JSON rather than the in-process struct.
type healthProbeResponse struct {
	Status      string  `json:"status"`
	Live        bool    `json:"live"`
	Ready       bool    `json:"ready"`
	Reason      string  `json:"reason"`
	TipGapSlots *uint64 `json:"tipGapSlots"`
}

// startHealthListener starts the health listener exactly as Run does --
// newHealthServer, then serveAuxiliaryListener in its own goroutine -- and
// returns its base URL. Requests in these tests therefore traverse the real
// net/http server and the real mux, not a handler called directly.
func startHealthListener(
	t *testing.T,
	cfg *config.Config,
	tipGap health.TipGapFunc,
) string {
	t.Helper()

	cfg.BindAddr = "127.0.0.1"
	cfg.HealthPort = freeTCPPort(t)

	srv := newHealthServer(cfg, tipGap)
	if srv == nil {
		t.Fatal("expected an enabled health listener")
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	go serveAuxiliaryListener("health", srv, logger)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	base := "http://" + srv.Addr
	waitForListener(t, base+health.PathLive)
	return base
}

func freeTCPPort(t *testing.T) uint {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %s", err)
	}
	defer l.Close()
	_, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split port: %s", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		t.Fatalf("parse port: %s", err)
	}
	return uint(port)
}

func waitForListener(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("health listener never accepted a connection at %s", url)
}

func getHealth(
	t *testing.T,
	url string,
) (int, healthProbeResponse) {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("GET %s: %s", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %s", url, err)
	}
	var decoded healthProbeResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode %s body %q: %s", url, body, err)
	}
	return resp.StatusCode, decoded
}

// TestHealthListenerServesInCoreModeWithAPIsDisabled is the negative case the
// issue turns on: the shipped docker-compose.yml runs the default `core`
// storage mode with no API plugin configured, and all three API listeners are
// gated on storageMode.IsAPI(). A probe wired the same way would be inert in
// exactly that configuration. The node here is a real *dingo.Node built by the
// production composition path with no API plugins and core storage.
func TestHealthListenerServesInCoreModeWithAPIsDisabled(t *testing.T) {
	t.Parallel()

	// Start from the shipped defaults (the plugin selections dingo.New
	// validates), then apply exactly what the shipped docker-compose.yml
	// runs: core storage, and no API listener configured at all.
	base := *config.GetConfig()
	cfg := &base
	cfg.Network = "preview"
	cfg.StorageMode = "core"
	cfg.Plugins.API.Blockfrost.Config = map[string]any{"port": 0}
	cfg.Plugins.API.Mesh.Config = map[string]any{"port": 0}
	cfg.Plugins.API.Utxorpc.Config = map[string]any{"port": 0}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	listeners := []dingo.ListenerConfig{
		{
			ListenNetwork: "tcp",
			ListenAddress: net.JoinHostPort(
				"127.0.0.1",
				strconv.FormatUint(uint64(freeTCPPort(t)), 10),
			),
		},
	}
	node, err := dingo.New(
		buildDingoConfig(
			cfg,
			logger,
			nil,
			listeners,
			false,
			dingo.StorageModeCore,
			30*time.Second,
			chainsync.DefaultStallTimeout,
			chainsync.HeaderSyncStrategyPrimary,
		),
	)
	if err != nil {
		t.Fatalf("build node: %s", err)
	}
	t.Cleanup(func() { _ = node.Stop() })

	baseURL := startHealthListener(t, cfg, node.TipGapSlots)

	for _, path := range []string{health.PathHealth, health.PathLive} {
		code, body := getHealth(t, baseURL+path)
		if code != http.StatusOK {
			t.Fatalf(
				"%s in core mode with APIs disabled = %d, want 200",
				path,
				code,
			)
		}
		if !body.Live {
			t.Fatalf("%s reported live=false: %+v", path, body)
		}
	}

	// A node that has never seen a slot tick has no chain tip, so readiness
	// must refuse rather than default to ready.
	code, body := getHealth(t, baseURL+health.PathReady)
	if code != http.StatusServiceUnavailable {
		t.Fatalf(
			"%s for a node with no tip = %d, want 503 (body %+v)",
			health.PathReady,
			code,
			body,
		)
	}
	if body.Ready {
		t.Fatalf("readiness true for a node with no tip: %+v", body)
	}
	if body.Reason == "" {
		t.Fatalf("expected a reason for the unready verdict: %+v", body)
	}
}

// TestHealthListenerReportsUnreadyWhenTipFrozen covers the condition a probe
// exists to catch: the process is up and answering, but its tip has stopped
// advancing. Liveness must stay 200 (a restart does not repair a wedged
// fetch, and a restart loop would destroy the evidence), while readiness must
// fail so an orchestrator drains traffic.
func TestHealthListenerReportsUnreadyWhenTipFrozen(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		HealthReadyGapSlots: 1000,
	}
	frozen := func() (uint64, bool) { return 5000, true }
	base := startHealthListener(t, cfg, frozen)

	code, body := getHealth(t, base+health.PathReady)
	if code != http.StatusServiceUnavailable {
		t.Fatalf(
			"%s with a 5000-slot tip gap = %d, want 503 (body %+v)",
			health.PathReady,
			code,
			body,
		)
	}
	if body.Ready {
		t.Fatalf("readiness true with a 5000-slot tip gap: %+v", body)
	}
	if body.TipGapSlots == nil || *body.TipGapSlots != 5000 {
		t.Fatalf("expected the observed tip gap in the body: %+v", body)
	}

	code, body = getHealth(t, base+health.PathLive)
	if code != http.StatusOK {
		t.Fatalf(
			"%s with a frozen tip = %d, want 200: liveness must not "+
				"restart-loop a node a restart cannot repair",
			health.PathLive,
			code,
		)
	}
	if body.Ready {
		t.Fatalf("liveness body must still report ready=false: %+v", body)
	}
}

// TestHealthListenerReportsReadyWithinTolerance is the counterpart: a probe
// that can only ever answer 503 is as useless as one that can only answer 200.
func TestHealthListenerReportsReadyWithinTolerance(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		HealthReadyGapSlots: 1000,
	}
	caughtUp := func() (uint64, bool) { return 12, true }
	base := startHealthListener(t, cfg, caughtUp)

	code, body := getHealth(t, base+health.PathReady)
	if code != http.StatusOK {
		t.Fatalf(
			"%s with a 12-slot tip gap = %d, want 200 (body %+v)",
			health.PathReady,
			code,
			body,
		)
	}
	if !body.Ready {
		t.Fatalf("readiness false with a 12-slot tip gap: %+v", body)
	}
}

// A node that is legitimately catching up must not be killed: the gap is
// enormous, so readiness refuses, but liveness stays 200.
func TestHealthListenerStaysLiveDuringInitialSync(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{HealthReadyGapSlots: 1000}
	syncing := func() (uint64, bool) { return 90_000_000, true }
	base := startHealthListener(t, cfg, syncing)

	if code, body := getHealth(t, base+health.PathHealth); code != http.StatusOK {
		t.Fatalf(
			"%s during sync = %d, want 200 (%+v)",
			health.PathHealth,
			code,
			body,
		)
	}
	if code, _ := getHealth(t, base+health.PathReady); code != http.StatusServiceUnavailable {
		t.Fatalf("%s during sync = %d, want 503", health.PathReady, code)
	}
}

// The listener is opt-out, and opting out must not leave a half-built server.
func TestNewHealthServerDisabledOnZeroPort(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{BindAddr: "127.0.0.1", HealthPort: 0}
	if srv := newHealthServer(cfg, nil); srv != nil {
		t.Fatalf("healthPort 0 must disable the listener, got %+v", srv)
	}
}

// The health listener binds BindAddr -- the address the relay and metrics
// listeners use -- and not the API listeners' own bind address. A kubelet or
// load-balancer probe reaches the container from outside, so a loopback
// default would fail those closed.
func TestHealthServerBindsPublicBindAddr(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{BindAddr: "0.0.0.0", HealthPort: 12799}
	srv := newHealthServer(cfg, nil)
	if srv == nil {
		t.Fatal("expected an enabled health listener")
	}
	if got, want := srv.Addr, "0.0.0.0:12799"; got != want {
		t.Fatalf("health listener address = %q, want %q", got, want)
	}
}
