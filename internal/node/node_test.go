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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/health"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// sentinelConfig builds a config whose every secret-bearing field carries a
// distinctive sentinel value, alongside non-secret values that must survive
// redaction.
func sentinelConfig() (*config.Config, []string) {
	cfg := &config.Config{
		Network:      "preview",
		DatabasePath: "/var/lib/dingo",
		KoiosParity: config.KoiosParityConfig{
			Enabled: true,
			APIKey:  "SENTINEL-KOIOS-API-KEY",
		},
		BarkBaseUrl: "https://bark:SENTINEL-BARK-PASSWORD@bark.example/api",
		Mithril: config.MithrilConfig{
			AggregatorURL: "https://aggregator.example/aggregator" +
				"?apiKey=SENTINEL-MITHRIL-KEY",
		},
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Metadata: hostplugin.Selection{
					Provider: "postgres",
					Config: map[string]any{
						"host":     "db.example",
						"user":     "dingo",
						"password": "SENTINEL-PG-PASSWORD",
						"dsn": "postgres://dingo:SENTINEL-DSN-PASSWORD" +
							"@db.example:5432/dingo?sslmode=require",
						"futureKey": "SENTINEL-UNKNOWN-PROVIDER-KEY",
					},
				},
			},
		},
	}
	return cfg, []string{
		"SENTINEL-KOIOS-API-KEY",
		"SENTINEL-BARK-PASSWORD",
		"SENTINEL-MITHRIL-KEY",
		"SENTINEL-PG-PASSWORD",
		"SENTINEL-DSN-PASSWORD",
		"SENTINEL-UNKNOWN-PROVIDER-KEY",
	}
}

// TestLogStartupConfigRedactsSecrets covers the startup debug log that
// records the effective configuration: no secret-bearing value may reach it.
func TestLogStartupConfigRedactsSecrets(t *testing.T) {
	t.Parallel()

	cfg, sentinels := sentinelConfig()
	for _, handler := range []struct {
		name string
		make func(*bytes.Buffer) slog.Handler
	}{
		{"text", func(b *bytes.Buffer) slog.Handler {
			return slog.NewTextHandler(b, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})
		}},
		{"json", func(b *bytes.Buffer) slog.Handler {
			return slog.NewJSONHandler(b, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})
		}},
	} {
		t.Run(handler.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logStartupConfig(slog.New(handler.make(&buf)), cfg)
			rendered := buf.String()
			if rendered == "" {
				t.Fatal("startup config log produced no output")
			}
			for _, sentinel := range sentinels {
				if strings.Contains(rendered, sentinel) {
					t.Errorf(
						"rendered log leaks %s: %s",
						sentinel,
						rendered,
					)
				}
			}
			// Negative case: redaction must not blank the whole record.
			for _, want := range []string{
				"preview",
				"/var/lib/dingo",
				"db.example",
				"postgres",
				"sslmode=require",
			} {
				if !strings.Contains(rendered, want) {
					t.Errorf(
						"rendered log dropped non-secret %q: %s",
						want,
						rendered,
					)
				}
			}
		})
	}
}

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
// NewHealthServer, then serveAuxiliaryListener in its own goroutine -- and
// returns its base URL. Requests in these tests therefore traverse the real
// net/http server and the real mux, not a handler called directly.
func startHealthListener(
	t *testing.T,
	cfg *config.Config,
	tipGap health.TipGapFunc,
) string {
	t.Helper()

	cfg.BindAddr = "127.0.0.1"
	listener, port := reservedTCPListener(t, "127.0.0.1")
	cfg.HealthPort = port

	srv := NewHealthServer(cfg, tipGap)
	if srv == nil {
		t.Fatal("expected an enabled health listener")
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	go serveAuxiliaryListenerOn("health", srv, listener, logger)
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

// reservedTCPListener binds a loopback port on host and returns the live
// listener with the port it bound. The listener stays bound for the whole
// test and is handed to the server under test.
//
// Binding, closing and returning the number instead would be a race rather
// than a reservation: any other bind in the process -- most often a request
// for a kernel-assigned port -- can take it in the gap, and the server then
// fails to come up. The health port has no kernel-assigned form to fall back
// on, because 0 is the operator's opt-out.
func reservedTCPListener(t *testing.T, host string) (net.Listener, uint) {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatalf("reserve port on %s: %s", host, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	_, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split port: %s", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("parse port: %s", err)
	}
	return l, uint(port)
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

// TestHealthListenerServesInCoreModeWithAPIsDisabled verifies that health
// probes remain available in core storage mode, where client API listeners
// are disabled. The node is built through the production composition path
// with no API plugins and core storage.
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
			// Kernel-assigned: the node binds it once and nothing
			// here reads the port back.
			ListenAddress: "127.0.0.1:0",
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
	if srv := NewHealthServer(cfg, nil); srv != nil {
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
	srv := NewHealthServer(cfg, nil)
	if srv == nil {
		t.Fatal("expected an enabled health listener")
	}
	if got, want := srv.Addr, "0.0.0.0:12799"; got != want {
		t.Fatalf("health listener address = %q, want %q", got, want)
	}
}

// An IPv6 bindAddr has to be bracketed: "%s:%d" would produce "::1:12799",
// which net.Listen rejects, and the probes would silently never come up.
func TestHealthServerBracketsIPv6BindAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bindAddr string
		want     string
	}{
		{bindAddr: "0.0.0.0", want: "0.0.0.0:12799"},
		{bindAddr: "127.0.0.1", want: "127.0.0.1:12799"},
		{bindAddr: "::", want: "[::]:12799"},
		{bindAddr: "::1", want: "[::1]:12799"},
	}

	for _, test := range tests {
		t.Run(test.bindAddr, func(t *testing.T) {
			t.Parallel()
			cfg := &config.Config{
				BindAddr:   test.bindAddr,
				HealthPort: 12799,
			}
			srv := NewHealthServer(cfg, nil)
			if srv == nil {
				t.Fatal("expected an enabled health listener")
			}
			if srv.Addr != test.want {
				t.Fatalf(
					"health listener address = %q, want %q",
					srv.Addr,
					test.want,
				)
			}
		})
	}
}

// The address must not merely look right: it has to bind and answer.
func TestHealthListenerBindsIPv6Loopback(t *testing.T) {
	t.Parallel()

	// One bind does both jobs: it establishes that the host has an IPv6
	// loopback at all, and it holds the port the server is about to serve
	// on.
	listener, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback on this host: %s", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	_, portStr, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split port: %s", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("parse port: %s", err)
	}

	cfg := &config.Config{HealthPort: uint(port), BindAddr: "::1"}
	srv := NewHealthServer(cfg, func() (uint64, bool) { return 4, true })
	if srv == nil {
		t.Fatal("expected an enabled health listener")
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	go serveAuxiliaryListenerOn("health", srv, listener, logger)
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
	if code, body := getHealth(t, base+health.PathReady); code != http.StatusOK {
		t.Fatalf(
			"%s over IPv6 = %d, want 200 (%+v)",
			health.PathReady,
			code,
			body,
		)
	}
}

func TestWaitForSignalOrErrorPrefersQueuedError(t *testing.T) {
	t.Parallel()

	signalCtx, signalCtxStop := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	expectedErr := errors.New("metrics server: bind failed")

	errChan <- expectedErr
	signalCtxStop()

	err, signaled := waitForSignalOrError(signalCtx, errChan)
	if signaled {
		t.Fatal("expected queued error to win over signal shutdown")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

// A bind failure on a non-essential observability listener (metrics, pprof
// or the health probe) must be logged and non-fatal: Run gets no listener
// back and carries on, rather than an error that would take down an
// otherwise-healthy node.
func TestBindAuxiliaryListenerBindFailureIsNonFatal(t *testing.T) {
	t.Parallel()

	// Occupy a port so the auxiliary listener cannot bind.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to occupy port: %s", err)
	}
	defer occupied.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	srv := &http.Server{
		Addr:              occupied.Addr().String(),
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: time.Second,
	}

	if listener := bindAuxiliaryListener("metrics", srv, logger); listener != nil {
		listener.Close()
		t.Fatal(
			"bindAuxiliaryListener returned a listener for an occupied port",
		)
	}
	if logged := buf.String(); !strings.Contains(logged, "metrics") {
		t.Fatalf(
			"expected a log mentioning the metrics listener, got: %q",
			logged,
		)
	}
}

// The bind Run performs hands the socket straight to serveAuxiliaryListenerOn,
// so a free port yields a listener bound to the server's own address.
func TestBindAuxiliaryListenerBindsServerAddress(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	srv := &http.Server{
		Addr:              "127.0.0.1:0",
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: time.Second,
	}

	listener := bindAuxiliaryListener("metrics", srv, logger)
	if listener == nil {
		t.Fatalf("expected a bound listener, got none; logs: %q", buf.String())
	}
	defer listener.Close()
	if host, _, err := net.SplitHostPort(listener.Addr().String()); err != nil ||
		host != "127.0.0.1" {
		t.Fatalf("listener bound to %s, want 127.0.0.1", listener.Addr())
	}
}

func TestPprofDebugServerUsesDedicatedBindAddress(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		BindAddr:      "0.0.0.0",
		DebugBindAddr: "127.0.0.1",
		DebugPort:     6060,
	}

	srv := newPprofDebugServer(cfg)
	if srv == nil {
		t.Fatal("expected enabled pprof debug server")
	}
	if got, want := srv.Addr, "127.0.0.1:6060"; got != want {
		t.Fatalf("pprof address = %q, want %q", got, want)
	}

	cfg.DebugBindAddr = "0.0.0.0"
	srv = newPprofDebugServer(cfg)
	if srv == nil {
		t.Fatal("expected explicitly exposed pprof debug server")
	}
	if got, want := srv.Addr, "0.0.0.0:6060"; got != want {
		t.Fatalf("explicit wildcard pprof address = %q, want %q", got, want)
	}
}

func TestMetricsServerUsesDedicatedBindAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		bindAddr string
		want     string
	}{
		{"loopback", "127.0.0.1", "127.0.0.1:12798"},
		{"default loopback", "", "127.0.0.1:12798"},
		{"wildcard opt-in", "0.0.0.0", "0.0.0.0:12798"},
		{"remote address opt-in", "10.0.0.5", "10.0.0.5:12798"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// BindAddr stays wildcard to show the metrics listener does
			// not inherit it.
			srv := newMetricsServer(&config.Config{
				BindAddr:        "0.0.0.0",
				MetricsBindAddr: tc.bindAddr,
				MetricsPort:     12798,
			})
			if srv.Addr != tc.want {
				t.Fatalf("metrics address = %q, want %q", srv.Addr, tc.want)
			}
		})
	}
}

func TestMetricsServerDisabledByZeroPort(t *testing.T) {
	t.Parallel()

	srv := newMetricsServer(&config.Config{
		MetricsBindAddr: "0.0.0.0",
		MetricsPort:     0,
	})
	if srv != nil {
		t.Fatalf("metricsPort 0 must start no listener, got %q", srv.Addr)
	}
}

func TestShutdownNodeResourcesSkipsDisabledMetrics(t *testing.T) {
	t.Parallel()

	stopped := false
	err := shutdownNodeResources(
		optionalShutdown(newMetricsServer(&config.Config{})),
		nil,
		nil,
		func() error {
			stopped = true
			return nil
		},
		time.Second,
	)
	if err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
	if !stopped {
		t.Fatal("node must stop when metrics are disabled")
	}
}

func TestWaitForSignalOrErrorReturnsSignalWithoutQueuedError(t *testing.T) {
	t.Parallel()

	signalCtx, signalCtxStop := context.WithCancel(context.Background())
	errChan := make(chan error, 1)

	signalCtxStop()

	err, signaled := waitForSignalOrError(signalCtx, errChan)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !signaled {
		t.Fatal("expected signal shutdown when no error is queued")
	}
}

func TestShutdownNodeResourcesAggregatesErrors(t *testing.T) {
	t.Parallel()

	metricsErr := errors.New("metrics failed")
	nodeErr := errors.New("node failed")

	err := shutdownNodeResources(
		func(context.Context) error {
			return metricsErr
		},
		nil,
		nil,
		func() error {
			return nodeErr
		},
		5*time.Second,
	)
	if err == nil {
		t.Fatal("expected shutdown error")
	}
	if !errors.Is(err, metricsErr) {
		t.Fatalf("expected metrics shutdown error to be joined: %v", err)
	}
	if !errors.Is(err, nodeErr) {
		t.Fatalf("expected node stop error to be joined: %v", err)
	}
	if !strings.Contains(
		err.Error(),
		"metrics server shutdown: metrics failed",
	) {
		t.Fatalf("expected metrics shutdown context in error: %v", err)
	}
	if !strings.Contains(err.Error(), "node stop: node failed") {
		t.Fatalf("expected node stop context in error: %v", err)
	}
}

func TestShutdownNodeResourcesReturnsNilWithoutErrors(t *testing.T) {
	t.Parallel()

	err := shutdownNodeResources(
		func(context.Context) error {
			return nil
		},
		nil,
		nil,
		func() error {
			return nil
		},
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("expected nil shutdown error, got %v", err)
	}
}

// TestBuildDingoConfigWiresAPIConfig asserts that a loaded
// internal/config.Config's api.tls policy (as set via YAML/env/CLI)
// actually reaches the dingo.Config that Run() hands to dingo.New() --
// regression test for the top-level API security defaults
// being silently dropped because Run's real composition call never invoked
// dingo.WithAPIConfig.
func TestBuildDingoConfigWiresAPIConfig(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		API: config.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/shared/cert.pem"),
				KeyFilePath:  new("/shared/key.pem"),
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	got := built.APIConfig()
	if got.TLS.Mode == nil || *got.TLS.Mode != "server" {
		t.Fatalf("expected api.tls.mode to flow through, got %+v", got.TLS)
	}
	if got.TLS.CertFilePath == nil ||
		*got.TLS.CertFilePath != "/shared/cert.pem" {
		t.Fatalf(
			"expected api.tls.certFilePath to flow through, got %+v",
			got.TLS,
		)
	}
}

// TestBuildDingoConfigWiresBarkOperatorFingerprints pins the production
// composition boundary between loaded YAML/env/CLI configuration and the root
// configuration that Run passes to dingo.New and, in turn, Bark.
func TestBuildDingoConfigWiresBarkOperatorFingerprints(t *testing.T) {
	t.Parallel()

	want := []string{
		strings.Repeat("ab", 32),
		strings.Repeat("cd", 32),
	}
	wantLifecycle := []string{strings.Repeat("ef", 32)}
	cfg := &config.Config{
		BarkOperatorCertificateFingerprints:          want,
		BarkLifecycleEnabled:                         true,
		BarkLifecycleOperatorCertificateFingerprints: wantLifecycle,
	}

	built := buildDingoConfig(
		cfg,
		slog.New(slog.NewTextHandler(new(bytes.Buffer), nil)),
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.BarkOperatorCertificateFingerprints(); !slices.Equal(
		got,
		want,
	) {
		t.Fatalf(
			"expected Bark operator fingerprints to flow through, got %v",
			got,
		)
	}
	if !built.BarkLifecycleEnabled() {
		t.Fatal("expected Bark lifecycle service to remain enabled")
	}
	if got := built.BarkLifecycleOperatorCertificateFingerprints(); !slices.Equal(
		got,
		wantLifecycle,
	) {
		t.Fatalf(
			"expected Bark lifecycle operator fingerprints to flow through, got %v",
			got,
		)
	}
}

// TestBuildDingoConfigWiresMidnightServerPolicy pins the composition boundary
// between YAML/env/CLI configuration and the root node configuration used by
// both initial startup and live API reinitialization.
func TestBuildDingoConfigWiresMidnightServerPolicy(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		Midnight: config.MidnightConfig{
			Enabled:                     true,
			ServerEnabled:               true,
			ReflectionEnabled:           true,
			Port:                        50052,
			Host:                        "127.0.0.2",
			CNightPolicyID:              "policy",
			PermissionedCandidatePolicy: "permissioned",
		},
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeAPI,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	got := built.Midnight()
	if got != cfg.Midnight {
		t.Fatalf("expected Midnight config to flow through, got %+v", got)
	}
}

// TestRootPeerTargetComposition verifies Cardano fallback values and Dingo's
// higher-precedence root-peer setting reach the top-level Dingo configuration.
func TestRootPeerTargetComposition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		dingoTarget   int
		cardanoTarget int
		want          int
	}{
		{name: "cardano explicit", cardanoTarget: 12, want: 12},
		{name: "default", want: 0},
		{name: "unlimited", cardanoTarget: -1, want: -1},
		{
			name:          "dingo config takes precedence",
			dingoTarget:   7,
			cardanoTarget: 12,
			want:          7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{TargetNumberOfRootPeers: tt.dingoTarget}
			applyRootPeerTargetFallback(cfg, tt.cardanoTarget)

			built := buildDingoConfig(
				cfg,
				slog.New(slog.NewTextHandler(new(bytes.Buffer), nil)),
				nil,
				nil,
				false,
				dingo.StorageModeCore,
				30*time.Second,
				chainsync.DefaultStallTimeout,
				chainsync.HeaderSyncStrategyPrimary,
			)

			if got := built.TargetNumberOfRootPeers(); got != tt.want {
				t.Fatalf("expected root-peer target %d, got %d", tt.want, got)
			}
		})
	}
}

// TestKoiosParityConfigForwardsEveryField pins that the serve path hands the
// node every KoiosParity setting.
//
// internal/node builds the dingo.KoiosParityConfig by hand, so a field added to
// internal/config is silently dropped until someone remembers to add it here —
// which is exactly what happened to AccountChunkSize and AccountChunkMaxBytes,
// and then to BaseURL. A dropped field does not fail: the option keeps its
// package default and the operator's setting is ignored with no diagnostic,
// which for BaseURL meant a run aimed at a self-hosted host silently querying
// the public one.
func TestKoiosParityConfigForwardsEveryField(t *testing.T) {
	src := reflect.TypeFor[config.KoiosParityConfig]()
	dst := reflect.TypeFor[dingo.KoiosParityConfig]()

	for field := range src.Fields() {
		name := field.Name
		if _, ok := dst.FieldByName(name); !ok {
			continue // not part of the node-facing config
		}
		// Match the assignment, not just the field name: checking only that
		// the name appears would accept a cross-wiring such as
		// "AccountChunkSize: cfg.KoiosParity.AccountChunkMaxBytes".
		assign := regexp.MustCompile(
			`\b` + regexp.QuoteMeta(name) +
				`:\s*&?cfg\.KoiosParity\.` + regexp.QuoteMeta(name) + `\b`,
		)
		if !assign.MatchString(nodeSourceForKoiosParity(t)) {
			t.Errorf(
				"internal/config KoiosParityConfig.%s is not forwarded from "+
					"cfg.KoiosParity.%s in WithKoiosParity; the operator's "+
					"setting would be silently ignored or cross-wired",
				name, name,
			)
		}
	}
}

// TestKoiosParityForwardingGuardCatchesCrossWiring proves the guard checks the
// assignment rather than the field name. Matching only the name would accept a
// field wired from the wrong source, which fails exactly as silently as a field
// left out entirely.
func TestKoiosParityForwardingGuardCatchesCrossWiring(t *testing.T) {
	crossWired := `dingo.WithKoiosParity(dingo.KoiosParityConfig{
		AccountChunkSize: cfg.KoiosParity.AccountChunkMaxBytes,
	`
	assign := regexp.MustCompile(
		`\bAccountChunkSize:\s*&?cfg\.KoiosParity\.AccountChunkSize\b`,
	)
	if assign.MatchString(crossWired) {
		t.Error("guard accepted a cross-wired assignment")
	}
	if !strings.Contains(crossWired, "AccountChunkSize:") {
		t.Error("the weaker name-only check would have accepted it")
	}
}

// nodeSourceForKoiosParity returns the WithKoiosParity call site's source.
func nodeSourceForKoiosParity(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("node.go")
	if err != nil {
		t.Fatalf("read node.go: %v", err)
	}
	s := string(b)
	start := strings.Index(s, "dingo.WithKoiosParity(")
	if start < 0 {
		t.Fatal("WithKoiosParity call not found in node.go")
	}
	end := strings.Index(s[start:], "}),")
	if end < 0 {
		t.Fatal("WithKoiosParity call not terminated")
	}
	return s[start : start+end]
}

// TestBuildDingoConfigWiresForgeTolerances ensures the remaining operator-set
// forge tolerances reach the node config built on the serve path.
func TestBuildDingoConfigWiresForgeTolerances(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		ForgeSyncToleranceSlots:          321,
		ForgeStaleGapThresholdSlots:      654,
		ForgeUpstreamStalenessSlots:      17,
		ForgeAppliedTipStalenessSlots:    9,
		ForgeEndorserBlockStalenessSlots: 23,
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))
	built := buildDingoConfig(
		cfg, logger, nil, nil, false, dingo.StorageModeCore,
		30*time.Second, chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)
	if got := built.ForgeSyncToleranceSlots(); got != 321 {
		t.Fatalf("expected forgeSyncToleranceSlots 321, got %d", got)
	}
	if got := built.ForgeStaleGapThresholdSlots(); got != 654 {
		t.Fatalf("expected forgeStaleGapThresholdSlots 654, got %d", got)
	}
	if got := built.ForgeUpstreamStalenessSlots(); got != 17 {
		t.Fatalf("expected forgeUpstreamStalenessSlots 17, got %d", got)
	}
	if got := built.ForgeAppliedTipStalenessSlots(); got != 9 {
		t.Fatalf("expected forgeAppliedTipStalenessSlots 9, got %d", got)
	}
	if got := built.ForgeEndorserBlockStalenessSlots(); got != 23 {
		t.Fatalf("expected forgeEndorserBlockStalenessSlots 23, got %d", got)
	}
}

// TestBuildDingoConfigWiresBlockPipelineFlags is the regression test for
// the block pipeline flags: BlockPipelineEnabled and
// BlockPipelineValidateEnabled were correctly parsed into
// internal/config.Config but buildDingoConfig never called a With... option to
// forward either one, so dingo.NewConfig built its internal config from fresh
// Go zero values and the parallel block decode pipeline (ledger/state.go's "if
// cfg.BlockPipelineEnabled && !cfg.ManualBlockProcessing") never constructed on
// the live serve path, regardless of the flag or environment variable.
func TestBuildDingoConfigWiresBlockPipelineFlags(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		BlockPipelineEnabled:         true,
		BlockPipelineValidateEnabled: true,
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if !built.BlockPipelineEnabled() {
		t.Fatal(
			"expected BlockPipelineEnabled to flow through, got false; " +
				"the loaded value never reached dingo.Config, so the " +
				"parallel block decode pipeline never activates on the " +
				"serve path however it is configured",
		)
	}
	if !built.BlockPipelineValidateEnabled() {
		t.Fatal(
			"expected BlockPipelineValidateEnabled to flow through, got " +
				"false; the loaded value never reached dingo.Config, so " +
				"the pipeline's parallel VRF/KES validate stage never " +
				"activates on the serve path however it is configured",
		)
	}
}

// TestBuildDingoConfigForwardsScalarConfigFields is recurrence-prevention
// coverage for the defect class of the block pipeline flags, not just the
// single field it reported: buildDingoConfig hand-lists roughly 85 individual
// dingo.With...(...) calls, one per field, and has now silently dropped a field
// from that list twice -- AccountChunkSize/AccountChunkMaxBytes for KoiosParity
// (caught and fixed separately, see the comment on dingo.WithKoiosParity's call
// site in node.go), then BlockPipelineEnabled/BlockPipelineValidateEnabled (the
// second time) -- with no general check that every field actually made the
// list.
//
// It enumerates every top-level internal/config.Config field whose Kind is a
// plain scalar (bool, a signed/unsigned integer, float64, string, or
// []string) and that has a plausibly corresponding dingo.Config field --
// matched case-insensitively by name and required to share the same
// reflect.Kind. It fills every matched field with a distinctive non-zero
// value, runs the result through buildDingoConfig (the same function and
// option list Run() calls), and asserts every matched field reads back
// unchanged from the built dingo.Config.
//
// Deliberately out of scope, and not checked here -- a fully general,
// structurally-verified diff across every nested type was not pursued,
// because dingo.Config's fields are unexported and frequently intentionally
// renamed, reshaped or split (a []string becomes a *bool-defaulted pointer;
// DatabaseWorkers and DatabaseQueueSize become one DatabaseWorkerPoolConfig
// struct field; BackfillBatchSize is read directly off internal/config.Config
// by cmd/dingo and never touches dingo.Config at all), so a mechanical
// field-shape comparison would have to hand-write the same per-field mapping
// this test exists to avoid maintaining:
//
//   - Nested config structs (Plugins, API, KoiosParity, Midnight,
//     TokenRegistry, OffchainMetadata, HistoryExpiry, GenesisBootstrap,
//     Cache, Chainsync, DatabaseLifecycle, Logging, Mithril): each is either
//     forwarded by hand-mapping to a differently-named dingo type or, for
//     DatabaseLifecycle, passed through as the identical named type.
//     KoiosParity has its own dedicated field-by-field test above
//     (TestKoiosParityConfigForwardsEveryField); Midnight, API and the Bark
//     fields have their own narrower pinning tests elsewhere in this file.
//   - PeerSharing, StorageMode, RelayPort and ShutdownTimeout: Run()
//     resolves each of these into a separate buildDingoConfig parameter
//     (peerSharing, storageMode, shutdownTimeout) before calling it, rather
//     than buildDingoConfig reading the cfg field directly. Matching on the
//     cfg field's name would either false-positive (StorageMode: same name
//     and Kind as dingo.Config's storageMode field, but resolved through the
//     parameter, not a cfg passthrough) or match nothing anyway (PeerSharing
//     is a *bool against a bool field; RelayPort forwards to a
//     differently-named outboundSourcePort field).
//   - A cfg field with no same-named dingo.Config field is skipped
//     silently rather than reported, so this mechanism cannot see a gap
//     whose dingo.Config counterpart was renamed. The fields in that
//     position today -- the port, path and bind-address fields, Topology,
//     CardanoConfig and LedgerCatchupTimeout -- are consumed directly by
//     internal/node, cmd/dingo or internal/config and never enter
//     dingo.Config at all.
//   - SlotsPerKESPeriod, MaxKESEvolutions, Mithril and BackfillBatchSize:
//     dingo.Config exposes an accessor reading straight from its embedded
//     cfg pointer, with no backing private field and no With... option at
//     all. Grepping the repository found no production caller of any of the
//     first three accessors, so whether buildDingoConfig forwards them
//     presently has no observable effect either way; BackfillBatchSize is
//     read directly off the loaded internal/config.Config by cmd/dingo and
//     mithril/sync.go instead.
//
// Known, pre-existing gaps of this same shape found while writing this test
// are excluded below rather than fixed here.
func TestBuildDingoConfigForwardsScalarConfigFields(t *testing.T) {
	t.Parallel()

	// knownGaps are real forwarding gaps of the same shape as the block
	// pipeline flags, found while writing this test and deliberately not fixed
	// in the same commit as that unrelated fix. Remove an entry here once its
	// fix lands, so this test starts asserting it.
	knownGaps := map[string]string{}
	// excluded are cfg fields resolved through a separate buildDingoConfig
	// parameter, or otherwise not part of the direct cfg-to-dingo.Config
	// passthrough this test checks; see the function doc comment.
	excluded := map[string]bool{
		"PeerSharing":     true,
		"StorageMode":     true,
		"RelayPort":       true,
		"ShutdownTimeout": true,
	}

	cfgVal := reflect.New(reflect.TypeFor[config.Config]()).Elem()
	cfgType := cfgVal.Type()

	dingoType := reflect.ValueOf(dingo.NewConfig()).Type()
	dingoFieldIndex := map[string]int{}
	for i := range dingoType.NumField() {
		dingoFieldIndex[strings.ToLower(dingoType.Field(i).Name)] = i
	}

	isScalarKind := func(k reflect.Kind) bool {
		switch k {
		case reflect.Bool, reflect.String,
			reflect.Float32, reflect.Float64,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return true
		default:
			return false
		}
	}

	type match struct {
		name       string
		dingoIndex int
		kind       reflect.Kind
	}
	var matches []match

	for f := range cfgType.Fields() {
		if f.PkgPath != "" { // unexported field (e.g. provenance)
			continue
		}
		if excluded[f.Name] {
			continue
		}
		if _, ok := knownGaps[f.Name]; ok {
			continue
		}
		kind := f.Type.Kind()
		supported := isScalarKind(kind) ||
			(kind == reflect.Slice && f.Type.Elem().Kind() == reflect.String)
		if !supported {
			continue
		}
		di, ok := dingoFieldIndex[strings.ToLower(f.Name)]
		if !ok {
			continue
		}
		dField := dingoType.Field(di)
		if dField.Type.Kind() != kind {
			continue
		}
		if kind == reflect.Slice &&
			dField.Type.Elem().Kind() != reflect.String {
			continue
		}
		matches = append(matches, match{
			name:       f.Name,
			dingoIndex: di,
			kind:       kind,
		})
	}

	// A sharp drop here means the matching logic regressed and is silently
	// checking far fewer fields than intended, not that the config shrank.
	if len(matches) < 40 {
		t.Fatalf(
			"only matched %d scalar fields between internal/config.Config "+
				"and dingo.Config, expected at least 40; the field-matching "+
				"logic may have regressed and this test may be silently "+
				"checking almost nothing",
			len(matches),
		)
	}

	// Fill every matched field with a distinctive, non-zero, non-default
	// value.
	want := make(map[string]any, len(matches))
	for i, m := range matches {
		fv := cfgVal.FieldByName(m.name)
		switch m.kind {
		case reflect.Bool:
			fv.SetBool(true)
			want[m.name] = true
		case reflect.String:
			s := fmt.Sprintf("dingo4599-%s-%d", m.name, i)
			fv.SetString(s)
			want[m.name] = s
		case reflect.Float32, reflect.Float64:
			v := 1.5 + float64(i)
			fv.SetFloat(v)
			want[m.name] = v
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Int64:
			v := int64(10 + i)
			fv.SetInt(v)
			want[m.name] = v
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
			reflect.Uint64:
			v := uint64(10 + i) // #nosec G115 -- i is a small loop index
			fv.SetUint(v)
			want[m.name] = v
		case reflect.Slice:
			s := []string{fmt.Sprintf("dingo4599-%s-%d", m.name, i)}
			fv.Set(reflect.ValueOf(s))
			want[m.name] = s
		}
	}

	cfg, ok := reflect.TypeAssert[*config.Config](cfgVal.Addr())
	if !ok {
		t.Fatal("cfgVal.Addr().Interface() did not assert to *config.Config")
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)
	builtVal := reflect.ValueOf(built)

	for _, m := range matches {
		got := builtVal.Field(m.dingoIndex)
		switch m.kind {
		case reflect.Bool:
			if want, got := want[m.name].(bool), got.Bool(); got != want {
				t.Errorf(
					"internal/config.Config.%s did not reach dingo.Config: "+
						"got %v, want %v; this field is silently dropped "+
						"on the serve path",
					m.name, got, want,
				)
			}
		case reflect.String:
			if want, got := want[m.name].(string), got.String(); got != want {
				t.Errorf(
					"internal/config.Config.%s did not reach dingo.Config: "+
						"got %q, want %q; this field is silently dropped "+
						"on the serve path",
					m.name, got, want,
				)
			}
		case reflect.Float32, reflect.Float64:
			if want, got := want[m.name].(float64), got.Float(); got != want {
				t.Errorf(
					"internal/config.Config.%s did not reach dingo.Config: "+
						"got %v, want %v; this field is silently dropped "+
						"on the serve path",
					m.name, got, want,
				)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Int64:
			if want, got := want[m.name].(int64), got.Int(); got != want {
				t.Errorf(
					"internal/config.Config.%s did not reach dingo.Config: "+
						"got %v, want %v; this field is silently dropped "+
						"on the serve path",
					m.name, got, want,
				)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
			reflect.Uint64:
			if want, got := want[m.name].(uint64), got.Uint(); got != want {
				t.Errorf(
					"internal/config.Config.%s did not reach dingo.Config: "+
						"got %v, want %v; this field is silently dropped "+
						"on the serve path",
					m.name, got, want,
				)
			}
		case reflect.Slice:
			want := want[m.name].([]string)
			gotSlice := make([]string, got.Len())
			for i := range got.Len() {
				gotSlice[i] = got.Index(i).String()
			}
			if !slices.Equal(gotSlice, want) {
				t.Errorf(
					"internal/config.Config.%s did not reach dingo.Config: "+
						"got %v, want %v; this field is silently dropped "+
						"on the serve path",
					m.name, gotSlice, want,
				)
			}
		}
	}

	for name, reason := range knownGaps {
		t.Logf(
			"known forwarding gap excluded from this test: %s: %s",
			name,
			reason,
		)
	}
}

// TestBuildDingoConfigWiresTokenRegistryHeaders pins the composition step a
// loaded header secret takes on its way to the registry sync.
func TestBuildDingoConfigWiresTokenRegistryHeaders(t *testing.T) {
	t.Parallel()

	headers := map[string]string{"Authorization": "Bearer wired"}
	cfg := &config.Config{
		TokenRegistry: config.TokenRegistryConfig{HeaderSecrets: headers},
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	built := buildDingoConfig(
		cfg,
		logger,
		nil,
		nil,
		false,
		dingo.StorageModeCore,
		30*time.Second,
		chainsync.DefaultStallTimeout,
		chainsync.HeaderSyncStrategyPrimary,
	)

	if got := built.TokenRegistry().HeaderSecrets; !maps.Equal(got, headers) {
		t.Fatalf("token registry headers = %v, want %v", got, headers)
	}
}
