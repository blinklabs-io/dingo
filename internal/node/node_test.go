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
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/config"
)

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

// A bind failure on a non-essential observability listener (metrics or pprof)
// must be logged and non-fatal: it returns instead of blocking, and never
// signals an error that would take down an otherwise-healthy node.
func TestServeAuxiliaryListenerBindFailureIsNonFatal(t *testing.T) {
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

	done := make(chan struct{})
	go func() {
		serveAuxiliaryListener("metrics", srv, logger)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(
			"serveAuxiliaryListener did not return on bind failure; " +
				"a non-essential listener must not block or be fatal",
		)
	}
	// Read after the goroutine finished, so no concurrent buffer access.
	if logged := buf.String(); !strings.Contains(logged, "metrics") {
		t.Fatalf(
			"expected a log mentioning the metrics listener, got: %q",
			logged,
		)
	}
}

func TestPprofDebugServerUsesDedicatedBindAddress(t *testing.T) {
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
// internal/config.Config's api.tls/api.auth policy (as set via YAML/env/CLI)
// actually reaches the dingo.Config that Run() hands to dingo.New() --
// regression test for the top-level API security defaults (dingo#2998)
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
			Auth: apiconfig.AuthPolicy{
				Mode:  new("token"),
				Token: new("shared-secret"),
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
	if got.Auth.Mode == nil || *got.Auth.Mode != "token" {
		t.Fatalf("expected api.auth.mode to flow through, got %+v", got.Auth)
	}
	if got.Auth.Token == nil || *got.Auth.Token != "shared-secret" {
		t.Fatalf(
			"expected api.auth.token to flow through, got %+v",
			got.Auth,
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
	cfg := &config.Config{
		BarkOperatorCertificateFingerprints: want,
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
			AllowInsecureRemote:         true,
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

// TestBuildDingoConfigWiresForgeTolerances asserts that the forge tolerances a
// loaded internal/config.Config carries actually reach the dingo.Config that
// Run hands to dingo.New. This is the composition path the binary really
// takes: buildDingoConfig calls dingo.NewConfig with an explicit option list
// and NewConfig starts from a fresh internal config, so a field that has no
// With... entry here is silently dropped no matter how completely it is
// plumbed through YAML, env, flags, defaults and the accessor.
//
// ForgeHeaderFrontierToleranceSlots was exactly that: parsed, defaulted,
// flagged, documented and asserted at every other layer, yet absent from this
// list, so an operator's value was discarded and the forger always fell back
// to its built-in default. The neighbouring tolerances are asserted alongside
// it so a future option-list edit that drops any of them fails here.
func TestBuildDingoConfigWiresForgeTolerances(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		ForgeSyncToleranceSlots:           321,
		ForgeStaleGapThresholdSlots:       654,
		ForgeHeaderFrontierToleranceSlots: 42,
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

	if got := built.ForgeSyncToleranceSlots(); got != 321 {
		t.Fatalf("expected forgeSyncToleranceSlots 321, got %d", got)
	}
	if got := built.ForgeStaleGapThresholdSlots(); got != 654 {
		t.Fatalf("expected forgeStaleGapThresholdSlots 654, got %d", got)
	}
	if got := built.ForgeHeaderFrontierToleranceSlots(); got != 42 {
		t.Fatalf(
			"expected forgeHeaderFrontierToleranceSlots 42, got %d; the "+
				"loaded value never reached dingo.Config, so the forger "+
				"silently uses its built-in default",
			got,
		)
	}
}
