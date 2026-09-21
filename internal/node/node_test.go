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
	"os"
	"reflect"
	"regexp"
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

// TestBuildDingoConfigWiresForgeTolerances asserts that the forge tolerances a
// loaded internal/config.Config carries actually reach the dingo.Config that
// Run hands to dingo.New. This is the composition path the binary really
// takes: buildDingoConfig calls dingo.NewConfig with an explicit option list
// and NewConfig starts from a fresh internal config, so a field that has no
// With... entry here is silently dropped no matter how completely it is
// plumbed through YAML, env, flags, defaults and the accessor.
//
// ForgePrimaryChainTipToleranceSlots was exactly that: parsed, defaulted,
// flagged, documented and asserted at every other layer, yet absent from this
// list, so an operator's value was discarded and the forger always fell back
// to its built-in default. The neighbouring tolerances are asserted alongside
// it so a future option-list edit that drops any of them fails here.
func TestBuildDingoConfigWiresForgeTolerances(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		ForgeSyncToleranceSlots:            321,
		ForgeStaleGapThresholdSlots:        654,
		ForgePrimaryChainTipToleranceSlots: 42,
		ForgeUpstreamStalenessSlots:        17,
		ForgeAppliedTipStalenessSlots:      9,
		ForgeEndorserBlockStalenessSlots:   23,
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
	if got := built.ForgePrimaryChainTipToleranceSlots(); got != 42 {
		t.Fatalf(
			"expected forgePrimaryChainTipToleranceSlots 42, got %d; the "+
				"loaded value never reached dingo.Config, so the forger "+
				"silently uses its built-in default",
			got,
		)
	}
	if got := built.ForgeUpstreamStalenessSlots(); got != 17 {
		t.Fatalf(
			"expected forgeUpstreamStalenessSlots 17, got %d; the loaded "+
				"value never reached dingo.Config, so the forger silently "+
				"uses its built-in default",
			got,
		)
	}
	if got := built.ForgeAppliedTipStalenessSlots(); got != 9 {
		t.Fatalf(
			"expected forgeAppliedTipStalenessSlots 9, got %d; the loaded "+
				"value never reached dingo.Config, so the wall-clock "+
				"staleness backstop stays off however it is configured",
			got,
		)
	}
	if got := built.ForgeEndorserBlockStalenessSlots(); got != 23 {
		t.Fatalf(
			"expected forgeEndorserBlockStalenessSlots 23, got %d; the "+
				"loaded value never reached dingo.Config, so the "+
				"endorser-block staleness bound stays off however it is "+
				"configured",
			got,
		)
	}
}

// TestBuildDingoConfigWiresTrustCanonicalWithdrawalOnRewardMismatch is the
// same class of bug TestBuildDingoConfigWiresForgeTolerances documents
// (dingo#3885 follow-up): TrustCanonicalWithdrawalOnRewardMismatch was
// parsed, defaulted, flagged, documented and asserted at every other layer,
// but buildDingoConfig's explicit dingo.NewConfig option list had no
// dingo.WithTrustCanonicalWithdrawalOnRewardMismatch entry, so an operator's
// --trust-canonical-withdrawal-on-reward-mismatch flag was silently dropped
// and the reconciliation path never ran -- observed live as 194 identical
// rejections of the same block with the flag enabled.
func TestBuildDingoConfigWiresTrustCanonicalWithdrawalOnRewardMismatch(
	t *testing.T,
) {
	t.Parallel()

	cfg := &config.Config{
		TrustCanonicalWithdrawalOnRewardMismatch: true,
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

	if got := built.TrustCanonicalWithdrawalOnRewardMismatch(); !got {
		t.Fatalf(
			"expected TrustCanonicalWithdrawalOnRewardMismatch true, got "+
				"%v; the loaded value never reached dingo.Config, so the "+
				"reconciliation path never runs however it is configured",
			got,
		)
	}
}

// TestBuildDingoConfigWiresTrustCanonicalTreasuryValueOnMismatch is the same
// class of bug TestBuildDingoConfigWiresForgeTolerances documents, for
// TrustCanonicalTreasuryValueOnMismatch's own dingo.NewConfig option-list
// entry: parsed, defaulted, flagged and documented at every other layer is
// worthless if buildDingoConfig's explicit option list never forwards it.
func TestBuildDingoConfigWiresTrustCanonicalTreasuryValueOnMismatch(
	t *testing.T,
) {
	t.Parallel()

	cfg := &config.Config{
		TrustCanonicalTreasuryValueOnMismatch: true,
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

	if got := built.TrustCanonicalTreasuryValueOnMismatch(); !got {
		t.Fatalf(
			"expected TrustCanonicalTreasuryValueOnMismatch true, got %v; "+
				"the loaded value never reached dingo.Config, so the "+
				"reconciliation path never runs however it is configured",
			got,
		)
	}
}

// TestBuildDingoConfigWiresTrustCanonicalReferenceScriptOnMismatch is the
// same class of bug TestBuildDingoConfigWiresForgeTolerances documents, for
// TrustCanonicalReferenceScriptOnMismatch's own dingo.NewConfig option-list
// entry.
func TestBuildDingoConfigWiresTrustCanonicalReferenceScriptOnMismatch(
	t *testing.T,
) {
	t.Parallel()

	cfg := &config.Config{
		TrustCanonicalReferenceScriptOnMismatch: true,
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

	if got := built.TrustCanonicalReferenceScriptOnMismatch(); !got {
		t.Fatalf(
			"expected TrustCanonicalReferenceScriptOnMismatch true, got %v; "+
				"the loaded value never reached dingo.Config, so the "+
				"trust path never runs however it is configured",
			got,
		)
	}
}
