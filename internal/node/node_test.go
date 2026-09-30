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
	"fmt"
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

// TestBuildDingoConfigWiresBlockPipelineFlags is the regression test for
// dingo#4599: BlockPipelineEnabled and BlockPipelineValidateEnabled were
// correctly parsed into internal/config.Config but buildDingoConfig never
// called a With... option to forward either one, so dingo.NewConfig built
// its internal config from fresh Go zero values and the parallel block
// decode pipeline (ledger/state.go's
// "if cfg.BlockPipelineEnabled && !cfg.ManualBlockProcessing") never
// constructed on the live serve path, regardless of the flag or environment
// variable.
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
// coverage for the defect class dingo#4599 belongs to, not just the single
// field it reported: buildDingoConfig hand-lists roughly 85 individual
// dingo.With...(...) calls, one per field, and has now silently dropped a
// field from that list twice -- AccountChunkSize/AccountChunkMaxBytes for
// KoiosParity (caught and fixed separately, see the comment on
// dingo.WithKoiosParity's call site in node.go), then
// BlockPipelineEnabled/BlockPipelineValidateEnabled (this issue) -- with no
// general check that every field actually made the list.
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
// are excluded below rather than fixed here; dingo#4600 tracks them.
func TestBuildDingoConfigForwardsScalarConfigFields(t *testing.T) {
	t.Parallel()

	// knownGaps are real forwarding gaps of the same shape as dingo#4599,
	// found while writing this test and deliberately not fixed in the same
	// commit as that unrelated fix; dingo#4600 tracks all three. Remove an
	// entry here once its fix lands, so this test starts asserting it.
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

	cfg, ok := cfgVal.Addr().Interface().(*config.Config)
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
