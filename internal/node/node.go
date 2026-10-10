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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// reloadOnSignal calls reload for every signal received on sigs until ctx is
// done. A failed reload is logged and ends nothing: the node keeps running on
// the credentials it already has.
func reloadOnSignal(
	ctx context.Context,
	sigs <-chan os.Signal,
	reload func() error,
	logger *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-sigs:
			logger.Info(
				"reloading block producer credentials",
				"component", "node",
				"signal", sig.String(),
			)
			if err := reload(); err != nil {
				logger.Error(
					"block producer credential reload failed; keeping the loaded credentials",
					"component",
					"node",
					"error",
					err,
				)
			}
		}
	}
}

func waitForSignalOrError(
	signalCtx context.Context,
	errChan <-chan error,
) (error, bool) {
	select {
	case err := <-errChan:
		return err, false
	case <-signalCtx.Done():
		// Prefer a queued component error over treating shutdown as a clean
		// signal-driven exit when both happen at roughly the same time.
		select {
		case err := <-errChan:
			return err, false
		default:
		}
		return nil, true
	}
}

// waitForStop is waitForSignalOrError plus the remote lifecycle request that
// ended the run, if any. endRequests closes the node's request intake and
// returns the accepted request, so a request is either returned here or
// refused; Bark keeps serving into shutdown, and a request accepted after this
// point would never be performed. It is called however the run ended: Node.Run
// returns nil on the cancellation a request causes, which can reach errChan
// before the cancellation is observed.
func waitForStop(
	signalCtx context.Context,
	errChan <-chan error,
	endRequests func() (dingo.ShutdownRequest, bool),
) (*dingo.ShutdownRequest, bool, error) {
	err, signaled := waitForSignalOrError(signalCtx, errChan)
	req, ok := endRequests()
	if !ok {
		return nil, signaled, err
	}
	return &req, signaled, err
}

// runRequestedShutdown performs a stop or restart accepted over the remote
// lifecycle service. The graceful shutdown is abandoned once req.Timeout
// elapses so the request can never leave the process hanging, and a restart
// re-executes the process even then.
func runRequestedShutdown(
	req dingo.ShutdownRequest,
	causalErr error,
	shutdown func() error,
	reExec func() error,
) error {
	done := make(chan error, 1)
	go func() { done <- shutdown() }()
	remaining := req.Timeout
	if !req.Deadline.IsZero() {
		remaining = max(time.Until(req.Deadline), 0)
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(remaining):
		err = fmt.Errorf("graceful shutdown exceeded %s", req.Timeout)
	}
	if !req.Restart {
		return errors.Join(causalErr, err)
	}
	if reExecErr := reExec(); reExecErr != nil {
		return errors.Join(causalErr, err, reExecErr)
	}
	return errors.Join(causalErr, err)
}

func gracefulShutdown(
	logger *slog.Logger,
	metricsServer *http.Server,
	debugServer *http.Server,
	healthServer *http.Server,
	d *dingo.Node,
	timeout time.Duration,
) error {
	shutdownErr := shutdownNodeResources(
		optionalShutdown(metricsServer),
		optionalShutdown(debugServer),
		optionalShutdown(healthServer),
		d.Stop,
		timeout,
	)
	if shutdownErr != nil {
		logger.Error(
			"graceful shutdown failed",
			"error",
			shutdownErr,
		)
	}
	return shutdownErr
}

// optionalShutdown adapts a listener that may be disabled (a nil *http.Server)
// to the shutdown func shutdownNodeResources takes.
func optionalShutdown(srv *http.Server) func(context.Context) error {
	if srv == nil {
		return nil
	}
	return srv.Shutdown
}

func shutdownNodeResources(
	metricsServerShutdown func(context.Context) error,
	debugServerShutdown func(context.Context) error,
	healthServerShutdown func(context.Context) error,
	nodeStop func() error,
	timeout time.Duration,
) error {
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		timeout,
	)
	defer cancel()
	var err error
	if metricsServerShutdown != nil {
		if shutdownErr := metricsServerShutdown(shutdownCtx); shutdownErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("metrics server shutdown: %w", shutdownErr),
			)
		}
	}
	if debugServerShutdown != nil {
		if shutdownErr := debugServerShutdown(shutdownCtx); shutdownErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("debug server shutdown: %w", shutdownErr),
			)
		}
	}
	if healthServerShutdown != nil {
		if shutdownErr := healthServerShutdown(shutdownCtx); shutdownErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("health server shutdown: %w", shutdownErr),
			)
		}
	}
	if stopErr := nodeStop(); stopErr != nil {
		err = errors.Join(
			err,
			fmt.Errorf("node stop: %w", stopErr),
		)
	}
	return err
}

// bindAuxiliaryListener binds the address of a non-essential observability
// HTTP server (the prometheus metrics endpoint, the pprof debug endpoint or
// the health probe). A bind failure is logged and reported as a nil
// listener, never as an error: losing metrics, pprof or the probe must not
// take down a node that is otherwise healthy (for example a node that has
// just finished an expensive backfill, started while the configured port is
// held by another process). This mirrors how `dingo mithril sync` already
// tolerates a metrics-port conflict.
func bindAuxiliaryListener(
	name string,
	srv *http.Server,
	logger *slog.Logger,
) net.Listener {
	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		logger.Error(
			name+" listener stopped; continuing without it",
			"component", "node",
			"addr", srv.Addr,
			"error", err,
		)
		return nil
	}
	return listener
}

// serveAuxiliaryListenerOn serves a non-essential observability HTTP server
// on a socket the caller has already bound. Binding is separated from
// serving so the caller owns the listener rather than naming a port: a port
// number learned from a listener that was then closed is not a reservation,
// and anything asking the kernel for an arbitrary port can take it before
// the rebind. A serve failure is logged but never fatal.
func serveAuxiliaryListenerOn(
	name string,
	srv *http.Server,
	listener net.Listener,
	logger *slog.Logger,
) {
	if err := srv.Serve(listener); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		logger.Error(
			name+" listener stopped; continuing without it",
			"component", "node",
			"addr", srv.Addr,
			"error", err,
		)
	}
}

// newMetricsServer builds the Prometheus listener on its own dedicated
// mux so pprof or other handlers registered on DefaultServeMux are never
// exposed, or returns nil when metricsPort is 0.
func newMetricsServer(cfg *config.Config) *http.Server {
	if cfg.MetricsPort == 0 {
		return nil
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	return &http.Server{
		Addr:              cfg.MetricsListenAddress(),
		Handler:           metricsMux,
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

func newPprofDebugServer(cfg *config.Config) *http.Server {
	if cfg.DebugPort == 0 {
		return nil
	}
	debugMux := http.NewServeMux()
	debugMux.HandleFunc("/debug/pprof/", pprof.Index)
	debugMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	debugMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	debugMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	debugMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{
		Addr:              cfg.DebugListenAddress(),
		Handler:           debugMux,
		ReadHeaderTimeout: 60 * time.Second,
	}
}

// nodeHealthChecks is the node's contribution to the probes beyond the tip
// gap: a stopped slot clock fails liveness, and an unavailable database or a
// block producer that cannot forge holds readiness.
func nodeHealthChecks(d *dingo.Node) []health.Check {
	return []health.Check{
		{Liveness: true, Fn: d.EventLoopResponsive},
		{Fn: d.DatabaseReady},
		{Fn: d.BlockProducerReady},
	}
}

// NewHealthServer builds the dedicated liveness/readiness listener, or nil
// when healthPort is 0.
//
// Two properties are covered by tests:
//
//  1. It is not gated on storage mode. Client API listeners require
//     storageMode.IsAPI(), while health probes must remain available in core
//     mode so an orchestrator can verify node state.
//  2. It binds cfg.BindAddr, the address the relay and metrics listeners
//     already use, not the API listeners' loopback-by-default address. A
//     probe is operational surface: a Docker HEALTHCHECK runs inside the
//     container and would be satisfied by loopback, but a Kubernetes kubelet
//     probe or an ECS/ALB target-group check reaches the container from
//     outside, and loopback would fail those closed.
//
// It is exported because `dingo mithril sync` serves the same listener while
// bootstrapping, with a nil tipGap. That bootstrap runs as its own process
// before serve, for hours on mainnet, and the image's HEALTHCHECK is probing
// throughout it; without a listener there the probe is refused and an
// orchestrator replaces the container mid-download. A nil tipGap is the
// accurate answer for it: live, and not ready because there is no chain tip
// yet.
func NewHealthServer(
	cfg *config.Config,
	tipGap health.TipGapFunc,
	checks ...health.Check,
) *http.Server {
	if cfg.HealthPort == 0 {
		return nil
	}
	readyTipGapSlots := uint64(cfg.HealthReadyGapSlots)
	if readyTipGapSlots == 0 {
		readyTipGapSlots = config.DefaultHealthReadyGapSlots
	}
	return &http.Server{
		// JoinHostPort, not "%s:%d": an IPv6 bindAddr such as "::" has to
		// be bracketed or net.Listen rejects the address.
		Addr: net.JoinHostPort(
			cfg.BindAddr,
			strconv.FormatUint(uint64(cfg.HealthPort), 10),
		),
		Handler:           health.NewMux(tipGap, readyTipGapSlots, checks...),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// logStartupConfig debug-logs the effective node configuration through
// Config's redacted representation (Config.LogValue), so a debug log never
// persists a Koios API key, an inline API auth token, or a storage provider
// password or DSN credential.
func logStartupConfig(logger *slog.Logger, cfg *config.Config) {
	logger.Debug("config", "component", "node", "config", cfg)
}

func Run(cfg *config.Config, logger *slog.Logger) error {
	cfg.ApplyRunModeOverrides(cfg.RunMode)
	if cfg.RunMode.IsDevMode() {
		logger.Info("dev mode: forcing API storage and block production")
	}
	logStartupConfig(logger, cfg)
	logger.Debug(
		fmt.Sprintf("topology: %+v", config.GetTopologyConfig()),
		"component", "node",
	)
	// Derive default config path from cfg.Network when cfg.CardanoConfig is empty
	cardanoConfigPath := cfg.CardanoConfig
	network := cfg.Network
	if cardanoConfigPath == "" {
		if network == "" {
			network = "preview"
		}
		cardanoConfigPath = cardano.EmbeddedConfigPath(network)
	}

	var nodeCfg *cardano.CardanoNodeConfig
	var err error
	nodeCfg, err = cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		network,
		cardano.EmbeddedConfigFS,
	)
	if err != nil {
		return err
	}
	logger.Debug(
		fmt.Sprintf(
			"cardano network config: %+v",
			nodeCfg,
		),
		"component", "node",
	)
	// Apply cardano-node config.json P2P targets as fallback when the
	// Dingo-native config (YAML / env / CLI) does not specify them.
	// Priority: Dingo config > cardano config.json > peergov defaults.
	if nodeCfg != nil {
		rp, kp, ep, ap := nodeCfg.P2PTargets()
		applyRootPeerTargetFallback(cfg, rp)
		if cfg.TargetNumberOfKnownPeers == 0 && kp > 0 {
			cfg.TargetNumberOfKnownPeers = kp
		}
		if cfg.TargetNumberOfEstablishedPeers == 0 && ep > 0 {
			cfg.TargetNumberOfEstablishedPeers = ep
		}
		if cfg.TargetNumberOfActivePeers == 0 && ap > 0 {
			cfg.TargetNumberOfActivePeers = ap
		}
	}
	var cardanoNodePeerSharing *bool
	if nodeCfg != nil {
		cardanoNodePeerSharing = nodeCfg.PeerSharing
	}
	peerSharing := resolvePeerSharing(
		cfg.PeerSharing,
		cfg.BlockProducer,
		cardanoNodePeerSharing,
		logger,
	)

	listeners := []dingo.ListenerConfig{}
	if cfg.RelayPort > 0 {
		// Public "relay" port (node-to-node)
		listeners = append(
			listeners,
			dingo.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: net.JoinHostPort(
					cfg.BindAddr,
					strconv.FormatUint(uint64(cfg.RelayPort), 10),
				),
				ReuseAddress: true,
			},
		)
	}
	if cfg.PrivatePort > 0 {
		// Private TCP port (node-to-client)
		listeners = append(
			listeners,
			dingo.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: net.JoinHostPort(
					cfg.PrivateBindAddr,
					strconv.FormatUint(uint64(cfg.PrivatePort), 10),
				),
				UseNtC: true,
			},
		)
	}
	if cfg.SocketPath != "" {
		// Private UNIX socket (node-to-client)
		listeners = append(
			listeners,
			dingo.ListenerConfig{
				ListenNetwork: "unix",
				ListenAddress: cfg.SocketPath,
				UseNtC:        true,
			},
		)
	}

	// Parse shutdown timeout
	shutdownTimeout := 30 * time.Second // Default timeout
	if cfg.ShutdownTimeout != "" {
		var err error
		shutdownTimeout, err = time.ParseDuration(cfg.ShutdownTimeout)
		if err != nil {
			return fmt.Errorf("invalid shutdown timeout: %w", err)
		}
	}
	// Use the package-level default to avoid drift.
	chainsyncStallTimeout := chainsync.DefaultStallTimeout
	if cfg.Chainsync.StallTimeout != "" {
		var err error
		chainsyncStallTimeout, err = time.ParseDuration(
			cfg.Chainsync.StallTimeout,
		)
		if err != nil {
			return fmt.Errorf(
				"invalid chainsync stall timeout: %w",
				err,
			)
		}
	}
	chainsyncStrategy, err := chainsync.ParseHeaderSyncStrategy(
		cfg.Chainsync.Strategy,
	)
	if err != nil {
		return fmt.Errorf("invalid chainsync strategy: %w", err)
	}

	// Validate storage mode
	storageMode := dingo.StorageMode(cfg.StorageMode)
	if storageMode == "" {
		storageMode = dingo.StorageModeCore
	}
	if !storageMode.Valid() {
		return fmt.Errorf(
			"invalid storage mode %q: must be %q or %q",
			cfg.StorageMode,
			dingo.StorageModeCore,
			dingo.StorageModeAPI,
		)
	}
	blockfrostPort := config.APIPluginPort(cfg.Plugins.API.Blockfrost)
	kupoPort := config.APIPluginPort(cfg.Plugins.API.Kupo)
	utxorpcPort := config.APIPluginPort(cfg.Plugins.API.Utxorpc)
	meshPort := config.APIPluginPort(cfg.Plugins.API.Mesh)
	mcpPort := config.APIPluginPort(cfg.Plugins.API.Mcp)
	logger.Info("storage mode",
		"mode", string(storageMode),
		"blockfrost", storageMode.IsAPI() && blockfrostPort > 0,
		"kupo", storageMode.IsAPI() && kupoPort > 0,
		"utxorpc", storageMode.IsAPI() && utxorpcPort > 0,
		"mesh", storageMode.IsAPI() && meshPort > 0,
		"mcp", mcpPort > 0,
		"midnight_indexing", cfg.Midnight.Enabled && storageMode.IsAPI(),
		"midnight_grpc", storageMode.IsAPI() &&
			cfg.Midnight.ServerEnabled && cfg.Midnight.Port > 0,
	)

	d, err := dingo.New(
		buildDingoConfig(
			cfg,
			logger,
			nodeCfg,
			listeners,
			peerSharing,
			storageMode,
			shutdownTimeout,
			chainsyncStallTimeout,
			chainsyncStrategy,
		),
	)
	if err != nil {
		return err
	}
	metricsServer := newMetricsServer(cfg)
	if metricsServer != nil {
		logger.Info(
			"serving prometheus metrics on "+metricsServer.Addr,
			"component",
			"node",
		)
	}
	// Optional debug listener with pprof handlers, on a separate port from
	// metrics so monitoring scrapers never see profiling endpoints.
	debugServer := newPprofDebugServer(cfg)
	if debugServer != nil {
		logger.Info(
			"serving pprof debug endpoints on "+debugServer.Addr,
			"component", "node",
		)
	}
	// Liveness/readiness listener, on a port of its own so an orchestrator
	// or load balancer can probe the node without being handed the metrics
	// or pprof surface. Started for every storage mode.
	healthServer := NewHealthServer(
		cfg, d.TipGapSlots, nodeHealthChecks(d)...,
	)
	if healthServer != nil {
		logger.Info(
			"serving health probes on "+healthServer.Addr,
			"component", "node",
		)
	}
	// Wait for interrupt/termination signal
	signalCtx, signalCtxStop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer signalCtxStop()
	// A block producer re-reads its credential files on SIGHUP. Relays leave
	// the signal at its default so their behaviour is unchanged.
	if cfg.BlockProducer {
		reloadSigs := make(chan os.Signal, 1)
		signal.Notify(reloadSigs, syscall.SIGHUP)
		defer signal.Stop(reloadSigs)
		go reloadOnSignal(
			signalCtx, reloadSigs, d.ReloadBlockProducerCredentials, logger,
		)
	}

	// Error channel for the node goroutine. The metrics, pprof debug and
	// health listeners are non-essential observability endpoints; their
	// bind/serve failures are logged but never queued here, so a port
	// conflict on them cannot take down the node.
	errChan := make(chan error, 1)
	if metricsServer != nil {
		if listener := bindAuxiliaryListener(
			"metrics", metricsServer, logger,
		); listener != nil {
			go serveAuxiliaryListenerOn(
				"metrics", metricsServer, listener, logger,
			)
		}
	}
	if debugServer != nil {
		if listener := bindAuxiliaryListener(
			"pprof debug", debugServer, logger,
		); listener != nil {
			go serveAuxiliaryListenerOn(
				"pprof debug", debugServer, listener, logger,
			)
		}
	}
	if healthServer != nil {
		if listener := bindAuxiliaryListener(
			"health", healthServer, logger,
		); listener != nil {
			go serveAuxiliaryListenerOn("health", healthServer, listener, logger)
		}
	}
	// A remote stop or restart ends the run exactly like a signal does;
	// waitForStop then tells them apart.
	go func() {
		select {
		case <-d.ShutdownRequests():
			signalCtxStop()
		case <-signalCtx.Done():
		}
	}()
	go func() {
		//nolint:contextcheck
		err := d.Run(signalCtx)
		if errors.Is(err, context.Canceled) {
			return
		}
		select {
		case errChan <- err:
		case <-signalCtx.Done():
		}
	}()

	// Wait for signal, remote request or error
	req, signaled, err := waitForStop(signalCtx, errChan, d.EndShutdownRequests)
	shutdown := func() error {
		return gracefulShutdown(
			logger,
			metricsServer,
			debugServer,
			healthServer,
			d,
			shutdownTimeout,
		)
	}
	if req != nil {
		logger.Info(
			"remote lifecycle request received, initiating graceful shutdown",
			"restart", req.Restart,
			"timeout", req.Timeout,
		)
		return runRequestedShutdown(*req, err, shutdown, dingo.ReExec)
	}
	if signaled {
		logger.Info("signal received, initiating graceful shutdown")
		if err := shutdown(); err != nil {
			return err
		}
		logger.Info("shutdown complete")
		return nil
	}

	if err == nil {
		logger.Info("node stopped")
		return shutdown()
	}

	logger.Error("node error", "error", err)
	signalCtxStop()

	cleanupErr := shutdownNodeResources(
		optionalShutdown(metricsServer),
		optionalShutdown(debugServer),
		optionalShutdown(healthServer),
		d.Stop,
		shutdownTimeout,
	)
	if cleanupErr != nil {
		logger.Error(
			"error cleanup failed",
			"error",
			cleanupErr,
			"node_error",
			err,
		)
		return errors.Join(err, cleanupErr)
	}

	return err
}

func applyRootPeerTargetFallback(cfg *config.Config, target int) {
	if cfg.TargetNumberOfRootPeers == 0 && target != 0 {
		cfg.TargetNumberOfRootPeers = target
	}
}

// forgeEBCap resolves an optional endorser-block cap. Load applies the
// defaults, so nil here means the Config was built directly rather than
// loaded; an explicit 0 is preserved and disables the cap.
func forgeEBCap(v *uint64, fallback uint64) uint64 {
	if v == nil {
		return fallback
	}
	return *v
}

// buildDingoConfig translates the loaded internal/config.Config, plus the
// values Run derives from it (the resolved cardano-node config, listeners,
// peer-sharing decision, storage mode, and parsed durations/strategy), into
// a dingo.Config. It is split out from Run so that the full field mapping
// -- including cfg.API, the shared api.tls policy defaults -- can
// be asserted directly in tests without needing to start the node.
func buildDingoConfig(
	cfg *config.Config,
	logger *slog.Logger,
	nodeCfg *cardano.CardanoNodeConfig,
	listeners []dingo.ListenerConfig,
	peerSharing bool,
	storageMode dingo.StorageMode,
	shutdownTimeout time.Duration,
	chainsyncStallTimeout time.Duration,
	chainsyncStrategy chainsync.HeaderSyncStrategy,
) dingo.Config {
	// Validated by config.Validate before the node starts, so a parse
	// failure here leaves the zero value and selects the default.
	localStateQueryViewMaxLifetime, _ := time.ParseDuration(
		cfg.LocalStateQueryViewMaxLifetime,
	)
	return dingo.NewConfig(
		dingo.WithIntersectTip(cfg.IntersectTip),
		dingo.WithLogger(logger),
		dingo.WithDatabasePath(cfg.DatabasePath),
		dingo.WithPluginSelection(
			plugin.CapabilityStorageBlob,
			cfg.Plugins.Storage.Blob,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityStorageMetadata,
			cfg.Plugins.Storage.Metadata,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityMempool,
			cfg.Plugins.Mempool,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityAPIBlockfrost,
			cfg.Plugins.API.Blockfrost,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityAPIKupo,
			cfg.Plugins.API.Kupo,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityAPIMesh,
			cfg.Plugins.API.Mesh,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityAPIUtxorpc,
			cfg.Plugins.API.Utxorpc,
		),
		dingo.WithPluginSelection(
			plugin.CapabilityAPIMcp,
			cfg.Plugins.API.Mcp,
		),
		dingo.WithNetwork(cfg.Network),
		dingo.WithNetworkMagic(cfg.NetworkMagic),
		dingo.WithCardanoNodeConfig(nodeCfg),
		dingo.WithListeners(listeners...),
		dingo.WithOutboundSourcePort(cfg.RelayPort),
		dingo.WithPeerSharing(peerSharing),
		dingo.WithUtxorpcTlsCertFilePath(cfg.TlsCertFilePath),
		dingo.WithUtxorpcTlsKeyFilePath(cfg.TlsKeyFilePath),
		dingo.WithAPIConfig(cfg.API),
		dingo.WithBarkBaseUrl(cfg.BarkBaseUrl),
		dingo.WithBarkBlockDownloadHosts(cfg.BarkBlockDownloadHosts),
		dingo.WithBarkPort(cfg.BarkPort),
		dingo.WithBarkHost(cfg.BarkHost),
		dingo.WithBarkClientCAFilePath(cfg.BarkClientCAFilePath),
		dingo.WithBarkArchiveMaxConcurrentFetches(
			cfg.BarkArchiveMaxConcurrentFetches,
		),
		dingo.WithBarkOperatorCertificateFingerprints(
			cfg.BarkOperatorCertificateFingerprints,
		),
		dingo.WithBarkLifecycleEnabled(cfg.BarkLifecycleEnabled),
		dingo.WithBarkLifecycleOperatorCertificateFingerprints(
			cfg.BarkLifecycleOperatorCertificateFingerprints,
		),
		dingo.WithHistoryExpiry(dingo.HistoryExpiryConfig{
			Enabled:   cfg.HistoryExpiry.Enabled,
			Frequency: cfg.HistoryExpiry.Frequency,
		}),
		dingo.WithKoiosParity(dingo.KoiosParityConfig{
			Enabled:               cfg.KoiosParity.Enabled,
			Network:               cfg.KoiosParity.Network,
			CachePath:             cfg.KoiosParity.CachePath,
			APIKey:                cfg.KoiosParity.APIKey,
			BaseURL:               cfg.KoiosParity.BaseURL,
			AllowInsecureHTTP:     cfg.KoiosParity.AllowInsecureHTTP,
			AllowPrivateAddresses: cfg.KoiosParity.AllowPrivateAddresses,
			Strict:                cfg.KoiosParity.Strict,
			GraceHours:            cfg.KoiosParity.GraceHours,
			Accounts:              &cfg.KoiosParity.Accounts,
			// AccountChunkSize and AccountChunkMaxBytes were omitted here
			// while every other KoiosParity field was forwarded, so
			// --koios-parity-account-chunk-size and
			// --koios-parity-account-chunk-max-bytes silently did nothing on
			// the serve path and the package defaults always won.
			AccountChunkSize:     cfg.KoiosParity.AccountChunkSize,
			AccountChunkMaxBytes: cfg.KoiosParity.AccountChunkMaxBytes,
		}),
		dingo.WithCORSAllowedOrigins(cfg.CORSAllowedOrigins),
		dingo.WithOffchainMetadataConfig(
			dingo.OffchainMetadataConfig{
				Interval: cfg.OffchainMetadata.Interval,
				RequestTimeout: cfg.OffchainMetadata.
					RequestTimeout,
				UserAgent: cfg.OffchainMetadata.UserAgent,
				IPFSGatewayURL: cfg.OffchainMetadata.
					IPFSGatewayURL,
				BatchSize: cfg.OffchainMetadata.BatchSize,
				MaxBytes:  cfg.OffchainMetadata.MaxBytes,
				AllowPrivateAddresses: cfg.OffchainMetadata.
					AllowPrivateAddresses,
			},
		),
		dingo.WithTokenRegistryConfig(
			dingo.TokenRegistryConfig{
				Enabled:   cfg.TokenRegistry.Enabled,
				SourceURL: cfg.TokenRegistry.SourceURL,
				Interval:  cfg.TokenRegistry.Interval,
				RequestTimeout: cfg.TokenRegistry.
					RequestTimeout,
				UserAgent: cfg.TokenRegistry.UserAgent,
				Headers:   cfg.TokenRegistry.HeaderSecrets,
				MaxBytes:  cfg.TokenRegistry.MaxBytes,
				MaxDecompressedBytes: cfg.TokenRegistry.
					MaxDecompressedBytes,
				MaxEntryBytes: cfg.TokenRegistry.
					MaxEntryBytes,
				MaxArchiveEntries: cfg.TokenRegistry.
					MaxArchiveEntries,
				MaxAcceptedEntries: cfg.TokenRegistry.
					MaxAcceptedEntries,
				MaxBatchBytes: cfg.TokenRegistry.
					MaxBatchBytes,
				StoreLogos: cfg.TokenRegistry.StoreLogos,
				AllowPrivateAddresses: cfg.TokenRegistry.
					AllowPrivateAddresses,
			},
		),
		dingo.WithMidnightConfig(dingo.MidnightConfig{
			Enabled:                     cfg.Midnight.Enabled,
			ServerEnabled:               cfg.Midnight.ServerEnabled,
			ReflectionEnabled:           cfg.Midnight.ReflectionEnabled,
			Port:                        cfg.Midnight.Port,
			Host:                        cfg.Midnight.Host,
			CNightPolicyID:              cfg.Midnight.CNightPolicyID,
			CNightAssetName:             cfg.Midnight.CNightAssetName,
			MappingValidatorAddress:     cfg.Midnight.MappingValidatorAddress,
			AuthTokenPolicyID:           cfg.Midnight.AuthTokenPolicyID,
			AuthTokenAssetName:          cfg.Midnight.AuthTokenAssetName,
			CommitteeCandidateAddress:   cfg.Midnight.CommitteeCandidateAddress,
			TechnicalCommitteeAddress:   cfg.Midnight.TechnicalCommitteeAddress,
			TechnicalCommitteePolicyID:  cfg.Midnight.TechnicalCommitteePolicyID,
			CouncilAddress:              cfg.Midnight.CouncilAddress,
			CouncilPolicyID:             cfg.Midnight.CouncilPolicyID,
			PermissionedCandidatePolicy: cfg.Midnight.PermissionedCandidatePolicy,
		}),
		dingo.WithValidateHistorical(cfg.ValidateHistorical),
		dingo.WithStrictUtxoValidation(cfg.StrictUtxoValidation),
		dingo.WithRunMode(string(cfg.RunMode)),
		dingo.WithStartEra(string(cfg.StartEra)),
		dingo.WithShutdownTimeout(shutdownTimeout),
		dingo.WithLocalStateQueryViewMaxLifetime(localStateQueryViewMaxLifetime),
		// Enable metrics with default prometheus registry
		dingo.WithPrometheusRegistry(prometheus.DefaultRegisterer),
		dingo.WithTracing(cfg.Tracing),
		dingo.WithTracingStdout(cfg.TracingStdout),
		dingo.WithTopologyConfig(config.GetTopologyConfig()),
		dingo.WithDatabaseWorkerPoolConfig(ledger.DatabaseWorkerPoolConfig{
			WorkerPoolSize: cfg.DatabaseWorkers,
			TaskQueueSize:  cfg.DatabaseQueueSize,
			Disabled:       false,
		}),
		dingo.WithPeerTargets(
			cfg.TargetNumberOfKnownPeers,
			cfg.TargetNumberOfEstablishedPeers,
			cfg.TargetNumberOfActivePeers,
		),
		dingo.WithRootPeerTarget(cfg.TargetNumberOfRootPeers),
		dingo.WithGenesisBootstrap(cfg.GenesisBootstrap.Enabled),
		dingo.WithGenesisWindowSlots(cfg.GenesisBootstrap.WindowSlots),
		dingo.WithGenesisCorroborationPeers(
			cfg.GenesisBootstrap.CorroborationPeers,
		),
		dingo.WithGenesisLimitOnPatience(
			cfg.GenesisBootstrap.LimitOnPatienceEnabled,
			cfg.GenesisBootstrap.LimitOnPatienceCapacity,
			cfg.GenesisBootstrap.LimitOnPatienceRate,
		),
		dingo.WithBootstrapPromotionMinDiversityGroups(
			cfg.GenesisBootstrap.PromotionMinDiversityGroups,
		),
		dingo.WithActivePeersQuotas(
			cfg.ActivePeersTopologyQuota,
			cfg.ActivePeersGossipQuota,
			cfg.ActivePeersLedgerQuota,
		),
		dingo.WithMinHotPeers(cfg.MinHotPeers),
		dingo.WithReconcileInterval(cfg.ReconcileInterval),
		dingo.WithInactivityTimeout(cfg.InactivityTimeout),
		dingo.WithInboundPeerGovernance(
			cfg.InboundWarmTarget,
			cfg.InboundHotQuota,
			cfg.InboundMinTenure,
			cfg.InboundHotScoreThreshold,
			cfg.InboundPruneAfter,
			cfg.InboundDuplexOnlyForHot,
			cfg.InboundCooldown,
		),
		dingo.WithMaxConnectionsPerIP(cfg.MaxConnectionsPerIP),
		dingo.WithMaxInboundConns(cfg.MaxInboundConns),
		dingo.WithMaxNtCConns(cfg.MaxNtCConns),
		dingo.WithMaxNtCConnectionsPerIP(cfg.MaxNtCConnectionsPerIP),
		dingo.WithMaxTrustedLocalNtCConns(cfg.MaxTrustedLocalNtCConns),
		dingo.WithSkipRewardLiveStakeBackfillCheck(cfg.SkipRewardLiveStakeBackfillCheck),
		dingo.WithCacheConfig(
			cfg.Cache.BlockLRUEntries,
			cfg.Cache.HotUtxoEntries,
			cfg.Cache.HotTxEntries,
			cfg.Cache.HotTxMaxBytes,
		),
		dingo.WithChainsyncMaxClients(
			cfg.Chainsync.MaxClients,
		),
		dingo.WithChainsyncStallTimeout(
			chainsyncStallTimeout,
		),
		dingo.WithChainsyncHeaderStrategy(
			chainsyncStrategy,
		),
		dingo.WithBindAddr(cfg.BindAddr),
		dingo.WithStorageMode(storageMode),
		// CIP-23 minimum pool margin (consensus-affecting)
		dingo.WithMinPoolMargin(cfg.MinPoolMargin),
		// CIP-50 pledge-leverage staking rewards (consensus-affecting)
		dingo.WithPledgeLeverage(
			cfg.PledgeLeverageEnabled,
			cfg.PledgeLeverage,
		),
		// CIP-0163 full-pot reward distribution (consensus-affecting)
		dingo.WithFullPotRewards(cfg.FullPotRewardsEnabled),
		dingo.WithUnsafeFullPotRewardsOnStandardNetworks(
			cfg.UnsafeFullPotRewardsOnStandardNetworks,
		),
		// Block production (SPO mode)
		dingo.WithBlockProducer(cfg.BlockProducer),
		dingo.WithShelleyVRFKey(cfg.ShelleyVRFKey),
		dingo.WithShelleyKESKey(cfg.ShelleyKESKey),
		dingo.WithShelleyOperationalCertificate(
			cfg.ShelleyOperationalCertificate,
		),
		// node_forging.go gates agent-backed KES signing on a non-empty
		// socket path, so dropping any of these three silently falls back
		// to local-file signing on the serve path.
		dingo.WithShelleyKESAgentSocket(cfg.ShelleyKESAgentSocket),
		dingo.WithShelleyKESAgentMode(cfg.ShelleyKESAgentMode),
		dingo.WithShelleyKESAgentSignTimeout(
			cfg.ShelleyKESAgentSignTimeout,
		),
		dingo.WithForgeSyncToleranceSlots(
			cfg.ForgeSyncToleranceSlots,
		),
		dingo.WithForgeStaleGapThresholdSlots(
			cfg.ForgeStaleGapThresholdSlots,
		),
		dingo.WithForgeUpstreamStalenessSlots(
			cfg.ForgeUpstreamStalenessSlots,
		),
		dingo.WithForgeAppliedTipStalenessSlots(
			cfg.ForgeAppliedTipStalenessSlots,
		),
		dingo.WithForgeEndorserBlockStalenessSlots(
			cfg.ForgeEndorserBlockStalenessSlots,
		),
		dingo.WithForgeEBSelectionReserve(cfg.ForgeEBSelectionReserve),
		dingo.WithForgeEBMaxTxRefs(
			forgeEBCap(cfg.ForgeEBMaxTxRefs, config.DefaultForgeEBMaxTxRefs),
		),
		dingo.WithForgeEBMaxBytes(
			forgeEBCap(cfg.ForgeEBMaxBytes, config.DefaultForgeEBMaxBytes),
		),
		dingo.WithValidateForgedBlock(cfg.ValidateForgedBlock),
		// Parallel block-decode pipeline (decode and validate stages). Not
		// consensus-affecting; off by default.
		dingo.WithBlockPipelineEnabled(cfg.BlockPipelineEnabled),
		dingo.WithBlockPipelineValidateEnabled(
			cfg.BlockPipelineValidateEnabled,
		),
		dingo.WithLedgerPrefetchAheadEnabled(cfg.LedgerPrefetchAheadEnabled),
		dingo.WithLedgerApplyRowBatchingEnabled(
			cfg.LedgerApplyRowBatchingEnabled,
		),
		// CIP-0163 reward-account inactivity expiry (consensus-affecting)
		dingo.WithDelegatorInactivity(
			cfg.DelegatorInactivityEnabled,
			cfg.DelegatorInactivity,
		),
		dingo.WithDatabaseLifecycle(cfg.DatabaseLifecycle),
		// Leios voting (experimental)
		dingo.WithLeiosVoteSigningKeyFile(
			cfg.LeiosVoteSigningKeyFile,
		),
	)
}
