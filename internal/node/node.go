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
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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

func gracefulShutdown(
	logger *slog.Logger,
	metricsServer *http.Server,
	d *dingo.Node,
	timeout time.Duration,
) error {
	shutdownErr := shutdownNodeResources(
		metricsServer.Shutdown,
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

func shutdownNodeResources(
	metricsServerShutdown func(context.Context) error,
	nodeStop func() error,
	timeout time.Duration,
) error {
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		timeout,
	)
	defer cancel()
	var err error
	if shutdownErr := metricsServerShutdown(shutdownCtx); shutdownErr != nil {
		err = errors.Join(
			err,
			fmt.Errorf("metrics server shutdown: %w", shutdownErr),
		)
	}
	if stopErr := nodeStop(); stopErr != nil {
		err = errors.Join(
			err,
			fmt.Errorf("node stop: %w", stopErr),
		)
	}
	return err
}

func Run(cfg *config.Config, logger *slog.Logger) error {
	logger.Debug(fmt.Sprintf("config: %+v", cfg), "component", "node")
	logger.Debug(
		fmt.Sprintf("topology: %+v", config.GetTopologyConfig()),
		"component", "node",
	)
	// TODO: make this safer, check PID, create parent, etc. (#276)
	if runtime.GOOS != "windows" {
		if _, err := os.Stat(cfg.SocketPath); err == nil {
			os.Remove(cfg.SocketPath)
		}
	}
	// Derive default config path from cfg.Network when cfg.CardanoConfig is empty
	cardanoConfigPath := cfg.CardanoConfig
	network := cfg.Network
	if cardanoConfigPath == "" {
		if network == "" {
			network = "preview"
		}
		cardanoConfigPath = network + "/config.json"
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
	// Dingo-native config (dingo.yaml / env) does not specify them.
	// Priority: dingo.yaml/env > cardano config.json > peergov defaults.
	if nodeCfg != nil {
		rp, kp, ep, ap := nodeCfg.P2PTargets()
		if cfg.TargetNumberOfKnownPeers == 0 && kp > 0 {
			cfg.TargetNumberOfKnownPeers = kp
		}
		if cfg.TargetNumberOfEstablishedPeers == 0 && ep > 0 {
			cfg.TargetNumberOfEstablishedPeers = ep
		}
		if cfg.TargetNumberOfActivePeers == 0 && ap > 0 {
			cfg.TargetNumberOfActivePeers = ap
		}
		_ = rp // TargetNumberOfRootPeers not yet wired to peergov
	}

	listeners := []dingo.ListenerConfig{}
	if cfg.RelayPort > 0 {
		// Public "relay" port (node-to-node)
		listeners = append(
			listeners,
			dingo.ListenerConfig{
				ListenNetwork: "tcp",
				ListenAddress: fmt.Sprintf(
					"%s:%d",
					cfg.BindAddr,
					cfg.RelayPort,
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
				ListenAddress: fmt.Sprintf(
					"%s:%d",
					cfg.PrivateBindAddr,
					cfg.PrivatePort,
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
	// Dev mode always uses API storage for full transaction metadata
	if cfg.RunMode.IsDevMode() && !storageMode.IsAPI() {
		logger.Info(
			"dev mode: overriding storage mode to api",
			"previous", string(storageMode),
		)
		storageMode = dingo.StorageModeAPI
	}
	logger.Info("storage mode",
		"mode", string(storageMode),
		"blockfrost", storageMode.IsAPI() && cfg.BlockfrostPort > 0,
		"utxorpc", storageMode.IsAPI() && cfg.UtxorpcPort > 0,
		"mesh", storageMode.IsAPI() && cfg.MeshPort > 0,
	)

	d, err := dingo.New(
		dingo.NewConfig(
			dingo.WithIntersectTip(cfg.IntersectTip),
			dingo.WithLogger(logger),
			dingo.WithDatabasePath(cfg.DatabasePath),
			dingo.WithBlobPlugin(cfg.BlobPlugin),
			dingo.WithMetadataPlugin(cfg.MetadataPlugin),
			dingo.WithMempoolCapacity(cfg.MempoolCapacity),
			dingo.WithEvictionWatermark(cfg.EvictionWatermark),
			dingo.WithRejectionWatermark(cfg.RejectionWatermark),
			dingo.WithNetwork(cfg.Network),
			dingo.WithNetworkMagic(cfg.NetworkMagic),
			dingo.WithCardanoNodeConfig(nodeCfg),
			dingo.WithListeners(listeners...),
			dingo.WithOutboundSourcePort(cfg.RelayPort),
			dingo.WithPeerSharing(cfg.PeerSharing),
			dingo.WithUtxorpcPort(cfg.UtxorpcPort),
			dingo.WithUtxorpcTlsCertFilePath(cfg.TlsCertFilePath),
			dingo.WithUtxorpcTlsKeyFilePath(cfg.TlsKeyFilePath),
			dingo.WithBarkBaseUrl(cfg.BarkBaseUrl),
			dingo.WithBarkSecurityWindow(cfg.BarkSecurityWindow),
			dingo.WithBarkPort(cfg.BarkPort),
			dingo.WithValidateHistorical(cfg.ValidateHistorical),
			dingo.WithRunMode(string(cfg.RunMode)),
			dingo.WithShutdownTimeout(shutdownTimeout),
			// Enable metrics with default prometheus registry
			dingo.WithPrometheusRegistry(prometheus.DefaultRegisterer),
			// TODO: make this configurable (#387)
			// dingo.WithTracing(true),
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
			dingo.WithGenesisBootstrap(cfg.GenesisBootstrap.Enabled),
			dingo.WithGenesisWindowSlots(cfg.GenesisBootstrap.WindowSlots),
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
			dingo.WithBindAddr(cfg.BindAddr),
			dingo.WithBlockfrostPort(cfg.BlockfrostPort),
			dingo.WithMeshPort(cfg.MeshPort),
			dingo.WithStorageMode(storageMode),
			// Block production (SPO mode)
			dingo.WithBlockProducer(cfg.BlockProducer),
			dingo.WithShelleyVRFKey(cfg.ShelleyVRFKey),
			dingo.WithShelleyKESKey(cfg.ShelleyKESKey),
			dingo.WithShelleyOperationalCertificate(
				cfg.ShelleyOperationalCertificate,
			),
			dingo.WithForgeSyncToleranceSlots(
				cfg.ForgeSyncToleranceSlots,
			),
			dingo.WithForgeStaleGapThresholdSlots(
				cfg.ForgeStaleGapThresholdSlots,
			),
		),
	)
	if err != nil {
		return err
	}
	// Metrics listener with dedicated mux to avoid exposing
	// pprof or other handlers registered on DefaultServeMux.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsAddr := fmt.Sprintf(
		"%s:%d",
		cfg.BindAddr,
		cfg.MetricsPort,
	)
	logger.Info(
		"serving prometheus metrics on "+metricsAddr,
		"component",
		"node",
	)
	metricsServer := &http.Server{
		Addr:              metricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	// Wait for interrupt/termination signal
	signalCtx, signalCtxStop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer signalCtxStop()

	// Error channel for both node and metrics goroutines
	errChan := make(chan error, 2)
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			logger.Error(
				fmt.Sprintf("failed to start metrics listener: %s", err),
				"component", "node",
			)
			errChan <- fmt.Errorf("metrics server: %w", err)
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

	// Wait for signal or error
	err, signaled := waitForSignalOrError(signalCtx, errChan)
	if signaled {
		logger.Info("signal received, initiating graceful shutdown")

		if err := gracefulShutdown(
			logger,
			metricsServer,
			d,
			shutdownTimeout,
		); err != nil {
			return err
		}
		logger.Info("shutdown complete")
		return nil
	}

	if err == nil {
		logger.Info("node stopped")
		if err := gracefulShutdown(
			logger,
			metricsServer,
			d,
			shutdownTimeout,
		); err != nil {
			return err
		}
		return nil
	}

	logger.Error("node error", "error", err)
	signalCtxStop()

	cleanupErr := shutdownNodeResources(
		metricsServer.Shutdown,
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
