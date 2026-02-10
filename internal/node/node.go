// Copyright 2025 Blink Labs Software
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
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // #nosec G108
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
	if cardanoConfigPath == "" {
		network := cfg.Network
		if network == "" {
			network = "preview"
		}
		cardanoConfigPath = network + "/config.json"
	}

	var nodeCfg *cardano.CardanoNodeConfig
	var err error
	nodeCfg, err = cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		cfg.Network,
		cardano.EmbeddedConfigPreviewNetworkFS,
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
			dingo.WithCardanoNodeConfig(nodeCfg),
			dingo.WithListeners(listeners...),
			dingo.WithOutboundSourcePort(cfg.RelayPort),
			dingo.WithUtxorpcPort(cfg.UtxorpcPort),
			dingo.WithUtxorpcTlsCertFilePath(cfg.TlsCertFilePath),
			dingo.WithUtxorpcTlsKeyFilePath(cfg.TlsKeyFilePath),
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
			dingo.WithActivePeersQuotas(
				cfg.ActivePeersTopologyQuota,
				cfg.ActivePeersGossipQuota,
				cfg.ActivePeersLedgerQuota,
			),
			dingo.WithChainsyncMaxClients(
				cfg.Chainsync.MaxClients,
			),
			dingo.WithChainsyncStallTimeout(
				chainsyncStallTimeout,
			),
		),
	)
	if err != nil {
		return err
	}
	// Metrics and debug listener
	http.Handle("/metrics", promhttp.Handler())
	logger.Info(
		"serving prometheus metrics on "+fmt.Sprintf(
			"%s:%d",
			cfg.BindAddr,
			cfg.MetricsPort,
		),
		"component",
		"node",
	)
	metricsServer := &http.Server{
		Addr: fmt.Sprintf(
			"%s:%d",
			cfg.BindAddr,
			cfg.MetricsPort,
		),
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			logger.Error(
				fmt.Sprintf("failed to start metrics listener: %s", err),
				"component", "node",
			)
			os.Exit(1)
		}
	}()
	// Wait for interrupt/termination signal
	signalCtx, signalCtxStop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer signalCtxStop()

	// Run node in goroutine
	errChan := make(chan error, 1)
	go func() {
		//nolint:contextcheck
		err := d.Run(signalCtx)
		select {
		case errChan <- err:
		case <-signalCtx.Done():
		}
	}()

	// Wait for signal or error
	select {
	case <-signalCtx.Done():
		logger.Info("signal received, initiating graceful shutdown")

		// Shutdown metrics server
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			shutdownTimeout,
		)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("metrics server shutdown error", "error", err)
		}

		// Shutdown node
		if err := d.Stop(); err != nil {
			logger.Error("shutdown errors occurred", "error", err)
			return err
		}
		logger.Info("shutdown complete")
		return nil

	case err := <-errChan:
		if err == nil {
			logger.Info("node stopped")
			// Graceful cleanup
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				shutdownTimeout,
			)
			defer cancel()
			if err := metricsServer.Shutdown(shutdownCtx); err != nil {
				logger.Error("metrics server shutdown error", "error", err)
			}
			if err := d.Stop(); err != nil {
				logger.Error("shutdown errors occurred", "error", err)
				return err
			}
			return nil
		}
		logger.Error("node error", "error", err)
		signalCtxStop()

		// Shutdown node resources
		if stopErr := d.Stop(); stopErr != nil {
			logger.Error(
				"shutdown errors occurred during error cleanup",
				"error",
				stopErr,
			)
		}

		// Cleanup on error
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			shutdownTimeout,
		)
		defer cancel()
		if shutdownErr := metricsServer.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("metrics server shutdown error", "error", shutdownErr)
		}

		return err
	}
}
