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
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/config"
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
	if _, err := os.Stat(cfg.SocketPath); err == nil {
		os.Remove(cfg.SocketPath)
	}
	var nodeCfg *cardano.CardanoNodeConfig
	if cfg.CardanoConfig != "" {
		tmpCfg, err := cardano.NewCardanoNodeConfigFromFile(cfg.CardanoConfig)
		if err != nil {
			return err
		}
		nodeCfg = tmpCfg
		logger.Debug(
			fmt.Sprintf(
				"cardano network config: %+v",
				nodeCfg,
			),
			"component", "node",
		)
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
	d, err := dingo.New(
		dingo.NewConfig(
			dingo.WithIntersectTip(cfg.IntersectTip),
			dingo.WithLogger(logger),
			dingo.WithDatabasePath(cfg.DatabasePath),
			dingo.WithBadgerCacheSize(cfg.BadgerCacheSize),
			dingo.WithMempoolCapacity(cfg.MempoolCapacity),
			dingo.WithNetwork(cfg.Network),
			dingo.WithCardanoNodeConfig(nodeCfg),
			dingo.WithListeners(listeners...),
			dingo.WithOutboundSourcePort(cfg.RelayPort),
			dingo.WithUtxorpcPort(cfg.UtxorpcPort),
			dingo.WithUtxorpcTlsCertFilePath(cfg.TlsCertFilePath),
			dingo.WithUtxorpcTlsKeyFilePath(cfg.TlsKeyFilePath),
			dingo.WithDevMode(cfg.DevMode),
			// Enable metrics with default prometheus registry
			dingo.WithPrometheusRegistry(prometheus.DefaultRegisterer),
			// TODO: make this configurable (#387)
			// dingo.WithTracing(true),
			dingo.WithTopologyConfig(config.GetTopologyConfig()),
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
	go func() {
		debugger := &http.Server{
			Addr: fmt.Sprintf(
				"%s:%d",
				cfg.BindAddr,
				cfg.MetricsPort,
			),
			ReadHeaderTimeout: 60 * time.Second,
		}
		if err := debugger.ListenAndServe(); err != nil {
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
	go func() {
		<-signalCtx.Done()
		logger.Info("signal received, shutting down")
		if err := d.Stop(); err != nil { //nolint:contextcheck
			logger.Error(
				"failure(s) while shutting down",
				"error",
				err,
			)
		}
		os.Exit(0)
	}()
	// Run node
	if err := d.Run(); err != nil {
		return err
	}
	return nil
}
