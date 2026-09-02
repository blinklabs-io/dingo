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

// node-parity compares Dingo's and a reference cardano-node's ledger state
// (protocol parameters, stake distribution, and the whole UTxO set) over
// their node-to-client LocalStateQuery interfaces, on preview or preprod
// (blinklabs-io/dingo#1900).
//
// It does not start, stop, or manage either node: point it at two
// already-running, already-synced NtC listeners with --dingo-addr and
// --cardano-addr (a host:port for a TCP-exposed endpoint, or a leading-"/"
// Unix socket path for a real cardano-node). `check` runs one comparison
// cycle; `watch` repeats it on an interval, matching cmd/koios-parity's
// on-demand-check-plus-polling-watch shape rather than a persistent,
// block-reactive stream.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/spf13/cobra"
)

const programName = "node-parity"

// defaultMetricsAddr matches this tool's docs/dashboards/prometheus.yaml
// example scrape target. Port 9464 (the OpenTelemetry Prometheus exporter's
// conventional default) is used rather than cardano-node's own 12798, since
// a node-parity process is not a Cardano node and its metrics job is meant
// to sit alongside, not collide with, the dingo/cardano-node jobs.
const defaultMetricsAddr = ":9464"

// globalFlags are shared across every subcommand and the default (no
// subcommand) action.
var globalFlags struct {
	network     string
	dingoAddr   string
	cardanoAddr string
	metricsAddr string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	rootCmd := &cobra.Command{
		Use:   programName,
		Short: "Compare Dingo's ledger state against a reference cardano-node",
		Long: `node-parity compares Dingo's and cardano-node's ledger state (protocol
parameters, stake distribution, whole UTxO set) over LocalStateQuery, on
preview or preprod.

Default action (no subcommand): run one check cycle and print the result,
same as 'check'.

This tool never pins a specific historical block: Dingo's LocalStateQuery
Acquire always answers at its live tip (blinklabs-io/dingo#382), so every
check instead confirms both nodes agree on a tip before and after the
query, discarding (not failing) the cycle if they don't.`,
		RunE: checkRun,
		// This tool logs structured JSON via slog; cobra's own plain-text
		// "Error: ..." plus a full usage dump on every runtime failure (as
		// opposed to a genuine flag-parsing mistake) would double the error
		// output and read oddly alongside it, so both are silenced here and
		// main logs the returned error itself.
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.PersistentFlags().StringVar(
		&globalFlags.network, "network", "",
		"cardano network: preview or preprod",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.dingoAddr, "dingo-addr", "",
		"Dingo's node-to-client address (host:port, or a leading-\"/\" Unix socket path)",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.cardanoAddr, "cardano-addr", "",
		"cardano-node's node-to-client address (host:port, or a leading-\"/\" Unix socket path)",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.metricsAddr, "metrics-addr", defaultMetricsAddr,
		"address to serve Prometheus /metrics on for 'watch' (empty disables it; unused by 'check')",
	)

	rootCmd.AddCommand(checkCommand())
	rootCmd.AddCommand(watchCommand())

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

// requireNetwork validates --network the same way cmd/koios-parity does:
// this tool is scoped to preview/preprod only.
func requireNetwork() (string, error) {
	switch globalFlags.network {
	case "preview", "preprod":
		return globalFlags.network, nil
	case "":
		return "", errors.New("--network is required (preview or preprod)")
	default:
		return "", fmt.Errorf(
			"--network must be 'preview' or 'preprod', got %q",
			globalFlags.network,
		)
	}
}

// requireAddrs validates that both node addresses were supplied; neither has
// a sane default since this tool does not manage node lifecycle.
func requireAddrs() error {
	if globalFlags.dingoAddr == "" {
		return errors.New("--dingo-addr is required")
	}
	if globalFlags.cardanoAddr == "" {
		return errors.New("--cardano-addr is required")
	}
	return nil
}

// networkMagic resolves a network name to its Ouroboros network magic, the
// same lookup the dingo binary itself uses (internal/config/config.go).
func networkMagic(network string) (uint32, error) {
	n, ok := ouroboros.NetworkByName(network)
	if !ok {
		return 0, fmt.Errorf("unknown network: %s", network)
	}
	return n.NetworkMagic, nil
}
