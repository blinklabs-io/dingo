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
// cycle; `watch` follows both nodes' ChainSync feeds and runs a check the
// moment either one's tip changes, with --fallback-interval as a backstop
// safety net in case a watcher's subscription silently stalls, rather than
// polling on a fixed clock alone.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
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
	atSlot      uint64
	atHash      string
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

By default, each check confirms both nodes agree on a tip and pins the
comparison to that point (blinklabs-io/dingo#382). Pass --at-slot and
--at-hash together to instead compare at an explicit historical block,
regardless of where either node's live tip currently is -- this is what
lets a caller fall behind and still walk through specific past blocks one
at a time.`,
		Args: cobra.NoArgs,
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
	rootCmd.PersistentFlags().Uint64Var(
		&globalFlags.atSlot, "at-slot", 0,
		"explicit historical mode (check only): compare at this exact slot instead of the live tip -- requires --at-hash",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.atHash, "at-hash", "",
		"explicit historical mode (check only): hex-encoded block hash at --at-slot, disambiguating it across a fork/rollback",
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

// requireAtPoint validates --at-slot/--at-hash: both-or-neither, since a
// historical point (blinklabs-io/dingo#382) needs both the slot and the
// hash to be unambiguous across a fork/rollback (see Tip.point). Returns
// nil, nil when neither flag was set, meaning "live-tip-agreement mode" --
// Check's existing default behavior.
func requireAtPoint() (*nodeparity.Tip, error) {
	if globalFlags.atSlot == 0 && globalFlags.atHash == "" {
		return nil, nil
	}
	if globalFlags.atSlot == 0 || globalFlags.atHash == "" {
		return nil, errors.New(
			"--at-slot and --at-hash must both be set, or neither",
		)
	}
	decoded, err := hex.DecodeString(globalFlags.atHash)
	if err != nil {
		return nil, fmt.Errorf("--at-hash: %w", err)
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf(
			"--at-hash: must decode to 32 bytes, got %d",
			len(decoded),
		)
	}
	return &nodeparity.Tip{
		Slot: globalFlags.atSlot,
		Hash: globalFlags.atHash,
	}, nil
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
