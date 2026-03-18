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

// Command txpump submits randomised batches of Cardano ADA payment
// transactions to a node via the Ouroboros N2C LocalTxSubmission protocol.
// It is designed for Antithesis property-based testing: crypto/rand is used
// for all randomness so that Antithesis can substitute a deterministic source
// via /dev/urandom and replay runs exactly.
//
// Configuration is read entirely from environment variables; see
// internal/txpump/config.go for the full list.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/blinklabs-io/dingo/internal/test/antithesis/internal/txpump"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := txpump.LoadConfig()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	txlog, err := txpump.NewTxLogger(cfg.LogDir)
	if err != nil {
		logger.Error("failed to open tx log", "err", err)
		os.Exit(1)
	}
	defer txlog.Close() //nolint:errcheck // best-effort flush

	wallet := txpump.NewWallet()
	if cfg.GenesisUTxOFile != "" {
		utxos, loadErr := txpump.LoadGenesisUTxOs(cfg.GenesisUTxOFile)
		if loadErr != nil {
			logger.Error("failed to load genesis UTxOs", "err", loadErr)
			os.Exit(1)
		}
		wallet.Add(utxos...)
		logger.Info(
			"seeded wallet from genesis UTxOs",
			"count", len(utxos),
			"path", cfg.GenesisUTxOFile,
		)
	}

	if wallet.Len() == 0 {
		logger.Error("wallet is empty; set TXPUMP_GENESIS_UTXO_FILE to seed initial UTxOs")
		os.Exit(1)
	}

	pump := txpump.NewPump(cfg, wallet, logger, txlog)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	logger.Info(
		"txpump starting",
		"node_addr", cfg.NodeAddr,
		"network_magic", cfg.NetworkMagic,
		"tx_count_min", cfg.TxCountMin,
		"tx_count_max", cfg.TxCountMax,
		"types", cfg.Types,
	)

	runErr := pump.Run(ctx)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("pump exited with error", "err", runErr)
		os.Exit(1)
	}

	logger.Info("txpump stopped")
}
