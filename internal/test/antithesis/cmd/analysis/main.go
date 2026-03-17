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

// Command analysis reads Cardano node log files from a shared volume and
// reports Antithesis property-based assertions (safety and liveness) on a
// configurable ticker.  When built without the "antithesis" build tag the
// assertions are written to stderr as structured JSON logs so that the binary
// remains useful outside the Antithesis platform.
//
// Configuration is read entirely from environment variables; see
// internal/analysis/config.go for the full list.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/blinklabs-io/dingo/internal/test/antithesis/internal/analysis"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := analysis.LoadConfig()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	logger.Info(
		"analysis binary starting",
		"log_dir", cfg.LogDir,
		"initial_wait_s", int(cfg.InitialWait.Seconds()),
		"check_interval_s", int(cfg.CheckInterval.Seconds()),
		"max_fork_depth", cfg.MaxForkDepth,
		"pools", cfg.Pools,
		"min_blocks_sample", cfg.MinBlocksSample,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	analyzer := analysis.NewAnalyzer(cfg, logger)

	if runErr := analyzer.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("analyzer exited with error", "err", runErr)
		os.Exit(1)
	}

	logger.Info("analysis stopped")
}
