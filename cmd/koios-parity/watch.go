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

package main

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

func watchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Long-running loop: fetch+check whenever a new epoch closes",
		Long: `Polls Dingo's /epochs/latest at --interval and runs fetch+check
when the epoch advances. Useful for operators running a continuously-syncing node.
Does not replace manual 'run --all' after a ledger replay.`,
		RunE: watchRun,
	}

	cmd.Flags().String("dingo-api", "",
		"Dingo Blockfrost base URL (or DINGO_BLOCKFROST_URL)")
	cmd.Flags().String("api-key", "", "Koios Bearer token (or KOIOS_API_KEY)")
	cmd.Flags().Duration("interval", 15*time.Minute, "poll interval")
	cmd.Flags().Int("concurrency", 5, "Koios fetch workers")
	cmd.Flags().Int("workers", 0, "check workers (default: NumCPU)")
	cmd.Flags().Int("grace-hours", defaultGraceHours,
		"pools absent from Koios in recently-fetched epochs → reference_lag (not FAIL)")

	return cmd
}

func watchRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}

	cachePath := resolveCachePath()
	interval, _ := cmd.Flags().GetDuration("interval")
	if interval <= 0 {
		return errors.New("--interval must be positive")
	}
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	workers, _ := cmd.Flags().GetInt("workers")
	graceHours, _ := cmd.Flags().GetInt("grace-hours")
	apiKey := koiosAPIKey(cmd)
	dingoURL := dingoAPIURL(cmd)

	logger := slog.Default()
	ctx := cmd.Context()

	dingo := koiosparity.NewBlockfrostClient(dingoURL)
	var lastEpoch uint64

	logger.Info("koios-parity: watch started",
		"network", network,
		"dingo_api", dingoURL,
		"interval", interval,
	)

	for {
		current, epochErr := dingo.GetLatestEpoch(ctx)
		if epochErr != nil {
			logger.Warn("koios-parity: could not get latest epoch from Dingo",
				"error", epochErr,
			)
		} else if current != lastEpoch {
			// Determine the range of newly closed epochs.
			// First iteration: only the most recently closed epoch.
			// Subsequent iterations: all epochs that closed since last poll.
			var fromClosed uint64
			if lastEpoch == 0 {
				fromClosed = current - 1
			} else {
				fromClosed = lastEpoch
			}
			toClosed := current - 1

			if fromClosed <= toClosed {
				logger.Info("koios-parity: new closed epochs detected",
					"from", fromClosed, "through", toClosed)
			}
			lastEpoch = current

			if _, fetchErr := koiosparity.Fetch(ctx, koiosparity.FetchConfig{
				Network:      network,
				APIKey:       apiKey,
				CachePath:    cachePath,
				Concurrency:  concurrency,
				FromEpoch:    fromClosed,
				ThroughEpoch: toClosed,
			}, logger); fetchErr != nil {
				logger.Warn("koios-parity: fetch error", "error", fetchErr)
			}

			result, checkErr := koiosparity.Check(ctx, koiosparity.CheckConfig{
				Network:      network,
				DingoAPIURL:  dingoURL,
				CachePath:    cachePath,
				Workers:      workers,
				FromEpoch:    fromClosed,
				ThroughEpoch: toClosed,
				GraceHours:   graceHours,
			}, logger)
			if checkErr != nil {
				logger.Warn("koios-parity: check error", "error", checkErr)
			} else {
				status := "PASS"
				if len(result.FailEpochs) > 0 {
					status = fmt.Sprintf("FAIL (%d mismatches)", result.MismatchCount)
				} else if len(result.ErrorEpochs) > 0 {
					status = "ERROR"
				}
				logger.Info("koios-parity: epochs result",
					"from", fromClosed, "through", toClosed,
					"status", status,
				)
			}
		}

		select {
		case <-ctx.Done():
			logger.Info("koios-parity: watch stopped")
			return nil
		case <-time.After(interval):
		}
	}
}
