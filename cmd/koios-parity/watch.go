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
		Long: `Polls Dingo's epoch_summary table at --interval and runs fetch+check
when the epoch number advances. Reads directly from metadata.sqlite — no API contact.
Useful for operators running a continuously-syncing node.
Does not replace manual 'run --all' after a ledger replay.`,
		RunE: watchRun,
	}

	cmd.Flags().String("api-key", "", "Koios Bearer token (or KOIOS_API_KEY)")
	cmd.Flags().Duration("interval", 15*time.Minute, "poll interval")
	cmd.Flags().Int("concurrency", 5, "Koios fetch workers")
	cmd.Flags().Int("workers", 0, "check workers (default: NumCPU)")
	cmd.Flags().Int("grace-hours", defaultGraceHours,
		"pools absent from Koios in recently-fetched epochs → reference_lag (not FAIL)")
	addDingoDBFlags(cmd)

	return cmd
}

func watchRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}

	cachePath := resolveCachePath()
	dbCfg := resolveDingoDB(cmd)
	interval, _ := cmd.Flags().GetDuration("interval")
	if interval <= 0 {
		return errors.New("--interval must be positive")
	}
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	workers, _ := cmd.Flags().GetInt("workers")
	graceHours, _ := cmd.Flags().GetInt("grace-hours")
	apiKey := koiosAPIKey(cmd)

	logger := slog.Default()
	ctx := cmd.Context()

	// Open the Dingo database once for the lifetime of the watch loop.
	// Read-only connections (SQLite WAL, or a shared postgres/mysql pool) see
	// rows committed by the live node on each new query.
	dingo, err := koiosparity.OpenDingoDB(dbCfg)
	if err != nil {
		return fmt.Errorf("open dingo db: %w", err)
	}
	defer dingo.Close() //nolint:errcheck

	var lastEpoch uint64 // last epoch we successfully processed

	logger.Info("koios-parity: watch started",
		"network", network,
		"metadata_plugin", dbCfg.Plugin,
		"interval", interval,
	)

	for {
		current, epochErr := dingo.GetLatestEpoch(ctx)
		if epochErr != nil {
			logger.Warn("koios-parity: could not get latest epoch from Dingo DB",
				"error", epochErr,
			)
		} else if current > 0 && current != lastEpoch {
			// Determine the range of newly closed epochs.
			// GetLatestEpoch returns the epoch whose summary Dingo has already written,
			// so current is checkable immediately — no off-by-one subtraction needed.
			// First iteration: check only the latest available epoch.
			// Subsequent iterations: all epochs since the last processed one.
			var fromClosed uint64
			if lastEpoch == 0 {
				fromClosed = current
			} else {
				fromClosed = lastEpoch + 1
			}
			toClosed := current

			if fromClosed > toClosed {
				// Edge-case guard: skip an inverted range rather than pass it
				// downstream. Advance cursor so the next tick reflects reality.
				logger.Warn("koios-parity: skipping inverted epoch range",
					"from", fromClosed, "through", toClosed)
				lastEpoch = current
			} else {
				logger.Info("koios-parity: new closed epochs detected",
					"from", fromClosed, "through", toClosed)

				fetchErr := error(nil)
				if _, fetchErr = koiosparity.Fetch(ctx, koiosparity.FetchConfig{
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
					DingoDB:      dbCfg,
					CachePath:    cachePath,
					Workers:      workers,
					FromEpoch:    fromClosed,
					ThroughEpoch: toClosed,
					GraceHours:   graceHours,
				}, logger)

				if fetchErr != nil || checkErr != nil {
					// Keep lastEpoch unchanged so the next tick retries this range.
					if checkErr != nil {
						logger.Warn("koios-parity: check error", "error", checkErr)
					}
				} else if result != nil {
					// Only advance the cursor after both fetch and check succeed.
					lastEpoch = current
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
		}

		select {
		case <-ctx.Done():
			logger.Info("koios-parity: watch stopped")
			return nil
		case <-time.After(interval):
		}
	}
}
