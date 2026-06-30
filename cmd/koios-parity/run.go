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
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

func addRunFlags(cmd *cobra.Command) {
	cmd.Flags().String("dingo-api", "",
		"Dingo Blockfrost base URL (or DINGO_BLOCKFROST_URL)")
	cmd.Flags().String("api-key", "",
		"Koios Bearer token (or KOIOS_API_KEY)")
	cmd.Flags().String("report-dir", "",
		"directory for JSON report (default: {dingo-data}/.koios/ or .koios/)")
	cmd.Flags().Int("concurrency", 5, "Koios fetch worker count")
	cmd.Flags().Int("workers", 0, "check worker count (default: NumCPU)")
	cmd.Flags().Int("grace-hours", defaultGraceHours,
		"pools absent from Koios in epochs fetched within this window → reference_lag")
	cmd.Flags().Bool("skip-fetch", false, "skip Koios fetch phase")
	cmd.Flags().Bool("skip-check", false, "skip compare phase")
	cmd.Flags().Bool("all", false, "re-check all cached epochs (not just unchecked)")
}

func runCommand(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}

	cachePath := resolveCachePath()
	skipFetch, _ := cmd.Flags().GetBool("skip-fetch")
	skipCheck, _ := cmd.Flags().GetBool("skip-check")
	all, _ := cmd.Flags().GetBool("all")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	workers, _ := cmd.Flags().GetInt("workers")
	graceHours, _ := cmd.Flags().GetInt("grace-hours")
	reportDir, _ := cmd.Flags().GetString("report-dir")

	logger := slog.Default()
	ctx := cmd.Context()

	if !skipFetch {
		slog.Info("koios-parity: fetch phase starting", "network", network)
		if _, fetchErr := koiosparity.Fetch(ctx, koiosparity.FetchConfig{
			Network:     network,
			APIKey:      koiosAPIKey(cmd),
			CachePath:   cachePath,
			Concurrency: concurrency,
		}, logger); fetchErr != nil {
			return fmt.Errorf("fetch: %w", fetchErr)
		}
	}

	var checkResult *koiosparity.CheckResult
	if !skipCheck {
		slog.Info("koios-parity: check phase starting", "network", network)
		checkResult, err = koiosparity.Check(ctx, koiosparity.CheckConfig{
			Network:     network,
			DingoAPIURL: dingoAPIURL(cmd),
			CachePath:   cachePath,
			Workers:     workers,
			All:         all,
			GraceHours:  graceHours,
		}, logger)
		if err != nil {
			return fmt.Errorf("check: %w", err)
		}
	}

	// Print status summary.
	cache, err := koiosparity.OpenCache(cachePath, logger)
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}
	fetchedEpochs, err := cache.GetAllFetchedEpochs(network)
	if err != nil {
		return fmt.Errorf("get fetched epochs: %w", err)
	}
	statuses, err := cache.GetStatusSummary(network)
	if err != nil {
		return fmt.Errorf("get status: %w", err)
	}
	summary := koiosparity.BuildStatusSummary(network, fetchedEpochs, statuses)
	koiosparity.PrintStatus(os.Stdout, summary, false, statuses)

	// Write JSON report; log each failure so operators know when the report is missing.
	dir := resolveReportDir(reportDir)
	reportPath := fmt.Sprintf("%s/report-%s-%s.json",
		dir, network, time.Now().Format("2006-01-02"))
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		logger.Warn("koios-parity: could not create report dir", "path", dir, "error", mkErr)
	} else if f, openErr := os.Create(reportPath); openErr != nil {
		logger.Warn("koios-parity: could not create report file", "path", reportPath, "error", openErr)
	} else {
		report, buildErr := koiosparity.BuildJSONReport(
			network,
			time.Now().UTC().Format(time.RFC3339),
			fetchedEpochs,
			statuses,
			func(epoch uint64) ([]koiosparity.CheckMismatch, error) {
				return cache.GetMismatches(network, epoch, "")
			},
		)
		if buildErr != nil {
			f.Close()
			logger.Warn("koios-parity: could not build report", "error", buildErr)
		} else if writeErr := koiosparity.WriteJSONReport(f, report); writeErr != nil {
			f.Close()
			logger.Warn("koios-parity: could not write report", "path", reportPath, "error", writeErr)
		} else {
			f.Close()
			slog.Info("koios-parity: report written", "path", reportPath)
		}
	}

	// Exit code: 1 if any FAIL epoch.
	if checkResult != nil && len(checkResult.FailEpochs) > 0 {
		os.Exit(1)
	}
	return nil
}
