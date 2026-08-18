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
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

func addRunFlags(cmd *cobra.Command) {
	addDingoDBFlags(cmd)
	cmd.Flags().String("api-key", "",
		"Koios Bearer token (or KOIOS_API_KEY)")
	cmd.Flags().String("report-dir", "",
		"directory for JSON report (default: {dingo-data}/.koios/)")
	cmd.Flags().Int("concurrency", 5, "Koios fetch worker count")
	cmd.Flags().Int("workers", 0, "check worker count (default: NumCPU)")
	cmd.Flags().Int("grace-hours", defaultGraceHours,
		"pools absent from Koios in epochs fetched within this window → reference_lag")
	cmd.Flags().Bool("skip-fetch", false, "skip Koios fetch phase")
	cmd.Flags().Bool("skip-check", false, "skip compare phase")
	cmd.Flags().
		Bool("all", false, "re-check all cached epochs (not just unchecked/stale)")
	addAccountsFlag(cmd)
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

	accounts := accountsEnabled(cmd)
	var accountsSource koiosparity.RewardParitySource
	if accounts {
		// See fetchRun's identical comment: only opened when --accounts is
		// set, and only ever a direct read-only query, never an HTTP call to
		// Dingo's own API.
		dingo, dingoErr := koiosparity.OpenDingoDB(resolveDingoDB(cmd))
		if dingoErr != nil {
			return fmt.Errorf(
				"open dingo db (required for --accounts): %w",
				dingoErr,
			)
		}
		defer dingo.Close() //nolint:errcheck
		accountsSource = dingo
	}

	if !skipFetch {
		slog.Info("koios-parity: fetch phase starting", "network", network)
		fetchResult, fetchErr := koiosparity.Fetch(ctx, koiosparity.FetchConfig{
			Network:         network,
			APIKey:          koiosAPIKey(cmd),
			CachePath:       cachePath,
			Concurrency:     concurrency,
			AccountsEnabled: accounts,
			AccountsSource:  accountsSource,
		}, logger)
		if fetchErr != nil {
			return fmt.Errorf("fetch: %w", fetchErr)
		}
		// FailedEpochs means one or more epochs hit an isolated, transient fetch
		// failure and were left uncached rather than aborting the whole run (see
		// FetchResult.FailedEpochs). Unlike watch's continuous loop, a one-shot
		// run has no "next tick" to retry them, so surface it as a hard failure
		// now instead of silently proceeding to check phase against an
		// incomplete cache.
		if len(fetchResult.FailedEpochs) > 0 {
			return fmt.Errorf(
				"fetch: %d epoch(s) failed transiently and are not cached: %v",
				len(fetchResult.FailedEpochs), fetchResult.FailedEpochs,
			)
		}
	}

	if !skipCheck {
		slog.Info("koios-parity: check phase starting", "network", network)
		if _, err := koiosparity.Check(ctx, koiosparity.CheckConfig{
			Network:         network,
			DingoDB:         resolveDingoDB(cmd),
			CachePath:       cachePath,
			Workers:         workers,
			All:             all,
			GraceHours:      graceHours,
			AccountsEnabled: accounts,
		}, logger); err != nil {
			return fmt.Errorf("check: %w", err)
		}
	}

	// Print status summary.
	cache, err := koiosparity.OpenCache(cachePath, logger)
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}
	defer cache.Close() //nolint:errcheck

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

	// Write JSON report. writeParityReport captures every failure path
	// (including a file-Close error) into its returned error rather than only
	// logging it: automation depends on runCommand's error return, not
	// printed output, to detect that the promised JSON report is missing or
	// incomplete — an unwritable --report-dir, a report-creation failure, or
	// a BuildJSONReport/WriteJSONReport failure must never let this function
	// return nil merely because the check phase itself reported PASS.
	dir := resolveReportDir(reportDir)
	reportPath := fmt.Sprintf("%s/report-%s-%s.json",
		dir, network, time.Now().Format("2006-01-02"))
	reportErr := writeParityReport(
		logger,
		dir,
		reportPath,
		func(path string) (io.WriteCloser, error) {
			return os.Create(path)
		},
		func() (*koiosparity.JSONReport, error) {
			return koiosparity.BuildJSONReport(
				network,
				time.Now().UTC().Format(time.RFC3339),
				fetchedEpochs,
				statuses,
				func(epoch uint64) ([]koiosparity.CheckMismatch, error) {
					return cache.GetMismatches(network, epoch, "")
				},
			)
		},
	)

	// A FAIL or ERROR epoch must surface as a non-zero exit so automation can't
	// mistake an incomplete or failed parity check for success; propagated via
	// RunE's error return (main's rootCmd.Execute() handles os.Exit(1)) rather
	// than exiting directly here. This is derived from `statuses` (the
	// persisted, network-wide status just printed above) rather than
	// checkResult directly: checkResult is nil when --skip-check is set, and
	// even when the check phase ran, Check may have found nothing needing
	// (re)check and so performed no fresh work — in both cases a prior FAIL or
	// ERROR still sitting in the cache must not be reported as success.
	//
	// reportErr is combined via errors.Join rather than either one masking the
	// other: a report-output failure must surface even when the check itself
	// passed, and a real check FAIL/ERROR must still surface even when the
	// report happened to write successfully.
	checkErr := checkResultErr(
		koiosparity.EffectiveCheckOutcome(statuses, 0, 0),
	)
	return errors.Join(reportErr, checkErr)
}

// writeParityReport creates dir/path, builds a JSON report via build, and
// writes it via koiosparity.WriteJSONReport, returning a non-nil error for
// every failure mode instead of only logging it: an unwritable dir
// (os.MkdirAll failure), a report-file creation failure, a build failure, a
// WriteJSONReport failure, or a Close error on the created file — any of
// these previously left runCommand returning nil (as long as the parity
// check itself was PASS), silently hiding a missing or incomplete JSON
// report from automation that only checks the process exit code.
//
// create/build are injected (rather than calling os.Create/
// koiosparity.BuildJSONReport directly) so tests can simulate a build or
// write/close failure deterministically without relying on filesystem
// permission tricks that root ignores or platform-specific special files.
// runCommand's production call passes real os.Create and a closure wrapping
// koiosparity.BuildJSONReport.
//
// A Close error is folded into the same returned error as a build/write
// failure via errors.Join, rather than one silently discarding the other:
// both indicate the on-disk report may be missing, truncated, or stale.
func writeParityReport(
	logger *slog.Logger,
	dir, path string,
	create func(path string) (io.WriteCloser, error),
	build func() (*koiosparity.JSONReport, error),
) error {
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		logger.Warn(
			"koios-parity: could not create report dir",
			"path",
			dir,
			"error",
			mkErr,
		)
		return fmt.Errorf("create report dir %s: %w", dir, mkErr)
	}

	f, openErr := create(path)
	if openErr != nil {
		logger.Warn(
			"koios-parity: could not create report file",
			"path",
			path,
			"error",
			openErr,
		)
		return fmt.Errorf("create report file %s: %w", path, openErr)
	}

	report, buildErr := build()
	if buildErr != nil {
		logger.Warn("koios-parity: could not build report", "error", buildErr)
		reportErr := fmt.Errorf("build report: %w", buildErr)
		if closeErr := f.Close(); closeErr != nil {
			logger.Warn(
				"koios-parity: could not close report file",
				"path",
				path,
				"error",
				closeErr,
			)
			reportErr = errors.Join(
				reportErr,
				fmt.Errorf("close report file %s: %w", path, closeErr),
			)
		}
		return reportErr
	}

	if writeErr := koiosparity.WriteJSONReport(f, report); writeErr != nil {
		logger.Warn(
			"koios-parity: could not write report",
			"path",
			path,
			"error",
			writeErr,
		)
		reportErr := fmt.Errorf("write report %s: %w", path, writeErr)
		if closeErr := f.Close(); closeErr != nil {
			logger.Warn(
				"koios-parity: could not close report file",
				"path",
				path,
				"error",
				closeErr,
			)
			reportErr = errors.Join(
				reportErr,
				fmt.Errorf("close report file %s: %w", path, closeErr),
			)
		}
		return reportErr
	}

	if closeErr := f.Close(); closeErr != nil {
		logger.Warn(
			"koios-parity: could not close report file",
			"path",
			path,
			"error",
			closeErr,
		)
		return fmt.Errorf("close report file %s: %w", path, closeErr)
	}

	logger.Info("koios-parity: report written", "path", path)
	return nil
}
