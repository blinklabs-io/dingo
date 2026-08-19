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

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

func fetchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Pull Koios reference data into the local cache",
		Long: `Incremental Koios fetch into cache.db. Resumes from last cached epoch + 1.
Does not contact Dingo, unless --accounts is set: #3097's per-account fetch
phase then opens Dingo's metadata database read-only (see --metadata-plugin/
--metadata-dsn) to resolve the account universe. Safe to interrupt and resume.`,
		RunE: fetchRun,
	}

	cmd.Flags().String("api-key", "", "Koios Bearer token (or KOIOS_API_KEY)")
	cmd.Flags().Int("concurrency", 5, "parallel fetch workers")
	cmd.Flags().
		Uint64("from-epoch", 0, "start epoch (gaps in [from, through] are filled; add --force-refresh to overwrite cached rows)")
	cmd.Flags().
		Uint64("through-epoch", 0, "stop at this epoch (default: tip-1)")
	cmd.Flags().
		Bool("force-refresh", false, "re-fetch and overwrite all epochs in [from-epoch, through-epoch], not just missing ones")
	cmd.Flags().Int("grace-hours", defaultGraceHours,
		"a just-closed epoch's zero-row --accounts fetch within this window is retried, not accepted as final (see check/run/watch's identical flag)")
	addAccountsFlag(cmd)
	// Only used when --accounts is set (see fetchRun): the standalone fetch
	// command otherwise never contacts Dingo's database at all.
	addDingoDBFlags(cmd)

	return cmd
}

func fetchRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}

	concurrency, _ := cmd.Flags().GetInt("concurrency")
	fromEpoch, _ := cmd.Flags().GetUint64("from-epoch")
	throughEpoch, _ := cmd.Flags().GetUint64("through-epoch")
	forceRefresh, _ := cmd.Flags().GetBool("force-refresh")
	graceHours, err := resolveGraceHours(cmd)
	if err != nil {
		return err
	}

	if forceRefresh && !cmd.Flags().Changed("from-epoch") {
		return errors.New(
			"--force-refresh requires an explicit --from-epoch to prevent accidental full historical re-fetch",
		)
	}

	accounts := accountsEnabled(cmd)
	var accountsSource koiosparity.RewardParitySource
	if accounts {
		// #3097's address universe unions Koios's own list with Dingo's known
		// addresses (see koiosparity.BuildAccountAddressUniverse) — open a
		// read-only connection to Dingo's metadata DB for that purpose only;
		// this is still the same direct, read-only GORM-backed query this
		// tool has always used for the Dingo side, never an HTTP call to
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

	result, err := koiosparity.Fetch(cmd.Context(), koiosparity.FetchConfig{
		Network:         network,
		APIKey:          koiosAPIKey(cmd),
		CachePath:       resolveCachePath(),
		Concurrency:     concurrency,
		FromEpoch:       fromEpoch,
		ThroughEpoch:    throughEpoch,
		ForceRefresh:    forceRefresh,
		AccountsEnabled: accounts,
		AccountsSource:  accountsSource,
		GraceHours:      graceHours,
	}, slog.Default())
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}

	fmt.Printf("fetch complete: %d epochs, %d pool rows (epochs %d–%d)\n",
		result.EpochsFetched, result.PoolsFetched,
		result.FromEpoch, result.ThroughEpoch,
	)
	if len(result.FailedEpochs) > 0 {
		fmt.Printf(
			"warning: %d epoch(s) hit a transient fetch failure and remain uncached: %v\n"+
				"  they will be retried automatically on the next `fetch` run\n",
			len(result.FailedEpochs),
			result.FailedEpochs,
		)
		return fmt.Errorf(
			"%d epoch(s) failed transiently; rerun fetch to retry",
			len(result.FailedEpochs),
		)
	}
	return nil
}
