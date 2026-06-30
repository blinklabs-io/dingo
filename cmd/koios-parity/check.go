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

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

func checkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Compare Koios cache against Dingo Blockfrost API",
		Long: `Compares cached Koios reference data against Dingo's Blockfrost API.
No Koios HTTP calls; no direct SQLite reads outside the cache.`,
		RunE: checkRun,
	}

	cmd.Flags().String("dingo-api", defaultDingoAPI,
		"Dingo Blockfrost base URL (or DINGO_BLOCKFROST_URL)")
	cmd.Flags().Int("workers", 0, "parallel check workers (default: NumCPU)")
	cmd.Flags().Int("grace-hours", defaultGraceHours,
		"pools absent from Koios in recently-fetched epochs → reference_lag (not FAIL)")
	cmd.Flags().Bool("all", false, "re-check all cached epochs, ignoring prior results")
	cmd.Flags().Uint64("from-epoch", 0, "lower bound (inclusive)")
	cmd.Flags().Uint64("through-epoch", 0, "upper bound (inclusive)")

	return cmd
}

func checkRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}

	workers, _ := cmd.Flags().GetInt("workers")
	all, _ := cmd.Flags().GetBool("all")
	fromEpoch, _ := cmd.Flags().GetUint64("from-epoch")
	throughEpoch, _ := cmd.Flags().GetUint64("through-epoch")
	graceHours, _ := cmd.Flags().GetInt("grace-hours")

	result, err := koiosparity.Check(cmd.Context(), koiosparity.CheckConfig{
		Network:      network,
		DingoAPIURL:  dingoAPIURL(cmd),
		CachePath:    resolveCachePath(),
		Workers:      workers,
		All:          all,
		FromEpoch:    fromEpoch,
		ThroughEpoch: throughEpoch,
		GraceHours:   graceHours,
	}, slog.Default())
	if err != nil {
		return err
	}

	fmt.Printf("check complete: %d epochs, %d mismatches (%d fail, %d error)\n",
		result.EpochsChecked,
		result.MismatchCount,
		len(result.FailEpochs),
		len(result.ErrorEpochs),
	)
	if len(result.FailEpochs) > 0 {
		fmt.Print("failing epochs: ")
		for i, e := range result.FailEpochs {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Print(e)
		}
		fmt.Println()
	}
	return nil
}
