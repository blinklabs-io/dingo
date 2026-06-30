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

func fetchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Pull Koios reference data into the local cache",
		Long: `Incremental Koios fetch into cache.db. Resumes from last cached epoch + 1.
Does not contact Dingo. Safe to interrupt and resume.`,
		RunE: fetchRun,
	}

	cmd.Flags().String("api-key", "", "Koios Bearer token (or KOIOS_API_KEY)")
	cmd.Flags().Int("concurrency", 5, "parallel fetch workers")
	cmd.Flags().Uint64("from-epoch", 0, "force re-fetch from this epoch number")
	cmd.Flags().Uint64("through-epoch", 0, "stop at this epoch (default: tip-1)")

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

	result, err := koiosparity.Fetch(cmd.Context(), koiosparity.FetchConfig{
		Network:      network,
		APIKey:       koiosAPIKey(cmd),
		CachePath:    resolveCachePath(),
		Concurrency:  concurrency,
		FromEpoch:    fromEpoch,
		ThroughEpoch: throughEpoch,
	}, slog.Default())
	if err != nil {
		return err
	}

	fmt.Printf("fetch complete: %d epochs, %d pool rows (epochs %d–%d)\n",
		result.EpochsFetched, result.PoolsFetched,
		result.FromEpoch, result.ThroughEpoch,
	)
	return nil
}
