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
	"log/slog"
	"os"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

func statusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show parity cache and check summary",
		Long:  "Read-only view of the cache. No network or Dingo API contact.",
		RunE:  statusRun,
	}
	cmd.Flags().Bool("verbose", false, "show per-failing-epoch mismatch counts")
	return cmd
}

func statusRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork(cmd)
	if err != nil {
		return err
	}

	cachePath := resolveCachePath()
	verbose, _ := cmd.Flags().GetBool("verbose")

	cache, err := koiosparity.OpenCache(cachePath, slog.Default())
	if err != nil {
		return err
	}

	fetchedEpochs, err := cache.GetAllFetchedEpochs(network)
	if err != nil {
		return err
	}

	statuses, err := cache.GetStatusSummary(network)
	if err != nil {
		return err
	}

	summary := koiosparity.BuildStatusSummary(network, fetchedEpochs, statuses)
	koiosparity.PrintStatus(os.Stdout, summary, verbose, statuses)
	return nil
}
