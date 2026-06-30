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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

// explainMismatch is the CLI-facing JSON shape for explain --json.
// Using a dedicated type avoids leaking internal DB fields (ID, StakeAddress,
// CheckedAt, etc.) into the stable CLI output format.
type explainMismatch struct {
	Pool       string `json:"pool,omitempty"`
	Field      string `json:"field"`
	DingoValue string `json:"dingo_value"`
	KoiosValue string `json:"koios_value"`
	Category   string `json:"category"`
}

func explainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "explain",
		Short: "Drill down into mismatches for a specific epoch",
		Long: `Shows field-level mismatches from the cache for the given epoch.
Use --live to re-run the comparison against Dingo's API (requires --dingo-api).`,
		RunE: explainRun,
	}

	cmd.Flags().Uint64("epoch", 0, "epoch to explain (required)")
	cmd.Flags().String("pool", "", "optional pool bech32 filter")
	cmd.Flags().Bool("live", false, "re-compare against Dingo API instead of cached results")
	cmd.Flags().String("dingo-api", "", "Dingo Blockfrost base URL (or DINGO_BLOCKFROST_URL)")
	cmd.Flags().Bool("json", false, "emit JSON output")

	return cmd
}

func explainRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}

	// Use Changed() so that epoch 0 is a valid argument.
	if !cmd.Flags().Changed("epoch") {
		return errors.New("--epoch is required (use --epoch <number>)")
	}
	epoch, _ := cmd.Flags().GetUint64("epoch")
	poolFilter, _ := cmd.Flags().GetString("pool")
	live, _ := cmd.Flags().GetBool("live")
	asJSON, _ := cmd.Flags().GetBool("json")
	cachePath := resolveCachePath()

	cache, err := koiosparity.OpenCache(cachePath, slog.Default())
	if err != nil {
		return err
	}

	var mismatches []koiosparity.CheckMismatch

	if live {
		checkResult, checkErr := koiosparity.Check(cmd.Context(), koiosparity.CheckConfig{
			Network:      network,
			DingoAPIURL:  dingoAPIURL(cmd),
			CachePath:    cachePath,
			All:          true,
			FromEpoch:    epoch,
			ThroughEpoch: epoch,
		}, slog.Default())
		if checkErr != nil {
			return fmt.Errorf("live check: %w", checkErr)
		}
		_ = checkResult
	}

	mismatches, err = cache.GetMismatches(network, epoch, poolFilter)
	if err != nil {
		return fmt.Errorf("get mismatches: %w", err)
	}

	if asJSON {
		out := make([]explainMismatch, len(mismatches))
		for i, m := range mismatches {
			out[i] = explainMismatch{
				Pool:       m.PoolBech32,
				Field:      m.Field,
				DingoValue: m.DingoValue,
				KoiosValue: m.KoiosValue,
				Category:   m.Category,
			}
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	koiosparity.PrintExplain(os.Stdout, network, epoch, mismatches, poolFilter)
	return nil
}
