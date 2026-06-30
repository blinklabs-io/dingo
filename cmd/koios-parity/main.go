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

// koios-parity validates Dingo's closed-epoch reward inputs against Koios on
// preview and preprod networks. It fetches Koios reference data into a local
// SQLite cache and compares it against Dingo's Blockfrost-compatible API.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const (
	programName        = "koios-parity"
	defaultDingoAPI    = "http://127.0.0.1:3000"
	defaultCacheSubdir = ".koios/cache.db"
	defaultGraceHours  = 24
)

// Global flags shared across subcommands.
var globalFlags struct {
	cachePath string
	network   string
	dingoData string // parent dir for cache and report paths
}

func main() {
	rootCmd := &cobra.Command{
		Use:   programName,
		Short: "Validate Dingo reward state against Koios reference data",
		Long: `koios-parity compares Dingo's closed-epoch reward inputs against Koios
on preview and preprod networks.

Default action (no subcommand): fetch Koios data, compare against Dingo, print status.

Data sources:
  Reference: Koios public REST API (cached in cache.db via 'fetch')
  Dingo:     Running node's Blockfrost-compatible API (live HTTP in 'check')

Environment:
  DINGO_BLOCKFROST_URL   default for --dingo-api (fallback: http://127.0.0.1:3000)
  KOIOS_API_KEY          default for --api-key`,
		// Default action: run fetch + check + status.
		RunE: runCommand,
	}

	// Persistent flags available to all subcommands.
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.cachePath, "cache", "",
		"path to cache.db (default: {dingo-data}/.koios/cache.db or .koios/cache.db)",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.network, "network", "",
		"cardano network: preview or preprod",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.dingoData, "dingo-data", "",
		"optional parent directory for --cache and report paths",
	)

	// Wire up run-mode flags directly on the root command.
	addRunFlags(rootCmd)

	// Subcommands.
	rootCmd.AddCommand(fetchCommand())
	rootCmd.AddCommand(checkCommand())
	rootCmd.AddCommand(statusCommand())
	rootCmd.AddCommand(explainCommand())
	rootCmd.AddCommand(watchCommand())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// resolveCachePath returns the effective cache path.
// Priority: explicit --cache flag > {dingo-data}/.koios/cache.db > .koios/cache.db
func resolveCachePath() string {
	if globalFlags.cachePath != "" {
		return globalFlags.cachePath
	}
	if globalFlags.dingoData != "" {
		return filepath.Join(globalFlags.dingoData, ".koios", "cache.db")
	}
	return defaultCacheSubdir
}

// resolveReportDir returns the directory for JSON reports.
func resolveReportDir(override string) string {
	if override != "" {
		return override
	}
	if globalFlags.dingoData != "" {
		return filepath.Join(globalFlags.dingoData, ".koios")
	}
	return ".koios"
}

// requireNetwork returns the network name or an error.
// Env precedence: CARDANO_NETWORK (repo-standard) then KOIOS_NETWORK (compat).
func requireNetwork() (string, error) {
	net := globalFlags.network
	if net == "" {
		if v := os.Getenv("CARDANO_NETWORK"); v != "" {
			net = v
		} else if v := os.Getenv("KOIOS_NETWORK"); v != "" {
			net = v
		}
	}
	if net == "" {
		return "", errors.New("--network is required (preview or preprod)")
	}
	if net != "preview" && net != "preprod" {
		return "", fmt.Errorf("--network must be 'preview' or 'preprod', got %q", net)
	}
	return net, nil
}

// dingoAPIURL returns the Dingo Blockfrost URL from flag, env, or the default.
// Using Changed() ensures an empty flag default doesn't shadow DINGO_BLOCKFROST_URL.
func dingoAPIURL(cmd *cobra.Command) string {
	if cmd.Flags().Changed("dingo-api") {
		url, _ := cmd.Flags().GetString("dingo-api")
		return url
	}
	if url := os.Getenv("DINGO_BLOCKFROST_URL"); url != "" {
		return url
	}
	return defaultDingoAPI
}

// koiosAPIKey returns the Koios Bearer token from flag or environment.
func koiosAPIKey(cmd *cobra.Command) string {
	if key, _ := cmd.Flags().GetString("api-key"); key != "" {
		return key
	}
	return os.Getenv("KOIOS_API_KEY")
}
