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

// koios-parity validates Dingo's closed-epoch reward state against Koios
// reference data on preview and preprod networks.
//
// It fetches Koios reference data (epoch info, per-pool history) into a local
// SQLite cache and then compares it directly against Dingo's metadata database
// (reward_pool_input, epoch_summary, reward_ada_pots). No Blockfrost or HTTP
// endpoint on the Dingo node is contacted for the comparison; the tool reads
// the node's SQLite file directly in read-only mode.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/spf13/cobra"
)

const (
	programName        = "koios-parity"
	defaultDingoData   = ".dingo" // Dingo's default data directory
	defaultCacheSubdir = ".koios/cache.db"
	defaultGraceHours  = 24
)

// Global flags shared across subcommands.
var globalFlags struct {
	cachePath string
	network   string
	dingoData string // Dingo node data directory (contains metadata.sqlite + cache subdir)
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
  Dingo:     Node's metadata.sqlite read directly (reward_pool_input, epoch_summary)

Environment:
  DINGO_DATA_DIR     path to Dingo data directory (default: .dingo)
  CARDANO_NETWORK    cardano network name (preview or preprod)
  KOIOS_NETWORK      fallback network name env var
  KOIOS_API_KEY      Koios Bearer token for rate-limited access`,
		RunE: runCommand,
	}

	// Persistent flags available to all subcommands.
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.cachePath, "cache", "",
		"path to cache.db (default: {dingo-data}/.koios/cache.db)",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.network, "network", "",
		"cardano network: preview or preprod",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.dingoData, "dingo-data", "",
		"Dingo node data directory containing metadata.sqlite (default: $DINGO_DATA_DIR or .dingo)",
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

// resolveDingoDataDir returns the Dingo data directory.
// Priority: --dingo-data flag > DINGO_DATA_DIR env > .dingo default.
func resolveDingoDataDir() string {
	if globalFlags.dingoData != "" {
		return globalFlags.dingoData
	}
	if v := os.Getenv("DINGO_DATA_DIR"); v != "" {
		return v
	}
	return defaultDingoData
}

// resolveCachePath returns the effective cache path.
// Priority: explicit --cache flag > {dingo-data}/.koios/cache.db.
func resolveCachePath() string {
	if globalFlags.cachePath != "" {
		return globalFlags.cachePath
	}
	return filepath.Join(resolveDingoDataDir(), defaultCacheSubdir)
}

// resolveReportDir returns the directory for JSON reports.
func resolveReportDir(override string) string {
	if override != "" {
		return override
	}
	return filepath.Join(resolveDingoDataDir(), ".koios")
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

// koiosAPIKey returns the Koios Bearer token from flag or environment.
func koiosAPIKey(cmd *cobra.Command) string {
	if key, _ := cmd.Flags().GetString("api-key"); key != "" {
		return key
	}
	return os.Getenv("KOIOS_API_KEY")
}

// addDingoDB registers --metadata-plugin and --metadata-dsn on cmd and
// should be called for every subcommand that reads from Dingo's database.
func addDingoDBFlags(cmd *cobra.Command) {
	cmd.Flags().String("metadata-plugin", "",
		"Dingo metadata backend: sqlite (default), postgres, or mysql")
	cmd.Flags().String("metadata-dsn", "",
		"connection string for postgres/mysql (unused for sqlite)")
}

// resolveDingoDB returns the DingoDBConfig for cmd.
// Plugin defaults to "sqlite"; DataDir is always set for cache-path resolution.
func resolveDingoDB(cmd *cobra.Command) koiosparity.DingoDBConfig {
	plugin, _ := cmd.Flags().GetString("metadata-plugin")
	dsn, _ := cmd.Flags().GetString("metadata-dsn")
	if plugin == "" {
		if v := os.Getenv("DINGO_METADATA_PLUGIN"); v != "" {
			plugin = v
		}
	}
	if dsn == "" {
		if v := os.Getenv("DINGO_METADATA_DSN"); v != "" {
			dsn = v
		}
	}
	return koiosparity.DingoDBConfig{
		Plugin:  plugin,
		DataDir: resolveDingoDataDir(),
		DSN:     dsn,
	}
}
