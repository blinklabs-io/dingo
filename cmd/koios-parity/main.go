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
// (reward_pool_input, epoch_summary, reward_ada_pots). No Blockfrost or any
// other HTTP endpoint on the Dingo node is contacted for the comparison.
//
// Three metadata backends are supported, matching Dingo's own
// plugins.storage.metadata selection (see resolveDingoDB):
//   - sqlite (default): opens {data-dir}/metadata.sqlite in read-only WAL mode
//   - postgres: connects via --metadata-dsn or Dingo's resolved metadata DSN/config
//   - mysql:    connects via --metadata-dsn or Dingo's resolved metadata DSN/config
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/koiosparity"
	mysqldriver "github.com/go-sql-driver/mysql"
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
	cachePath       string
	network         string
	dingoData       string // Dingo node data directory (contains metadata.sqlite + cache subdir)
	dingoConfigFile string // path to Dingo's own dingo.yaml, mirroring dingo's own --config flag
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

Metadata backend resolution (plugin, DSN, data directory) is loaded from
Dingo's own configuration — the same dingo.yaml/env vars/CARDANO_DATABASE_PATH
the Dingo process itself resolves — so this tool inspects whichever database
that node is actually configured for. See --dingo-config, --metadata-plugin,
--metadata-dsn, and --dingo-data to override any part of it explicitly.

Environment:
  CARDANO_DATABASE_PATH                    Dingo data directory (same var Dingo itself reads)
  DINGO_PLUGINS_STORAGE_METADATA_PROVIDER  metadata backend: sqlite (default), postgres, mysql
  DINGO_PLUGINS_STORAGE_METADATA_CONFIG_*  metadata provider config (e.g. _DSN, _HOST, _DATA_DIR)
  DINGO_DATA_DIR                           koios-parity-only override for the data directory
  CARDANO_NETWORK                          cardano network name (preview or preprod)
  KOIOS_API_KEY                            Koios Bearer token for rate-limited access`,
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
		"Dingo node data directory containing metadata.sqlite "+
			"(default: resolved from Dingo's own config, then $DINGO_DATA_DIR, then .dingo)",
	)
	rootCmd.PersistentFlags().StringVar(
		&globalFlags.dingoConfigFile, "dingo-config", "",
		"path to the Dingo node's dingo.yaml (default: same search Dingo itself uses — "+
			"~/.dingo/dingo.yaml, then /etc/dingo/dingo.yaml)",
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

// dingoConfigOnce/dingoConfigCached memoize loadedDingoConfig within a single
// process run: every subcommand resolves the data dir and/or metadata plugin
// exactly once, so loading (and warning on failure) more than once would just
// be noisy repetition of the same result.
var (
	dingoConfigOnce   sync.Once
	dingoConfigCached *config.Config
)

// loadedDingoConfig loads Dingo's own resolved configuration — dingo.yaml
// (searched the same way Dingo's binary does, or --dingo-config), then
// CARDANO_*/DINGO_PLUGINS_* environment overlays via config.LoadConfig — so
// this tool's DB resolution mirrors the real node's instead of guessing from
// koios-parity-invented env var names that Dingo never reads. Returns nil
// (logging a warning) if the config can't be loaded; callers fall back to
// their own defaults in that case.
func loadedDingoConfig() *config.Config {
	dingoConfigOnce.Do(func() {
		cfg, err := config.LoadConfig(globalFlags.dingoConfigFile)
		if err != nil {
			slog.Default().Warn(
				"koios-parity: could not load dingo config; falling back to sqlite/.dingo defaults",
				"error", err,
			)
			return
		}
		dingoConfigCached = cfg
	})
	return dingoConfigCached
}

// resolveDingoDataDir returns the Dingo data directory.
// Priority: --dingo-data flag > DINGO_DATA_DIR env (koios-parity-only override,
// for pointing at a separate copy of the database) > the metadata plugin's own
// dataDir config override > Dingo's resolved DatabasePath (CARDANO_DATABASE_PATH
// or dingo.yaml's databaseName, same as the real node) > .dingo default.
func resolveDingoDataDir() string {
	if globalFlags.dingoData != "" {
		return globalFlags.dingoData
	}
	if v := os.Getenv("DINGO_DATA_DIR"); v != "" {
		return v
	}
	if cfg := loadedDingoConfig(); cfg != nil {
		if dataDir, ok := stringConfigValue(cfg.Plugins.Storage.Metadata.Config, "dataDir"); ok &&
			dataDir != "" {
			return dataDir
		}
		if cfg.DatabasePath != "" {
			return cfg.DatabasePath
		}
	}
	return defaultDingoData
}

// stringConfigValue reads a string field out of a plugin Selection.Config map
// (values arrive as `any` from YAML/env parsing — see plugin.ApplyEnvironment).
func stringConfigValue(cfg map[string]any, key string) (string, bool) {
	v, ok := cfg[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// intConfigValue reads a numeric field (e.g. a port number) out of a plugin
// Selection.Config map. gopkg.in/yaml.v3's Unmarshal-into-`any` (used by
// plugin.ApplyEnvironment for every env-var scalar, and by YAML config
// parsing) only ever produces `int` or `float64` for a numeric scalar.
// Returns 0 if absent or not numeric.
func intConfigValue(cfg map[string]any, key string) int {
	switch v := cfg[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	default:
		return 0
	}
}

// dsnFromMetadataConfig builds a connection string from Dingo's resolved
// plugins.storage.metadata.config map. A flat "dsn" key (the pattern used by
// this repo's own k8s examples, e.g. examples/dingo-gov-lens/k8s/
// dingo-values.yaml) is used verbatim; otherwise it's assembled from discrete
// fields the same way database/plugin/metadata/{postgres,mysql}'s own
// RegisterProvider descriptor defaults + Start() methods do internally, so
// this tool connects with the exact credentials the running Dingo node was
// configured with.
//
// Selecting the postgres/mysql provider with no config section at all (or one
// missing every discrete field) is valid Dingo configuration: the provider's
// own descriptor default (see postgres/mysql's RegisterProvider) fills every
// field before Start() ever runs, so it always has a complete, connectable
// DSN. cfg may be nil — map indexing on a nil map is safe in Go and yields
// the same "absent" result as an empty map — so this always returns a
// non-empty DSN for a supported plugin rather than requiring the caller to
// have supplied at least one field.
func dsnFromMetadataConfig(plugin string, cfg map[string]any) string {
	if dsn, ok := stringConfigValue(cfg, "dsn"); ok && dsn != "" {
		return dsn
	}

	host, _ := stringConfigValue(cfg, "host")
	user, _ := stringConfigValue(cfg, "user")
	database, _ := stringConfigValue(cfg, "database")
	password, _ := stringConfigValue(cfg, "password")
	sslMode, _ := stringConfigValue(cfg, "sslMode")
	timeZone, _ := stringConfigValue(cfg, "timeZone")
	port := intConfigValue(cfg, "port")

	switch plugin {
	case "postgres":
		// Defaults match postgres.RegisterProvider's descriptor default
		// (Config{Host: "localhost", Port: 5432, User: "postgres", Database:
		// "postgres", SSLMode: "require", TimeZone: "UTC"}), which is what a
		// bare `provider: postgres` config section resolves to before
		// Start() builds its connection string the same way below.
		if host == "" {
			host = "localhost"
		}
		if user == "" {
			user = "postgres"
		}
		if database == "" {
			database = "postgres"
		}
		if sslMode == "" {
			sslMode = "require"
		}
		if timeZone == "" {
			timeZone = "UTC"
		}
		if port == 0 {
			port = 5432
		}
		parts := []string{
			"host=" + host,
			"user=" + user,
			"password=" + password,
			"dbname=" + database,
			"port=" + strconv.Itoa(port),
			"sslmode=" + sslMode,
			"TimeZone=" + timeZone,
		}
		return strings.Join(parts, " ")
	case "mysql":
		// Defaults match mysql.RegisterProvider's descriptor default
		// (Config{Host: "localhost", Port: 3306, User: "root", Database:
		// "dingo", TimeZone: "UTC"}). The DSN itself is built with
		// go-sql-driver/mysql's own Config/FormatDSN — the same type
		// database/plugin/metadata/mysql's provider uses — rather
		// than hand-formatting the connection string, so query-parameter
		// encoding (parseTime, allowNativePasswords, loc, tls) stays
		// byte-for-byte consistent with the real provider.
		if host == "" {
			host = "localhost"
		}
		if user == "" {
			user = "root"
		}
		if database == "" {
			database = "dingo"
		}
		if timeZone == "" {
			timeZone = "UTC"
		}
		if port == 0 {
			port = 3306
		}
		mcfg := mysqldriver.Config{
			User:                 user,
			Passwd:               password,
			Net:                  "tcp",
			Addr:                 fmt.Sprintf("%s:%d", host, port),
			DBName:               database,
			ParseTime:            true,
			AllowNativePasswords: true,
		}
		loc, err := time.LoadLocation(timeZone)
		if err != nil {
			loc = time.UTC
		}
		mcfg.Loc = loc
		mcfg.Params = map[string]string{"loc": timeZone}
		if sslMode != "" {
			mcfg.Params["tls"] = sslMode
		}
		return mcfg.FormatDSN()
	default:
		return ""
	}
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
		return "", fmt.Errorf(
			"--network must be 'preview' or 'preprod', got %q",
			net,
		)
	}
	return net, nil
}

// checkResultErr returns a non-nil error when result reports any FAIL or
// ERROR epoch, so 'check' and the default 'run' command signal an incomplete
// or failed parity check through the process exit code — automation invoking
// either must not be able to mistake FailEpochs/ErrorEpochs for success just
// because RunE returned nil. Both callers propagate this error up to main's
// rootCmd.Execute() error path (os.Exit(1)) rather than calling os.Exit
// themselves from within RunE.
func checkResultErr(result *koiosparity.CheckResult) error {
	if result == nil {
		return nil
	}
	switch {
	case len(result.FailEpochs) > 0 && len(result.ErrorEpochs) > 0:
		return fmt.Errorf(
			"parity check failed: %d failing epoch(s) %v, %d error epoch(s) %v",
			len(result.FailEpochs), result.FailEpochs,
			len(result.ErrorEpochs), result.ErrorEpochs,
		)
	case len(result.FailEpochs) > 0:
		return fmt.Errorf(
			"parity check failed: %d failing epoch(s) %v",
			len(result.FailEpochs),
			result.FailEpochs,
		)
	case len(result.ErrorEpochs) > 0:
		return fmt.Errorf(
			"parity check incomplete: %d error epoch(s) %v",
			len(result.ErrorEpochs),
			result.ErrorEpochs,
		)
	default:
		return nil
	}
}

// resolveGraceHours reads the --grace-hours flag and rejects a negative
// value, mirroring internal/config/validate.go's identical check on
// koiosParity.graceHours ("must not be negative; 0 selects the default of
// 24"). This matters beyond input hygiene: FetchAccountRewardsForEpoch's and
// compare.go's zero-row/reference-lag gates only disable the grace check
// when graceHours <= 0 — a negative value reaches that same "disabled" branch
// as 0, but arrives silently instead of through the explicit, documented
// opt-out. Left unvalidated, a just-closed epoch's empty --accounts response
// could be committed complete and later read back as a valid, authoritative
// empty reference set. Every subcommand exposing --grace-hours (fetch, check,
// run, watch) must call this instead of a bare GetInt.
func resolveGraceHours(cmd *cobra.Command) (int, error) {
	graceHours, _ := cmd.Flags().GetInt("grace-hours")
	if graceHours < 0 {
		return 0, fmt.Errorf(
			"--grace-hours must not be negative, got %d (0 disables the grace/reference-lag window)",
			graceHours,
		)
	}
	return graceHours, nil
}

// koiosAPIKey returns the Koios Bearer token from flag or environment.
func koiosAPIKey(cmd *cobra.Command) string {
	if key, _ := cmd.Flags().GetString("api-key"); key != "" {
		return key
	}
	return os.Getenv("KOIOS_API_KEY")
}

// addAccountsFlag registers the #3097 per-account exact-parity opt-in flag,
// shared by fetch/check/run/watch. Per-account fetching/checking issues far
// more Koios requests than pool-level work (a chunked request set covering
// the full address universe per epoch, versus one request per pool), so this
// stays opt-in for the standalone CLI even though the in-process observer
// (ObserverConfig.AccountsEnabled, wired from
// dingo.KoiosParityConfig/DefaultKoiosParityConfig) defaults it on.
func addAccountsFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("accounts", false,
		"also fetch/check #3097 per-account exact reward parity (opt-in: substantially more Koios requests; or KOIOS_PARITY_ACCOUNTS=true)")
}

// accountsEnabled resolves the --accounts flag, falling back to the
// koios-parity-only KOIOS_PARITY_ACCOUNTS env var (no existing repo-standard
// name applies to this tool-specific opt-in switch — see CLAUDE.md's env-var
// naming guidance in this tool's own conventions).
//
// Per CLAUDE.md's config precedence (CLI > env > YAML > defaults), an
// explicitly-set --accounts flag always wins, even --accounts=false — it
// must not be overridden by KOIOS_PARITY_ACCOUNTS=true in the environment.
func accountsEnabled(cmd *cobra.Command) bool {
	if cmd.Flags().Changed("accounts") {
		v, _ := cmd.Flags().GetBool("accounts")
		return v
	}
	v := strings.TrimSpace(os.Getenv("KOIOS_PARITY_ACCOUNTS"))
	return v == "1" || strings.EqualFold(v, "true")
}

// addAccountChunkFlags registers dingo #3099's --account-chunk-size/
// --account-chunk-max-bytes flags, shared by fetch/run/watch (the
// subcommands that actually issue /account_reward_history requests — check
// only reads the cache, so it has no use for these). 0 (the default for
// both) means "use the package default"
// (koiosparity.koiosAccountChunkSize/koiosAccountChunkMaxBytesDefault) —
// unset flags never change existing --accounts behavior.
func addAccountChunkFlags(cmd *cobra.Command) {
	cmd.Flags().Int("account-chunk-size", 0,
		"max stake addresses per /account_reward_history request when --accounts is set (0 = package default, 100)")
	cmd.Flags().Int("account-chunk-max-bytes", 0,
		"max encoded body size per /account_reward_history request when --accounts is set (0 = package default, 4KiB)")
}

// resolveAccountChunkFlags reads --account-chunk-size/--account-chunk-max-bytes,
// rejecting a negative value the same way resolveGraceHours does for
// --grace-hours: a negative value would silently reach chunkAddressesByCountAndSize's
// own "<=0 means use the default" branch instead of failing loudly on an
// obviously-wrong operator input.
func resolveAccountChunkFlags(
	cmd *cobra.Command,
) (size, maxBytes int, err error) {
	size, _ = cmd.Flags().GetInt("account-chunk-size")
	if size < 0 {
		return 0, 0, fmt.Errorf(
			"--account-chunk-size must not be negative, got %d (0 selects the package default)",
			size,
		)
	}
	maxBytes, _ = cmd.Flags().GetInt("account-chunk-max-bytes")
	if maxBytes < 0 {
		return 0, 0, fmt.Errorf(
			"--account-chunk-max-bytes must not be negative, got %d (0 selects the package default)",
			maxBytes,
		)
	}
	return size, maxBytes, nil
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
//
// Priority (highest first):
//  1. --metadata-plugin / --metadata-dsn flags — explicit overrides for
//     pointing this tool at a different copy of the database than the one
//     the live Dingo node itself is configured for.
//  2. Dingo's own resolved plugins.storage.metadata selection: loaded via
//     loadedDingoConfig(), which applies DINGO_PLUGINS_STORAGE_METADATA_PROVIDER
//     and DINGO_PLUGINS_STORAGE_METADATA_CONFIG_* the same way the real Dingo
//     process does (see internal/config.LoadConfig and plugin.ApplyEnvironment).
//  3. "sqlite" default, with no DSN (dingo_db.go's OpenDingoDB already
//     defaults an empty plugin to sqlite; this mirrors that explicitly).
func resolveDingoDB(cmd *cobra.Command) koiosparity.DingoDBConfig {
	plugin, _ := cmd.Flags().GetString("metadata-plugin")
	dsn, _ := cmd.Flags().GetString("metadata-dsn")

	var metadataConfig map[string]any
	if cfg := loadedDingoConfig(); cfg != nil {
		if plugin == "" {
			plugin = cfg.Plugins.Storage.Metadata.Provider
		}
		metadataConfig = cfg.Plugins.Storage.Metadata.Config
	}
	if plugin == "" {
		plugin = "sqlite"
	}

	if dsn == "" {
		dsn = dsnFromMetadataConfig(plugin, metadataConfig)
	}

	return koiosparity.DingoDBConfig{
		Plugin:  plugin,
		DataDir: resolveDingoDataDir(),
		DSN:     dsn,
	}
}
