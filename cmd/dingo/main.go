// Copyright 2026 Blink Labs Software
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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"

	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/version"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"
)

const (
	programName = "dingo"
)

func slogPrintf(format string, v ...any) {
	slog.Info(fmt.Sprintf(format, v...),
		"component", programName,
	)
}

var (
	globalFlags = struct {
		debug bool
	}{}
	configFile string
)

func commonRun(cfg *config.Config) *slog.Logger {
	// Configure logger from config. The --debug flag is the highest-precedence
	// override (CLI > env > YAML > defaults): it forces debug level and source
	// locations regardless of the configured level.
	logger, levelOK, formatOK := newLogger(
		os.Stdout,
		cfg.Logging.Format,
		cfg.Logging.Level,
		globalFlags.debug,
	)
	slog.SetDefault(logger)
	// Config-validation warnings go to stderr, not the logger: a high
	// configured level (e.g. error) would otherwise suppress them, leaving the
	// operator with no feedback that their value was ignored.
	if !levelOK {
		fmt.Fprintf(
			os.Stderr,
			"%s: unknown logging level %q, using info\n",
			programName, cfg.Logging.Level,
		)
	}
	if !formatOK {
		fmt.Fprintf(
			os.Stderr,
			"%s: unknown logging format %q, using text\n",
			programName, cfg.Logging.Format,
		)
	}
	// Configure max processes with our logger wrapper, toss undo func
	_, err := maxprocs.Set(maxprocs.Logger(slogPrintf))
	if err != nil {
		// If we hit this, something really wrong happened
		slog.Error(err.Error())
		os.Exit(1)
	}
	logger.Info(
		"version: "+version.GetVersionString(),
		"component", programName,
	)
	return logger
}

// newLogger builds a slog.Logger writing to w. format selects the handler
// ("json" for JSON, anything else for text); level sets the minimum level;
// debug forces debug level and source locations. The two bools report whether
// level/format were recognized so the caller can warn on unknown values.
func newLogger(
	w io.Writer,
	format, level string,
	debug bool,
) (*slog.Logger, bool, bool) {
	logLevel, levelOK := parseLogLevel(level)
	addSource := false
	if debug {
		logLevel = slog.LevelDebug
		addSource = true
	}
	opts := &slog.HandlerOptions{
		AddSource: addSource,
		Level:     logLevel,
	}
	f := strings.ToLower(strings.TrimSpace(format))
	formatOK := f == "" || f == "text" || f == "json"
	var handler slog.Handler
	if f == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}
	return slog.New(handler), levelOK, formatOK
}

// parseLogLevel maps a config level string to a slog.Level. It returns
// ok=false for an unrecognized value so the caller can warn; an empty value
// is treated as the default ("info").
func parseLogLevel(level string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, true
	case "", "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return slog.LevelInfo, false
	}
}

func listAllPlugins() string {
	var buf strings.Builder
	host, err := internalplugins.NewHost()
	if err != nil {
		return fmt.Sprintf("failed to build plugin host: %v\n", err)
	}
	buf.WriteString("Available plugins:\n")
	var previous string
	for _, provider := range host.Providers() {
		capability := string(provider.Capability)
		if capability != previous {
			fmt.Fprintf(&buf, "\n%s:\n", capability)
			previous = capability
		}
		fmt.Fprintf(&buf, "  %s: %s\n", provider.Name, provider.Description)
	}

	return buf.String()
}

// topLevelCommand returns the top-level subcommand under root for cmd
// (walking up past any nested subcommands such as `mithril list`), or
// nil when cmd is the bare root command itself.
func topLevelCommand(cmd *cobra.Command) *cobra.Command {
	if !cmd.HasParent() {
		return nil
	}
	top := cmd
	for top.Parent().HasParent() {
		top = top.Parent()
	}
	return top
}

// effectiveRunMode reports the run mode a command invocation will
// actually execute, which governs the config it requires. Only the bare
// `dingo` process honors the configured runMode; each explicit
// subcommand runs a fixed operation, so its listener/source requirements
// come from the command (cmd, the leaf being run) rather than cfg.RunMode.
func effectiveRunMode(cmd *cobra.Command, cfg *config.Config) config.RunMode {
	top := topLevelCommand(cmd)
	if top == nil {
		// Bare root command dispatches on the configured run mode,
		// falling through to serve for an empty or unrecognized mode
		// (matching rootCmd's dispatch default). Serving-listener checks
		// therefore still apply to an invalid runMode; the invalid mode
		// itself is reported separately by Validate.
		if cfg.RunMode.Valid() && cfg.RunMode != "" {
			return cfg.RunMode
		}
		return config.RunModeServe
	}
	switch top.Name() {
	case "serve":
		return config.RunModeServe
	case "load":
		return config.RunModeLoad
	case "sync":
		// `dingo sync [--mithril]` runs the Mithril snapshot sync, which
		// starts a metrics listener and an optional pprof debug listener.
		return config.RunModeSync
	case "mithril":
		// Only `mithril sync` binds the metrics/debug listeners; the
		// read-only `mithril list` / `mithril show` (and bare `mithril`)
		// query the aggregator and start nothing.
		if cmd.Name() == "sync" {
			return config.RunModeSync
		}
		return config.RunModeMithril
	default:
		// No other non-informational top-level command exists today;
		// fall back to full serving validation so a future command's
		// misconfiguration is caught rather than silently skipped.
		return config.RunModeServe
	}
}

// isInformationalCommand reports whether a top-level command only prints
// static information — version, list, and cobra's built-in help and
// completion commands — and therefore needs no runtime configuration at
// all: config loading and validation are skipped for them. (cobra runs
// the root PersistentPreRunE for help and completion too, so without
// the exemption a missing or invalid config would block `dingo help`
// and shell-completion generation.)
func isInformationalCommand(top *cobra.Command) bool {
	if top == nil {
		return false
	}
	switch top.Name() {
	case "version", "list", "help", "completion":
		return true
	default:
		return false
	}
}

func listCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all available plugins",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Print(listAllPlugins())
		},
	}
	return cmd
}

func main() {
	// Parse profiling flags before cobra setup (handle both --flag=value and --flag value syntax)
	cpuprofile := ""
	memprofile := ""
	var newArgs []string
	args := os.Args
	if len(args) > 0 {
		args = args[1:] // Skip program name
	} else {
		args = []string{}
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case strings.HasPrefix(arg, "--cpuprofile="):
			cpuprofile = strings.TrimPrefix(arg, "--cpuprofile=")
		case arg == "--cpuprofile" && i+1 < len(args):
			cpuprofile = args[i+1]
			i++ // Skip next arg
		case strings.HasPrefix(arg, "--memprofile="):
			memprofile = strings.TrimPrefix(arg, "--memprofile=")
		case arg == "--memprofile" && i+1 < len(args):
			memprofile = args[i+1]
			i++ // Skip next arg
		default:
			// Not a profiling flag, keep it
			newArgs = append(newArgs, arg)
		}
	}
	// Reconstruct os.Args with program name (os.Args[0] is never nil in practice, but nilaway doesn't know this)
	progArg := programName
	if len(os.Args) > 0 {
		progArg = os.Args[0]
	}
	os.Args = append([]string{progArg}, newArgs...)

	// Initialize CPU profiling (starts immediately, stops on exit)
	if cpuprofile != "" {
		cpuprofile = filepath.Clean(cpuprofile)
		fmt.Fprintf(os.Stderr, "Starting CPU profiling to %q\n", cpuprofile)         //nolint:gosec // stderr output, no XSS risk
		f, err := os.OpenFile(cpuprofile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // user-specified profiling output path
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer func() {
			pprof.StopCPUProfile()
			fmt.Fprintf(os.Stderr, "CPU profiling stopped\n")
		}()
	}

	rootCmd := &cobra.Command{
		Use:   programName,
		Short: "Dingo - a Go Cardano node",
		Long: `Dingo - a Go Cardano node by Blink Labs.

Configuration Precedence (highest to lowest):
  1. CLI flags          (e.g. --network preview)
  2. Environment vars   (e.g. CARDANO_NETWORK=preview)
  3. Config file        (dingo.yaml or --config path)
  4. Built-in defaults

Plugins:
  Provider selectors use --blob, --metadata, --mempool,
  --blockfrost-provider, --mesh-provider, and --utxorpc-provider.
  Provider configuration uses the plugins YAML tree or generic
  DINGO_PLUGINS_* environment variables. Run 'dingo list' to see providers.

Storage Mode:
  --storage-mode sets the global storage mode for all plugins. Use "core"
  for minimal validation data or "api" for full indexing (witnesses,
  scripts, datums, redeemers). Dev mode always enables "api" mode.

Network:
  --network sets the Cardano network name and automatically derives the
  path to the corresponding Cardano node config files (genesis, topology).
  This overrides the CARDANO_NETWORK env var and the network config field.

Database Workers:
  --db-workers controls the worker pool size.`,
		Run: func(cmd *cobra.Command, args []string) {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				slog.Error("no config found in context")
				os.Exit(1)
			}

			// When no subcommand given, check RunMode from config.
			// cfg.RunMode is a configured mode (validated by
			// RunMode.Valid); the effective-only sync/mithril modes never
			// reach here.
			switch cfg.RunMode { //nolint:exhaustive // sync/mithril are effective-only, never configured
			case config.RunModeLoad:
				// Validate() has already enforced that ImmutableDbPath
				// is set for load mode
				loadRun(cmd.Context(), []string{cfg.ImmutableDbPath}, cfg)
			case config.RunModeServe, config.RunModeDev, config.RunModeLeios:
				// serve, dev, and leios modes all run the server
				serveRun(cmd, args, cfg)
			default:
				// Empty or unrecognized RunMode defaults to serve mode
				serveRun(cmd, args, cfg)
			}
		},
	}

	// Global flags
	rootCmd.PersistentFlags().
		BoolVarP(&globalFlags.debug, "debug", "D", false, "enable debug logging")
	rootCmd.PersistentFlags().
		StringVar(&configFile, "config", "", "path to config file")
	config.RegisterFlags(rootCmd)

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		top := topLevelCommand(cmd)

		// The informational commands (version, list, help, completion)
		// print static output and read no configuration, so they skip
		// config loading and validation entirely: they must work even
		// when the config file is missing or invalid.
		if isInformationalCommand(top) {
			return nil
		}

		cfg, err := config.LoadConfig(configFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		if err := config.ApplyFlags(cmd, cfg); err != nil {
			return fmt.Errorf("applying CLI flags: %w", err)
		}

		// `dingo load <path>`: the positional argument is the
		// highest-precedence source for ImmutableDbPath; merge it before
		// validation so a config with runMode "load" and no
		// immutableDbPath doesn't fail spuriously.
		if top != nil && top.Name() == "load" && len(args) > 0 {
			cfg.ImmutableDbPath = args[0]
		}

		// Every configuration source is merged at this point (defaults,
		// YAML, environment, CLI flags, and the load positional
		// argument), so defaults derived from other settings can now be
		// filled in and the final configuration validated.
		cfg.ApplyDefaults()

		if err := cfg.Validate(effectiveRunMode(cmd, cfg)); err != nil {
			return fmt.Errorf("invalid configuration: %w", err)
		}

		// Topology derives from the network and topology settings, so it
		// is resolved only now that the merged configuration is final and
		// valid — resolving it earlier could reject a YAML/env value a
		// CLI flag has since repaired.
		if _, err := config.LoadTopologyConfig(); err != nil {
			return fmt.Errorf("loading topology: %w", err)
		}

		cmd.SetContext(config.WithContext(cmd.Context(), cfg))
		return nil
	}

	// Subcommands
	rootCmd.AddCommand(serveCommand())
	rootCmd.AddCommand(loadCommand())
	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(versionCommand())
	rootCmd.AddCommand(mithrilCommand())
	rootCmd.AddCommand(syncCommand())

	// Execute cobra command
	exitCode := 0
	if err := rootCmd.Execute(); err != nil {
		exitCode = 1
	}

	// Finalize memory profiling before exit
	if memprofile != "" {
		memprofile = filepath.Clean(memprofile)
		f, err := os.OpenFile(memprofile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // user-specified profiling output path
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create memory profile: %v\n", err)
		} else {
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "could not write memory profile: %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "Memory profiling complete\n")
			}
			f.Close()
		}
	}

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
