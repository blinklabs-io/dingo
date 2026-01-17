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
	"os"
	"runtime/pprof"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
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

func commonRun() *slog.Logger {
	// Configure logger
	logLevel := slog.LevelInfo
	addSource := false
	if globalFlags.debug {
		logLevel = slog.LevelDebug
		addSource = true
	}
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: addSource,
			Level:     logLevel,
		}),
	)
	slog.SetDefault(logger)
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

func listPlugins(
	blobPlugin, metadataPlugin string,
) (shouldExit bool, output string) {
	var buf strings.Builder
	listed := false

	if blobPlugin == "list" {
		buf.WriteString("Available blob plugins:\n")
		blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
		for _, p := range blobPlugins {
			buf.WriteString(fmt.Sprintf("  %s: %s\n", p.Name, p.Description))
		}
		listed = true
	}

	if metadataPlugin == "list" {
		if listed {
			buf.WriteString("\n")
		}
		buf.WriteString("Available metadata plugins:\n")
		metadataPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)
		for _, p := range metadataPlugins {
			buf.WriteString(fmt.Sprintf("  %s: %s\n", p.Name, p.Description))
		}
		listed = true
	}

	if listed {
		return true, buf.String()
	}
	return false, ""
}

func listAllPlugins() string {
	var buf strings.Builder
	buf.WriteString("Available plugins:\n\n")

	buf.WriteString("Blob Storage Plugins:\n")
	blobPlugins := plugin.GetPlugins(plugin.PluginTypeBlob)
	for _, p := range blobPlugins {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", p.Name, p.Description))
	}

	buf.WriteString("\nMetadata Storage Plugins:\n")
	metadataPlugins := plugin.GetPlugins(plugin.PluginTypeMetadata)
	for _, p := range metadataPlugins {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", p.Name, p.Description))
	}

	return buf.String()
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
	programName := "dingo"
	if len(os.Args) > 0 {
		programName = os.Args[0]
	}
	os.Args = append([]string{programName}, newArgs...)

	// Initialize CPU profiling (starts immediately, stops on exit)
	if cpuprofile != "" {
		fmt.Fprintf(os.Stderr, "Starting CPU profiling to %s\n", cpuprofile)
		f, err := os.Create(cpuprofile)
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
		Use: programName,
		Run: func(cmd *cobra.Command, args []string) {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				slog.Error("no config found in context")
				os.Exit(1)
			}

			// When no subcommand given, check RunMode from config
			switch cfg.RunMode {
			case config.RunModeLoad:
				if cfg.ImmutableDbPath == "" {
					slog.Error(
						"immutableDbPath must be set when runMode is 'load'",
					)
					os.Exit(1)
				}
				loadRun(cmd, []string{cfg.ImmutableDbPath}, cfg)
			case config.RunModeServe, config.RunModeDev:
				// serve and dev modes both run the server
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
	rootCmd.PersistentFlags().
		StringP("blob", "b", config.DefaultBlobPlugin, "blob store plugin to use, 'list' to show available")
	rootCmd.PersistentFlags().
		StringP("metadata", "m", config.DefaultMetadataPlugin, "metadata store plugin to use, 'list' to show available")
	rootCmd.PersistentFlags().
		Int("db-workers", 5, "database worker pool worker count")
	rootCmd.PersistentFlags().
		Int("db-queue-size", 50, "database worker pool task queue size")

	// Add plugin-specific flags
	if err := plugin.PopulateCmdlineOptions(rootCmd.PersistentFlags()); err != nil {
		fmt.Fprintf(os.Stderr, "Error adding plugin flags: %v\n", err)
		os.Exit(1)
	}

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Handle plugin listing before config loading
		blobPlugin, _ := cmd.Root().PersistentFlags().GetString("blob")
		metadataPlugin, _ := cmd.Root().PersistentFlags().GetString("metadata")

		shouldExit, output := listPlugins(blobPlugin, metadataPlugin)
		if shouldExit {
			fmt.Print(output)
			os.Exit(0)
		}

		cfg, err := config.LoadConfig(configFile)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		// Override config with command line flags
		if blobPlugin != config.DefaultBlobPlugin {
			cfg.BlobPlugin = blobPlugin
		}
		if metadataPlugin != config.DefaultMetadataPlugin {
			cfg.MetadataPlugin = metadataPlugin
		}

		// Override database worker pool config if flags are provided
		if cmd.Root().PersistentFlags().Changed("db-workers") {
			if workers, err := cmd.Root().PersistentFlags().GetInt("db-workers"); err == nil {
				cfg.DatabaseWorkers = workers
			}
		}
		if cmd.Root().PersistentFlags().Changed("db-queue-size") {
			if queueSize, err := cmd.Root().PersistentFlags().GetInt("db-queue-size"); err == nil {
				cfg.DatabaseQueueSize = queueSize
			}
		}

		cmd.SetContext(config.WithContext(cmd.Context(), cfg))
		return nil
	}

	// Subcommands
	rootCmd.AddCommand(serveCommand())
	rootCmd.AddCommand(loadCommand())
	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(versionCommand())

	// Execute cobra command
	exitCode := 0
	if err := rootCmd.Execute(); err != nil {
		exitCode = 1
	}

	// Finalize memory profiling before exit
	if memprofile != "" {
		f, err := os.Create(memprofile)
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
