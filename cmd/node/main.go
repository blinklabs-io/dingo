// Copyright 2024 Blink Labs Software
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

	"github.com/blinklabs-io/node/internal/node"
	"github.com/blinklabs-io/node/internal/version"
	"github.com/spf13/cobra"
)

const (
	programName = "node"
)

func main() {
	globalFlags := struct {
		version bool
		debug   bool
	}{}

	rootCmd := &cobra.Command{
		Use: programName,
		Run: func(cmd *cobra.Command, args []string) {
			if globalFlags.version {
				fmt.Printf("%s %s\n", programName, version.GetVersionString())
				os.Exit(0)
			}
			// Configure logger
			logLevel := slog.LevelInfo
			if globalFlags.debug {
				logLevel = slog.LevelDebug
			}
			logger := slog.New(
				slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
					Level: logLevel,
				}),
			)
			slog.SetDefault(logger)
			// Run node
			logger.Info(fmt.Sprintf("running: %s version %s", programName, version.GetVersionString()))
			if err := node.Run(logger); err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		},
	}

	// Global flags
	rootCmd.PersistentFlags().
		BoolVarP(&globalFlags.debug, "debug", "D", false, "enable debug logging")
	rootCmd.PersistentFlags().
		BoolVarP(&globalFlags.version, "version", "", false, "show version and exit")

	// Execute cobra command
	if err := rootCmd.Execute(); err != nil {
		// NOTE: we purposely don't display the error, since cobra will have already displayed it
		os.Exit(1)
	}
}
