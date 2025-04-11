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

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/spf13/cobra"
)

func loadRun(_ *cobra.Command, args []string, cfg *config.Config) {
	if len(args) != 1 {
		slog.Error("you must provide the path to an ImmutableDB")
		os.Exit(1)
	}
	logger := commonRun()
	if err := node.Load(cfg, logger, args[0]); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func loadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load db-path",
		Short: "Load blocks from ImmutableDB",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				slog.Error("no config found in context")
				os.Exit(1)
			}
			loadRun(cmd, args, cfg)
		},
	}
	return cmd
}
