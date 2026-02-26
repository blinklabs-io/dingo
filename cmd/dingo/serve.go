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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/spf13/cobra"
)

func serveRun(_ *cobra.Command, _ []string, cfg *config.Config) {
	logger := commonRun()

	// Check for an in-progress sync. If the "sync_status" key in
	// the sync_state table holds a non-empty value, a previous sync
	// did not complete. The user must finish (or re-run) the sync
	// before starting the node.
	if err := checkSyncState(cfg, logger); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	// Run node
	if err := node.Run(cfg, logger); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func checkSyncState(
	cfg *config.Config,
	logger *slog.Logger,
) error {
	db, err := database.New(&database.Config{
		DataDir:        cfg.DatabasePath,
		Logger:         logger,
		BlobPlugin:     cfg.BlobPlugin,
		MetadataPlugin: cfg.MetadataPlugin,
		MaxConnections: 1,
	})
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	val, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return fmt.Errorf("checking sync state: %w", err)
	}
	if val == "" {
		return nil
	}
	return fmt.Errorf(
		"incomplete sync detected (sync_status=%q). "+
			"Run 'dingo sync' (or 'dingo sync --mithril' for "+
			"Mithril bootstrap) to resume before starting the node",
		val,
	)
}

func serveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run as a node",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				slog.Error("no config found in context")
				os.Exit(1)
			}
			serveRun(cmd, args, cfg)
		},
	}
	return cmd
}
