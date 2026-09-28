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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/spf13/cobra"
)

func loadRun(ctx context.Context, args []string, cfg *config.Config) error {
	var immutablePath string

	// CLI argument takes priority over config
	if len(args) >= 1 {
		immutablePath = args[0]
	} else if cfg.ImmutableDbPath != "" {
		immutablePath = cfg.ImmutableDbPath
	} else {
		return errors.New(
			"path to ImmutableDB required (via argument or immutableDbPath config)",
		)
	}

	logger, err := commonRun(cfg)
	if err != nil {
		return err
	}
	if err := node.Load(ctx, cfg, logger, immutablePath); err != nil {
		return fmt.Errorf("loading ImmutableDB: %w", err)
	}
	return nil
}

func loadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load [db-path]",
		Short: "Load blocks from ImmutableDB (path via arg or immutableDbPath config)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			return loadRun(cmd.Context(), args, cfg)
		},
	}
	return cmd
}
