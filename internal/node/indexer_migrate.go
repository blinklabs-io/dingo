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

package node

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/storageadopt"
)

// MigrateToIndexerDB recovers any interrupted restore before adopting API
// storage. SQLite copies the API store into the IndexerDB directory. PostgreSQL
// and MySQL keep the API store in place and provision a separate consensus
// database.
//
// The store is copied through <databasePath>/.indexer-migrate-*, the copy
// gets the ledger tip as its tip and checkpoint plus the layout latch, and a
// rename publishes it as <databasePath>/indexer. The consensus store then
// latches the layout and moves from API to core mode, which keeps its API rows
// on disk unread. Every step is resumable: before the rename the original is
// untouched and the next start copies again; after it, the next start finds
// the latched IndexerDB and completes the consensus store's latches.
func MigrateToIndexerDB(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
) error {
	nodeCfg, err := dingo.NewConfigFromInternal(cfg, logger, nil, nil, nil)
	if err != nil {
		return err
	}
	if err := dingo.ReconcileInterruptedRestore(ctx, nodeCfg); err != nil {
		return fmt.Errorf("recover interrupted restore before storage migration: %w", err)
	}
	return storageadopt.MigrateToIndexerDB(ctx, cfg, logger)
}
