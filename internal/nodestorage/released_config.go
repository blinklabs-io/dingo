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

package nodestorage

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
)

// MigrateReleased returns the StorageDependencies.MigrateReleased of the
// ledger database cfg configures: the move of a v0.94.2 database's ledger
// state out of its metadata store, run before anything opens the ledger's
// database. nodeCfg may be nil, in which case it is loaded from cfg when a
// migration runs.
func MigrateReleased(
	cfg *config.Config,
	dbConfig *database.Config,
	nodeCfg *cardano.CardanoNodeConfig,
	logger *slog.Logger,
) func(
	context.Context,
	blob.BlobStore,
	func(context.Context) (metadata.MetadataStore, func() error, error),
) error {
	return func(
		ctx context.Context,
		blobStore blob.BlobStore,
		openMetadata func(context.Context) (metadata.MetadataStore, func() error, error),
	) error {
		state, err := database.ReadLedgerStoreState(blobStore)
		if err != nil || state != database.LedgerStoreReleased {
			return err
		}
		if nodeCfg == nil {
			if nodeCfg, err = LoadNodeConfig(cfg); err != nil {
				return err
			}
		}
		_, err = ReleasedMigration{
			Config:       dbConfig,
			Blob:         blobStore,
			OpenMetadata: openMetadata,
			UtxoSet: UtxoSetConfig{
				DataDir:           cfg.DatabasePath,
				Logger:            logger,
				CardanoNodeConfig: nodeCfg,
			},
			Logger:                     logger,
			RewindBlocks:               SecurityParam(nodeCfg),
			DelegatorInactivityEnabled: cfg.DelegatorInactivityEnabled,
			DelegatorInactivity:        cfg.DelegatorInactivity,
		}.Run(ctx)
		return err
	}
}

// LoadNodeConfig loads the Cardano node configuration cfg names, the
// embedded one of its network (preview when none) when it names no file.
func LoadNodeConfig(cfg *config.Config) (*cardano.CardanoNodeConfig, error) {
	path := cfg.CardanoConfig
	network := cfg.Network
	if path == "" {
		if network == "" {
			network = "preview"
		}
		path = cardano.EmbeddedConfigPath(network)
	}
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		path, network, cardano.EmbeddedConfigFS,
	)
	if err != nil {
		return nil, fmt.Errorf("loading cardano node config: %w", err)
	}
	return nodeCfg, nil
}
