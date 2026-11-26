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

package plugins

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"sync"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
)

// StorageSelections identifies the providers and provider-owned
// configuration for the selectable storage capabilities.
type StorageSelections struct {
	// IndexerDB keeps a client/server API database in place as IndexerDB
	// and selects the provider-owned consensus namespace beside it.
	IndexerDB bool
	Blob      plugin.Selection
	Metadata  plugin.Selection
}

// ConfiguredStorageSelections returns the storage selections of cfg. Every
// open of a node's own stores goes through it: a selection that omits
// IndexerDB opens an adopted PostgreSQL or MySQL IndexerDB as the
// consensus store, which latches core mode into it.
func ConfiguredStorageSelections(cfg *config.Config) StorageSelections {
	return StorageSelections{
		IndexerDB: cfg.HasIndexerDB() || cfg.ServerCoreNamespace,
		Blob:      cfg.Plugins.Storage.Blob,
		Metadata:  cfg.Plugins.Storage.Metadata,
	}
}

// StorageDependencies contains shared application settings injected into
// storage providers. These settings are deliberately not provider config.
type StorageDependencies struct {
	// GenesisStakingEmpty certifies that the loaded genesis has no initial staking.
	GenesisStakingEmpty bool
	DataDir             string
	RunMode             string
	StorageMode         string
	MaxConnections      int
	Logger              *slog.Logger
	PromRegistry        prometheus.Registerer
	// TracingEnabled is forwarded to metadata.ProviderDependencies; see its
	// doc comment.
	TracingEnabled bool
	// StatementGuard is forwarded to metadata.ProviderDependencies.
	StatementGuard metadata.StatementGuard
	// MigrateReleased, when set, is run by OpenDatabase before it opens a
	// ledger database: it moves the ledger state of a v0.94.2 database out of
	// the metadata store openMetadata opens (see nodestorage.ReleasedMigration).
	// The resolved stores include the provider needed by existing ImmutableDB
	// markers; their lifecycle remains owned by the host.
	// Without it, a v0.94.2 database fails to open.
	MigrateReleased func(
		ctx context.Context,
		stores database.Stores,
		openMetadata func(context.Context) (metadata.MetadataStore, func() error, error),
	) error
}

// ResolveStorage starts the stores of the ledger's database, the block store
// and the ImmutableDB, in dependency order. The ledger's database keeps no
// metadata store; ResolveMetadata starts one for the databases that do. The
// supplied host owns their lifecycle; callers must stop it if resolution
// fails after one provider has started.
func ResolveStorage(
	ctx context.Context,
	host *plugin.Host,
	selections StorageSelections,
	deps StorageDependencies,
) (database.Stores, error) {
	blobStore, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		selections.Blob.Provider,
		selections.Blob.Config,
		blob.ProviderDependencies{
			DataDir:      deps.DataDir,
			RunMode:      deps.RunMode,
			StorageMode:  deps.StorageMode,
			Logger:       deps.Logger,
			PromRegistry: deps.PromRegistry,
		},
	)
	if err != nil {
		return database.Stores{}, fmt.Errorf("resolve blob storage: %w", err)
	}
	immutableProvider := immplugin.ProviderNameForBlob(selections.Blob.Provider)
	var immutableObjects blob.ImmutableObjects
	if immutableProvider == immplugin.S3ProviderName ||
		immutableProvider == immplugin.GCSProviderName {
		var ok bool
		immutableObjects, ok = blobStore.(blob.ImmutableObjects)
		if !ok {
			return database.Stores{}, fmt.Errorf(
				"blob provider %q does not expose ImmutableDB file operations",
				selections.Blob.Provider,
			)
		}
	}
	immutableStore, err := plugin.Resolve[immplugin.ImmutableDB](
		ctx,
		host,
		plugin.CapabilityStorageImmutable,
		immutableProvider,
		nil,
		immplugin.ProviderDependencies{
			DataDir:     deps.DataDir,
			Logger:      deps.Logger,
			ObjectFiles: immutableObjects,
		},
	)
	if err != nil {
		return database.Stores{}, fmt.Errorf(
			"resolve immutable storage: %w",
			err,
		)
	}
	return database.Stores{
		Blob:      blobStore,
		Immutable: immutableStore,
	}, nil
}

// ResolveMetadata starts the selected metadata provider on host: on a server
// provider in the consensus namespace when the selection says the API store
// beside it was adopted as the IndexerDB.
func ResolveMetadata(
	ctx context.Context,
	host *plugin.Host,
	selections StorageSelections,
	deps StorageDependencies,
) (metadata.MetadataStore, error) {
	store, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		selections.Metadata.Provider,
		selections.Metadata.Config,
		metadata.ProviderDependencies{
			CoreNamespace: selections.IndexerDB &&
				(selections.Metadata.Provider == "postgres" || selections.Metadata.Provider == "mysql"),
			DataDir:             deps.DataDir,
			StorageMode:         deps.StorageMode,
			MaxConnections:      deps.MaxConnections,
			Logger:              deps.Logger,
			PromRegistry:        deps.PromRegistry,
			TracingEnabled:      deps.TracingEnabled,
			GenesisStakingEmpty: deps.GenesisStakingEmpty,
			StatementGuard:      deps.StatementGuard,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("resolve metadata storage: %w", err)
	}
	return store, nil
}

// ReleasedMetadataOpener returns the function nodestorage.ReleasedMigration
// opens a v0.94.2 metadata store with: the configured metadata provider, on a
// host of its own that closing the store stops. A v0.94.2 database holds its
// ledger state where the configured provider puts it, and on a server
// provider in the consensus namespace an adopted API store moved it to.
func ReleasedMetadataOpener(
	selections StorageSelections,
	deps StorageDependencies,
) func(context.Context) (metadata.MetadataStore, func() error, error) {
	return func(
		ctx context.Context,
	) (metadata.MetadataStore, func() error, error) {
		host, err := NewHost()
		if err != nil {
			return nil, nil, err
		}
		// The ledger's database registers the store's collectors once it
		// opens; this store is gone by then.
		deps.PromRegistry = nil
		deps.StatementGuard = nil
		store, err := ResolveMetadata(ctx, host, selections, deps)
		if err != nil {
			return nil, nil, errors.Join(
				err, host.Stop(context.WithoutCancel(ctx)),
			)
		}
		return store, func() error {
			return host.Stop(context.WithoutCancel(ctx))
		}, nil
	}
}

// IndexerDirName is the directory under the data directory holding a SQLite
// IndexerDB.
const IndexerDirName = "indexer"

// IndexerMetadataSelection derives the IndexerDB's metadata provider config
// from the released metadata selection. SQLite gets its own directory. On
// server stores the released database remains the IndexerDB while
// ResolveStorage selects the provider-owned consensus namespace, so a
// selection without IndexerDB would share its tables with the consensus
// store and is refused.
func IndexerMetadataSelection(
	selections StorageSelections,
	dataDir string,
) (plugin.Selection, string, error) {
	provider := selections.Metadata.Provider
	cfg := maps.Clone(selections.Metadata.Config)
	if cfg == nil {
		cfg = map[string]any{}
	}
	indexerDir := dataDir
	if provider == "sqlite" {
		// The consensus store's directory override must not be inherited.
		delete(cfg, "dataDir")
		if dataDir != "" {
			indexerDir = filepath.Join(dataDir, IndexerDirName)
		}
	} else if !selections.IndexerDB {
		return plugin.Selection{}, "", fmt.Errorf(
			"the %s IndexerDB needs the consensus store in its own "+
				"namespace: the storage selection does not select it",
			provider,
		)
	}
	return plugin.Selection{Provider: provider, Config: cfg}, indexerDir, nil
}

// ResolveIndexerMetadata starts the IndexerDB's own metadata store on host.
// It is opened in API mode: the IndexerDB is the store that holds API rows.
func ResolveIndexerMetadata(
	ctx context.Context,
	host *plugin.Host,
	selections StorageSelections,
	deps StorageDependencies,
) (metadata.MetadataStore, error) {
	selection, dataDir, err := IndexerMetadataSelection(
		selections,
		deps.DataDir,
	)
	if err != nil {
		return nil, err
	}
	store, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		selection.Provider,
		selection.Config,
		metadata.ProviderDependencies{
			DataDir:             dataDir,
			StorageMode:         "api",
			MaxConnections:      deps.MaxConnections,
			Logger:              deps.Logger,
			PromRegistry:        deps.PromRegistry,
			TracingEnabled:      deps.TracingEnabled,
			GenesisStakingEmpty: deps.GenesisStakingEmpty,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("resolve indexer metadata storage: %w", err)
	}
	return store, nil
}

// IndexerMetricsRole is the value of the role label on the IndexerDB's
// metrics. IndexerMetricsPrefix also prefixes their names: a collector
// registered twice under one name with different label sets panics, so the
// label alone cannot let both databases register the same collectors.
const (
	IndexerMetricsRole   = "indexer"
	IndexerMetricsPrefix = "indexer_"
)

// OpenIndexerDatabase composes the IndexerDB: its own metadata store, opened
// in API mode on host, which also keeps its UTxO and transaction CBOR
// entries, reading blocks from the consensus database's block store. Its
// metrics carry role="indexer" and an indexer_ name prefix so they register
// beside the consensus database's.
//
// The caller closes the returned database before the host stops, and before
// the consensus database closes the block store it reads.
func OpenIndexerDatabase(
	ctx context.Context,
	host *plugin.Host,
	consensus *database.Database,
	dbConfig *database.Config,
	selections StorageSelections,
	deps StorageDependencies,
) (*database.Database, error) {
	if deps.PromRegistry != nil {
		deps.PromRegistry = prometheus.WrapRegistererWithPrefix(
			IndexerMetricsPrefix,
			prometheus.WrapRegistererWith(
				prometheus.Labels{
					"role": IndexerMetricsRole,
				},
				deps.PromRegistry,
			),
		)
	}
	store, err := ResolveIndexerMetadata(ctx, host, selections, deps)
	if err != nil {
		return nil, err
	}
	cfg := *dbConfig
	cfg.Role = database.RoleIndexer
	cfg.StorageMode = "api"
	cfg.PromRegistry = deps.PromRegistry
	blocks, err := database.NewIndexerBlobStore(consensus.Blob(), store)
	if err != nil {
		return nil, err
	}
	db, err := database.New(ctx, &cfg, database.Stores{
		Blob:     blocks,
		Metadata: store,
	})
	if err != nil {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
		return nil, fmt.Errorf("open indexer database: %w", err)
	}
	return db, nil
}

// DatabaseRuntime is a composed database plus the plugin host that owns its
// injected stores. Close preserves database-before-store shutdown ordering.
type DatabaseRuntime struct {
	Database *database.Database
	Host     *plugin.Host
	once     sync.Once
	err      error
}

// Close stops database-owned resources before provider-owned stores.
func (r *DatabaseRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		if r.Database != nil {
			r.err = errors.Join(r.err, r.Database.Close()) //nolint:contextcheck
		}
		if r.Host != nil {
			r.err = errors.Join(r.err, r.Host.Stop(ctx))
		}
	})
	return r.err
}

// OpenDatabase explicitly composes a fresh host, storage providers, and a
// database. A non-nil error is returned only after all started resources have
// been stopped.
func OpenDatabase(
	ctx context.Context,
	dbConfig *database.Config,
	selections StorageSelections,
	deps StorageDependencies,
) (*DatabaseRuntime, error) {
	host, err := NewHost()
	if err != nil {
		return nil, err
	}
	stores, err := ResolveStorage(ctx, host, selections, deps)
	if err != nil {
		_ = host.Stop(context.WithoutCancel(ctx))
		return nil, err
	}
	if dbConfig != nil && dbConfig.Role != "" {
		store, err := ResolveMetadata(ctx, host, selections, deps)
		if err != nil {
			_ = host.Stop(context.WithoutCancel(ctx))
			return nil, err
		}
		stores.Metadata = store
	}
	if deps.MigrateReleased != nil && dbConfig != nil && dbConfig.Role == "" {
		if err := deps.MigrateReleased(
			ctx, stores, ReleasedMetadataOpener(selections, deps),
		); err != nil {
			return nil, errors.Join(
				fmt.Errorf("migrate the v0.94.2 ledger state: %w", err),
				host.Stop(context.WithoutCancel(ctx)),
			)
		}
	}
	db, err := database.New(ctx, dbConfig, stores) //nolint:contextcheck
	runtime := &DatabaseRuntime{Database: db, Host: host}
	if db == nil {
		closeErr := runtime.Close(context.WithoutCancel(ctx))
		if err != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, errors.Join(
			errors.New("database construction returned no database"),
			closeErr,
		)
	}
	if err != nil {
		return nil, errors.Join(
			err,
			runtime.Close(context.WithoutCancel(ctx)),
		)
	}
	return runtime, nil
}

// GenesisStakingEmpty reports whether a known genesis contains no initial staking.
func GenesisStakingEmpty(genesis *shelley.ShelleyGenesis) bool {
	if genesis == nil {
		return false
	}
	if len(genesis.Staking.Pools) != 0 || len(genesis.Staking.Stake) != 0 {
		return false
	}
	extra := genesis.ExtraConfig
	return extra == nil ||
		(len(extra.StakePools.Data) == 0 && len(extra.StakeCredentials.Data) == 0)
}

// ResolveGenesisStakingEmpty certifies staking only from a loaded config or an
// explicitly selected config path or network. An unspecified genesis remains
// unverified; a selected config that cannot be loaded returns an error.
func ResolveGenesisStakingEmpty(
	config *cardano.CardanoNodeConfig,
	path, network string,
	networkMagic uint32,
) (bool, error) {
	if config == nil {
		if path == "" && network == "" && networkMagic != 0 {
			if known, ok := ouroboros.NetworkByNetworkMagic(networkMagic); ok {
				network = known.Name
			}
		}
		if path == "" && network == "" {
			return false, nil
		}
		if path == "" {
			path = cardano.EmbeddedConfigPath(network)
		}
		var err error
		config, err = cardano.LoadCardanoNodeConfigWithFallback(
			path, network, cardano.EmbeddedConfigFS,
		)
		if err != nil {
			return false, err
		}
	}
	return GenesisStakingEmpty(config.ShelleyGenesis()), nil
}
