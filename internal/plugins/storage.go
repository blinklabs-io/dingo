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
	"reflect"
	"sync"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/immutable/chunkfile"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
)

// StorageSelections identifies the providers and provider-owned
// configuration for the selectable storage capabilities. The ImmutableDB has
// one built-in provider and no selection.
type StorageSelections struct {
	Blob     plugin.Selection
	Metadata plugin.Selection
	// IndexerMetadataConfig holds provider settings that override the
	// metadata selection's for the split IndexerDB's own store.
	IndexerMetadataConfig map[string]any
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
}

// ResolveStorage starts the selected storage providers in dependency order.
// The supplied host owns their lifecycle; callers must stop it if resolution
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
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		selections.Metadata.Provider,
		selections.Metadata.Config,
		metadata.ProviderDependencies{
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
		return database.Stores{}, fmt.Errorf(
			"resolve metadata storage: %w",
			err,
		)
	}
	immutableStore, err := plugin.Resolve[immplugin.ImmutableDB](
		ctx,
		host,
		plugin.CapabilityStorageImmutable,
		chunkfile.ProviderName,
		nil,
		immplugin.ProviderDependencies{
			DataDir: deps.DataDir,
			Logger:  deps.Logger,
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
		Metadata:  metadataStore,
		Immutable: immutableStore,
	}, nil
}

// IndexerDirName is the directory under the data directory holding a SQLite
// IndexerDB.
const IndexerDirName = "indexer"

// IndexerMetadataSelection derives the IndexerDB's metadata provider config
// from the consensus selection and the indexer overrides. A SQLite store gets
// its own directory under the data directory. A client/server provider has no
// such default, so the overrides must name a different database: two stores
// in one schema would share tables.
func IndexerMetadataSelection(
	selections StorageSelections,
	dataDir string,
) (plugin.Selection, string, error) {
	provider := selections.Metadata.Provider
	cfg := maps.Clone(selections.Metadata.Config)
	if cfg == nil {
		cfg = map[string]any{}
	}
	maps.Copy(cfg, selections.IndexerMetadataConfig)
	indexerDir := dataDir
	if provider == "sqlite" {
		// The consensus store's directory override must not be inherited.
		if _, own := selections.IndexerMetadataConfig["dataDir"]; !own {
			delete(cfg, "dataDir")
			indexerDir = filepath.Join(dataDir, IndexerDirName)
		}
	} else if len(selections.IndexerMetadataConfig) == 0 ||
		reflect.DeepEqual(cfg, selections.Metadata.Config) {
		return plugin.Selection{}, "", fmt.Errorf(
			"indexer.metadata must configure a separate %s database for "+
				"the IndexerDB: the layout split would otherwise share "+
				"tables with the consensus store",
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
	selection, dataDir, err := IndexerMetadataSelection(selections, deps.DataDir)
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
// in API mode on host, over the consensus database's blob store. The blob
// view is read-only for blocks and keeps the IndexerDB's offsets under their
// own key prefix. The IndexerDB keeps no commit timestamp, and its metrics
// carry role="indexer" and an indexer_ name prefix so they register beside
// the consensus database's.
//
// The caller closes the returned database before the host stops, and before
// the consensus database closes the blob store it reads.
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
				prometheus.Labels{"role": IndexerMetricsRole}, deps.PromRegistry,
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
	db, err := database.New(ctx, &cfg, database.Stores{
		Blob:     blob.NewIndexerStore(consensus.Blob()),
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
	Database      *database.Database
	Host          *plugin.Host
	once          sync.Once
	err           error
	recoveryError error
}

// RecoveryError reports a recoverable error encountered while opening the
// database. OpenDatabase returns these errors on the owned runtime instead of
// as its error result, so a non-nil returned error always means there is no
// live runtime for the caller to close.
func (r *DatabaseRuntime) RecoveryError() error {
	if r == nil {
		return nil
	}
	return r.recoveryError
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
// been stopped. Recoverable database initialization errors are exposed through
// DatabaseRuntime.RecoveryError on the successfully owned runtime.
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
		if _, ok := errors.AsType[database.CommitTimestampError](err); ok {
			runtime.recoveryError = err
			return runtime, nil
		}
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
