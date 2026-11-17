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

package dblifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
)

// IndexerRoller rolls an IndexerDB back to a block of the ledger database.
type IndexerRoller interface {
	RollbackTo(ctx context.Context, target indexerdb.Tip) error
}

// IndexerRollbackFactory builds the roller over an opened IndexerDB and the
// ledger database it reads blocks from. It is a seam because the writer that
// owns the rollback lives in a package that imports this one.
type IndexerRollbackFactory func(
	consensus, indexer *database.Database,
) (IndexerRoller, error)

// SetIndexerRollbackFactory sets what an offline truncate on the split layout
// rolls the IndexerDB back with. Without it such a truncate is refused.
func (s *Service) SetIndexerRollbackFactory(f IndexerRollbackFactory) {
	s.rollbackFactory = f
}

func (s *Service) databaseConfig() *database.Config {
	return &database.Config{
		DataDir:        s.cfg.DatabasePath,
		Logger:         s.logger,
		StorageMode:    s.cfg.ConsensusStorageMode(),
		Network:        s.cfg.Network,
		NetworkMagic:   s.cfg.NetworkMagic,
		StartEra:       string(s.cfg.StartEra),
		BlobPlugin:     s.cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: s.cfg.Plugins.Storage.Metadata.Provider,
		AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
			nil, s.cfg.CardanoConfig, s.cfg.Network,
		),
	}
}

func (s *Service) storageSelections() internalplugins.StorageSelections {
	return internalplugins.StorageSelections{
		Blob:                  s.cfg.Plugins.Storage.Blob,
		Metadata:              s.cfg.Plugins.Storage.Metadata,
		IndexerMetadataConfig: s.cfg.Indexer.Metadata,
	}
}

func (s *Service) storageDependencies() (
	internalplugins.StorageDependencies,
	error,
) {
	genesisStakingEmpty, err := internalplugins.ResolveGenesisStakingEmpty(
		nil, s.cfg.CardanoConfig, s.cfg.Network, s.cfg.NetworkMagic,
	)
	if err != nil {
		return internalplugins.StorageDependencies{}, fmt.Errorf(
			"loading cardano node config: %w", err,
		)
	}
	return internalplugins.StorageDependencies{
		GenesisStakingEmpty: genesisStakingEmpty,
		DataDir:             s.cfg.DatabasePath,
		StorageMode:         s.cfg.ConsensusStorageMode(),
		MaxConnections:      s.cfg.DatabaseWorkers,
		Logger:              s.logger,
		TracingEnabled:      s.cfg.Tracing,
	}, nil
}

// checkSplitSupported refuses, before anything changes, the snapshot and
// restore shapes the IndexerDB payload does not cover.
func (s *Service) checkSplitSupported() error {
	return CheckIndexerPayloadSupported(
		s.cfg.Plugins.Storage.Metadata.Provider,
		len(s.cfg.Indexer.Metadata) != 0,
		s.cfg.DatabaseLifecycle.SnapshotTrustKeyFile,
	)
}

// withIndexer opens the IndexerDB over ledger on a plugin host of its own and
// runs fn with it. The IndexerDB closes, then the host, before it returns.
func (s *Service) withIndexer(
	ctx context.Context,
	ledger *database.Database,
	fn func(indexer *database.Database) error,
) (err error) {
	host, err := internalplugins.NewHost()
	if err != nil {
		return fmt.Errorf("build storage plugin host for indexer: %w", err)
	}
	defer func() {
		err = errors.Join(err, host.Stop(context.WithoutCancel(ctx)))
	}()
	deps, err := s.storageDependencies()
	if err != nil {
		return err
	}
	dbConfig := s.databaseConfig()
	dbConfig.IndexerLayout = config.IndexerLayoutSplit
	idx, err := internalplugins.OpenIndexerDatabase(
		ctx, host, ledger, dbConfig, s.storageSelections(), deps,
	)
	if err != nil {
		return fmt.Errorf("open the IndexerDB: %w", err)
	}
	defer func() { err = errors.Join(err, idx.Close()) }()
	return fn(idx)
}

// indexerStoreExists reports whether a default-location SQLite IndexerDB has
// been created. Opening one that does not exist would create it empty.
func (s *Service) indexerStoreExists() bool {
	_, err := os.Stat(filepath.Join(
		s.cfg.DatabasePath, internalplugins.IndexerDirName, "metadata.sqlite",
	))
	return err == nil
}

// captureIndexer backs the IndexerDB up before the ledger is snapshotted and
// refuses one whose tip is past the ledger's, which the restore would refuse.
func (s *Service) captureIndexer(
	ctx context.Context,
	ledger *database.Database,
) (staged string, payload IndexerPayload, cleanup func(), err error) {
	cleanup = func() {}
	if !s.indexerStoreExists() {
		return "", payload, cleanup, errors.New(
			"the data directory holds no IndexerDB to snapshot: start the " +
				"node on the split layout first",
		)
	}
	err = s.withIndexer(ctx, ledger, func(idx *database.Database) error {
		store, err := indexerdb.NewStore(
			idx, idx, s.cfg.Indexer.MaxReadyLagBlocks,
		)
		if err != nil {
			return err
		}
		ledgerTip, err := ledger.GetTip(nil)
		if err != nil {
			return fmt.Errorf("read ledger tip: %w", err)
		}
		tip, err := store.Tip(nil)
		if err != nil {
			return fmt.Errorf("read indexer tip: %w", err)
		}
		if tip.Slot > ledgerTip.Point.Slot {
			return fmt.Errorf(
				"the IndexerDB is at slot %d, past the ledger's slot %d: "+
					"start the node so the writer heals it first",
				tip.Slot, ledgerTip.Point.Slot,
			)
		}
		staged, payload, cleanup, err = CaptureIndexerPayload(
			ctx, idx, store, s.cfg.DatabasePath,
		)
		return err
	})
	if err != nil {
		cleanup()
		return "", IndexerPayload{}, func() {}, err
	}
	return staged, payload, cleanup, nil
}

// rollbackIndexer rolls the IndexerDB back to target while ledger still
// stores the blocks being removed. A split data directory that never created
// an IndexerDB has nothing to roll back.
func (s *Service) rollbackIndexer(
	ctx context.Context,
	ledger *database.Database,
	target models.Block,
	opts ...lifecycle.TruncateOption,
) error {
	// The ledger's refusals, including the live UTxO set's, run first: a
	// refused truncate must leave the IndexerDB where it is.
	if err := lifecycle.CheckTruncate(ctx, ledger, target, opts...); err != nil {
		return err
	}
	defaultSQLite := s.cfg.Plugins.Storage.Metadata.Provider == "sqlite" &&
		len(s.cfg.Indexer.Metadata) == 0
	if defaultSQLite && !s.indexerStoreExists() {
		return nil
	}
	return s.withIndexer(ctx, ledger, func(idx *database.Database) error {
		roller, err := s.rollbackFactory(ledger, idx)
		if err != nil {
			return fmt.Errorf("build indexer writer: %w", err)
		}
		if err := roller.RollbackTo(ctx, indexerdb.Tip{
			Slot:        target.Slot,
			BlockNumber: target.Number,
			Hash:        target.Hash,
		}); err != nil {
			return fmt.Errorf("roll back indexer: %w", err)
		}
		return nil
	})
}

// indexerRestore stages the snapshot's IndexerDB payload beside the data
// directory once the ledger manifest is known, and installs it in the
// ledger's staging directory after checking its tip against the restored
// ledger.
type indexerRestore struct {
	ctx     context.Context //nolint:containedctx // the restore's own context, kept for the staged hook
	service *Service
	payload IndexerPayload
	stage   string
}

func (r *indexerRestore) stageFor(
	snapshotDir string,
	manifest lifecycle.Manifest,
) error {
	stage, err := os.MkdirTemp(
		filepath.Dir(r.service.cfg.DatabasePath), ".dingo-indexer-restore-",
	)
	if err != nil {
		return fmt.Errorf("create IndexerDB restore directory: %w", err)
	}
	r.stage = stage
	return RestoreIndexerPayload(snapshotDir, stage, r.payload, manifest)
}

func (r *indexerRestore) cleanup() {
	if r.stage != "" {
		_ = os.RemoveAll(r.stage)
	}
}

// install moves the staged IndexerDB into the ledger's staging directory,
// which the restore then renames into place, so a data directory never exists
// holding a restored ledger without its IndexerDB.
func (r *indexerRestore) install(ledgerStagingDir string) error {
	deps, err := r.service.storageDependencies()
	if err != nil {
		return err
	}
	if err := ValidateIndexerPayloadTip(
		r.ctx, ledgerStagingDir, r.payload,
		r.service.databaseConfig(), r.service.storageSelections(), deps,
	); err != nil {
		return err
	}
	if err := os.Rename(
		filepath.Join(r.stage, internalplugins.IndexerDirName),
		filepath.Join(ledgerStagingDir, internalplugins.IndexerDirName),
	); err != nil {
		return fmt.Errorf("install the restored IndexerDB: %w", err)
	}
	return nil
}
