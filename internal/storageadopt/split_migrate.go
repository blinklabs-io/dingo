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

// Package storageadopt migrates existing API stores to the split layout.
package storageadopt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/settingsresolve"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/plugin"
)

const (
	migrateWorkPrefix = ".indexer-migrate-"
	// migrateSpaceMargin is free space required beyond the size of the copy.
	migrateSpaceMargin = 64 << 20
)

// Steps a migration reports to its test hook, in order.
const (
	migrateStepCopied  = "copied"
	migrateStepRenamed = "renamed"
	migrateStepLatched = "latched"
)

// ArchiveStore wraps a blob store with the archive fallback cfg configures.
// Package bark sets it; storageadopt cannot import bark, which imports it.
var ArchiveStore func(
	cfg *config.Config,
	upstream blob.BlobStore,
) (blob.BlobStore, error)

// MigrateToSplit adopts an API database in place. The caller must recover
// interrupted restores before adopting active storage.
func MigrateToSplit(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	m := &splitMigration{cfg: cfg, logger: logger}
	return m.run(ctx)
}

// SupportsAPIAdoption reports whether the metadata provider can adopt a
// released API database.
func SupportsAPIAdoption(provider string) bool {
	switch provider {
	case "sqlite", "postgres", "mysql":
		return true
	default:
		return false
	}
}

// AdoptStagedAPI migrates restored API storage before node activation. For
// server providers the caller must prepare an empty derived core database
// and retain its rollback backup.
func AdoptStagedAPI(
	ctx context.Context,
	cfg config.Config,
	logger *slog.Logger,
	genesis *cardano.CardanoNodeConfig,
	stagingDir string,
) error {
	cfg.DatabasePath = stagingDir
	m := &splitMigration{cfg: &cfg, logger: logger, genesis: genesis}
	switch cfg.Plugins.Storage.Metadata.Provider {
	case "sqlite":
		return m.migrate(ctx)
	case "postgres", "mysql":
		return m.migrateServer(ctx)
	default:
		return fmt.Errorf("cannot adopt restored API metadata provider %q", cfg.Plugins.Storage.Metadata.Provider)
	}
}

type splitMigration struct {
	hostFactory func() (*plugin.Host, error)
	cfg         *config.Config
	logger      *slog.Logger
	genesis     *cardano.CardanoNodeConfig
	// afterStep, when set, runs after each step and fails the migration
	// there. Tests use it to stop at the boundary a crash can fall on.
	afterStep func(step string) error
	// availableBytes reports free space on the volume of a path; nil uses
	// the platform's.
	availableBytes func(path string) (uint64, error)
}

func (m *splitMigration) newHost() (*plugin.Host, error) {
	if m.hostFactory != nil {
		return m.hostFactory()
	}
	return internalplugins.NewHost()
}

func (m *splitMigration) step(name string) error {
	if m.afterStep == nil {
		return nil
	}
	return m.afterStep(name)
}

func (m *splitMigration) selections() internalplugins.StorageSelections {
	return internalplugins.ConfiguredStorageSelections(m.cfg)
}

func (m *splitMigration) databaseConfig(
	storageMode, layout string,
) *database.Config {
	cfg := m.cfg
	return &database.Config{
		// Adoption runs before the ledger state moves out of the metadata
		// store, so it opens the v0.94.2 database as that move's source.
		Role:           database.RoleReleased,
		DataDir:        cfg.DatabasePath,
		Logger:         m.logger,
		StorageMode:    storageMode,
		IndexerLayout:  layout,
		Network:        cfg.Network,
		NetworkMagic:   cfg.NetworkMagic,
		StartEra:       string(cfg.StartEra),
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
			m.genesis, cfg.CardanoConfig, cfg.Network,
		),
	}
}

func (m *splitMigration) dependencies(
	storageMode string,
) (internalplugins.StorageDependencies, error) {
	genesisStakingEmpty, err := internalplugins.ResolveGenesisStakingEmpty(
		m.genesis, m.cfg.CardanoConfig, m.cfg.Network, m.cfg.NetworkMagic,
	)
	if err != nil {
		return internalplugins.StorageDependencies{}, fmt.Errorf(
			"loading cardano node config: %w", err,
		)
	}
	return internalplugins.StorageDependencies{
		GenesisStakingEmpty: genesisStakingEmpty,
		DataDir:             m.cfg.DatabasePath,
		RunMode:             string(m.cfg.RunMode),
		StorageMode:         storageMode,
		Logger:              m.logger,
		TracingEnabled:      m.cfg.Tracing,
	}, nil
}

func (m *splitMigration) run(ctx context.Context) error {
	cfg := m.cfg
	if cfg.Plugins.Storage.Metadata.Provider == "postgres" ||
		cfg.Plugins.Storage.Metadata.Provider == "mysql" {
		return m.migrateServer(ctx)
	}
	if !cfg.SplitIndexer() ||
		cfg.Plugins.Storage.Metadata.Provider != "sqlite" {
		return nil
	}
	if _, err := os.Stat(
		filepath.Join(cfg.DatabasePath, "metadata.sqlite"),
	); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	// A stop after the rename leaves the emptied work directory behind, so
	// this is not limited to the copying path.
	if err := m.removeStaleWork(); err != nil {
		return err
	}
	// The read opens the store on its own short-lived context.
	gates, ok, err := settingsresolve.ReadPersistedGateValues( //nolint:contextcheck
		cfg,
	)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// A latched layout with the mode still API is a stop between the two
	// latches; migrate finds the IndexerDB and finishes the mode change.
	if gates["storage_mode"] != "api" {
		return nil
	}
	if network := gates["network"]; network != cfg.Network {
		return fmt.Errorf(
			"the database is for network %q, the configuration for %q",
			network, cfg.Network,
		)
	}
	if magic := gates["network_magic"]; magic != "" && cfg.NetworkMagic != 0 &&
		magic != strconv.FormatUint(uint64(cfg.NetworkMagic), 10) {
		return fmt.Errorf(
			"the database has network magic %s, the configuration %d",
			magic, cfg.NetworkMagic,
		)
	}
	return m.migrate(ctx)
}

func (m *splitMigration) migrate(ctx context.Context) (err error) {
	cfg := m.cfg
	selections := m.selections()
	deps, err := m.dependencies("api")
	if err != nil {
		return err
	}
	_, indexerDir, err := internalplugins.IndexerMetadataSelection(
		selections, cfg.DatabasePath,
	)
	if err != nil {
		return err
	}
	source, err := internalplugins.OpenDatabase(
		ctx, m.databaseConfig("api", ""), selections, deps,
	)
	if err != nil {
		return fmt.Errorf(
			"open the API-mode database for migration to the split "+
				"layout: %w", err,
		)
	}
	sourceOpen := true
	closeSource := func() error {
		if !sourceOpen {
			return nil
		}
		sourceOpen = false
		return source.Close(context.WithoutCancel(ctx))
	}
	defer func() { err = errors.Join(err, closeSource()) }()
	db := source.Database

	if err := refuseInterruptedBackfill(db.Metadata()); err != nil {
		return err
	}
	tip, err := db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read ledger tip: %w", err)
	}
	if len(tip.Point.Hash) == 0 {
		return nil
	}
	block, err := database.BlockByHash(ctx, db, tip.Point.Hash)
	if err != nil {
		return fmt.Errorf("load the ledger tip block: %w", err)
	}
	ledgerTip := indexerdb.Tip{
		Slot: block.Slot, BlockNumber: block.Number, Hash: block.Hash,
	}
	var epoch uint64
	if epochs, epochErr := db.GetEpochs(nil); epochErr == nil &&
		len(epochs) > 0 {
		epoch = epochs[len(epochs)-1].EpochId
	}

	if entries, readErr := os.ReadDir(indexerDir); readErr == nil &&
		len(entries) != 0 {
		// The rename happened and the consensus store's latches did not.
		latched, inspectErr := m.indexerCopyIsLatched(ctx, db, deps)
		if inspectErr != nil {
			return inspectErr
		}
		if !latched {
			return fmt.Errorf(
				"%s holds an IndexerDB that is not a completed migration "+
					"of this database: move it away to migrate again",
				indexerDir,
			)
		}
		m.logger.Info(
			"completing the interrupted migration to the split layout",
			"component", "indexer", "indexer_dir", indexerDir,
		)
		return m.latchConsensus(ctx, db, closeSource)
	} else if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}

	return m.copyAndPublish(
		ctx, db, closeSource, selections, deps, indexerDir, ledgerTip, epoch,
	)
}

func (m *splitMigration) copyAndPublish(
	ctx context.Context,
	db *database.Database,
	closeSource func() error,
	selections internalplugins.StorageSelections,
	deps internalplugins.StorageDependencies,
	indexerDir string,
	ledgerTip indexerdb.Tip,
	epoch uint64,
) error {
	cfg := m.cfg
	backuper, ok := db.Metadata().(metadata.Backuper)
	if !ok {
		return errors.New("the metadata store cannot be copied")
	}
	if err := m.checkSpace(); err != nil {
		return err
	}
	tmpRoot, err := os.MkdirTemp(cfg.DatabasePath, migrateWorkPrefix)
	if err != nil {
		return fmt.Errorf("create migration work directory: %w", err)
	}
	defer os.RemoveAll(tmpRoot) //nolint:errcheck // best-effort cleanup
	tmpIndexer := filepath.Join(tmpRoot, internalplugins.IndexerDirName)
	if err := os.MkdirAll(tmpIndexer, 0o700); err != nil {
		return err
	}
	m.logger.Info(
		"migrating the API-mode database to the split layout: copying "+
			"the metadata store",
		"component", "indexer",
		"slot", ledgerTip.Slot, "block", ledgerTip.BlockNumber,
	)
	started := time.Now()
	if err := backuper.BackupTo(
		ctx, filepath.Join(tmpIndexer, "metadata.sqlite"),
	); err != nil {
		return fmt.Errorf("copy the metadata store: %w", err)
	}
	m.logger.Info(
		"copied the metadata store",
		"component", "indexer", "elapsed", time.Since(started).String(),
	)
	if err := m.prepareCopy(
		ctx, db, selections, deps, tmpRoot, ledgerTip, epoch,
	); err != nil {
		return err
	}
	if err := m.step(migrateStepCopied); err != nil {
		return err
	}
	// The copy is complete and carries its tip and latch; the rename
	// publishes it.
	if err := os.Rename(tmpIndexer, indexerDir); err != nil {
		return fmt.Errorf("move the IndexerDB into place: %w", err)
	}
	if err := m.step(migrateStepRenamed); err != nil {
		return err
	}
	return m.latchConsensus(ctx, db, closeSource)
}

// latchConsensus latches the split layout in the consensus store, closes it
// and reopens it as a core store, which latches core mode and shows the
// result opens.
func (m *splitMigration) latchConsensus(
	ctx context.Context,
	db *database.Database,
	closeSource func() error,
) error {
	if err := db.SetNodeSettingsGates(
		ctx,
		nodesettings.Values{"indexer_layout": config.IndexerLayoutSplit},
	); err != nil {
		return fmt.Errorf("latch the split layout: %w", err)
	}
	if err := closeSource(); err != nil {
		return fmt.Errorf("close the database: %w", err)
	}
	if err := m.step(migrateStepLatched); err != nil {
		return err
	}
	coreDeps, err := m.dependencies("core")
	if err != nil {
		return err
	}
	reopened, err := internalplugins.OpenDatabase(
		ctx,
		m.databaseConfig("core", config.IndexerLayoutSplit),
		m.selections(),
		coreDeps,
	)
	if err != nil {
		return fmt.Errorf("open the consensus store as core: %w", err)
	}
	if err := reopened.Close(context.WithoutCancel(ctx)); err != nil {
		return err
	}
	m.logger.Info(
		"migrated the database to the split layout",
		"component", "indexer",
	)
	return nil
}

// errInterruptedBackfill refuses an API store whose v0.94.2 Mithril bootstrap
// stopped while it was still writing API history from the imported blocks.
// The store becomes the IndexerDB at the ledger tip, which would declare
// complete a history that stops at the backfill's last block. Every provider
// refuses such a store before it migrates the schema
// (metadata.ErrInterruptedBackfill), so one that reaches this check already
// has this version's schema, which v0.94.2 no longer opens.
var errInterruptedBackfill = errors.New(
	"the API database holds an interrupted Mithril metadata backfill " +
		"(sync_status is \"backfill\"), so its API history is incomplete " +
		"and it cannot become the IndexerDB. Its schema is already this " +
		"version's, so v0.94.2 cannot finish the backfill on it: restore " +
		"the database as v0.94.2 left it, let that release finish the " +
		"backfill, then start this version",
)

func refuseInterruptedBackfill(store metadata.MetadataStore) error {
	status, err := store.GetSyncState("sync_status", nil)
	if err != nil {
		return fmt.Errorf("read sync status: %w", err)
	}
	if status == "backfill" {
		return errInterruptedBackfill
	}
	return nil
}

// removeStaleWork deletes the work directories an interrupted copy left.
func (m *splitMigration) removeStaleWork() error {
	entries, err := os.ReadDir(m.cfg.DatabasePath)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), migrateWorkPrefix) {
			continue
		}
		m.logger.Info(
			"removing the work directory of an interrupted migration",
			"component", "indexer", "dir", e.Name(),
		)
		if err := os.RemoveAll(
			filepath.Join(m.cfg.DatabasePath, e.Name()),
		); err != nil {
			return err
		}
	}
	return nil
}

// checkSpace refuses when the volume cannot hold a second copy of the
// metadata store.
func (m *splitMigration) checkSpace() error {
	var size uint64
	for _, name := range []string{
		"metadata.sqlite", "metadata.sqlite-wal",
	} {
		if info, err := os.Stat(
			filepath.Join(m.cfg.DatabasePath, name),
		); err == nil {
			size += uint64(info.Size()) // #nosec G115 -- file size
		}
	}
	need := size + migrateSpaceMargin
	available := m.availableBytes
	if available == nil {
		available = utxoset.AvailableDiskBytes
	}
	avail, err := available(m.cfg.DatabasePath)
	if err != nil {
		return fmt.Errorf("check free disk space: %w", err)
	}
	if avail < need {
		return fmt.Errorf(
			"not enough free disk space to migrate the database to the "+
				"split layout: the copy of the %d-byte metadata store "+
				"needs %d bytes and %d are available in %s",
			size, need, avail, m.cfg.DatabasePath,
		)
	}
	return nil
}

// prepareCopy opens the copy under tmpRoot as an IndexerDB over the source's
// blob store, records the ledger tip as its tip and checkpoint, and latches
// the split layout in it.
func (m *splitMigration) prepareCopy(
	ctx context.Context,
	source *database.Database,
	selections internalplugins.StorageSelections,
	deps internalplugins.StorageDependencies,
	tmpRoot string,
	tip indexerdb.Tip,
	epoch uint64,
) (err error) {
	host, err := m.newHost()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, host.Stop(context.WithoutCancel(ctx)))
	}()
	copyDeps := deps
	copyDeps.DataDir = tmpRoot
	idx, err := internalplugins.OpenIndexerDatabase(
		ctx, host, source, m.databaseConfig("api", ""), selections, copyDeps,
	)
	if err != nil {
		return fmt.Errorf("open the copied store: %w", err)
	}
	defer func() { err = errors.Join(err, idx.Close()) }()
	store, err := indexerdb.NewStore(
		idx, idx, m.cfg.Indexer.MaxReadyLagBlocks,
	)
	if err != nil {
		return err
	}
	if err := m.copyOffsetEntries(ctx, source, idx); err != nil {
		return err
	}
	// The released store holds copies of the payloads it indexed; the
	// IndexerDB reads them from the blocks instead.
	if m.cfg.BarkBaseUrl != "" {
		// A block that expired locally is fetched from the archive to
		// compute its offsets, and reads of those rows proxy the same fetch.
		if ArchiveStore == nil {
			return errors.New("an archive is configured but no archive store is linked")
		}
		archived, err := ArchiveStore(m.cfg, idx.Blob())
		if err != nil {
			return fmt.Errorf("create archive blob store: %w", err)
		}
		idx.SetBlobStore(archived)
	}
	m.logger.Info(
		"converting the copied payload columns to block offsets",
		"component", "indexer",
	)
	if err := idx.MigratePayloadOffsets(ctx); err != nil {
		return fmt.Errorf("convert payload columns to block offsets: %w", err)
	}
	// Snapshots whose stake inputs the ledger is still writing are the
	// writer's to copy once they finish; the copy holds them only in part.
	pending, err := source.GetSyncState(
		models.RewardStakeInputsPendingKey, nil,
	)
	if err != nil {
		return fmt.Errorf("read pending stake inputs: %w", err)
	}
	checkpoints, err := m.checkpointsBelow(ctx, source, tip)
	if err != nil {
		return err
	}
	txn := idx.Transaction(ctx, true)
	if err := txn.Do(func(txn *database.Txn) error {
		for _, cp := range checkpoints {
			if err := store.AddCheckpoint(cp, txn); err != nil {
				return err
			}
		}
		if pending != "" {
			if err := idx.Metadata().SetSyncState(
				indexer.DeferredStakeInputsKey, pending, txn.Metadata(),
			); err != nil {
				return err
			}
		}
		if err := store.AddCheckpoint(tip, txn); err != nil {
			return err
		}
		return store.SetTip(tip, txn)
	}); err != nil {
		return fmt.Errorf("record the IndexerDB tip: %w", err)
	}
	if err := idx.Metadata().SetNodeSettingsGates(
		ctx,
		nodesettings.Values{"indexer_layout": config.IndexerLayoutSplit},
		epoch, tip.Slot,
	); err != nil {
		return fmt.Errorf("latch the split layout in the copy: %w", err)
	}
	return nil
}

// offsetCopyBatch is how many entries one blob transaction of the offset copy
// stages.
const offsetCopyBatch = 1000

// copyOffsetEntries gives the IndexerDB the ledger's UTxO and transaction
// entries under its own key prefix. The IndexerDB reads an output's CBOR from
// its own entries, and a block can only rebuild the ones it produced: the
// entries of outputs no block produced, such as genesis outputs, exist only in
// the ledger's namespace. The entries are keyed by identity, so copying one
// that is already there rewrites the same value and a repeated copy is safe.
func (m *splitMigration) copyOffsetEntries(
	ctx context.Context,
	source, idx *database.Database,
) error {
	src := source.Blob()
	if src == nil || idx.Blob() == nil {
		return errors.New(
			"the databases have no blob store to copy offsets from",
		)
	}
	read := src.NewTransaction(false)
	defer read.Rollback() //nolint:errcheck // read-only
	write := idx.Blob().NewTransaction(true)
	staged := 0
	commit := func() error {
		if staged == 0 {
			return nil
		}
		if err := write.Commit(); err != nil {
			return fmt.Errorf("commit the copied offset entries: %w", err)
		}
		write = idx.Blob().NewTransaction(true)
		staged = 0
		return nil
	}
	defer func() { _ = write.Rollback() }()
	copied := 0
	for _, kind := range []struct {
		prefix string
		size   int
		set    func(txn types.Txn, key, val []byte) error
	}{
		{
			types.UtxoBlobKeyPrefix, len(types.UtxoBlobKey(make([]byte, 32), 0)),
			func(txn types.Txn, key, val []byte) error {
				return idx.Blob().SetUtxo(
					txn, key[1:33],
					binary.BigEndian.Uint32(key[33:]), val,
				)
			},
		},
		{
			types.TxBlobKeyPrefix, len(types.TxBlobKey(make([]byte, 32))),
			func(txn types.Txn, key, val []byte) error {
				return idx.Blob().SetTx(txn, key[1:], val)
			},
		},
	} {
		it := src.NewIterator(read, types.BlobIteratorOptions{
			Prefix: []byte(kind.prefix),
		})
		for it.Rewind(); it.ValidForPrefix([]byte(kind.prefix)); it.Next() {
			if err := ctx.Err(); err != nil {
				it.Close()
				return err
			}
			item := it.Item()
			if item == nil {
				it.Close()
				return errors.New(
					"read an offset entry: the iterator holds no item",
				)
			}
			key := item.Key()
			if len(key) != kind.size {
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				it.Close()
				return fmt.Errorf("read an offset entry: %w", err)
			}
			if err := kind.set(write, key, val); err != nil {
				it.Close()
				return fmt.Errorf("copy an offset entry: %w", err)
			}
			staged++
			copied++
			if staged >= offsetCopyBatch {
				if err := commit(); err != nil {
					it.Close()
					return err
				}
			}
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return fmt.Errorf("walk the offset entries: %w", err)
		}
	}
	if err := commit(); err != nil {
		return err
	}
	m.logger.Info(
		"copied the ledger's offset entries for the IndexerDB",
		"component", "indexer", "entries", copied,
	)
	return nil
}

// migrationCheckpointOffsets are the block distances below the ledger tip at
// which a migrated IndexerDB records checkpoints. 432 and 2160 are the
// security parameters of the public networks, so one of them is the immutable
// tip; the rest reach back toward the retention limit. A store migrated with
// only its tip as a checkpoint has no ancestor once the ledger drops the
// blocks of a fork deeper than that tip.
var migrationCheckpointOffsets = []uint64{
	432, 2160, 5000, 10_000, indexerdb.CheckpointRetainBlocks,
}

// checkpointsBelow returns the canonical blocks at the migration offsets
// below tip, skipping offsets beyond the start of the chain.
func (m *splitMigration) checkpointsBelow(
	ctx context.Context,
	source *database.Database,
	tip indexerdb.Tip,
) ([]indexerdb.Tip, error) {
	out := make([]indexerdb.Tip, 0, len(migrationCheckpointOffsets))
	for _, offset := range migrationCheckpointOffsets {
		if offset > tip.BlockNumber {
			continue
		}
		block, err := database.BlockByNumber(
			ctx, source, tip.BlockNumber-offset,
		)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				continue
			}
			return nil, fmt.Errorf("load checkpoint block: %w", err)
		}
		out = append(out, indexerdb.Tip{
			Slot: block.Slot, BlockNumber: block.Number, Hash: block.Hash,
		})
	}
	return out, nil
}

// indexerCopyIsLatched opens the published IndexerDB and reports whether it
// carries the split latch and a tip, which only a finished copy does.
func (m *splitMigration) indexerCopyIsLatched(
	ctx context.Context,
	source *database.Database,
	deps internalplugins.StorageDependencies,
) (latched bool, err error) {
	host, err := m.newHost()
	if err != nil {
		return false, err
	}
	defer func() {
		err = errors.Join(err, host.Stop(context.WithoutCancel(ctx)))
	}()
	idx, err := internalplugins.OpenIndexerDatabase(
		ctx, host, source, m.databaseConfig("api", ""), m.selections(), deps,
	)
	if err != nil {
		return false, fmt.Errorf("open the existing IndexerDB: %w", err)
	}
	defer func() { err = errors.Join(err, idx.Close()) }()
	gates, err := idx.Metadata().GetNodeSettingsGates(ctx)
	if err != nil {
		return false, err
	}
	store, err := indexerdb.NewStore(
		idx, idx, m.cfg.Indexer.MaxReadyLagBlocks,
	)
	if err != nil {
		return false, err
	}
	tip, err := store.Tip(nil)
	if err != nil {
		return false, err
	}
	return gates["indexer_layout"] == config.IndexerLayoutSplit &&
		!tip.IsZero(), nil
}
