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

package storageadopt

import (
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/plugin"
)

func (m *indexerMigration) migrateServer(ctx context.Context) (err error) {
	deps, err := m.dependencies("api")
	if err != nil {
		return err
	}
	sourceHost, err := m.newHost()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, sourceHost.Stop(context.WithoutCancel(ctx))) }()
	sourceMeta, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		sourceHost,
		plugin.CapabilityStorageMetadata,
		m.cfg.Plugins.Storage.Metadata.Provider,
		m.cfg.Plugins.Storage.Metadata.Config,
		metadata.ProviderDependencies{
			DataDir: deps.DataDir, StorageMode: "api", Logger: m.logger, GenesisStakingEmpty: deps.GenesisStakingEmpty,
		},
	)
	if err != nil {
		return fmt.Errorf("open the existing API database: %w", err)
	}
	settings, err := sourceMeta.GetNodeSettings(ctx)
	if err != nil {
		return fmt.Errorf("read the API database settings: %w", err)
	}
	if !m.cfg.HasIndexerDB() &&
		(settings == nil || settings.StorageMode == "core") {
		return nil
	}
	if settings != nil && settings.StorageMode != "api" {
		return fmt.Errorf(
			"the configured API database has storage mode %q, so it cannot be adopted as IndexerDB",
			settings.StorageMode,
		)
	}
	if err := refuseInterruptedBackfill(sourceMeta); err != nil {
		return err
	}
	m.cfg.ServerCoreNamespace = true
	destHost, err := m.newHost()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, destHost.Stop(context.WithoutCancel(ctx))) }()
	destMeta, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		destHost,
		plugin.CapabilityStorageMetadata,
		m.cfg.Plugins.Storage.Metadata.Provider,
		m.cfg.Plugins.Storage.Metadata.Config,
		metadata.ProviderDependencies{
			DataDir: deps.DataDir, StorageMode: "core", Logger: m.logger, CoreNamespace: true, GenesisStakingEmpty: deps.GenesisStakingEmpty,
		},
	)
	if err != nil {
		return fmt.Errorf("prepare the consensus namespace: %w", err)
	}
	sourceSQL, ok := sourceMeta.(*sqlstore.Store)
	if !ok {
		return errors.New(
			"the API provider does not expose SQL consensus migration",
		)
	}
	destSQL, ok := destMeta.(*sqlstore.Store)
	if !ok {
		return errors.New(
			"the consensus provider does not expose SQL consensus migration",
		)
	}
	namespace := destSQL.Namespace()
	complete, err := destMeta.GetSyncState(sqlstore.CoreCopyCompleteKey, nil)
	if err != nil {
		return fmt.Errorf("read consensus migration completion: %w", err)
	}
	if complete != "" && complete != "1" {
		return fmt.Errorf(
			"unsupported consensus migration completion marker %q",
			complete,
		)
	}
	adopted, err := sourceMeta.GetSyncState(sqlstore.CoreCopyAdoptedKey, nil)
	if err != nil {
		return fmt.Errorf("read the IndexerDB adoption marker: %w", err)
	}
	if complete == "1" {
		if adopted == "" {
			return m.markAdopted(sourceMeta)
		}
		return nil
	}
	if adopted != "" {
		return fmt.Errorf(
			"the %s database was adopted as the IndexerDB, but its consensus namespace %q holds no completed consensus state: it was dropped or emptied after adoption. Restore the namespace from a backup, or restore a snapshot; adopting again would rebuild the ledger from the IndexerDB's tip",
			m.cfg.Plugins.Storage.Metadata.Provider, namespace,
		)
	}
	// The source is the released database itself, not the consensus
	// namespace the configured selection names.
	sourceSelections := m.selections()
	sourceSelections.IndexerDB = false
	stores, err := internalplugins.ResolveStorage(
		ctx,
		sourceHost,
		sourceSelections,
		deps,
	)
	if err != nil {
		return err
	}
	stores.Metadata, err = internalplugins.ResolveMetadata(
		ctx, sourceHost, sourceSelections, deps,
	)
	if err != nil {
		return err
	}
	sourceCfg := m.databaseConfig("api", "")
	sourceCfg.Role = database.RoleIndexer
	source, err := database.New(ctx, sourceCfg, stores)
	if source != nil {
		defer func() { err = errors.Join(err, source.Close()) }()
	}
	if err != nil {
		return fmt.Errorf("open the API database for adoption: %w", err)
	}
	tip, err := source.GetTip(nil)
	if err != nil {
		return err
	}
	populated := len(tip.Point.Hash) != 0
	var epoch uint64
	if populated {
		epochs, err := source.GetEpochs(nil)
		if err != nil {
			return fmt.Errorf("read migration epoch: %w", err)
		}
		if len(epochs) != 0 {
			epoch = epochs[len(epochs)-1].EpochId
		}
		block, err := database.BlockByHash(ctx, source, tip.Point.Hash)
		if err != nil {
			return fmt.Errorf("load adoption tip block: %w", err)
		}
		selections := m.selections()
		selections.IndexerDB = true
		if err := m.prepareCopy(ctx, source, selections, deps, m.cfg.DatabasePath, indexerdb.Tip{Slot: block.Slot, BlockNumber: block.Number, Hash: block.Hash}, epoch); err != nil {
			return err
		}
	}
	// A stop from here on leaves a source that a release without the
	// layout refuses; the next start resumes the copy below.
	if err := sourceMeta.SetNodeSettingsGates(ctx, nodesettings.Values{"indexer_layout": "split"}, epoch, tip.Point.Slot); err != nil {
		return err
	}
	if err := m.step("source-latched"); err != nil {
		return err
	}
	m.logger.Info(
		"adopting the API database in place as the IndexerDB and copying consensus state into its consensus namespace",
		"component", "indexer",
		"provider", m.cfg.Plugins.Storage.Metadata.Provider,
		"namespace", namespace,
		"slot", tip.Point.Slot,
	)
	// The UTxO rows and the ledger-state families are copied with the rest:
	// the node reverts the consensus namespace below this tip before it moves
	// them into the LedgerDB, so that the set can roll back below the tip, and
	// the IndexerDB, which serves from these rows, must not be reverted.
	if err := sourceSQL.CopyConsensusStateTo(ctx, destSQL, func(table string) error {
		m.logger.Debug("copied consensus metadata batch", "component", "indexer", "table", table)
		return m.step("consensus-batch:" + table)
	}); err != nil {
		return err
	}
	if err := m.step("consensus-copied"); err != nil {
		return err
	}
	if err := destSQL.FinishConsensusSettingsCopy(ctx, epoch, tip.Point.Slot); err != nil {
		return err
	}
	if err := m.step("consensus-ready"); err != nil {
		return err
	}
	return m.markAdopted(sourceMeta)
}

// markAdopted records on the IndexerDB that its consensus namespace is
// complete, so a namespace lost later is refused rather than rebuilt.
func (m *indexerMigration) markAdopted(source metadata.MetadataStore) error {
	if err := source.SetSyncState(sqlstore.CoreCopyAdoptedKey, "1", nil); err != nil {
		return fmt.Errorf("record the IndexerDB adoption: %w", err)
	}
	return nil
}

// ErrServerAPINotAdopted reports a released PostgreSQL or MySQL API store that
// startup has not adopted, or not finished adopting, as the IndexerDB.
var ErrServerAPINotAdopted = errors.New(
	"the configured database holds a released API store that has not been adopted as the IndexerDB",
)

// CheckServerAdopted refuses to open an API node over a released
// PostgreSQL or MySQL API store whose adoption has not completed. Opening it
// would leave an empty consensus namespace beside an IndexerDB already at the
// released tip. The dingo command adopts it at startup, before any store
// opens; Node.Run does not. A store holds a released API store when it has a
// tip and either no IndexerDB layout latch, or a latch beside a consensus
// namespace an interrupted copy started. host registers the configured
// provider and is stopped by the caller.
func CheckServerAdopted(
	ctx context.Context,
	host *plugin.Host,
	selection plugin.Selection,
	dataDir string,
	genesisStakingEmpty bool,
) error {
	if selection.Provider != "postgres" && selection.Provider != "mysql" {
		return nil
	}
	resolve := func(core bool) (metadata.MetadataStore, error) {
		mode := "api"
		if core {
			mode = "core"
		}
		return plugin.Resolve[metadata.MetadataStore](
			ctx, host, plugin.CapabilityStorageMetadata,
			selection.Provider, selection.Config,
			metadata.ProviderDependencies{
				DataDir: dataDir, StorageMode: mode, CoreNamespace: core,
				GenesisStakingEmpty: genesisStakingEmpty,
			},
		)
	}
	store, err := resolve(false)
	if err != nil {
		return fmt.Errorf("open the configured %s database: %w", selection.Provider, err)
	}
	adopted, err := store.GetSyncState(sqlstore.CoreCopyAdoptedKey, nil)
	if err != nil {
		return fmt.Errorf("read the IndexerDB adoption marker: %w", err)
	}
	if adopted != "" {
		return nil
	}
	tip, err := store.GetTip(nil)
	if err != nil {
		return fmt.Errorf("read the configured %s database tip: %w", selection.Provider, err)
	}
	if len(tip.Point.Hash) == 0 {
		return nil
	}
	gates, err := store.GetNodeSettingsGates(ctx)
	if err != nil {
		return fmt.Errorf("read the configured %s database settings: %w", selection.Provider, err)
	}
	refuse := fmt.Errorf(
		"%w (%s tip at slot %d): start the dingo command once to adopt it, then start the node",
		ErrServerAPINotAdopted, selection.Provider, tip.Point.Slot,
	)
	if gates["indexer_layout"] != config.IndexerLayoutIndexerDB {
		return refuse
	}
	// An IndexerDB the node built itself has a consensus namespace with
	// no copy cursors; an interrupted adoption leaves them.
	if err := host.StopCapability(ctx, plugin.CapabilityStorageMetadata); err != nil {
		return err
	}
	core, err := resolve(true)
	if err != nil {
		return fmt.Errorf("open the consensus namespace: %w", err)
	}
	complete, err := core.GetSyncState(sqlstore.CoreCopyCompleteKey, nil)
	if err != nil {
		return fmt.Errorf("read consensus migration completion: %w", err)
	}
	if complete != "" {
		return nil
	}
	started, err := core.ListSyncStateKeysByPrefix(sqlstore.CoreCopyCursorPrefix, nil)
	if err != nil {
		return fmt.Errorf("read the consensus copy progress: %w", err)
	}
	if len(started) != 0 {
		return refuse
	}
	return nil
}
