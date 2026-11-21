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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/plugin"
)

// IndexerMetadataBackupFileName holds a provider-native IndexerDB backup.
const IndexerMetadataBackupFileName = "indexer.metadata"

const legacyCoreBackupFileName = "legacy-core.metadata"

type indexerBackupSource struct {
	path string
	slot uint64
	hash string
}

// WithIndexerBackup includes an already captured IndexerDB backup in the
// authenticated manifest before the snapshot is published or mirrored.
func WithIndexerBackup(path string, slot uint64, hash string) ManifestOption {
	return func(cfg *manifestConfig) {
		cfg.indexerBackup = &indexerBackupSource{path: path, slot: slot, hash: hash}
	}
}

func captureIndexerBackup(ctx context.Context, root *os.Root, m *Manifest, opts []ManifestOption) error {
	var cfg manifestConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	source := cfg.indexerBackup
	if source == nil {
		return nil
	}
	if source.slot > m.TipSlot {
		return errors.New("IndexerDB backup tip is past the ledger snapshot")
	}
	in, err := os.Open(source.path)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := root.OpenFile(IndexerMetadataBackupFileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, hash), payloadContextReader{ctx: ctx, reader: in})
	if err := errors.Join(copyErr, out.Sync(), out.Close()); err != nil {
		return err
	}
	m.IndexerBytes = n
	m.IndexerSHA256 = hex.EncodeToString(hash.Sum(nil))
	m.IndexerTipSlot = source.slot
	m.IndexerTipHash = source.hash
	return m.validateIndexerPayload()
}

func indexerRestoreManifest(m Manifest) Manifest {
	m.StorageMode = "api"
	return m
}

func coreRestoreManifest(m Manifest) Manifest {
	m.StorageMode = "core"
	m.Gates = nodesettings.Values{"indexer_layout": "split"}
	return m
}

func resolveLegacyCore(
	ctx context.Context, host *plugin.Host, m Manifest, dir string, config map[string]any,
) (metadata.MetadataStore, error) {
	return plugin.Resolve[metadata.MetadataStore](
		ctx, host, plugin.CapabilityStorageMetadata, m.MetadataPlugin, config,
		metadata.ProviderDependencies{CoreNamespace: true, DataDir: dir, StorageMode: "core"},
	)
}

func backupLegacyCore(
	ctx context.Context, host *plugin.Host, m Manifest, dir string, config map[string]any,
	r *restoreRollback,
) (err error) {
	store, err := resolveLegacyCore(ctx, host, m, dir, config)
	if err != nil {
		return fmt.Errorf("open consensus metadata for restore backup: %w", err)
	}
	defer func() {
		err = errors.Join(err, host.StopCapability(context.WithoutCancel(ctx), plugin.CapabilityStorageMetadata))
	}()
	backuper, ok := store.(metadata.Backuper)
	if !ok {
		return errors.New("consensus metadata provider cannot retain a restore backup")
	}
	r.legacyCorePath = filepath.Join(r.dir, legacyCoreBackupFileName)
	if err := backuper.BackupTo(ctx, r.legacyCorePath); err != nil {
		return fmt.Errorf("back up original consensus metadata before restore: %w", err)
	}
	return nil
}

func resetLegacyCore(
	ctx context.Context, host *plugin.Host, m Manifest, dir string, config map[string]any,
) (err error) {
	store, err := resolveLegacyCore(ctx, host, m, dir, config)
	if err != nil {
		return fmt.Errorf("open consensus metadata for restore: %w", err)
	}
	defer func() {
		err = errors.Join(err, host.StopCapability(context.WithoutCancel(ctx), plugin.CapabilityStorageMetadata))
	}()
	resettable, ok := store.(metadata.Resettable)
	if !ok {
		return errors.New("consensus metadata provider cannot reset for restore")
	}
	if err := resettable.Reset(ctx); err != nil {
		return fmt.Errorf("clear consensus metadata before adoption: %w", err)
	}
	return nil
}

func checkLegacyCoreTarget(
	ctx context.Context, host *plugin.Host, m Manifest, dir string, config map[string]any,
) (err error) {
	if metadata.ResetOfPopulatedTargetAllowed(ctx) {
		return errors.New("deferred legacy restore requires offline target validation")
	}
	store, err := resolveLegacyCore(ctx, host, m, dir, config)
	if err != nil {
		return fmt.Errorf("open the derived consensus metadata store: %w", err)
	}
	defer func() {
		err = errors.Join(err, host.StopCapability(context.WithoutCancel(ctx), plugin.CapabilityStorageMetadata))
	}()
	check, ok := store.(interface{ CheckRestoreTarget(context.Context) error })
	if !ok {
		return errors.New("consensus metadata provider cannot check an offline restore target")
	}
	if err := check.CheckRestoreTarget(ctx); err != nil {
		return fmt.Errorf("check the derived consensus metadata store: %w", err)
	}
	return nil
}

func resolveIndexerMetadata(ctx context.Context, host *plugin.Host, m Manifest, dataDir string, config map[string]any) (metadata.MetadataStore, error) {
	return plugin.Resolve[metadata.MetadataStore](ctx, host, plugin.CapabilityStorageMetadata, m.MetadataPlugin, config, metadata.ProviderDependencies{DataDir: filepath.Join(dataDir, "indexer"), StorageMode: "api"})
}

func restoreIndexerMetadata(ctx context.Context, host *plugin.Host, m Manifest, path, dataDir string, config map[string]any) error {
	return restoreMetadataStore(ctx, host, indexerRestoreManifest(m), path, filepath.Join(dataDir, "indexer"), config, nil)
}

func validateIndexerMetadata(ctx context.Context, host *plugin.Host, m Manifest, core *database.Database, dataDir string, config map[string]any) error {
	if m.IndexerSHA256 == "" {
		return nil
	}
	if m.IndexerTipHash != "" {
		hash, err := hex.DecodeString(m.IndexerTipHash)
		if err != nil {
			return fmt.Errorf("decode IndexerDB backup tip: %w", err)
		}
		canonical, err := database.IsCanonicalBlock(ctx, core, hash)
		if err != nil {
			return fmt.Errorf("look up the IndexerDB payload tip: %w", err)
		}
		if !canonical {
			return fmt.Errorf("the snapshot's IndexerDB payload tip %s at slot %d is not on the ledger chain: take the snapshot again", m.IndexerTipHash, m.IndexerTipSlot)
		}
		block, err := database.BlockByHash(ctx, core, hash)
		if err != nil {
			return fmt.Errorf("look up the IndexerDB payload tip: %w", err)
		}
		if block.Slot != m.IndexerTipSlot {
			return fmt.Errorf("the IndexerDB payload records its tip at slot %d, but that block is at slot %d", m.IndexerTipSlot, block.Slot)
		}
	}
	store, err := resolveIndexerMetadata(ctx, host, m, dataDir, config)
	if err != nil {
		return err
	}
	cfg := *core.Config()
	cfg.Immutable = database.ImmutableConfig{}
	cfg.Role = database.RoleIndexer
	cfg.StorageMode = "api"
	cfg.DataDir = filepath.Join(dataDir, "indexer")
	idx, err := database.New(ctx, &cfg, database.Stores{Metadata: store, Blob: blob.NewIndexerStore(core.Blob())})
	if idx != nil {
		defer idx.Close()
	}
	if err != nil {
		return err
	}
	reader, err := indexerdb.NewStore(idx, idx, 0)
	if err != nil {
		return err
	}
	tip, err := reader.Tip(nil)
	if err != nil {
		return err
	}
	if tip.Slot != m.IndexerTipSlot || !strings.EqualFold(hex.EncodeToString(tip.Hash), m.IndexerTipHash) {
		return fmt.Errorf("IndexerDB backup tip does not match its manifest at slot %d", m.IndexerTipSlot)
	}
	return nil
}

func prepareIndexerRollback(ctx context.Context, host *plugin.Host, m Manifest, snapshotDir, dataDir string, config map[string]any, r *restoreRollback) error {
	if m.IndexerSHA256 == "" {
		return nil
	}
	store, err := resolveIndexerMetadata(ctx, host, m, dataDir, config)
	if err != nil {
		return err
	}
	defer host.StopCapability( //nolint:errcheck
		context.WithoutCancel(ctx),
		plugin.CapabilityStorageMetadata,
	)
	if validator, ok := store.(metadata.BackupValidator); ok {
		if err := validator.ValidateBackup(ctx, filepath.Join(snapshotDir, IndexerMetadataBackupFileName)); err != nil {
			return fmt.Errorf("validate IndexerDB backup: %w", err)
		}
	}
	destructive, ok := store.(interface{ HasDestructiveReset() bool })
	if !ok || !destructive.HasDestructiveReset() {
		return nil
	}
	if !metadata.ResetOfPopulatedTargetAllowed(ctx) {
		settings, err := store.GetNodeSettings(ctx)
		if err != nil {
			return err
		}
		if settings != nil {
			return errors.New("the IndexerDB target is populated; replacing it requires a live restore of the configured node")
		}
		return nil
	}
	backup, ok := store.(metadata.Backuper)
	if !ok {
		return errors.New("IndexerDB cannot retain a rollback backup")
	}
	r.indexerPath = filepath.Join(r.dir, IndexerMetadataBackupFileName)
	return backup.BackupTo(ctx, r.indexerPath)
}

func (m Manifest) validateIndexerPayload() error {
	if m.IndexerSHA256 == "" {
		if m.IndexerBytes != 0 || m.IndexerTipSlot != 0 || m.IndexerTipHash != "" {
			return errors.New("manifest has IndexerDB fields without its payload digest")
		}
		return nil
	}
	digest, err := hex.DecodeString(m.IndexerSHA256)
	if err != nil || len(digest) != sha256.Size || m.IndexerBytes <= 0 {
		return errors.New("manifest has an invalid IndexerDB payload size or digest")
	}
	hash, err := hex.DecodeString(m.IndexerTipHash)
	if err != nil || (len(hash) != 0 && len(hash) != sha256.Size) || (m.IndexerTipSlot != 0 && len(hash) == 0) {
		return errors.New("manifest has an invalid IndexerDB tip hash")
	}
	if m.IndexerTipSlot > m.TipSlot {
		return errors.New("manifest IndexerDB tip is past the ledger snapshot")
	}
	return nil
}
