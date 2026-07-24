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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// Restore populates targetDataDir (which must not already exist, or must
// be empty) from the snapshot at snapshotDir, then opens the result with
// database.New to confirm it passes the same startup consistency checks
// (checkNodeSettings, checkCommitTimestamp) any other dingo startup does,
// and that its tip matches what the manifest recorded. The opened database
// is closed again before returning — the caller is responsible for
// (re)opening it for real use.
//
// snapshotDir may instead be a cloud destination URI (s3://bucket/prefix
// or gcs://bucket/prefix; see RegisterCloudDestinationScheme) — Restore
// downloads it into a local temp directory first, then proceeds exactly
// as it would for a local snapshotDir. This is also how a snapshot
// created on one node can be restored onto another, since the two never
// need to share a filesystem.
//
// This is an offline operation: targetDataDir must not be concurrently
// held open by another *database.Database (e.g. a running node), since it
// restores the metadata store before starting it (metadata.Restorer) and
// the blob store immediately after starting it empty (blob.Restorer) —
// two-phase orchestration that a live store's own Start/Stop lifecycle
// cannot safely interleave with.
func Restore(
	ctx context.Context,
	snapshotDir string,
	targetDataDir string,
) (Manifest, error) {
	return RestoreValidated(ctx, snapshotDir, targetDataDir, nil)
}

// RestoreValidated is Restore, but — when validate is non-nil — calls
// validate(manifest) immediately after resolving the snapshot's manifest
// and before targetDataDir is touched in any way (not even the
// empty/absent check), returning validate's error without doing anything
// destructive if it fails.
//
// This is the hook an offline caller (see internal/dblifecycle.Service.
// Restore) uses to run Manifest.CheckCompatibility against its own
// configured plugins/network/storage mode before committing to a
// restore, without paying for a second cloud download to re-resolve the
// manifest it already checked: calling PeekManifest and then Restore
// separately would download a cloud snapshotDir twice.
func RestoreValidated(
	ctx context.Context,
	snapshotDir string,
	targetDataDir string,
	validate func(Manifest) error,
) (m Manifest, err error) {
	manifest, snapshotDir, cleanup, err := resolveManifest(ctx, snapshotDir)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return Manifest{}, err
	}
	if validate != nil {
		if err := validate(manifest); err != nil {
			return Manifest{}, err
		}
	}

	if err := requireEmptyOrAbsent(targetDataDir); err != nil {
		return Manifest{}, err
	}
	if err := os.MkdirAll(targetDataDir, 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create target data directory %q: %w", targetDataDir, err,
		)
	}
	// Best-effort cleanup on any failure below so a retry doesn't hit a
	// half-restored directory that looks superficially non-empty.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(targetDataDir)
		}
	}()

	if err := restoreMetadataStore(ctx, manifest, snapshotDir, targetDataDir); err != nil {
		return Manifest{}, err
	}
	if err := restoreBlobStore(ctx, manifest, snapshotDir, targetDataDir); err != nil {
		return Manifest{}, err
	}
	if err := validateRestoredDatabase(manifest, targetDataDir); err != nil {
		return Manifest{}, err
	}

	return manifest, nil
}

// PeekManifest resolves snapshotDir (a local path or a cloud destination
// URI — see Restore's doc comment) and reads its manifest, without
// restoring anything. Intended for a caller that needs to validate a
// snapshot's recorded plugins/network/storage mode (Manifest.
// CheckPluginMatch, or comparing StorageMode/Network directly) against a
// target's actual configuration before Restore ever touches
// targetDataDir — Restore's own validateRestoredDatabase only checks the
// manifest against itself, since it opens the restored copy using the
// manifest's own recorded plugins, not necessarily whatever the caller
// actually intends to run it with afterward.
//
// For a cloud snapshotDir, this tries FetchCloudManifest first — which
// fetches just the one manifest.json object via CloudManifestFetcher,
// without downloading the (possibly very large) blob/metadata backups
// alongside it — before falling back to the full download-based
// resolveManifest path below. FetchCloudManifest's ok=false covers two
// distinct cases resolveManifest already handles correctly on its own:
// snapshotDir isn't a recognized cloud URI at all (a plain local path),
// or it is one but that destination type doesn't implement
// CloudManifestFetcher — either way, falling through to resolveManifest
// is the right move, so its own error (if any) from the failed
// FetchCloudManifest attempt is deliberately discarded here rather than
// duplicating PeekManifest's cloud-vs-local branching a second time.
func PeekManifest(ctx context.Context, snapshotDir string) (Manifest, error) {
	if m, ok, err := FetchCloudManifest(ctx, snapshotDir); ok {
		return m, err
	}
	manifest, _, cleanup, err := resolveManifest(ctx, snapshotDir)
	if cleanup != nil {
		defer cleanup()
	}
	return manifest, err
}

// resolveManifest downloads snapshotDir first if it's a cloud destination
// URI, then reads its manifest. Returns the manifest, the resolved local
// snapshot directory to read backup files from (== snapshotDir itself
// when it was already local), and a cleanup func for any downloaded temp
// directory — nil when nothing was downloaded, so callers must nil-check
// before deferring it.
func resolveManifest(
	ctx context.Context,
	snapshotDir string,
) (manifest Manifest, resolvedDir string, cleanup func(), err error) {
	resolvedDir = snapshotDir
	if _, ok := recognizedCloudScheme(snapshotDir); ok {
		localSnapshotDir, cloudCleanup, downloadErr := downloadCloudSnapshot(ctx, snapshotDir)
		if downloadErr != nil {
			return Manifest{}, "", nil, downloadErr
		}
		resolvedDir = localSnapshotDir
		cleanup = cloudCleanup
	}
	manifest, err = ReadManifest(resolvedDir)
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		return Manifest{}, "", nil, err
	}
	return manifest, resolvedDir, cleanup, nil
}

// requireEmptyOrAbsent returns an error if dir exists and already contains
// entries; a non-existent or empty directory is fine.
func requireEmptyOrAbsent(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read target data directory %q: %w", dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("target data directory %q is not empty", dir)
	}
	return nil
}

// restoreMetadataStore restores the metadata store's on-disk file into
// targetDataDir before the plugin is started, per metadata.Restorer's
// contract.
func restoreMetadataStore(
	ctx context.Context,
	manifest Manifest,
	snapshotDir string,
	targetDataDir string,
) error {
	if err := plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		manifest.MetadataPlugin,
		"data-dir",
		targetDataDir,
	); err != nil {
		return fmt.Errorf("configure metadata plugin: %w", err)
	}
	metadataPlugin := plugin.GetPlugin(
		plugin.PluginTypeMetadata,
		manifest.MetadataPlugin,
	)
	if metadataPlugin == nil {
		return plugin.MissingPluginError(
			plugin.PluginTypeMetadata,
			manifest.MetadataPlugin,
		)
	}
	restorer, ok := metadataPlugin.(metadata.Restorer)
	if !ok {
		return fmt.Errorf(
			"metadata plugin %q does not support restore",
			manifest.MetadataPlugin,
		)
	}
	backupPath := filepath.Join(snapshotDir, MetadataBackupFileName)
	if err := restorer.RestoreFrom(ctx, backupPath); err != nil {
		return fmt.Errorf("restore metadata store: %w", err)
	}
	return nil
}

// restoreBlobStore starts the blob plugin against an empty targetDataDir
// and loads the backup into it, per blob.Restorer's contract.
func restoreBlobStore(
	ctx context.Context,
	manifest Manifest,
	snapshotDir string,
	targetDataDir string,
) error {
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		manifest.BlobPlugin,
		"data-dir",
		targetDataDir,
	); err != nil {
		return fmt.Errorf("configure blob plugin: %w", err)
	}
	blobPlugin := plugin.GetPlugin(plugin.PluginTypeBlob, manifest.BlobPlugin)
	if blobPlugin == nil {
		return plugin.MissingPluginError(
			plugin.PluginTypeBlob,
			manifest.BlobPlugin,
		)
	}
	restorer, ok := blobPlugin.(blob.Restorer)
	if !ok {
		return fmt.Errorf(
			"blob plugin %q does not support restore",
			manifest.BlobPlugin,
		)
	}
	if err := blobPlugin.Start(); err != nil {
		return fmt.Errorf("start blob plugin for restore: %w", err)
	}
	backupPath := filepath.Join(snapshotDir, BlobBackupFileName)
	backupFile, err := os.Open(backupPath)
	if err != nil {
		_ = blobPlugin.Stop()
		return fmt.Errorf("open %q: %w", backupPath, err)
	}
	restoreErr := restorer.Restore(ctx, backupFile)
	closeErr := backupFile.Close()
	stopErr := blobPlugin.Stop()
	if restoreErr != nil {
		return fmt.Errorf("restore blob store: %w", restoreErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %q: %w", backupPath, closeErr)
	}
	if stopErr != nil {
		return fmt.Errorf("stop blob plugin after restore: %w", stopErr)
	}
	return nil
}

// validateRestoredDatabase opens the restored store the same way a normal
// dingo startup would, letting database.New's own checkNodeSettings and
// checkCommitTimestamp checks validate internal consistency, then
// additionally confirms the restored tip matches what the manifest
// recorded before closing it again.
func validateRestoredDatabase(manifest Manifest, targetDataDir string) error {
	db, err := database.New(&database.Config{
		DataDir:        targetDataDir,
		BlobPlugin:     manifest.BlobPlugin,
		MetadataPlugin: manifest.MetadataPlugin,
		StorageMode:    manifest.StorageMode,
		Network:        manifest.Network,
	})
	if db != nil {
		defer db.Close()
	}
	if err != nil {
		return fmt.Errorf(
			"open restored database for validation: %w", err,
		)
	}
	tip, err := db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("get restored tip: %w", err)
	}
	if tip.Point.Slot != manifest.TipSlot ||
		!bytes.Equal(tip.Point.Hash, manifest.TipHash) {
		return fmt.Errorf(
			"restored tip (slot=%d) does not match manifest tip (slot=%d)",
			tip.Point.Slot,
			manifest.TipSlot,
		)
	}
	return nil
}
