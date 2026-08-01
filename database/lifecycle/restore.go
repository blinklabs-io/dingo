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
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	"github.com/blinklabs-io/dingo/plugin"
)

// syncDir is fsyncdir.Sync by default; a package-level var (not a direct
// call) so tests can inject a failure and verify RestoreValidated actually
// surfaces it, instead of silently activating (or reporting success for) a
// restore whose durability on disk was never confirmed.
var syncDir = fsyncdir.Sync

// syncDirTree fsyncs every directory under root, so a file written into
// any of them has a durable directory entry, not just durable content --
// a file's own fsync does not guarantee the directory entry naming it
// survives a power loss. The restored files' own content durability is
// each storage plugin's responsibility (sqlite's RestoreFrom and badger's
// clean Close via host.StopCapability both already fsync what they wrote);
// this closes the remaining directory-entry gap uniformly at this layer,
// so restore's durability does not depend on trusting every plugin's
// internals, present and future, to already cover it.
func syncDirTree(root string) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		return syncDir(path)
	})
}

// Restore populates targetDataDir (which must not already exist, or must
// be empty) from the snapshot at snapshotDir, then opens the result with
// database.New to confirm it passes the same startup consistency checks
// (checkNodeSettings, checkCommitTimestamp) any other dingo startup does,
// and that its tip matches what the manifest recorded. The opened database
// is closed again before returning — the caller is responsible for
// (re)opening it for real use.
//
// The actual restore work happens in a sibling staging directory, only
// atomically renamed into targetDataDir once fully validated — see
// RestoreValidated's doc comment for why: an interruption (including one
// the caller's process cannot catch, e.g. a plain, non-signal-handled
// process kill) leaves targetDataDir completely untouched rather than
// half-restored.
//
// snapshotDir may instead be a cloud destination URI (s3://bucket/prefix
// or gcs://bucket/prefix; see DestinationRegistry) — Restore downloads it
// into a local temp directory first, then proceeds exactly as it would
// for a local snapshotDir. This is also how a snapshot created on one node
// can be restored onto another, since the two never need to share a
// filesystem. registry may be nil if snapshotDir is always a local path.
//
// host resolves the manifest-recorded blob/metadata plugins against
// targetDataDir; composition code (node.go, cmd/dingo, internal/dblifecycle,
// bark) builds and owns it — typically a fresh, single-use host built
// just for this call via internal/plugins.NewHost, mirroring how
// internal/plugins.OpenDatabase builds its own scratch host for a
// temporary open elsewhere. This package never constructs one itself: a
// domain package under the database import boundary registering
// providers or owning a plugin host of its own would split provider
// ownership away from the application's composition root.
//
// This is an offline operation: targetDataDir must not be concurrently
// held open by another *database.Database (e.g. a running node), since it
// restores the metadata store before starting it (metadata.Restorer) and
// the blob store immediately after starting it empty (blob.Restorer) —
// two-phase orchestration that a live store's own Start/Stop lifecycle
// cannot safely interleave with.
func Restore(
	ctx context.Context,
	host *plugin.Host,
	registry *DestinationRegistry,
	snapshotDir string,
	targetDataDir string,
) (Manifest, error) {
	return RestoreValidated(ctx, host, registry, snapshotDir, targetDataDir, nil)
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
//
// Interruption safety: every actual restore step (metadata restore, blob
// restore, validateRestoredDatabase) runs against a sibling staging
// directory (targetDataDir + ".restore-staging"), never targetDataDir
// itself. Only once all of them succeed is the staging directory
// atomically renamed into targetDataDir's place. A caller whose context
// is cancelled mid-restore, or whose process is killed outright (a plain
// SIGKILL, or any termination path that skips Go's deferred cleanup —
// notably, the offline `dingo database restore` CLI's default process
// termination does not install a signal-aware context, so an operator's
// Ctrl+C takes this path, not graceful cancellation), is left with
// targetDataDir exactly as it was before the call: absent, or the same
// empty directory requireEmptyOrAbsent confirmed. The half-restored
// staging directory (if any) is simply an orphaned sibling that a retry's
// own os.RemoveAll(stagingDir) clears on its next attempt.
//
// Crash durability (power loss, not just an interrupted process that
// leaves the OS itself running): atomic rename alone only guarantees
// targetDataDir never shows a partially-visible directory -- it says
// nothing about whether the renamed files, or the rename itself, survive
// a power loss. Before activating the rename, every directory in the
// staged tree is fsynced (syncDirTree); after the rename, the parent
// directory is fsynced too, since the rename changed its own entries. If
// either sync fails, this returns an error without pretending the restore
// completed durably; a failed pre-rename sync leaves targetDataDir
// untouched exactly like any other failure above, and a failed
// post-rename sync is still surfaced even though targetDataDir was
// already activated, since durability could not be confirmed.
func RestoreValidated(
	ctx context.Context,
	host *plugin.Host,
	registry *DestinationRegistry,
	snapshotDir string,
	targetDataDir string,
	validate func(Manifest) error,
) (m Manifest, err error) {
	manifest, snapshotDir, cleanup, err := resolveManifest(ctx, registry, snapshotDir)
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

	// Restore into a sibling staging directory rather than targetDataDir
	// directly, and only atomically rename it into place once every step
	// below has fully succeeded. This is what makes an interruption
	// partway through -- a process kill via SIGINT/SIGTERM, which the
	// default Go runtime does not run deferred cleanup for -- leave
	// targetDataDir completely untouched (absent, or still the empty
	// directory requireEmptyOrAbsent just confirmed) instead of half-
	// restored: the rename below is a single atomic filesystem operation
	// on the same volume, so there is no window where targetDataDir could
	// ever observe a partial write. Mirrors node_lifecycle.go's identical
	// staging-then-swap pattern for the live restore path.
	stagingDir := targetDataDir + ".restore-staging"
	if err := os.RemoveAll(stagingDir); err != nil {
		return Manifest{}, fmt.Errorf(
			"clear restore staging directory %q: %w", stagingDir, err,
		)
	}
	if err := os.MkdirAll(filepath.Dir(targetDataDir), 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create parent directory for %q: %w", targetDataDir, err,
		)
	}
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create restore staging directory %q: %w", stagingDir, err,
		)
	}
	// Best-effort cleanup on any failure below so a retry doesn't hit a
	// half-restored staging directory that looks superficially non-empty
	// -- targetDataDir itself is never touched until the rename below
	// succeeds, so it needs no equivalent cleanup.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	if err := restoreMetadataStore(ctx, host, manifest, snapshotDir, stagingDir); err != nil {
		return Manifest{}, err
	}
	if err := restoreBlobStore(ctx, host, manifest, snapshotDir, stagingDir); err != nil {
		return Manifest{}, err
	}
	if err := validateRestoredDatabase(ctx, host, manifest, stagingDir); err != nil {
		return Manifest{}, err
	}

	// Durability: fsync every directory in the staged tree before
	// activating it. Atomic rename only guarantees targetDataDir can never
	// show a partially-visible directory; it says nothing about whether
	// the rename itself, or the files it names, survive a power loss --
	// that requires an explicit fsync of the directory entries involved,
	// which this and the parent-directory sync below provide.
	if err := syncDirTree(stagingDir); err != nil {
		return Manifest{}, fmt.Errorf(
			"sync restored data directory %q before activating it: %w",
			stagingDir, err,
		)
	}

	// Every step above is validated against the staging copy; activate it
	// with one atomic rename. targetDataDir was already confirmed empty
	// or absent above and has not been touched since, so clearing it
	// first (a no-op if absent, and safe since it's empty if present --
	// some platforms' rename cannot replace an existing directory at
	// all) cannot lose anything.
	if err := os.RemoveAll(targetDataDir); err != nil {
		return Manifest{}, fmt.Errorf(
			"clear target data directory %q before activating restore: %w",
			targetDataDir, err,
		)
	}
	if err := os.Rename(stagingDir, targetDataDir); err != nil {
		return Manifest{}, fmt.Errorf(
			"activate restored data directory: %w", err,
		)
	}

	// The rename above changed the parent directory's own entries
	// (removing stagingDir's name, adding targetDataDir's); fsync it so
	// that change itself is durable, not just atomic. Without this, a
	// power loss right after a successful rename could still lose the
	// restored database on restart even though the rename was atomic.
	if err := syncDir(filepath.Dir(targetDataDir)); err != nil {
		return Manifest{}, fmt.Errorf(
			"sync parent directory of %q after activating restore: %w",
			targetDataDir, err,
		)
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
func PeekManifest(
	ctx context.Context,
	registry *DestinationRegistry,
	snapshotDir string,
) (Manifest, error) {
	if m, ok, err := FetchCloudManifest(ctx, registry, snapshotDir); ok {
		return m, err
	}
	manifest, _, cleanup, err := resolveManifest(ctx, registry, snapshotDir)
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
	registry *DestinationRegistry,
	snapshotDir string,
) (manifest Manifest, resolvedDir string, cleanup func(), err error) {
	resolvedDir = snapshotDir
	if _, ok := recognizedCloudScheme(registry, snapshotDir); ok {
		localSnapshotDir, cloudCleanup, downloadErr := downloadCloudSnapshot(ctx, registry, snapshotDir)
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

// restoreMetadataStore resolves the manifest-recorded metadata plugin
// against targetDataDir just long enough to construct it (plugin.Resolve
// always constructs and starts a provider together — there is no
// construct-only step), then stops it immediately and undoes whatever it
// created on disk, since metadata.Restorer's contract requires
// RestoreFrom to run before the store has ever been started, or after
// Close() — which StopCapability guarantees here.
func restoreMetadataStore(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	snapshotDir string,
	targetDataDir string,
) error {
	store, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		manifest.MetadataPlugin,
		nil,
		metadata.ProviderDependencies{
			DataDir:     targetDataDir,
			StorageMode: manifest.StorageMode,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"resolve metadata plugin %q: %w",
			manifest.MetadataPlugin,
			err,
		)
	}
	restorer, ok := store.(metadata.Restorer)
	if !ok {
		_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
		return fmt.Errorf(
			"metadata plugin %q does not support restore",
			manifest.MetadataPlugin,
		)
	}
	if err := host.StopCapability(ctx, plugin.CapabilityStorageMetadata); err != nil {
		return fmt.Errorf("stop metadata plugin before restore: %w", err)
	}
	// RestoreFrom refuses to overwrite an existing destination -- undo
	// whatever the brief resolve-and-start above wrote to targetDataDir so
	// it looks exactly as empty as before that call.
	if err := os.RemoveAll(targetDataDir); err != nil {
		return fmt.Errorf(
			"reset target data directory %q: %w", targetDataDir, err,
		)
	}
	if err := os.MkdirAll(targetDataDir, 0o755); err != nil {
		return fmt.Errorf(
			"recreate target data directory %q: %w", targetDataDir, err,
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
	host *plugin.Host,
	manifest Manifest,
	snapshotDir string,
	targetDataDir string,
) error {
	store, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		manifest.BlobPlugin,
		nil,
		blob.ProviderDependencies{DataDir: targetDataDir},
	)
	if err != nil {
		return fmt.Errorf(
			"resolve blob plugin %q: %w",
			manifest.BlobPlugin,
			err,
		)
	}
	restorer, ok := store.(blob.Restorer)
	if !ok {
		_ = host.StopCapability(ctx, plugin.CapabilityStorageBlob)
		return fmt.Errorf(
			"blob plugin %q does not support restore",
			manifest.BlobPlugin,
		)
	}
	backupPath := filepath.Join(snapshotDir, BlobBackupFileName)
	backupFile, err := os.Open(backupPath)
	if err != nil {
		_ = host.StopCapability(ctx, plugin.CapabilityStorageBlob)
		return fmt.Errorf("open %q: %w", backupPath, err)
	}
	restoreErr := restorer.Restore(ctx, backupFile)
	closeErr := backupFile.Close()
	stopErr := host.StopCapability(ctx, plugin.CapabilityStorageBlob)
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
func validateRestoredDatabase(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	targetDataDir string,
) error {
	blobStore, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		manifest.BlobPlugin,
		nil,
		blob.ProviderDependencies{DataDir: targetDataDir},
	)
	if err != nil {
		return fmt.Errorf(
			"resolve blob plugin %q for validation: %w",
			manifest.BlobPlugin,
			err,
		)
	}
	defer host.StopCapability( //nolint:errcheck
		context.WithoutCancel(ctx), plugin.CapabilityStorageBlob,
	)
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		manifest.MetadataPlugin,
		nil,
		metadata.ProviderDependencies{
			DataDir:     targetDataDir,
			StorageMode: manifest.StorageMode,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"resolve metadata plugin %q for validation: %w",
			manifest.MetadataPlugin,
			err,
		)
	}
	defer host.StopCapability( //nolint:errcheck
		context.WithoutCancel(ctx), plugin.CapabilityStorageMetadata,
	)

	db, err := database.New(&database.Config{
		DataDir:     targetDataDir,
		StorageMode: manifest.StorageMode,
		Network:     manifest.Network,
	}, database.Stores{Blob: blobStore, Metadata: metadataStore})
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
		!bytes.Equal(tip.Point.Hash, manifest.TipHash) ||
		tip.BlockNumber != manifest.TipBlockNumber {
		return fmt.Errorf(
			"restored tip (slot=%d, block=%d) does not match manifest tip (slot=%d, block=%d)",
			tip.Point.Slot,
			tip.BlockNumber,
			manifest.TipSlot,
			manifest.TipBlockNumber,
		)
	}
	return nil
}
