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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/fsyncdir"
	"github.com/blinklabs-io/dingo/plugin"
)

// checkNoDataDirOverride rejects a provider config carrying a non-empty
// "dataDir" override (the yaml key every storage provider currently uses
// for this, e.g. database/plugin/blob/badger's and .../metadata/sqlite's
// own Config.DataDir, which always wins over ProviderDependencies.DataDir
// when set -- see those packages' provider.go). See RestoreStorageConfig's
// doc comment for why this specific setting cannot simply be propagated
// like any other provider config: restore writes into a sibling staging
// directory it derives from targetDataDir, and a provider redirected
// elsewhere by its own config would write outside that staging directory
// entirely, silently losing the atomic-rename interruption safety
// RestoreValidated otherwise guarantees.
func checkNoDataDirOverride(pluginKind string, cfg map[string]any) error {
	raw, ok := cfg["dataDir"]
	if !ok {
		return nil
	}
	if raw == nil {
		return nil
	}
	override, ok := raw.(string)
	if !ok {
		return fmt.Errorf(
			"restore does not support a non-string %s provider dataDir override",
			pluginKind,
		)
	}
	if override == "" {
		return nil
	}
	return fmt.Errorf(
		"restore does not support a configured %s provider dataDir override (%q): "+
			"restore always writes under the given targetDataDir's own staging "+
			"directory, and honoring a separate override would bypass its "+
			"atomic-rename interruption safety",
		pluginKind,
		override,
	)
}

// syncDir is fsyncdir.Sync by default; a package-level var (not a direct
// call) so tests can inject a failure and verify RestoreValidated actually
// surfaces it, instead of silently activating (or reporting success for) a
// restore whose durability on disk was never confirmed.
var syncDir = fsyncdir.Sync

// ErrRestoreRollbackPending means restore mutated an external store and its
// automatic compensation did not complete. The error includes the retained
// backup directory. A live caller must not reopen or serve the target until
// recovery succeeds.
var ErrRestoreRollbackPending = errors.New("automatic restore rollback failed")

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
	return filepath.WalkDir(
		root,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			return syncDir(path)
		},
	)
}

// RestoreStorageConfig carries the caller's configured provider-owned
// settings (plugin.Selection.Config, e.g. Badger/SQLite tuning options) for
// the blob/metadata plugins Restore resolves, mirroring what a normal
// dingo startup (internal/plugins.ResolveStorage) already passes instead
// of the nil config every resolve here used before this existed. The zero
// value (both nil) preserves the original behavior exactly.
//
// A per-plugin "dataDir" override specifically is deliberately rejected
// (see checkNoDataDirOverride) rather than honored: restoreMetadataStore/
// restoreBlobStore write into a sibling staging directory, never
// targetDataDir directly, so the atomic-rename interruption-safety
// RestoreValidated's own doc comment describes holds; a provider
// redirected by its own config to some other directory entirely would
// write outside that staging directory, bypassing the same guarantee.
// Refusing the restore outright when this is configured is safer than
// either silently writing to the wrong place (the original gap this type
// exists to close) or silently ignoring the override (a different,
// equally silent divergence from what a real subsequent startup would do).
type RestoreStorageConfig struct {
	Blob     map[string]any
	Metadata map[string]any
	// ActiveDataDir identifies the currently active local directory during a
	// live restore. Cloud blob restores use its marker to retain the namespace
	// that Commit may reclaim or Rollback must preserve.
	ActiveDataDir string
	// RecoveryMarkerDirs are trusted local directories where a live restore's
	// operation marker may reside after an interrupted directory swap.
	RecoveryMarkerDirs []string
	// Logger, when set, receives the restore's deferred-index repair
	// progress. A full manifest rebuild on a multi-million-row table takes
	// minutes and is otherwise silent. Nil disables that reporting; nothing
	// else in this package logs.
	Logger *slog.Logger
	// AlonzoLovelacePerUtxoWord is passed through to the validating open's
	// database.Config. It is here rather than derived because this package
	// sits under the database import boundary and cannot read a
	// cardano-node config: without it the validating open of a snapshot
	// taken from a pre-gouroboros-v0.205.7 database refuses the restore
	// and demands a resync, exactly as a real startup would that omitted
	// it. Zero preserves that refusal for a caller that has no genesis.
	AlonzoLovelacePerUtxoWord uint64
}

const staleRestoreWorkDirAge = 7 * 24 * time.Hour

var restoreWorkDirPrefixes = []string{
	".dingo-restore-payloads-",
	".dingo-restore-staging-",
	".dingo-restore-target-",
	".dingo-restore-move-",
	".dingo-cloud-snapshot-",
	".dingo-verify-snapshot-",
}

// leasedRestoreWorkDirPrefixes name the work directories created through
// createOwnedRestoreDir, which leases each one for its creator's lifetime.
// The others are created elsewhere or hold an operator's original target, so
// only the age cutoff can show they are abandoned.
var leasedRestoreWorkDirPrefixes = []string{
	".dingo-restore-payloads-",
	".dingo-restore-staging-",
	".dingo-cloud-snapshot-",
}

const (
	restoreRecoveryFileName   = "recovery.json"
	restoreRecoveryMarkerName = ".dingo-restore-operation"
	restoreRecoveryIDBytes    = 32
)

// RestoreRecoveryDir returns the durable recovery directory associated with
// a live restore target.
func RestoreRecoveryDir(targetDataDir string) string {
	return targetDataDir + ".remote-rollback"
}

func restoreRecoveryMarkerDirs(
	cfg RestoreStorageConfig,
	additional ...string,
) []string {
	dirs := make([]string, 0, 1+len(cfg.RecoveryMarkerDirs)+len(additional))
	seen := make(map[string]struct{}, cap(dirs))
	candidates := append([]string{cfg.ActiveDataDir}, cfg.RecoveryMarkerDirs...)
	// The caller's target directory (the restore staging sibling on startup
	// recovery) is not a trusted marker location. It only stands in when no
	// active data directory is configured.
	if cfg.ActiveDataDir == "" {
		candidates = append(candidates, additional...)
	}
	for _, dir := range candidates {
		if dir == "" {
			continue
		}
		dir = filepath.Clean(dir)
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		dirs = append(dirs, dir)
	}
	return dirs
}

func newRestoreRecoveryID() (string, error) {
	raw := make([]byte, restoreRecoveryIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate restore recovery operation ID: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func writeRestoreRecoveryMarker(dir, operationID string) error {
	path := filepath.Join(dir, restoreRecoveryMarkerName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create restore recovery marker: %w", err)
	}
	if _, err := file.WriteString(operationID + "\n"); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write restore recovery marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("sync restore recovery marker: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close restore recovery marker: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync restore recovery marker directory: %w", err)
	}
	return nil
}

func removeRestoreRecoveryMarkers(dirs []string, operationID string) error {
	var errs []error
	for _, dir := range dirs {
		path := filepath.Join(dir, restoreRecoveryMarkerName)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if operationID != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if strings.TrimSpace(string(data)) != operationID {
				continue
			}
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
			continue
		}
		if err := syncDir(dir); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// CleanStaleRestoreWorkDirs removes abandoned restore work directories from
// parentDir. A directory a running restore still holds is never removed. On
// Unix, a populated staging, payload, or cloud download directory that no
// process holds is removed at once, because its restore exited without
// cleaning up. Every other unheld directory, and on Windows every directory,
// is removed only once it is older than a week, since a recent one may belong
// to another active operation.
func CleanStaleRestoreWorkDirs(parentDir string) error {
	parent, err := os.OpenRoot(parentDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open restore work directory parent %q: %w", parentDir, err)
	}
	defer parent.Close() //nolint:errcheck // cleanup errors are returned below
	entries, err := fs.ReadDir(parent.FS(), ".")
	if err != nil {
		return fmt.Errorf("read restore work directory parent %q: %w", parentDir, err)
	}
	cutoff := time.Now().Add(-staleRestoreWorkDirAge)
	for _, entry := range entries {
		if !entry.IsDir() || !hasRestoreWorkDirPrefix(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return fmt.Errorf("inspect restore work directory %q: %w", entry.Name(), err)
		}
		if err := cleanStaleRestoreEntry(parent, parentDir, entry.Name(), info, cutoff); err != nil {
			return err
		}
	}
	return nil
}

func cleanStaleRestoreEntry(
	parent *os.Root,
	parentDir, name string,
	expected os.FileInfo,
	cutoff time.Time,
) error {
	path := filepath.Join(parentDir, name)
	dir, err := openRestoreDir(parent, name, path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open stale restore work directory %q: %w", path, err)
	}
	if !os.SameFile(expected, dir.info) {
		_ = dir.closeRoot()
		return fmt.Errorf("%w: %q changed after enumeration", errRestoreDirReplaced, path)
	}
	lease, acquired, err := leaseRestoreDir(dir.root, false)
	switch {
	case errors.Is(err, errRestoreLeaseUnsupported):
		if dir.info.ModTime().After(cutoff) {
			return dir.closeRoot()
		}
		return cleanupStaleRestoreDir(dir, path)
	case err != nil:
		_ = dir.closeRoot()
		return fmt.Errorf("probe restore work directory %q: %w", path, err)
	case !acquired:
		// A running operation holds it, however old it looks.
		return dir.closeRoot()
	}
	defer lease.Close() //nolint:errcheck // releasing a probe lock
	if dir.info.ModTime().After(cutoff) {
		reclaim, err := abandonedRestoreWorkDir(dir, name)
		if err != nil || !reclaim {
			return errors.Join(err, dir.closeRoot())
		}
	}
	return cleanupStaleRestoreDir(dir, path)
}

// abandonedRestoreWorkDir reports whether a recent, unheld work directory
// can be reclaimed before the age cutoff. Only names whose creator leases
// them qualify, and only once populated: the creator takes its lease before
// writing anything, so an empty one may be a new directory whose creator has
// not leased it yet.
func abandonedRestoreWorkDir(dir *restoreDir, name string) (bool, error) {
	leased := false
	for _, prefix := range leasedRestoreWorkDirPrefixes {
		if strings.HasPrefix(name, prefix) {
			leased = true
			break
		}
	}
	if !leased {
		return false, nil
	}
	opened, err := dir.root.Open(".")
	if err != nil {
		return false, err
	}
	defer opened.Close() //nolint:errcheck // read-only directory listing
	names, err := opened.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(names) != 0, nil
}

func hasRestoreWorkDirPrefix(name string) bool {
	for _, prefix := range restoreWorkDirPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// Restore populates targetDataDir (which must not already exist, or must
// be empty) from the snapshot at snapshotDir, then opens the result with
// database.New to confirm it passes the same startup consistency checks
// (CheckNodeSettings, checkCommitTimestamp) any other dingo startup does,
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
// storageConfig propagates the caller's actual configured provider
// settings into the plugins this resolves — see RestoreStorageConfig's
// doc comment. Pass the zero value if the caller has no such
// configuration to propagate.
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
	storageConfig RestoreStorageConfig,
	opts ...ManifestOption,
) (Manifest, error) {
	return RestoreValidated(
		ctx, host, registry, snapshotDir, targetDataDir, nil, storageConfig,
		opts...,
	)
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
// directory with a unique .dingo-restore-staging- name, never targetDataDir
// itself. Only once all of them succeed is the staging directory
// atomically renamed into targetDataDir's place. A caller whose context
// is cancelled mid-restore, or whose process is killed outright (a plain
// SIGKILL, or any termination path that skips Go's deferred cleanup —
// notably, the offline `dingo database restore` CLI's default process
// termination does not install a signal-aware context, so an operator's
// Ctrl+C takes this path, not graceful cancellation), is left with
// targetDataDir exactly as it was before the call: absent, or the same
// empty directory opened and confirmed. The half-restored staging and
// payload directories are orphaned siblings. The next restore into the same
// parent removes them through retained directory handles before it starts:
// on Unix at once, since the killed process no longer holds them (see
// CleanStaleRestoreWorkDirs), and on Windows after the age cutoff.
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
	storageConfig RestoreStorageConfig,
	opts ...ManifestOption,
) (Manifest, error) {
	return restoreValidated(
		ctx,
		host,
		registry,
		snapshotDir,
		targetDataDir,
		validate,
		storageConfig,
		nil,
		opts...,
	)
}

// RestoreRecoverable performs the same validated restore but retains any
// original external-store backups after the replacement passes lifecycle
// validation. The caller must call Commit after its own enclosing operation
// succeeds, or Rollback if a later step fails. This is used by live node
// restore so a local directory-swap failure cannot strand new remote metadata
// beside the original local blob store (or the inverse).
//
// The supplied plugin host must remain active until Commit or Rollback returns;
// Rollback resolves the same providers through it to reload the retained copy.
// If automatic compensation fails, this returns both
// ErrRestoreRollbackPending and a non-nil recovery handle so a caller can
// recognize that the target is unsafe and retry Rollback while the host is
// still active.
func RestoreRecoverable(
	ctx context.Context,
	host *plugin.Host,
	registry *DestinationRegistry,
	snapshotDir string,
	targetDataDir string,
	validate func(Manifest) error,
	storageConfig RestoreStorageConfig,
	opts ...ManifestOption,
) (Manifest, *RestoreRecovery, error) {
	var recovery *RestoreRecovery
	manifest, err := restoreValidated(
		ctx,
		host,
		registry,
		snapshotDir,
		targetDataDir,
		validate,
		storageConfig,
		&recovery,
		opts...,
	)
	return manifest, recovery, err
}

func restoreValidated(
	ctx context.Context,
	host *plugin.Host,
	registry *DestinationRegistry,
	snapshotDir string,
	targetDataDir string,
	validate func(Manifest) error,
	storageConfig RestoreStorageConfig,
	retainRecovery **RestoreRecovery,
	opts ...ManifestOption,
) (m Manifest, err error) {
	// Downloads and private payload copies are as large as the snapshot, so
	// they are made beside the target, on the volume sized for the restored
	// data, never in the system temp directory.
	workParent := filepath.Dir(targetDataDir)
	if err := os.MkdirAll(workParent, 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create parent directory for %q: %w", targetDataDir, err,
		)
	}
	if err := validateRestoreWorkParent(workParent); err != nil {
		return Manifest{}, err
	}
	workRoot, err := os.OpenRoot(workParent)
	if err != nil {
		return Manifest{}, fmt.Errorf(
			"open parent directory for %q: %w", targetDataDir, err,
		)
	}
	defer workRoot.Close() //nolint:errcheck // restore result is reported below
	if err := CleanStaleRestoreWorkDirs(workParent); err != nil {
		return Manifest{}, err
	}
	manifest, err := resolveManifest(
		ctx, registry, snapshotDir, workParent, opts...,
	)
	if err != nil {
		return Manifest{}, err
	}
	if validate != nil {
		if err := validate(manifest); err != nil {
			return Manifest{}, err
		}
	}
	snapshotDir, cleanup, err := fetchPayloads(
		ctx, registry, snapshotDir, workParent, manifest,
	)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return Manifest{}, err
	}
	if err := checkNoDataDirOverride("blob", storageConfig.Blob); err != nil {
		return Manifest{}, err
	}
	if err := checkNoDataDirOverride("metadata", storageConfig.Metadata); err != nil {
		return Manifest{}, err
	}

	targetName := filepath.Base(filepath.Clean(targetDataDir))
	var originalTarget *restoreDir
	if _, statErr := workRoot.Lstat(targetName); statErr == nil {
		originalTarget, err = openRestoreDir(workRoot, targetName, targetDataDir)
		if err != nil {
			return Manifest{}, err
		}
		entries, readErr := fs.ReadDir(originalTarget.root.FS(), ".")
		if readErr != nil {
			_ = originalTarget.closeRoot()
			return Manifest{}, readErr
		}
		if len(entries) != 0 {
			_ = originalTarget.closeRoot()
			return Manifest{}, fmt.Errorf(
				"target data directory %q is not empty", targetDataDir,
			)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return Manifest{}, fmt.Errorf(
			"inspect target data directory %q: %w", targetDataDir, statErr,
		)
	}
	defer func() {
		if originalTarget != nil {
			_ = originalTarget.closeRoot()
		}
	}()

	// Restore into a sibling staging directory rather than targetDataDir
	// directly, and only atomically rename it into place once every step
	// below has fully succeeded. This is what makes an interruption
	// partway through -- a process kill via SIGINT/SIGTERM, which the
	// default Go runtime does not run deferred cleanup for -- leave
	// targetDataDir completely untouched (absent, or still the empty
	// directory opened and confirmed above) instead of half-
	// restored: the rename below is a single atomic filesystem operation
	// on the same volume, so there is no window where targetDataDir could
	// ever observe a partial write. Mirrors node_lifecycle.go's identical
	// staging-then-swap pattern for the live restore path.
	stagingOwned, err := createOwnedRestoreDir(
		workParent, ".dingo-restore-staging-",
	)
	if err != nil {
		return Manifest{}, fmt.Errorf(
			"create restore staging directory: %w", err,
		)
	}
	staging := stagingOwned.dir
	stagingDir := staging.path
	activated := false
	// Best-effort cleanup on any failure below so a retry doesn't hit a
	// half-restored staging directory that looks superficially non-empty
	// -- targetDataDir itself is never touched until the rename below
	// succeeds, so it needs no equivalent cleanup.
	defer func() {
		if err != nil && !activated {
			_ = stagingOwned.cleanup()
		} else {
			_ = stagingOwned.close()
		}
	}()

	// Verified only now that the staging directory exists, and before any
	// store is touched, so a payload that does not match its manifest is
	// refused with the target and every external store still as they were.
	if err := manifest.verifyPayloads(
		ctx, snapshotDir, len(manifestKey(opts)) > 0,
	); err != nil {
		return Manifest{}, err
	}
	var blobNamespace string
	if manifest.BlobPlugin == "s3" || manifest.BlobPlugin == "gcs" {
		blobNamespace, err = blob.NewNamespace()
		if err != nil {
			return Manifest{}, err
		}
		if err := blob.WriteNamespace(stagingDir, blobNamespace); err != nil {
			return Manifest{}, err
		}
	}

	rollback, err := prepareRestore(
		ctx,
		host,
		manifest,
		snapshotDir,
		stagingDir,
		storageConfig,
		retainRecovery != nil,
	)
	if err != nil {
		return Manifest{}, err
	}
	retainRollback := func() {
		if retainRecovery != nil {
			*retainRecovery = &RestoreRecovery{
				rollback:      rollback,
				host:          host,
				manifest:      manifest,
				targetDataDir: targetDataDir,
				storageConfig: storageConfig,
			}
		}
	}
	defer func() {
		if err == nil {
			if retainRecovery != nil {
				retainRollback()
				return
			}
			_ = rollback.removeDir()
			return
		}
		if !rollback.metadataMutated && !rollback.blobMutated {
			_ = rollback.removeDir()
			return
		}
		rollbackTargetDir := stagingDir
		if activated {
			rollbackTargetDir = targetDataDir
		}
		rollbackErr := rollback.restore(
			context.WithoutCancel(
				ctx,
			),
			host,
			manifest,
			rollbackTargetDir,
			storageConfig,
		)
		if rollbackErr == nil {
			rollback.dispositionComplete = true
			rollbackErr = rollback.removeDir()
			if rollbackErr == nil {
				return
			}
		}
		retainRollback()
		err = errors.Join(
			err,
			fmt.Errorf(
				"%w; original backups preserved at %q: %w",
				ErrRestoreRollbackPending,
				rollback.dir,
				rollbackErr,
			),
		)
	}()
	if rollback.durable {
		if err := rollback.persist(manifest); err != nil {
			return Manifest{}, err
		}
	}

	rollback.metadataMutated = rollback.metadataPath != ""
	if err := restoreMetadataStore(
		ctx,
		host,
		manifest,
		filepath.Join(snapshotDir, MetadataBackupFileName),
		stagingDir,
		storageConfig.Metadata,
		staging,
	); err != nil {
		return Manifest{}, err
	}
	if err := staging.checkNamed(); err != nil {
		return Manifest{}, err
	}
	// File-backed metadata restore recreates stagingDir. Restore the marker
	// before opening the blob provider so it resolves the same namespace that
	// prepareRestore checked.
	if blobNamespace != "" {
		if err := blob.WriteNamespace(stagingDir, blobNamespace); err != nil {
			return Manifest{}, err
		}
	}
	rollback.blobMutated = rollback.blobPath != "" || rollback.cloudBlob
	if err := restoreBlobStore(
		ctx,
		host,
		manifest,
		filepath.Join(snapshotDir, BlobBackupFileName),
		stagingDir,
		storageConfig.Blob,
		rollback.blobPath != "",
	); err != nil {
		return Manifest{}, err
	}
	if err := staging.checkNamed(); err != nil {
		return Manifest{}, err
	}
	if err := validateRestoredDatabase(
		ctx, host, manifest, stagingDir, storageConfig,
	); err != nil {
		return Manifest{}, err
	}
	if err := staging.checkNamed(); err != nil {
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
	if err := staging.checkNamed(); err != nil {
		return Manifest{}, err
	}

	// Every step above is validated against the staging copy. Dispose an
	// accepted empty target before publishing, matching the prior activation
	// contract. The staged tree moves through a random quarantine name and is
	// published with a no-replace rename, so a concurrent destination creator
	// is preserved and makes activation fail.
	activated, err = activateRestoreDirs(
		staging,
		originalTarget,
		workParent,
		targetName,
		targetDataDir,
	)
	if err != nil {
		return Manifest{}, err
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
// alongside it — before falling back to resolveManifest below, which
// downloads manifest.json alone. FetchCloudManifest's ok=false covers two
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
	opts ...ManifestOption,
) (Manifest, error) {
	if _, err := manifestByteLimit(opts); err != nil {
		return Manifest{}, err
	}
	if !recognizedCloudScheme(registry, snapshotDir) {
		return ReadManifest(snapshotDir, opts...)
	}
	if err := requireManifestKey(opts); err != nil {
		return Manifest{}, err
	}
	if m, ok, err := FetchCloudManifest(ctx, registry, snapshotDir, opts...); ok {
		return m, err
	}
	return resolveManifest(ctx, registry, snapshotDir, "", opts...)
}

// resolveManifest reads the manifest of the snapshot at snapshotDir, a local
// directory or a cloud destination URI, authenticating it with opts. A cloud
// snapshot's manifest.json is fetched on its own into a temporary directory
// under workParent (the system temp directory when empty), bounded by the
// manifest size limit, and nothing else under the prefix is read.
func resolveManifest(
	ctx context.Context,
	registry *DestinationRegistry,
	snapshotDir string,
	workParent string,
	opts ...ManifestOption,
) (Manifest, error) {
	limit, err := manifestByteLimit(opts)
	if err != nil {
		return Manifest{}, err
	}
	if !recognizedCloudScheme(registry, snapshotDir) {
		if len(manifestKey(opts)) == 0 {
			if err := requirePrivateLocalSnapshot(snapshotDir); err != nil {
				return Manifest{}, err
			}
		}
		return ReadManifest(snapshotDir, opts...)
	}
	if err := requireManifestKey(opts); err != nil {
		return Manifest{}, err
	}
	// The download directory is temporary; the signed ID must match the
	// object prefix the snapshot was requested by.
	opts, err = withCloudManifestID(snapshotDir, opts)
	if err != nil {
		return Manifest{}, err
	}
	// One byte over the limit lets ReadManifest, not the transfer, report an
	// oversized manifest as ErrManifestTooLarge.
	downloadLimit := limit
	if limit < math.MaxInt64 {
		downloadLimit++
	}
	dir, cleanup, err := downloadCloudFiles(
		ctx, registry, snapshotDir, workParent,
		[]DownloadFile{{Name: ManifestFileName, MaxBytes: downloadLimit}},
	)
	if err != nil {
		return Manifest{}, err
	}
	defer cleanup()
	return ReadManifest(dir, opts...)
}

// fetchPayloads returns a local directory, created under workParent, holding
// the backup files manifest declares. A cloud snapshot downloads only those
// files, each bounded by its declared size. It returns a cleanup func for any downloaded temp directory —
// nil when nothing was downloaded, so callers must nil-check before deferring
// it. The files are not yet verified; see Manifest.verifyPayloads.
func fetchPayloads(
	ctx context.Context,
	registry *DestinationRegistry,
	snapshotDir string,
	workParent string,
	manifest Manifest,
) (resolvedDir string, cleanup func(), err error) {
	if !recognizedCloudScheme(registry, snapshotDir) {
		// Consume private copies: reopening the operator's path after digest
		// verification permits a replacement file to bypass authentication.
		owned, err := createOwnedRestoreDir(workParent, ".dingo-restore-payloads-")
		if err != nil {
			return "", nil, err
		}
		dir := owned.dir.path
		cleanup := func() { _ = owned.cleanup() }
		for _, file := range manifest.payloadDownloads() {
			src, err := os.Open(filepath.Join(snapshotDir, file.Name))
			if err != nil {
				return "", cleanup, err
			}
			err = writeBoundedFile(filepath.Join(dir, file.Name),
				payloadContextReader{ctx: ctx, reader: src}, file)
			closeErr := src.Close()
			if err != nil {
				if errors.Is(err, ErrDownloadTooLarge) {
					err = fmt.Errorf("%w: %w", ErrSnapshotPayloadMismatch, err)
				}
				return "", cleanup, err
			}
			if closeErr != nil {
				return "", cleanup, closeErr
			}
		}
		return dir, cleanup, nil
	}
	return downloadCloudFiles(
		ctx, registry, snapshotDir, workParent, manifest.payloadDownloads(),
	)
}

// restoreRollback records pre-mutation backups for remote stores. Local
// Badger/SQLite restores need no entry here because their entire replacement
// remains inside the disposable staging directory until the final rename.
type restoreRollback struct {
	dir                 string
	dirHandle           *ownedRestoreDir
	dirCleanupAttempted bool
	dirCleanupErr       error
	metadataPath        string
	blobPath            string
	newBlobNamespace    string
	oldBlobNamespace    string
	cloudBlob           bool
	durable             bool
	metadataMutated     bool
	blobMutated         bool
	dispositionComplete bool
	operationID         string
	markerDirs          []string
}

// RestoreRecovery retains the original external stores after a successful
// RestoreRecoverable call until its caller completes the enclosing live-node
// operation. It is also returned with ErrRestoreRollbackPending so the caller
// can retry an unsuccessful automatic rollback while the plugin host remains
// active. It is safe to call Commit or Rollback more than once; the first
// successful disposition wins.
type RestoreRecovery struct {
	mu            sync.Mutex
	rollback      restoreRollback
	host          *plugin.Host
	manifest      Manifest
	targetDataDir string
	storageConfig RestoreStorageConfig
	done          bool
}

// BackupDir returns the directory holding the original external-store backups.
// It remains useful in an error message if automatic rollback cannot complete.
func (r *RestoreRecovery) BackupDir() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rollback.dir
}

// Commit accepts the replacement and removes the retained original backups.
func (r *RestoreRecovery) Commit() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil
	}
	if !r.rollback.dispositionComplete {
		if err := r.rollback.removeOriginalBlobNamespace(
			context.Background(), r.host, r.manifest, r.storageConfig,
		); err != nil {
			return err
		}
		r.rollback.dispositionComplete = true
	}
	if err := r.rollback.removeDir(); err != nil {
		return fmt.Errorf("remove restore rollback directory: %w", err)
	}
	r.done = true
	return nil
}

// Rollback rejects the replacement and restores both original external stores.
// Cancellation of the initiating operation is deliberately detached so it
// cannot interrupt compensation after a destructive reset has begun.
func (r *RestoreRecovery) Rollback(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil
	}
	if !r.rollback.dispositionComplete {
		if err := r.rollback.restore(
			context.WithoutCancel(ctx),
			r.host,
			r.manifest,
			r.targetDataDir,
			r.storageConfig,
		); err != nil {
			return fmt.Errorf(
				"automatic restore rollback failed; original backups preserved at %q: %w",
				r.rollback.dir,
				err,
			)
		}
		r.rollback.dispositionComplete = true
	}
	if err := r.rollback.removeDir(); err != nil {
		return fmt.Errorf("remove restore rollback directory: %w", err)
	}
	r.done = true
	return nil
}

// stopBlobCapabilityUncancelable stops the blob capability using a context
// that cannot be cut short by ctx's own cancellation. BlobStoreBadger.
// CloseContext (the real on-disk "badger" provider's StopFunc) races ctx
// against its own background close and returns as soon as ctx is Done,
// before the underlying badger.DB.Close call -- which releases the OS-level
// directory lock -- has actually run; see CloseContext's and Closed's doc
// comments in database/plugin/blob/badger/database.go. Every blob
// StopCapability call in this file that precedes a later in-process reopen
// of the same directory (prepareRestore's preflight resolve,
// restoreBlobStore's post-Restore stop, and the automatic rollback's own
// reopen) used ctx directly, so a restore whose context was canceled mid-
// Restore -- the ordinary case when a caller's own context is what just
// failed Restore -- could leave that store's close still running in the
// background while a subsequent resolve (the automatic rollback, or a
// caller retrying against the same host) reopened the same path and lost
// the race for the lock. Detaching cancellation here does not change
// whether the restore itself is cancelable: only this cleanup step, which
// must run to completion regardless of why it is running, is affected.
func stopBlobCapabilityUncancelable(
	ctx context.Context,
	host *plugin.Host,
) error {
	return host.StopCapability(
		context.WithoutCancel(ctx), plugin.CapabilityStorageBlob,
	)
}

// prepareRestore validates both incoming archives before either live store is
// reset. When the live-node call path explicitly permits replacement of a
// populated remote target, it also captures both original stores first. This
// makes every later error recoverable by restoreRollback.restore.
func prepareRestore(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	snapshotDir string,
	targetDataDir string,
	storageConfig RestoreStorageConfig,
	durable bool,
) (restoreRollback, error) {
	var rollbackOwned *ownedRestoreDir
	var err error
	if durable {
		rollbackDir := RestoreRecoveryDir(targetDataDir)
		if err := RecoverInterruptedRestore(
			ctx,
			host,
			rollbackDir,
			targetDataDir,
			storageConfig,
			true,
		); err != nil {
			return restoreRollback{}, fmt.Errorf(
				"dispose completed restore recovery: %w", err,
			)
		}
		rollbackOwned, err = createOwnedRestoreDirAt(rollbackDir)
	} else {
		rollbackOwned, err = createOwnedRestoreDir(
			filepath.Dir(targetDataDir), ".dingo-restore-rollback-",
		)
	}
	if err != nil {
		return restoreRollback{}, fmt.Errorf(
			"create restore rollback directory: %w", err,
		)
	}
	rollback := restoreRollback{
		dir:        rollbackOwned.dir.path,
		dirHandle:  rollbackOwned,
		durable:    durable,
		markerDirs: restoreRecoveryMarkerDirs(storageConfig, targetDataDir),
	}
	cleanupOnError := func(err error) (restoreRollback, error) {
		_ = rollback.removeDir()
		return restoreRollback{}, err
	}

	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		manifest.MetadataPlugin,
		storageConfig.Metadata,
		metadata.ProviderDependencies{
			DataDir:     targetDataDir,
			StorageMode: manifest.StorageMode,
		},
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf(
			"resolve metadata plugin %q for restore preflight: %w",
			manifest.MetadataPlugin, err,
		))
	}
	metadataBackupPath := filepath.Join(snapshotDir, MetadataBackupFileName)
	if _, ok := metadataStore.(metadata.Restorer); !ok {
		_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
		return cleanupOnError(fmt.Errorf(
			"metadata plugin %q does not support restore",
			manifest.MetadataPlugin,
		))
	}
	if _, err := os.Stat(metadataBackupPath); err != nil {
		_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
		return cleanupOnError(fmt.Errorf(
			"backup %q: %w", metadataBackupPath, err,
		))
	}
	if validator, ok := metadataStore.(metadata.BackupValidator); ok {
		if err := validator.ValidateBackup(ctx, metadataBackupPath); err != nil {
			_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
			return cleanupOnError(fmt.Errorf(
				"validate metadata backup %q: %w", metadataBackupPath, err,
			))
		}
	}
	metadataDestructive := false
	if destructive, ok := metadataStore.(interface {
		HasDestructiveReset() bool
	}); ok {
		metadataDestructive = destructive.HasDestructiveReset()
	}
	if metadataDestructive && metadata.ResetOfPopulatedTargetAllowed(ctx) {
		backuper, ok := metadataStore.(metadata.Backuper)
		if !ok {
			_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
			return cleanupOnError(fmt.Errorf(
				"metadata plugin %q cannot retain a rollback backup",
				manifest.MetadataPlugin,
			))
		}
		rollback.metadataPath = filepath.Join(
			rollback.dir, MetadataBackupFileName,
		)
		if err := backuper.BackupTo(ctx, rollback.metadataPath); err != nil {
			_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
			return cleanupOnError(fmt.Errorf(
				"backup original metadata store before restore: %w", err,
			))
		}
	}
	if err := host.StopCapability(
		ctx, plugin.CapabilityStorageMetadata,
	); err != nil {
		return cleanupOnError(fmt.Errorf(
			"stop metadata plugin after restore preflight: %w", err,
		))
	}

	blobStore, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		manifest.BlobPlugin,
		storageConfig.Blob,
		blob.ProviderDependencies{DataDir: targetDataDir, RunMode: "load"},
	)
	if err != nil {
		return cleanupOnError(fmt.Errorf(
			"resolve blob plugin %q for restore preflight: %w",
			manifest.BlobPlugin, err,
		))
	}
	blobNamespace, err := blob.ReadNamespace(targetDataDir)
	if err != nil {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return cleanupOnError(err)
	}
	rollback.newBlobNamespace = blobNamespace
	rollback.cloudBlob = manifest.BlobPlugin == "s3" || manifest.BlobPlugin == "gcs"
	if rollback.cloudBlob && storageConfig.ActiveDataDir != "" {
		rollback.oldBlobNamespace, err = blob.ReadNamespace(storageConfig.ActiveDataDir)
		if err != nil {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return cleanupOnError(err)
		}
	}
	if _, ok := blobStore.(blob.Restorer); !ok {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return cleanupOnError(fmt.Errorf(
			"blob plugin %q does not support restore", manifest.BlobPlugin,
		))
	}
	blobBackupPath := filepath.Join(snapshotDir, BlobBackupFileName)
	blobBackupFile, err := os.Open(blobBackupPath)
	if err != nil {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return cleanupOnError(fmt.Errorf("open %q: %w", blobBackupPath, err))
	}
	if validator, ok := blobStore.(blob.BackupValidator); ok {
		err = validator.ValidateBackup(ctx, blobBackupFile)
	}
	closeErr := blobBackupFile.Close()
	if err != nil {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return cleanupOnError(fmt.Errorf(
			"validate blob backup %q: %w", blobBackupPath, err,
		))
	}
	if closeErr != nil {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return cleanupOnError(fmt.Errorf(
			"close blob backup %q after validation: %w",
			blobBackupPath,
			closeErr,
		))
	}
	if _, resettable := blobStore.(blob.Resettable); resettable &&
		blobNamespace == "" &&
		metadata.ResetOfPopulatedTargetAllowed(ctx) {
		backuper, ok := blobStore.(blob.Backuper)
		if !ok {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return cleanupOnError(fmt.Errorf(
				"blob plugin %q cannot retain a rollback backup",
				manifest.BlobPlugin,
			))
		}
		rollback.blobPath = filepath.Join(rollback.dir, BlobBackupFileName)
		rollbackFile, openErr := os.OpenFile(
			rollback.blobPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if openErr != nil {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return cleanupOnError(fmt.Errorf(
				"create original blob rollback backup: %w", openErr,
			))
		}
		backupErr := backuper.Backup(ctx, rollbackFile)
		if backupErr == nil {
			backupErr = rollbackFile.Sync()
		}
		backupCloseErr := rollbackFile.Close()
		if backupErr != nil {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return cleanupOnError(fmt.Errorf(
				"backup original blob store before restore: %w", backupErr,
			))
		}
		if backupCloseErr != nil {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return cleanupOnError(fmt.Errorf(
				"close original blob rollback backup: %w", backupCloseErr,
			))
		}
	}
	if err := stopBlobCapabilityUncancelable(ctx, host); err != nil {
		return cleanupOnError(fmt.Errorf(
			"stop blob plugin after restore preflight: %w", err,
		))
	}
	if durable && (rollback.metadataPath != "" || rollback.cloudBlob) {
		markerDir := storageConfig.ActiveDataDir
		if markerDir == "" {
			markerDir = targetDataDir
		}
		rollback.operationID, err = newRestoreRecoveryID()
		if err != nil {
			return cleanupOnError(err)
		}
		if err := writeRestoreRecoveryMarker(
			markerDir, rollback.operationID,
		); err != nil {
			return cleanupOnError(err)
		}
	}
	return rollback, nil
}

func (r *restoreRollback) restore(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	targetDataDir string,
	storageConfig RestoreStorageConfig,
) error {
	var errs []error
	if r.blobMutated && r.cloudBlob {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		selectErr := selectBlobNamespace(r.dir, r.newBlobNamespace)
		if selectErr != nil {
			errs = append(
				errs,
				fmt.Errorf("select staged blob namespace: %w", selectErr),
			)
		}
		if selectErr == nil || blob.NamespacePublished(selectErr) {
			if err := resetBlobStore(
				ctx, host, manifest, r.dir, storageConfig.Blob,
			); err != nil {
				errs = append(errs, fmt.Errorf("remove staged blob namespace: %w", err))
			}
		}
	} else if r.blobMutated && r.blobPath != "" {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		if err := restoreBlobStore(
			ctx,
			host,
			manifest,
			r.blobPath,
			targetDataDir,
			storageConfig.Blob,
			true,
		); err != nil {
			errs = append(
				errs,
				fmt.Errorf("restore original blob store: %w", err),
			)
		}
	}
	if r.metadataMutated && r.metadataPath != "" {
		_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
		rollbackCtx := metadata.AllowResetOfPopulatedTarget(ctx)
		if err := restoreMetadataStore(
			rollbackCtx,
			host,
			manifest,
			r.metadataPath,
			targetDataDir,
			storageConfig.Metadata,
			nil,
		); err != nil {
			errs = append(
				errs, fmt.Errorf("restore original metadata store: %w", err),
			)
		}
	}
	if len(errs) == 0 && r.cloudBlob {
		if err := selectBlobNamespace(targetDataDir, r.oldBlobNamespace); err != nil {
			errs = append(errs, fmt.Errorf("restore original blob namespace marker: %w", err))
		}
	}
	return errors.Join(errs...)
}

func resetBlobStore(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	targetDataDir string,
	providerConfig map[string]any,
) error {
	store, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		manifest.BlobPlugin,
		providerConfig,
		blob.ProviderDependencies{DataDir: targetDataDir, RunMode: "load"},
	)
	if err != nil {
		return fmt.Errorf("resolve blob plugin %q: %w", manifest.BlobPlugin, err)
	}
	resettable, ok := store.(blob.Resettable)
	if !ok {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return fmt.Errorf("blob plugin %q does not support reset", manifest.BlobPlugin)
	}
	resetErr := resettable.Reset(ctx)
	stopErr := stopBlobCapabilityUncancelable(ctx, host)
	if resetErr != nil {
		return fmt.Errorf("reset blob plugin %q: %w", manifest.BlobPlugin, resetErr)
	}
	if stopErr != nil {
		return fmt.Errorf("stop blob plugin after reset: %w", stopErr)
	}
	return nil
}

type restoreRecoveryRecord struct {
	OperationID      string `json:"operationId"`
	BlobPlugin       string `json:"blobPlugin"`
	MetadataPlugin   string `json:"metadataPlugin"`
	StorageMode      string `json:"storageMode"`
	NewBlobNamespace string `json:"newBlobNamespace,omitempty"`
	OldBlobNamespace string `json:"oldBlobNamespace,omitempty"`
	CloudBlob        bool   `json:"cloudBlob"`
	MetadataMutated  bool   `json:"metadataMutated"`
}

func (r *restoreRollback) persist(manifest Manifest) error {
	if err := syncDirTree(r.dir); err != nil {
		return fmt.Errorf("sync restore recovery directory: %w", err)
	}
	record := restoreRecoveryRecord{
		OperationID:      r.operationID,
		BlobPlugin:       manifest.BlobPlugin,
		MetadataPlugin:   manifest.MetadataPlugin,
		StorageMode:      manifest.StorageMode,
		NewBlobNamespace: r.newBlobNamespace,
		OldBlobNamespace: r.oldBlobNamespace,
		CloudBlob:        r.cloudBlob,
		MetadataMutated:  r.metadataPath != "",
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode restore recovery record: %w", err)
	}
	tmp, err := os.CreateTemp(r.dir, restoreRecoveryFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("create restore recovery record: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set restore recovery record permissions: %w", err)
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write restore recovery record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync restore recovery record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close restore recovery record: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(r.dir, restoreRecoveryFileName)); err != nil {
		return fmt.Errorf("activate restore recovery record: %w", err)
	}
	if err := syncDir(r.dir); err != nil {
		return fmt.Errorf("sync restore recovery record directory: %w", err)
	}
	if err := syncDir(filepath.Dir(r.dir)); err != nil {
		return fmt.Errorf("sync restore recovery parent directory: %w", err)
	}
	return nil
}

func (r *restoreRollback) removeDir() error {
	if !r.dirCleanupAttempted {
		r.dirCleanupAttempted = true
		if r.dirHandle == nil {
			r.dirCleanupErr = fmt.Errorf(
				"%w: restore rollback directory has no retained owner",
				errRestoreDirReplaced,
			)
		} else {
			r.dirCleanupErr = r.dirHandle.cleanup()
		}
		r.dirHandle = nil
	}
	if r.dirCleanupErr != nil {
		return r.dirCleanupErr
	}
	var errs []error
	if r.durable {
		if err := syncDir(filepath.Dir(r.dir)); err != nil {
			errs = append(errs, err)
		}
		if r.operationID != "" {
			if err := removeRestoreRecoveryMarkers(
				r.markerDirs, r.operationID,
			); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (r *restoreRollback) authenticate() error {
	if !r.metadataMutated && !r.blobMutated {
		return nil
	}
	for _, dir := range r.markerDirs {
		path := filepath.Join(dir, restoreRecoveryMarkerName)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(file, 129))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(data) > 128 {
			continue
		}
		if strings.TrimSpace(string(data)) == r.operationID {
			return nil
		}
	}
	return errors.New(
		"restore recovery record is not bound to the active restore operation",
	)
}

func readRestoreRecovery(dir string) (restoreRollback, Manifest, error) {
	owned, err := openOwnedRestoreDir(dir)
	if err != nil {
		return restoreRollback{}, Manifest{}, err
	}
	rollback := restoreRollback{dir: dir, dirHandle: owned}
	file, err := owned.dir.root.Open(restoreRecoveryFileName)
	if err != nil {
		return rollback, Manifest{}, err
	}
	defer file.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return rollback, Manifest{}, fmt.Errorf(
			"read restore recovery record: %w", err,
		)
	}
	if len(data) > 4096 {
		return rollback, Manifest{}, errors.New(
			"restore recovery record exceeds 4096 bytes",
		)
	}
	var record restoreRecoveryRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return rollback, Manifest{}, fmt.Errorf(
			"decode restore recovery record: %w", err,
		)
	}
	if record.BlobPlugin == "" || record.MetadataPlugin == "" {
		return rollback, Manifest{}, errors.New(
			"restore recovery record is missing provider names",
		)
	}
	if record.MetadataMutated || record.CloudBlob {
		operationID, err := hex.DecodeString(record.OperationID)
		if err != nil || len(operationID) != restoreRecoveryIDBytes {
			return rollback, Manifest{}, errors.New(
				"restore recovery record has an invalid operation ID",
			)
		}
	}
	rollback.metadataPath = filepath.Join(dir, MetadataBackupFileName)
	rollback.newBlobNamespace = record.NewBlobNamespace
	rollback.oldBlobNamespace = record.OldBlobNamespace
	rollback.cloudBlob = record.CloudBlob
	rollback.metadataMutated = record.MetadataMutated
	rollback.blobMutated = record.CloudBlob
	rollback.durable = true
	rollback.operationID = record.OperationID
	manifest := Manifest{
		BlobPlugin:     record.BlobPlugin,
		MetadataPlugin: record.MetadataPlugin,
		StorageMode:    record.StorageMode,
	}
	return rollback, manifest, nil
}

// RecoverInterruptedRestore disposes a durable live-restore recovery record.
// accept keeps the restored external stores and reclaims the prior blob
// namespace. Otherwise both external stores are returned to their prior state.
func RecoverInterruptedRestore(
	ctx context.Context,
	host *plugin.Host,
	recoveryDir string,
	targetDataDir string,
	storageConfig RestoreStorageConfig,
	accept bool,
) error {
	rollback, manifest, err := readRestoreRecovery(recoveryDir)
	defer func() {
		if rollback.dirHandle != nil {
			_ = rollback.dirHandle.close()
		}
	}()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if rollback.dirHandle != nil {
				if removeErr := rollback.removeDir(); removeErr != nil {
					return fmt.Errorf(
						"remove incomplete restore recovery directory: %w",
						removeErr,
					)
				}
			}
			// A marker left without a record, or without its recovery
			// directory, belongs to no restore and would block the next
			// marker write.
			if markerErr := removeRestoreRecoveryMarkers(
				restoreRecoveryMarkerDirs(storageConfig, targetDataDir), "",
			); markerErr != nil {
				return fmt.Errorf(
					"remove incomplete restore recovery marker: %w",
					markerErr,
				)
			}
			return nil
		}
		return err
	}
	rollback.markerDirs = restoreRecoveryMarkerDirs(storageConfig, targetDataDir)
	// Only an external-store reset needs the marker binding; accepting a
	// completed restore just disposes of the record.
	if !accept {
		if err := rollback.authenticate(); err != nil {
			return err
		}
	}
	if accept {
		err = rollback.removeOriginalBlobNamespace(
			context.WithoutCancel(ctx), host, manifest, storageConfig,
		)
	} else {
		err = rollback.restore(
			context.WithoutCancel(ctx),
			host,
			manifest,
			targetDataDir,
			storageConfig,
		)
	}
	if err != nil {
		return err
	}
	if err := rollback.removeDir(); err != nil {
		return fmt.Errorf("remove restore recovery directory: %w", err)
	}
	return nil
}

func (r *restoreRollback) removeOriginalBlobNamespace(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	storageConfig RestoreStorageConfig,
) error {
	if !r.cloudBlob || r.oldBlobNamespace == "" {
		return nil
	}
	_ = stopBlobCapabilityUncancelable(ctx, host)
	selectErr := selectBlobNamespace(r.dir, r.oldBlobNamespace)
	if selectErr != nil && !blob.NamespacePublished(selectErr) {
		return fmt.Errorf("select original blob namespace: %w", selectErr)
	}
	if err := resetBlobStore(ctx, host, manifest, r.dir, storageConfig.Blob); err != nil {
		return errors.Join(
			selectErr,
			fmt.Errorf("remove original blob namespace: %w", err),
		)
	}
	return selectErr
}

func selectBlobNamespace(dir, namespace string) error {
	if namespace != "" {
		return blob.WriteNamespace(dir, namespace)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create blob namespace directory: %w", err)
	}
	err := os.Remove(filepath.Join(dir, blob.NamespaceFileName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove blob namespace marker: %w", err)
	}
	return syncDir(dir)
}

// restoreMetadataStore resolves the manifest-recorded metadata plugin
// against targetDataDir just long enough to construct it (plugin.Resolve
// always constructs and starts a provider together — there is no
// construct-only step), then stops it immediately and undoes whatever it
// created, since metadata.Restorer's contract requires RestoreFrom to run
// before the store has ever been started, or after Close() — which
// StopCapability guarantees here.
//
// "Undo" is two different operations depending on the backend. For a
// file-based store (sqlite/badger), deleting targetDataDir is enough: that
// directory is the entirety of what the brief resolve-and-start touched.
// For a live client/server store (postgres/mysql), that brief start
// instead ran real migrations against the actual configured remote
// database — a directory wipe does nothing to that (confirmed via a live
// end-to-end restore attempt against a real Postgres server, which failed
// because the migrated-but-otherwise-empty database no longer looked
// "empty" to RestoreFrom's own precondition check). Resettable-implementing
// stores get an explicit Reset call for this, and it must happen before
// StopCapability: Reset needs the still-open connection pool, which
// StopCapability closes.
func restoreMetadataStore(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	backupPath string,
	targetDataDir string,
	providerConfig map[string]any,
	target *restoreDir,
) error {
	store, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		manifest.MetadataPlugin,
		providerConfig,
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
	if resettable, ok := store.(metadata.Resettable); ok {
		if err := resettable.Reset(ctx); err != nil {
			_ = host.StopCapability(ctx, plugin.CapabilityStorageMetadata)
			return fmt.Errorf(
				"reset metadata plugin %q before restore: %w",
				manifest.MetadataPlugin,
				err,
			)
		}
	}
	if err := host.StopCapability(ctx, plugin.CapabilityStorageMetadata); err != nil {
		return fmt.Errorf("stop metadata plugin before restore: %w", err)
	}
	var ownedParent *os.Root
	if target == nil {
		parentPath := filepath.Dir(targetDataDir)
		if err := os.MkdirAll(parentPath, 0o755); err != nil {
			return fmt.Errorf("create metadata restore parent: %w", err)
		}
		ownedParent, err = os.OpenRoot(parentPath)
		if err != nil {
			return fmt.Errorf("open metadata restore parent: %w", err)
		}
		defer ownedParent.Close() //nolint:errcheck // cleanup errors are returned below
		name := filepath.Base(filepath.Clean(targetDataDir))
		if _, statErr := ownedParent.Lstat(name); errors.Is(statErr, os.ErrNotExist) {
			ownedTarget, createErr := createOwnedRestoreDirAt(targetDataDir)
			if createErr != nil {
				return fmt.Errorf("create metadata restore directory: %w", createErr)
			}
			target = ownedTarget.dir
			defer ownedTarget.close() //nolint:errcheck // restore error is returned below
		} else if statErr != nil {
			return fmt.Errorf("inspect metadata restore directory: %w", statErr)
		} else {
			target, err = openRestoreDir(
				ownedParent,
				name,
				targetDataDir,
			)
			if err != nil {
				return fmt.Errorf("open metadata restore directory: %w", err)
			}
			defer target.closeRoot() //nolint:errcheck // cleanup errors are returned below
		}
	}
	// RestoreFrom refuses to overwrite an existing destination -- undo
	// whatever the brief resolve-and-start above wrote to targetDataDir so
	// it looks exactly as empty as before that call. Harmless no-op for a
	// live client/server store, which never wrote real data under
	// targetDataDir in the first place.
	if err := target.removeContents(); err != nil {
		return fmt.Errorf(
			"reset target data directory %q: %w", targetDataDir, err,
		)
	}
	if err := restorer.RestoreFrom(ctx, backupPath); err != nil {
		return fmt.Errorf("restore metadata store: %w", err)
	}
	return nil
}

// restoreBlobStore starts the blob plugin against an empty targetDataDir
// and loads the backup into it, per blob.Restorer's contract.
//
// RunMode is set to "load" so providers that run a periodic background
// GC (e.g. Badger's value-log GC ticker) skip starting it. Badger's own
// docs require Load to be the only thing operating on the DB -- no
// concurrent reads, writes, or GC -- and a large restore can run for
// long enough that a normally-scheduled GC tick would otherwise land
// mid-Load.
func restoreBlobStore(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	backupPath string,
	targetDataDir string,
	providerConfig map[string]any,
	replacePopulated bool,
) error {
	store, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		manifest.BlobPlugin,
		providerConfig,
		blob.ProviderDependencies{DataDir: targetDataDir, RunMode: "load"},
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
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return fmt.Errorf(
			"blob plugin %q does not support restore",
			manifest.BlobPlugin,
		)
	}
	if replacePopulated {
		resettable, ok := store.(blob.Resettable)
		if !ok {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return fmt.Errorf(
				"blob plugin %q does not support replacing a populated store",
				manifest.BlobPlugin,
			)
		}
		if err := resettable.Reset(ctx); err != nil {
			_ = stopBlobCapabilityUncancelable(ctx, host)
			return fmt.Errorf(
				"reset blob plugin %q before restore: %w",
				manifest.BlobPlugin, err,
			)
		}
	}
	backupFile, err := os.Open(backupPath)
	if err != nil {
		_ = stopBlobCapabilityUncancelable(ctx, host)
		return fmt.Errorf("open %q: %w", backupPath, err)
	}
	restoreErr := restorer.Restore(ctx, backupFile)
	closeErr := backupFile.Close()
	stopErr := stopBlobCapabilityUncancelable(ctx, host)
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

// rebuildRestoredDeferredIndexes repairs the staged metadata copy's deferred
// indexes before the restore activates it.
//
// The build runs on ctx where the store supports it
// (metadata.ContextDeferredIndexBuilder): a restore is cancellable, and an
// uninterruptible rebuild would outlive the cancellation by however long the
// largest index takes. It is still a staging-directory operation either way,
// so targetDataDir is untouched whichever path runs.
func rebuildRestoredDeferredIndexes(
	ctx context.Context,
	manager metadata.DeferredIndexManager,
	logger *slog.Logger,
) error {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	var missing []string
	var listErr error
	if lister, ok := manager.(metadata.ContextMissingDeferredIndexLister); ok {
		missing, listErr = lister.MissingDeferredIndexesContext(ctx)
	} else if lister, ok := manager.(metadata.MissingDeferredIndexLister); ok {
		missing, listErr = lister.MissingDeferredIndexes()
	}
	if listErr != nil {
		logger.Warn(
			"could not list missing deferred metadata indexes in the "+
				"restored database; rebuilding without naming them",
			"error", listErr,
		)
	} else if len(missing) > 0 {
		logger.Info(
			"rebuilding missing deferred metadata indexes in the "+
				"restored database",
			"indexes", strings.Join(missing, ","),
			"count", len(missing),
		)
	}
	start := time.Now()
	var err error
	if builder, ok := manager.(metadata.DeferredIndexProgressBuilder); ok {
		err = builder.BuildDeferredIndexesContextWithProgress(
			ctx,
			func(index string) {
				logger.Info("building deferred metadata index", "index", index)
			},
			func(index string, elapsed time.Duration) {
				logger.Info(
					"deferred metadata index ready",
					"index", index,
					"duration", elapsed,
				)
			},
		)
		if err != nil {
			return fmt.Errorf("restore deferred metadata indexes: %w", err)
		}
		logger.Info(
			"deferred metadata index repair complete in the restored database",
			"duration", time.Since(start),
		)
		return nil
	}
	if builder, ok := manager.(metadata.ContextDeferredIndexBuilder); ok {
		err = builder.BuildDeferredIndexesContext(ctx)
	} else {
		err = manager.BuildDeferredIndexes()
	}
	if err != nil {
		return fmt.Errorf("restore deferred metadata indexes: %w", err)
	}
	logger.Info(
		"deferred metadata index repair complete in the restored database",
		"duration", time.Since(start),
	)
	return nil
}

// validateRestoredDatabase opens the restored store the same way a normal
// dingo startup would, letting database.New's own CheckNodeSettings and
// checkCommitTimestamp checks validate internal consistency, then confirms
// the restored tip matches what the manifest recorded before closing it.
func validateRestoredDatabase(
	ctx context.Context,
	host *plugin.Host,
	manifest Manifest,
	targetDataDir string,
	storageConfig RestoreStorageConfig,
) error {
	blobStore, err := plugin.Resolve[blob.BlobStore](
		ctx,
		host,
		plugin.CapabilityStorageBlob,
		manifest.BlobPlugin,
		storageConfig.Blob,
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
		storageConfig.Metadata,
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
	if manager, ok := metadataStore.(metadata.DeferredIndexManager); ok {
		if err := rebuildRestoredDeferredIndexes(
			ctx,
			manager,
			storageConfig.Logger,
		); err != nil {
			return err
		}
	}
	db, err := database.New(ctx, &database.Config{
		DataDir:                   targetDataDir,
		StorageMode:               manifest.StorageMode,
		Network:                   manifest.Network,
		AlonzoLovelacePerUtxoWord: storageConfig.AlonzoLovelacePerUtxoWord,
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
