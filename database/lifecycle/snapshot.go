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
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// File synchronization failures cannot be induced reliably on a healthy
// filesystem. Keep this seam so publication failure tests exercise Snapshot.
var syncSnapshotFile = (*os.File).Sync

// BlobBackupFileName and MetadataBackupFileName are the fixed file names
// Snapshot writes a backup's blob and metadata stores under, inside the
// snapshot directory alongside manifest.json.
const (
	BlobBackupFileName     = "blob.bak"
	MetadataBackupFileName = "metadata.sqlite"
)

// Snapshot captures a point-in-time backup of db's blob and metadata
// stores into dir, which must not already exist, writing a manifest
// alongside them. Both stores are backed up via their native
// MVCC/versioned mechanism (blob.Backuper, metadata.Backuper), each
// independently consistent as of whenever it runs — but the two backup
// calls run concurrently (not sequentially: see the comment further down
// on why neither exposes a way to separate "capture a consistent point"
// from "stream/copy it"), so without synchronization a commit landing
// during either one's own window would write its commit timestamp to one
// store's backup and not the other's, and the restored copy would fail
// its cross-store consistency check. Snapshot closes that window with
// Database.PauseCommitsContext, which blocks new read-write Txns from
// being constructed (not reads, and not a quiesce — nothing is torn down
// or disconnected) while both backups capture their stores. A successful
// snapshot holds the barrier through the slower backup. An aborted snapshot
// releases it as soon as cancellation or a backup failure makes the result
// unusable, then waits for both providers before removing partial files.
// This is safe to call against a database a live node is actively writing to.
//
// The hold is observed in the dingo_snapshot_commit_pause_seconds histogram
// and can be bounded with WithMaxCommitPause.
//
// dingoVersion is recorded in the manifest for cross-version restore
// detection; pass the running binary's version string. blobPluginName and
// metadataPluginName are recorded in the manifest for Restore's later
// plugin-match check (Manifest.CheckPluginMatch) — db itself no longer
// knows which provider names resolved its injected Stores, so the caller
// (which does know, from its own plugin selection) must supply them.
func Snapshot(
	ctx context.Context,
	db *database.Database,
	dir string,
	trigger string,
	dingoVersion string,
	blobPluginName string,
	metadataPluginName string,
	opts ...ManifestOption,
) (m Manifest, err error) {
	if _, err := manifestByteLimit(opts); err != nil {
		return Manifest{}, err
	}
	maxPause, pauseNow, pauseContext, pauseDeadline, err := commitPauseConfig(opts)
	if err != nil {
		return Manifest{}, err
	}
	metrics, err := snapshotMetricsFor(db.Config().PromRegistry)
	if err != nil {
		return Manifest{}, err
	}
	// Pinned for the whole call: the Backup below runs long, and the store
	// backing blobBackuper must not be drained and closed while it does.
	blobStore, releaseBlob := db.PinBlob()
	defer releaseBlob()
	blobBackuper, ok := blobStore.(blob.Backuper)
	if !ok {
		return Manifest{}, fmt.Errorf(
			"blob plugin %q does not support snapshotting",
			blobPluginName,
		)
	}
	metadataBackuper, ok := db.Metadata().(metadata.Backuper)
	if !ok {
		return Manifest{}, fmt.Errorf(
			"metadata plugin %q does not support snapshotting",
			metadataPluginName,
		)
	}

	// dir's leaf component is created with a plain, non-recursive Mkdir
	// (checked for fs.ErrExist below), not MkdirAll, specifically so this
	// call can tell "I just created dir, and own it exclusively" apart
	// from "dir already existed" — including the case where a concurrent
	// Snapshot call to the same path won that race a moment earlier.
	// MkdirAll doesn't error on an existing directory, and an earlier
	// os.Stat-then-MkdirAll check has a TOCTOU gap between the two calls,
	// so either would let two concurrent callers both believe they own
	// dir; the failure-cleanup defer below then RemoveAlls it, deleting
	// the other (possibly still in-flight, possibly already-succeeded)
	// caller's backup files out from under it. Only the call that
	// actually wins Mkdir's exclusive creation may remove dir on failure.
	// Record missing ancestors before creation. A concurrent creator may
	// win a MkdirAll race; synchronizing its entry as well is harmless.
	var newParents []string
	for parent := filepath.Dir(dir); ; parent = filepath.Dir(parent) {
		if _, err := os.Stat(parent); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return Manifest{}, fmt.Errorf("stat snapshot parent %q: %w", parent, err)
		}
		newParents = append(newParents, parent)
		if filepath.Dir(parent) == parent {
			break
		}
	}
	parentDir := filepath.Dir(dir)
	if err := requirePrivateSnapshotParent(parentDir); err != nil {
		return Manifest{}, err
	}
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create snapshot parent directory %q: %w", parentDir, err,
		)
	}
	if err := requirePrivateSnapshotParent(parentDir); err != nil {
		return Manifest{}, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return Manifest{}, fmt.Errorf(
				"snapshot directory %q already exists", dir,
			)
		}
		return Manifest{}, fmt.Errorf(
			"create snapshot directory %q: %w", dir, err,
		)
	}
	snapshotRoot, err := os.OpenRoot(dir)
	if err != nil {
		return Manifest{}, fmt.Errorf("open snapshot directory %q: %w", dir, err)
	}
	defer snapshotRoot.Close() //nolint:errcheck // the directory handle has no buffered data
	// Mkdir and OpenRoot are separate operations. Confirm the handle still
	// names the directory we created before writing any backup bytes through
	// it; after this check the handle pins all writes even if the path moves.
	if err := snapshotRootMatchesPath(snapshotRoot, dir); err != nil {
		return Manifest{}, err
	}
	// Best-effort cleanup so a failed snapshot doesn't permanently block
	// retrying at the same path with an "already exists" error. Safe from
	// this point on: the Mkdir above guarantees this call exclusively
	// created dir, so nothing else can be concurrently writing into it.
	defer func() {
		if err != nil && !errors.Is(err, ErrSnapshotCatalogUpdate) {
			if snapshotRootMatchesPath(snapshotRoot, dir) == nil {
				_ = RemoveSnapshotContext(context.WithoutCancel(ctx), dir)
			} else {
				removeSnapshotRootContents(snapshotRoot)
			}
		}
	}()
	if err := restrictPrivateSnapshotDirectory(snapshotRoot); err != nil {
		return Manifest{}, fmt.Errorf(
			"restrict snapshot directory %q: %w", dir, err,
		)
	}

	blobPath := filepath.Join(dir, BlobBackupFileName)
	blobFile, err := snapshotRoot.OpenFile(
		BlobBackupFileName,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return Manifest{}, fmt.Errorf("create %q: %w", blobPath, err)
	}
	// Close before the failure-cleanup defer removes dir. This matters when
	// an error occurs before the backup goroutine takes ownership below
	// (for example, a pre-cancelled context rejected by PauseCommitsContext):
	// Unix permits unlinking the still-open blob file, but Windows does not.
	defer blobFile.Close() //nolint:errcheck // the backup path records its close error below

	// Everything read or backed up between PauseCommits and resume()
	// describes the same set of committed writes: no new commit can land
	// in this window, so the tip/commitTimestamp read here matches
	// exactly what the two backup calls capture.
	//
	// The two backups run concurrently, not sequentially: badger's Backup
	// and SQLite's VACUUM INTO are each independently MVCC/WAL-consistent
	// as of whenever they start and don't themselves need writers
	// blocked, but neither exposes a way to capture "a consistent point"
	// separately from "stream/copy it" — Backup(ctx, w) and
	// BackupTo(ctx, path) each do both in one call. So the full duration
	// of whichever runs must still be covered by PauseCommits, or a
	// commit landing between the two backups' own snapshot moments could
	// be captured by one store's backup and not the other's, and the
	// restored copy would fail its cross-store consistency check. Running
	// them concurrently at least bounds the pause by the slower of the
	// two rather than their sum. A pause-free backup would need
	// Backuper's badger/SQLite implementations to separately expose
	// "capture a version now" from "stream that version" — a larger
	// rework of both, not attempted here.
	metadataPath := filepath.Join(dir, MetadataBackupFileName)
	// BackupTo writes by path, so it cannot write through snapshotRoot.
	// Stage the backup in a private directory beside the snapshot, under
	// the parent validated above, and move it into the opened snapshot
	// directory afterwards. The system temp directory is not used: it may be
	// small, memory-backed, or on another volume, and the metadata backup is
	// as large as the metadata store.
	metadataStageDir, err := os.MkdirTemp(parentDir, ".dingo-snapshot-metadata-*")
	if err != nil {
		return Manifest{}, fmt.Errorf("create private metadata staging directory: %w", err)
	}
	defer os.RemoveAll(metadataStageDir) //nolint:errcheck // best-effort removal of private temporary data
	metadataStageRoot, err := os.OpenRoot(metadataStageDir)
	if err != nil {
		return Manifest{}, fmt.Errorf("open private metadata staging directory: %w", err)
	}
	if err := restrictPrivateSnapshotDirectory(metadataStageRoot); err != nil {
		_ = metadataStageRoot.Close()
		return Manifest{}, fmt.Errorf(
			"restrict private metadata staging directory: %w", err,
		)
	}
	if err := metadataStageRoot.Close(); err != nil {
		return Manifest{}, fmt.Errorf("close private metadata staging directory: %w", err)
	}
	metadataStagePath := filepath.Join(metadataStageDir, MetadataBackupFileName)
	if err := snapshotRootMatchesPath(snapshotRoot, dir); err != nil {
		return Manifest{}, err
	}
	logger := db.Logger()
	if logger != nil {
		logger.Debug(
			"pausing commits for snapshot backup",
			"component", "database",
			"dir", dir,
		)
	}
	// PauseCommitsContext, not PauseCommits: acquiring the exclusive side
	// can block for as long as any currently open write transaction takes
	// to commit, and this ctx is exactly what a caller cancels to give up
	// on a Snapshot call that's stuck waiting behind one.
	pauseCtx := ctx
	if pauseContext != nil {
		pauseCtx = pauseContext(ctx)
	}
	resume, err := db.PauseCommitsContext(pauseCtx)
	if err != nil {
		return Manifest{}, fmt.Errorf("pause commits: %w", err)
	}
	pauseStart := pauseNow()
	var pauseDuration time.Duration
	pauseExceeded := false
	barrierHeld := true
	releaseBarrier := func() {
		if barrierHeld {
			resume()
			pauseDuration = pauseNow().Sub(pauseStart)
			barrierHeld = false
		}
	}
	defer func() {
		result := snapshotResultOK
		switch {
		case pauseExceeded:
			result = snapshotResultExceeded
		case err != nil:
			result = snapshotResultFailed
		}
		metrics.pause.WithLabelValues(result).Observe(pauseDuration.Seconds())
	}()
	defer releaseBarrier()
	// The limit starts once the barrier is held; backupCtx is cancelled by
	// the caller's ctx as well, so cancellation behaves as before.
	backupCtx, cancelBackup := context.WithCancel(ctx)
	if maxPause > 0 {
		timeoutCtx, cancelTimeout := pauseDeadline(backupCtx, maxPause)
		cancelParent := cancelBackup
		cancelBackup = func() {
			cancelTimeout()
			cancelParent()
		}
		backupCtx = timeoutCtx
	}
	defer cancelBackup()
	type snapshotState struct {
		tip             ochainsync.Tip
		commitTimestamp int64
		gates           nodesettings.Values
		err             error
	}
	stateReady := make(chan snapshotState, 1)
	go func() {
		var state snapshotState
		tipTxn := db.MetadataTxn(backupCtx, false)
		state.tip, state.err = db.GetTip(tipTxn)
		tipTxn.Release()
		if state.err == nil && backupCtx.Err() == nil {
			state.commitTimestamp, state.err = db.Metadata().GetCommitTimestamp(backupCtx)
		}
		if state.err == nil && backupCtx.Err() == nil {
			state.gates, state.err = db.Metadata().GetNodeSettingsGates(backupCtx)
		}
		stateReady <- state
	}()
	var state snapshotState
	select {
	case state = <-stateReady:
	case <-backupCtx.Done():
		pauseExceeded = maxPause > 0 && ctx.Err() == nil
		if pauseExceeded {
			return Manifest{}, fmt.Errorf("%w (%s): %w", ErrCommitPauseExceeded, maxPause, backupCtx.Err())
		}
		return Manifest{}, backupCtx.Err()
	}
	if state.err != nil {
		if maxPause > 0 && ctx.Err() == nil &&
			errors.Is(backupCtx.Err(), context.DeadlineExceeded) {
			pauseExceeded = true
			return Manifest{}, fmt.Errorf(
				"read snapshot state: %w (%s): %w",
				ErrCommitPauseExceeded,
				maxPause,
				errors.Join(backupCtx.Err(), state.err),
			)
		}
		return Manifest{}, fmt.Errorf("read snapshot state: %w", state.err)
	}
	if err := backupCtx.Err(); err != nil {
		pauseExceeded = maxPause > 0 && ctx.Err() == nil
		if pauseExceeded {
			return Manifest{}, fmt.Errorf("%w (%s): %w", ErrCommitPauseExceeded, maxPause, err)
		}
		return Manifest{}, err
	}
	tip, commitTimestamp, gates := state.tip, state.commitTimestamp, state.gates

	type backupResult struct {
		store string
		err   error
	}
	backupResults := make(chan backupResult, 2)
	go func() {
		backupResults <- backupResult{
			store: "blob",
			err:   blobBackuper.Backup(backupCtx, blobFile),
		}
	}()
	go func() {
		backupResults <- backupResult{
			store: "metadata",
			err:   metadataBackuper.BackupTo(backupCtx, metadataStagePath),
		}
	}()
	var backupErr, metadataErr error
	backupCtxDone := backupCtx.Done()
	pauseLimitExpired := false
	for completed := 0; completed < 2; {
		select {
		case result := <-backupResults:
			completed++
			if result.store == "blob" {
				backupErr = result.err
			} else {
				metadataErr = result.err
			}
			if result.err != nil {
				// A failed store makes the cross-store snapshot unusable.
				// Cancel its peer and resume commits before waiting for it
				// to stop writing the partial backup.
				cancelBackup()
				releaseBarrier()
				backupCtxDone = nil
			}
		case <-backupCtxDone:
			// An aborted snapshot cannot be published. Provider cleanup
			// must not extend the node's commit pause.
			pauseLimitExpired = maxPause > 0 && ctx.Err() == nil &&
				errors.Is(backupCtx.Err(), context.DeadlineExceeded)
			cancelBackup()
			releaseBarrier()
			backupCtxDone = nil
		}
	}

	// A provider may return success despite ignoring cancellation. Measure
	// the barrier hold against the deadline rather than relying on its error.
	pauseExceeded = pauseLimitExpired || (maxPause > 0 && ctx.Err() == nil &&
		(errors.Is(backupCtx.Err(), context.DeadlineExceeded) || pauseDuration >= maxPause))
	cancelBackup()
	releaseBarrier()
	if logger != nil {
		logger.Debug(
			"resumed commits after snapshot backup",
			"component", "database",
			"dir", dir,
			"paused_for", pauseDuration,
		)
	}

	if pauseExceeded {
		return Manifest{}, fmt.Errorf(
			"%w (%s): %w",
			ErrCommitPauseExceeded, maxPause,
			errors.Join(context.DeadlineExceeded, backupErr, metadataErr),
		)
	}
	if backupErr != nil && !errors.Is(backupErr, context.Canceled) &&
		metadataErr != nil && !errors.Is(metadataErr, context.Canceled) {
		return Manifest{}, errors.Join(
			fmt.Errorf("backup blob store: %w", backupErr),
			fmt.Errorf("backup metadata store: %w", metadataErr),
		)
	}
	if backupErr != nil && !errors.Is(backupErr, context.Canceled) {
		return Manifest{}, fmt.Errorf("backup blob store: %w", backupErr)
	}
	if metadataErr != nil && !errors.Is(metadataErr, context.Canceled) {
		return Manifest{}, fmt.Errorf("backup metadata store: %w", metadataErr)
	}
	if backupErr != nil {
		return Manifest{}, fmt.Errorf("backup blob store: %w", backupErr)
	}
	if metadataErr != nil {
		return Manifest{}, fmt.Errorf("backup metadata store: %w", metadataErr)
	}
	if err := os.Chmod(metadataStagePath, 0o600); err != nil {
		return Manifest{}, fmt.Errorf("restrict %q: %w", metadataStagePath, err)
	}
	if err := snapshotRootMatchesPath(snapshotRoot, dir); err != nil {
		return Manifest{}, err
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	// Flush after releasing the commit barrier: both backup streams are
	// complete, so making their files durable needs no further writer pause.
	// Blob Backuper only writes to our io.Writer; neither backup interface
	// requires the provider to synchronize its output.
	if err := syncSnapshotFile(blobFile); err != nil {
		return Manifest{}, fmt.Errorf("sync %q: %w", blobPath, err)
	}
	if err := blobFile.Close(); err != nil {
		return Manifest{}, fmt.Errorf("close %q: %w", blobPath, err)
	}
	if err := installStagedSnapshotFile(
		ctx, snapshotRoot, metadataStageDir, MetadataBackupFileName,
	); err != nil {
		return Manifest{}, fmt.Errorf("install %q: %w", metadataPath, err)
	}
	metadataFile, err := snapshotRoot.OpenFile(MetadataBackupFileName, os.O_WRONLY, 0)
	if err != nil {
		return Manifest{}, fmt.Errorf("open %q for sync: %w", metadataPath, err)
	}
	metadataSyncErr := syncSnapshotFile(metadataFile)
	metadataCloseErr := metadataFile.Close()
	if err := errors.Join(metadataSyncErr, metadataCloseErr); err != nil {
		return Manifest{}, fmt.Errorf(
			"sync and close %q: %w",
			metadataPath,
			err,
		)
	}
	// Persist the backup names before a manifest can make them discoverable.
	if err := syncDir(dir); err != nil {
		return Manifest{}, fmt.Errorf("sync snapshot directory: %w", err)
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return Manifest{}, fmt.Errorf("sync snapshot parent: %w", err)
	}
	for _, parent := range newParents {
		if err := syncDir(filepath.Dir(parent)); err != nil {
			return Manifest{}, fmt.Errorf(
				"sync snapshot ancestor %q: %w",
				parent,
				err,
			)
		}
	}

	blobInfo, err := snapshotRoot.Stat(BlobBackupFileName)
	if err != nil {
		return Manifest{}, fmt.Errorf("stat %q: %w", blobPath, err)
	}
	metadataInfo, err := snapshotRoot.Stat(MetadataBackupFileName)
	if err != nil {
		return Manifest{}, fmt.Errorf("stat %q: %w", metadataPath, err)
	}

	blobDigest, err := hashSnapshotRootFile(ctx, snapshotRoot, BlobBackupFileName)
	if err != nil {
		return Manifest{}, fmt.Errorf("hash %q: %w", blobPath, err)
	}
	metadataDigest, err := hashSnapshotRootFile(ctx, snapshotRoot, MetadataBackupFileName)
	if err != nil {
		return Manifest{}, fmt.Errorf("hash %q: %w", metadataPath, err)
	}
	metrics.bytes.WithLabelValues("blob").Add(float64(blobInfo.Size()))
	metrics.bytes.WithLabelValues("metadata").Add(float64(metadataInfo.Size()))

	manifest := Manifest{
		CreatedAt:       time.Now().UTC(),
		Trigger:         trigger,
		StorageMode:     db.Config().StorageMode,
		Network:         db.Config().Network,
		CommitTimestamp: commitTimestamp,
		TipSlot:         tip.Point.Slot,
		TipHash:         tip.Point.Hash,
		TipBlockNumber:  tip.BlockNumber,
		BlobPlugin:      blobPluginName,
		MetadataPlugin:  metadataPluginName,
		Gates:           gates,
		DingoVersion:    dingoVersion,
		BlobBytes:       blobInfo.Size(),
		MetadataBytes:   metadataInfo.Size(),
		BlobSHA256:      blobDigest,
		MetadataSHA256:  metadataDigest,
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	written, err := writeManifestAtRoot(ctx, snapshotRoot, dir, manifest, opts...)
	if err != nil {
		return Manifest{}, err
	}
	return written, nil
}

func snapshotRootMatchesPath(root *os.Root, dir string) error {
	rootInfo, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("stat opened snapshot directory: %w", err)
	}
	pathInfo, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("%w: inspect %q: %w", ErrSnapshotDirectoryReplaced, dir, err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.IsDir() ||
		!os.SameFile(rootInfo, pathInfo) {
		return fmt.Errorf("%w: %q", ErrSnapshotDirectoryReplaced, dir)
	}
	return nil
}

func removeSnapshotRootContents(root *os.Root) {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return
	}
	for _, entry := range entries {
		_ = root.RemoveAll(entry.Name())
	}
}

// copyStagedSnapshotFile copies a staged file into the opened snapshot
// directory. It is the fallback when the staged file cannot be renamed into
// that directory by handle.
func copyStagedSnapshotFile(
	ctx context.Context,
	root *os.Root,
	stageDir string,
	name string,
) error {
	src, err := os.Open(filepath.Join(stageDir, name))
	if err != nil {
		return fmt.Errorf("open staged %q: %w", name, err)
	}
	defer src.Close() //nolint:errcheck // read errors are reported by io.Copy
	dst, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, &payloadContextReader{ctx: ctx, reader: src})
	return errors.Join(copyErr, dst.Close())
}

func hashSnapshotRootFile(
	ctx context.Context,
	root *os.Root,
	name string,
) (string, error) {
	file, err := root.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close() //nolint:errcheck // read error is reported by io.Copy
	hash := sha256.New()
	if _, err := io.Copy(hash, &payloadContextReader{ctx: ctx, reader: file}); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
