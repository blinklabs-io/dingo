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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

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
// independently consistent as of whenever it runs — but the two calls
// still happen one after the other, so without synchronization a commit
// landing in between would write its commit timestamp to one store's
// backup and not the other's, and the restored copy would fail its
// cross-store consistency check. Snapshot closes that window with
// Database.PauseCommits, which blocks new read-write Txns from being
// constructed (not reads, and not a quiesce — nothing is torn down or
// disconnected) for just the two backup calls. This is safe to call
// against a database a live node is actively writing to.
//
// dingoVersion is recorded in the manifest for cross-version restore
// detection; pass the running binary's version string.
func Snapshot(
	ctx context.Context,
	db *database.Database,
	dir string,
	trigger string,
	dingoVersion string,
) (m Manifest, err error) {
	blobBackuper, ok := db.Blob().(blob.Backuper)
	if !ok {
		return Manifest{}, fmt.Errorf(
			"blob plugin %q does not support snapshotting",
			db.Config().BlobPlugin,
		)
	}
	metadataBackuper, ok := db.Metadata().(metadata.Backuper)
	if !ok {
		return Manifest{}, fmt.Errorf(
			"metadata plugin %q does not support snapshotting",
			db.Config().MetadataPlugin,
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
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create snapshot parent directory %q: %w", filepath.Dir(dir), err,
		)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return Manifest{}, fmt.Errorf(
				"snapshot directory %q already exists", dir,
			)
		}
		return Manifest{}, fmt.Errorf(
			"create snapshot directory %q: %w", dir, err,
		)
	}
	// Best-effort cleanup so a failed snapshot doesn't permanently block
	// retrying at the same path with an "already exists" error. Safe from
	// this point on: the Mkdir above guarantees this call exclusively
	// created dir, so nothing else can be concurrently writing into it.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()

	blobPath := filepath.Join(dir, BlobBackupFileName)
	blobFile, err := os.OpenFile(
		blobPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o644,
	)
	if err != nil {
		return Manifest{}, fmt.Errorf("create %q: %w", blobPath, err)
	}

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
	logger := db.Logger()
	pauseStart := time.Now()
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
	resume, err := db.PauseCommitsContext(ctx)
	if err != nil {
		return Manifest{}, fmt.Errorf("pause commits: %w", err)
	}
	tip, tipErr := db.GetTip(nil)
	commitTimestamp, commitTimestampErr := db.Metadata().GetCommitTimestamp()

	var backupErr, closeErr, metadataErr error
	var backupWG sync.WaitGroup
	backupWG.Add(2)
	go func() {
		defer backupWG.Done()
		backupErr = blobBackuper.Backup(ctx, blobFile)
		closeErr = blobFile.Close()
	}()
	go func() {
		defer backupWG.Done()
		metadataErr = metadataBackuper.BackupTo(ctx, metadataPath)
	}()
	backupWG.Wait()

	resume()
	if logger != nil {
		logger.Debug(
			"resumed commits after snapshot backup",
			"component", "database",
			"dir", dir,
			"paused_for", time.Since(pauseStart),
		)
	}

	if tipErr != nil {
		return Manifest{}, fmt.Errorf("get tip: %w", tipErr)
	}
	if commitTimestampErr != nil {
		return Manifest{}, fmt.Errorf(
			"get commit timestamp: %w",
			commitTimestampErr,
		)
	}
	if backupErr != nil {
		return Manifest{}, fmt.Errorf("backup blob store: %w", backupErr)
	}
	if closeErr != nil {
		return Manifest{}, fmt.Errorf("close %q: %w", blobPath, closeErr)
	}
	if metadataErr != nil {
		return Manifest{}, fmt.Errorf("backup metadata store: %w", metadataErr)
	}

	blobInfo, err := os.Stat(blobPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("stat %q: %w", blobPath, err)
	}
	metadataInfo, err := os.Stat(metadataPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("stat %q: %w", metadataPath, err)
	}

	manifest := Manifest{
		CreatedAt:       time.Now().UTC(),
		Trigger:         trigger,
		StorageMode:     db.Config().StorageMode,
		Network:         db.Config().Network,
		CommitTimestamp: commitTimestamp,
		TipSlot:         tip.Point.Slot,
		TipHash:         tip.Point.Hash,
		TipBlockNumber:  tip.BlockNumber,
		BlobPlugin:      db.Config().BlobPlugin,
		MetadataPlugin:  db.Config().MetadataPlugin,
		DingoVersion:    dingoVersion,
		BlobBytes:       blobInfo.Size(),
		MetadataBytes:   metadataInfo.Size(),
	}
	if err := WriteManifest(dir, manifest); err != nil {
		return Manifest{}, err
	}
	// WriteManifest computes the checksum (and fills in FormatVersion) on
	// its own local copy of manifest, passed by value — the caller's copy
	// here is never updated, so re-read what was actually written rather
	// than return a Manifest whose Checksum/FormatVersion don't match the
	// file this function just produced.
	written, err := ReadManifest(dir)
	if err != nil {
		return Manifest{}, fmt.Errorf("re-read manifest after write: %w", err)
	}
	return written, nil
}
