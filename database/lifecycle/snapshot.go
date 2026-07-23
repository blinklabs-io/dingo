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
// Database.PauseCommits, which blocks new Txn.Commit calls (not reads,
// and not a quiesce — nothing is torn down or disconnected) for just the
// two backup calls. This is safe to call against a database a live node
// is actively writing to.
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
	if _, statErr := os.Stat(dir); statErr == nil {
		return Manifest{}, fmt.Errorf(
			"snapshot directory %q already exists", dir,
		)
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return Manifest{}, fmt.Errorf("stat %q: %w", dir, statErr)
	}

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

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Manifest{}, fmt.Errorf(
			"create snapshot directory %q: %w", dir, err,
		)
	}
	// Best-effort cleanup so a failed snapshot doesn't permanently block
	// retrying at the same path with an "already exists" error.
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
	resume := db.PauseCommits()
	tip, tipErr := db.GetTip(nil)
	commitTimestamp, commitTimestampErr := db.Metadata().GetCommitTimestamp()
	backupErr := blobBackuper.Backup(ctx, blobFile)
	closeErr := blobFile.Close()
	var metadataErr error
	if backupErr == nil && closeErr == nil {
		metadataErr = metadataBackuper.BackupTo(ctx, metadataPath)
	}
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
