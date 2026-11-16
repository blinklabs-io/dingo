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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
)

// An IndexerDB travels beside a ledger snapshot as two extra files, kept apart
// from the ledger manifest and its digests so a snapshot of the shared layout
// stays exactly what it was.
const (
	// IndexerPayloadFileName is the IndexerDB's SQLite backup in a snapshot
	// directory.
	IndexerPayloadFileName = "indexer.sqlite"
	// IndexerPayloadManifestName describes the backup: its digest, size and
	// the IndexerDB tip it holds.
	IndexerPayloadManifestName = "indexer.json"

	// indexerRestoredFileName is the name the SQLite metadata provider gives
	// its database file inside the IndexerDB directory.
	indexerRestoredFileName = "metadata.sqlite"
)

// IndexerPayload is the content of IndexerPayloadManifestName.
type IndexerPayload struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	// TipSlot and TipHash are the IndexerDB tip read before the backup
	// started. The writer never indexes past the ledger's tip, and the ledger
	// is captured after the IndexerDB, so the ledger manifest's tip is at or
	// past this slot.
	TipSlot uint64 `json:"tipSlot"`
	TipHash string `json:"tipHash"`
}

// IndexerPayloadSource captures the IndexerDB of a running node for a ledger
// snapshot, and refuses the configurations the payload does not cover.
type IndexerPayloadSource struct {
	// Check returns an error for a configuration whose snapshots cannot carry
	// or restore the payload.
	Check func() error
	// Capture backs the IndexerDB up and returns the staged file, its
	// description and a cleanup that removes the staging area.
	Capture func(
		ctx context.Context,
	) (staged string, payload IndexerPayload, cleanup func(), err error)
}

// CheckIndexerPayloadSupported refuses the snapshot and restore shapes an
// IndexerDB payload does not cover: a store that is not a SQLite directory
// under the data directory, and a manifest trust key, which authenticates the
// ledger manifest and would not cover the payload.
func CheckIndexerPayloadSupported(
	metadataProvider string,
	hasIndexerMetadataOverride bool,
	trustKeyFile string,
) error {
	if hasIndexerMetadataOverride {
		return errors.New(
			"snapshot and restore with indexer.layout split need the " +
				"IndexerDB in its default directory: remove indexer.metadata",
		)
	}
	if metadataProvider != "sqlite" {
		return errors.New(
			"snapshot and restore with indexer.layout split support the " +
				"sqlite metadata provider only",
		)
	}
	if trustKeyFile != "" {
		return errors.New(
			"snapshot and restore with indexer.layout split cannot be " +
				"combined with a snapshot trust key: the key authenticates " +
				"the ledger manifest and not the IndexerDB payload",
		)
	}
	return nil
}

// CheckIndexerSnapshotDestination refuses a cloud destination: the payload is
// written beside the local snapshot only.
func CheckIndexerSnapshotDestination(cloudDestination string) error {
	if cloudDestination != "" {
		return errors.New(
			"snapshot with indexer.layout split cannot be mirrored to a " +
				"cloud destination: the IndexerDB payload is local only",
		)
	}
	return nil
}

// CaptureIndexerPayload backs up the IndexerDB db into a private directory
// under stageParent and returns the staged file and its description. The
// backup is a consistent point in time; the ledger is captured after it.
func CaptureIndexerPayload(
	ctx context.Context,
	db *database.Database,
	store *indexerdb.Store,
	stageParent string,
) (stagedPath string, payload IndexerPayload, cleanup func(), err error) {
	cleanup = func() {}
	if db == nil || store == nil {
		return "", payload, cleanup, errors.New("indexer database is not open")
	}
	backuper, ok := db.Metadata().(metadata.Backuper)
	if !ok {
		return "", payload, cleanup, errors.New(
			"the IndexerDB metadata store does not support snapshotting",
		)
	}
	stage, err := os.MkdirTemp(stageParent, ".indexer-snapshot-")
	if err != nil {
		return "", payload, cleanup, fmt.Errorf(
			"create IndexerDB snapshot staging directory: %w", err,
		)
	}
	cleanup = func() { _ = os.RemoveAll(stage) }
	stagedPath = filepath.Join(stage, IndexerPayloadFileName)
	// A writer batch can commit while the backup runs, so the tip read before
	// it may not be the backup's. The tip only moves forward between reads,
	// which makes a tip equal on both sides of a backup the backup's own; a
	// moved tip means the backup is taken again.
	var tip indexerdb.Tip
	for attempt := 0; ; attempt++ {
		before, err := store.Tip(nil)
		if err != nil {
			cleanup()
			return "", payload, func() {}, fmt.Errorf(
				"read indexer tip: %w", err,
			)
		}
		_ = os.Remove(stagedPath)
		if err := backuper.BackupTo(ctx, stagedPath); err != nil {
			cleanup()
			return "", payload, func() {}, fmt.Errorf(
				"back up the IndexerDB: %w", err,
			)
		}
		after, err := store.Tip(nil)
		if err != nil {
			cleanup()
			return "", payload, func() {}, fmt.Errorf(
				"read indexer tip: %w", err,
			)
		}
		if before.Slot == after.Slot && bytes.Equal(before.Hash, after.Hash) {
			tip = after
			break
		}
		if attempt+1 >= payloadBackupAttempts {
			cleanup()
			return "", payload, func() {}, fmt.Errorf(
				"the IndexerDB tip moved during each of %d backups: "+
					"snapshot while the node is stopped",
				payloadBackupAttempts,
			)
		}
	}
	payload = IndexerPayload{
		TipSlot: tip.Slot,
		TipHash: hex.EncodeToString(tip.Hash),
	}
	return stagedPath, payload, cleanup, nil
}

// payloadBackupAttempts bounds how often a backup is retaken because the
// IndexerDB tip moved while it ran.
const payloadBackupAttempts = 3

// InstallIndexerPayload copies the staged backup into the snapshot directory
// and writes its description. Copying hashes the bytes written.
func InstallIndexerPayload(
	destDir, stagedPath string,
	payload IndexerPayload,
) error {
	digest, size, err := copyHashed(
		stagedPath, filepath.Join(destDir, IndexerPayloadFileName),
	)
	if err != nil {
		return fmt.Errorf("write the IndexerDB payload: %w", err)
	}
	payload.SHA256, payload.Bytes = digest, size
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := os.WriteFile(
		filepath.Join(destDir, IndexerPayloadManifestName), raw, 0o600,
	); err != nil {
		return fmt.Errorf("write the IndexerDB payload manifest: %w", err)
	}
	return nil
}

// copyHashed copies src to a new file dst, flushes it and returns the SHA-256
// and size of the bytes it wrote.
func copyHashed(src, dst string) (string, int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", 0, err
	}
	defer in.Close() //nolint:errcheck // read-only handle
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(out, h), in)
	if err == nil {
		err = out.Sync()
	}
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(dst)
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// ReadIndexerPayload loads and checks the description of the IndexerDB
// payload in a local snapshot directory.
func ReadIndexerPayload(snapshotDir string) (IndexerPayload, error) {
	var payload IndexerPayload
	if strings.Contains(snapshotDir, "://") {
		return payload, errors.New(
			"restore with indexer.layout split needs a local snapshot " +
				"directory that holds the IndexerDB payload",
		)
	}
	raw, err := os.ReadFile(
		filepath.Join(snapshotDir, IndexerPayloadManifestName),
	)
	if err != nil {
		return payload, fmt.Errorf(
			"the snapshot carries no IndexerDB payload (%s): take it with "+
				"a split node: %w",
			IndexerPayloadManifestName, err,
		)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf(
			"parse %s: %w",
			IndexerPayloadManifestName,
			err,
		)
	}
	if payload.SHA256 == "" {
		return payload, fmt.Errorf(
			"%s declares no digest", IndexerPayloadManifestName,
		)
	}
	return payload, nil
}

// RestoreIndexerPayload copies the snapshot's IndexerDB backup into the
// staged data directory stagingDir, where the swap publishes it with the
// ledger store. It refuses a payload whose digest differs from its
// description, or whose tip is past the ledger manifest's.
func RestoreIndexerPayload(
	snapshotDir, stagingDir string,
	payload IndexerPayload,
	ledger lifecycle.Manifest,
) error {
	if payload.TipSlot > ledger.TipSlot {
		return fmt.Errorf(
			"the IndexerDB payload is at slot %d, past the ledger "+
				"snapshot's slot %d: the snapshot is inconsistent",
			payload.TipSlot, ledger.TipSlot,
		)
	}
	dir := filepath.Join(stagingDir, internalplugins.IndexerDirName)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	digest, size, err := copyHashed(
		filepath.Join(snapshotDir, IndexerPayloadFileName),
		filepath.Join(dir, indexerRestoredFileName),
	)
	if err != nil {
		return fmt.Errorf("restore the IndexerDB payload: %w", err)
	}
	if size != payload.Bytes || !strings.EqualFold(digest, payload.SHA256) {
		_ = os.RemoveAll(dir)
		return fmt.Errorf(
			"%w: %s", lifecycle.ErrSnapshotPayloadMismatch,
			IndexerPayloadFileName,
		)
	}
	return nil
}
