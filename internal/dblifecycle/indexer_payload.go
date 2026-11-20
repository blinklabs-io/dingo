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
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// IndexerPayloadFileName is the provider-native IndexerDB snapshot backup.
const IndexerPayloadFileName = lifecycle.IndexerMetadataBackupFileName

// IndexerPayload describes a captured provider-native IndexerDB backup.
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

// CheckIndexerPayloadSupported validates the metadata selection used by snapshots.
func CheckIndexerPayloadSupported(metadataProvider string) error {
	switch metadataProvider {
	case "sqlite", "postgres", "mysql":
		return nil
	default:
		return fmt.Errorf("unsupported IndexerDB metadata provider %q", metadataProvider)
	}
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

// ReadIndexerPayload reads the authenticated manifest's IndexerDB description.
func ReadIndexerPayload(snapshotDir string) (IndexerPayload, error) {
	m, err := lifecycle.ReadManifest(snapshotDir)
	if err != nil {
		return IndexerPayload{}, err
	}
	if m.IndexerSHA256 == "" {
		return IndexerPayload{}, errors.New("the snapshot carries no IndexerDB payload")
	}
	return IndexerPayload{SHA256: m.IndexerSHA256, Bytes: m.IndexerBytes, TipSlot: m.IndexerTipSlot, TipHash: m.IndexerTipHash}, nil
}
