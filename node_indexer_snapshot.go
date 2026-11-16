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

package dingo

import (
	"context"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/plugin"
)

const (
	// IndexerPayloadFileName is the IndexerDB's SQLite backup in a snapshot
	// directory.
	IndexerPayloadFileName = dblifecycle.IndexerPayloadFileName
	// IndexerPayloadManifestName describes the backup: its digest, size and
	// the IndexerDB tip it holds.
	IndexerPayloadManifestName = dblifecycle.IndexerPayloadManifestName
)

type indexerPayload = dblifecycle.IndexerPayload

// checkIndexerSnapshotSupported refuses the snapshot and restore shapes an
// IndexerDB payload does not cover.
func (n *Node) checkIndexerSnapshotSupported() error {
	return dblifecycle.CheckIndexerPayloadSupported(
		n.config.pluginSelections[plugin.CapabilityStorageMetadata].Provider,
		len(n.config.indexerMetadataConfig) != 0,
		n.config.databaseLifecycle.SnapshotTrustKeyFile,
	)
}

// captureIndexerPayload backs up the IndexerDB into a private directory under
// the data directory. The writer keeps running: the backup is a consistent
// point in time, and the ledger is captured after it.
func (n *Node) captureIndexerPayload(
	ctx context.Context,
) (string, indexerPayload, func(), error) {
	return dblifecycle.CaptureIndexerPayload(
		ctx, n.indexerDB, n.indexerStore, n.config.dataDir,
	)
}

// indexerPayloadSource is the IndexerDB capture the scheduled snapshot
// manager runs on the split layout.
func (n *Node) indexerPayloadSource() *dblifecycle.IndexerPayloadSource {
	if !n.splitIndexer() {
		return nil
	}
	return &dblifecycle.IndexerPayloadSource{
		Check:   n.checkIndexerSnapshotSupported,
		Capture: n.captureIndexerPayload,
	}
}

// restoreIndexerPayload copies the snapshot's IndexerDB backup into the
// staged data directory, where the swap publishes it with the ledger store.
func restoreIndexerPayload(
	snapshotDir, stagingDir string,
	payload indexerPayload,
	ledger lifecycle.Manifest,
) error {
	return dblifecycle.RestoreIndexerPayload(
		snapshotDir, stagingDir, payload, ledger,
	)
}
