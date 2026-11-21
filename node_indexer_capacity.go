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
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// measuredMainnetIndexerBytes is the observed mainnet API metadata footprint
// documented in README.md. It is a warning threshold, not a storage limit.
const measuredMainnetIndexerBytes uint64 = 752 << 30

type indexerCapacityProbe func(string) (current, available uint64, err error)

func localIndexerCapacity(dataDir string) (uint64, uint64, error) {
	dir := filepath.Join(dataDir, internalplugins.IndexerDirName)
	var current uint64
	for _, name := range []string{
		"metadata.sqlite",
		"metadata.sqlite-wal",
		"metadata.sqlite-shm",
	} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, 0, fmt.Errorf("inspect IndexerDB capacity: %w", err)
		}
		if info.Size() <= 0 {
			continue
		}
		// FileInfo.Size is non-negative here.
		current += uint64(info.Size()) //nolint:gosec
	}
	available, err := utxoset.AvailableDiskBytes(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("inspect IndexerDB capacity: %w", err)
	}
	return current, available, nil
}

func (n *Node) warnMainnetIndexerCapacity(
	store *indexerdb.Store,
	probe indexerCapacityProbe,
) {
	if n.config.cfg == nil ||
		n.config.cfg.NetworkMagic != ouroboros.NetworkCardanoMainnet.NetworkMagic {
		return
	}
	provider := n.storageSelections().Metadata.Provider
	if provider != "" && provider != "sqlite" {
		return
	}
	phase, err := store.Bootstrap(nil)
	if err != nil || (phase != indexerdb.PhaseReplay && phase != indexerdb.PhaseIndex) {
		return
	}
	current, available, err := probe(n.config.dataDir)
	if err != nil {
		n.config.logger.Warn(
			"could not measure disk capacity for mainnet IndexerDB replay",
			"error", err,
		)
		return
	}
	var minimumAdditional uint64
	if current < measuredMainnetIndexerBytes {
		minimumAdditional = measuredMainnetIndexerBytes - current
	}
	if available >= minimumAdditional {
		return
	}
	n.config.logger.Warn(
		"mainnet IndexerDB replay may exhaust disk; add capacity and restart if it stops, and replay will resume from the last committed batch",
		"current_bytes", current,
		"available_bytes", available,
		"measured_indexer_bytes", measuredMainnetIndexerBytes,
		"minimum_additional_bytes", minimumAdditional,
	)
}
