// Copyright 2025 Blink Labs Software
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

package chain

import (
	"encoding/hex"
	"log/slog"
	"slices"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type chainBatchPosition struct {
	tip              ochainsync.Tip
	blockIndex       uint64
	generation       uint64
	headerGeneration uint64
}

type chainBatchSnapshot struct {
	chainBatchPosition
	headers []queuedHeader
	blocks  []ocommon.Point
}

// Callers hold both chain locks while capturing or restoring a snapshot.
func (c *Chain) batchPositionLocked() chainBatchPosition {
	return chainBatchPosition{
		tip:              c.currentTip,
		blockIndex:       c.tipBlockIndex,
		generation:       c.mutationGeneration,
		headerGeneration: c.headerMutationGeneration,
	}
}

func (c *Chain) batchSnapshotLocked() chainBatchSnapshot {
	snapshot := chainBatchSnapshot{
		chainBatchPosition: c.batchPositionLocked(),
		headers:            slices.Clone(c.headers),
	}
	if !c.persistent {
		snapshot.blocks = slices.Clone(c.blocks)
	}
	return snapshot
}

func (c *Chain) restoreBatchBlocksLocked(snapshot chainBatchSnapshot) {
	c.currentTip = snapshot.tip
	c.tipBlockIndex = snapshot.blockIndex
	c.mutationGeneration = snapshot.generation
	if !c.persistent {
		c.blocks = snapshot.blocks
	}
	c.withdrawalGeneration.Add(1)
}

func (c *Chain) restoreBatchHeadersLocked(snapshot chainBatchSnapshot) {
	c.headerMutationGeneration = snapshot.headerGeneration
	c.setHeadersLocked(snapshot.headers)
	c.withdrawalGeneration.Add(1)
}

// The caller holds the batch-commit barrier. A commit failure must not
// overwrite a later block mutation or independently admitted headers.
func (c *Chain) restoreFailedBatch(
	saved chainBatchSnapshot,
	applied chainBatchPosition,
	err error,
	message string,
) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	if c.batchRestoreIsSafeLocked(
		applied.tip,
		applied.blockIndex,
		applied.generation,
	) {
		c.restoreBatchBlocksLocked(saved)
		if c.headerMutationGeneration == applied.headerGeneration {
			c.restoreBatchHeadersLocked(saved)
		}
		return
	}
	slog.Default().Error(message,
		"component", "chain",
		"chain_id", c.id,
		"applied_tip_block_index", applied.blockIndex,
		"tip_block_index", c.tipBlockIndex,
		"applied_generation", applied.generation,
		"mutation_generation", c.mutationGeneration,
		"applied_tip_slot", applied.tip.Point.Slot,
		"applied_tip_hash", hex.EncodeToString(applied.tip.Point.Hash),
		"tip_slot", c.currentTip.Point.Slot,
		"tip_hash", hex.EncodeToString(c.currentTip.Point.Hash),
		"error", err,
	)
}
