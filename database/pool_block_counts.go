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

package database

import (
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// MergeImportedPoolBlockCounts adds the block counts carried by a bootstrap
// snapshot to the counts this node observed for the same epoch, and reports
// whether the epoch's counts are known at all.
//
// The two sources are disjoint by construction. A bootstrap applies no block at
// or below its anchor, and CountPoolBlocksInSlotRange raises its start slot
// past the recorded anchor for exactly that reason, so the observed counts
// cover (anchor, epochEnd] and the imported nesBcur covers [epochStart,
// anchor]. For the epoch before the anchor's the observed side is empty and the
// imported nesBprev is the whole epoch. Both sides already exclude TPraos
// overlay slots when the observed side is read through the reward round's
// overlay-aware reader.
//
// The per-pool counts are merged only for pools the caller asked about, while
// the epoch total takes every imported pool, because the total is the
// denominator of every pool's beta and the reference sums the whole BlocksMade
// map to obtain it.
//
// The bool is false when the epoch lies at or below the anchor and the
// snapshot's counts for it were not imported: zero counts and unknown counts
// are not the same answer.
func MergeImportedPoolBlockCounts(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	epoch uint64,
	epochStartSlot uint64,
	counts map[string]uint64,
	totalBlocks uint64,
) (map[string]uint64, uint64, bool, error) {
	// Read the anchor from the same sync state, in the same transaction, that
	// CountPoolBlocksInSlotRange raised its start slot with: the two must
	// agree about which slots the observed counts cover. A malformed value is
	// an error here for the reason it is one there -- read as "no anchor" it
	// would restore the uncounted-epoch zero at exactly the moment the anchor
	// could not be confirmed.
	anchor, anchored, err := mithrilAnchorSlot(meta, metaTxn)
	if err != nil {
		return nil, 0, false, err
	}
	if !anchored || anchor < epochStartSlot {
		return counts, totalBlocks, true, nil
	}
	imported, importedTotal, importedKnown, err := meta.
		GetImportedPoolBlockCounts(epoch, metaTxn)
	if err != nil {
		return nil, 0, false, fmt.Errorf(
			"get imported pool block counts for epoch %d: %w",
			epoch, err,
		)
	}
	if !importedKnown {
		return nil, 0, false, nil
	}
	if err := addImportedPoolCounts(counts, imported, epoch); err != nil {
		return nil, 0, false, err
	}
	if totalBlocks > math.MaxUint64-importedTotal {
		return nil, 0, false, fmt.Errorf(
			"imported block total overflow for epoch %d", epoch,
		)
	}
	return counts, totalBlocks + importedTotal, true, nil
}

// CountPoolBlocksLifetime returns each requested pool's observed block count
// plus the blocks a bootstrap snapshot recorded for the anchor's epoch and the
// epoch before it, the only two epochs below the anchor the snapshot covers.
// Epochs older than those were never held by this node, so on a bootstrapped
// node the result is a lower bound on the pool's lifetime total.
func CountPoolBlocksLifetime(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	poolKeys []lcommon.PoolKeyHash,
	endSlot uint64,
) (map[string]uint64, error) {
	counts, _, err := meta.CountPoolBlocksInSlotRange(
		poolKeys, 0, endSlot, metaTxn,
	)
	if err != nil {
		return nil, err
	}
	anchor, anchored, err := mithrilAnchorSlot(meta, metaTxn)
	if err != nil || !anchored {
		return counts, err
	}
	anchorEpoch, err := meta.GetEpochBySlot(anchor, metaTxn)
	if err != nil {
		return nil, fmt.Errorf("get epoch of Mithril trust boundary: %w", err)
	}
	if anchorEpoch == nil {
		return counts, nil
	}
	epochs := []uint64{anchorEpoch.EpochId}
	if anchorEpoch.EpochId > 0 {
		epochs = append(epochs, anchorEpoch.EpochId-1)
	}
	for _, epoch := range epochs {
		// Only the per-pool counts are used, so the total is ignored. An
		// epoch with no imported counts adds nothing.
		imported, _, known, err := meta.GetImportedPoolBlockCounts(
			epoch, metaTxn,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"get imported pool block counts for epoch %d: %w",
				epoch, err,
			)
		}
		if !known {
			continue
		}
		if err := addImportedPoolCounts(counts, imported, epoch); err != nil {
			return nil, err
		}
	}
	return counts, nil
}

// mithrilAnchorSlot reads the recorded Mithril trust boundary. The bool is
// false when no snapshot was imported.
func mithrilAnchorSlot(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) (uint64, bool, error) {
	value, err := meta.GetSyncState(mithrilLedgerSlotSyncKey, metaTxn)
	if err != nil {
		return 0, false, fmt.Errorf("read Mithril trust boundary: %w", err)
	}
	if value == "" {
		return 0, false, nil
	}
	slot, err := parseMithrilTrustBoundary(value)
	if err != nil {
		return 0, false, err
	}
	return slot, true, nil
}

// addImportedPoolCounts adds imported counts into counts for the pools already
// present in it.
func addImportedPoolCounts(
	counts map[string]uint64,
	imported map[string]uint64,
	epoch uint64,
) error {
	for poolKey, blocks := range imported {
		observed, ok := counts[poolKey]
		if !ok {
			continue
		}
		if observed > math.MaxUint64-blocks {
			return fmt.Errorf(
				"imported block count overflow for epoch %d pool %x",
				epoch, poolKey,
			)
		}
		counts[poolKey] = observed + blocks
	}
	return nil
}
