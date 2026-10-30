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

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

// BlockHistoryRetained reports whether block_nonce holds a row for every block
// in the inclusive slot range, which a block list read from it needs in order
// to be complete.
//
// Two things leave gaps. A bootstrap applies no block at or below its anchor,
// so a range reaching the anchor is covered only once the historical backfill
// has completed and written the rows below it. Nonce pruning deletes every
// non-checkpoint row below an epoch-start cutoff and keeps each epoch's
// checkpoint rows, so the epoch holding the earliest non-checkpoint row is
// where retained history begins, and any row below it belongs to a pruned
// epoch. An epoch below that point whose only block was its checkpoint is
// indistinguishable from a pruned one and is reported as not retained.
func BlockHistoryRetained(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	startSlot uint64,
	endSlot uint64,
) (bool, error) {
	if endSlot < startSlot {
		return true, nil
	}
	anchor, anchored, err := mithrilAnchorSlot(meta, metaTxn)
	if err != nil {
		return false, err
	}
	var from uint64
	if anchored {
		if startSlot > anchor {
			from = anchor + 1
		} else {
			covered, err := backfilledBelowAnchor(meta, metaTxn, anchor)
			if err != nil {
				return false, err
			}
			if !covered {
				return false, nil
			}
		}
	}
	upper := endSlot
	first, found, err := meta.GetEarliestNonCheckpointBlockSlot(from, metaTxn)
	if err != nil {
		return false, err
	}
	if found {
		floor := first
		epoch, err := meta.GetEpochBySlot(first, metaTxn)
		if err != nil {
			return false, fmt.Errorf("get epoch of slot %d: %w", first, err)
		}
		// The anchor's epoch began before the anchor, but its rows start
		// past it, so retained history there begins at the anchor.
		if epoch != nil && epoch.StartSlot <= first {
			floor = max(epoch.StartSlot, from)
		}
		if startSlot >= floor {
			return true, nil
		}
		upper = min(upper, floor-1)
	}
	_, below, err := meta.GetBlockHashesInSlotRange(
		startSlot, upper, 1, 0, "asc", metaTxn,
	)
	if err != nil {
		return false, err
	}
	return below == 0, nil
}

// backfillPhase mirrors node.BackfillPhase, the checkpoint phase of the
// historical metadata backfill.
const backfillPhase = "metadata"

// backfilledBelowAnchor reports whether the historical backfill completed and
// left block_nonce rows at or below the Mithril anchor. A backfill that ran
// with nonce rows disabled completes without writing any, so the checkpoint
// alone is not enough.
func backfilledBelowAnchor(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	anchor uint64,
) (bool, error) {
	cp, err := meta.GetBackfillCheckpoint(backfillPhase, metaTxn)
	if err != nil {
		return false, fmt.Errorf("get backfill checkpoint: %w", err)
	}
	if cp == nil || !cp.Completed {
		return false, nil
	}
	first, found, err := meta.GetEarliestNonCheckpointBlockSlot(0, metaTxn)
	if err != nil {
		return false, err
	}
	return found && first <= anchor, nil
}
