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

package ledger

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrLeaderStakeSnapshotUnavailable reports that the stake distribution
// electing leaders at a slot cannot be read: the slot lies in an epoch the
// ledger has not reached, or the distribution is missing, empty, pruned, or
// has no usable denominator. It never means the pool is ineligible.
var ErrLeaderStakeSnapshotUnavailable = errLeaderStakeSnapshotUnavailable

// ElectingPoolStake returns the pool's stake in the distribution that elects
// leaders at slot: the same snapshot, and the same choice between the mark
// snapshot and a Mithril-imported active distribution, that header
// verification checks a block from that pool at that slot against. A pool
// absent from a populated distribution has zero stake and no error.
func (ls *LedgerState) ElectingPoolStake(
	slot uint64,
	poolKeyHash lcommon.PoolKeyHash,
) (uint64, error) {
	epochCache := ls.epochCacheSnapshot()
	epoch, err := epochForSlotInCache(epochCache, slot)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: slot %d: %w",
			ErrLeaderStakeSnapshotUnavailable,
			slot,
			err,
		)
	}
	snap := ls.resolveElectingSnapshotAtSlot(
		slot, epoch.EpochId, poolKeyHash, epochCache, nil,
	)
	if snap.selErr != nil {
		return 0, snap.selErr
	}
	if snap.rowErr != nil {
		return 0, fmt.Errorf("lookup pool stake: %w", snap.rowErr)
	}
	if snap.row != nil && snap.row.TotalStake > 0 {
		if snap.kind == models.PoolStakeSnapshotTypeActive &&
			snap.row.StakeDenominator == 0 {
			return 0, fmt.Errorf(
				"%w: epoch %d active distribution has no denominator",
				ErrLeaderStakeSnapshotUnavailable,
				snap.epoch,
			)
		}
		return uint64(snap.row.TotalStake), nil
	}
	// The reference distribution is always populated, so an empty one is a
	// dingo-side gap and absence from it says nothing about the pool.
	if ls.poolSnapshotPruned(snap.epoch, snap.kind) {
		return 0, fmt.Errorf(
			"%w: %w: epoch %d %s snapshot",
			ErrLeaderStakeSnapshotUnavailable,
			errPoolSnapshotPruned,
			snap.epoch,
			snap.kind,
		)
	}
	total, err := ls.db.Metadata().GetTotalActiveStake(
		snap.epoch,
		snap.kind,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("lookup total active stake: %w", err)
	}
	if total == 0 {
		return 0, fmt.Errorf(
			"%w: epoch %d %s snapshot is empty",
			ErrLeaderStakeSnapshotUnavailable,
			snap.epoch,
			snap.kind,
		)
	}
	return 0, nil
}
