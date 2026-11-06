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

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

// pendingRewardStakeInputHistoryFloor retains history until its deferred basis
// is durable. Rolled-back markers do not protect snapshots that no longer exist.
func pendingRewardStakeInputHistoryFloor(
	meta metadata.MetadataStore,
	txn types.Txn,
) (uint64, bool, error) {
	pending, err := loadRewardStakeInputsPending(meta, txn)
	if err != nil {
		return 0, false, err
	}
	var floor uint64
	protected := false
	for _, entry := range pending.Entries {
		if entry.BoundarySlot == 0 {
			return 0, false, fmt.Errorf(
				"invalid pending reward snapshot boundary for epoch %d",
				entry.Epoch,
			)
		}
		snapshot, err := meta.GetRewardSnapshot(entry.Epoch, "mark", txn)
		if err != nil {
			return 0, false, err
		}
		if snapshot == nil || snapshot.BoundarySlot != entry.BoundarySlot {
			continue
		}
		if snapshot.CapturedSlot >= snapshot.BoundarySlot {
			return 0, false, fmt.Errorf(
				"invalid pending reward snapshot boundary for epoch %d",
				entry.Epoch,
			)
		}
		if !protected || snapshot.CapturedSlot < floor {
			floor = snapshot.CapturedSlot
			protected = true
		}
	}
	return floor, protected, nil
}
