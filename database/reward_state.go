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

import "fmt"

// RebuildRewardLiveStake rebuilds the live reward stake aggregate from
// canonical account and live UTxO metadata.
func (d *Database) RebuildRewardLiveStake(slot uint64, txn *Txn) error {
	if txn == nil {
		return d.metadata.RebuildRewardLiveStake(slot, nil)
	}
	if err := txn.db.metadata.RebuildRewardLiveStake(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("rebuild reward live stake at slot %d: %w", slot, err)
	}
	return nil
}

// DeleteRewardStateAfterSlot deletes reward-state rows captured from
// rolled-back blocks.
func (d *Database) DeleteRewardStateAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	if txn == nil {
		return d.metadata.DeleteRewardStateAfterSlot(slot, nil)
	}
	if err := txn.db.metadata.DeleteRewardStateAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf("delete reward state after slot %d: %w", slot, err)
	}
	return nil
}
