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
	"strconv"
)

// RollbackHighWaterSyncKey is the sync_state key holding the highest block
// index the persistent chain held before its most recent rollback. The chain
// measures every peer rollback against it, so successive shallow rollbacks
// cannot add up to more than the security parameter. Only the chain raises it
// during normal operation; an operator truncate lowers it with
// LowerRollbackHighWater because blocks above the target no longer exist to
// be rolled back.
const RollbackHighWaterSyncKey = "chain_rollback_high_water"

// LowerRollbackHighWater caps the rollback high-water mark at blockIndex. A
// mark at or below blockIndex, or an absent one, is left unchanged.
func (d *Database) LowerRollbackHighWater(blockIndex uint64, txn *Txn) error {
	raw, err := d.GetSyncState(RollbackHighWaterSyncKey, txn)
	if err != nil {
		return fmt.Errorf("read rollback high-water mark: %w", err)
	}
	if raw == "" {
		return nil
	}
	mark, err := strconv.ParseUint(raw, 10, 64)
	if err == nil && mark <= blockIndex {
		return nil
	}
	if err := d.SetSyncState(
		RollbackHighWaterSyncKey,
		strconv.FormatUint(blockIndex, 10),
		txn,
	); err != nil {
		return fmt.Errorf("lower rollback high-water mark: %w", err)
	}
	return nil
}
