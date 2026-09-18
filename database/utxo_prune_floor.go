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

// ConsumedUtxoPruneFloor returns the highest slot the consumed-UTxO sweep has
// hard-deleted spent rows at or below, or 0 when nothing has been swept.
// Rolling back below this slot cannot restore the UTxOs consumed above it.
//
// It fails closed: a read or parse failure is returned rather than reported as
// "nothing swept", because callers use it to refuse a destructive rollback and
// a floor that cannot be verified must not read as absent.
func (d *Database) ConsumedUtxoPruneFloor(txn *Txn) (uint64, error) {
	val, err := d.GetSyncState(ConsumedUtxoPruneFloorSyncKey, txn)
	if err != nil {
		return 0, fmt.Errorf("read consumed UTxO prune floor: %w", err)
	}
	if val == "" {
		return 0, nil
	}
	slot, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"parse consumed UTxO prune floor %q: %w",
			val,
			err,
		)
	}
	return slot, nil
}

// writeConsumedUtxoPruneFloor records slot as the floor. It performs no read of
// its own: the caller compares against the value it read before deleting
// anything, so the only work left after an irreversible blob delete is this
// write. Callers must not lower the floor -- rows an earlier, higher sweep
// removed do not become restorable because a later sweep ran at a lower slot.
func (d *Database) writeConsumedUtxoPruneFloor(slot uint64, txn *Txn) error {
	return d.SetSyncState(
		ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(slot, 10),
		txn,
	)
}
