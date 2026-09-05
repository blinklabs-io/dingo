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

// consumedUtxoPruneFloorSyncKey records how far the consumed-UTxO sweep has
// hard-deleted spent rows.
//
// UtxosDeleteConsumed removes rows outright (blob objects and the metadata row
// that is the source of truth), while TruncateAfterSlot restores spent UTxOs
// with an UPDATE -- SetUtxosNotDeletedAfterSlot, `deleted_slot > ?`. An UPDATE
// can only reach rows that still exist, so once a row has been swept, no
// rollback can put it back. A rollback below the swept slot therefore leaves
// the live UTxO set missing every output that was consumed in the swept range
// and reports the tip repaired anyway.
//
// The two bounds are meant to line up: the sweep runs at
// tip-stabilityWindow and a single rollback reaches at most one stability
// window below the tip. Nothing enforced that, and successive recovery rewinds
// compound -- each one rewinds a further window below the *already lowered*
// tip while this floor stays fixed at the highest tip the node reached (issue
// #3766). Recording the floor lets rollback refuse rather than silently
// diverge.
const consumedUtxoPruneFloorSyncKey = "consumed_utxo_prune_slot"

// ConsumedUtxoPruneFloor returns the highest slot the consumed-UTxO sweep has
// hard-deleted spent rows at or below, or 0 when nothing has been swept.
// Rolling back below this slot cannot restore the UTxOs consumed above it.
//
// It fails closed: a read or parse failure is returned rather than reported as
// "nothing swept", because callers use it to refuse a destructive rollback and
// a floor that cannot be verified must not read as absent.
func (d *Database) ConsumedUtxoPruneFloor(txn *Txn) (uint64, error) {
	val, err := d.GetSyncState(consumedUtxoPruneFloorSyncKey, txn)
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

// setConsumedUtxoPruneFloor raises the recorded floor to slot. The floor only
// ever moves up: a later sweep at a lower slot (the tip fell after a rollback)
// does not make the rows an earlier, higher sweep already removed restorable
// again.
func (d *Database) setConsumedUtxoPruneFloor(slot uint64, txn *Txn) error {
	current, err := d.ConsumedUtxoPruneFloor(txn)
	if err != nil {
		return err
	}
	if slot <= current {
		return nil
	}
	return d.SetSyncState(
		consumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(slot, 10),
		txn,
	)
}

// ClearConsumedUtxoPruneFloorAbove drops the recorded floor when it is above
// slot, reporting the previous value and whether it was cleared.
//
// This exists for lifecycle.Truncate, which deliberately rewinds further than
// the live ledger may -- it is not bound by the security parameter either,
// because an operator invoking CIP-0135 disaster recovery has taken
// responsibility for the resulting state. Leaving a floor above the new tip
// would refuse every subsequent rollback until the node resynced past it,
// wedging the recovery the truncate was performed to enable. The consumed rows
// swept between slot and the old floor stay gone; clearing the record is an
// admission that this database no longer has a rollback boundary it can
// enforce, not a claim that those rows came back.
func (d *Database) ClearConsumedUtxoPruneFloorAbove(
	slot uint64,
	txn *Txn,
) (uint64, bool, error) {
	current, err := d.ConsumedUtxoPruneFloor(txn)
	if err != nil {
		return 0, false, err
	}
	if current <= slot {
		return current, false, nil
	}
	if err := d.DeleteSyncState(consumedUtxoPruneFloorSyncKey, txn); err != nil {
		return current, false, fmt.Errorf(
			"clear consumed UTxO prune floor: %w",
			err,
		)
	}
	return current, true, nil
}
