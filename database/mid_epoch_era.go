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
	"strings"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// MidEpochEraSwitchSyncKey is the sync-state key recording the last era
// translation applied inside an epoch rather than at its start.
const MidEpochEraSwitchSyncKey = "mid_epoch_era_switch"

// PendingRatificationSyncKey names the record of an epoch boundary whose
// RATIFY decision or mark snapshot is not written yet. The LedgerDB set keeps
// it; a v0.94.2 metadata store kept it in sync_state.
const PendingRatificationSyncKey = "dingo:governance:ratify-pending"

// MidEpochEraSwitch records an era translation applied before the block at
// Slot, inside the epoch whose row Epoch names. The epoch's row carries ToEra
// from then on; a rollback to a slot before Slot gives the row FromEra back,
// because the epoch row is the one place the ledger reads its era from and
// epoch rows survive a rollback that stays inside their epoch.
type MidEpochEraSwitch struct {
	Epoch   uint64
	Slot    uint64
	FromEra uint
	ToEra   uint
}

func (m MidEpochEraSwitch) encode() string {
	return fmt.Sprintf("%d:%d:%d:%d", m.Epoch, m.Slot, m.FromEra, m.ToEra)
}

// ParseMidEpochEraSwitch decodes a MidEpochEraSwitchSyncKey value.
func ParseMidEpochEraSwitch(value string) (MidEpochEraSwitch, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 4 {
		return MidEpochEraSwitch{}, fmt.Errorf(
			"malformed mid-epoch era switch %q", value,
		)
	}
	var nums [4]uint64
	for i, part := range parts {
		// Era fields are stored as uint, so bound them to its width.
		bits := 64
		if i >= 2 {
			bits = strconv.IntSize
		}
		n, err := strconv.ParseUint(part, 10, bits)
		if err != nil {
			return MidEpochEraSwitch{}, fmt.Errorf(
				"malformed mid-epoch era switch %q: %w", value, err,
			)
		}
		nums[i] = n
	}
	return MidEpochEraSwitch{
		Epoch:   nums[0],
		Slot:    nums[1],
		FromEra: uint(nums[2]),
		ToEra:   uint(nums[3]),
	}, nil
}

// SetMidEpochEraSwitch records sw. It refuses on a database with no
// metadata store: the set is then the only copy of the record.
func (d *Database) SetMidEpochEraSwitch(sw MidEpochEraSwitch, txn *Txn) error {
	if d.metadata == nil {
		return errNoMetadataStore(nodesettings.FamilyEraSwitch)
	}
	return d.SetSyncState(MidEpochEraSwitchSyncKey, sw.encode(), txn)
}

// revertMidEpochEraSwitchAfterSlot undoes a recorded switch whose block lies
// after slot: the switched epoch's row returns to its source era, if the
// rollback left the row, and the record is dropped. A database with no
// metadata store has neither; the set reverts both in the same rollback.
func (d *Database) revertMidEpochEraSwitchAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	sw, err := d.truncateEraSwitch(txn)
	if err != nil || sw == nil {
		return err
	}
	if sw.Slot <= slot {
		return nil
	}
	if d.metadata != nil {
		epochs, err := d.GetEpochs(txn)
		if err != nil {
			return fmt.Errorf("load epochs to revert era switch: %w", err)
		}
		for _, epoch := range epochs {
			if epoch.EpochId != sw.Epoch || epoch.EraId != sw.ToEra {
				continue
			}
			if err := d.SetEpoch(
				epoch.StartSlot,
				epoch.EpochId,
				epoch.Nonce,
				epoch.EvolvingNonce,
				epoch.CandidateNonce,
				epoch.LastEpochBlockNonce,
				sw.FromEra,
				epoch.SlotLength,
				epoch.LengthInSlots,
				txn,
			); err != nil {
				return fmt.Errorf("revert era switch epoch row: %w", err)
			}
		}
	}
	if d.metadata == nil {
		return nil
	}
	return d.DeleteSyncState(MidEpochEraSwitchSyncKey, txn)
}
