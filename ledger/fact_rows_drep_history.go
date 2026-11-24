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
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// DRepHistoryReader serves the DRep history rows the set holds: the expiry
// history, the dormancy counter history, the dormant expiry epoch events and
// the DRep delegation clears.
type DRepHistoryReader interface {
	DRepHistoryLoaded() (bool, error)
	DRepHistoryLost() (bool, error)
	DRepHistory() (ledgerdb.DRepHistoryRows, error)
}

// DRepHistoryRangeReader is a DRepHistoryReader that reads one slot range of
// the history without reading the rest.
type DRepHistoryRangeReader interface {
	DRepHistoryReader
	DRepHistoryIn(from, through uint64) (ledgerdb.DRepHistoryRows, error)
}

// drepHistory lists the DRep history rows the set holds for table. It refuses
// when the set does not hold them all: a set that loaded before the history
// was drained holds none, which an export would pass off as an empty history.
func (ls *LedgerState) drepHistory(
	table metadata.FactTable,
	from, through uint64,
) (ledgerdb.DRepHistoryRows, error) {
	r := ls.drepReader(nil)
	if r == nil {
		return ledgerdb.DRepHistoryRows{}, ledgerStateUnserved(nodesettings.FamilyDReps)
	}
	h, ok := ls.readSet(nil).(DRepHistoryReader)
	if !ok {
		return ledgerdb.DRepHistoryRows{}, ledgerStateUnserved(nodesettings.FamilyDReps)
	}
	loaded, err := h.DRepHistoryLoaded()
	if err != nil {
		return ledgerdb.DRepHistoryRows{}, err
	}
	lost, err := h.DRepHistoryLost()
	if err != nil {
		return ledgerdb.DRepHistoryRows{}, err
	}
	if !loaded || lost {
		return ledgerdb.DRepHistoryRows{}, fmt.Errorf(
			"%w: %s: %w", ErrFactTableUnserved, table.Name,
			errors.New("the DRep history was drained before the set loaded it"),
		)
	}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	if rr, ok := h.(DRepHistoryRangeReader); ok {
		return rr.DRepHistoryIn(from, through)
	}
	return h.DRepHistory()
}

func (ls *LedgerState) drepExpiryHistoryFactRows(
	table metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	h, err := ls.drepHistory(table, from, through)
	if err != nil {
		return metadata.FactRows{}, err
	}
	rows := slices.DeleteFunc(h.Expiry, func(r ledgerdb.DRepExpiryHistoryRow) bool {
		return !inSlotRange(r.Slot, from, through)
	})
	slices.SortFunc(rows, func(a, b ledgerdb.DRepExpiryHistoryRow) int {
		return cmp.Or(
			cmp.Compare(a.Tag, b.Tag),
			bytes.Compare(a.Credential, b.Credential),
			cmp.Compare(a.Slot, b.Slot),
		)
	})
	out := metadata.FactRows{Columns: drepExpiryHistoryColumns}
	for _, r := range rows {
		out.Rows = append(out.Rows, drepExpiryHistoryRow(r))
	}
	return out, nil
}

var drepExpiryHistoryColumns = []string{
	"credential_tag", "credential", "added_slot", "last_activity_epoch",
	"expiry_epoch",
}

func drepExpiryHistoryRow(r ledgerdb.DRepExpiryHistoryRow) []any {
	return []any{
		sqlInt(uint64(r.Tag)), nilBytes(r.Credential), sqlInt(r.Slot),
		sqlInt(r.Activity), sqlInt(r.Expiry),
	}
}

func (ls *LedgerState) drepDormancyHistoryFactRows(
	table metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	h, err := ls.drepHistory(table, from, through)
	if err != nil {
		return metadata.FactRows{}, err
	}
	rows := slices.DeleteFunc(h.Dormancy, func(r ledgerdb.DRepDormancyHistoryRow) bool {
		return !inSlotRange(r.Slot, from, through)
	})
	slices.SortFunc(rows, func(a, b ledgerdb.DRepDormancyHistoryRow) int {
		return cmp.Or(
			cmp.Compare(a.Slot, b.Slot), cmp.Compare(a.Previous, b.Previous),
		)
	})
	out := metadata.FactRows{Columns: drepDormancyHistoryColumns}
	for _, r := range rows {
		out.Rows = append(out.Rows, drepDormancyHistoryRow(r))
	}
	return out, nil
}

var drepDormancyHistoryColumns = []string{
	"added_slot", "previous_dormant_epochs",
}

func drepDormancyHistoryRow(r ledgerdb.DRepDormancyHistoryRow) []any {
	return []any{sqlInt(r.Slot), sqlInt(r.Previous)}
}

func (ls *LedgerState) drepExpiryEpochEventFactRows(
	table metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	h, err := ls.drepHistory(table, from, through)
	if err != nil {
		return metadata.FactRows{}, err
	}
	out := metadata.FactRows{Columns: []string{"added_slot"}}
	slots := slices.DeleteFunc(h.Events, func(slot uint64) bool {
		return !inSlotRange(slot, from, through)
	})
	slices.Sort(slots)
	for _, slot := range slots {
		out.Rows = append(out.Rows, []any{sqlInt(slot)})
	}
	return out, nil
}

func (ls *LedgerState) accountDrepClearFactRows(
	table metadata.FactTable,
	from, through uint64,
) (metadata.FactRows, error) {
	h, err := ls.drepHistory(table, from, through)
	if err != nil {
		return metadata.FactRows{}, err
	}
	rows := slices.DeleteFunc(h.Clears, func(r ledgerdb.DRepClearRow) bool {
		return !inSlotRange(r.Slot, from, through)
	})
	slices.SortFunc(rows, func(a, b ledgerdb.DRepClearRow) int {
		return cmp.Or(
			cmp.Compare(a.Tag, b.Tag),
			bytes.Compare(a.StakingKey, b.StakingKey),
			cmp.Compare(a.Slot, b.Slot),
		)
	})
	out := metadata.FactRows{Columns: []string{
		"credential_tag", "staking_key", "added_slot",
	}}
	for _, r := range rows {
		out.Rows = append(out.Rows, []any{
			sqlInt(uint64(r.Tag)), nilBytes(r.StakingKey), sqlInt(r.Slot),
		})
	}
	return out, nil
}
