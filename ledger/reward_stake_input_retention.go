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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
)

// pendingRewardStakeInputHistoryFloor retains history until its deferred basis
// is durable. Rolled-back markers do not protect snapshots that no longer exist.
func pendingRewardStakeInputHistoryFloor(
	meta ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	if txn == nil {
		return 0, false, errors.New(
			"pending reward retention requires a read snapshot",
		)
	}
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
	snapshotFloor, snapshotPending, err := pendingBoundarySnapshotHistoryFloor(
		meta,
		txn,
	)
	if err != nil {
		return 0, false, err
	}
	if snapshotPending && (!protected || snapshotFloor < floor) {
		floor = snapshotFloor
		protected = true
	}
	return floor, protected, nil
}

// A deferred SNAP owns history before it can create the stake-input marker.
// Both markers must be read through one transaction across their handoff.
func pendingBoundarySnapshotHistoryFloor(
	meta ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) (uint64, bool, error) {
	raw, err := meta.GetSyncState(pendingRatificationSyncKey, txn)
	if err != nil {
		return 0, false, err
	}
	if raw == "" {
		return 0, false, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return 0, false, err
	}
	for _, key := range []string{"epoch", "boundary_slot", "id"} {
		value, present := envelope[key]
		if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return 0, false, errors.New(
				"invalid pending boundary snapshot record",
			)
		}
	}
	if value, present := envelope["snapshot"]; present &&
		bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return 0, false, errors.New("invalid pending boundary snapshot flag")
	}
	var record pendingRatificationRecord
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return 0, false, err
	}
	if record.Epoch == 0 || record.BoundarySlot == 0 || record.ID == 0 {
		return 0, false, errors.New(
			"invalid pending boundary snapshot position",
		)
	}
	if !record.Snapshot {
		return 0, false, nil
	}
	epoch, err := meta.GetEpoch(record.Epoch, txn)
	if err != nil {
		return 0, false, err
	}
	if epoch == nil || epoch.StartSlot != record.BoundarySlot {
		return 0, false, errors.New(
			"pending boundary snapshot does not match its epoch",
		)
	}
	return record.BoundarySlot - 1, true, nil
}
