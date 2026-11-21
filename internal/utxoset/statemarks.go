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

package utxoset

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/ledger"
)

var _ ledger.StateMarkReader = (*Set)(nil)

var errStateMarkRow = errors.New("utxoset: malformed state mark row")

// stateMarkRows converts the commit's changes to the delegator inactivity
// activation and the pending ratification record.
func stateMarkRows(lc ledger.LedgerChanges) []badgerutxo.RowPut {
	var rows []badgerutxo.RowPut
	for _, c := range lc.StateMarks {
		switch c.Kind {
		case ledger.StateMarkInactivity:
			rows = append(rows, badgerutxo.RowPut{
				Slot: c.Slot, Key: badgerutxo.InactivityEpochKey(),
				Value: appendU64(nil, c.Epoch),
			})
		case ledger.StateMarkRatifyPending:
			rows = append(rows, badgerutxo.RowPut{
				Slot: c.Slot, Key: badgerutxo.RatifyPendingKey(),
				Value: c.Value, Delete: c.Delete,
			})
		}
	}
	return rows
}

// storeKeepsNone reports whether err says the metadata store no longer
// keeps the family a read asked it for, so it has no rows of it for a state
// mark load to take over.
func storeKeepsNone(err error) bool {
	return errors.Is(err, nodesettings.ErrFamilyDrained)
}

// ensureInactivity loads the epoch the delegator inactivity activation ran
// at from the metadata store into a set from before it kept it. The
// credentials it stamped are loaded onto their account rows.
func (s *Set) ensureInactivity(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.InactivityLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the inactivity marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	raw, err := ldb.GetSyncState(
		database.DelegatorInactivityActivatedSyncKey, txn,
	)
	if err != nil && !storeKeepsNone(err) {
		return fmt.Errorf("utxoset: read the inactivity activation: %w", err)
	}
	var epoch, slot uint64
	if raw != "" {
		epoch, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf(
				"utxoset: parse the inactivity activation %q: %w", raw, err,
			)
		}
		// The activation ran at the boundary that entered its epoch, the
		// slot a rollback undoes it below.
		row, err := ldb.GetEpoch(epoch, txn)
		if err != nil {
			return fmt.Errorf("utxoset: read epoch %d: %w", epoch, err)
		}
		if row == nil {
			return fmt.Errorf(
				"utxoset: the inactivity activation names epoch %d, which "+
					"the metadata store does not hold", epoch,
			)
		}
		slot = row.StartSlot
	}
	err = s.store.LoadInactivity(func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error {
		if raw == "" {
			return nil
		}
		k := badgerutxo.InactivityEpochKey()
		if err := put(k, appendU64(nil, epoch)); err != nil {
			return err
		}
		return tombstone(slot, k)
	})
	if err != nil {
		return fmt.Errorf("utxoset: load the inactivity activation: %w", err)
	}
	return nil
}

// InactivityLoaded reports whether the set holds the delegator inactivity
// activation.
func (s *Set) InactivityLoaded() (bool, error) {
	return s.store.InactivityLoaded()
}

// InactivityActivation returns the epoch the delegator inactivity activation
// ran at and whether it has run.
func (s *Set) InactivityActivation(asOf *uint64) (uint64, bool, error) {
	v, ok, err := s.rewardRow(badgerutxo.InactivityEpochKey(), asOf)
	if err != nil || !ok {
		return 0, false, err
	}
	if len(v) != 8 {
		return 0, false, errStateMarkRow
	}
	return binary.BigEndian.Uint64(v), true, nil
}

// ensureRatifyPending loads the pending ratification record from the
// metadata store into a set from before it kept one.
func (s *Set) ensureRatifyPending(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.RatifyPendingLoaded()
	if err != nil {
		return fmt.Errorf(
			"utxoset: read the pending ratification marker: %w", err,
		)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	raw, err := ldb.GetSyncState(statedrain.PendingRatificationSyncKey, txn)
	if err != nil && !storeKeepsNone(err) {
		return fmt.Errorf("utxoset: read the pending ratification: %w", err)
	}
	tip, err := ldb.GetTip(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read the tip: %w", err)
	}
	err = s.store.LoadRatifyPending(func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error {
		if raw == "" {
			return nil
		}
		// The boundary that wrote the record is the slot a rollback drops
		// it below. A record that does not parse has no slot to drop it
		// at; the boundary apply stages such a record at slot 0 too.
		var h struct {
			BoundarySlot uint64 `json:"boundary_slot"`
		}
		_ = json.Unmarshal([]byte(raw), &h)
		// The metadata store is reverted below its tip before it is
		// loaded, and that revert leaves the record of a boundary it
		// removed. The ledger applies that boundary again and writes the
		// record itself; kept here, the record would name a decision no
		// job is deciding, and the boundary would refuse to go on.
		if h.BoundarySlot > tip.Point.Slot {
			return nil
		}
		k := badgerutxo.RatifyPendingKey()
		if err := put(k, []byte(raw)); err != nil {
			return err
		}
		return tombstone(h.BoundarySlot, k)
	})
	if err != nil {
		return fmt.Errorf("utxoset: load the pending ratification: %w", err)
	}
	return nil
}

// RatifyPendingLoaded reports whether the set holds the pending ratification
// record.
func (s *Set) RatifyPendingLoaded() (bool, error) {
	return s.store.RatifyPendingLoaded()
}

// RatifyPending returns the pending ratification record and whether one is
// set.
func (s *Set) RatifyPending(asOf *uint64) ([]byte, bool, error) {
	return s.rewardRow(badgerutxo.RatifyPendingKey(), asOf)
}
