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
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// StateMarkKind names the ledger state a StateMarkChange writes.
type StateMarkKind uint8

const (
	// StateMarkInactivity records the epoch the one-time CIP-0163
	// delegator inactivity activation ran at. The accounts it stamped carry
	// the activation themselves.
	StateMarkInactivity StateMarkKind = iota
	// StateMarkRatifyPending sets or deletes the record of a boundary whose
	// ratification decision is not written yet.
	StateMarkRatifyPending
)

// StateMarkChange is one change to the delegator inactivity activation or
// the pending ratification record, journaled at Slot: a rollback below Slot
// undoes it. Epoch belongs to an activation, Value and Delete to the pending
// record.
type StateMarkChange struct {
	Kind   StateMarkKind
	Slot   uint64
	Epoch  uint64
	Value  []byte
	Delete bool
}

// StateMarkReader serves the delegator inactivity activation and the pending
// ratification record from a live UTxO set that keeps them. An asOf reads
// them as a rollback to that slot would leave them.
type StateMarkReader interface {
	InactivityLoaded() (bool, error)
	// InactivityActivation returns the epoch the activation ran at and
	// whether it has run.
	InactivityActivation(asOf *uint64) (uint64, bool, error)
	RatifyPendingLoaded() (bool, error)
	// RatifyPending returns the pending ratification record and whether
	// one is set.
	RatifyPending(asOf *uint64) ([]byte, bool, error)
}

// errInactivityMarkerWrite refuses a write of the activation marker alone:
// the boundary that runs the activation stages the epoch with the accounts
// it stamps.
var errInactivityMarkerWrite = errors.New(
	"the delegator inactivity activation is written only by the boundary " +
		"that runs it",
)

// stateMarksView is the delegator inactivity activation and the pending
// ratification record as txn sees them: the set's rows as the lowest staged
// rollback leaves them, under the staged changes no rollback dropped.
type stateMarksView struct {
	r       StateMarkReader
	at      *uint64
	changes []StateMarkChange
}

func (ls *LedgerState) stateMarksView(
	txn ledgerdb.Txn,
	family nodesettings.Family,
) (stateMarksView, error) {
	r, ok := ls.readSet(txn).(StateMarkReader)
	if !ok || ls.utxoSetFailed.Load() {
		return stateMarksView{}, drainedUnserved(family)
	}
	loaded := r.RatifyPendingLoaded
	if family == nodesettings.FamilyAccounts {
		loaded = r.InactivityLoaded
	}
	if ok, err := loaded(); err != nil || !ok {
		if err == nil {
			err = drainedUnserved(family)
		}
		return stateMarksView{}, err
	}
	v := stateMarksView{r: r}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	for _, o := range ls.stagedEpochStateOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if v.at == nil || to < *v.at {
				v.at = &to
			}
			v.changes = slices.DeleteFunc(
				v.changes,
				func(c StateMarkChange) bool { return c.Slot > to },
			)
		}
		v.changes = append(v.changes, o.stateMarks...)
	}
	return v, nil
}

func (v stateMarksView) activation() (uint64, bool, error) {
	epoch, ok, err := v.r.InactivityActivation(v.at)
	if err != nil {
		return 0, false, err
	}
	for _, c := range v.changes {
		if c.Kind == StateMarkInactivity {
			epoch, ok = c.Epoch, true
		}
	}
	return epoch, ok, nil
}

func (v stateMarksView) ratifyPending() ([]byte, bool, error) {
	raw, ok, err := v.r.RatifyPending(v.at)
	if err != nil {
		return nil, false, err
	}
	for _, c := range v.changes {
		if c.Kind == StateMarkRatifyPending {
			raw, ok = c.Value, !c.Delete
		}
	}
	return raw, ok, nil
}

// inactivityMarker is the activation epoch in decimal, as the metadata
// store's sync_state held it, or empty before the activation has run.
func (s stagingLedgerDB) inactivityMarker(txn ledgerdb.Txn) (string, error) {
	v, err := s.ls.stateMarksView(txn, nodesettings.FamilyAccounts)
	if err != nil {
		return "", err
	}
	epoch, ok, err := v.activation()
	if err != nil || !ok {
		return "", err
	}
	return strconv.FormatUint(epoch, 10), nil
}

// AccountInactivityActivationMembership returns those of refs the activation
// stamped, from the set's accounts.
func (s stagingLedgerDB) AccountInactivityActivationMembership(
	_ context.Context,
	refs []models.StakeCredentialRef,
	txn ledgerdb.Txn,
) (map[string]struct{}, error) {
	r := s.ls.accountServing(txn)
	if r == nil {
		return nil, drainedUnserved(nodesettings.FamilyAccounts)
	}
	out := make(map[string]struct{})
	for _, ref := range refs {
		a, err := s.ls.accountView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if a != nil && a.InactivityActivation {
			out[ref.MapKey()] = struct{}{}
		}
	}
	return out, nil
}

func (s stagingLedgerDB) ratifyPendingState(txn ledgerdb.Txn) (string, error) {
	v, err := s.ls.stateMarksView(txn, nodesettings.FamilyGovernance)
	if err != nil {
		return "", err
	}
	raw, ok, err := v.ratifyPending()
	if err != nil || !ok {
		return "", err
	}
	return string(raw), nil
}

// ratifyPendingSlot is the boundary slot a pending ratification record
// names, the slot it is journaled at. A record that does not parse has no
// slot to drop it at and is journaled at slot 0.
func ratifyPendingSlot(raw []byte) uint64 {
	var h struct {
		BoundarySlot uint64 `json:"boundary_slot"`
	}
	_ = json.Unmarshal(raw, &h)
	return h.BoundarySlot
}

func (s stagingLedgerDB) setRatifyPending(value string, txn ledgerdb.Txn) {
	raw := []byte(value)
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		StateMarks: []StateMarkChange{{
			Kind: StateMarkRatifyPending, Slot: ratifyPendingSlot(raw),
			Value: raw,
		}},
	})
}

// deleteRatifyPending stages the record's deletion at the boundary slot the
// record names, the slot the decision it stood for is written at: a rollback
// that keeps the boundary keeps both, and one below it leaves neither.
func (s stagingLedgerDB) deleteRatifyPending(txn ledgerdb.Txn) error {
	v, err := s.ls.stateMarksView(txn, nodesettings.FamilyGovernance)
	if err != nil {
		return err
	}
	raw, ok, err := v.ratifyPending()
	if err != nil || !ok {
		return err
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		StateMarks: []StateMarkChange{{
			Kind: StateMarkRatifyPending, Slot: ratifyPendingSlot(raw),
			Delete: true,
		}},
	})
	return nil
}

// isInactivityMarker reports whether key is the delegator inactivity
// activation's sync_state key.
func isInactivityMarker(key string) bool {
	return key == database.DelegatorInactivityActivatedSyncKey
}
