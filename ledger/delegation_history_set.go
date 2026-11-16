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
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// DelegationEvent is a stake pool delegation certificate: a stake delegation,
// or a registration or vote delegation certificate that names a pool. Slot is
// the slot it was applied at, which journals and rolls it back; a certificate
// of a Leios closure takes effect at the closure's context slot,
// EffectiveSlot, or zero when that is Slot.
type DelegationEvent struct {
	Slot          uint64
	Position      uint64
	EffectiveSlot uint64
	Pool          []byte
}

func (e DelegationEvent) effective() uint64 {
	if e.EffectiveSlot != 0 {
		return e.EffectiveSlot
	}
	return e.Slot
}

// DelegationEventChange is a delegation a commit applied.
type DelegationEventChange struct {
	Ref   models.StakeCredentialRef
	Event DelegationEvent
}

// DelegationHistoryReader serves pool delegation certificates from a live
// UTxO set that keeps them.
type DelegationHistoryReader interface {
	DelegationHistoryLoaded() (bool, error)
	// DelegationHistory returns every delegation of ref, oldest first.
	DelegationHistory(ref models.StakeCredentialRef) ([]DelegationEvent, error)
	// DelegationHistoryRefs returns every credential with a delegation.
	DelegationHistoryRefs() ([]models.StakeCredentialRef, error)
}

func delegationEventBefore(a, b DelegationEvent) bool {
	return a.Slot < b.Slot || (a.Slot == b.Slot && a.Position < b.Position)
}

// delegationHistoryReader returns the set's delegation history reader once it
// holds the history, or nil when reads stay on the metadata store.
func (ls *LedgerState) delegationHistoryReader(
	txn ledgerdb.Txn,
) DelegationHistoryReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if _, mode := ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return nil
	}
	r, ok := ls.readSet(txn).(DelegationHistoryReader)
	if !ok {
		return nil
	}
	if !ls.delegationHistoryLoaded.Load() {
		loaded, err := r.DelegationHistoryLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.delegationHistoryLoaded.Store(true)
	}
	return r
}

// delegationHistoryView returns ref's delegations as txn sees the set, oldest
// first: the committed events, cut back by each staged rollback, with the
// staged batches' events laid over them.
func (ls *LedgerState) delegationHistoryView(
	r DelegationHistoryReader,
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
) ([]DelegationEvent, error) {
	events, err := r.DelegationHistory(ref)
	if err != nil {
		return nil, err
	}
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			events = slices.DeleteFunc(events, func(e DelegationEvent) bool {
				return e.Slot > to
			})
		}
		for _, c := range o.delegations {
			if !sameRef(c.Ref, ref) {
				continue
			}
			events = slices.DeleteFunc(events, func(e DelegationEvent) bool {
				return e.Slot == c.Event.Slot && e.Position == c.Event.Position
			})
			events = append(events, c.Event)
		}
	}
	slices.SortFunc(events, func(a, b DelegationEvent) int {
		if delegationEventBefore(a, b) {
			return -1
		}
		if delegationEventBefore(b, a) {
			return 1
		}
		return 0
	})
	return events, nil
}

// stagedDelegationRefs appends to refs, which seen indexes, the credentials
// the batches txn sees have staged a delegation for and refs lacks.
func (ls *LedgerState) stagedDelegationRefs(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	seen map[string]struct{},
) []models.StakeCredentialRef {
	for _, o := range ls.stagedOps(txn) {
		for _, c := range o.delegations {
			if _, ok := seen[c.Ref.MapKey()]; !ok {
				seen[c.Ref.MapKey()] = struct{}{}
				refs = append(refs, c.Ref)
			}
		}
	}
	return refs
}

// LatestDelegationEffectiveAt returns the delegation the store's boundary
// stake queries rank latest at slot: among the certificates whose effective
// slot is at or before slot (<=, sqlstore/historical_stake.go:1263), the
// newest by effective slot, applied slot and position (:1326,
// ranked_delegation). It returns nil when there is none.
func LatestDelegationEffectiveAt(
	events []DelegationEvent,
	slot uint64,
) *DelegationEvent {
	var best *DelegationEvent
	for i := range events {
		e := &events[i]
		if e.effective() > slot {
			continue
		}
		if best == nil || e.effective() > best.effective() ||
			(e.effective() == best.effective() &&
				(e.Slot > best.Slot ||
					(e.Slot == best.Slot && e.Position > best.Position))) {
			best = e
		}
	}
	return best
}

// stageDelegationEvent stages a pool delegation certificate of ref.
func (ls *LedgerState) stageDelegationEvent(
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
	slot, effective, position uint64,
	pool []byte,
) {
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		DelegationEvents: []DelegationEventChange{{
			Ref: models.NewStakeCredentialRef(ref.Tag, bytes.Clone(ref.Key)),
			Event: DelegationEvent{
				Slot: slot, Position: position,
				EffectiveSlot: poolEventEffectiveSlot(slot, effective),
				Pool:          bytes.Clone(pool),
			},
		}},
	})
}
