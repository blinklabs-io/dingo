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
	"cmp"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// certOrder ranks a certificate as the metadata store's boundary stake
// queries do under Leios: by the slot it takes effect at, then the slot it was
// applied at, then its position in the block.
type certOrder struct {
	effective, slot, position uint64
}

func (a certOrder) compare(b certOrder) int {
	return cmp.Or(
		cmp.Compare(a.effective, b.effective),
		cmp.Compare(a.slot, b.slot),
		cmp.Compare(a.position, b.position),
	)
}

// leiosStakeReady reports whether the set holds every history the Leios
// derivation of the boundary stake reads: stake registrations, pool
// delegations, pool certificates, closure output effects and witnesses.
func (ls *LedgerState) leiosStakeReady(txn ledgerdb.Txn) bool {
	if ls.registrationHistoryReader(txn) == nil ||
		ls.delegationHistoryReader(txn) == nil ||
		ls.closureEffectReader(txn) == nil ||
		ls.witnessEachReader(txn) == nil {
		return false
	}
	return ls.poolHistoryReader(txn) != nil
}

// closureEffectReader returns the set's closure output effect reader once it
// holds the effects, or nil.
func (ls *LedgerState) closureEffectReader(
	txn ledgerdb.Txn,
) ClosureUtxoEffectReader {
	r, ok := ls.readSet(txn).(ClosureUtxoEffectReader)
	if !ok {
		return nil
	}
	if !ls.closureEffectsLoaded.Load() {
		loaded, err := r.ClosureUtxoEffectsLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.closureEffectsLoaded.Store(true)
	}
	return r
}

// witnessEachReader returns the set's witness reader once it holds every
// witness, or nil.
func (ls *LedgerState) witnessEachReader(txn ledgerdb.Txn) WitnessEachReader {
	r, ok := ls.readSet(txn).(WitnessEachReader)
	if !ok {
		return nil
	}
	loaded, err := r.WitnessSlotsLoaded()
	if err != nil || !loaded {
		return nil
	}
	return r
}

// candidateRefs returns every credential the boundary may find delegated: the
// accounts and, under Leios, the credentials with a delegation certificate,
// whose account row holds only the delegation applied last.
func (b *boundaryStake) candidateRefs() ([]models.StakeCredentialRef, error) {
	refs, err := b.ar.ActiveAccountRefs()
	if err != nil {
		return nil, err
	}
	if b.past {
		ah, ok := b.ar.(AccountAsOfReader)
		if !ok {
			return nil, b.ls.unavailableAsOf("accounts")
		}
		changed, err := ah.AccountRefsChangedAfter(b.slot)
		if err != nil {
			return nil, err
		}
		seen := refSet(refs)
		for _, ref := range changed {
			if _, dup := seen[ref.MapKey()]; !dup {
				seen[ref.MapKey()] = struct{}{}
				refs = append(refs, ref)
			}
		}
	} else {
		refs, err = b.stagedAccountRefs(refs, refSet(refs))
		if err != nil {
			return nil, err
		}
	}
	if !b.leios {
		return refs, nil
	}
	dh := b.ls.delegationHistoryReader(b.txn)
	if dh == nil {
		return nil, b.ls.unavailableAsOf("pool delegations")
	}
	delegated, err := dh.DelegationHistoryRefs()
	if err != nil {
		return nil, err
	}
	seen := refSet(refs)
	for _, ref := range delegated {
		if _, dup := seen[ref.MapKey()]; !dup {
			seen[ref.MapKey()] = struct{}{}
			refs = append(refs, ref)
		}
	}
	return b.ls.stagedDelegationRefs(b.txn, refs, seen), nil
}

// leiosDelegation resolves the pool ref is delegated to at the boundary as the
// store's active_delegation CTE does (sqlstore/historical_stake.go:1326-1380),
// from certificates ranked by the slot they take effect at. It returns nil
// when ref has none.
func (b *boundaryStake) leiosDelegation(
	ref models.StakeCredentialRef,
	rh RegistrationHistoryReader,
	dh DelegationHistoryReader,
	book *poolBook,
) (*boundaryDelegation, error) {
	stake, err := b.ls.stakeHistoryView(rh, b.txn, ref)
	if err != nil {
		return nil, err
	}
	delegs, err := b.ls.delegationHistoryView(dh, b.txn, ref)
	if err != nil {
		return nil, err
	}
	acct, err := b.account(ref, b.stateSlot())
	if err != nil {
		return nil, err
	}
	// A credential with no certificate applied by the snapshot slot falls
	// back to its account row, as the store's account branch does (:1296,
	// :1324, noHistorySQL).
	atSlot := func(slot uint64) bool { return slot <= b.slot }
	anyStake := slices.ContainsFunc(stake, func(e StakeEvent) bool {
		return atSlot(e.Slot)
	})
	anyDeleg := slices.ContainsFunc(delegs, func(e DelegationEvent) bool {
		return atSlot(e.Slot)
	})
	var reg *certOrder
	registered := false
	if e := LatestStakeEventEffectiveAt(stake, b.slot); e != nil {
		reg = &certOrder{e.effective(), e.Slot, e.Position}
		registered = e.Registered
	} else if !anyStake {
		if a := b.accountAt(ref, b.slot, acct); a != nil &&
			a.CreatedSlot <= b.slot {
			reg = &certOrder{a.CreatedSlot, a.CreatedSlot, 0}
			registered = a.Active
		}
	}
	if reg == nil || !registered {
		return nil, nil
	}
	var del *certOrder
	var pool []byte
	if e := LatestDelegationEffectiveAt(delegs, b.slot); e != nil {
		del = &certOrder{e.effective(), e.Slot, e.Position}
		pool = e.Pool
	} else if !anyStake && !anyDeleg {
		if a := b.accountAt(ref, b.slot, acct); a != nil && a.Active &&
			len(a.Pool) > 0 && a.AddedSlot <= b.slot {
			del = &certOrder{a.AddedSlot, a.AddedSlot, 0}
			pool = a.Pool
		}
	}
	if del == nil || del.compare(*reg) < 0 {
		return nil, nil
	}
	events, err := book.events(pool)
	if err != nil {
		return nil, err
	}
	if b.poolReaped(events, del.effective) {
		return nil, nil
	}
	return &boundaryDelegation{ref: ref, pool: pool, acct: acct}, nil
}

// accountAt is the account row as it stood at the snapshot slot: acct, read at
// the state slot, unless the boundary is past and the snapshot slot differs.
// The store's account branch reads the account's current row instead, and
// drops a credential whose row a later certificate re-stamped past the slot
// while it has no certificate by then; the set keeps the row as of the
// snapshot, which is the state the ledger snapshots.
func (b *boundaryStake) accountAt(
	ref models.StakeCredentialRef,
	slot uint64,
	acct *models.Account,
) *models.Account {
	if slot == b.stateSlot() {
		return acct
	}
	a, err := b.account(ref, slot)
	if err != nil {
		return nil
	}
	return a
}

// poolReaped reports whether a pool retirement removes a delegation that took
// effect at delegated: the store's NOT EXISTS clause over pool_retirement
// (:1352-1393). A retirement counts when it takes effect by the snapshot slot
// and its epoch starts after the delegation and by the reap slot, unless a
// later-applied registration or retirement of the pool takes effect before
// that epoch starts.
func (b *boundaryStake) poolReaped(events []PoolEvent, delegated uint64) bool {
	reapSlot := b.stateSlot()
	for i := range events {
		rt := events[i]
		if rt.Registration != nil || rt.effective() > b.slot {
			continue
		}
		var start uint64
		found := false
		for _, e := range b.epochs {
			if e.EpochId == rt.RetiringEpoch {
				start, found = e.StartSlot, true
				break
			}
		}
		if !found || start <= delegated || start > reapSlot {
			continue
		}
		superseded := slices.ContainsFunc(events, func(o PoolEvent) bool {
			return o.effective() < start && poolEventBeforeInStore(rt, o)
		})
		if !superseded {
			return true
		}
	}
	return false
}

// poolEventBeforeInStore is poolEventBefore with the store's position for a
// retirement a snapshot import wrote without a certificate: the store ranks a
// row with no certificate as block 0, certificate 0, so it precedes any
// certificate of its slot where the set positions it last.
func poolEventBeforeInStore(a, b PoolEvent) bool {
	pos := func(e PoolEvent) uint64 {
		if e.Position == PoolEventSyntheticPosition {
			return 0
		}
		return e.Position
	}
	return a.Slot < b.Slot || (a.Slot == b.Slot && pos(a) < pos(b))
}

// leiosDelegations is delegations for a chain whose certificates take effect
// at their closure's context slot.
func (b *boundaryStake) leiosDelegations() ([]boundaryDelegation, error) {
	rh := b.ls.registrationHistoryReader(b.txn)
	dh := b.ls.delegationHistoryReader(b.txn)
	if rh == nil || dh == nil {
		return nil, b.ls.unavailableAsOf("stake certificate history")
	}
	hist := b.ls.poolHistoryReader(b.txn)
	if hist == nil {
		return nil, b.ls.unavailableAsOf("pool certificate history")
	}
	// Only the certificate history is read, so verify mode, which does not
	// serve pool reads from the set, can compare too.
	book := &poolBook{ls: b.ls, txn: b.txn, hist: hist}
	refs, err := b.candidateRefs()
	if err != nil {
		return nil, err
	}
	out := make([]boundaryDelegation, 0, len(refs))
	for _, ref := range refs {
		d, err := b.leiosDelegation(ref, rh, dh, book)
		if err != nil {
			return nil, err
		}
		if d != nil {
			out = append(out, *d)
		}
	}
	return out, nil
}

// leiosWitnessGuard refuses a credential whose reward balance or expiry the
// set cannot put in effective order. A reward withdrawal of a closure applied
// after the snapshot slot and effective at or before it changes the balance the
// store reads at that slot, and the set keeps no amount for it. The store
// reads the expiry epoch from the credential's latest witness by effective
// slot at or before the snapshot, and the set keeps it from the latest by
// applied slot; they agree unless those two witnesses fall in different
// epochs. A witness the store does not pick, because it is superseded or
// takes effect after the snapshot, moves neither.
func (b *boundaryStake) leiosWitnessGuard(
	d boundaryDelegation,
	expiryEpoch uint64,
) error {
	wr := b.ls.witnessEachReader(b.txn)
	if wr == nil {
		return b.ls.unavailableAsOf("witnesses")
	}
	err := wr.EachWithdrawalWitness(d.ref, func(slot, effective uint64) error {
		if slot > b.slot && effective <= b.slot {
			return b.ls.unavailableAsOf(
				"the reward balance across a closure withdrawal",
			)
		}
		return nil
	})
	if err != nil || expiryEpoch == 0 {
		return err
	}
	var applied, effective uint64
	var hasApplied, hasEffective bool
	err = wr.EachWitness(d.ref, func(slot, eff uint64) error {
		if slot <= b.slot && (!hasApplied || slot > applied) {
			applied, hasApplied = slot, true
		}
		if eff <= b.slot && (!hasEffective || eff > effective) {
			effective, hasEffective = eff, true
		}
		return nil
	})
	if err != nil || (!hasApplied && !hasEffective) {
		return err
	}
	if hasApplied && hasEffective && applied == effective {
		return nil
	}
	ae, aok := epochAt(b.epochs, applied)
	ee, eok := epochAt(b.epochs, effective)
	if !hasApplied || !hasEffective || !aok || !eok || ae.EpochId != ee.EpochId {
		return b.ls.unavailableAsOf(
			"the expiry of a closure witness effective in another epoch",
		)
	}
	return nil
}

// adjustSums turns the stake sums read from the outputs live at the snapshot
// slot into the sums the store counts at it, with the effects of closures
// applied after it.
func (b *boundaryStake) adjustSums(sums map[string]uint64) error {
	er := b.ls.closureEffectReader(b.txn)
	if er == nil {
		return b.ls.unavailableAsOf("closure output effects")
	}
	var effects []ClosureUtxoEffect
	err := er.ClosureUtxoEffectsAfter(b.slot, func(e ClosureUtxoEffect) error {
		effects = append(effects, e)
		return nil
	})
	if err != nil {
		return err
	}
	return AdjustStakeSumsEffective(sums, effects, b.slot)
}
