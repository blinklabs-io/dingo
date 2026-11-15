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
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrLedgerStateAsOfUnavailable reports a ledger state read as of a past slot
// that the live UTxO set cannot answer: the slot is older than its journals,
// or the transaction holds staged changes the journals do not describe. It is
// returned only when a family the read needs has been drained from the
// metadata store; otherwise the read is answered from the metadata store.
var ErrLedgerStateAsOfUnavailable = errors.New(
	"ledger state before the live UTxO set's history is not available when the set is the only copy",
)

// StateHistory is implemented by a live UTxO set whose computed rows can be
// read as of an earlier slot. StateReachesSlot returns an error wrapping
// ErrLedgerStateAsOfUnavailable unless every computed-row family is exact as
// of slot.
type StateHistory interface {
	StateReachesSlot(slot uint64) error
}

// DRepAsOfReader lists DReps as a rollback to slot would leave them.
type DRepAsOfReader interface {
	DrepsAt(slot uint64, fn func(*models.Drep) error) error
}

// AccountAsOfReader names the accounts whose state as of a slot may differ
// from their current one.
type AccountAsOfReader interface {
	AccountRefsChangedAfter(slot uint64) ([]models.StakeCredentialRef, error)
}

// DRepDepositAsOfReader lists the deposit of every DRep's latest registration
// as a rollback to slot would leave it.
type DRepDepositAsOfReader interface {
	DRepDepositsAt(
		slot uint64,
		fn func(tag uint8, cred []byte, deposit *uint64) error,
	) error
}

// PoolAsOfReader lists registered pool states as a rollback to slot would
// leave them.
type PoolAsOfReader interface {
	PoolStatesAt(slot uint64, fn func(*PoolState) error) error
}

// PoolKeyAsOfReader names the pools whose state as of a slot may differ from
// their current one, including a pool removed after the slot.
type PoolKeyAsOfReader interface {
	PoolKeysChangedAfter(slot uint64) ([][]byte, error)
}

// DRepLinkAsOfReader names the DRep delegator links whose state as of a slot
// may differ from their current one, including a link closed after the slot.
type DRepLinkAsOfReader interface {
	DRepLinksChangedAfter(slot uint64) ([][2]models.StakeCredentialRef, error)
}

// readSetAsOf runs fn against the live UTxO set as of slot. The set must
// reach slot before and after fn: the journals are pruned from the old end
// while fn reads, so a read is exact only if the floor stayed below slot.
func readSetAsOf[T any](
	ls *LedgerState,
	txn ledgerdb.Txn,
	slot uint64,
	fn func() (T, error),
) (T, error) {
	var zero T
	if len(ls.stagedOps(txn)) > 0 {
		return zero, fmt.Errorf(
			"%w: the transaction holds staged changes",
			ErrLedgerStateAsOfUnavailable,
		)
	}
	h, ok := ls.readSet(txn).(StateHistory)
	if !ok {
		return zero, fmt.Errorf(
			"%w: the live UTxO set keeps no state history",
			ErrLedgerStateAsOfUnavailable,
		)
	}
	if err := h.StateReachesSlot(slot); err != nil {
		return zero, err
	}
	v, err := fn()
	if err != nil {
		return zero, err
	}
	if err := h.StateReachesSlot(slot); err != nil {
		return zero, err
	}
	return v, nil
}

// The ledger-state families each as-of read's metadata store answer reads.
// A read needing several fails closed once any is drained.
var (
	drepFamilies = nodesettings.FamilyMask(0).
			With(nodesettings.FamilyDReps)
	govFamilies = nodesettings.FamilyMask(0).
			With(nodesettings.FamilyGovernance)
	poolFamilies = nodesettings.FamilyMask(0).
			With(nodesettings.FamilyPoolsHistory)
	accountFamilies = nodesettings.FamilyMask(0).
			With(nodesettings.FamilyAccounts)
)

// asOfRead is one ledger state read as of a slot: how the metadata store and
// the live UTxO set answer it, when each may, and how the answers compare.
type asOfRead[T any] struct {
	what string
	// families are the ledger-state families whose SQL rows the metadata
	// store's answer reads. The read fails closed once any is drained.
	families nodesettings.FamilyMask
	// serves reports whether set mode answers the read from the set.
	serves func() bool
	// verifies reports whether verify mode compares the read with the set.
	verifies func() bool
	// current, when set and true, says the read is of the state txn sees
	// now rather than of a past slot, so the set answers it without its
	// journals.
	current   func() bool
	compare   func(g verifyReadGuard, what string, same bool, setErr error, detail ...any)
	fromStore func() (T, error)
	fromSet   func() (T, error)
	same      func(want, got T) (bool, []any)
}

// fromSetAt reads the set for r as of slot.
func (r asOfRead[T]) fromSetAt(
	ls *LedgerState,
	txn ledgerdb.Txn,
	slot uint64,
) (T, error) {
	if r.current != nil && r.current() {
		return r.fromSet()
	}
	return readSetAsOf(ls, txn, slot, r.fromSet)
}

// answerAsOf answers r. In set mode a read the set reaches is the set's. A
// read it does not reach is the metadata store's, or, once a family it needs
// is drained, an ErrLedgerStateAsOfUnavailable error. In verify mode
// the store answers and the set's answer is compared when the set reaches the
// slot.
func answerAsOf[T any](
	ls *LedgerState,
	txn ledgerdb.Txn,
	slot uint64,
	r asOfRead[T],
) (T, error) {
	noStore := ls.familiesDrained(r.families)
	if r.serves() {
		v, err := r.fromSetAt(ls, txn, slot)
		if err == nil ||
			!errors.Is(err, ErrLedgerStateAsOfUnavailable) ||
			noStore {
			return v, err
		}
	} else if noStore {
		var zero T
		return zero, fmt.Errorf(
			"%w: the live UTxO set does not serve %s",
			ErrLedgerStateAsOfUnavailable, r.what,
		)
	}
	g := ls.verifyGuard(txn)
	want, err := r.fromStore()
	if err != nil || !r.verifies() {
		return want, err
	}
	got, setErr := r.fromSetAt(ls, txn, slot)
	if errors.Is(setErr, ErrLedgerStateAsOfUnavailable) {
		return want, nil
	}
	same := false
	var detail []any
	if setErr == nil {
		same, detail = r.same(want, got)
	}
	r.compare(g, r.what, same, setErr, detail...)
	return want, nil
}

// SetDrainedFamilies records the families whose SQL rows are gone. The mask
// only widens: a drained family's rows are never restored.
func (ls *LedgerState) SetDrainedFamilies(mask nodesettings.FamilyMask) {
	for {
		cur := ls.drainedFamilies.Load()
		next := cur | uint64(mask)
		if cur == next || ls.drainedFamilies.CompareAndSwap(cur, next) {
			return
		}
	}
}

// DrainedFamilies returns the families whose SQL rows are gone.
func (ls *LedgerState) DrainedFamilies() nodesettings.FamilyMask {
	return nodesettings.FamilyMask(ls.drainedFamilies.Load())
}

// familyDrained reports whether the metadata store no longer holds f's rows.
func (ls *LedgerState) familyDrained(f nodesettings.Family) bool {
	return ls.DrainedFamilies().Has(f)
}

// familiesDrained reports whether any family of mask is drained.
func (ls *LedgerState) familiesDrained(mask nodesettings.FamilyMask) bool {
	return slices.ContainsFunc(mask.Families(), ls.familyDrained)
}

func (ls *LedgerState) unavailableAsOf(what string) error {
	return fmt.Errorf(
		"%w: the live UTxO set cannot read %s",
		ErrLedgerStateAsOfUnavailable, what,
	)
}

func refFilter(refs []models.StakeCredentialRef) map[string]struct{} {
	if len(refs) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(refs))
	for _, r := range refs {
		m[r.MapKey()] = struct{}{}
	}
	return m
}

func wants(filter map[string]struct{}, ref models.StakeCredentialRef) bool {
	if filter == nil {
		return true
	}
	_, ok := filter[ref.MapKey()]
	return ok
}

// drepsAt is GetDrepsAtSlot from the set: the DReps among refs, every DRep
// when refs is empty, that were active as of slot.
func (ls *LedgerState) drepsAt(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	slot uint64,
) ([]*models.Drep, error) {
	r, ok := ls.drepReader(txn).(DRepAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("DReps")
	}
	filter := refFilter(refs)
	out := []*models.Drep{}
	err := r.DrepsAt(slot, func(d *models.Drep) error {
		if !d.Active || !wants(
			filter, models.NewStakeCredentialRef(d.CredentialTag, d.Credential),
		) {
			return nil
		}
		c := *d
		out = append(out, &c)
		return nil
	})
	return out, err
}

func sameDrepLists(want, got []*models.Drep) (bool, []any) {
	byKey := make(map[string]*models.Drep, len(got))
	for _, d := range got {
		byKey[models.NewStakeCredentialRef(d.CredentialTag, d.Credential).MapKey()] = d
	}
	same := len(want) == len(got)
	for _, w := range want {
		g, ok := byKey[models.NewStakeCredentialRef(w.CredentialTag, w.Credential).MapKey()]
		if !ok || !sameDrep(*w, *g) {
			same = false
			break
		}
	}
	return same, []any{"store_count", len(want), "set_count", len(got)}
}

func (s stagingLedgerDB) GetDrepsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]*models.Drep]{
		what:     fmt.Sprintf("DReps at slot %d", slot),
		families: drepFamilies,
		serves:   func() bool { return s.ls.drepServing(txn) != nil },
		verifies: func() bool { return s.ls.drepVerifying(txn) != nil },
		compare:  s.ls.compareDrepRead,
		fromStore: func() ([]*models.Drep, error) {
			return s.LedgerDB.GetDrepsAtSlot(ctx, refs, slot, txn)
		},
		fromSet: func() ([]*models.Drep, error) {
			return s.ls.drepsAt(txn, refs, slot)
		},
		same: sameDrepLists,
	})
}

// drepDelegatorsAt is GetDRepDelegatorsAtSlot from the set: the active
// accounts delegating to the credential DReps among dreps, every one when
// dreps is empty, as of slot. The candidates are the accounts that name a DRep
// now and the accounts changed since slot, the only ones that can have named
// one then.
func (ls *LedgerState) drepDelegatorsAt(
	txn ledgerdb.Txn,
	dreps []models.StakeCredentialRef,
	slot uint64,
) (map[string][]models.StakeCredentialRef, error) {
	ar := ls.accountReader(txn)
	if ar == nil {
		return nil, ls.unavailableAsOf("DRep delegators")
	}
	ah, ok := ar.(AccountAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("DRep delegators")
	}
	named, err := ar.AccountsWithDrep()
	if err != nil {
		return nil, err
	}
	changed, err := ah.AccountRefsChangedAfter(slot)
	if err != nil {
		return nil, err
	}
	filter := refFilter(dreps)
	out := make(map[string][]models.StakeCredentialRef)
	seen := make(map[string]struct{}, len(named)+len(changed))
	for _, ref := range slices.Concat(named, changed) {
		if _, dup := seen[ref.MapKey()]; dup {
			continue
		}
		seen[ref.MapKey()] = struct{}{}
		a, err := ar.AccountAt(ref.Tag, ref.Key, slot)
		if err != nil {
			return nil, err
		}
		if a == nil || !a.Active || len(a.Drep) == 0 ||
			a.DrepType > models.DrepTypeScriptHash {
			continue
		}
		drep := models.NewStakeCredentialRef(
			uint8(a.DrepType),
			a.Drep,
		) //nolint:gosec // a credential DRep type
		if wants(filter, drep) {
			out[drep.MapKey()] = append(out[drep.MapKey()], ref)
		}
	}
	return out, nil
}

func sameDelegators(
	want, got map[string][]models.StakeCredentialRef,
) (bool, []any) {
	same := len(want) == len(got)
	for k, w := range want {
		g, ok := got[k]
		if !ok || len(g) != len(w) {
			same = false
			break
		}
		have := make(map[string]struct{}, len(g))
		for _, s := range g {
			have[s.MapKey()] = struct{}{}
		}
		for _, s := range w {
			if _, ok := have[s.MapKey()]; !ok {
				same = false
			}
		}
	}
	return same, []any{"store_dreps", len(want), "set_dreps", len(got)}
}

func (s stagingLedgerDB) GetDRepDelegatorsAtSlot(
	ctx context.Context,
	dreps []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	return answerAsOf(
		s.ls,
		txn,
		slot,
		asOfRead[map[string][]models.StakeCredentialRef]{
			what:     fmt.Sprintf("DRep delegators at slot %d", slot),
			families: drepFamilies.With(nodesettings.FamilyAccounts),
			serves:   func() bool { return s.ls.drepServing(txn) != nil },
			verifies: func() bool { return s.ls.drepVerifying(txn) != nil },
			compare:  s.ls.compareDrepRead,
			fromStore: func() (map[string][]models.StakeCredentialRef, error) {
				return s.LedgerDB.GetDRepDelegatorsAtSlot(ctx, dreps, slot, txn)
			},
			fromSet: func() (map[string][]models.StakeCredentialRef, error) {
				return s.ls.drepDelegatorsAt(txn, dreps, slot)
			},
			same: sameDelegators,
		},
	)
}

// registrationHistoryServes reports whether registration reads are answered
// from the set.
func (ls *LedgerState) registrationHistoryServes(txn ledgerdb.Txn) bool {
	r := ls.registrationHistoryReader(txn)
	return r != nil && ls.setServes(
		&ls.registrationHistoryLoaded, r.RegistrationHistoryLoaded,
	)
}

// drepDepositsAt is GetDrepRegistrationDepositsAtSlot from the set.
func (ls *LedgerState) drepDepositsAt(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	slot uint64,
) (map[string]uint64, error) {
	r, ok := ls.registrationHistoryReader(txn).(DRepDepositAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("DRep deposits")
	}
	filter := refFilter(refs)
	out := make(map[string]uint64)
	err := r.DRepDepositsAt(
		slot,
		func(tag uint8, cred []byte, deposit *uint64) error {
			if deposit != nil &&
				wants(filter, models.NewStakeCredentialRef(tag, cred)) {
				out[models.DrepDepositKey(tag, cred)] = *deposit
			}
			return nil
		},
	)
	return out, err
}

func (s stagingLedgerDB) GetDrepRegistrationDepositsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[map[string]uint64]{
		what:     fmt.Sprintf("DRep deposits at slot %d", slot),
		families: drepFamilies,
		serves:   func() bool { return s.ls.registrationHistoryServes(txn) },
		verifies: func() bool {
			return s.ls.registrationHistoryReader(txn) != nil
		},
		compare: func(g verifyReadGuard, what string, same bool, setErr error, _ ...any) {
			s.ls.compareRegistrationRead(g, what, same, setErr)
		},
		fromStore: func() (map[string]uint64, error) {
			return s.LedgerDB.GetDrepRegistrationDepositsAtSlot(
				ctx,
				refs,
				slot,
				txn,
			)
		},
		fromSet: func() (map[string]uint64, error) {
			return s.ls.drepDepositsAt(txn, refs, slot)
		},
		same: func(want, got map[string]uint64) (bool, []any) {
			return maps.Equal(want, got), nil
		},
	})
}

// votesAt is GetGovernanceVotesAtSlot from the set: the live votes on the
// proposal with row ID proposalID as a rollback to slot would leave them, in
// row ID order. The vote rows with the state journal hold each vote's value
// at every slot inside the journal window, so no vote history is kept.
func (ls *LedgerState) votesAt(
	txn ledgerdb.Txn,
	proposalID uint,
	slot uint64,
) ([]*models.GovernanceVote, error) {
	r := ls.governanceReader(txn)
	if r == nil {
		return nil, ls.unavailableAsOf("governance votes")
	}
	txHash, idx, found, err := ls.proposalKeyByID(r, txn, proposalID)
	if err != nil {
		return nil, err
	}
	out := []*models.GovernanceVote{}
	if !found {
		return out, nil
	}
	rows, err := r.VotesAt(txHash, idx, &slot)
	if err != nil {
		return nil, err
	}
	for _, v := range rows {
		if v.DeletedSlot == nil {
			c := cleanVote(v)
			out = append(out, &c)
		}
	}
	slices.SortFunc(out, func(a, b *models.GovernanceVote) int {
		return cmp.Compare(a.ID, b.ID)
	})
	return out, nil
}

// sameVotesAt compares the votes a read as of a slot gives. The store reports
// a vote's update slot from its history, which names the cast itself for a
// vote never replaced, where the set holds none, so it is not compared.
func sameVotesAt(want, got []*models.GovernanceVote) (bool, []any) {
	byVoter := make(map[string]*models.GovernanceVote, len(got))
	for _, v := range got {
		byVoter[voterKey(*v)] = v
	}
	same := len(want) == len(got)
	for _, w := range want {
		g, ok := byVoter[voterKey(*w)]
		if !ok || g.ID != w.ID || g.ProposalID != w.ProposalID ||
			g.Vote != w.Vote || g.AnchorURL != w.AnchorURL ||
			!bytes.Equal(g.AnchorHash, w.AnchorHash) ||
			g.AddedSlot != w.AddedSlot {
			same = false
			break
		}
	}
	return same, []any{"store_votes", len(want), "set_votes", len(got)}
}

func (s stagingLedgerDB) GetGovernanceVotesAtSlot(
	ctx context.Context,
	proposalID uint,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]*models.GovernanceVote]{
		what: fmt.Sprintf(
			"votes on proposal %d at slot %d",
			proposalID,
			slot,
		),
		families: govFamilies,
		serves: func() bool {
			r, err := s.ls.governanceServing(txn)
			return err == nil && r != nil
		},
		verifies: func() bool {
			_, mode := s.ls.utxoSetReader()
			return mode != UtxoSetReadsOff && s.ls.governanceReader(txn) != nil
		},
		compare: s.ls.compareGovernanceRead,
		fromStore: func() ([]*models.GovernanceVote, error) {
			return s.LedgerDB.GetGovernanceVotesAtSlot(
				ctx,
				proposalID,
				slot,
				txn,
			)
		},
		fromSet: func() ([]*models.GovernanceVote, error) {
			return s.ls.votesAt(txn, proposalID, slot)
		},
		same: sameVotesAt,
	})
}

// poolHistoryServes returns the pool history reader when registration reads
// are answered from the set.
func (ls *LedgerState) poolHistoryServes(txn ledgerdb.Txn) PoolHistoryReader {
	r := ls.poolHistoryReader(txn)
	if r == nil || !ls.setServes(&ls.poolHistoryLoaded, r.PoolHistoryLoaded) {
		return nil
	}
	return r
}

// poolRegistrationsAt is GetPoolRegistrationsAtSlot from the pool history:
// each pool's newest registration certificate at or before slot, without the
// metadata store's row IDs. A Leios certified registration takes effect at
// its effective slot, which the history keeps.
func (ls *LedgerState) poolRegistrationsAt(
	txn ledgerdb.Txn,
	pools []lcommon.PoolKeyHash,
	slot uint64,
) ([]models.PoolRegistration, error) {
	r := ls.poolHistoryReader(txn)
	if r == nil {
		return nil, ls.unavailableAsOf("pool registrations")
	}
	out := []models.PoolRegistration{}
	seen := make(map[string]struct{}, len(pools))
	for _, pkh := range pools {
		if _, dup := seen[string(pkh[:])]; dup {
			continue
		}
		seen[string(pkh[:])] = struct{}{}
		events, err := r.PoolHistory(pkh[:])
		if err != nil {
			return nil, err
		}
		if reg := latestPoolRegistrationEffectiveAt(events, slot); reg != nil {
			out = append(out, *CleanPoolRegistration(reg))
		}
	}
	return out, nil
}

func samePoolRegistrations(want, got []models.PoolRegistration) (bool, []any) {
	byPool := make(map[string]*models.PoolRegistration, len(got))
	for i := range got {
		byPool[string(got[i].PoolKeyHash)] = &got[i]
	}
	same := len(want) == len(got)
	for i := range want {
		if !samePoolRegistration(
			&want[i],
			byPool[string(want[i].PoolKeyHash)],
		) {
			same = false
			break
		}
	}
	return same, []any{"store_count", len(want), "set_count", len(got)}
}

func (s stagingLedgerDB) GetPoolRegistrationsAtSlot(
	pools []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]models.PoolRegistration]{
		what:     fmt.Sprintf("pool registrations at slot %d", slot),
		families: poolFamilies,
		serves:   func() bool { return s.ls.poolHistoryServes(txn) != nil },
		verifies: func() bool { return s.ls.poolHistoryReader(txn) != nil },
		compare: func(g verifyReadGuard, what string, same bool, setErr error, _ ...any) {
			s.ls.comparePoolRead(g, what, same, setErr)
		},
		fromStore: func() ([]models.PoolRegistration, error) {
			return s.LedgerDB.GetPoolRegistrationsAtSlot(pools, slot, txn)
		},
		fromSet: func() ([]models.PoolRegistration, error) {
			return s.ls.poolRegistrationsAt(txn, pools, slot)
		},
		same: samePoolRegistrations,
	})
}

// retiringPoolsAt is GetRetiringPoolsAtSlot from the pool states as of slot:
// the registered pools whose pending retirement is after currentEpoch.
func (ls *LedgerState) retiringPoolsAt(
	txn ledgerdb.Txn,
	currentEpoch, slot uint64,
) ([]models.PoolRetiringRow, error) {
	r, ok := ls.poolReader(txn).(PoolAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("retiring pools")
	}
	out := []models.PoolRetiringRow{}
	err := r.PoolStatesAt(slot, func(st *PoolState) error {
		if st.RetiringEpoch != nil && *st.RetiringEpoch > currentEpoch {
			out = append(out, models.PoolRetiringRow{
				PoolKeyHash: bytes.Clone(st.Pool),
				Epoch:       *st.RetiringEpoch,
			})
		}
		return nil
	})
	slices.SortFunc(out, func(a, b models.PoolRetiringRow) int {
		return cmp.Or(
			cmp.Compare(a.Epoch, b.Epoch),
			bytes.Compare(a.PoolKeyHash, b.PoolKeyHash),
		)
	})
	return out, err
}

func sameRetiringPools(want, got []models.PoolRetiringRow) (bool, []any) {
	byPool := make(map[string]uint64, len(got))
	for _, r := range got {
		byPool[string(r.PoolKeyHash)] = r.Epoch
	}
	same := len(want) == len(got)
	for _, w := range want {
		if e, ok := byPool[string(w.PoolKeyHash)]; !ok || e != w.Epoch {
			same = false
			break
		}
	}
	return same, []any{"store_count", len(want), "set_count", len(got)}
}

func (s stagingLedgerDB) GetRetiringPoolsAtSlot(
	currentEpoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]models.PoolRetiringRow]{
		what:     fmt.Sprintf("retiring pools at slot %d", slot),
		families: poolFamilies,
		serves:   func() bool { return s.ls.poolServing(txn) != nil },
		verifies: func() bool { return s.ls.poolVerifyReader(txn) != nil },
		compare: func(g verifyReadGuard, what string, same bool, setErr error, _ ...any) {
			s.ls.comparePoolRead(g, what, same, setErr)
		},
		fromStore: func() ([]models.PoolRetiringRow, error) {
			return s.LedgerDB.GetRetiringPoolsAtSlot(currentEpoch, slot, txn)
		},
		fromSet: func() ([]models.PoolRetiringRow, error) {
			return s.ls.retiringPoolsAt(txn, currentEpoch, slot)
		},
		same: sameRetiringPools,
	})
}

// poolDepositsAt is GetPoolDepositsHeldAtSlot from the pool states as of
// slot.
func (ls *LedgerState) poolDepositsAt(
	txn ledgerdb.Txn,
	pools []lcommon.PoolKeyHash,
	slot uint64,
) (map[string]uint64, error) {
	r := ls.poolReader(txn)
	if r == nil {
		return nil, ls.unavailableAsOf("pool deposits")
	}
	out := make(map[string]uint64, len(pools))
	for _, pkh := range pools {
		st, err := r.PoolStateAt(pkh[:], slot)
		if err != nil {
			return nil, err
		}
		if st != nil && st.DepositHeld != nil {
			out[string(pkh[:])] = *st.DepositHeld
		}
	}
	return out, nil
}

func (s stagingLedgerDB) GetPoolDepositsHeldAtSlot(
	pools []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[map[string]uint64]{
		what:     fmt.Sprintf("pool deposits at slot %d", slot),
		families: poolFamilies,
		serves:   func() bool { return s.ls.poolServing(txn) != nil },
		verifies: func() bool { return s.ls.poolVerifyReader(txn) != nil },
		compare: func(g verifyReadGuard, what string, same bool, setErr error, _ ...any) {
			s.ls.comparePoolRead(g, what, same, setErr)
		},
		fromStore: func() (map[string]uint64, error) {
			return s.LedgerDB.GetPoolDepositsHeldAtSlot(pools, slot, txn)
		},
		fromSet: func() (map[string]uint64, error) {
			return s.ls.poolDepositsAt(txn, pools, slot)
		},
		same: func(want, got map[string]uint64) (bool, []any) {
			return maps.Equal(want, got), nil
		},
	})
}

// accountsAt is GetAccountsByCredentialAtSlot from the set: the accounts among
// refs that were active as of slot.
func (ls *LedgerState) accountsAt(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	slot uint64,
) (map[string]*models.Account, error) {
	r := ls.accountReader(txn)
	if r == nil {
		return nil, ls.unavailableAsOf("accounts")
	}
	out := make(map[string]*models.Account, len(refs))
	for _, ref := range refs {
		a, err := r.AccountAt(ref.Tag, ref.Key, slot)
		if err != nil {
			return nil, err
		}
		if a != nil && a.Active {
			out[ref.MapKey()] = a
		}
	}
	return out, nil
}

func sameAccountsAt(want, got map[string]*models.Account) (bool, []any) {
	same := len(want) == len(got)
	for k, w := range want {
		g, ok := got[k]
		if !ok || !sameAccount(*w, *g) {
			same = false
			break
		}
	}
	return same, []any{"store_count", len(want), "set_count", len(got)}
}

func (s stagingLedgerDB) GetAccountsByCredentialAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[map[string]*models.Account]{
		what:     fmt.Sprintf("accounts at slot %d", slot),
		families: accountFamilies,
		serves:   func() bool { return s.ls.accountServing(txn) != nil },
		verifies: func() bool {
			_, ok := s.ls.accountReadSource(txn)
			return ok
		},
		compare: s.ls.compareAccountRead,
		fromStore: func() (map[string]*models.Account, error) {
			return s.LedgerDB.GetAccountsByCredentialAtSlot(
				ctx,
				refs,
				slot,
				txn,
			)
		},
		fromSet: func() (map[string]*models.Account, error) {
			return s.ls.accountsAt(txn, refs, slot)
		},
		same: sameAccountsAt,
	})
}
