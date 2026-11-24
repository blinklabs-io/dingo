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
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrLedgerStateAsOfUnavailable reports a ledger state read as of a past slot
// that the live UTxO set cannot answer: the slot is older than its journals,
// or the transaction holds staged changes the journals do not describe. It is
// returned when the configured set cannot answer the requested historical state.
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

// asOfRead is one ledger state read as of a slot, which the live UTxO set
// answers.
type asOfRead[T any] struct {
	what string
	// serves reports whether the set answers the read.
	serves func() bool
	// current, when set and true, says the read is of the state txn sees
	// now rather than of a past slot, so the set answers it without its
	// journals.
	current func() bool
	fromSet func() (T, error)
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

// answerAsOf reads from the configured set and returns an error when it cannot
// serve the requested state.
func answerAsOf[T any](
	ls *LedgerState,
	txn ledgerdb.Txn,
	slot uint64,
	r asOfRead[T],
) (T, error) {
	if !r.serves() {
		var zero T
		return zero, fmt.Errorf(
			"%w: the live UTxO set does not serve %s",
			ErrLedgerStateAsOfUnavailable, r.what,
		)
	}
	return r.fromSetAt(ls, txn, slot)
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

func (s stagingLedgerDB) GetDrepsAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]*models.Drep]{
		what:   fmt.Sprintf("DReps at slot %d", slot),
		serves: func() bool { return s.ls.drepServing(txn) != nil },
		fromSet: func() ([]*models.Drep, error) {
			return s.ls.drepsAt(txn, refs, slot)
		},
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
			what:   fmt.Sprintf("DRep delegators at slot %d", slot),
			serves: func() bool { return s.ls.drepServing(txn) != nil },
			fromSet: func() (map[string][]models.StakeCredentialRef, error) {
				return s.ls.drepDelegatorsAt(txn, dreps, slot)
			},
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
		what:   fmt.Sprintf("DRep deposits at slot %d", slot),
		serves: func() bool { return s.ls.registrationHistoryServes(txn) },
		fromSet: func() (map[string]uint64, error) {
			return s.ls.drepDepositsAt(txn, refs, slot)
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
		serves: func() bool {
			r, err := s.ls.governanceServing(txn)
			return err == nil && r != nil
		},
		fromSet: func() ([]*models.GovernanceVote, error) {
			return s.ls.votesAt(txn, proposalID, slot)
		},
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

func (s stagingLedgerDB) GetPoolRegistrationsAtSlot(
	pools []lcommon.PoolKeyHash,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]models.PoolRegistration]{
		what:   fmt.Sprintf("pool registrations at slot %d", slot),
		serves: func() bool { return s.ls.poolHistoryServes(txn) != nil },
		fromSet: func() ([]models.PoolRegistration, error) {
			return s.ls.poolRegistrationsAt(txn, pools, slot)
		},
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

func (s stagingLedgerDB) GetRetiringPoolsAtSlot(
	currentEpoch uint64,
	slot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[[]models.PoolRetiringRow]{
		what:   fmt.Sprintf("retiring pools at slot %d", slot),
		serves: func() bool { return s.ls.poolServing(txn) != nil },
		fromSet: func() ([]models.PoolRetiringRow, error) {
			return s.ls.retiringPoolsAt(txn, currentEpoch, slot)
		},
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
		what:   fmt.Sprintf("pool deposits at slot %d", slot),
		serves: func() bool { return s.ls.poolServing(txn) != nil },
		fromSet: func() (map[string]uint64, error) {
			return s.ls.poolDepositsAt(txn, pools, slot)
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
	rr := ls.rewardOutputReader(txn)
	if rr == nil {
		return nil, ls.unavailableAsOf("reward outputs")
	}
	out := make(map[string]*models.Account, len(refs))
	for _, ref := range refs {
		a, err := r.AccountAt(ref.Tag, ref.Key, slot)
		if err != nil {
			return nil, err
		}
		if a == nil || !a.Active {
			continue
		}
		// The journaled row lacks the credits of rounds not yet folded
		// into it, which the metadata store's answer adds.
		pending, err := pendingRewardCreditAt(rr, ref.Tag, ref.Key, slot)
		if err != nil {
			return nil, err
		}
		reward, overflow := addRewardUint64(uint64(a.Reward), pending)
		if overflow {
			return nil, fmt.Errorf(
				"reward account balance overflow for %x", ref.Key,
			)
		}
		a.Reward = types.Uint64(reward)
		out[ref.MapKey()] = a
	}
	return out, nil
}

func (s stagingLedgerDB) GetAccountsByCredentialAtSlot(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	return answerAsOf(s.ls, txn, slot, asOfRead[map[string]*models.Account]{
		what:   fmt.Sprintf("accounts at slot %d", slot),
		serves: func() bool { return s.ls.accountServing(txn) != nil },
		fromSet: func() (map[string]*models.Account, error) {
			return s.ls.accountsAt(txn, refs, slot)
		},
	})
}
