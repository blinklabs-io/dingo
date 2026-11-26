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
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// StakeRegistrationClass says whether a registration is a row of the
// metadata store's stake_registration table, the one
// GetStakeRegistrationsByCredential reads.
type StakeRegistrationClass uint8

const (
	// StakeClassUnknown is a loaded registration whose table the history
	// rows do not name.
	StakeClassUnknown StakeRegistrationClass = iota
	// StakeClassLegacy is a stake registration certificate.
	StakeClassLegacy
	// StakeClassOther is any other registration or a deregistration.
	StakeClassOther
)

// StakeEvent is a stake registration or deregistration certificate, as the
// metadata store's account registration history keeps it. Position orders
// the events of one slot as PoolEventPosition does. Deposit is nil for a
// registration whose deposit was never known.
type StakeEvent struct {
	Slot       uint64
	Position   uint64
	Registered bool
	Deposit    *uint64
	TxHash     []byte
	BlockHash  []byte
	TxSlot     uint64
	Class      StakeRegistrationClass
	// EffectiveSlot is the slot a Leios closure's certificate takes effect
	// at, its context slot, or zero when that is Slot. Slot journals and
	// rolls the event back.
	EffectiveSlot uint64
}

// effective is the slot the certificate takes effect at.
func (e StakeEvent) effective() uint64 {
	if e.EffectiveSlot != 0 {
		return e.EffectiveSlot
	}
	return e.Slot
}

// LatestStakeEventEffectiveAt returns the registration or deregistration the
// store's boundary stake queries rank latest at slot: among the events whose
// effective slot is at or before slot (<=, sqlstore/historical_stake.go:1275),
// the newest by effective slot, then applied slot, then position
// (historical_stake.go:1326, ranked_registration). It returns nil when there
// is none. The pointer stake queries rank by applied slot alone
// (sqlstore/pointer_stake.go:225-228, 592-595), so they do not use it.
func LatestStakeEventEffectiveAt(events []StakeEvent, slot uint64) *StakeEvent {
	var best *StakeEvent
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

// StakeEventChange is a stake event a commit applied.
type StakeEventChange struct {
	Ref   models.StakeCredentialRef
	Event StakeEvent
}

// DRepDepositChange is a DRep registration a commit applied. Deposit is nil
// when the registration's deposit was never known.
type DRepDepositChange struct {
	Slot       uint64
	Tag        uint8
	Credential []byte
	Deposit    *uint64
}

// RegistrationHistoryReader serves stake registration history and DRep
// registration deposits from a live UTxO set.
type RegistrationHistoryReader interface {
	// RegistrationHistoryLoaded reports whether the reader holds every event
	// up to its tip; until it does, reads fail closed when the set cannot serve.
	RegistrationHistoryLoaded() (bool, error)
	// StakeHistory returns every event of ref, oldest first.
	StakeHistory(ref models.StakeCredentialRef) ([]StakeEvent, error)
	// StakeCredentialsInRange returns each credential with an event whose
	// applied slot is in [from, to].
	StakeCredentialsInRange(
		from, to uint64,
	) ([]models.StakeCredentialRef, error)
	// DRepDeposit returns the deposit of the DRep's latest registration, nil
	// when it has none or the deposit is unknown.
	DRepDeposit(tag uint8, cred []byte) (*uint64, error)
	// DRepDepositAt is DRepDeposit as a rollback to slot would leave it.
	DRepDepositAt(tag uint8, cred []byte, slot uint64) (*uint64, error)
}

func stakeEventBefore(a, b StakeEvent) bool {
	return a.Slot < b.Slot || (a.Slot == b.Slot && a.Position < b.Position)
}

// registrationHistoryReader returns the set's registration history reader
// when history is unavailable.
func (ls *LedgerState) registrationHistoryReader(
	txn ledgerdb.Txn,
) RegistrationHistoryReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	r, ok := ls.readSet(txn).(RegistrationHistoryReader)
	if !ok {
		return nil
	}
	if !ls.registrationHistoryLoaded.Load() {
		loaded, err := r.RegistrationHistoryLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.registrationHistoryLoaded.Store(true)
	}
	return r
}

// stakeHistoryView returns ref's events as txn sees the set, oldest first:
// the committed events, cut back by each staged rollback, with the staged
// batches' events laid over them.
func (ls *LedgerState) stakeHistoryView(
	r RegistrationHistoryReader,
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
) ([]StakeEvent, error) {
	events, err := r.StakeHistory(ref)
	if err != nil {
		return nil, err
	}
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			events = slices.DeleteFunc(events, func(e StakeEvent) bool {
				return e.Slot > to
			})
		}
		for _, c := range o.stakeEvents {
			if !sameRef(c.Ref, ref) {
				continue
			}
			events = slices.DeleteFunc(events, func(e StakeEvent) bool {
				return e.Slot == c.Event.Slot && e.Position == c.Event.Position
			})
			events = append(events, c.Event)
		}
	}
	slices.SortFunc(events, func(a, b StakeEvent) int {
		if stakeEventBefore(a, b) {
			return -1
		}
		if stakeEventBefore(b, a) {
			return 1
		}
		return 0
	})
	return events, nil
}

// stakeCredentialsInRangeView returns the credentials with an event applied in
// [from, to] as txn sees the set. A staged rollback removes committed events
// and a staged event replaces one at its slot and position, so with either
// staged each candidate's history is read through stakeHistoryView.
func (ls *LedgerState) stakeCredentialsInRangeView(
	r RegistrationHistoryReader,
	txn ledgerdb.Txn,
	from, to uint64,
) ([]models.StakeCredentialRef, error) {
	committed, err := r.StakeCredentialsInRange(from, to)
	if err != nil {
		return nil, err
	}
	if from > to {
		return committed, nil
	}
	cands := make(map[string]models.StakeCredentialRef, len(committed))
	for _, ref := range committed {
		cands[ref.MapKey()] = ref
	}
	staged := false
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			staged = true
		}
		for _, c := range o.stakeEvents {
			staged = true
			if c.Event.Slot >= from && c.Event.Slot <= to {
				cands[c.Ref.MapKey()] = c.Ref
			}
		}
	}
	if !staged {
		return committed, nil
	}
	out := make([]models.StakeCredentialRef, 0, len(cands))
	for _, ref := range cands {
		events, err := ls.stakeHistoryView(r, txn, ref)
		if err != nil {
			return nil, err
		}
		if slices.ContainsFunc(events, func(e StakeEvent) bool {
			return e.Slot >= from && e.Slot <= to
		}) {
			out = append(out, ref)
		}
	}
	return out, nil
}

// drepDepositView returns the deposit of the DRep's latest registration as
// txn sees the set: the newest staged registration under the staged
// rollbacks, else the committed row as the lowest staged rollback leaves it.
func (ls *LedgerState) drepDepositView(
	r RegistrationHistoryReader,
	txn ledgerdb.Txn,
	tag uint8,
	cred []byte,
) (*uint64, error) {
	var ceiling *uint64
	var staged []DRepDepositChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(staged, func(c DRepDepositChange) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.drepDeposits {
			if c.Tag == tag && bytes.Equal(c.Credential, cred) {
				staged = append(staged, c)
			}
		}
	}
	if len(staged) > 0 {
		return staged[len(staged)-1].Deposit, nil
	}
	if ceiling != nil {
		return r.DRepDepositAt(tag, cred, *ceiling)
	}
	return r.DRepDeposit(tag, cred)
}

func (ls *LedgerState) stageStakeEvent(
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
	ev StakeEvent,
) {
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		StakeEvents: []StakeEventChange{{Ref: ref, Event: ev}},
	})
}

func (ls *LedgerState) stageDRepDeposit(
	txn ledgerdb.Txn,
	slot uint64,
	tag uint8,
	cred []byte,
	deposit *uint64,
) {
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		DRepDeposits: []DRepDepositChange{{
			Slot: slot, Tag: tag, Credential: bytes.Clone(cred), Deposit: deposit,
		}},
	})
}

// stakeCertificateEvent describes a stake registration or deregistration
// certificate as the metadata store's account registration history records
// it: the registering types take the deposit computed for the certificate
// (nil when it has none), a deregistration with an amount refunds it, and a
// legacy deregistration carries no amount, which the history reports as zero.
func stakeCertificateEvent(
	cert lcommon.Certificate,
	deposit *uint64,
) (ev StakeEvent, ok bool) {
	zero := uint64(0)
	switch c := cert.(type) {
	case *lcommon.StakeRegistrationCertificate:
		return StakeEvent{Registered: true, Deposit: deposit, Class: StakeClassLegacy}, true
	case *lcommon.RegistrationCertificate,
		*lcommon.StakeRegistrationDelegationCertificate,
		*lcommon.StakeVoteRegistrationDelegationCertificate,
		*lcommon.VoteRegistrationDelegationCertificate:
		return StakeEvent{Registered: true, Deposit: deposit, Class: StakeClassOther}, true
	case *lcommon.StakeDeregistrationCertificate:
		return StakeEvent{Deposit: &zero, Class: StakeClassOther}, true
	case *lcommon.DeregistrationCertificate:
		amount := c.Amount
		return StakeEvent{Deposit: &amount, Class: StakeClassOther}, true
	}
	return StakeEvent{}, false
}

func (e StakeEvent) action() string {
	if e.Registered {
		return "registered"
	}
	return "deregistered"
}

func (e StakeEvent) historyRow() models.AccountRegistrationHistoryRow {
	return models.AccountRegistrationHistoryRow{
		AddedSlot:  e.Slot,
		BlockIndex: uint32(e.Position >> 32),        //nolint:gosec // the position packs a uint32
		CertIndex:  uint32(e.Position & 0xffffffff), //nolint:gosec // masked to a uint32
		TxHash:     e.TxHash,
		Action:     e.action(),
		Deposit:    e.Deposit,
		TxSlot:     e.TxSlot,
		BlockHash:  e.BlockHash,
	}
}

func sameDepositPtr(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// registrationServing returns the set's registration history reader when
// those reads are answered from it and the set holds
// the history.
func (ls *LedgerState) registrationServing(
	txn ledgerdb.Txn,
) RegistrationHistoryReader {
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	return ls.registrationHistoryReader(txn)
}

// historyRows renders events, oldest first, as the metadata store lists a
// credential's history in order, under limit and offset.
func historyRows(
	events []StakeEvent,
	limit, offset int,
	order string,
) []models.AccountRegistrationHistoryRow {
	rows := make([]models.AccountRegistrationHistoryRow, 0, len(events))
	for _, e := range events {
		rows = append(rows, e.historyRow())
	}
	if !strings.EqualFold(order, "asc") {
		slices.Reverse(rows)
	}
	if offset > 0 {
		rows = rows[min(offset, len(rows)):]
	}
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

// latestStakeEventAtOrBefore is the newest event at or before slot, or nil.
func latestStakeEventAtOrBefore(
	events []StakeEvent,
	slot uint64,
) *models.AccountRegistrationHistoryRow {
	for _, e := range slices.Backward(events) {
		if e.Slot <= slot {
			row := e.historyRow()
			return &row
		}
	}
	return nil
}

// stakeRegistrationCerts is the stake registration certificates of events,
// and false when a loaded registration's table is unknown, which the set
// cannot tell from a stake registration.
func stakeRegistrationCerts(
	events []StakeEvent,
	tag uint8,
	key []byte,
) ([]lcommon.StakeRegistrationCertificate, bool, error) {
	credType := uint(lcommon.CredentialTypeAddrKeyHash)
	if tag == 1 {
		credType = lcommon.CredentialTypeScriptHash
	}
	out := []lcommon.StakeRegistrationCertificate{}
	for _, e := range events {
		switch {
		case e.Registered && e.Class == StakeClassUnknown:
			return nil, false, nil
		case e.Registered && e.Class == StakeClassLegacy:
			hash, err := lcommon.NewBlake2b224Checked(key)
			if err != nil {
				return nil, false, fmt.Errorf(
					"stake registration credential: %w", err,
				)
			}
			out = append(out, lcommon.StakeRegistrationCertificate{
				CertType: uint(lcommon.CertificateTypeStakeRegistration),
				StakeCredential: lcommon.Credential{
					CredType:   credType,
					Credential: lcommon.CredentialHash(hash),
				},
			})
		}
	}
	return out, true, nil
}

// GetAccountRegistrationHistoryByCredential returns a credential's registration
// events in the requested order, under the limit and offset.
func (s stagingLedgerDB) GetAccountRegistrationHistoryByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	limit int,
	offset int,
	order string,
	txn ledgerdb.Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		events, err := s.ls.stakeHistoryView(
			r, txn, models.NewStakeCredentialRef(credentialTag, stakeKey),
		)
		if err != nil {
			return nil, err
		}
		return historyRows(events, limit, offset, order), nil
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

// GetStakeCredentialsWithRegistrationEvents lists credentials with registration
// or deregistration events in the requested slot range.
func (s stagingLedgerDB) GetStakeCredentialsWithRegistrationEvents(
	fromSlot, toSlot uint64,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		return s.ls.stakeCredentialsInRangeView(r, txn, fromSlot, toSlot)
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

// GetLatestAccountRegistrationAtOrBefore is returns the newest
// registration or deregistration at or before the slot.
func (s stagingLedgerDB) GetLatestAccountRegistrationAtOrBefore(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	slot uint64,
	txn ledgerdb.Txn,
) (*models.AccountRegistrationHistoryRow, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		events, err := s.ls.stakeHistoryView(
			r, txn, models.NewStakeCredentialRef(credentialTag, stakeKey),
		)
		if err != nil {
			return nil, err
		}
		return latestStakeEventAtOrBefore(events, slot), nil
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

// GetStakeRegistrationsByCredential returns stake registration certificates
// represented in the set's history for the credential.
func (s stagingLedgerDB) GetStakeRegistrationsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]lcommon.StakeRegistrationCertificate, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		events, err := s.ls.stakeHistoryView(
			r, txn, models.NewStakeCredentialRef(credentialTag, stakingKey),
		)
		if err != nil {
			return nil, err
		}
		certs, ok, err := stakeRegistrationCerts(
			events, credentialTag, stakingKey,
		)
		if err != nil {
			return nil, err
		}
		if ok {
			return certs, nil
		}
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

// GetAccountsActiveAtSlot returns the requested credentials whose newest
// registration at or before the slot follows their newest deregistration.
func (s stagingLedgerDB) GetAccountsActiveAtSlot(
	stakeCredentials []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) (map[string]struct{}, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		if ar := s.ls.accountServing(txn); ar != nil {
			got := make(map[string]struct{}, len(stakeCredentials))
			for _, ref := range stakeCredentials {
				active, err := s.activeAtSlot(r, ar, txn, ref, slot)
				if err != nil {
					return nil, err
				}
				if active {
					got[ref.MapKey()] = struct{}{}
				}
			}
			return got, nil
		}
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

func (s stagingLedgerDB) activeAtSlot(
	r RegistrationHistoryReader,
	ar AccountReader,
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
	slot uint64,
) (bool, error) {
	events, err := s.ls.stakeHistoryView(r, txn, ref)
	if err != nil {
		return false, err
	}
	var reg, dereg *StakeEvent
	everDeregistered := false
	for i := range events {
		e := &events[i]
		if !e.Registered {
			everDeregistered = true
		}
		if e.Slot > slot {
			continue
		}
		if e.Registered {
			reg = e
		} else {
			dereg = e
		}
	}
	if reg != nil {
		return dereg == nil || stakeEventBefore(*dereg, *reg), nil
	}
	if dereg != nil {
		return false, nil
	}
	a, err := s.ls.accountView(ar, txn, ref.Tag, ref.Key)
	if err != nil || a == nil || a.CreatedSlot > slot {
		return false, err
	}
	return a.Active || everDeregistered, nil
}

// GetDrepLastRegistrationDeposit returns the latest registration's
// deposit.
func (s stagingLedgerDB) GetDrepLastRegistrationDeposit(
	ctx context.Context,
	credentialTag uint8,
	credential []byte,
	txn ledgerdb.Txn,
) (*uint64, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		return s.ls.drepDepositView(r, txn, credentialTag, credential)
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

// GetDrepLastRegistrationDeposits returns the latest registration
// deposit of every active DRep the set holds.
func (s stagingLedgerDB) GetDrepLastRegistrationDeposits(
	ctx context.Context,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if r := s.ls.registrationServing(txn); r != nil {
		if dr := s.ls.drepServing(txn); dr != nil {
			return s.drepDeposits(r, dr, txn)
		}
	}
	return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

// drepDeposits is the latest registration deposit of every active DRep the
// set holds, as txn sees it.
func (s stagingLedgerDB) drepDeposits(
	r RegistrationHistoryReader,
	dr DRepReader,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	got := make(map[string]uint64)
	refs, err := s.ls.drepCandidates(dr, txn)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		d, err := s.ls.drepView(dr, txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if d == nil || !d.Active {
			continue
		}
		dep, err := s.ls.drepDepositView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if dep != nil {
			got[models.DrepDepositKey(ref.Tag, ref.Key)] = *dep
		}
	}
	return got, nil
}
