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
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// DRepReader serves DReps, their delegator links and the dormancy counter
// from a live UTxO set that computes them.
type DRepReader interface {
	DrepStateLoaded() (bool, error)
	Drep(tag uint8, cred []byte) (*models.Drep, error)
	DrepAt(tag uint8, cred []byte, slot uint64) (*models.Drep, error)
	Dreps(fn func(*models.Drep) error) error
	DRepLinkLive(drep, stake models.StakeCredentialRef) (bool, error)
	DRepLinkLiveAt(drep, stake models.StakeCredentialRef, slot uint64) (bool, error)
	DRepLinks(
		drep *models.StakeCredentialRef,
		fn func(drep, stake models.StakeCredentialRef) error,
	) error
	DormantEpochs() (uint64, error)
	DormantEpochsAt(slot uint64) (uint64, error)
}

// drepReader returns the set's DRep reader when the set computes DRep
// state.
func (ls *LedgerState) drepReader(txn ledgerdb.Txn) DRepReader {
	if ls.accountReader(txn) == nil {
		return nil
	}
	r, ok := ls.readSet(txn).(DRepReader)
	if !ok {
		return nil
	}
	return r
}

// stagedOps returns the staged batches txn sees, in order, and the lowest
// staged rollback target among them, if any.
func (ls *LedgerState) stagedOps(txn ledgerdb.Txn) []epochStateOps {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	return ls.stagedEpochStateOps(txn)
}

// drepView returns the DRep of (tag, cred) as txn sees the set.
func (ls *LedgerState) drepView(
	r DRepReader,
	txn ledgerdb.Txn,
	tag uint8,
	cred []byte,
) (*models.Drep, error) {
	var ceiling *uint64
	var staged []DRepChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(staged, func(c DRepChange) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.dreps {
			if c.Drep.CredentialTag == tag && bytes.Equal(c.Drep.Credential, cred) {
				staged = append(staged, c)
			}
		}
	}
	if len(staged) > 0 {
		last := staged[len(staged)-1]
		if last.Delete {
			return nil, nil
		}
		d := last.Drep
		return &d, nil
	}
	if ceiling != nil {
		return r.DrepAt(tag, cred, *ceiling)
	}
	return r.Drep(tag, cred)
}

func sameRef(a, b models.StakeCredentialRef) bool {
	return a.Tag == b.Tag && bytes.Equal(a.Key, b.Key)
}

// drepLinkView reports whether stake's link to drep is live as txn sees the
// set.
func (ls *LedgerState) drepLinkView(
	r DRepReader,
	txn ledgerdb.Txn,
	drep, stake models.StakeCredentialRef,
) (bool, error) {
	var ceiling *uint64
	var staged []DRepLinkChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(staged, func(c DRepLinkChange) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.drepLinks {
			if sameRef(c.Drep, drep) && sameRef(c.Stake, stake) {
				staged = append(staged, c)
			}
		}
	}
	if len(staged) > 0 {
		return staged[len(staged)-1].Live, nil
	}
	if ceiling != nil {
		return r.DRepLinkLiveAt(drep, stake, *ceiling)
	}
	return r.DRepLinkLive(drep, stake)
}

// dormancyView returns the DRep dormancy counter as txn sees the set.
func (ls *LedgerState) dormancyView(r DRepReader, txn ledgerdb.Txn) (uint64, error) {
	var ceiling *uint64
	var staged []DormancyChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(staged, func(c DormancyChange) bool {
				return c.Slot > to
			})
		}
		staged = append(staged, o.dormancy...)
	}
	if len(staged) > 0 {
		return staged[len(staged)-1].Epochs, nil
	}
	if ceiling != nil {
		return r.DormantEpochsAt(*ceiling)
	}
	return r.DormantEpochs()
}

// drepLinkCandidates returns the links the set holds, of drep alone when it
// is not nil, with those the batches txn sees staged; a caller confirms
// each with drepLinkView.
func (ls *LedgerState) drepLinkCandidates(
	r DRepReader,
	txn ledgerdb.Txn,
	drep *models.StakeCredentialRef,
) ([][2]models.StakeCredentialRef, error) {
	var out [][2]models.StakeCredentialRef
	seen := make(map[string]struct{})
	add := func(d, s models.StakeCredentialRef) {
		k := d.MapKey() + "|" + s.MapKey()
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, [2]models.StakeCredentialRef{d, s})
		}
	}
	if err := r.DRepLinks(drep, func(d, s models.StakeCredentialRef) error {
		add(d, s)
		return nil
	}); err != nil {
		return nil, err
	}
	var ceiling *uint64
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil && (ceiling == nil || *o.rollbackTo < *ceiling) {
			to := *o.rollbackTo
			ceiling = &to
		}
		for _, c := range o.drepLinks {
			if drep == nil || sameRef(c.Drep, *drep) {
				add(c.Drep, c.Stake)
			}
		}
	}
	if ceiling == nil {
		return out, nil
	}
	// A staged rollback restores the links closed after its target, which the
	// set's current rows no longer list.
	lr, ok := r.(DRepLinkAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("DRep delegators")
	}
	changed, err := lr.DRepLinksChangedAfter(*ceiling)
	if err != nil {
		return nil, err
	}
	for _, l := range changed {
		if drep == nil || sameRef(l[0], *drep) {
			add(l[0], l[1])
		}
	}
	return out, nil
}

func (ls *LedgerState) stageDrep(txn ledgerdb.Txn, slot uint64, d models.Drep) {
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		DReps: []DRepChange{{Slot: slot, Drep: d}},
	})
}

// stageDrepHistory stages the DRep history rows the metadata store records
// beside a write.
func (ls *LedgerState) stageDrepHistory(
	txn ledgerdb.Txn,
	rows ...DRepHistoryChange,
) {
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		DRepHistory: rows,
	})
}

// stageDrepExpiryHistory stages the activity and expiry epochs of d from slot
// on, which the metadata store records in drep_expiry_history.
func (ls *LedgerState) stageDrepExpiryHistory(
	txn ledgerdb.Txn,
	slot uint64,
	d models.Drep,
) {
	ls.stageDrepHistory(txn, DRepHistoryChange{
		Kind: DRepHistoryExpiry, Slot: slot, Tag: d.CredentialTag,
		Key: bytes.Clone(d.Credential), Activity: d.LastActivityEpoch,
		Expiry: d.ExpiryEpoch,
	})
}

func (ls *LedgerState) stageDrepLink(
	txn ledgerdb.Txn,
	slot uint64,
	drep, stake models.StakeCredentialRef,
	live bool,
) {
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		DRepLinks: []DRepLinkChange{
			{Slot: slot, Drep: drep, Stake: stake, Live: live},
		},
	})
}

// clearAccountDrep stages ref's DRep delegation cleared at slot, as the
// metadata store's DRep clears do.
func (ls *LedgerState) clearAccountDrep(
	r AccountReader,
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	a, err := ls.accountView(r, txn, ref.Tag, ref.Key)
	if err != nil || a == nil {
		return err
	}
	next := *a
	next.Drep, next.DrepType, next.AddedSlot = nil, 0, slot
	ls.stageDrepHistory(txn, DRepHistoryChange{
		Kind: DRepHistoryClear, Slot: slot, Tag: ref.Tag,
		Key: bytes.Clone(ref.Key),
	})
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		Accounts: []AccountChange{{Slot: slot, Account: next}},
	})
	return nil
}

// stageDrepCertificate stages a DRep registration, update or
// deregistration as setDrepCertificateState and removeAllDrepDelegators do
// in the metadata store.
func (ls *LedgerState) stageDrepCertificate(
	ar AccountReader,
	r DRepReader,
	txn ledgerdb.Txn,
	tag uint8,
	cred []byte,
	kind models.DrepCertificateKind,
	anchorURL string,
	anchorHash []byte,
	slot uint64,
	protocolMajor uint64,
) error {
	prev, err := ls.drepView(r, txn, tag, cred)
	if err != nil {
		return err
	}
	if kind != models.DrepCertificateRegistration && (prev == nil || !prev.Active) {
		return fmt.Errorf("%w: DRep %x", models.ErrDrepNotFound, cred)
	}
	next := models.Drep{CredentialTag: tag, Credential: bytes.Clone(cred)}
	if prev != nil {
		next = *prev
	}
	next.AnchorURL, next.AnchorHash = anchorURL, anchorHash
	next.AddedSlot = slot
	next.Active = kind != models.DrepCertificateDeregistration
	ls.stageDrep(txn, slot, next)
	if kind != models.DrepCertificateDeregistration {
		return nil
	}
	drep := models.NewStakeCredentialRef(tag, cred)
	links, err := ls.drepLinkCandidates(r, txn, &drep)
	if err != nil {
		return err
	}
	var clear []models.StakeCredentialRef
	if protocolMajor >= 10 {
		refs, err := ls.accountsByDrepCandidates(ar, txn, uint64(tag), cred)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			a, err := ls.accountView(ar, txn, ref.Tag, ref.Key)
			if err != nil {
				return err
			}
			if a != nil && a.Active && a.DrepType == uint64(tag) &&
				bytes.Equal(a.Drep, cred) {
				clear = append(clear, ref)
			}
		}
	} else {
		for _, l := range links {
			live, err := ls.drepLinkView(r, txn, l[0], l[1])
			if err != nil {
				return err
			}
			if live {
				clear = append(clear, l[1])
			}
		}
	}
	for _, ref := range clear {
		if err := ls.clearAccountDrep(ar, txn, ref, slot); err != nil {
			return err
		}
	}
	for _, l := range links {
		live, err := ls.drepLinkView(r, txn, l[0], l[1])
		if err != nil {
			return err
		}
		if live {
			ls.stageDrepLink(txn, slot, l[0], l[1], false)
		}
	}
	return nil
}

// accountsByDrepCandidates returns the active accounts the set's DRep index
// names for (typ, drep), with those the batches txn sees staged; a caller
// confirms each with accountView.
func (ls *LedgerState) accountsByDrepCandidates(
	ar AccountReader,
	txn ledgerdb.Txn,
	typ uint64,
	drep []byte,
) ([]models.StakeCredentialRef, error) {
	refs, err := ar.AccountsByDrep(typ, drep)
	if err != nil {
		return nil, err
	}
	return ls.accountCandidates(ar, txn, refs, refSet(refs))
}

// stageDrepLinkUpdate keeps the delegator links in step with an account
// certificate that rewrites the account's DRep delegation, as
// applyAccountCertificate does: it closes the link to the prior DRep when
// models.ShouldRemovePriorDrepDelegation says so, and opens one to a newly
// named credential DRep that is registered.
func (ls *LedgerState) stageDrepLinkUpdate(
	r DRepReader,
	txn ledgerdb.Txn,
	stake models.StakeCredentialRef,
	prev *models.Account,
	effect models.AccountDelegation,
	slot uint64,
	protocolMajor uint64,
) error {
	var oldDrep []byte
	var oldType uint64
	if prev != nil {
		oldDrep, oldType = prev.Drep, prev.DrepType
	}
	if models.ShouldRemovePriorDrepDelegation(
		protocolMajor, oldType, len(oldDrep) > 0,
		effect.DrepType, len(effect.Drep) > 0,
	) {
		old := models.NewStakeCredentialRef(uint8(oldType), oldDrep) //nolint:gosec // a DRep credential type
		live, err := ls.drepLinkView(r, txn, old, stake)
		if err != nil {
			return err
		}
		if live {
			ls.stageDrepLink(txn, slot, old, stake, false)
		}
	}
	if len(effect.Drep) == 0 || effect.DrepType > models.DrepTypeScriptHash {
		return nil
	}
	target, err := ls.drepView(r, txn, uint8(effect.DrepType), effect.Drep) //nolint:gosec // a DRep credential type
	if err != nil || target == nil || !target.Active {
		return err
	}
	link := models.NewStakeCredentialRef(uint8(effect.DrepType), effect.Drep) //nolint:gosec // a DRep credential type
	live, err := ls.drepLinkView(r, txn, link, stake)
	if err != nil || live {
		return err
	}
	ls.stageDrepLink(txn, slot, link, stake, true)
	return nil
}

// drepStaging returns the readers DRep staging needs, or false when the set
// does not compute DRep state or has stopped following the ledger.
func (ls *LedgerState) drepStaging(
	txn ledgerdb.Txn,
) (AccountReader, DRepReader) {
	ar := ls.accountReader(txn)
	if ar == nil || ls.utxoSetFailed.Load() {
		return nil, nil
	}
	r := ls.drepReader(txn)
	if r == nil {
		return nil, nil
	}
	return ar, r
}

// drepsUnserved is the error of a DRep write the set cannot stage: no SQL
// row holds it, so the write would be lost.
func (s stagingLedgerDB) drepsUnserved() error {
	return ledgerStateUnserved(nodesettings.FamilyDReps)
}

// failDrepStaging stops the set following the ledger and, since the set
// alone holds the write, returns err so the write is not lost.
func (s stagingLedgerDB) failDrepStaging(txn ledgerdb.Txn, err error) error {
	s.ls.failAccountStaging(txn, err)
	return err
}

func (s stagingLedgerDB) UpdateDRepActivity(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	activityEpoch uint64,
	inactivityPeriod uint64,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	_, r := s.ls.drepStaging(txn)
	if r == nil {
		return s.drepsUnserved()
	}
	d, err := s.ls.drepView(r, txn, credentialTag, drepCredential)
	if err != nil {
		return s.failDrepStaging(txn, err)
	}
	if d == nil {
		// The metadata store's answer for a missing DRep, which vote
		// processing repairs by recreating the record; nothing is staged.
		return models.ErrDrepActivityNotUpdated
	}
	next := *d
	next.LastActivityEpoch = activityEpoch
	next.ExpiryEpoch = activityEpoch + inactivityPeriod
	s.ls.stageDrep(txn, slot, next)              //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	s.ls.stageDrepExpiryHistory(txn, slot, next) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}

// RecordDRepActivityEpoch refuses. It records a DRep's activity epoch while
// keeping the expiry a Mithril snapshot recorded, for replay below the
// snapshot's anchor, which only the IndexerDB writer runs; the ledger applies
// activity with UpdateDRepActivity, which journals the expiry at its slot.
func (s stagingLedgerDB) RecordDRepActivityEpoch(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	activityEpoch uint64,
	txn ledgerdb.Txn,
) error {
	return errDRepActivityEpochReplayOnly
}

var errDRepActivityEpochReplayOnly = errors.New(
	"DRep activity epochs below a Mithril anchor are recorded by the IndexerDB writer",
)

func (s stagingLedgerDB) InsertDrepIfAbsent(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn ledgerdb.Txn,
) error {
	_, r := s.ls.drepStaging(txn)
	if r == nil {
		return s.drepsUnserved()
	}
	d, err := s.ls.drepView(r, txn, credentialTag, cred)
	if err != nil {
		return s.failDrepStaging(txn, err)
	}
	if d != nil {
		return nil
	}
	s.ls.stageDrep(txn, slot, models.Drep{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		CredentialTag: credentialTag,
		Credential:    bytes.Clone(cred),
		AnchorURL:     url,
		AnchorHash:    hash,
		AddedSlot:     slot,
		Active:        active,
	})
	return nil
}

func (s stagingLedgerDB) ResetDormantDRepEpochs(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	_, r := s.ls.drepStaging(txn)
	if r == nil {
		return s.drepsUnserved()
	}
	n, err := s.ls.dormancyView(r, txn)
	if err != nil {
		return s.failDrepStaging(txn, err)
	}
	if n != 0 {
		s.ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Dormancy: []DormancyChange{{Slot: slot, Epochs: 0}},
		})
		s.ls.stageDrepHistory(txn, DRepHistoryChange{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Kind: DRepHistoryDormancy, Slot: slot, Previous: n,
		})
	}
	return nil
}

// BumpDormantDRepExpiries stages the bump when the metadata store applied
// it: it applies once per epoch, and its count does not say whether it did,
// so the dormancy counter before and after tells.
func (s stagingLedgerDB) BumpDormantDRepExpiries(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	return s.bumpDormantDRepExpiriesInSet(slot, txn) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
}

func (ls *LedgerState) stageDormantBump(r DRepReader, txn ledgerdb.Txn, slot uint64) error {
	n, err := ls.dormancyView(r, txn)
	if err != nil {
		return err
	}
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		Dormancy: []DormancyChange{{Slot: slot, Epochs: n + 1}},
	})
	ls.stageDrepHistory(
		txn,
		DRepHistoryChange{Kind: DRepHistoryEvent, Slot: slot},
		DRepHistoryChange{Kind: DRepHistoryDormancy, Slot: slot, Previous: n},
	)
	refs, err := ls.drepCandidates(r, txn)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		d, err := ls.drepView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return err
		}
		if d == nil || !d.Active || d.ExpiryEpoch == 0 {
			continue
		}
		next := *d
		next.ExpiryEpoch++
		ls.stageDrep(txn, slot, next)
		ls.stageDrepExpiryHistory(txn, slot, next)
	}
	return nil
}

// drepCandidates returns every DRep the set holds and the batches txn sees
// staged; a caller confirms each with drepView.
func (ls *LedgerState) drepCandidates(
	r DRepReader,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	var out []models.StakeCredentialRef
	seen := make(map[string]struct{})
	add := func(tag uint8, cred []byte) {
		ref := models.NewStakeCredentialRef(tag, cred)
		if _, ok := seen[ref.MapKey()]; !ok {
			seen[ref.MapKey()] = struct{}{}
			out = append(out, ref)
		}
	}
	err := r.Dreps(func(d *models.Drep) error {
		add(d.CredentialTag, d.Credential)
		return nil
	})
	for _, o := range ls.stagedOps(txn) {
		for _, c := range o.dreps {
			add(c.Drep.CredentialTag, c.Drep.Credential)
		}
	}
	return out, err
}

func (s stagingLedgerDB) ClearDanglingDRepDelegations(
	ctx context.Context,
	atSlot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	ar, r := s.ls.drepStaging(txn)
	if ar == nil || r == nil {
		return 0, s.drepsUnserved()
	}
	n, err := s.ls.stageDanglingClear(ar, r, txn, atSlot) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	if err != nil {
		return 0, s.failDrepStaging(txn, err)
	}
	return n, nil
}

// stageDanglingClear clears every account's delegation to a credential DRep
// that is not registered, closes every link that no longer matches an
// active account's delegation to a registered DRep, and opens the missing
// ones, as clearDanglingDRepDelegations does at the protocol version 10
// hard fork. It returns the number of account delegations it cleared.
func (ls *LedgerState) stageDanglingClear(
	ar AccountReader,
	r DRepReader,
	txn ledgerdb.Txn,
	slot uint64,
) (int, error) {
	cleared := 0
	refs, err := ar.AccountsWithDrep()
	if err != nil {
		return 0, err
	}
	refs, err = ls.accountCandidates(ar, txn, refs, refSet(refs))
	if err != nil {
		return 0, err
	}
	registered := func(typ uint64, cred []byte) (bool, error) {
		d, err := ls.drepView(r, txn, uint8(typ), cred) //nolint:gosec // a DRep credential type
		return d != nil && d.Active, err
	}
	for _, ref := range refs {
		a, err := ls.accountView(ar, txn, ref.Tag, ref.Key)
		if err != nil {
			return 0, err
		}
		if a == nil || len(a.Drep) == 0 || a.DrepType > models.DrepTypeScriptHash {
			continue
		}
		ok, err := registered(a.DrepType, a.Drep)
		if err != nil {
			return 0, err
		}
		if !ok {
			if err := ls.clearAccountDrep(ar, txn, ref, slot); err != nil {
				return 0, err
			}
			cleared++
		}
	}
	links, err := ls.drepLinkCandidates(r, txn, nil)
	if err != nil {
		return 0, err
	}
	for _, l := range links {
		live, err := ls.drepLinkView(r, txn, l[0], l[1])
		if err != nil || !live {
			if err != nil {
				return 0, err
			}
			continue
		}
		a, err := ls.accountView(ar, txn, l[1].Tag, l[1].Key)
		if err != nil {
			return 0, err
		}
		keep := a != nil && a.Active && a.DrepType <= models.DrepTypeScriptHash &&
			a.DrepType == uint64(l[0].Tag) && bytes.Equal(a.Drep, l[0].Key)
		if keep {
			if keep, err = registered(a.DrepType, a.Drep); err != nil {
				return 0, err
			}
		}
		if !keep {
			ls.stageDrepLink(txn, slot, l[0], l[1], false)
		}
	}
	for _, ref := range refs {
		a, err := ls.accountView(ar, txn, ref.Tag, ref.Key)
		if err != nil {
			return 0, err
		}
		if a == nil || !a.Active || len(a.StakingKey) == 0 || len(a.Drep) == 0 ||
			a.DrepType > models.DrepTypeScriptHash {
			continue
		}
		ok, err := registered(a.DrepType, a.Drep)
		if err != nil || !ok {
			if err != nil {
				return 0, err
			}
			continue
		}
		link := models.NewStakeCredentialRef(uint8(a.DrepType), a.Drep) //nolint:gosec // a DRep credential type
		live, err := ls.drepLinkView(r, txn, link, ref)
		if err != nil {
			return 0, err
		}
		if !live {
			ls.stageDrepLink(txn, slot, link, ref, true)
		}
	}
	return cleared, nil
}

// drepServing returns the DRep reader when DRep reads are answered from the
// set, or nil when the set does not serve them.
func (ls *LedgerState) drepServing(txn ledgerdb.Txn) DRepReader {
	_, r := ls.drepStaging(txn)
	if r == nil || !ls.setServes(&ls.drepsSetLoaded, r.DrepStateLoaded) {
		return nil
	}
	return r
}

func (s stagingLedgerDB) GetDrepByCredential(
	ctx context.Context,
	credentialTag uint8,
	cred []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Drep, error) {
	r := s.ls.drepServing(txn)
	if r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyDReps)
	}
	d, err := s.ls.drepView(r, txn, credentialTag, cred)
	if err != nil {
		return nil, err
	}
	if d == nil || !includeInactive && !d.Active {
		return nil, models.ErrDrepNotFound
	}
	return d, nil
}

func (s stagingLedgerDB) GetActiveDreps(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	r := s.ls.drepServing(txn)
	if r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyDReps)
	}
	return s.ls.activeDrepsView(r, txn)
}

// activeDrepsView is the active DReps as txn sees the set. The metadata
// store returns them in no particular order, and so does this.
func (ls *LedgerState) activeDrepsView(
	r DRepReader,
	txn ledgerdb.Txn,
) ([]*models.Drep, error) {
	refs, err := ls.drepCandidates(r, txn)
	if err != nil {
		return nil, err
	}
	var out []*models.Drep
	for _, ref := range refs {
		d, err := ls.drepView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if d != nil && d.Active {
			out = append(out, d)
		}
	}
	return out, nil
}

func (s stagingLedgerDB) GetDormantDRepEpochs(
	ctx context.Context,
	txn ledgerdb.Txn,
) (uint64, error) {
	r := s.ls.drepServing(txn)
	if r == nil {
		return 0, s.ls.setFamilyUnserved(nodesettings.FamilyDReps)
	}
	return s.ls.dormancyView(r, txn)
}

// bumpDormantDRepExpiriesInSet applies the epoch's dormancy bump to the set
// alone. The metadata store applies it once per boundary slot, so the bump
// is skipped when the set already holds a counter change at slot, staged in
// txn or committed, that no staged rollback undoes.
func (s stagingLedgerDB) bumpDormantDRepExpiriesInSet(
	slot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	_, r := s.ls.drepStaging(txn)
	if r == nil {
		return 0, ledgerStateUnserved(nodesettings.FamilyDReps)
	}
	var staged []DormancyChange
	var ceiling *uint64
	for _, o := range s.ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(staged, func(c DormancyChange) bool {
				return c.Slot > to
			})
		}
		staged = append(staged, o.dormancy...)
	}
	if slices.ContainsFunc(staged, func(c DormancyChange) bool {
		return c.Slot == slot
	}) {
		return 0, nil
	}
	if slot > 0 {
		// A staged rollback below slot leaves the committed counter as of
		// its target, so a bump the rollback undoes is not yet applied.
		upTo := func(at uint64) uint64 {
			if ceiling != nil {
				return min(at, *ceiling)
			}
			return at
		}
		at, err := r.DormantEpochsAt(upTo(slot))
		if err != nil {
			return 0, err
		}
		before, err := r.DormantEpochsAt(upTo(slot - 1))
		if err != nil {
			return 0, err
		}
		if at != before {
			return 0, nil
		}
	}
	if err := s.ls.stageDormantBump(r, txn, slot); err != nil { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		return 0, err
	}
	return 0, nil
}
