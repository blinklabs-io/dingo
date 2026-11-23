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
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// ImportBaseline is the virtual registration a snapshot import, genesis or a
// catch-up import established for an account, as the metadata store's
// account_import_baseline keeps it. No certificate exists for it. Active is
// false for the state a newer snapshot left the account in, which reads as no
// registration. Deposit is nil for a baseline whose deposit was never known.
//
// Pool, Drep and DrepType are the delegations the import recorded, which a
// later certificate overwrites on the account but not here. A set whose
// baselines were written before it kept them reads them as empty.
type ImportBaseline struct {
	Ref       models.StakeCredentialRef
	Active    bool
	AddedSlot uint64
	Deposit   *uint64
	Pool      []byte
	Drep      []byte
	DrepType  uint64
}

// ImportBaselineChange is an import baseline as the change at Slot leaves it.
type ImportBaselineChange struct {
	Slot     uint64
	Baseline ImportBaseline
}

// WitnessChange is a witness of Ref at Slot: a stake-witnessing certificate
// or a reward withdrawal.
//
// Slot journals and rolls the witness back. A witness of a Leios closure takes
// effect at the closure's context slot, EffectiveSlot, zero when it is Slot.
//
// Withdrawal marks a witness that is a reward withdrawal, which changes the
// credential's reward balance where a certificate does not.
type WitnessChange struct {
	Slot          uint64
	Ref           models.StakeCredentialRef
	EffectiveSlot uint64
	Withdrawal    bool
}

// ImportBaselineReader serves import baselines from a live UTxO set that
// keeps them.
type ImportBaselineReader interface {
	ImportBaselinesLoaded() (bool, error)
	// ImportBaseline returns ref's baseline, or nil.
	ImportBaseline(ref models.StakeCredentialRef) (*ImportBaseline, error)
	// ImportBaselineAt is ImportBaseline as a rollback to slot would leave
	// it.
	ImportBaselineAt(
		ref models.StakeCredentialRef,
		slot uint64,
	) (*ImportBaseline, error)
}

// WitnessReader serves witness slots from a live UTxO set that keeps them.
type WitnessReader interface {
	WitnessSlotsLoaded() (bool, error)
	// LastWitnessSlot returns the greatest slot at or below maxSlot at which
	// ref was witnessed.
	LastWitnessSlot(
		ref models.StakeCredentialRef,
		maxSlot uint64,
	) (uint64, bool, error)
}

// WitnessEffectiveReader serves the slot a witness takes effect at, which a
// Leios closure's context moves before the slot it was applied at.
type WitnessEffectiveReader interface {
	// LastWitnessEffectiveSlot returns the greatest effective slot at or
	// below maxSlot at which ref was witnessed.
	LastWitnessEffectiveSlot(
		ref models.StakeCredentialRef,
		maxSlot uint64,
	) (uint64, bool, error)
}

// WitnessEachReader lists every witness of a credential with the slot it was
// applied at and the slot it takes effect at.
type WitnessEachReader interface {
	WitnessSlotsLoaded() (bool, error)
	EachWitness(
		ref models.StakeCredentialRef,
		fn func(slot, effective uint64) error,
	) error
	// EachWithdrawalWitness lists the reward withdrawals of ref that a Leios
	// closure applied at a later slot than the one they take effect at.
	EachWithdrawalWitness(
		ref models.StakeCredentialRef,
		fn func(slot, effective uint64) error,
	) error
}

// registration is the import registration b reads as: none for an absent or
// inactive baseline.
func (b *ImportBaseline) registration() *models.AccountImportRegistration {
	if b == nil || !b.Active {
		return nil
	}
	r := &models.AccountImportRegistration{AddedSlot: b.AddedSlot}
	if b.Deposit != nil {
		d := *b.Deposit
		r.Deposit = &d
	}
	return r
}

// importBaselineReader returns the set's import baseline reader once it holds
// the baselines, or nil when the set does not serve them.
func (ls *LedgerState) importBaselineReader(
	txn ledgerdb.Txn,
) ImportBaselineReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	r, ok := ls.readSet(txn).(ImportBaselineReader)
	if !ok {
		return nil
	}
	if !ls.importBaselinesSetLoaded.Load() {
		loaded, err := r.ImportBaselinesLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.importBaselinesSetLoaded.Store(true)
	}
	return r
}

// importBaselineServing returns the reader when baseline reads are answered
// from the set.
func (ls *LedgerState) importBaselineServing(
	txn ledgerdb.Txn,
) ImportBaselineReader {
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	return ls.importBaselineReader(txn)
}

// witnessReader and witnessServing are importBaselineReader and
// importBaselineServing for witness slots.
func (ls *LedgerState) witnessReader(txn ledgerdb.Txn) WitnessReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	r, ok := ls.readSet(txn).(WitnessReader)
	if !ok {
		return nil
	}
	if !ls.witnessSetLoaded.Load() {
		loaded, err := r.WitnessSlotsLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.witnessSetLoaded.Store(true)
	}
	return r
}

func (ls *LedgerState) witnessServing(txn ledgerdb.Txn) WitnessReader {
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	return ls.witnessReader(txn)
}

// importBaselineView returns ref's baseline as txn sees the set: the newest
// staged baseline no staged rollback dropped, else the committed baseline as
// the lowest staged rollback leaves it.
func (ls *LedgerState) importBaselineView(
	r ImportBaselineReader,
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
) (*ImportBaseline, error) {
	var ceiling *uint64
	var staged []ImportBaselineChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(
				staged,
				func(c ImportBaselineChange) bool {
					return c.Slot > to
				},
			)
		}
		for _, c := range o.importBaselines {
			if sameRef(c.Baseline.Ref, ref) {
				staged = append(staged, c)
			}
		}
	}
	if len(staged) > 0 {
		b := staged[len(staged)-1].Baseline
		return &b, nil
	}
	if ceiling != nil {
		return r.ImportBaselineAt(ref, *ceiling)
	}
	return r.ImportBaseline(ref)
}

// lastWitnessView returns the greatest slot at or below maxSlot at which ref
// was witnessed, as txn sees the set: the committed witnesses cut back to
// the lowest staged rollback, with the staged ones the rollbacks after them
// left laid over.
func (ls *LedgerState) lastWitnessView(
	r WitnessReader,
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
	maxSlot uint64,
) (uint64, bool, error) {
	ceiling := uint64(math.MaxUint64)
	var staged []WitnessChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			ceiling = min(ceiling, to)
			staged = slices.DeleteFunc(staged, func(c WitnessChange) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.witnesses {
			if sameRef(c.Ref, ref) {
				staged = append(staged, c)
			}
		}
	}
	last, found, err := r.LastWitnessSlot(ref, min(maxSlot, ceiling))
	if err != nil {
		return 0, false, err
	}
	for _, c := range staged {
		if c.Slot <= maxSlot && (!found || c.Slot > last) {
			last, found = c.Slot, true
		}
	}
	return last, found, nil
}

// lastWitnessSlots is AccountLastWitnessSlots over the set.
func (ls *LedgerState) lastWitnessSlots(
	r WitnessReader,
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	maxSlot uint64,
) (map[string]uint64, error) {
	out := make(map[string]uint64, len(refs))
	for _, ref := range refs {
		last, found, err := ls.lastWitnessView(r, txn, ref, maxSlot)
		if err != nil {
			return nil, err
		}
		if found {
			out[ref.MapKey()] = last
		}
	}
	return out, nil
}

// stageWitness stages a witness of ref applied at slot that takes effect at
// effective.
func (ls *LedgerState) stageWitness(
	txn ledgerdb.Txn,
	ref models.StakeCredentialRef,
	slot uint64,
	effective uint64,
	withdrawal bool,
) {
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Witnesses: []WitnessChange{{
				Slot:          slot,
				EffectiveSlot: poolEventEffectiveSlot(slot, effective),
				Withdrawal:    withdrawal,
				Ref: models.NewStakeCredentialRef(
					ref.Tag,
					bytes.Clone(ref.Key),
				),
			}},
		},
	)
}

// stageImportBaseline stages an import baseline established at b.AddedSlot.
func (ls *LedgerState) stageImportBaseline(txn ledgerdb.Txn, b ImportBaseline) {
	b.Ref = models.NewStakeCredentialRef(b.Ref.Tag, bytes.Clone(b.Ref.Key))
	b.Pool, b.Drep = bytes.Clone(b.Pool), bytes.Clone(b.Drep)
	if b.Deposit != nil {
		d := *b.Deposit
		b.Deposit = &d
	}
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			ImportBaselines: []ImportBaselineChange{
				{Slot: b.AddedSlot, Baseline: b},
			},
		},
	)
}

// GetAccountImportRegistrationByCredential returns the account's imported
// registration baseline, if one exists.
func (s stagingLedgerDB) GetAccountImportRegistrationByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	txn ledgerdb.Txn,
) (*models.AccountImportRegistration, error) {
	ref := models.NewStakeCredentialRef(credentialTag, stakeKey)
	r := s.ls.importBaselineServing(txn)
	if r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
	}
	b, err := s.ls.importBaselineView(r, txn, ref)
	if err != nil {
		return nil, err
	}
	return b.registration(), nil
}

// AccountLastWitnessSlots returns each requested credential's latest witness
// slot at or below maxSlot.
func (s stagingLedgerDB) AccountLastWitnessSlots(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	maxSlot uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	r := s.ls.witnessServing(txn)
	if r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
	}
	return s.ls.lastWitnessSlots(r, txn, refs, maxSlot)
}
