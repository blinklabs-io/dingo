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
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
)

// RewardOutputChangeKind is what a RewardOutputChange does.
type RewardOutputChangeKind uint8

const (
	// RewardOutputPut sets the credit-eligible output row.
	RewardOutputPut RewardOutputChangeKind = iota
	// RewardOutputDelete removes the row: the output was folded into its
	// account, or is no longer eligible.
	RewardOutputDelete
	// RewardOutputWipe removes every row of the output's epoch.
	RewardOutputWipe
)

// RewardOutputChange is a write to a reward account output the set keeps. The
// set keeps an output only while it is spendable, unguarded and unfolded.
// Slot is the slot a rollback restores the row at: the output's captured slot
// for a put and its boundary slot for the fold that deletes it. A wipe is
// never undone by a rollback.
type RewardOutputChange struct {
	Kind   RewardOutputChangeKind
	Slot   uint64
	Output models.RewardAccountOutput
}

// RewardRoundChange is a reward credit round a boundary applied, restored
// by a rollback below its boundary slot.
type RewardRoundChange struct {
	Round models.RewardCreditRound
}

// PostSnapshotCredit is a reward credit a boundary applied after its SNAP
// point. A Loaded credit is the total the metadata store held for the
// credential at Slot when the set loaded it, with no source.
type PostSnapshotCredit struct {
	Slot   uint64
	Ref    models.StakeCredentialRef
	Source []byte
	Amount uint64
	Loaded bool
}

// RewardOutputRef identifies a reward account output.
type RewardOutputRef struct {
	Tag   uint8
	Key   []byte
	Epoch uint64
	Pool  []byte
	Type  string
}

// RewardOutputReader serves reward account outputs, credited rounds and
// post-snapshot credits from a live UTxO set that keeps them. A ceiling reads
// the rows as a rollback to that slot would leave them.
type RewardOutputReader interface {
	// RewardOutputsLoaded reports whether the reader holds every row up to
	// its tip; until it does, reads stay on the metadata store.
	RewardOutputsLoaded() (bool, error)
	// RewardRounds returns every credited round, oldest first.
	RewardRounds(ceiling *uint64) ([]models.RewardCreditRound, error)
	// RewardRound returns the credited round of epoch.
	RewardRound(
		epoch uint64,
		ceiling *uint64,
	) (models.RewardCreditRound, bool, error)
	// CredentialRewardOutputs returns every output row of one credential.
	CredentialRewardOutputs(
		tag uint8,
		key []byte,
		ceiling *uint64,
	) ([]models.RewardAccountOutput, error)
	// RewardOutputEpochs returns the epochs that have an output row.
	RewardOutputEpochs(ceiling *uint64) ([]uint64, error)
	// EpochRewardOutputs returns up to limit outputs of epoch.
	EpochRewardOutputs(
		epoch uint64,
		limit int,
		ceiling *uint64,
	) ([]RewardOutputRef, error)
	// PostSnapshotCredits returns the credits at slot.
	PostSnapshotCredits(
		slot uint64,
		ceiling *uint64,
	) ([]PostSnapshotCredit, error)
}

// rewardOutputReader returns the set's reward output reader once it holds the
// rows, or nil when reads stay on the metadata store.
func (ls *LedgerState) rewardOutputReader(txn ledgerdb.Txn) RewardOutputReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	r, ok := ls.readSet(txn).(RewardOutputReader)
	if !ok {
		return nil
	}
	if !ls.rewardOutputsLoaded.Load() {
		loaded, err := r.RewardOutputsLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.rewardOutputsLoaded.Store(true)
	}
	return r
}

// rewardOutputServing returns the set's reward output reader when the reward
// reads are answered from it and the set holds the rows.
func (ls *LedgerState) rewardOutputServing(
	txn ledgerdb.Txn,
) RewardOutputReader {
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	return ls.rewardOutputReader(txn)
}

// rewardStaged is what the batches a transaction sees did to the reward rows,
// in order, with the changes a staged rollback undoes dropped.
type rewardStaged struct {
	ceiling *uint64
	outputs []RewardOutputChange
	rounds  []RewardRoundChange
	credits []PostSnapshotCredit
}

// rewardStagedOps returns txn's staged reward changes. The caller holds
// utxoSetApplyMu for reading.
func (ls *LedgerState) rewardStagedOps(txn ledgerdb.Txn) rewardStaged {
	var st rewardStaged
	for _, o := range ls.stagedEpochStateOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if st.ceiling == nil || to < *st.ceiling {
				st.ceiling = &to
			}
			st.outputs = slices.DeleteFunc(st.outputs, func(c RewardOutputChange) bool {
				return c.Slot > to
			})
			st.rounds = slices.DeleteFunc(st.rounds, func(c RewardRoundChange) bool {
				return c.Round.BoundarySlot > to
			})
			st.credits = slices.DeleteFunc(st.credits, func(c PostSnapshotCredit) bool {
				return c.Slot > to
			})
		}
		st.outputs = append(st.outputs, o.rewardOutputs...)
		st.rounds = append(st.rounds, o.rewardRounds...)
		st.credits = append(st.credits, o.postSnapshot...)
	}
	return st
}

type rewardOutputID struct {
	epoch uint64
	pool  string
	typ   string
}

func rewardOutputIDOf(o *models.RewardAccountOutput) rewardOutputID {
	return rewardOutputID{o.Epoch, string(o.PoolKeyHash), o.RewardType}
}

func sameRewardCredential(o *models.RewardAccountOutput, tag uint8, key []byte) bool {
	return o.CredentialTag == tag && bytes.Equal(o.StakingKey, key)
}

// rewardRoundSeenBy reports whether epoch's round is credited as txn sees
// the set.
func (ls *LedgerState) rewardRoundSeenBy(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	epoch uint64,
) (bool, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	return roundCreditedLocked(r, ls.rewardStagedOps(txn), epoch)
}

// rewardRoundsView returns the credited rounds as txn sees the set, oldest
// first.
func (ls *LedgerState) rewardRoundsView(
	r RewardOutputReader,
	txn ledgerdb.Txn,
) ([]models.RewardCreditRound, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	return ls.rewardRoundsViewLocked(r, ls.rewardStagedOps(txn))
}

func (ls *LedgerState) rewardRoundsViewLocked(
	r RewardOutputReader,
	st rewardStaged,
) ([]models.RewardCreditRound, error) {
	committed, err := r.RewardRounds(st.ceiling)
	if err != nil {
		return nil, err
	}
	byEpoch := make(map[uint64]models.RewardCreditRound, len(committed))
	for _, c := range committed {
		byEpoch[c.SnapshotEpoch] = c
	}
	for _, c := range st.rounds {
		byEpoch[c.Round.SnapshotEpoch] = c.Round
	}
	out := make([]models.RewardCreditRound, 0, len(byEpoch))
	for _, c := range byEpoch {
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b models.RewardCreditRound) int {
		return cmp.Compare(a.SnapshotEpoch, b.SnapshotEpoch)
	})
	return out, nil
}

// credentialRewardOutputsLocked returns one credential's output rows with st
// applied over the committed rows, in the order the metadata store lists
// them: epoch, pool, type.
func credentialRewardOutputsLocked(
	r RewardOutputReader,
	st rewardStaged,
	tag uint8,
	key []byte,
) ([]models.RewardAccountOutput, error) {
	committed, err := r.CredentialRewardOutputs(tag, key, st.ceiling)
	if err != nil {
		return nil, err
	}
	rows := make(map[rewardOutputID]models.RewardAccountOutput, len(committed))
	for i := range committed {
		rows[rewardOutputIDOf(&committed[i])] = committed[i]
	}
	for i := range st.outputs {
		c := &st.outputs[i]
		switch c.Kind {
		case RewardOutputWipe:
			for id := range rows {
				if id.epoch == c.Output.Epoch {
					delete(rows, id)
				}
			}
		case RewardOutputPut:
			if sameRewardCredential(&c.Output, tag, key) {
				rows[rewardOutputIDOf(&c.Output)] = c.Output
			}
		case RewardOutputDelete:
			if sameRewardCredential(&c.Output, tag, key) {
				delete(rows, rewardOutputIDOf(&c.Output))
			}
		}
	}
	out := make([]models.RewardAccountOutput, 0, len(rows))
	for _, o := range rows {
		out = append(out, o)
	}
	slices.SortFunc(out, func(a, b models.RewardAccountOutput) int {
		if c := cmp.Compare(a.Epoch, b.Epoch); c != 0 {
			return c
		}
		if c := bytes.Compare(a.PoolKeyHash, b.PoolKeyHash); c != 0 {
			return c
		}
		return cmp.Compare(a.RewardType, b.RewardType)
	})
	return out, nil
}

// pendingRewardOutputsView returns one credential's unfolded credits in the
// credited rounds as txn sees the set.
func (ls *LedgerState) pendingRewardOutputsView(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	tag uint8,
	key []byte,
) ([]models.RewardAccountOutput, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	st := ls.rewardStagedOps(txn)
	rows, err := credentialRewardOutputsLocked(r, st, tag, key)
	if err != nil {
		return nil, err
	}
	credited := make(map[uint64]bool)
	out := rows[:0]
	for _, o := range rows {
		in, seen := credited[o.Epoch]
		if !seen {
			in, err = roundCreditedLocked(r, st, o.Epoch)
			if err != nil {
				return nil, err
			}
			credited[o.Epoch] = in
		}
		if in {
			out = append(out, o)
		}
	}
	return out, nil
}

func roundCreditedLocked(
	r RewardOutputReader,
	st rewardStaged,
	epoch uint64,
) (bool, error) {
	for _, c := range slices.Backward(st.rounds) {
		if c.Round.SnapshotEpoch == epoch {
			return true, nil
		}
	}
	_, ok, err := r.RewardRound(epoch, st.ceiling)
	return ok, err
}

// pendingRewardCreditView is a credential's total of pending credits as txn
// sees the set.
func (ls *LedgerState) pendingRewardCreditView(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	tag uint8,
	key []byte,
) (uint64, error) {
	rows, err := ls.pendingRewardOutputsView(r, txn, tag, key)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, o := range rows {
		if uint64(o.Amount) > math.MaxUint64-total {
			return 0, fmt.Errorf("pending reward credit overflow for %x", key)
		}
		total += uint64(o.Amount)
	}
	return total, nil
}

// pendingRewardCreditAt is a credential's total of pending credits as the
// set stood at slot: the rows a rollback to slot would leave, of the rounds
// credited by then.
func pendingRewardCreditAt(
	r RewardOutputReader,
	tag uint8,
	key []byte,
	slot uint64,
) (uint64, error) {
	rows, err := r.CredentialRewardOutputs(tag, key, &slot)
	if err != nil {
		return 0, err
	}
	var total uint64
	credited := make(map[uint64]bool)
	for _, o := range rows {
		in, seen := credited[o.Epoch]
		if !seen {
			round, ok, err := r.RewardRound(o.Epoch, &slot)
			if err != nil {
				return 0, err
			}
			in = ok && round.BoundarySlot <= slot
			credited[o.Epoch] = in
		}
		if !in {
			continue
		}
		if uint64(o.Amount) > math.MaxUint64-total {
			return 0, fmt.Errorf("pending reward credit overflow for %x", key)
		}
		total += uint64(o.Amount)
	}
	return total, nil
}

// epochHasRewardOutputLocked reports whether epoch has an output row with st
// applied over the committed rows.
func epochHasRewardOutputLocked(
	r RewardOutputReader,
	st rewardStaged,
	epoch uint64,
) (bool, error) {
	type ref struct {
		tag  uint8
		key  string
		pool string
		typ  string
	}
	staged := make(map[ref]bool)
	wiped := false
	for i := range st.outputs {
		c := &st.outputs[i]
		if c.Output.Epoch != epoch {
			continue
		}
		if c.Kind == RewardOutputWipe {
			wiped = true
			clear(staged)
			continue
		}
		staged[ref{
			c.Output.CredentialTag, string(c.Output.StakingKey),
			string(c.Output.PoolKeyHash), c.Output.RewardType,
		}] = c.Kind == RewardOutputPut
	}
	deleted := 0
	for _, present := range staged {
		if present {
			return true, nil
		}
		deleted++
	}
	if wiped {
		return false, nil
	}
	// Each staged delete can hide one committed row, so deleted+1 rows decide.
	rows, err := r.EpochRewardOutputs(epoch, deleted+1, st.ceiling)
	if err != nil {
		return false, err
	}
	for _, o := range rows {
		if _, hidden := staged[ref{o.Tag, string(o.Key), string(o.Pool), o.Type}]; !hidden {
			return true, nil
		}
	}
	return false, nil
}

// hasCreditedRewardOutputs reports whether a credited round, through epoch
// when limit is set, has an unfolded credit as txn sees the set.
func (ls *LedgerState) hasCreditedRewardOutputs(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	through *uint64,
) (bool, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	st := ls.rewardStagedOps(txn)
	candidates, err := r.RewardOutputEpochs(st.ceiling)
	if err != nil {
		return false, err
	}
	for i := range st.outputs {
		if st.outputs[i].Kind == RewardOutputPut {
			candidates = append(candidates, st.outputs[i].Output.Epoch)
		}
	}
	slices.Sort(candidates)
	candidates = slices.Compact(candidates)
	for _, epoch := range candidates {
		if through != nil && epoch > *through {
			break
		}
		credited, err := roundCreditedLocked(r, st, epoch)
		if err != nil || !credited {
			if err != nil {
				return false, err
			}
			continue
		}
		has, err := epochHasRewardOutputLocked(r, st, epoch)
		if err != nil || has {
			return has, err
		}
	}
	return false, nil
}

// postSnapshotCreditsView returns the credits at slot per credential as txn
// sees the set, ordered as the metadata store lists them.
// postSnapshotCreditRows returns the credits at slot as txn sees the set,
// each distinct by credential, kind and source.
func (ls *LedgerState) postSnapshotCreditRows(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	slot uint64,
) ([]PostSnapshotCredit, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	st := ls.rewardStagedOps(txn)
	committed, err := r.PostSnapshotCredits(slot, st.ceiling)
	if err != nil {
		return nil, err
	}
	type id struct {
		ref    string
		loaded bool
		source string
	}
	credits := make(map[id]PostSnapshotCredit, len(committed))
	add := func(c PostSnapshotCredit) {
		credits[id{c.Ref.MapKey(), c.Loaded, string(c.Source)}] = c
	}
	for _, c := range committed {
		add(c)
	}
	for _, c := range st.credits {
		if c.Slot == slot {
			add(c)
		}
	}
	return slices.Collect(maps.Values(credits)), nil
}

func (ls *LedgerState) postSnapshotCreditsView(
	r RewardOutputReader,
	txn ledgerdb.Txn,
	slot uint64,
) ([]models.AccountRewardDelta, error) {
	credits, err := ls.postSnapshotCreditRows(r, txn, slot)
	if err != nil {
		return nil, err
	}
	totals := make(map[string]models.AccountRewardDelta)
	for _, c := range credits {
		d, ok := totals[c.Ref.MapKey()]
		if !ok {
			d = models.AccountRewardDelta{
				CredentialTag: c.Ref.Tag,
				StakingKey:    c.Ref.Key,
				AddedSlot:     slot,
				PostSnapshot:  true,
			}
		}
		if c.Amount > math.MaxUint64-uint64(d.Amount) {
			return nil, fmt.Errorf("post-snapshot credit total overflows for %x", c.Ref.Key)
		}
		d.Amount = types.Uint64(uint64(d.Amount) + c.Amount)
		totals[c.Ref.MapKey()] = d
	}
	out := make([]models.AccountRewardDelta, 0, len(totals))
	for _, d := range totals {
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b models.AccountRewardDelta) int {
		if c := cmp.Compare(a.CredentialTag, b.CredentialTag); c != 0 {
			return c
		}
		return bytes.Compare(a.StakingKey, b.StakingKey)
	})
	return out, nil
}

// postSnapshotCreditEvents lists credits as the credit events the store keeps,
// each with the source hash that identifies it, by credential then source. A
// credit loaded from the store is a credential's total at the slot, with no
// source hash, and cannot stand for the events it summed.
func postSnapshotCreditEvents(
	credits []PostSnapshotCredit,
	slot uint64,
) ([]*models.AccountRewardDelta, error) {
	out := make([]*models.AccountRewardDelta, 0, len(credits))
	for _, c := range credits {
		if c.Loaded {
			return nil, fmt.Errorf(
				"%w: the credits at slot %d were loaded without their sources",
				ErrDrainedFamilyUnserved, slot,
			)
		}
		out = append(out, &models.AccountRewardDelta{
			CredentialTag: c.Ref.Tag,
			StakingKey:    c.Ref.Key,
			TxHash:        c.Source,
			Amount:        types.Uint64(c.Amount),
			AddedSlot:     slot,
			PostSnapshot:  true,
		})
	}
	slices.SortFunc(out, func(a, b *models.AccountRewardDelta) int {
		return cmp.Or(
			cmp.Compare(a.CredentialTag, b.CredentialTag),
			bytes.Compare(a.StakingKey, b.StakingKey),
			bytes.Compare(a.TxHash, b.TxHash),
		)
	})
	return out, nil
}

func cloneRewardOutput(o *models.RewardAccountOutput) models.RewardAccountOutput {
	c := *o
	c.StakingKey = bytes.Clone(o.StakingKey)
	c.PoolKeyHash = bytes.Clone(o.PoolKeyHash)
	return c
}

// stageRewardFolds stages the removal of outputs the metadata store folded
// into their accounts, each restored at its round's boundary slot, where its
// account credit is journaled.
func (ls *LedgerState) stageRewardFolds(
	txn ledgerdb.Txn,
	outputs []*models.RewardAccountOutput,
) {
	changes := make([]RewardOutputChange, 0, len(outputs))
	for _, o := range outputs {
		if o == nil {
			continue
		}
		changes = append(changes, RewardOutputChange{
			Kind: RewardOutputDelete, Slot: o.BoundarySlot,
			Output: cloneRewardOutput(o),
		})
	}
	if len(changes) == 0 {
		return
	}
	ls.stageLedgerChanges(txn, LedgerChanges{RewardOutputs: changes}) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
}

func (s stagingLedgerDB) SaveRewardAccountOutputs(
	outputs []*models.RewardAccountOutput,
	txn ledgerdb.Txn,
) error {
	if !s.rewardOutputsDrained() {
		if err := s.LedgerDB.SaveRewardAccountOutputs(outputs, txn); err != nil {
			return err
		}
	}
	s.stageDerivedAccounts(txn, outputs)
	changes := make([]RewardOutputChange, 0, len(outputs))
	for _, o := range outputs {
		if o == nil {
			continue
		}
		kind := RewardOutputPut
		if !o.Spendable || o.Guarded {
			kind = RewardOutputDelete
		}
		changes = append(changes, RewardOutputChange{
			Kind: kind, Slot: o.CapturedSlot, Output: cloneRewardOutput(o),
		})
	}
	if len(changes) > 0 {
		s.ls.stageLedgerChanges(txn, LedgerChanges{RewardOutputs: changes}) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	}
	return nil
}

func (s stagingLedgerDB) DeleteRewardOutputsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) error {
	if !s.rewardOutputsDrained() {
		if err := s.LedgerDB.DeleteRewardOutputsForEpoch(epoch, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{RewardDerived: []RewardDerivedChange{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		Kind: RewardDerivedReplace, Epoch: epoch,
	}}})
	s.ls.stageLedgerChanges(txn, LedgerChanges{RewardOutputs: []RewardOutputChange{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		Kind: RewardOutputWipe, Output: models.RewardAccountOutput{Epoch: epoch},
	}}})
	return nil
}

func (s stagingLedgerDB) AddAppliedRewardCreditRound(
	round models.RewardCreditRound,
	txn ledgerdb.Txn,
) error {
	if !s.rewardOutputsDrained() {
		if err := s.LedgerDB.AddAppliedRewardCreditRound(round, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		RewardRounds: []RewardRoundChange{{Round: round}},
	})
	return nil
}

func (s stagingLedgerDB) ClaimPendingRewardCreditsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if s.rewardOutputsDrained() {
		return s.claimPendingFromSet(txn, credentialTag, stakingKey)
	}
	out, err := s.LedgerDB.ClaimPendingRewardCreditsForCredential(
		credentialTag, stakingKey, txn,
	)
	if err != nil {
		return out, err
	}
	s.ls.stageRewardFolds(txn, out)
	return out, nil
}

func (s stagingLedgerDB) ClaimUnfoldedRewardCredits(
	snapshotEpoch uint64,
	limit int,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	if s.rewardOutputsDrained() {
		return s.claimUnfoldedFromSet(txn, snapshotEpoch, limit)
	}
	out, err := s.LedgerDB.ClaimUnfoldedRewardCredits(snapshotEpoch, limit, txn)
	if err != nil {
		return out, err
	}
	s.ls.stageRewardFolds(txn, out)
	return out, nil
}

func sameRewardRound(a, b models.RewardCreditRound) bool {
	return a.SnapshotEpoch == b.SnapshotEpoch && a.BoundarySlot == b.BoundarySlot
}

func sameRewardOutput(a, b *models.RewardAccountOutput) bool {
	return a.Epoch == b.Epoch && a.CredentialTag == b.CredentialTag &&
		bytes.Equal(a.StakingKey, b.StakingKey) &&
		bytes.Equal(a.PoolKeyHash, b.PoolKeyHash) &&
		a.RewardType == b.RewardType && a.Amount == b.Amount &&
		a.Spendable == b.Spendable && a.Guarded == b.Guarded &&
		a.CapturedSlot == b.CapturedSlot && a.BoundarySlot == b.BoundarySlot
}

func (s stagingLedgerDB) HasAppliedRewardCreditRound(
	epoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return false, err
	}
	return s.ls.rewardRoundSeenBy(r, txn, epoch)
}

func (s stagingLedgerDB) GetPendingRewardCreditRounds(
	txn ledgerdb.Txn,
) ([]models.RewardCreditRound, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return nil, err
	}
	return s.ls.rewardRoundsView(r, txn)
}

func (s stagingLedgerDB) GetPendingRewardAccountOutputsForCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn ledgerdb.Txn,
) ([]*models.RewardAccountOutput, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return nil, err
	}
	if len(stakingKey) == 0 {
		return nil, nil
	}
	rows, err := s.ls.pendingRewardOutputsView(r, txn, credentialTag, stakingKey)
	if err != nil {
		return nil, err
	}
	out := make([]*models.RewardAccountOutput, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}
	return out, nil
}

func (s stagingLedgerDB) HasPendingRewardCreditRounds(
	txn ledgerdb.Txn,
) (bool, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return false, err
	}
	return s.ls.hasCreditedRewardOutputs(r, txn, nil)
}

func (s stagingLedgerDB) HasUnfoldedRewardCreditsThroughEpoch(
	snapshotEpoch uint64,
	txn ledgerdb.Txn,
) (bool, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return false, err
	}
	return s.ls.hasCreditedRewardOutputs(r, txn, &snapshotEpoch)
}

// GetPostSnapshotRewardCreditEvents is the store's own read until the accounts
// or reward-outputs family is drained, whose rows it joins; after that the
// events come from the set's credits, which keep the source hash of each
// credit the set followed.
func (s stagingLedgerDB) GetPostSnapshotRewardCreditEvents(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, s.ls.setFamilyUnserved(nodesettings.FamilyRewardOutputs)
	}
	credits, err := s.ls.postSnapshotCreditRows(r, txn, slot)
	if err != nil {
		return nil, err
	}
	return postSnapshotCreditEvents(credits, slot)
}

func (s stagingLedgerDB) GetPostSnapshotRewardCredits(
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.AccountRewardDelta, error) {
	r, err := s.rewardOutputsServed(txn)
	if err != nil {
		return nil, err
	}
	rows, err := s.ls.postSnapshotCreditsView(r, txn, slot)
	if err != nil {
		return nil, err
	}
	out := make([]*models.AccountRewardDelta, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}
	return out, nil
}
