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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// ProposalChange is a governance proposal row as the change at Slot leaves
// it. The row is keyed by its transaction hash and action index and carries
// the metadata store's row ID in Proposal.ID, which every put of it repeats.
type ProposalChange struct {
	Slot     uint64
	Proposal models.GovernanceProposal
}

// VoteChange is a vote row as the change at Slot leaves it, on the proposal
// named by TxHash and ActionIndex. Vote.ID and Vote.ProposalID are the
// metadata store's row IDs; the set does not keep the cast's position.
type VoteChange struct {
	Slot        uint64
	TxHash      []byte
	ActionIndex uint32
	Vote        models.GovernanceVote
}

// GovernanceReader serves governance proposals and votes from a live UTxO
// set that computes them. A read with at set returns the rows as a rollback
// to *at would leave them. Rows are returned soft-deleted ones included.
type GovernanceReader interface {
	GovernanceLoaded() (bool, error)
	ProposalAt(txHash []byte, actionIndex uint32, at *uint64) (*models.GovernanceProposal, error)
	ProposalsAt(at *uint64) ([]models.GovernanceProposal, error)
	VotesAt(txHash []byte, actionIndex uint32, at *uint64) ([]models.GovernanceVote, error)
	// NextGovernanceIDs returns the next proposal and vote row IDs. They
	// exceed every ID the reader has held a row under and never fall back,
	// whatever a rollback removes.
	NextGovernanceIDs() (proposal, vote uint64, err error)
}

func (ls *LedgerState) governanceReader(txn ledgerdb.Txn) GovernanceReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(GovernanceReader)
	if !ok {
		return nil
	}
	return r
}

func govKey(txHash []byte, actionIndex uint32) string {
	return string(binary.BigEndian.AppendUint32(bytes.Clone(txHash), actionIndex))
}

func voterKey(v models.GovernanceVote) string {
	return string(append([]byte{v.VoterType, v.VoterCredentialTag}, v.VoterCredential...))
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cleanProposal deep-copies p.
func cleanProposal(p models.GovernanceProposal) models.GovernanceProposal {
	p.TxHash = bytes.Clone(p.TxHash)
	p.ParentTxHash = bytes.Clone(p.ParentTxHash)
	p.ParentActionIdx = clonePtr(p.ParentActionIdx)
	p.EnactedEpoch = clonePtr(p.EnactedEpoch)
	p.EnactedSlot = clonePtr(p.EnactedSlot)
	p.RatifiedEpoch = clonePtr(p.RatifiedEpoch)
	p.RatifiedSlot = clonePtr(p.RatifiedSlot)
	p.PolicyHash = bytes.Clone(p.PolicyHash)
	p.AnchorHash = bytes.Clone(p.AnchorHash)
	p.ReturnAddress = bytes.Clone(p.ReturnAddress)
	p.GovActionCbor = bytes.Clone(p.GovActionCbor)
	p.ExpiredEpoch = clonePtr(p.ExpiredEpoch)
	p.ExpiredSlot = clonePtr(p.ExpiredSlot)
	p.DroppedEpoch = clonePtr(p.DroppedEpoch)
	p.DroppedSlot = clonePtr(p.DroppedSlot)
	p.TxIndex = clonePtr(p.TxIndex)
	p.DeletedSlot = clonePtr(p.DeletedSlot)
	return p
}

// cleanVote deep-copies v without the cast's position, which the set does
// not keep and the metadata store does not read back.
func cleanVote(v models.GovernanceVote) models.GovernanceVote {
	v.CastTxHash, v.CastCertIndex = nil, 0
	v.VoterCredential = bytes.Clone(v.VoterCredential)
	v.AnchorHash = bytes.Clone(v.AnchorHash)
	v.VoteUpdatedSlot = clonePtr(v.VoteUpdatedSlot)
	v.DeletedSlot = clonePtr(v.DeletedSlot)
	return v
}

func samePtrT[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// sameProposal compares every field but the action CBOR. A
// rollback across an era translation restores the set's pre-translation
// bytes where the metadata store keeps the translated ones, so reads do not
// compare them.
func sameProposal(a, b *models.GovernanceProposal) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ID == b.ID && bytes.Equal(a.TxHash, b.TxHash) &&
		a.ActionIndex == b.ActionIndex && a.ActionType == b.ActionType &&
		a.ProposedEpoch == b.ProposedEpoch && a.ExpiresEpoch == b.ExpiresEpoch &&
		bytes.Equal(a.ParentTxHash, b.ParentTxHash) &&
		samePtrT(a.ParentActionIdx, b.ParentActionIdx) &&
		samePtrT(a.EnactedEpoch, b.EnactedEpoch) &&
		samePtrT(a.EnactedSlot, b.EnactedSlot) &&
		samePtrT(a.RatifiedEpoch, b.RatifiedEpoch) &&
		samePtrT(a.RatifiedSlot, b.RatifiedSlot) &&
		bytes.Equal(a.PolicyHash, b.PolicyHash) &&
		a.AnchorURL == b.AnchorURL && bytes.Equal(a.AnchorHash, b.AnchorHash) &&
		a.Deposit == b.Deposit && bytes.Equal(a.ReturnAddress, b.ReturnAddress) &&
		samePtrT(a.ExpiredEpoch, b.ExpiredEpoch) &&
		samePtrT(a.ExpiredSlot, b.ExpiredSlot) &&
		samePtrT(a.DroppedEpoch, b.DroppedEpoch) &&
		samePtrT(a.DroppedSlot, b.DroppedSlot) &&
		samePtrT(a.TxIndex, b.TxIndex) &&
		a.AddedSlot == b.AddedSlot && samePtrT(a.DeletedSlot, b.DeletedSlot)
}

func sameVote(a, b *models.GovernanceVote) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.ID == b.ID && a.ProposalID == b.ProposalID &&
		a.VoterType == b.VoterType &&
		a.VoterCredentialTag == b.VoterCredentialTag &&
		bytes.Equal(a.VoterCredential, b.VoterCredential) &&
		a.Vote == b.Vote && a.AnchorURL == b.AnchorURL &&
		bytes.Equal(a.AnchorHash, b.AnchorHash) &&
		a.AddedSlot == b.AddedSlot &&
		samePtrT(a.VoteUpdatedSlot, b.VoteUpdatedSlot) &&
		samePtrT(a.DeletedSlot, b.DeletedSlot)
}

// govStaged returns the proposal and vote puts the batches txn sees, in
// order, without the ones a later staged rollback drops, and the lowest
// staged rollback target.
func (ls *LedgerState) govStaged(
	txn ledgerdb.Txn,
) (*uint64, []ProposalChange, []VoteChange) {
	var ceiling *uint64
	var props []ProposalChange
	var votes []VoteChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			props = slices.DeleteFunc(props, func(c ProposalChange) bool {
				return c.Slot > to
			})
			votes = slices.DeleteFunc(votes, func(c VoteChange) bool {
				return c.Slot > to
			})
		}
		props = append(props, o.proposals...)
		votes = append(votes, o.votes...)
	}
	return ceiling, props, votes
}

// proposalView returns the proposal row as txn sees the set, soft-deleted
// or not, or nil.
func (ls *LedgerState) proposalView(
	r GovernanceReader,
	txn ledgerdb.Txn,
	txHash []byte,
	actionIndex uint32,
) (*models.GovernanceProposal, error) {
	ceiling, props, _ := ls.govStaged(txn)
	p, err := r.ProposalAt(txHash, actionIndex, ceiling)
	if err != nil {
		return nil, err
	}
	for _, c := range props {
		if c.Proposal.ActionIndex == actionIndex &&
			bytes.Equal(c.Proposal.TxHash, txHash) {
			row := c.Proposal
			p = &row
		}
	}
	return p, nil
}

// proposalsView returns every proposal row as txn sees the set.
func (ls *LedgerState) proposalsView(
	r GovernanceReader,
	txn ledgerdb.Txn,
) ([]models.GovernanceProposal, error) {
	ceiling, props, _ := ls.govStaged(txn)
	rows, err := r.ProposalsAt(ceiling)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]models.GovernanceProposal, len(rows))
	for _, p := range rows {
		byKey[govKey(p.TxHash, p.ActionIndex)] = p
	}
	for _, c := range props {
		byKey[govKey(c.Proposal.TxHash, c.Proposal.ActionIndex)] = c.Proposal
	}
	out := make([]models.GovernanceProposal, 0, len(byKey))
	for _, p := range byKey {
		out = append(out, p)
	}
	return out, nil
}

// votesView returns the vote rows on a proposal as txn sees the set,
// soft-deleted ones included.
func (ls *LedgerState) votesView(
	r GovernanceReader,
	txn ledgerdb.Txn,
	txHash []byte,
	actionIndex uint32,
) ([]models.GovernanceVote, error) {
	ceiling, _, staged := ls.govStaged(txn)
	rows, err := r.VotesAt(txHash, actionIndex, ceiling)
	if err != nil {
		return nil, err
	}
	byVoter := make(map[string]models.GovernanceVote, len(rows))
	for _, v := range rows {
		byVoter[voterKey(v)] = v
	}
	for _, c := range staged {
		if c.ActionIndex == actionIndex && bytes.Equal(c.TxHash, txHash) {
			byVoter[voterKey(c.Vote)] = c.Vote
		}
	}
	out := make([]models.GovernanceVote, 0, len(byVoter))
	for _, v := range byVoter {
		out = append(out, v)
	}
	return out, nil
}

func cmpSubmission(a, b models.GovernanceProposal) int {
	ai, bi := 0, 0
	var at, bt uint32
	if a.TxIndex != nil {
		ai, at = 1, *a.TxIndex
	}
	if b.TxIndex != nil {
		bi, bt = 1, *b.TxIndex
	}
	return cmp.Or(
		cmp.Compare(ai, bi), cmp.Compare(at, bt),
		bytes.Compare(a.TxHash, b.TxHash),
		cmp.Compare(a.ActionIndex, b.ActionIndex),
	)
}

func cmpProposals(a, b models.GovernanceProposal) int {
	return cmp.Or(
		cmp.Compare(a.ProposedEpoch, b.ProposedEpoch),
		cmp.Compare(a.AddedSlot, b.AddedSlot),
		cmpSubmission(a, b),
	)
}

// cmpRatified orders as the metadata store orders ratified proposals; both
// ratification fields are set on the rows it compares.
func cmpRatified(a, b models.GovernanceProposal) int {
	return cmp.Or(
		cmp.Compare(deref(a.RatifiedEpoch), deref(b.RatifiedEpoch)),
		cmp.Compare(deref(a.RatifiedSlot), deref(b.RatifiedSlot)),
		cmpProposals(a, b),
	)
}

func deref(p *uint64) uint64 {
	if p == nil {
		return 0
	}
	return *p
}

// selectProposals returns the proposals keep accepts, in the order cmp
// gives.
func selectProposals(
	all []models.GovernanceProposal,
	keep func(models.GovernanceProposal) bool,
	order func(a, b models.GovernanceProposal) int,
) []models.GovernanceProposal {
	out := make([]models.GovernanceProposal, 0)
	for _, p := range all {
		if keep(p) {
			out = append(out, p)
		}
	}
	slices.SortFunc(out, order)
	return out
}

func slotAfter(p *uint64, slot uint64) bool { return p == nil || *p > slot }

// lastEnactedProposal selects the most recently enacted proposal of the
// given types that no enacted child of those types, enacted in the same
// transition, replaced. Ties are broken by row ID, as the metadata store
// breaks them.
func lastEnactedProposal(
	all []models.GovernanceProposal,
	types []uint8,
) *models.GovernanceProposal {
	inTypes := func(t uint8) bool { return slices.Contains(types, t) }
	var best *models.GovernanceProposal
	for i := range all {
		p := all[i]
		if !inTypes(p.ActionType) || p.EnactedEpoch == nil || p.DeletedSlot != nil {
			continue
		}
		replaced := slices.ContainsFunc(all, func(c models.GovernanceProposal) bool {
			return c.ParentTxHash != nil && bytes.Equal(c.ParentTxHash, p.TxHash) &&
				c.ParentActionIdx != nil && *c.ParentActionIdx == p.ActionIndex &&
				inTypes(c.ActionType) && c.DeletedSlot == nil &&
				samePtrT(c.EnactedEpoch, p.EnactedEpoch) &&
				samePtrT(c.EnactedSlot, p.EnactedSlot)
		})
		if replaced {
			continue
		}
		if best == nil || cmp.Or(
			cmp.Compare(deref(p.EnactedEpoch), deref(best.EnactedEpoch)),
			cmp.Compare(deref(p.EnactedSlot), deref(best.EnactedSlot)),
			cmp.Compare(p.ID, best.ID),
		) > 0 {
			best = &all[i]
		}
	}
	return best
}

// mergeProposal is the row SetGovernanceProposal leaves: an existing row
// takes the new base fields, a non-empty action CBOR, a position when
// given, and a lifecycle field only when the write sets it, as the upsert's
// COALESCE does; its added slot is kept.
func mergeProposal(
	prev *models.GovernanceProposal,
	in models.GovernanceProposal,
) models.GovernanceProposal {
	in = cleanProposal(in)
	if prev == nil {
		return in
	}
	next := cleanProposal(*prev)
	next.ActionType, next.ProposedEpoch, next.ExpiresEpoch =
		in.ActionType, in.ProposedEpoch, in.ExpiresEpoch
	next.ParentTxHash, next.ParentActionIdx = in.ParentTxHash, in.ParentActionIdx
	next.PolicyHash, next.AnchorURL, next.AnchorHash =
		in.PolicyHash, in.AnchorURL, in.AnchorHash
	next.Deposit, next.ReturnAddress = in.Deposit, in.ReturnAddress
	if len(in.GovActionCbor) > 0 {
		next.GovActionCbor = in.GovActionCbor
	}
	coalesce := func(dst **uint64, src *uint64) {
		if src != nil {
			*dst = src
		}
	}
	coalesce(&next.EnactedEpoch, in.EnactedEpoch)
	coalesce(&next.EnactedSlot, in.EnactedSlot)
	coalesce(&next.RatifiedEpoch, in.RatifiedEpoch)
	coalesce(&next.RatifiedSlot, in.RatifiedSlot)
	coalesce(&next.ExpiredEpoch, in.ExpiredEpoch)
	coalesce(&next.ExpiredSlot, in.ExpiredSlot)
	coalesce(&next.DroppedEpoch, in.DroppedEpoch)
	coalesce(&next.DroppedSlot, in.DroppedSlot)
	coalesce(&next.DeletedSlot, in.DeletedSlot)
	if in.TxIndex != nil {
		next.TxIndex = in.TxIndex
	}
	return next
}

// lifecycle is one set-never-clear group of a proposal's fields and the slot
// a rollback to before it undoes.
type lifecycle struct {
	slot  func(models.GovernanceProposal) *uint64
	apply func(dst *models.GovernanceProposal, src models.GovernanceProposal)
	clear func(dst *models.GovernanceProposal)
}

var proposalLifecycles = []lifecycle{
	{
		slot: func(p models.GovernanceProposal) *uint64 { return p.RatifiedSlot },
		apply: func(d *models.GovernanceProposal, s models.GovernanceProposal) {
			d.RatifiedEpoch, d.RatifiedSlot = s.RatifiedEpoch, s.RatifiedSlot
		},
		clear: func(d *models.GovernanceProposal) { d.RatifiedEpoch, d.RatifiedSlot = nil, nil },
	},
	{
		slot: func(p models.GovernanceProposal) *uint64 { return p.EnactedSlot },
		apply: func(d *models.GovernanceProposal, s models.GovernanceProposal) {
			d.EnactedEpoch, d.EnactedSlot = s.EnactedEpoch, s.EnactedSlot
		},
		clear: func(d *models.GovernanceProposal) { d.EnactedEpoch, d.EnactedSlot = nil, nil },
	},
	{
		slot: func(p models.GovernanceProposal) *uint64 { return p.ExpiredSlot },
		apply: func(d *models.GovernanceProposal, s models.GovernanceProposal) {
			d.ExpiredEpoch, d.ExpiredSlot = s.ExpiredEpoch, s.ExpiredSlot
		},
		clear: func(d *models.GovernanceProposal) { d.ExpiredEpoch, d.ExpiredSlot = nil, nil },
	},
	{
		slot: func(p models.GovernanceProposal) *uint64 { return p.DroppedSlot },
		apply: func(d *models.GovernanceProposal, s models.GovernanceProposal) {
			d.DroppedEpoch, d.DroppedSlot = s.DroppedEpoch, s.DroppedSlot
		},
		clear: func(d *models.GovernanceProposal) { d.DroppedEpoch, d.DroppedSlot = nil, nil },
	},
	{
		slot:  func(p models.GovernanceProposal) *uint64 { return p.DeletedSlot },
		apply: func(d *models.GovernanceProposal, s models.GovernanceProposal) { d.DeletedSlot = s.DeletedSlot },
		clear: func(d *models.GovernanceProposal) { d.DeletedSlot = nil },
	},
}

// proposalPuts returns the puts that take prev (nil for a new row) to next.
// Each lifecycle group the write sets is put at its own slot, so a rollback
// between two of them leaves the earlier ones, as the metadata store's
// per-slot rollback does; a new row is first put at its added slot without
// its lifecycle.
func proposalPuts(prev *models.GovernanceProposal, next models.GovernanceProposal) []ProposalChange {
	if prev != nil && sameProposal(prev, &next) &&
		bytes.Equal(prev.GovActionCbor, next.GovActionCbor) {
		return nil
	}
	var base models.GovernanceProposal
	out := make([]ProposalChange, 0, 2)
	if prev == nil {
		base = cleanProposal(next)
		for _, l := range proposalLifecycles {
			l.clear(&base)
		}
		out = append(out, ProposalChange{Slot: next.AddedSlot, Proposal: cleanProposal(base)})
	} else {
		base = cleanProposal(*prev)
	}
	slotOf := func(l lifecycle, p models.GovernanceProposal) uint64 {
		if s := l.slot(p); s != nil {
			return *s
		}
		return next.AddedSlot
	}
	var slots []uint64
	for _, l := range proposalLifecycles {
		changed := l.slot(next) != nil && !samePtrT(l.slot(next), l.slot(base))
		if changed && !slices.Contains(slots, slotOf(l, next)) {
			slots = append(slots, slotOf(l, next))
		}
	}
	slices.Sort(slots)
	for i, s := range slots {
		row := cleanProposal(base)
		for _, l := range proposalLifecycles {
			if l.slot(next) != nil && slotOf(l, next) <= s {
				l.apply(&row, next)
			}
		}
		if i == len(slots)-1 {
			row = cleanProposal(next)
		}
		out = append(out, ProposalChange{Slot: s, Proposal: row})
	}
	if len(slots) == 0 {
		// Nothing but base fields or the action CBOR differs: a new row's
		// first put already is the whole row, an existing row gets one put.
		if len(out) > 0 {
			out = out[:0]
		}
		out = append(out, ProposalChange{Slot: next.AddedSlot, Proposal: cleanProposal(next)})
	}
	return out
}

func (ls *LedgerState) stageGovernance(txn ledgerdb.Txn, lc LedgerChanges) {
	ls.stageLedgerChanges(txn, lc) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
}

func (s stagingLedgerDB) SetGovernanceProposal(
	ctx context.Context,
	proposal *models.GovernanceProposal,
	txn ledgerdb.Txn,
) error {
	if s.ls.governanceDrained() {
		return s.setDrainedProposal(proposal, txn) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	}
	if err := s.LedgerDB.SetGovernanceProposal(ctx, proposal, txn); err != nil {
		return err
	}
	r := s.ls.governanceReader(txn)
	if r == nil {
		return nil
	}
	// The store wrote the row ID back into proposal.
	if proposal.ID == 0 {
		s.ls.failAccountStaging(fmt.Errorf(
			"proposal %x#%d has no row ID after the write",
			proposal.TxHash, proposal.ActionIndex,
		))
		return nil
	}
	prev, err := s.ls.proposalView(r, txn, proposal.TxHash, proposal.ActionIndex)
	if err == nil && prev != nil && prev.ID != proposal.ID {
		err = fmt.Errorf(
			"proposal %x#%d has row ID %d in the set and %d in the store",
			proposal.TxHash, proposal.ActionIndex, prev.ID, proposal.ID,
		)
	}
	if err != nil {
		s.ls.failAccountStaging(err)
		return nil
	}
	puts := proposalPuts(prev, mergeProposal(prev, *proposal))
	if len(puts) > 0 {
		s.ls.stageGovernance(txn, LedgerChanges{Proposals: puts}) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	}
	return nil
}

func (s stagingLedgerDB) ClearGovernanceProposalRatification(
	ctx context.Context,
	txHash []byte,
	actionIndex uint32,
	transitionSlot uint64,
	txn ledgerdb.Txn,
) error {
	if s.ls.governanceDrained() {
		return s.clearDrainedRatification( //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			txHash, actionIndex, transitionSlot, txn,
		)
	}
	err := s.LedgerDB.ClearGovernanceProposalRatification(
		ctx, txHash, actionIndex, transitionSlot, txn,
	)
	if err != nil {
		return err
	}
	r := s.ls.governanceReader(txn)
	if r == nil {
		return nil
	}
	prev, err := s.ls.proposalView(r, txn, txHash, actionIndex)
	if err != nil || prev == nil {
		if err == nil {
			err = fmt.Errorf("proposal %x#%d is not in the set", txHash, actionIndex)
		}
		s.ls.failAccountStaging(err)
		return nil
	}
	if prev.RatifiedEpoch == nil && prev.RatifiedSlot == nil {
		return nil
	}
	next := cleanProposal(*prev)
	next.RatifiedEpoch, next.RatifiedSlot = nil, nil
	s.ls.stageGovernance(txn, LedgerChanges{Proposals: []ProposalChange{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		{Slot: transitionSlot, Proposal: next},
	}})
	return nil
}

// proposalKeyByID finds the transaction hash and action index of the
// proposal with row ID id as txn sees the set, whatever its lifecycle state.
func (ls *LedgerState) proposalKeyByID(
	r GovernanceReader,
	txn ledgerdb.Txn,
	id uint,
) ([]byte, uint32, bool, error) {
	all, err := ls.proposalsView(r, txn)
	if err != nil {
		return nil, 0, false, err
	}
	for _, p := range all {
		if p.ID == id {
			return p.TxHash, p.ActionIndex, true, nil
		}
	}
	return nil, 0, false, nil
}

func (s stagingLedgerDB) SetGovernanceVote(
	ctx context.Context,
	vote *models.GovernanceVote,
	txn ledgerdb.Txn,
) error {
	if s.ls.governanceDrained() {
		return s.setDrainedVote(vote, txn) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	}
	if err := s.LedgerDB.SetGovernanceVote(ctx, vote, txn); err != nil {
		return err
	}
	r := s.ls.governanceReader(txn)
	if r == nil {
		return nil
	}
	var txHash []byte
	var idx uint32
	var found bool
	var err error
	if vote.ID == 0 {
		err = errors.New("the metadata store gave the vote no row ID")
	} else {
		txHash, idx, found, err = s.ls.proposalKeyByID(r, txn, vote.ProposalID)
	}
	if err == nil && !found {
		err = fmt.Errorf(
			"vote on proposal %d, which is not in the set", vote.ProposalID,
		)
	}
	var rows []models.GovernanceVote
	if err == nil {
		rows, err = s.ls.votesView(r, txn, txHash, idx)
	}
	if err != nil {
		s.ls.failAccountStaging(err)
		return nil
	}
	next := cleanVote(*vote)
	var prev *models.GovernanceVote
	for i := range rows {
		if voterKey(rows[i]) == voterKey(next) {
			prev = &rows[i]
			next.AddedSlot = prev.AddedSlot
		}
	}
	if sameVote(prev, &next) {
		return nil
	}
	slot := vote.AddedSlot
	if vote.VoteUpdatedSlot != nil {
		slot = *vote.VoteUpdatedSlot
	}
	s.ls.stageGovernance(txn, LedgerChanges{Votes: []VoteChange{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		Slot: slot, TxHash: bytes.Clone(txHash), ActionIndex: idx, Vote: next,
	}}})
	return nil
}

func (s stagingLedgerDB) DeleteGovernanceVotesForDrep(
	ctx context.Context,
	credentialTag uint8,
	credential []byte,
	epoch uint64,
	deletedSlot uint64,
	txn ledgerdb.Txn,
) (int, error) {
	if s.ls.governanceDrained() {
		return s.deleteDrainedDRepVotes( //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			credentialTag, credential, epoch, deletedSlot, txn,
		)
	}
	g := s.ls.verifyGuard(txn)
	n, err := s.LedgerDB.DeleteGovernanceVotesForDrep(
		ctx, credentialTag, credential, epoch, deletedSlot, txn,
	)
	if err != nil {
		return n, err
	}
	r := s.ls.governanceReader(txn)
	if r == nil {
		return n, nil
	}
	changes, err := s.ls.drepVoteDeletions(
		r, txn, credentialTag, credential, epoch, deletedSlot,
	)
	if err != nil {
		s.ls.failAccountStaging(err)
		return n, nil
	}
	if len(changes) > 0 {
		s.ls.stageGovernance(txn, LedgerChanges{Votes: changes}) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	}
	if _, mode := s.ls.utxoSetReader(); mode != UtxoSetReadsOff {
		s.ls.compareGovernanceRead(
			g, fmt.Sprintf("votes of DRep %x deleted", credential),
			len(changes) == n, nil,
		)
	}
	return n, nil
}

// drepVoteDeletions returns the soft deletions of a DRep's live votes on the
// proposals still open at epoch, as the metadata store's update selects them.
func (ls *LedgerState) drepVoteDeletions(
	r GovernanceReader,
	txn ledgerdb.Txn,
	credentialTag uint8,
	credential []byte,
	epoch uint64,
	deletedSlot uint64,
) ([]VoteChange, error) {
	proposals, err := ls.proposalsView(r, txn)
	var changes []VoteChange
	for i := 0; err == nil && i < len(proposals); i++ {
		p := proposals[i]
		if p.ExpiresEpoch < epoch || p.EnactedEpoch != nil ||
			p.ExpiredEpoch != nil || p.DeletedSlot != nil {
			continue
		}
		var votes []models.GovernanceVote
		votes, err = ls.votesView(r, txn, p.TxHash, p.ActionIndex)
		for _, v := range votes {
			if v.VoterType != models.VoterTypeDRep ||
				v.VoterCredentialTag != credentialTag ||
				!bytes.Equal(v.VoterCredential, credential) || v.DeletedSlot != nil {
				continue
			}
			v.DeletedSlot = &deletedSlot
			changes = append(changes, VoteChange{
				Slot: deletedSlot, TxHash: bytes.Clone(p.TxHash),
				ActionIndex: p.ActionIndex, Vote: cleanVote(v),
			})
		}
	}
	return changes, err
}

func (ls *LedgerState) compareGovernanceRead(
	g verifyReadGuard,
	what string,
	same bool,
	setErr error,
	detail ...any,
) {
	if !ls.verifying() {
		return
	}
	if setErr == nil && !same && ls.verifyReadRaced(g) {
		return
	}
	ls.metrics.governanceReadsVerified.Inc()
	if same && setErr == nil {
		return
	}
	n := ls.governanceReadMismatches.Add(1)
	ls.metrics.governanceReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set governance read differs from the metadata store",
		append([]any{
			"component", "ledger",
			"read", what,
			"set_error", setErr,
			"mismatches", n,
		}, detail...)...,
	)
}

// voteDiffLimit bounds how many votes a comparison names.
const voteDiffLimit = 10

func voteState(v models.GovernanceVote) string {
	vu := "nil"
	if v.VoteUpdatedSlot != nil {
		vu = strconv.FormatUint(*v.VoteUpdatedSlot, 10)
	}
	return fmt.Sprintf("(vote=%d added_slot=%d vote_updated_slot=%s)",
		v.Vote, v.AddedSlot, vu)
}

// votesDiff names the votes that only the metadata store's or only the set's
// list holds, and the fields of those both hold that differ, as store=V
// set=V.
func votesDiff(
	want []*models.GovernanceVote,
	got map[string]models.GovernanceVote,
) string {
	var parts []string
	seen := make(map[string]struct{}, len(want))
	label := func(v models.GovernanceVote) string {
		return fmt.Sprintf(
			"voter %d/%d/%x",
			v.VoterType,
			v.VoterCredentialTag,
			v.VoterCredential,
		)
	}
	for _, w := range want {
		key := voterKey(*w)
		seen[key] = struct{}{}
		g, ok := got[key]
		if !ok {
			parts = append(parts, "only in store "+label(*w)+" "+voteState(*w))
			continue
		}
		var diffs []string
		add := func(name string, differs bool, s, v any) {
			if differs {
				diffs = append(
					diffs,
					fmt.Sprintf("%s store=%v set=%v", name, s, v),
				)
			}
		}
		add("id", w.ID != g.ID, w.ID, g.ID)
		add(
			"proposal_id", w.ProposalID != g.ProposalID,
			w.ProposalID, g.ProposalID,
		)
		add("vote", w.Vote != g.Vote, w.Vote, g.Vote)
		add("anchor_url", w.AnchorURL != g.AnchorURL, w.AnchorURL, g.AnchorURL)
		add("anchor_hash", !bytes.Equal(w.AnchorHash, g.AnchorHash),
			hex.EncodeToString(w.AnchorHash), hex.EncodeToString(g.AnchorHash))
		add("added_slot", w.AddedSlot != g.AddedSlot, w.AddedSlot, g.AddedSlot)
		add(
			"vote_updated_slot",
			!samePtrT(w.VoteUpdatedSlot, g.VoteUpdatedSlot),
			ptrString(w.VoteUpdatedSlot),
			ptrString(g.VoteUpdatedSlot),
		)
		if len(diffs) > 0 {
			parts = append(parts, label(*w)+": "+strings.Join(diffs, ", "))
		}
	}
	for key, g := range got {
		if _, ok := seen[key]; !ok {
			parts = append(parts, "only in set "+label(g)+" "+voteState(g))
		}
	}
	slices.Sort(parts)
	if len(parts) > voteDiffLimit {
		parts = append(parts[:voteDiffLimit],
			fmt.Sprintf("and %d more", len(parts)-voteDiffLimit))
	}
	return strings.Join(parts, " | ")
}

func ptrString(p *uint64) string {
	if p == nil {
		return "nil"
	}
	return strconv.FormatUint(*p, 10)
}

// verifyGovernance compares a governance read with the set's, which same
// computes from the set's reader.
func (s stagingLedgerDB) verifyGovernance(
	g verifyReadGuard,
	what string,
	same func(GovernanceReader) (bool, error),
) {
	s.verifyGovernanceDetail(g, what,
		func(r GovernanceReader) (bool, []any, error) {
			ok, err := same(r)
			return ok, nil, err
		})
}

// verifyGovernanceDetail is verifyGovernance for a comparison that also
// reports detail, logged with a mismatch, as key-value pairs.
func (s stagingLedgerDB) verifyGovernanceDetail(
	g verifyReadGuard,
	what string,
	same func(GovernanceReader) (bool, []any, error),
) {
	if _, mode := s.ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return
	}
	r := s.ls.governanceReader(g.txn)
	if r == nil {
		return
	}
	ok, detail, err := same(r)
	s.ls.compareGovernanceRead(g, what, err == nil && ok, err, detail...)
}

func sameProposalLists(want []*models.GovernanceProposal, got []models.GovernanceProposal) bool {
	return slices.EqualFunc(want, got, func(a *models.GovernanceProposal, b models.GovernanceProposal) bool {
		return sameProposal(a, &b)
	})
}

// verifyProposals compares a proposal list read with the set's proposals
// that keep accepts, in the order the read has.
func (s stagingLedgerDB) verifyProposals(
	g verifyReadGuard,
	what string,
	txn ledgerdb.Txn,
	want []*models.GovernanceProposal,
	keep func(models.GovernanceProposal) bool,
	order func(a, b models.GovernanceProposal) int,
) {
	s.verifyGovernance(g, what, func(r GovernanceReader) (bool, error) {
		all, err := s.ls.proposalsView(r, txn)
		if err != nil {
			return false, err
		}
		return sameProposalLists(want, selectProposals(all, keep, order)), nil
	})
}

// proposalList answers a proposal list read: from the set in set mode, and
// from the metadata store otherwise, with the store's answer compared with
// the set's in verify mode. keep and order are the read's predicate and
// ORDER BY.
func (s stagingLedgerDB) proposalList(
	what string,
	txn ledgerdb.Txn,
	keep func(models.GovernanceProposal) bool,
	order func(a, b models.GovernanceProposal) int,
	fromStore func() ([]*models.GovernanceProposal, error),
) ([]*models.GovernanceProposal, error) {
	r, err := s.ls.governanceServing(txn)
	if err != nil {
		return nil, err
	}
	if r != nil {
		all, err := s.ls.proposalsView(r, txn)
		if err != nil {
			return nil, err
		}
		rows := selectProposals(all, keep, order)
		out := make([]*models.GovernanceProposal, len(rows))
		for i := range rows {
			c := cleanProposal(rows[i])
			out[i] = &c
		}
		return out, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := fromStore()
	if err != nil {
		return want, err
	}
	s.verifyProposals(g, what, txn, want, keep, order)
	return want, nil
}

// GetGovernanceProposal is answered from the set in set mode.
func (s stagingLedgerDB) GetGovernanceProposal(
	ctx context.Context,
	txHash []byte,
	actionIndex uint32,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	r, err := s.ls.governanceServing(txn)
	if err != nil {
		return nil, err
	}
	if r != nil {
		p, err := s.ls.proposalView(r, txn, txHash, actionIndex)
		if err != nil {
			return nil, err
		}
		if p == nil || p.DeletedSlot != nil {
			return nil, models.ErrGovernanceProposalNotFound
		}
		c := cleanProposal(*p)
		return &c, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetGovernanceProposal(ctx, txHash, actionIndex, txn)
	// A missing proposal is an answer too, and the set must agree.
	if err != nil && !errors.Is(err, models.ErrGovernanceProposalNotFound) {
		return want, err
	}
	s.verifyGovernance(g, fmt.Sprintf("proposal %x#%d", txHash, actionIndex),
		func(r GovernanceReader) (bool, error) {
			p, err := s.ls.proposalView(r, txn, txHash, actionIndex)
			if err != nil {
				return false, err
			}
			if p != nil && p.DeletedSlot != nil {
				p = nil
			}
			return sameProposal(want, p), nil
		})
	return want, err
}

func (s stagingLedgerDB) GetActiveGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("active proposals", txn,
		func(p models.GovernanceProposal) bool {
			return p.ExpiresEpoch >= epoch && p.EnactedEpoch == nil &&
				p.ExpiredEpoch == nil && p.DeletedSlot == nil
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetActiveGovernanceProposals(ctx, epoch, txn)
		})
}

func (s stagingLedgerDB) GetGovernanceProposalSet(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("proposal set", txn,
		func(p models.GovernanceProposal) bool {
			return p.EnactedEpoch == nil && p.DroppedEpoch == nil && p.DeletedSlot == nil
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetGovernanceProposalSet(ctx, txn)
		})
}

func (s stagingLedgerDB) GetGovernanceProposalSetAtSlot(
	ctx context.Context,
	slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList(fmt.Sprintf("proposal set at slot %d", slot), txn,
		func(p models.GovernanceProposal) bool {
			return p.AddedSlot <= slot && slotAfter(p.EnactedSlot, slot) &&
				slotAfter(p.DroppedSlot, slot) && slotAfter(p.DeletedSlot, slot)
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetGovernanceProposalSetAtSlot(ctx, slot, txn)
		})
}

func (s stagingLedgerDB) GetExpiringGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("expiring proposals", txn,
		func(p models.GovernanceProposal) bool {
			return p.ExpiresEpoch < epoch && p.RatifiedEpoch == nil &&
				p.EnactedEpoch == nil && p.ExpiredEpoch == nil && p.DeletedSlot == nil
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetExpiringGovernanceProposals(ctx, epoch, txn)
		})
}

func (s stagingLedgerDB) GetExpiredGovernanceProposalsAt(
	ctx context.Context,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("proposals expired at a boundary", txn,
		func(p models.GovernanceProposal) bool {
			return samePtrT(p.ExpiredEpoch, &epoch) && samePtrT(p.ExpiredSlot, &slot) &&
				p.EnactedEpoch == nil && p.DeletedSlot == nil
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetExpiredGovernanceProposalsAt(
				ctx, epoch, slot, txn,
			)
		})
}

func (s stagingLedgerDB) GetExpiredAwaitingDropGovernanceProposals(
	ctx context.Context,
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("proposals awaiting drop", txn,
		func(p models.GovernanceProposal) bool {
			return p.ExpiredEpoch != nil && *p.ExpiredEpoch < epoch &&
				p.DroppedEpoch == nil && p.DeletedSlot == nil
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetExpiredAwaitingDropGovernanceProposals(
				ctx, epoch, txn,
			)
		})
}

func (s stagingLedgerDB) GetDroppedGovernanceProposalsAt(
	ctx context.Context,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("proposals dropped at a boundary", txn,
		func(p models.GovernanceProposal) bool {
			return samePtrT(p.DroppedEpoch, &epoch) && samePtrT(p.DroppedSlot, &slot) &&
				p.DeletedSlot == nil
		}, cmpProposals,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetDroppedGovernanceProposalsAt(
				ctx, epoch, slot, txn,
			)
		})
}

func (s stagingLedgerDB) GetEnactedGovernanceProposalsAt(
	ctx context.Context,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("proposals enacted at a boundary", txn,
		func(p models.GovernanceProposal) bool {
			return p.RatifiedEpoch != nil && samePtrT(p.EnactedEpoch, &epoch) &&
				samePtrT(p.EnactedSlot, &slot) && p.DeletedSlot == nil
		}, cmpRatified,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetEnactedGovernanceProposalsAt(
				ctx, epoch, slot, txn,
			)
		})
}

func (s stagingLedgerDB) GetRatifiedGovernanceProposals(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	return s.proposalList("ratified proposals", txn,
		func(p models.GovernanceProposal) bool {
			return p.RatifiedEpoch != nil && p.EnactedEpoch == nil && p.DeletedSlot == nil
		}, cmpRatified,
		func() ([]*models.GovernanceProposal, error) {
			return s.LedgerDB.GetRatifiedGovernanceProposals(ctx, txn)
		})
}

// governanceServing returns the governance reader when governance reads are
// answered from the set, or nil when they stay on the metadata store. Once
// the family is drained the metadata store holds no rows, so the set answers
// whatever the read mode, and a set that cannot is an error rather than nil.
func (ls *LedgerState) governanceServing(
	txn ledgerdb.Txn,
) (GovernanceReader, error) {
	if ls.governanceDrained() {
		return ls.drainedGovernanceReader(txn)
	}
	r := ls.governanceReader(txn)
	if r == nil || !ls.setServes(&ls.governanceSetLoaded, r.GovernanceLoaded) {
		return nil, nil
	}
	return r, nil
}

// GetLastEnactedGovernanceProposal is answered from the set in set mode.
func (s stagingLedgerDB) GetLastEnactedGovernanceProposal(
	ctx context.Context,
	actionTypes []uint8,
	txn ledgerdb.Txn,
) (*models.GovernanceProposal, error) {
	r, err := s.ls.governanceServing(txn)
	if err != nil {
		return nil, err
	}
	if r != nil {
		all, err := s.ls.proposalsView(r, txn)
		if err != nil {
			return nil, err
		}
		p := lastEnactedProposal(all, actionTypes)
		if p == nil {
			return nil, nil
		}
		c := cleanProposal(*p)
		return &c, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetLastEnactedGovernanceProposal(ctx, actionTypes, txn)
	if err != nil {
		return want, err
	}
	s.verifyGovernance(g, "last enacted proposal", func(r GovernanceReader) (bool, error) {
		all, err := s.ls.proposalsView(r, txn)
		if err != nil {
			return false, err
		}
		return sameProposal(want, lastEnactedProposal(all, actionTypes)), nil
	})
	return want, nil
}

// liveVotesOn returns the votes on the proposal (txHash, idx) as txn sees
// the set, in row ID order, with the soft-deleted ones left out, as the
// metadata store reads them.
func (ls *LedgerState) liveVotesOn(
	r GovernanceReader,
	txn ledgerdb.Txn,
	txHash []byte,
	idx uint32,
) ([]models.GovernanceVote, error) {
	rows, err := ls.votesView(r, txn, txHash, idx)
	if err != nil {
		return nil, err
	}
	out := make([]models.GovernanceVote, 0, len(rows))
	for _, v := range rows {
		if v.DeletedSlot == nil {
			out = append(out, cleanVote(v))
		}
	}
	slices.SortFunc(out, func(a, b models.GovernanceVote) int {
		return cmp.Compare(a.ID, b.ID)
	})
	return out, nil
}

// proposalKeyInStore finds the transaction hash and action index of the
// proposal with row ID id among the metadata store's proposal set, the only
// proposals a vote can be cast on.
func (s stagingLedgerDB) proposalKeyInStore(
	ctx context.Context,
	txn ledgerdb.Txn,
	id uint,
) ([]byte, uint32, bool, error) {
	set, err := s.LedgerDB.GetGovernanceProposalSet(ctx, txn)
	if err != nil {
		return nil, 0, false, err
	}
	for _, p := range set {
		if p.ID == id {
			return p.TxHash, p.ActionIndex, true, nil
		}
	}
	return nil, 0, false, nil
}

// GetGovernanceVotes is answered from the set in set mode.
func (s stagingLedgerDB) GetGovernanceVotes(
	ctx context.Context,
	proposalID uint,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVote, error) {
	r, err := s.ls.governanceServing(txn)
	if err != nil {
		return nil, err
	}
	if r != nil {
		var rows []models.GovernanceVote
		txHash, idx, found, err := s.ls.proposalKeyByID(r, txn, proposalID)
		if err == nil && found {
			rows, err = s.ls.liveVotesOn(r, txn, txHash, idx)
		}
		if err != nil {
			return nil, err
		}
		out := make([]*models.GovernanceVote, len(rows))
		for i := range rows {
			out[i] = &rows[i]
		}
		return out, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetGovernanceVotes(ctx, proposalID, txn)
	if err != nil {
		return want, err
	}
	if _, mode := s.ls.utxoSetReader(); mode == UtxoSetReadsOff ||
		s.ls.governanceReader(txn) == nil {
		return want, nil
	}
	txHash, idx, found, err := s.proposalKeyInStore(ctx, txn, proposalID)
	if err != nil {
		return want, err
	}
	if !found {
		// Only a proposal in the set can be compared; one already enacted
		// or dropped has no votes to tally.
		return want, nil
	}
	s.verifyGovernanceDetail(
		g,
		fmt.Sprintf("votes on proposal %x#%d", txHash, idx),
		func(r GovernanceReader) (bool, []any, error) {
			rows, err := s.ls.liveVotesOn(r, txn, txHash, idx)
			if err != nil {
				return false, nil, err
			}
			got := make(map[string]models.GovernanceVote, len(rows))
			for _, v := range rows {
				got[voterKey(v)] = v
			}
			same := len(got) == len(want)
			for _, w := range want {
				v, ok := got[voterKey(*w)]
				if !ok || !sameVote(&v, w) {
					same = false
				}
			}
			if same {
				return true, nil, nil
			}
			return false, []any{
				"proposal", fmt.Sprintf("%x#%d", txHash, idx),
				"diff", votesDiff(want, got),
				"store_count", len(want), "set_count", len(got),
			}, nil
		})
	return want, nil
}
