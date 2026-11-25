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
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// setGovernanceReader returns the set's governance reader. No other store
// holds the family, so a set that is not following the ledger or does not
// hold every row is an error.
func (ls *LedgerState) setGovernanceReader(
	txn ledgerdb.Txn,
) (GovernanceReader, error) {
	r := ls.governanceReader(txn)
	if _, mode := ls.utxoSetReader(); r == nil || !mode {
		return nil, ledgerStateUnserved(nodesettings.FamilyGovernance)
	}
	if !ls.governanceSetLoaded.Load() {
		loaded, err := r.GovernanceLoaded()
		if err != nil {
			return nil, err
		}
		if !loaded {
			return nil, ledgerStateUnserved(nodesettings.FamilyGovernance)
		}
		ls.governanceSetLoaded.Store(true)
	}
	return r, nil
}

// nextGovernanceIDs returns the proposal and vote row IDs the next new row
// takes, as the metadata store's autoincrement would give them: above every
// ID the set has held and every ID a batch txn sees has staged, rolled back
// ones included, which the store never reuses. IDs a transaction allocated
// and abandoned are in no batch, so the next one reuses them, as the store
// does when a transaction rolls back.
func (ls *LedgerState) nextGovernanceIDs(
	r GovernanceReader,
	txn ledgerdb.Txn,
) (proposal, vote uint64, err error) {
	proposal, vote, err = r.NextGovernanceIDs()
	if err != nil {
		return 0, 0, err
	}
	for _, o := range ls.stagedOps(txn) {
		for _, c := range o.proposals {
			proposal = max(proposal, uint64(c.Proposal.ID)+1)
		}
		for _, c := range o.votes {
			vote = max(vote, uint64(c.Vote.ID)+1)
		}
	}
	return proposal, vote, nil
}

// stageProposal is SetGovernanceProposal staged for the set: the set
// is the only copy, so it gives a new proposal its row ID.
func (s stagingLedgerDB) stageProposal(
	proposal *models.GovernanceProposal,
	txn ledgerdb.Txn,
) error {
	if proposal == nil {
		return errors.New("set governance proposal: nil proposal")
	}
	if (proposal.RatifiedEpoch == nil) != (proposal.RatifiedSlot == nil) {
		return errors.New(
			"set governance proposal: ratified epoch and slot must both be set or both be nil",
		)
	}
	r, err := s.ls.setGovernanceReader(txn)
	if err != nil {
		return err
	}
	prev, err := s.ls.proposalView(
		r,
		txn,
		proposal.TxHash,
		proposal.ActionIndex,
	)
	if err != nil {
		return err
	}
	if prev != nil {
		proposal.ID = prev.ID
	} else {
		next, _, err := s.ls.nextGovernanceIDs(r, txn)
		if err != nil {
			return err
		}
		proposal.ID = uint(next)
	}
	puts := proposalPuts(prev, mergeProposal(prev, *proposal))
	if len(puts) > 0 {
		s.ls.stageGovernance(txn, LedgerChanges{Proposals: puts})
	}
	return nil
}

// stageRatificationClear is ClearGovernanceProposalRatification staged for
// the set.
func (s stagingLedgerDB) stageRatificationClear(
	txHash []byte,
	actionIndex uint32,
	transitionSlot uint64,
	txn ledgerdb.Txn,
) error {
	r, err := s.ls.setGovernanceReader(txn)
	if err != nil {
		return err
	}
	prev, err := s.ls.proposalView(r, txn, txHash, actionIndex)
	if err != nil {
		return err
	}
	if prev == nil {
		return errors.New(
			"clear proposal ratification: expected 1 row, found 0",
		)
	}
	if (prev.RatifiedEpoch == nil) != (prev.RatifiedSlot == nil) {
		return errors.New(
			"clear proposal ratification: inconsistent ratification marker",
		)
	}
	if prev.RatifiedEpoch == nil {
		return nil
	}
	next := cleanProposal(*prev)
	next.RatifiedEpoch, next.RatifiedSlot = nil, nil
	s.ls.stageGovernance(txn, LedgerChanges{Proposals: []ProposalChange{
		{Slot: transitionSlot, Proposal: next},
	}})
	return nil
}

// stageVote is SetGovernanceVote staged for the set. A voter's first
// vote on a proposal takes a new row ID; a replacement keeps the row's.
func (s stagingLedgerDB) stageVote(
	vote *models.GovernanceVote,
	txn ledgerdb.Txn,
) error {
	if vote == nil {
		return errors.New("set governance vote: nil vote")
	}
	r, err := s.ls.setGovernanceReader(txn)
	if err != nil {
		return err
	}
	txHash, idx, found, err := s.ls.proposalKeyByID(r, txn, vote.ProposalID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf(
			"vote on proposal %d, which is not in the set", vote.ProposalID,
		)
	}
	rows, err := s.ls.votesView(r, txn, txHash, idx)
	if err != nil {
		return err
	}
	next := cleanVote(*vote)
	var prev *models.GovernanceVote
	for i := range rows {
		if voterKey(rows[i]) == voterKey(next) {
			prev = &rows[i]
			next.AddedSlot = prev.AddedSlot
		}
	}
	if prev != nil {
		vote.ID = prev.ID
	} else {
		_, id, err := s.ls.nextGovernanceIDs(r, txn)
		if err != nil {
			return err
		}
		vote.ID = uint(id)
	}
	next.ID = vote.ID
	if sameVote(prev, &next) {
		return nil
	}
	slot := vote.AddedSlot
	if vote.VoteUpdatedSlot != nil {
		slot = *vote.VoteUpdatedSlot
	}
	s.ls.stageGovernance(txn, LedgerChanges{Votes: []VoteChange{{
		Slot: slot, TxHash: bytes.Clone(txHash), ActionIndex: idx, Vote: next,
	}}})
	return nil
}

// GetAllGovernanceProposals lists the set's proposals in row ID order, soft
// deleted ones included.
func (s stagingLedgerDB) GetAllGovernanceProposals(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	r, err := s.ls.setGovernanceReader(txn)
	if err != nil {
		return nil, err
	}
	all, err := s.ls.proposalsView(r, txn)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(all, func(a, b models.GovernanceProposal) int {
		return cmp.Compare(a.ID, b.ID)
	})
	out := make([]*models.GovernanceProposal, len(all))
	for i := range all {
		c := cleanProposal(all[i])
		out[i] = &c
	}
	return out, nil
}

// GetAllGovernanceVotes lists the set's votes, soft deleted ones included,
// from the set.
func (s stagingLedgerDB) GetAllGovernanceVotes(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]*models.GovernanceVoteRef, error) {
	r, err := s.ls.setGovernanceReader(txn)
	if err != nil {
		return nil, err
	}
	all, err := s.ls.proposalsView(r, txn)
	if err != nil {
		return nil, err
	}
	out := []*models.GovernanceVoteRef{}
	for _, p := range all {
		votes, err := s.ls.votesView(r, txn, p.TxHash, p.ActionIndex)
		if err != nil {
			return nil, err
		}
		for _, v := range votes {
			out = append(out, &models.GovernanceVoteRef{
				TxHash:      bytes.Clone(p.TxHash),
				ActionIndex: p.ActionIndex,
				Vote:        cleanVote(v),
			})
		}
	}
	return out, nil
}

// GetGovernanceProposalsRefundedAt reads the set's proposals whose deposit
// was returned when epoch began.
func (s stagingLedgerDB) GetGovernanceProposalsRefundedAt(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]*models.GovernanceProposal, error) {
	r, err := s.ls.setGovernanceReader(txn)
	if err != nil {
		return nil, err
	}
	all, err := s.ls.proposalsView(r, txn)
	if err != nil {
		return nil, err
	}
	rows := selectProposals(all, func(p models.GovernanceProposal) bool {
		return p.DeletedSlot == nil &&
			(p.RatifiedEpoch != nil && samePtrT(p.EnactedEpoch, &epoch) ||
				samePtrT(p.DroppedEpoch, &epoch))
	}, cmpProposals)
	out := make([]*models.GovernanceProposal, len(rows))
	for i := range rows {
		c := cleanProposal(rows[i])
		out[i] = &c
	}
	return out, nil
}
