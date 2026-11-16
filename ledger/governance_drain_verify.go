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

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// verifyGovernance compares every proposal and vote row the set holds with
// the metadata store's, soft-deleted rows, row IDs, lifecycle fields and
// action CBOR included, and requires the set's ID sequence to start above
// every stored ID so a drained family never reuses one.
func verifyGovernance(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	r, ok := set.(GovernanceReader)
	if !ok {
		return errors.New("the set does not serve governance")
	}
	stored, err := ldb.GetAllGovernanceProposals(ctx, txn)
	if err != nil {
		return err
	}
	held, err := r.ProposalsAt(nil)
	if err != nil {
		return err
	}
	byKey := make(map[string]models.GovernanceProposal, len(held))
	for _, p := range held {
		byKey[govKey(p.TxHash, p.ActionIndex)] = p
	}
	var maxProposal uint
	for _, p := range stored {
		maxProposal = max(maxProposal, p.ID)
		h, ok := byKey[govKey(p.TxHash, p.ActionIndex)]
		delete(byKey, govKey(p.TxHash, p.ActionIndex))
		if !ok || !sameProposal(p, &h) ||
			!bytes.Equal(p.GovActionCbor, h.GovActionCbor) {
			return fmt.Errorf(
				"governance proposal %x#%d differs between the metadata store and the set",
				p.TxHash,
				p.ActionIndex,
			)
		}
	}
	if len(byKey) > 0 {
		return fmt.Errorf(
			"the set holds %d governance proposals the metadata store does not: rows differ",
			len(byKey),
		)
	}
	refs, err := ldb.GetAllGovernanceVotes(ctx, txn)
	if err != nil {
		return err
	}
	var maxVote uint
	seen := 0
	for _, ref := range refs {
		maxVote = max(maxVote, ref.Vote.ID)
		rows, err := r.VotesAt(ref.TxHash, ref.ActionIndex, nil)
		if err != nil {
			return err
		}
		matched := false
		for i := range rows {
			if voterKey(rows[i]) == voterKey(ref.Vote) {
				matched = sameVote(&rows[i], &ref.Vote)
				break
			}
		}
		if !matched {
			return fmt.Errorf(
				"governance vote on %x#%d by %x differs between the metadata store and the set",
				ref.TxHash,
				ref.ActionIndex,
				ref.Vote.VoterCredential,
			)
		}
		seen++
	}
	total := 0
	for _, p := range held {
		rows, err := r.VotesAt(p.TxHash, p.ActionIndex, nil)
		if err != nil {
			return err
		}
		total += len(rows)
	}
	if total != seen {
		return fmt.Errorf(
			"governance votes differ between the metadata store and the set: %d rows against %d",
			seen,
			total,
		)
	}
	nextProposal, nextVote, err := r.NextGovernanceIDs()
	if err != nil {
		return err
	}
	if nextProposal <= uint64(maxProposal) || nextVote <= uint64(maxVote) {
		return errors.New(
			"the set's governance ID sequence would reuse an ID the metadata store holds: rows differ",
		)
	}
	return nil
}
