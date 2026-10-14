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

package governance

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
)

// ProposalDepositRefund is a proposal deposit that an epoch boundary credited
// to the proposal's return reward account.
type ProposalDepositRefund struct {
	CredentialTag uint8
	StakingKey    []byte
	// Amount is what the journal recorded as credited.
	Amount uint64
}

// proposalRefundSlot returns the boundary slot at which the proposal's deposit
// was returned when epoch began: its enactment, or its drop. A proposal is
// refunded at exactly one of the two.
func proposalRefundSlot(
	proposal *models.GovernanceProposal,
	epoch uint64,
) (uint64, bool) {
	if proposal == nil {
		return 0, false
	}
	if proposal.EnactedEpoch != nil && *proposal.EnactedEpoch == epoch &&
		proposal.EnactedSlot != nil {
		return *proposal.EnactedSlot, true
	}
	if proposal.DroppedEpoch != nil && *proposal.DroppedEpoch == epoch &&
		proposal.DroppedSlot != nil {
		return *proposal.DroppedSlot, true
	}
	return 0, false
}

// ProposalDepositRefundSlots returns the distinct boundary slots, ascending,
// at which the proposals' deposits were returned when epoch began. The journal
// credits at those slots are what MatchProposalDepositRefunds reads.
func ProposalDepositRefundSlots(
	proposals []*models.GovernanceProposal,
	epoch uint64,
) []uint64 {
	var slots []uint64
	for _, proposal := range proposals {
		slot, ok := proposalRefundSlot(proposal, epoch)
		if ok && !slices.Contains(slots, slot) {
			slots = append(slots, slot)
		}
	}
	slices.Sort(slots)
	return slots
}

// MatchProposalDepositRefunds returns the deposits among proposals that were
// actually credited to a reward account when epoch began, by pairing each
// proposal with the journal credit that carried it.
//
// A deposit whose return account was unregistered goes to the treasury and
// leaves no credit, so it is absent from the result; the proposal row alone
// cannot say which happened. The pairing key is the credit's source hash,
// the credential and the boundary slot: other post-snapshot credits to the
// same account (pool deposit refunds, treasury withdrawals) share the slot
// and the credential but not the source hash.
//
// A refund is journaled under the proposal-deposit-refund discriminator when
// the same proposal's treasury withdrawal pays the same account, and under the
// plain proposal hash otherwise (see refundProposalDeposit and the
// treasury-withdrawal branch of EnactProposal). The discriminated credit is
// tried first so a withdrawal credited under the plain hash is never read as
// the refund. A journal written before the discriminator existed holds only
// the withdrawal credit in that collision case, which is paired here as the
// refund and so reads as a value difference against the reference, which
// does list the deposit.
//
// credits must hold the journal credits at every slot returned by
// ProposalDepositRefundSlots. Each credit pairs with at most one proposal.
func MatchProposalDepositRefunds(
	proposals []*models.GovernanceProposal,
	epoch uint64,
	credits []*models.AccountRewardDelta,
) ([]ProposalDepositRefund, error) {
	used := make([]bool, len(credits))
	find := func(
		tag uint8,
		key []byte,
		slot uint64,
		source []byte,
	) (int, *models.AccountRewardDelta) {
		for i, credit := range credits {
			if used[i] || credit == nil ||
				credit.AddedSlot != slot ||
				credit.CredentialTag != tag ||
				!bytes.Equal(credit.StakingKey, key) ||
				!bytes.Equal(credit.TxHash, source) {
				continue
			}
			return i, credit
		}
		return -1, nil
	}
	var out []ProposalDepositRefund
	for _, proposal := range proposals {
		slot, ok := proposalRefundSlot(proposal, epoch)
		if !ok || proposal.Deposit == 0 {
			continue
		}
		tag, key, err := rewardAccountStakeCredential(proposal.ReturnAddress)
		if err != nil {
			return nil, fmt.Errorf(
				"proposal %s#%d: %w",
				shortHash(proposal.TxHash),
				proposal.ActionIndex,
				err,
			)
		}
		i, credit := find(
			tag, key, slot, proposalDepositRefundSourceHash(proposal),
		)
		if credit == nil {
			i, credit = find(
				tag, key, slot, proposalRewardSourceHash(proposal),
			)
		}
		if credit == nil {
			continue
		}
		used[i] = true
		out = append(out, ProposalDepositRefund{
			CredentialTag: tag,
			StakingKey:    key,
			Amount:        uint64(credit.Amount),
		})
	}
	return out, nil
}
