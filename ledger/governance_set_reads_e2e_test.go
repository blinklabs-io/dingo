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

package ledger_test

import (
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// governanceReadNames are the LedgerDB governance reads the set serves in
// set mode; GetGovernanceVotesAtSlot reads vote history and stays on the
// store.
var governanceReadNames = []string{
	"GetGovernanceProposal", "GetActiveGovernanceProposals",
	"GetGovernanceProposalSet", "GetGovernanceProposalSetAtSlot",
	"GetExpiringGovernanceProposals", "GetExpiredGovernanceProposalsAt",
	"GetExpiredAwaitingDropGovernanceProposals",
	"GetDroppedGovernanceProposalsAt", "GetEnactedGovernanceProposalsAt",
	"GetRatifiedGovernanceProposals", "GetLastEnactedGovernanceProposal",
	"GetGovernanceVotes",
}

func optU64(p *uint64) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatUint(*p, 10)
}

func optU32(p *uint32) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatUint(uint64(*p), 10)
}

// proposalRow renders every field of a proposal, row ID included. The
// action CBOR is left out when withCbor is false.
func proposalRow(p *models.GovernanceProposal, withCbor bool) string {
	if p == nil {
		return "<nil>"
	}
	cbor := fmt.Sprintf("%x", p.GovActionCbor)
	if !withCbor {
		cbor = ""
	}
	return fmt.Sprintf(
		"id=%d %x#%d type=%d proposed=%d expires=%d parent=%x/%s "+
			"enacted=%s/%s ratified=%s/%s expired=%s/%s dropped=%s/%s "+
			"policy=%x anchor=%s/%x deposit=%d return=%x added=%d deleted=%s "+
			"txindex=%s cbor=%s",
		p.ID, p.TxHash, p.ActionIndex, p.ActionType, p.ProposedEpoch,
		p.ExpiresEpoch, p.ParentTxHash, optU32(p.ParentActionIdx),
		optU64(p.EnactedEpoch), optU64(p.EnactedSlot),
		optU64(p.RatifiedEpoch), optU64(p.RatifiedSlot),
		optU64(p.ExpiredEpoch), optU64(p.ExpiredSlot),
		optU64(p.DroppedEpoch), optU64(p.DroppedSlot),
		p.PolicyHash, p.AnchorURL, p.AnchorHash, p.Deposit, p.ReturnAddress,
		p.AddedSlot, optU64(p.DeletedSlot), optU32(p.TxIndex), cbor,
	)
}

func proposalRows(ps []*models.GovernanceProposal, withCbor bool) []string {
	rows := make([]string, 0, len(ps))
	for _, p := range ps {
		rows = append(rows, proposalRow(p, withCbor))
	}
	return rows
}

func voteRow(v *models.GovernanceVote) string {
	return fmt.Sprintf(
		"id=%d proposal=%d voter=%d/%d/%x vote=%d anchor=%s/%x added=%d "+
			"updated=%s deleted=%s cast=%x/%d",
		v.ID, v.ProposalID, v.VoterType, v.VoterCredentialTag,
		v.VoterCredential, v.Vote, v.AnchorURL, v.AnchorHash, v.AddedSlot,
		optU64(v.VoteUpdatedSlot), optU64(v.DeletedSlot), v.CastTxHash,
		v.CastCertIndex,
	)
}

func updateCommitteeCbor(
	t *testing.T,
	cold lcommon.Credential,
	expiry uint64,
) []byte {
	t.Helper()
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil, nil,
		map[*lcommon.Credential]uint64{&cold: expiry},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	return encoded
}
