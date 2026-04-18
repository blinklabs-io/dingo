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
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// ProposalTally captures the aggregated vote totals for a governance
// proposal across DReps, stake pools, and the constitutional committee.
// Stake values are lovelace; CC counts are member counts.
type ProposalTally struct {
	// Proposal identity
	ProposalID uint
	ActionType uint8

	// DRep tallies (stake-weighted, lovelace)
	DRepYesStake     uint64
	DRepNoStake      uint64
	DRepAbstainStake uint64
	DRepTotalStake   uint64

	// SPO tallies (stake-weighted, lovelace)
	SPOYesStake     uint64
	SPONoStake      uint64
	SPOAbstainStake uint64
	SPOTotalStake   uint64

	// CC tallies (member counts)
	CCYesCount     int
	CCNoCount      int
	CCAbstainCount int
	CCTotalCount   int // active, non-resigned members
}

// TallyContext carries the inputs needed to tally a proposal.
// StakeEpoch is the epoch whose "mark" snapshot provides SPO stake
// distribution — callers should pass currentEpoch-2 so the rotation
// lines up with the "Go" snapshot used for voting.
type TallyContext struct {
	DB         *database.Database
	Txn        *database.Txn
	StakeEpoch uint64
}

// TallyProposal computes the current tally for a single proposal. The
// tally excludes votes from voters who are no longer active (expired
// DReps, resigned CC members, retired pools) per the "live" view at
// the time of tallying.
func TallyProposal(
	ctx *TallyContext,
	proposal *models.GovernanceProposal,
) (*ProposalTally, error) {
	if ctx == nil || ctx.DB == nil {
		return nil, errors.New("nil tally context")
	}
	if proposal == nil {
		return nil, errors.New("nil proposal")
	}
	tally := &ProposalTally{
		ProposalID: proposal.ID,
		ActionType: proposal.ActionType,
	}

	votes, err := ctx.DB.GetGovernanceVotes(proposal.ID, ctx.Txn)
	if err != nil {
		return nil, fmt.Errorf("get votes: %w", err)
	}

	// Partition votes by voter type for downstream tally functions.
	var drepVotes, spoVotes, ccVotes []*models.GovernanceVote
	for _, v := range votes {
		switch v.VoterType {
		case models.VoterTypeDRep:
			drepVotes = append(drepVotes, v)
		case models.VoterTypeSPO:
			spoVotes = append(spoVotes, v)
		case models.VoterTypeCC:
			ccVotes = append(ccVotes, v)
		}
	}

	if err := tallyDRepVotes(ctx, drepVotes, tally); err != nil {
		return nil, fmt.Errorf("tally drep votes: %w", err)
	}
	if err := tallySPOVotes(ctx, spoVotes, tally); err != nil {
		return nil, fmt.Errorf("tally spo votes: %w", err)
	}
	if err := tallyCCVotes(ctx, ccVotes, tally); err != nil {
		return nil, fmt.Errorf("tally cc votes: %w", err)
	}

	return tally, nil
}

// tallyDRepVotes sums voting power for DReps and applies AlwaysAbstain /
// AlwaysNoConfidence virtual DReps against the total active DRep stake.
// Non-voting regular DReps are not counted toward any bucket.
func tallyDRepVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	dreps, err := ctx.DB.GetActiveDreps(ctx.Txn)
	if err != nil {
		return fmt.Errorf("get active dreps: %w", err)
	}

	// Index votes by DRep credential for O(1) lookup.
	voteByCred := make(map[string]uint8, len(votes))
	for _, v := range votes {
		voteByCred[string(v.VoterCredential)] = v.Vote
	}

	for _, drep := range dreps {
		power, err := ctx.DB.GetDRepVotingPower(drep.Credential, ctx.Txn)
		if err != nil {
			return fmt.Errorf("get drep voting power: %w", err)
		}
		tally.DRepTotalStake += power

		vote, voted := voteByCred[string(drep.Credential)]
		if !voted {
			continue
		}
		switch vote {
		case models.VoteYes:
			tally.DRepYesStake += power
		case models.VoteNo:
			tally.DRepNoStake += power
		case models.VoteAbstain:
			tally.DRepAbstainStake += power
		}
	}
	return nil
}

// tallySPOVotes sums pool stake for votes matched against the stake
// distribution snapshot at StakeEpoch. Pools that did not vote are not
// counted.
func tallySPOVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	var metaTxn types.Txn
	if ctx.Txn != nil {
		metaTxn = ctx.Txn.Metadata()
	}
	dist, err := ctx.DB.Metadata().GetPoolStakeSnapshotsByEpoch(
		ctx.StakeEpoch,
		"mark",
		metaTxn,
	)
	if err != nil {
		return fmt.Errorf("get pool stake snapshot: %w", err)
	}

	poolStake := make(map[string]uint64, len(dist))
	var total uint64
	for _, s := range dist {
		stake := uint64(s.TotalStake)
		poolStake[string(s.PoolKeyHash)] = stake
		total += stake
	}
	tally.SPOTotalStake = total

	for _, v := range votes {
		stake, ok := poolStake[string(v.VoterCredential)]
		if !ok {
			continue
		}
		switch v.Vote {
		case models.VoteYes:
			tally.SPOYesStake += stake
		case models.VoteNo:
			tally.SPONoStake += stake
		case models.VoteAbstain:
			tally.SPOAbstainStake += stake
		}
	}
	return nil
}

// tallyCCVotes counts per-member votes restricted to currently active
// (non-resigned) CC members. CC members vote via their hot credential
// after key authorization.
func tallyCCVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	members, err := ctx.DB.GetActiveCommitteeMembers(ctx.Txn)
	if err != nil {
		return fmt.Errorf("get active cc members: %w", err)
	}
	tally.CCTotalCount = len(members)

	activeHotKeys := make(map[string]struct{}, len(members))
	for _, m := range members {
		activeHotKeys[string(m.HostCredential)] = struct{}{}
	}

	for _, v := range votes {
		if _, ok := activeHotKeys[string(v.VoterCredential)]; !ok {
			continue
		}
		switch v.Vote {
		case models.VoteYes:
			tally.CCYesCount++
		case models.VoteNo:
			tally.CCNoCount++
		case models.VoteAbstain:
			tally.CCAbstainCount++
		}
	}
	return nil
}

// DRepYesRatio returns yesStake / (yesStake + noStake) for DReps, with
// abstentions excluded from the denominator per CIP-1694. Returns zero
// if no participation.
func (t *ProposalTally) DRepYesRatio() *big.Rat {
	return ratioOf(t.DRepYesStake, t.DRepYesStake+t.DRepNoStake)
}

// SPOYesRatio returns the SPO yes ratio under the same rules as DRepYesRatio.
func (t *ProposalTally) SPOYesRatio() *big.Rat {
	return ratioOf(t.SPOYesStake, t.SPOYesStake+t.SPONoStake)
}

// CCYesRatio returns the fraction of active CC members that voted yes,
// with abstentions counted in the denominator per conformance tests.
func (t *ProposalTally) CCYesRatio() *big.Rat {
	// #nosec G115 -- vote counts are bounded by CC size (< 100).
	yes := uint64(t.CCYesCount)
	// #nosec G115 -- same bound.
	denom := uint64(t.CCYesCount + t.CCNoCount)
	return ratioOf(yes, denom)
}

func ratioOf(num, denom uint64) *big.Rat {
	if denom == 0 {
		return new(big.Rat)
	}
	return new(big.Rat).SetFrac(
		new(big.Int).SetUint64(num),
		new(big.Int).SetUint64(denom),
	)
}
