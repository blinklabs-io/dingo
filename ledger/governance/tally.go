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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// poolAutoVote enumerates the CIP-1694 reward-account delegation
// outcomes that produce an implicit SPO vote when the pool itself did
// not cast an explicit vote.
type poolAutoVote uint8

const (
	poolAutoVoteNone poolAutoVote = iota
	poolAutoVoteAbstain
	poolAutoVoteNoConfidence
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
	DB             *database.Database
	Txn            *database.Txn
	StakeEpoch     uint64
	CurrentEpoch   uint64
	CommitteeState *CommitteeVotingState
}

// CommitteeVotingState is the ratification view of the seated CC:
// non-expired, non-deleted cold credentials count in the denominator,
// while only members with a current hot-key authorization may cast votes.
type CommitteeVotingState struct {
	ActiveMemberCount     int
	MemberHotCredentials  []string
	HotCredentialPresence map[string]struct{}
}

// LoadCommitteeVotingState builds the current voting view of the CC by
// intersecting the seated committee membership table with the latest
// hot-key authorization certificates. Authorizations for removed or
// expired cold credentials are ignored.
func LoadCommitteeVotingState(
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
) (*CommitteeVotingState, error) {
	if db == nil {
		return nil, errors.New("nil database")
	}
	members, err := db.GetCommitteeMembers(txn)
	if err != nil {
		return nil, fmt.Errorf("get seated committee members: %w", err)
	}
	// Collect non-expired cold credentials so we can batch-check
	// resignation status. ExpiresEpoch is the first epoch the member
	// is no longer active; a member with ExpiresEpoch == currentEpoch
	// has just aged out and must not contribute to the CC denominator
	// this epoch (matches Cardano-ledger Haskell: active iff
	// currentEpoch < termEpoch).
	coldKeys := make([][]byte, 0, len(members))
	for _, member := range members {
		if member.ExpiresEpoch <= currentEpoch {
			continue
		}
		coldKeys = append(coldKeys, member.ColdCredHash)
	}
	// Resigned members must be excluded from the CC denominator per
	// CIP-1694; otherwise they act as implicit No votes because they
	// cannot cast a vote (no active hot-key authorization) but would
	// still occupy a slot in ActiveMemberCount.
	resigned, err := db.GetResignedCommitteeMembers(coldKeys, txn)
	if err != nil {
		return nil, fmt.Errorf("get resigned committee members: %w", err)
	}
	seated := make(map[string]struct{}, len(coldKeys))
	for _, key := range coldKeys {
		if resigned[string(key)] {
			continue
		}
		seated[string(key)] = struct{}{}
	}

	authorized, err := db.GetActiveCommitteeMembers(txn)
	if err != nil {
		return nil, fmt.Errorf("get active cc hot credentials: %w", err)
	}
	memberHotCredentials := make([]string, 0, len(authorized))
	hotCredentialPresence := make(map[string]struct{}, len(authorized))
	for _, member := range authorized {
		if _, ok := seated[string(member.ColdCredential)]; !ok {
			continue
		}
		hotCredential := string(member.HotCredential)
		memberHotCredentials = append(memberHotCredentials, hotCredential)
		hotCredentialPresence[hotCredential] = struct{}{}
	}

	return &CommitteeVotingState{
		ActiveMemberCount:     len(seated),
		MemberHotCredentials:  memberHotCredentials,
		HotCredentialPresence: hotCredentialPresence,
	}, nil
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

// tallyDRepVotes sums voting power for regular DReps and the predefined
// AlwaysAbstain / AlwaysNoConfidence DRep options. Non-voting regular
// DReps are not counted toward any bucket.
func tallyDRepVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	allDreps, err := ctx.DB.GetActiveDreps(ctx.Txn)
	if err != nil {
		return fmt.Errorf("get active dreps: %w", err)
	}

	// GetActiveDreps filters by the `active` flag (cleared only on
	// deregistration); expired-by-inactivity DReps still return active
	// and would inflate the tally denominator. Skip any whose
	// expiry_epoch has passed. A zero expiry_epoch means the DRep has
	// never had activity recorded and is treated as unexpired.
	dreps := make([]*models.Drep, 0, len(allDreps))
	for _, drep := range allDreps {
		if !drepActiveAtEpoch(drep, ctx.CurrentEpoch) {
			continue
		}
		dreps = append(dreps, drep)
	}

	if len(dreps) > 0 {
		// Batch-fetch voting power for all active DReps in one query to
		// avoid the N+1 round-trip that the per-DRep lookup produced.
		creds := make([][]byte, len(dreps))
		for i, drep := range dreps {
			creds[i] = drep.Credential
		}
		powers, err := ctx.DB.GetDRepVotingPowerBatch(creds, ctx.Txn)
		if err != nil {
			return fmt.Errorf("batch drep voting power: %w", err)
		}

		// Index votes by DRep credential for O(1) lookup.
		voteByCred := make(map[string]uint8, len(votes))
		for _, v := range votes {
			voteByCred[string(v.VoterCredential)] = v.Vote
		}

		for _, drep := range dreps {
			power := powers[string(drep.Credential)]
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
	}

	virtualPowers, err := ctx.DB.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		ctx.Txn,
	)
	if err != nil {
		return fmt.Errorf("predefined drep voting power: %w", err)
	}
	abstainPower := virtualPowers[models.DrepTypeAlwaysAbstain]
	noConfidencePower := virtualPowers[models.DrepTypeAlwaysNoConfidence]
	tally.DRepTotalStake += abstainPower + noConfidencePower
	tally.DRepAbstainStake += abstainPower
	if noConfidencePower > 0 {
		if lcommon.GovActionType(tally.ActionType) ==
			lcommon.GovActionTypeNoConfidence {
			tally.DRepYesStake += noConfidencePower
		} else {
			tally.DRepNoStake += noConfidencePower
		}
	}
	return nil
}

// tallySPOVotes computes SPO yes/no/abstain stake against the pool
// stake distribution snapshot at StakeEpoch, applying the CIP-1694
// reward-account delegation rules:
//
//   - Pools with an explicit vote in `votes` use that vote.
//   - Pools without an explicit vote fall back to the DRep delegation
//     attached to their reward-account stake credential:
//     AlwaysAbstain      → Abstain (excluded from the active
//     denominator via SPOYesRatio).
//     AlwaysNoConfidence → Yes for NoConfidence actions, No for any
//     other action type (matches the DRep
//     AlwaysNoConfidence handling).
//   - Any other delegation (regular DRep, no DRep set, or unregistered
//     reward account) does NOT auto-vote; the pool's stake remains in
//     SPOTotalStake and is implicitly "no" under CIP-1694 (in the
//     denominator, not the numerator).
//
// Reward-account → DRep delegation is read from the current Account
// state. This matches tallyDRepVotes, which also uses live state; a
// per-snapshot historical lookup would require schema additions Dingo
// does not yet maintain.
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

	var total uint64
	for _, s := range dist {
		total += uint64(s.TotalStake)
	}
	tally.SPOTotalStake = total
	if len(dist) == 0 {
		return nil
	}

	// Explicit vote precedence: build a pool-keyed view first so we
	// can short-circuit auto-vote lookup for pools that did vote.
	voteByPool := make(map[string]uint8, len(votes))
	for _, v := range votes {
		voteByPool[string(v.VoterCredential)] = v.Vote
	}

	autoVoteByPool, err := loadPoolAutoVotes(ctx, dist)
	if err != nil {
		return fmt.Errorf("load pool reward-account auto-votes: %w", err)
	}

	isNoConfidenceAction := lcommon.GovActionType(tally.ActionType) ==
		lcommon.GovActionTypeNoConfidence

	for _, s := range dist {
		stake := uint64(s.TotalStake)
		poolKey := string(s.PoolKeyHash)

		if v, voted := voteByPool[poolKey]; voted {
			switch v {
			case models.VoteYes:
				tally.SPOYesStake += stake
			case models.VoteNo:
				tally.SPONoStake += stake
			case models.VoteAbstain:
				tally.SPOAbstainStake += stake
			}
			continue
		}

		switch autoVoteByPool[poolKey] {
		case poolAutoVoteAbstain:
			tally.SPOAbstainStake += stake
		case poolAutoVoteNoConfidence:
			if isNoConfidenceAction {
				tally.SPOYesStake += stake
			} else {
				tally.SPONoStake += stake
			}
		case poolAutoVoteNone:
			// No auto-vote: pool contributes only to SPOTotalStake
			// (implicit no under CIP-1694).
		}
	}
	return nil
}

// loadPoolAutoVotes resolves each pool in the snapshot to its
// reward-account DRep delegation and returns the implied auto-vote.
// Pools whose reward account is unset, unregistered, or delegated to
// anything other than the predefined AlwaysAbstain /
// AlwaysNoConfidence DReps map to poolAutoVoteNone (no auto-vote).
func loadPoolAutoVotes(
	ctx *TallyContext,
	dist []*models.PoolStakeSnapshot,
) (map[string]poolAutoVote, error) {
	pkhs := make([]lcommon.PoolKeyHash, 0, len(dist))
	for _, s := range dist {
		pkhs = append(pkhs, lcommon.PoolKeyHash(s.PoolKeyHash))
	}
	pools, err := ctx.DB.GetPools(pkhs, ctx.Txn)
	if err != nil {
		return nil, fmt.Errorf("get pools: %w", err)
	}

	rewardAcctByPool := make(map[string][]byte, len(pools))
	rewardAccounts := make([][]byte, 0, len(pools))
	for i := range pools {
		ra := pools[i].RewardAccount
		if len(ra) == 0 {
			continue
		}
		rewardAcctByPool[string(pools[i].PoolKeyHash)] = ra
		rewardAccounts = append(rewardAccounts, ra)
	}
	if len(rewardAccounts) == 0 {
		return map[string]poolAutoVote{}, nil
	}

	// includeInactive=true so a deregistered reward account that still
	// carries an AlwaysAbstain/AlwaysNoConfidence delegation flag is
	// not silently dropped before we can decide.
	accounts, err := ctx.DB.GetAccounts(rewardAccounts, true, ctx.Txn)
	if err != nil {
		return nil, fmt.Errorf("get reward accounts: %w", err)
	}

	out := make(map[string]poolAutoVote, len(rewardAcctByPool))
	for poolKey, ra := range rewardAcctByPool {
		acct, ok := accounts[string(ra)]
		if !ok {
			continue
		}
		switch acct.DrepType {
		case models.DrepTypeAlwaysAbstain:
			out[poolKey] = poolAutoVoteAbstain
		case models.DrepTypeAlwaysNoConfidence:
			out[poolKey] = poolAutoVoteNoConfidence
		}
	}
	return out, nil
}

// tallyCCVotes counts per-member votes restricted to currently active
// (non-resigned) CC members. CC members vote via their hot credential
// after key authorization.
func tallyCCVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	committeeState := ctx.CommitteeState
	if committeeState == nil {
		var err error
		committeeState, err = LoadCommitteeVotingState(
			ctx.DB, ctx.Txn, ctx.CurrentEpoch,
		)
		if err != nil {
			return err
		}
	}
	tally.CCTotalCount = committeeState.ActiveMemberCount

	votesByHotCredential := make(map[string]uint8, len(votes))
	for _, vote := range votes {
		if _, ok := committeeState.HotCredentialPresence[string(vote.VoterCredential)]; ok {
			votesByHotCredential[string(vote.VoterCredential)] = vote.Vote
		}
	}

	for _, hotCredential := range committeeState.MemberHotCredentials {
		// Distinguish "no vote cast" from VoteNo. Both vote, voted are
		// needed because models.VoteNo is the zero value of uint8 and
		// would otherwise count every non-voting active CC member as
		// a No vote, inflating CCNoCount.
		vote, voted := votesByHotCredential[hotCredential]
		if !voted {
			continue
		}
		switch vote {
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

// DRepYesRatio returns yesStake / (totalActiveStake - abstainStake) per
// CIP-1694. Non-voting active DReps count implicitly against the proposal
// (in the denominator but not the numerator). Returns zero if every active
// DRep abstained or there is no active DRep stake.
func (t *ProposalTally) DRepYesRatio() *big.Rat {
	return ratioOf(
		t.DRepYesStake,
		saturatingSub(t.DRepTotalStake, t.DRepAbstainStake),
	)
}

// SPOYesRatio returns yesStake / (totalStake - abstainStake) per CIP-1694.
// Non-voting active pools count implicitly against the proposal.
func (t *ProposalTally) SPOYesRatio() *big.Rat {
	return ratioOf(
		t.SPOYesStake,
		saturatingSub(t.SPOTotalStake, t.SPOAbstainStake),
	)
}

// CCYesRatio returns yesCount / (totalActiveCount - abstainCount) per
// CIP-1694. Non-voting active CC members count implicitly against the
// proposal.
func (t *ProposalTally) CCYesRatio() *big.Rat {
	// #nosec G115 -- CC member counts bounded by CC size (< 100).
	yes := uint64(t.CCYesCount)
	// #nosec G115 -- same bound.
	denom := saturatingSub(
		uint64(t.CCTotalCount),
		uint64(t.CCAbstainCount),
	)
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

// saturatingSub returns a-b, or 0 when b > a. Guards the ratio
// denominators against a malformed tally where abstain would exceed
// the total (which should never happen but is cheap to defend against).
func saturatingSub(a, b uint64) uint64 {
	if b >= a {
		return 0
	}
	return a - b
}
