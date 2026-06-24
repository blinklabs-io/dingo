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
	// DRepState and SPOState carry the proposal-independent voting
	// denominators (DRep voting power, pool stake snapshot) so they are
	// computed once per epoch tick and reused across every proposal's
	// tally. When nil, the tally functions lazily load them, preserving
	// standalone behavior. ProcessEpoch precomputes both before the
	// proposal loop, mirroring CommitteeState.
	DRepState *DRepVotingState
	SPOState  *SPOVotingState
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

// DRepVotingState is the proposal-independent DRep voting view for an
// epoch tick: the active DReps, their stake-weighted voting power, and
// the predefined AlwaysAbstain / AlwaysNoConfidence powers. DRep voting
// power is a function of the stake snapshot, not of any individual
// proposal, so this is computed once per epoch and reused across every
// proposal's tally. Recomputing it per proposal ran the heavy
// account/utxo voting-power aggregation once for every active proposal;
// on a freshly Mithril-restored database at an epoch boundary with many
// active proposals that stalled the epoch rollover (and therefore the
// whole ledger) for hours.
type DRepVotingState struct {
	// Dreps are the active-at-epoch regular DReps (active flag set and
	// not expired by inactivity).
	Dreps []*models.Drep
	// Powers maps StakeCredentialRef.MapKey() to stake-weighted voting
	// power for every entry in Dreps.
	Powers map[string]uint64
	// AbstainPower / NoConfidencePower are the predefined DRep option
	// powers (AlwaysAbstain / AlwaysNoConfidence).
	AbstainPower      uint64
	NoConfidencePower uint64
}

// LoadDRepVotingState computes the DRep voting denominators for the
// given epoch. It is the single heavy query (active DReps + batched
// voting power) hoisted out of the per-proposal tally path.
func LoadDRepVotingState(
	db *database.Database,
	txn *database.Txn,
	currentEpoch uint64,
) (*DRepVotingState, error) {
	if db == nil {
		return nil, errors.New("nil database")
	}
	allDreps, err := db.GetActiveDreps(txn)
	if err != nil {
		return nil, fmt.Errorf("get active dreps: %w", err)
	}

	// GetActiveDreps filters by the `active` flag (cleared only on
	// deregistration); expired-by-inactivity DReps still return active
	// and would inflate the tally denominator. Skip any whose
	// expiry_epoch has passed. A zero expiry_epoch means the DRep has
	// never had activity recorded and is treated as unexpired.
	dreps := make([]*models.Drep, 0, len(allDreps))
	for _, drep := range allDreps {
		if !drepActiveAtEpoch(drep, currentEpoch) {
			continue
		}
		dreps = append(dreps, drep)
	}

	powers := make(map[string]uint64)
	if len(dreps) > 0 {
		// Batch-fetch voting power for all active DReps in one query to
		// avoid the N+1 round-trip that the per-DRep lookup produced.
		creds := make([]models.StakeCredentialRef, len(dreps))
		for i, drep := range dreps {
			creds[i] = models.StakeCredentialRef{Tag: drep.CredentialTag, Key: drep.Credential}
		}
		powers, err = db.GetDRepVotingPowerBatch(creds, txn)
		if err != nil {
			return nil, fmt.Errorf("batch drep voting power: %w", err)
		}
	}

	virtualPowers, err := db.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("predefined drep voting power: %w", err)
	}
	return &DRepVotingState{
		Dreps:             dreps,
		Powers:            powers,
		AbstainPower:      virtualPowers[models.DrepTypeAlwaysAbstain],
		NoConfidencePower: virtualPowers[models.DrepTypeAlwaysNoConfidence],
	}, nil
}

// tallyDRepVotes sums voting power for regular DReps and the predefined
// AlwaysAbstain / AlwaysNoConfidence DRep options. Non-voting regular
// DReps are not counted toward any bucket. The proposal-independent
// voting power is taken from ctx.DRepState when present (precomputed
// once per epoch by ProcessEpoch); otherwise it is loaded lazily.
func tallyDRepVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	state := ctx.DRepState
	if state == nil {
		var err error
		state, err = LoadDRepVotingState(ctx.DB, ctx.Txn, ctx.CurrentEpoch)
		if err != nil {
			return err
		}
	}

	if len(state.Dreps) > 0 {
		// Index votes by full DRep credential identity for O(1) lookup.
		voteByCred := make(map[string]uint8, len(votes))
		for _, v := range votes {
			ref := models.StakeCredentialRef{
				Tag: v.VoterCredentialTag,
				Key: v.VoterCredential,
			}
			voteByCred[ref.MapKey()] = v.Vote
		}

		for _, drep := range state.Dreps {
			ref := models.StakeCredentialRef{Tag: drep.CredentialTag, Key: drep.Credential}
			power := state.Powers[ref.MapKey()]
			tally.DRepTotalStake += power

			vote, voted := voteByCred[ref.MapKey()]
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

	abstainPower := state.AbstainPower
	noConfidencePower := state.NoConfidencePower
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
//   - Pools without an explicit vote fall back to the auto-vote
//     pre-computed at snapshot capture time
//     (PoolStakeSnapshot.RewardAccountAutoVote):
//     Abstain        → SPOAbstainStake (excluded from the active
//     denominator via SPOYesRatio).
//     NoConfidence   → Yes for NoConfidence actions, No for any
//     other action type (mirrors the DRep
//     AlwaysNoConfidence handling).
//   - Any other delegation (regular DRep, no DRep set, deregistered
//     reward account, etc.) maps to None at snapshot time; the pool's
//     stake remains in SPOTotalStake and counts as implicit no under
//     CIP-1694 (in the denominator, not the numerator).
//
// Reading the auto-vote off the snapshot row makes the tally
// snapshot-correct: a pool that re-delegates its reward account or
// changes its reward account between StakeEpoch and the ratification
// epoch does not retroactively shift the tally, matching
// cardano-ledger's ssDelegations/ssDReps semantics.
// SPOVotingState is the proposal-independent SPO voting view for an
// epoch tick: the pool stake distribution snapshot ("mark" at
// StakeEpoch) and its total stake. Like DRepVotingState it is computed
// once per epoch and reused across every proposal's tally.
type SPOVotingState struct {
	// Dist is the pool stake snapshot rows, each carrying the pool's
	// stake and its pre-resolved reward-account auto-vote.
	Dist []*models.PoolStakeSnapshot
	// TotalStake is the sum of every snapshot row's stake.
	TotalStake uint64
}

// LoadSPOVotingState reads the "mark" pool stake snapshot for stakeEpoch
// and sums its total stake.
func LoadSPOVotingState(
	db *database.Database,
	txn *database.Txn,
	stakeEpoch uint64,
) (*SPOVotingState, error) {
	if db == nil {
		return nil, errors.New("nil database")
	}
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	dist, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		stakeEpoch,
		"mark",
		metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf("get pool stake snapshot: %w", err)
	}
	var total uint64
	for _, s := range dist {
		total += uint64(s.TotalStake)
	}
	return &SPOVotingState{Dist: dist, TotalStake: total}, nil
}

func tallySPOVotes(
	ctx *TallyContext,
	votes []*models.GovernanceVote,
	tally *ProposalTally,
) error {
	state := ctx.SPOState
	if state == nil {
		var err error
		state, err = LoadSPOVotingState(ctx.DB, ctx.Txn, ctx.StakeEpoch)
		if err != nil {
			return err
		}
	}

	dist := state.Dist
	tally.SPOTotalStake = state.TotalStake
	if len(dist) == 0 {
		return nil
	}

	// Explicit vote precedence: build a pool-keyed view first so we
	// can short-circuit the auto-vote branch for pools that did vote.
	voteByPool := make(map[string]uint8, len(votes))
	for _, v := range votes {
		voteByPool[string(v.VoterCredential)] = v.Vote
	}

	isNoConfidenceAction := lcommon.GovActionType(tally.ActionType) ==
		lcommon.GovActionTypeNoConfidence

	for _, s := range dist {
		stake := uint64(s.TotalStake)

		if v, voted := voteByPool[string(s.PoolKeyHash)]; voted {
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

		// Only trust RewardAccountAutoVote when the row is flagged
		// as resolved. Unresolved rows (Mithril-imported set/go
		// rotations, or rows written by pre-CIP-1694 code) fall
		// back to PoolRewardAccountAutoVoteNone — implicit no — so
		// stale or never-computed values can never silently bucket
		// stake into Abstain or NoConfidence.
		if !s.RewardAccountAutoVoteResolved {
			continue
		}
		switch s.RewardAccountAutoVote {
		case models.PoolRewardAccountAutoVoteAbstain:
			tally.SPOAbstainStake += stake
		case models.PoolRewardAccountAutoVoteNoConfidence:
			if isNoConfidenceAction {
				tally.SPOYesStake += stake
			} else {
				tally.SPONoStake += stake
			}
		case models.PoolRewardAccountAutoVoteNone:
			// No auto-vote: pool contributes only to SPOTotalStake
			// (implicit no under CIP-1694).
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
