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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// enactStateTreasury is the treasury the ratify state's enact state carries.
// The reference node reports zero there, not the live treasury, because the
// field is the starting balance of a ratification pass, which only treasury
// withdrawals enacted in that pass change.
const enactStateTreasury uint64 = 0

// noFuturePParams is FuturePParams' NoPParamsUpdate, the value the ledger holds
// whenever no parameter change is scheduled for the next epoch.
var noFuturePParams = cbor.RawMessage{0x81, 0x00}

// queryShelleyGovState answers GetGovState with the Conway governance state.
//
// The proposals, enacted roots, committee, constitution and protocol
// parameters are read as they stand at the tip. The reference node also
// returns the DRep pulsing state it froze at the boundary that opened the
// epoch, and the ratification outcome computed then. Dingo keeps neither, so
// those two parts are rebuilt from live state: the DRep distribution and the
// pool distribution can differ from the node's once stake moves within an
// epoch, and the ratification outcome lists no enacted or expired actions and
// no delay. A pinned point is refused because that state cannot be rebuilt
// for an earlier slot.
func (ls *LedgerState) queryShelleyGovState(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (any, error) {
	if at.pinned() {
		return nil, fmt.Errorf(
			"%w: governance state as of slot %d cannot be reconstructed "+
				"-- the DRep distribution and ratification outcome of the "+
				"epoch boundary are not stored",
			ErrHistoricalStateUnavailable,
			at.Slot,
		)
	}
	txn, release := ls.readTxn(ctx, txn)
	defer release()

	proposals, err := ls.govStateProposals(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	roots, err := ls.govStateRoots(ctx, txn)
	if err != nil {
		return nil, err
	}
	committee, err := ls.govStateCommittee(ctx, txn)
	if err != nil {
		return nil, err
	}
	committeeRaw, err := olocalstatequery.EncodeCommittee(committee)
	if err != nil {
		return nil, fmt.Errorf("encode committee: %w", err)
	}
	constitution, err := ls.govStateConstitution(txn)
	if err != nil {
		return nil, err
	}
	currentPParams, prevPParams, err := ls.govStatePParams(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	pulsing, err := ls.govStatePulsingState(
		ctx,
		at,
		txn,
		proposals,
		roots,
		committeeRaw,
		constitution,
		currentPParams,
		prevPParams,
	)
	if err != nil {
		return nil, err
	}
	return []any{olocalstatequery.GovStateResult{
		ProposalRoots:    roots,
		Proposals:        proposals,
		Committee:        committeeRaw,
		Constitution:     constitution,
		CurrentPParams:   currentPParams,
		PrevPParams:      prevPParams,
		FuturePParams:    noFuturePParams,
		DRepPulsingState: pulsing,
	}}, nil
}

// govStateProposals returns the proposals GetProposals reports, with their
// votes.
func (ls *LedgerState) govStateProposals(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) ([]olocalstatequery.GovActionState, error) {
	reply, err := ls.queryShelleyGetProposals(ctx, nil, at, txn)
	if err != nil {
		return nil, err
	}
	wrapped, ok := reply.([]any)
	if !ok || len(wrapped) != 1 {
		return nil, fmt.Errorf("proposals reply has shape %T", reply)
	}
	proposals, ok := wrapped[0].(olocalstatequery.ProposalsResult)
	if !ok {
		return nil, fmt.Errorf("proposals reply holds %T", wrapped[0])
	}
	return proposals, nil
}

// govStateRoots returns the latest enacted action of each purpose.
func (ls *LedgerState) govStateRoots(
	ctx context.Context,
	txn ledgerdb.Txn,
) (olocalstatequery.PrevGovActionIds, error) {
	var roots olocalstatequery.PrevGovActionIds
	purposes := []struct {
		actionType uint8
		root       **lcommon.GovActionId
	}{
		{uint8(lcommon.GovActionTypeParameterChange), &roots.ParameterChange},
		{uint8(lcommon.GovActionTypeHardForkInitiation), &roots.HardFork},
		{uint8(lcommon.GovActionTypeUpdateCommittee), &roots.Committee},
		{uint8(lcommon.GovActionTypeNewConstitution), &roots.Constitution},
	}
	for _, purpose := range purposes {
		proposal, err := ls.ledgerDB().GetLastEnactedGovernanceProposal(
			ctx,
			governancePurposeActionTypes(purpose.actionType),
			txn,
		)
		if err != nil {
			return roots, fmt.Errorf("get enacted governance root: %w", err)
		}
		if proposal == nil {
			continue
		}
		id, err := lcommon.NewGovActionId(
			proposal.TxHash,
			proposal.ActionIndex,
		)
		if err != nil {
			return roots, fmt.Errorf("enacted governance root id: %w", err)
		}
		*purpose.root = &id
	}
	return roots, nil
}

// govStateCommittee returns the seated committee, or nil when none is seated
// or the committee has been dissolved by a NoConfidence action, which leaves
// no active members. The quorum is the enacted one, else the Conway genesis
// threshold the genesis committee was seated with.
func (ls *LedgerState) govStateCommittee(
	ctx context.Context,
	txn ledgerdb.Txn,
) (*olocalstatequery.Committee, error) {
	members, err := ls.ledgerDB().GetCommitteeMembers(ctx, txn)
	if err != nil {
		return nil, fmt.Errorf("get committee members: %w", err)
	}
	if len(members) == 0 {
		return nil, nil
	}
	var genesis *conway.ConwayGenesis
	if cfg := ls.config.CardanoNodeConfig; cfg != nil {
		genesis = cfg.ConwayGenesis()
	}
	quorum, err := governance.EffectiveCommitteeQuorum(
		ctx, ls.ledgerDB(), txn, genesis,
	)
	if err != nil {
		return nil, fmt.Errorf("get committee quorum: %w", err)
	}
	committee := &olocalstatequery.Committee{
		Members: make(
			map[olocalstatequery.StakeCredential]uint64,
			len(members),
		),
		Threshold: cbor.Rat{Rat: quorum},
	}
	for _, member := range members {
		hash, err := lcommon.NewBlake2b224Checked(member.ColdCredHash)
		if err != nil {
			return nil, fmt.Errorf("committee member credential: %w", err)
		}
		committee.Members[olocalstatequery.StakeCredential{
			Tag:   uint64(member.ColdCredentialTag),
			Bytes: hash,
		}] = member.ExpiresEpoch
	}
	return committee, nil
}

// govStateConstitution returns the current constitution.
func (ls *LedgerState) govStateConstitution(
	txn ledgerdb.Txn,
) (olocalstatequery.ConstitutionResult, error) {
	constitution, err := ls.ledgerDB().GetConstitution(txn)
	if err != nil {
		return olocalstatequery.ConstitutionResult{},
			fmt.Errorf("get constitution: %w", err)
	}
	if constitution == nil {
		return olocalstatequery.ConstitutionResult{},
			errors.New("get constitution: none is recorded")
	}
	dataHash, err := lcommon.NewBlake2b256Checked(constitution.AnchorHash)
	if err != nil {
		return olocalstatequery.ConstitutionResult{},
			fmt.Errorf("constitution anchor: %w", err)
	}
	return olocalstatequery.ConstitutionResult{
		Anchor: lcommon.GovAnchor{
			Url:      constitution.AnchorURL,
			DataHash: dataHash,
		},
		ScriptHash: constitution.PolicyHash,
	}, nil
}

// govStatePParams returns the protocol parameters in effect and those of the
// epoch before. When the earlier epoch has no stored row the parameters did
// not change, so the current ones stand in for them.
func (ls *LedgerState) govStatePParams(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (cbor.RawMessage, cbor.RawMessage, error) {
	reply, err := ls.queryShelleyCurrentProtocolParams(ctx, at, txn)
	if err != nil {
		return nil, nil, err
	}
	current, err := encodeWrappedReply(reply)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"encode current protocol parameters: %w",
			err,
		)
	}
	snapshot := ls.loadConsensusSnapshot()
	epoch := snapshot.currentEpoch.EpochId
	if epoch == 0 {
		return current, current, nil
	}
	prevReply, err := ls.historicalProtocolParameters(
		snapshot, epoch-1, at, txn,
	)
	if err != nil {
		if errors.Is(err, errPParamsRowMissing) ||
			errors.Is(err, ErrHistoricalStateUnavailable) {
			return current, current, nil
		}
		return nil, nil, err
	}
	prev, err := encodeWrappedReply(prevReply)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"encode previous protocol parameters: %w",
			err,
		)
	}
	return current, prev, nil
}

// encodeWrappedReply encodes the single value inside a one-element reply
// slice.
func encodeWrappedReply(reply any) (cbor.RawMessage, error) {
	wrapped, ok := reply.([]any)
	if !ok || len(wrapped) != 1 {
		return nil, fmt.Errorf("reply has shape %T", reply)
	}
	return cbor.Encode(wrapped[0])
}

// govStatePulsingState encodes the DRep pulsing state as the ledger's
// completed pulser: the epoch snapshot followed by the ratify state.
func (ls *LedgerState) govStatePulsingState(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
	proposals []olocalstatequery.GovActionState,
	roots olocalstatequery.PrevGovActionIds,
	committeeRaw cbor.RawMessage,
	constitution olocalstatequery.ConstitutionResult,
	currentPParams cbor.RawMessage,
	prevPParams cbor.RawMessage,
) (cbor.RawMessage, error) {
	drepDistr, err := ls.govStateDRepDistribution(ctx, txn)
	if err != nil {
		return nil, err
	}
	drepStateReply, err := ls.queryShelleyDRepState(ctx, nil, at, txn)
	if err != nil {
		return nil, err
	}
	drepState, err := drepStateFromReply(drepStateReply)
	if err != nil {
		return nil, err
	}
	poolDistr, err := ls.govStatePoolDistribution(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	snapshot, err := cbor.Encode([]any{
		proposals,
		drepDistr,
		drepState,
		poolDistr,
	})
	if err != nil {
		return nil, fmt.Errorf("encode pulsing snapshot: %w", err)
	}
	rootsRaw, err := roots.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("encode governance roots: %w", err)
	}
	constitutionRaw, err := encodeConstitution(constitution)
	if err != nil {
		return nil, err
	}
	enactState, err := cbor.Encode([]any{
		committeeRaw,
		constitutionRaw,
		currentPParams,
		prevPParams,
		enactStateTreasury,
		map[olocalstatequery.StakeCredential]uint64{},
		cbor.RawMessage(rootsRaw),
	})
	if err != nil {
		return nil, fmt.Errorf("encode enact state: %w", err)
	}
	ratifyState, err := cbor.Encode([]any{
		cbor.RawMessage(enactState),
		[]olocalstatequery.GovActionState{},
		cbor.Set{},
		false,
	})
	if err != nil {
		return nil, fmt.Errorf("encode ratify state: %w", err)
	}
	pulsing, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(ratifyState),
	})
	if err != nil {
		return nil, fmt.Errorf("encode DRep pulsing state: %w", err)
	}
	return pulsing, nil
}

// encodeConstitution encodes a constitution as the ledger does: the anchor and
// the guardrails script as a null or a hash.
func encodeConstitution(
	constitution olocalstatequery.ConstitutionResult,
) (cbor.RawMessage, error) {
	var guardrails any
	if constitution.ScriptHash != nil {
		guardrails = constitution.ScriptHash
	}
	enc, err := cbor.Encode([]any{constitution.Anchor, guardrails})
	if err != nil {
		return nil, fmt.Errorf("encode constitution: %w", err)
	}
	return enc, nil
}

// drepStateFromReply unwraps the DRep state map from a GetDRepState reply.
func drepStateFromReply(reply any) (olocalstatequery.DRepStateResult, error) {
	wrapped, ok := reply.([]any)
	if !ok || len(wrapped) != 1 {
		return nil, fmt.Errorf("DRep state reply has shape %T", reply)
	}
	state, ok := wrapped[0].(olocalstatequery.DRepStateResult)
	if !ok {
		return nil, fmt.Errorf("DRep state reply holds %T", wrapped[0])
	}
	return state, nil
}

// govStateDRepDistribution returns the stake delegated to each registered DRep
// and to the predefined options, encoded as a bare map: the snapshot carries
// the map itself, without the single-element wrapper a query reply adds.
func (ls *LedgerState) govStateDRepDistribution(
	ctx context.Context,
	txn ledgerdb.Txn,
) (cbor.RawMessage, error) {
	dreps, err := ls.ledgerDB().GetActiveDreps(ctx, txn)
	if err != nil {
		return nil, fmt.Errorf("get active DReps: %w", err)
	}
	var distr olocalstatequery.DRepStakeDistrResult
	if len(dreps) > 0 {
		creds := make([]models.StakeCredentialRef, len(dreps))
		for i, drep := range dreps {
			creds[i] = models.StakeCredentialRef{
				Tag: drep.CredentialTag,
				Key: drep.Credential,
			}
		}
		powers, err := ls.ledgerDB().GetDRepVotingPowerBatch(ctx, creds, 0, txn)
		if err != nil {
			return nil, fmt.Errorf("get DRep voting power: %w", err)
		}
		for i, drep := range dreps {
			if powers[creds[i].MapKey()] == 0 {
				continue
			}
			distr = append(distr, olocalstatequery.DRepStakeDistrEntry{
				Drep: lcommon.Drep{
					Type:       int(drep.CredentialTag),
					Credential: drep.Credential,
				},
				Stake: powers[creds[i].MapKey()],
			})
		}
	}
	predefined := []struct {
		modelType uint64
		drepType  int
	}{
		{models.DrepTypeAlwaysAbstain, int(models.DrepTypeAlwaysAbstain)},
		{
			models.DrepTypeAlwaysNoConfidence,
			int(models.DrepTypeAlwaysNoConfidence),
		},
	}
	predefinedTypes := make([]uint64, len(predefined))
	for i, p := range predefined {
		predefinedTypes[i] = p.modelType
	}
	virtual, err := ls.ledgerDB().GetDRepVotingPowerByType(
		ctx, predefinedTypes, 0, txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get predefined DRep voting power: %w", err)
	}
	for _, p := range predefined {
		if virtual[p.modelType] == 0 {
			continue
		}
		distr = append(distr, olocalstatequery.DRepStakeDistrEntry{
			Drep:  lcommon.Drep{Type: p.drepType},
			Stake: virtual[p.modelType],
		})
	}
	enc, err := distr.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("encode DRep distribution: %w", err)
	}
	// MarshalCBOR writes the query reply form; drop its one-element array
	// header to leave the map.
	if len(enc) == 0 || enc[0] != 0x81 {
		return nil, errors.New("DRep distribution has no result wrapper")
	}
	return enc[1:], nil
}

// govStatePoolDistribution returns each pool's stake as the pulsing snapshot
// records it.
func (ls *LedgerState) govStatePoolDistribution(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (map[ledger.Blake2b224]uint64, error) {
	dist, err := ls.PoolStakeDistribution(ctx, nil, at, txn)
	if err != nil {
		return nil, err
	}
	pools := make(map[ledger.Blake2b224]uint64, len(dist.Pools))
	for _, pool := range dist.Pools {
		pools[ledger.Blake2b224(pool.PoolKeyHash)] = pool.Stake
	}
	return pools, nil
}
