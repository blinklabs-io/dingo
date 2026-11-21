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
	"github.com/blinklabs-io/dingo/ledger/eras"
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
// The roots, committee, constitution and protocol parameters are read as they
// stand at the tip. The reference node also returns the DRep pulsing state it
// froze at the boundary that opened the epoch and the ratification outcome
// computed from it. Dingo stores neither, and a ratification outcome cannot be
// rebuilt from live state. It is empty exactly when no governance action
// exists to ratify, expire or enact, so the query is answered only then, with
// empty proposals, no enacted or expired actions, no delay and no scheduled
// parameter change; while an action is pending it fails with
// ErrQueryNotAnswered rather than report no outcome. The DRep and pool
// distributions are read live, so they can differ from the node's once stake
// moves within an epoch.
//
// A pinned point is answered only when it is the current tip, which is what a
// reopened view names while the chain has not advanced; the live state is then
// the state at that point. Any other point is refused, because the stake
// distributions of the epoch boundary cannot be rebuilt for an earlier slot.
func (ls *LedgerState) queryShelleyGovState(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (any, error) {
	txn, release := ls.readTxn(ctx, txn)
	defer release()
	if at.pinned() {
		tip, err := ls.ledgerDB().GetTip(txn)
		if err != nil {
			return nil, fmt.Errorf("get chain tip: %w", err)
		}
		if tip.Point.Slot != at.Slot ||
			!bytes.Equal(tip.Point.Hash, at.Hash) {
			return nil, fmt.Errorf(
				"%w: governance state as of slot %d cannot be "+
					"reconstructed -- the DRep distribution of the epoch "+
					"boundary is not stored",
				ErrHistoricalStateUnavailable,
				at.Slot,
			)
		}
		at = QueryPoint{}
	}
	epoch, err := ls.govStateEpoch(txn, at)
	if err != nil {
		return nil, err
	}
	if err := ls.govStateRequireNoPendingActions(ctx, epoch, txn); err != nil {
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
	currentPParams, prevPParams, err := ls.govStatePParams(
		ctx, at, txn, epoch,
	)
	if err != nil {
		return nil, err
	}
	pulsing, err := ls.govStatePulsingState(
		ctx,
		at,
		txn,
		epoch,
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
		Proposals:        []olocalstatequery.GovActionState{},
		Committee:        committeeRaw,
		Constitution:     constitution,
		CurrentPParams:   currentPParams,
		PrevPParams:      prevPParams,
		FuturePParams:    noFuturePParams,
		DRepPulsingState: pulsing,
	}}, nil
}

// govStateBoundarySlot returns the last slot before the epoch began, the point
// at which the reference node's pulser captures its inputs.
func govStateBoundarySlot(epoch models.Epoch) uint64 {
	if epoch.StartSlot == 0 {
		return 0
	}
	return epoch.StartSlot - 1
}

// govStateRequireNoPendingActions fails with ErrQueryNotAnswered while any
// governance action exists, now or in the epoch-start snapshot. Either can
// leave the ratification outcome non-empty, and Dingo cannot compute it.
func (ls *LedgerState) govStateRequireNoPendingActions(
	ctx context.Context,
	epoch models.Epoch,
	txn ledgerdb.Txn,
) error {
	live, err := ls.ledgerDB().GetGovernanceProposalSet(ctx, txn)
	if err != nil {
		return fmt.Errorf("get governance proposal set: %w", err)
	}
	if len(live) > 0 {
		return queryNotAnswered("GetGovState", missingRatificationOutcome)
	}
	// Slot zero precedes every block, so nothing was proposed then.
	boundary := govStateBoundarySlot(epoch)
	if boundary == 0 {
		return nil
	}
	atBoundary, err := ls.ledgerDB().
		GetGovernanceProposalSetAtSlot(ctx, boundary, txn)
	if err != nil {
		return fmt.Errorf(
			"get governance proposal set at slot %d: %w",
			boundary,
			err,
		)
	}
	if len(atBoundary) > 0 {
		return queryNotAnswered("GetGovState", missingRatificationOutcome)
	}
	return nil
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

// govStateEpoch returns the epoch the query answers for: the epoch at the tip
// of the read transaction, which for a view acquired before a boundary is the
// epoch it froze rather than the live one.
func (ls *LedgerState) govStateEpoch(
	txn ledgerdb.Txn,
	at QueryPoint,
) (models.Epoch, error) {
	row, err := ls.snapshotEpoch(txn, at)
	if err != nil {
		return models.Epoch{}, err
	}
	if row != nil {
		return *row, nil
	}
	return ls.loadConsensusSnapshot().currentEpoch, nil
}

// govStatePParams returns the protocol parameters in effect and those of the
// epoch before the one the query answers for. When the earlier epoch has no
// stored row the parameters did not change, so the current ones stand in for
// them. An earlier epoch in another era is converted to the current era's
// representation, as the era transition does.
func (ls *LedgerState) govStatePParams(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
	epoch models.Epoch,
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
	if epoch.EpochId == 0 {
		return current, current, nil
	}
	prevEpoch := epoch.EpochId - 1
	prevReply, err := ls.historicalProtocolParameters(
		ls.loadConsensusSnapshot(), prevEpoch, at, txn,
	)
	if err != nil {
		if errors.Is(err, errPParamsRowMissing) ||
			errors.Is(err, ErrHistoricalStateUnavailable) {
			return current, current, nil
		}
		return nil, nil, err
	}
	wrapped, ok := prevReply.([]any)
	if !ok || len(wrapped) != 1 {
		return nil, nil, fmt.Errorf(
			"previous protocol parameters reply has shape %T", prevReply,
		)
	}
	prevParams, ok := wrapped[0].(lcommon.ProtocolParameters)
	if !ok {
		return nil, nil, fmt.Errorf(
			"previous protocol parameters hold %T", wrapped[0],
		)
	}
	prevRow, err := ls.ledgerDB().GetEpoch(prevEpoch, txn)
	if err != nil {
		return nil, nil, fmt.Errorf("get epoch %d: %w", prevEpoch, err)
	}
	if prevRow != nil && prevRow.EraId != epoch.EraId {
		era := eras.GetEraById(epoch.EraId)
		if era != nil && era.HardForkFunc != nil {
			prevParams, err = era.HardForkFunc(
				ls.config.CardanoNodeConfig, prevParams,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"convert previous protocol parameters: %w", err,
				)
			}
		}
	}
	prev, err := cbor.Encode(prevParams)
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
	epoch models.Epoch,
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
	// The pulser captures DRep registrations at the boundary, so they are read
	// as of the last slot before the epoch began. No proposals exist here.
	boundary := govStateBoundarySlot(epoch)
	drepStateReply, err := ls.queryShelleyDRepStateAt(ctx, nil, boundary, txn)
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
		[]olocalstatequery.GovActionState{},
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
