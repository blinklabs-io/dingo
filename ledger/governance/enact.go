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
	"errors"
	"fmt"
	"sort"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// EnactmentContext carries the inputs enactment needs: a writable
// transaction, the epoch and slot at which enactment takes effect,
// and the protocol-parameter update function for the current era.
type EnactmentContext struct {
	DB       *database.Database
	Txn      *database.Txn
	Epoch    uint64
	Slot     uint64
	PParams  lcommon.ProtocolParameters
	UpdateFn func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error)
}

// EnactmentResult is returned from EnactProposal when an action mutates
// the in-memory protocol parameters (ParameterChange, HardForkInitiation).
// The caller is responsible for persisting UpdatedPParams via SetPParams.
type EnactmentResult struct {
	UpdatedPParams lcommon.ProtocolParameters
	PParamsChanged bool
}

// EnactProposal applies the side effects of a ratified governance
// proposal and marks it as enacted at the given epoch/slot. It
// returns an EnactmentResult reflecting any in-memory pparam change.
func EnactProposal(
	ctx *EnactmentContext,
	proposal *models.GovernanceProposal,
) (*EnactmentResult, error) {
	if ctx == nil || ctx.DB == nil {
		return nil, errors.New("nil enactment context")
	}
	if proposal == nil {
		return nil, errors.New("nil proposal")
	}
	result := &EnactmentResult{UpdatedPParams: ctx.PParams}

	action, err := decodeGovAction(
		proposal.GovActionCbor,
		proposal.ActionType,
	)
	if err != nil {
		return nil, fmt.Errorf("decode gov action: %w", err)
	}

	switch a := action.(type) {
	case *conway.ConwayParameterChangeGovAction:
		updated, err := ctx.UpdateFn(ctx.PParams, &a.ParamUpdate)
		if err != nil {
			return nil, fmt.Errorf("apply param update: %w", err)
		}
		result.UpdatedPParams = updated
		result.PParamsChanged = true

	case *lcommon.HardForkInitiationGovAction:
		updated, err := setProtocolVersion(
			ctx.PParams,
			a.ProtocolVersion.Major,
			a.ProtocolVersion.Minor,
		)
		if err != nil {
			return nil, fmt.Errorf("schedule hard fork: %w", err)
		}
		result.UpdatedPParams = updated
		result.PParamsChanged = true

	case *lcommon.TreasuryWithdrawalGovAction:
		if err := applyTreasuryWithdrawal(ctx, a); err != nil {
			return nil, fmt.Errorf("treasury withdrawal: %w", err)
		}

	case *lcommon.NoConfidenceGovAction:
		if err := ctx.DB.SoftDeleteAllCommitteeMembers(
			ctx.Slot, ctx.Txn,
		); err != nil {
			return nil, fmt.Errorf("no confidence: %w", err)
		}
		// Drop the enacted committee quorum so ratification falls back
		// to Conway genesis until a subsequent UpdateCommittee enacts
		// a new positive threshold.
		if err := ctx.DB.ClearCommitteeQuorum(
			ctx.Slot, ctx.Txn,
		); err != nil {
			return nil, fmt.Errorf("no confidence: clear quorum: %w", err)
		}

	case *lcommon.UpdateCommitteeGovAction:
		if err := applyUpdateCommittee(ctx, a); err != nil {
			return nil, fmt.Errorf("update committee: %w", err)
		}

	case *lcommon.NewConstitutionGovAction:
		anchor := a.Constitution.Anchor
		constitution := &models.Constitution{
			AnchorURL:  anchor.Url,
			AnchorHash: anchor.DataHash[:],
			PolicyHash: a.Constitution.ScriptHash,
			AddedSlot:  ctx.Slot,
		}
		if err := ctx.DB.SetConstitution(
			constitution, ctx.Txn,
		); err != nil {
			return nil, fmt.Errorf("set constitution: %w", err)
		}

	case *lcommon.InfoGovAction:
		// Info actions have no on-chain effect; they stay ratified
		// until they expire.

	default:
		return nil, fmt.Errorf(
			"unsupported gov action type: %T", action,
		)
	}

	// Per CIP-1694, the deposit is returned to the proposer's reward
	// account when the proposal is finalized (enactment here, or
	// expiry in the EpochInput expiry path).
	if err := refundProposalDeposit(
		ctx.DB, ctx.Txn, proposal, ctx.Slot,
	); err != nil {
		return nil, fmt.Errorf("refund proposal deposit: %w", err)
	}

	// Mark the proposal as enacted so it is no longer considered active
	// and the next ratification tick picks up a new root.
	enactedEpoch := ctx.Epoch
	enactedSlot := ctx.Slot
	proposal.EnactedEpoch = &enactedEpoch
	proposal.EnactedSlot = &enactedSlot
	if err := ctx.DB.SetGovernanceProposal(
		proposal, ctx.Txn,
	); err != nil {
		return nil, fmt.Errorf("mark proposal enacted: %w", err)
	}

	return result, nil
}

// applyTreasuryWithdrawal debits the treasury by the sum of the
// per-address amounts and credits each destination reward account.
func applyTreasuryWithdrawal(
	ctx *EnactmentContext,
	a *lcommon.TreasuryWithdrawalGovAction,
) error {
	if ctx == nil || ctx.DB == nil {
		return errors.New("nil enactment context")
	}
	var metaTxn types.Txn
	if ctx.Txn != nil {
		metaTxn = ctx.Txn.Metadata()
	}
	state, err := ctx.DB.Metadata().GetNetworkState(metaTxn)
	if err != nil {
		return fmt.Errorf("get network state: %w", err)
	}
	var treasury, reserves uint64
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	var total uint64
	for _, amount := range a.Withdrawals {
		if total > ^uint64(0)-amount {
			return errors.New("treasury withdrawal amount overflow")
		}
		total += amount
	}
	if total > treasury {
		return fmt.Errorf(
			"treasury withdrawal of %d exceeds tracked treasury balance %d",
			total, treasury,
		)
	}
	for rewardAddr, amount := range a.Withdrawals {
		if amount == 0 {
			continue
		}
		if rewardAddr == nil {
			return errors.New("nil treasury withdrawal reward address")
		}
		rewardAddrBytes, err := rewardAddr.Bytes()
		if err != nil {
			return fmt.Errorf("encode treasury withdrawal reward address: %w", err)
		}
		stakeCredential, err := rewardAccountStakeCredential(
			rewardAddrBytes,
		)
		if err != nil {
			return fmt.Errorf("treasury withdrawal reward account: %w", err)
		}
		if err := ctx.DB.AddAccountReward(
			stakeCredential,
			amount,
			ctx.Slot,
			ctx.Txn,
		); err != nil {
			return fmt.Errorf("credit reward account: %w", err)
		}
	}
	return ctx.DB.Metadata().SetNetworkState(
		treasury-total,
		reserves,
		ctx.Slot,
		metaTxn,
	)
}

// applyUpdateCommittee removes the requested cold credentials and
// adds or updates new members with their expiry epochs from the
// action's credential-to-epoch map, then records the enacted quorum.
func applyUpdateCommittee(
	ctx *EnactmentContext,
	a *lcommon.UpdateCommitteeGovAction,
) error {
	removeHashes := make([][]byte, 0, len(a.Credentials))
	for _, c := range a.Credentials {
		hash := c.Credential
		removeHashes = append(removeHashes, hash[:])
	}
	if err := ctx.DB.SoftDeleteCommitteeMembers(
		removeHashes, ctx.Slot, ctx.Txn,
	); err != nil {
		return fmt.Errorf("remove members: %w", err)
	}
	if err := ctx.DB.SetCommitteeQuorum(
		a.Quorum.Rat, ctx.Slot, ctx.Txn,
	); err != nil {
		return fmt.Errorf("set committee quorum: %w", err)
	}
	if len(a.CredEpochs) == 0 {
		return nil
	}
	members := make([]*models.CommitteeMember, 0, len(a.CredEpochs))
	for cred, expiry := range a.CredEpochs {
		if cred == nil {
			continue
		}
		hash := cred.Credential
		members = append(members, &models.CommitteeMember{
			ColdCredHash: hash[:],
			ExpiresEpoch: uint64(expiry),
			AddedSlot:    ctx.Slot,
		})
	}
	if len(members) == 0 {
		return nil
	}
	// Sort by cold credential hash so the auto-increment ID assigned
	// by the DB is stable across nodes (Go map iteration is random).
	sort.Slice(members, func(i, j int) bool {
		return bytes.Compare(members[i].ColdCredHash, members[j].ColdCredHash) < 0
	})
	return ctx.DB.SetCommitteeMembers(members, ctx.Txn)
}

// decodeGovAction re-hydrates the GovAction value from its CBOR form.
// We switch on the action type recorded in the proposal model to
// pick the concrete target type.
func decodeGovAction(
	data []byte,
	actionType uint8,
) (lcommon.GovAction, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf(
			"empty gov action cbor (action type %d)",
			actionType,
		)
	}
	switch lcommon.GovActionType(actionType) {
	case lcommon.GovActionTypeParameterChange:
		var a conway.ConwayParameterChangeGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	case lcommon.GovActionTypeHardForkInitiation:
		var a lcommon.HardForkInitiationGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	case lcommon.GovActionTypeTreasuryWithdrawal:
		var a lcommon.TreasuryWithdrawalGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	case lcommon.GovActionTypeNoConfidence:
		var a lcommon.NoConfidenceGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	case lcommon.GovActionTypeUpdateCommittee:
		var a lcommon.UpdateCommitteeGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	case lcommon.GovActionTypeNewConstitution:
		var a lcommon.NewConstitutionGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	case lcommon.GovActionTypeInfo:
		var a lcommon.InfoGovAction
		if _, err := cbor.Decode(data, &a); err != nil {
			return nil, err
		}
		return &a, nil
	}
	return nil, fmt.Errorf("unknown action type: %d", actionType)
}

// setProtocolVersion rebuilds the pparams with a new protocol version
// using the era's update function. We construct a minimal update that
// only touches the protocol version.
func setProtocolVersion(
	current lcommon.ProtocolParameters,
	major, minor uint,
) (lcommon.ProtocolParameters, error) {
	switch p := current.(type) {
	case *conway.ConwayProtocolParameters:
		updated := *p
		updated.ProtocolVersion.Major = major
		updated.ProtocolVersion.Minor = minor
		return &updated, nil
	}
	return nil, fmt.Errorf(
		"protocol version update unsupported for pparams type %T",
		current,
	)
}
