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
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
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

	// TreasuryWithdrawalRemaining tracks the ENACT rule's cumulative
	// withdrawal limit across all treasury-withdrawal actions enacted in
	// the same epoch boundary. Actual treasury can differ when a withdrawal
	// target is unregistered because unclaimed amounts remain in treasury.
	TreasuryWithdrawalRemaining    uint64
	TreasuryWithdrawalRemainingSet bool
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

	action, err := decodeGovActionForPParams(
		proposal.GovActionCbor,
		proposal.ActionType,
		ctx.PParams,
	)
	if err != nil {
		return nil, fmt.Errorf("decode gov action: %w", err)
	}

	switch a := action.(type) {
	case *conway.ConwayParameterChangeGovAction:
		updated, err := ctx.UpdateFn(ctx.PParams, a.ParamUpdate)
		if err != nil {
			return nil, fmt.Errorf("apply param update: %w", err)
		}
		result.UpdatedPParams = updated
		result.PParamsChanged = true

	case *gdijkstra.DijkstraParameterChangeGovAction:
		updated, err := ctx.UpdateFn(ctx.PParams, a.ParamUpdate)
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
		if err := applyTreasuryWithdrawal(ctx, a, proposal); err != nil {
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

func decodeGovActionForPParams(
	data []byte,
	actionType uint8,
	pparams lcommon.ProtocolParameters,
) (lcommon.GovAction, error) {
	if lcommon.GovActionType(actionType) ==
		lcommon.GovActionTypeParameterChange {
		if _, ok := pparams.(*gdijkstra.DijkstraProtocolParameters); ok {
			var a gdijkstra.DijkstraParameterChangeGovAction
			if _, err := cbor.Decode(data, &a); err != nil {
				return nil, err
			}
			return &a, nil
		}
	}
	return decodeGovAction(data, actionType)
}

// applyTreasuryWithdrawal debits the treasury by the sum of the
// per-address amounts, credits registered destination reward accounts,
// and leaves unclaimed withdrawals in the treasury. proposal identifies the
// enacted withdrawal action; its tx hash and action index are used as the
// per-event credit discriminator so the credit journals as a distinct,
// replay-idempotent row.
func applyTreasuryWithdrawal(
	ctx *EnactmentContext,
	a *lcommon.TreasuryWithdrawalGovAction,
	proposal *models.GovernanceProposal,
) error {
	if ctx == nil || ctx.DB == nil {
		return errors.New("nil enactment context")
	}
	sourceHash := proposalRewardSourceHash(proposal)
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
	if !ctx.TreasuryWithdrawalRemainingSet {
		ctx.TreasuryWithdrawalRemaining = treasury
		ctx.TreasuryWithdrawalRemainingSet = true
	}
	if total > ctx.TreasuryWithdrawalRemaining {
		return fmt.Errorf(
			"treasury withdrawal of %d exceeds tracked treasury withdrawal capacity %d",
			total, ctx.TreasuryWithdrawalRemaining,
		)
	}
	ctx.TreasuryWithdrawalRemaining -= total
	var unclaimed uint64
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
		credentialTag, stakeCredential, err := rewardAccountStakeCredential(
			rewardAddrBytes,
		)
		if err != nil {
			return fmt.Errorf("treasury withdrawal reward account: %w", err)
		}
		credited, err := CreditRegisteredRewardAccount(
			ctx.DB,
			ctx.Txn,
			credentialTag,
			stakeCredential,
			amount,
			ctx.Slot,
			sourceHash,
		)
		if err != nil {
			return err
		}
		if !credited {
			if unclaimed > ^uint64(0)-amount {
				return errors.New("unclaimed treasury withdrawal overflow")
			}
			unclaimed += amount
		}
	}
	return ctx.DB.Metadata().SetNetworkState(
		treasury-total+unclaimed,
		reserves,
		ctx.Slot,
		metaTxn,
	)
}

// CreditRegisteredRewardAccount credits a reward account by its stake
// credential, returning (true, nil) when the account exists and is active. It
// returns (false, nil) when no active account matches — the caller is expected
// to route the amount to the treasury instead. Shared by governance deposit
// refunds and POOLREAP pool-deposit refunds so both follow identical
// registered-vs-unclaimed accounting.
//
// sourceHash uniquely identifies the credit event (the refunded proposal
// identity hash, reaped pool key hash, or MIR event discriminator). It
// distinguishes two distinct refunds to the same account at the same epoch
// boundary as separate journal rows and makes a crash-replayed boundary
// idempotent. Pass nil when no per-event discriminator is available.
func CreditRegisteredRewardAccount(
	db *database.Database,
	txn *database.Txn,
	credentialTag uint8,
	stakeCredential []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
) (bool, error) {
	err := db.AddAccountRewardByCredential(
		credentialTag,
		stakeCredential,
		amount,
		slot,
		sourceHash,
		txn,
	)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, models.ErrAccountNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("credit reward account: %w", err)
}

// AddUnclaimedToTreasury adds an unclaimable amount (e.g. a deposit refund
// whose reward account is missing or inactive) to the treasury, leaving
// reserves unchanged and writing the updated NetworkState at the given slot.
// Shared by governance and POOLREAP for consistent treasury accounting.
func AddUnclaimedToTreasury(
	db *database.Database,
	txn *database.Txn,
	amount uint64,
	slot uint64,
) error {
	if amount == 0 {
		return nil
	}
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	state, err := db.Metadata().GetNetworkState(metaTxn)
	if err != nil {
		return fmt.Errorf("get network state: %w", err)
	}
	var treasury, reserves uint64
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	if treasury > ^uint64(0)-amount {
		return fmt.Errorf(
			"treasury overflow adding unclaimed reward amount %d",
			amount,
		)
	}
	return db.Metadata().SetNetworkState(
		treasury+amount,
		reserves,
		slot,
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
