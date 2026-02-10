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
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// processGovernanceProposals extracts governance proposals from a Conway-era
// transaction and persists them to the database. Each proposal procedure in the
// transaction body is mapped to a GovernanceProposal model with the appropriate
// action type, parent action, anchor, and deposit information.
//
// The govActionLifetime parameter determines how many epochs a proposal remains
// active before expiring.
func processGovernanceProposals(
	tx lcommon.Transaction,
	point ocommon.Point,
	currentEpoch uint64,
	govActionLifetime uint64,
	db *database.Database,
	txn *database.Txn,
) error {
	proposals := tx.ProposalProcedures()
	if len(proposals) == 0 {
		return nil
	}

	txHash := tx.Hash().Bytes()

	for i, proposal := range proposals {
		actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(
			proposal.GovAction(),
		)
		if err != nil {
			return fmt.Errorf(
				"proposal %d in tx %x: %w",
				i,
				txHash[:8],
				err,
			)
		}

		anchor := proposal.Anchor()
		anchorHash := anchor.DataHash

		rewardAddr := proposal.RewardAccount()
		rewardAddrBytes, err := rewardAddr.Bytes()
		if err != nil {
			return fmt.Errorf(
				"encode reward address for proposal %d in tx %x: %w",
				i,
				txHash[:8],
				err,
			)
		}

		govProposal := &models.GovernanceProposal{
			TxHash:        txHash,
			ActionIndex:   uint32(i), //nolint:gosec
			ActionType:    actionType,
			ProposedEpoch: currentEpoch,
			ExpiresEpoch:  currentEpoch + govActionLifetime,
			AnchorUrl:     anchor.Url,
			AnchorHash:    anchorHash[:],
			Deposit:       proposal.Deposit(),
			ReturnAddress: rewardAddrBytes,
			AddedSlot:     point.Slot,
		}

		if parentTxHash != nil {
			govProposal.ParentTxHash = parentTxHash
			govProposal.ParentActionIdx = parentActionIdx
		}

		if len(policyHash) > 0 {
			govProposal.PolicyHash = policyHash
		}

		if err := db.SetGovernanceProposal(govProposal, txn); err != nil {
			return fmt.Errorf(
				"set governance proposal %d in tx %x: %w",
				i,
				txHash[:8],
				err,
			)
		}
	}

	return nil
}

// processGovernanceVotes extracts voting procedures from a Conway-era
// transaction and persists them to the database. Each vote maps a voter
// (CC member, DRep, or SPO) and a governance action to a vote choice.
func processGovernanceVotes(
	tx lcommon.Transaction,
	point ocommon.Point,
	db *database.Database,
	txn *database.Txn,
) error {
	votingProcedures := tx.VotingProcedures()
	if len(votingProcedures) == 0 {
		return nil
	}

	txHash := tx.Hash().Bytes()

	// Cache proposal lookups to avoid redundant DB queries when multiple
	// voters vote on the same governance action within a single transaction.
	type proposalKey struct {
		txHash    string
		actionIdx uint32
	}
	proposalCache := make(map[proposalKey]*models.GovernanceProposal)

	for voter, actionVotes := range votingProcedures {
		if voter == nil {
			continue
		}

		voterType, err := mapVoterType(voter.Type)
		if err != nil {
			return fmt.Errorf(
				"vote in tx %x: %w",
				txHash[:8],
				err,
			)
		}

		for actionId, procedure := range actionVotes {
			if actionId == nil {
				continue
			}

			// Look up the proposal, using a local cache to deduplicate
			cacheKey := proposalKey{
				txHash:    string(actionId.TransactionId[:]),
				actionIdx: actionId.GovActionIdx,
			}
			proposal, ok := proposalCache[cacheKey]
			if !ok {
				var err error
				proposal, err = db.GetGovernanceProposal(
					actionId.TransactionId[:],
					actionId.GovActionIdx,
					txn,
				)
				if err != nil {
					return fmt.Errorf(
						"lookup governance proposal for vote in tx %x: %w",
						txHash[:8],
						err,
					)
				}
				proposalCache[cacheKey] = proposal
			}

			// VoteUpdatedSlot is set to the current slot so that, on
			// upsert (voter changes their vote), the new slot is
			// recorded for rollback safety. For a brand-new vote
			// this value is written on INSERT as well; the rollback
			// code handles both cases correctly.
			updatedSlot := point.Slot
			vote := &models.GovernanceVote{
				ProposalID:      proposal.ID,
				VoterType:       voterType,
				VoterCredential: voter.Hash[:],
				Vote:            procedure.Vote,
				AddedSlot:       point.Slot,
				VoteUpdatedSlot: &updatedSlot,
			}

			if procedure.Anchor != nil {
				vote.AnchorUrl = procedure.Anchor.Url
				vote.AnchorHash = procedure.Anchor.DataHash[:]
			}

			if err := db.SetGovernanceVote(vote, txn); err != nil {
				return fmt.Errorf(
					"set governance vote in tx %x: %w",
					txHash[:8],
					err,
				)
			}
		}
	}

	return nil
}

// extractGovActionInfo extracts the action type, parent action ID, and policy
// hash from a governance action. Different action types have different fields,
// so this function uses type switching to handle each case. Returns an error
// for unrecognized action types.
func extractGovActionInfo(
	action lcommon.GovAction,
) (actionType uint8, parentTxHash []byte, parentActionIdx *uint32, policyHash []byte, err error) {
	switch a := action.(type) {
	case *conway.ConwayParameterChangeGovAction:
		actionType = uint8(lcommon.GovActionTypeParameterChange)
		if a.ActionId != nil {
			parentTxHash = a.ActionId.TransactionId[:]
			idx := a.ActionId.GovActionIdx
			parentActionIdx = &idx
		}
		policyHash = a.PolicyHash
	case *lcommon.HardForkInitiationGovAction:
		actionType = uint8(lcommon.GovActionTypeHardForkInitiation)
		if a.ActionId != nil {
			parentTxHash = a.ActionId.TransactionId[:]
			idx := a.ActionId.GovActionIdx
			parentActionIdx = &idx
		}
	case *lcommon.TreasuryWithdrawalGovAction:
		actionType = uint8(lcommon.GovActionTypeTreasuryWithdrawal)
		policyHash = a.PolicyHash
	case *lcommon.NoConfidenceGovAction:
		actionType = uint8(lcommon.GovActionTypeNoConfidence)
		if a.ActionId != nil {
			parentTxHash = a.ActionId.TransactionId[:]
			idx := a.ActionId.GovActionIdx
			parentActionIdx = &idx
		}
	case *lcommon.UpdateCommitteeGovAction:
		actionType = uint8(lcommon.GovActionTypeUpdateCommittee)
		if a.ActionId != nil {
			parentTxHash = a.ActionId.TransactionId[:]
			idx := a.ActionId.GovActionIdx
			parentActionIdx = &idx
		}
	case *lcommon.NewConstitutionGovAction:
		actionType = uint8(lcommon.GovActionTypeNewConstitution)
		if a.ActionId != nil {
			parentTxHash = a.ActionId.TransactionId[:]
			idx := a.ActionId.GovActionIdx
			parentActionIdx = &idx
		}
	case *lcommon.InfoGovAction:
		actionType = uint8(lcommon.GovActionTypeInfo)
	default:
		return 0, nil, nil, nil, fmt.Errorf(
			"unrecognized governance action type: %T",
			action,
		)
	}
	return actionType, parentTxHash, parentActionIdx, policyHash, nil
}

// mapVoterType maps gouroboros voter type constants to the database model
// voter type constants. CC hot key hash and script hash both map to VoterTypeCC,
// DRep key hash and script hash both map to VoterTypeDRep, and staking pool
// key hash maps to VoterTypeSPO.
func mapVoterType(voterType uint8) (uint8, error) {
	switch voterType {
	case lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		lcommon.VoterTypeConstitutionalCommitteeHotScriptHash:
		return models.VoterTypeCC, nil
	case lcommon.VoterTypeDRepKeyHash,
		lcommon.VoterTypeDRepScriptHash:
		return models.VoterTypeDRep, nil
	case lcommon.VoterTypeStakingPoolKeyHash:
		return models.VoterTypeSPO, nil
	default:
		return 0, fmt.Errorf("unrecognized voter type: %d", voterType)
	}
}
