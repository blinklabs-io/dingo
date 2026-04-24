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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type proposalSource interface {
	Id() lcommon.Blake2b256
	ProposalProcedures() []lcommon.ProposalProcedure
}

// ProcessProposals extracts governance proposals from a Conway-era
// transaction and persists them to the database. Each proposal procedure in the
// transaction body is mapped to a GovernanceProposal model with the appropriate
// action type, parent action, anchor, and deposit information.
//
// The govActionLifetime parameter determines how many epochs a proposal remains
// active before expiring.
func ProcessProposals(
	tx lcommon.Transaction,
	point ocommon.Point,
	currentEpoch uint64,
	govActionLifetime uint64,
	db *database.Database,
	txn *database.Txn,
) error {
	return persistGovernanceProposals(
		tx,
		point,
		currentEpoch,
		govActionLifetime,
		db,
		txn,
	)
}

func persistGovernanceProposals(
	tx proposalSource,
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

	txHash := tx.Id().Bytes()
	if len(txHash) != 32 {
		return fmt.Errorf("invalid tx hash length: got %d", len(txHash))
	}
	txHashForLog := shortHash(txHash)

	for i, proposal := range proposals {
		govAction := proposal.GovAction()
		actionType, parentTxHash, parentActionIdx, policyHash, err := extractGovActionInfo(govAction)
		if err != nil {
			return fmt.Errorf(
				"proposal %d in tx %s: %w",
				i,
				txHashForLog,
				err,
			)
		}

		anchor := proposal.Anchor()
		anchorHash := anchor.DataHash

		rewardAddr := proposal.RewardAccount()
		rewardAddrBytes, err := rewardAddr.Bytes()
		if err != nil {
			return fmt.Errorf(
				"encode reward address for proposal %d in tx %s: %w",
				i,
				txHashForLog,
				err,
			)
		}

		actionCbor, err := cbor.Encode(govAction)
		if err != nil {
			return fmt.Errorf(
				"encode gov action cbor for proposal %d in tx %s: %w",
				i,
				txHashForLog,
				err,
			)
		}

		govProposal := &models.GovernanceProposal{
			TxHash:        txHash,
			ActionIndex:   uint32(i), //nolint:gosec
			ActionType:    actionType,
			ProposedEpoch: currentEpoch,
			ExpiresEpoch:  currentEpoch + govActionLifetime,
			AnchorURL:     anchor.Url,
			AnchorHash:    anchorHash[:],
			Deposit:       proposal.Deposit(),
			ReturnAddress: rewardAddrBytes,
			GovActionCbor: actionCbor,
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
				"set governance proposal %d in tx %s: %w",
				i,
				txHashForLog,
				err,
			)
		}
	}

	return nil
}

// ProcessVotes extracts voting procedures from a Conway-era
// transaction and persists them to the database. Each vote maps a voter
// (CC member, DRep, or SPO) and a governance action to a vote choice.
//
// When a DRep votes, their activity epoch is updated to the current epoch,
// which resets their expiry countdown based on the dRepInactivityPeriod.
func ProcessVotes(
	tx lcommon.Transaction,
	point ocommon.Point,
	currentEpoch uint64,
	drepInactivityPeriod uint64,
	db *database.Database,
	txn *database.Txn,
) error {
	votingProcedures := tx.VotingProcedures()
	if len(votingProcedures) == 0 {
		return nil
	}

	txHash := tx.Hash().Bytes()
	if len(txHash) != 32 {
		return fmt.Errorf("invalid tx hash length: got %d", len(txHash))
	}
	txHashForLog := shortHash(txHash)

	// Cache proposal lookups to avoid redundant DB queries when multiple
	// voters vote on the same governance action within a single transaction.
	type proposalKey struct {
		txHash    string
		actionIdx uint32
	}
	proposalCache := make(map[proposalKey]*models.GovernanceProposal)
	repairCache := newProposalRepairCache()

	// Track DRep credentials that have already had their activity updated
	// in this transaction to avoid redundant DB writes.
	drepActivityUpdated := make(map[string]bool)

	for voter, actionVotes := range votingProcedures {
		if voter == nil {
			continue
		}

		voterType, err := mapVoterType(voter.Type)
		if err != nil {
			return fmt.Errorf(
				"vote in tx %s: %w",
				txHashForLog,
				err,
			)
		}

		// Update DRep activity when a DRep votes (once per DRep per tx)
		if voterType == models.VoterTypeDRep {
			credKey := string(voter.Hash[:])
			if !drepActivityUpdated[credKey] {
				err := db.UpdateDRepActivity(
					voter.Hash[:],
					currentEpoch,
					drepInactivityPeriod,
					txn,
				)
				if errors.Is(err, models.ErrDrepActivityNotUpdated) {
					// A valid DRep vote proves the credential exists on-chain.
					// If metadata lost the DRep row during recovery/bootstrap,
					// recreate a minimal active record so replay can continue.
					// InsertDrepIfAbsent never overwrites an existing row, so
					// any real registration metadata (added_slot, anchor_url,
					// anchor_hash, active) is preserved and rollback semantics
					// in RestoreDrepStateAtSlot remain intact.
					if setErr := db.InsertDrepIfAbsent(
						voter.Hash[:],
						point.Slot,
						"",
						nil,
						true,
						txn,
					); setErr != nil {
						return fmt.Errorf(
							"repair missing drep in tx %s: %w",
							txHashForLog,
							setErr,
						)
					}
					if logger := db.Logger(); logger != nil {
						logger.Warn(
							"recreated missing drep record during vote replay",
							"tx_hash", txHashForLog,
							"drep_hash", hex.EncodeToString(voter.Hash[:]),
							"slot", point.Slot,
							"component", "governance",
						)
					}
					err = db.UpdateDRepActivity(
						voter.Hash[:],
						currentEpoch,
						drepInactivityPeriod,
						txn,
					)
				}
				if err != nil {
					return fmt.Errorf(
						"update drep activity in tx %s: %w",
						txHashForLog,
						err,
					)
				}
				drepActivityUpdated[credKey] = true
			}
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
					if !errors.Is(err, models.ErrGovernanceProposalNotFound) {
						return fmt.Errorf(
							"lookup governance proposal for vote in tx %s: %w",
							txHashForLog,
							err,
						)
					}
					var repairErr error
					proposal, repairErr = repairMissingGovernanceProposal(
						actionId.TransactionId[:],
						actionId.GovActionIdx,
						db,
						txn,
						repairCache,
					)
					if repairErr != nil {
						if errors.Is(
							repairErr,
							models.ErrGovernanceProposalNotFound,
						) {
							return fmt.Errorf(
								"repair missing governance proposal for vote in tx %s failed to find tx/proposal %s#%d: %w",
								txHashForLog,
								shortHash(actionId.TransactionId[:]),
								actionId.GovActionIdx,
								repairErr,
							)
						}
						return fmt.Errorf(
							"repair missing governance proposal for vote in tx %s: %w",
							txHashForLog,
							repairErr,
						)
					}
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
				vote.AnchorURL = procedure.Anchor.Url
				vote.AnchorHash = procedure.Anchor.DataHash[:]
			}

			if err := db.SetGovernanceVote(vote, txn); err != nil {
				return fmt.Errorf(
					"set governance vote in tx %s: %w",
					txHashForLog,
					err,
				)
			}
		}
	}

	return nil
}

type proposalRepairCache struct {
	epochsByID                 map[uint64]models.Epoch
	govActionValidityByEpochID map[uint64]uint64
}

func newProposalRepairCache() *proposalRepairCache {
	return &proposalRepairCache{
		epochsByID:                 make(map[uint64]models.Epoch),
		govActionValidityByEpochID: make(map[uint64]uint64),
	}
}

func (c *proposalRepairCache) epochForSlot(
	slot uint64,
	db *database.Database,
	txn *database.Txn,
) (models.Epoch, error) {
	if c == nil {
		epoch, err := db.GetEpochBySlot(slot, txn)
		if err != nil {
			return models.Epoch{}, err
		}
		if epoch == nil {
			return models.Epoch{}, fmt.Errorf(
				"no epoch found for slot %d",
				slot,
			)
		}
		return *epoch, nil
	}
	for _, epoch := range c.epochsByID {
		if epochContainsSlot(epoch, slot) {
			return epoch, nil
		}
	}
	epoch, err := db.GetEpochBySlot(slot, txn)
	if err != nil {
		return models.Epoch{}, err
	}
	if epoch == nil {
		return models.Epoch{}, fmt.Errorf("no epoch found for slot %d", slot)
	}
	c.epochsByID[epoch.EpochId] = *epoch
	return *epoch, nil
}

func (c *proposalRepairCache) govActionValidityPeriod(
	epoch models.Epoch,
	proposalTxHash []byte,
	db *database.Database,
	txn *database.Txn,
) (uint64, error) {
	if c != nil {
		if validity, ok := c.govActionValidityByEpochID[epoch.EpochId]; ok {
			return validity, nil
		}
	}
	if epoch.EraId != conway.EraIdConway {
		return 0, fmt.Errorf(
			"unexpected era %d for governance proposal tx %s, expected Conway era %d",
			epoch.EraId,
			shortHash(proposalTxHash),
			conway.EraIdConway,
		)
	}
	era := eras.GetEraById(epoch.EraId)
	if era == nil {
		return 0, fmt.Errorf(
			"unknown era %d for governance proposal tx %s",
			epoch.EraId,
			shortHash(proposalTxHash),
		)
	}
	pparams, err := db.GetPParams(epoch.EpochId, era.DecodePParamsFunc, txn)
	if err != nil {
		return 0, fmt.Errorf(
			"load protocol params for governance proposal tx %s: %w",
			shortHash(proposalTxHash),
			err,
		)
	}
	conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
	if !ok {
		return 0, fmt.Errorf(
			"unexpected protocol params %T for governance proposal tx %s in era %d",
			pparams,
			shortHash(proposalTxHash),
			epoch.EraId,
		)
	}
	validity := conwayPParams.GovActionValidityPeriod
	if c != nil {
		c.govActionValidityByEpochID[epoch.EpochId] = validity
	}
	return validity, nil
}

func repairMissingGovernanceProposal(
	proposalTxHash []byte,
	actionIndex uint32,
	db *database.Database,
	txn *database.Txn,
	repairCache *proposalRepairCache,
) (*models.GovernanceProposal, error) {
	txRecord, err := db.GetTransactionByHash(proposalTxHash, txn)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup governance proposal tx %s: %w",
			shortHash(proposalTxHash),
			err,
		)
	}
	if txRecord == nil {
		return nil, models.ErrGovernanceProposalNotFound
	}
	txBodyCbor, err := db.CborCache().ResolveTxCbor(txn, proposalTxHash)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve governance proposal tx body %s: %w",
			shortHash(proposalTxHash),
			err,
		)
	}
	txBody, err := gledger.NewTransactionBodyFromCbor(
		uint(txRecord.Type), //nolint:gosec // era ID fits in uint
		txBodyCbor,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"decode governance proposal tx body %s: %w",
			shortHash(proposalTxHash),
			err,
		)
	}
	if !bytes.Equal(txBody.Id().Bytes(), proposalTxHash) {
		return nil, fmt.Errorf(
			"decoded governance proposal tx body hash mismatch for %s",
			shortHash(proposalTxHash),
		)
	}
	epoch, err := repairCache.epochForSlot(txRecord.Slot, db, txn)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup proposal epoch for tx %s: %w",
			shortHash(proposalTxHash),
			err,
		)
	}
	govActionValidityPeriod, err := repairCache.govActionValidityPeriod(
		epoch,
		proposalTxHash,
		db,
		txn,
	)
	if err != nil {
		return nil, err
	}
	if err := persistGovernanceProposals(
		txBody,
		ocommon.Point{
			Slot: txRecord.Slot,
			Hash: txRecord.BlockHash,
		},
		epoch.EpochId,
		govActionValidityPeriod,
		db,
		txn,
	); err != nil {
		return nil, fmt.Errorf(
			"rebuild governance proposal from tx %s: %w",
			shortHash(proposalTxHash),
			err,
		)
	}
	return db.GetGovernanceProposal(proposalTxHash, actionIndex, txn)
}

func epochContainsSlot(epoch models.Epoch, slot uint64) bool {
	if slot < epoch.StartSlot || epoch.LengthInSlots == 0 {
		return false
	}
	endSlot := epoch.StartSlot + uint64(epoch.LengthInSlots)
	return endSlot < epoch.StartSlot || slot < endSlot
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
