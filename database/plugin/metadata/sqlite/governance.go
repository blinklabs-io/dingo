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

package sqlite

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetGovernanceProposal retrieves a governance proposal by transaction hash and action index.
// Returns nil if the proposal has been soft-deleted.
func (d *MetadataStoreSqlite) GetGovernanceProposal(
	txHash []byte,
	actionIndex uint32,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	var proposal models.GovernanceProposal
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"tx_hash = ? AND action_index = ? AND deleted_slot IS NULL",
		txHash,
		actionIndex,
	).First(&proposal); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &proposal, nil
}

// GetActiveGovernanceProposals retrieves all governance proposals that haven't expired.
func (d *MetadataStoreSqlite) GetActiveGovernanceProposals(
	epoch uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	var proposals []*models.GovernanceProposal
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"expires_epoch >= ? AND enacted_epoch IS NULL AND deleted_slot IS NULL",
		epoch,
	).Find(&proposals); result.Error != nil {
		return nil, result.Error
	}
	return proposals, nil
}

// SetGovernanceProposal creates or updates a governance proposal.
func (d *MetadataStoreSqlite) SetGovernanceProposal(
	proposal *models.GovernanceProposal,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tx_hash"},
			{Name: "action_index"},
		},
		// Note: added_slot is NOT updated on conflict to preserve rollback safety.
		// The original added_slot represents when the proposal was first submitted.
		// enacted_slot and ratified_slot track when status changes occur for rollback.
		DoUpdates: clause.AssignmentColumns([]string{
			"action_type",
			"proposed_epoch",
			"expires_epoch",
			"parent_tx_hash",
			"parent_action_idx",
			"enacted_epoch",
			"enacted_slot",
			"ratified_epoch",
			"ratified_slot",
			"policy_hash",
			"anchor_url",
			"anchor_hash",
			"deposit",
			"return_address",
			"deleted_slot",
		}),
	}
	if result := db.Clauses(onConflict).Create(proposal); result.Error != nil {
		return result.Error
	}
	return nil
}

// GetGovernanceVotes retrieves all votes for a governance proposal.
func (d *MetadataStoreSqlite) GetGovernanceVotes(
	proposalID uint,
	txn types.Txn,
) ([]*models.GovernanceVote, error) {
	var votes []*models.GovernanceVote
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"proposal_id = ? AND deleted_slot IS NULL",
		proposalID,
	).Find(&votes); result.Error != nil {
		return nil, result.Error
	}
	return votes, nil
}

// SetGovernanceVote records a vote on a governance proposal.
func (d *MetadataStoreSqlite) SetGovernanceVote(
	vote *models.GovernanceVote,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{Name: "proposal_id"},
			{Name: "voter_type"},
			{Name: "voter_credential"},
		},
		// Note: added_slot is NOT updated on conflict to preserve rollback safety.
		// The original added_slot represents when the vote was first cast.
		// vote_updated_slot tracks when vote changes occur for rollback.
		DoUpdates: clause.AssignmentColumns([]string{
			"vote",
			"anchor_url",
			"anchor_hash",
			"vote_updated_slot",
			"deleted_slot",
		}),
	}
	if result := db.Clauses(onConflict).Create(vote); result.Error != nil {
		return result.Error
	}
	return nil
}

// DeleteGovernanceProposalsAfterSlot removes proposals added after the given slot,
// clears deleted_slot for any that were soft-deleted after that slot, and reverts
// status changes (ratified/enacted) that occurred after the rollback slot.
// This is used during chain rollbacks.
func (d *MetadataStoreSqlite) DeleteGovernanceProposalsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Delete proposals added after the rollback slot
	if result := db.Where("added_slot > ?", slot).Delete(&models.GovernanceProposal{}); result.Error != nil {
		return result.Error
	}

	// Clear deleted_slot for proposals soft-deleted after the rollback slot
	if result := db.Model(&models.GovernanceProposal{}).
		Where("deleted_slot > ?", slot).
		Update("deleted_slot", nil); result.Error != nil {
		return result.Error
	}

	// Revert ratification that occurred after the rollback slot
	if result := db.Model(&models.GovernanceProposal{}).
		Where("ratified_slot > ?", slot).
		Updates(map[string]any{
			"ratified_epoch": nil,
			"ratified_slot":  nil,
		}); result.Error != nil {
		return result.Error
	}

	// Revert enactment that occurred after the rollback slot
	if result := db.Model(&models.GovernanceProposal{}).
		Where("enacted_slot > ?", slot).
		Updates(map[string]any{
			"enacted_epoch": nil,
			"enacted_slot":  nil,
		}); result.Error != nil {
		return result.Error
	}

	return nil
}

// DeleteGovernanceVotesAfterSlot removes votes added after the given slot
// and clears deleted_slot for any that were soft-deleted after that slot.
// This is used during chain rollbacks.
//
// NOTE: Vote changes (when a voter changes their vote) are tracked via vote_updated_slot.
// If a vote was changed after the rollback slot but originally cast before it,
// we cannot restore the previous vote value with the current upsert model.
// For full rollback support of vote changes, votes should be append-only.
// Currently, we delete any vote whose update occurred after the rollback slot
// to avoid having stale vote values persist.
func (d *MetadataStoreSqlite) DeleteGovernanceVotesAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Delete votes added after the rollback slot
	if result := db.Where("added_slot > ?", slot).Delete(&models.GovernanceVote{}); result.Error != nil {
		return result.Error
	}

	// Delete votes that were updated (changed) after the rollback slot
	// Since we can't restore the previous vote value, removing the vote is safer
	// than keeping a potentially incorrect value
	if result := db.Where("vote_updated_slot > ?", slot).Delete(&models.GovernanceVote{}); result.Error != nil {
		return result.Error
	}

	// Clear deleted_slot for votes soft-deleted after the rollback slot
	if result := db.Model(&models.GovernanceVote{}).
		Where("deleted_slot > ?", slot).
		Update("deleted_slot", nil); result.Error != nil {
		return result.Error
	}

	return nil
}
