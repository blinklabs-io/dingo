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

// govProposalUpsertColumns is the list of columns whose values are
// always replaced on upsert. Lifecycle columns (enacted_*, ratified_*,
// expired_*, deleted_slot) and gov_action_cbor are handled separately
// (see govProposalUpsertAssignments) so a re-submission of the base
// proposal record (e.g., a resumable ledgerstate import) does not
// clobber a previously-recorded state transition.
var govProposalUpsertColumns = []string{
	"action_type",
	"proposed_epoch",
	"expires_epoch",
	"parent_tx_hash",
	"parent_action_idx",
	"policy_hash",
	"anchor_url",
	"anchor_hash",
	"deposit",
	"return_address",
}

// govProposalLifecycleColumns are nullable columns that carry state
// transitions written by governance enactment/ratification/expiry/
// deletion. They must only be updated when the incoming value is
// non-NULL so an unrelated upsert cannot revert a transition to NULL.
var govProposalLifecycleColumns = []string{
	"enacted_epoch",
	"enacted_slot",
	"ratified_epoch",
	"ratified_slot",
	"expired_epoch",
	"expired_slot",
	"deleted_slot",
}

// govProposalUpsertAssignments builds the on-conflict Set used by
// SetGovernanceProposal. It uses AssignmentColumns for the plain
// columns and appends a CASE expression for gov_action_cbor and each
// lifecycle column so empty/NULL incoming values preserve the stored
// values rather than clobbering them.
func govProposalUpsertAssignments() clause.Set {
	set := clause.AssignmentColumns(govProposalUpsertColumns)
	set = append(set, clause.Assignment{
		Column: clause.Column{Name: "gov_action_cbor"},
		Value: gorm.Expr(
			"CASE WHEN excluded.gov_action_cbor IS NOT NULL AND " +
				"length(excluded.gov_action_cbor) > 0 " +
				"THEN excluded.gov_action_cbor " +
				"ELSE governance_proposal.gov_action_cbor END",
		),
	})
	for _, col := range govProposalLifecycleColumns {
		set = append(set, clause.Assignment{
			Column: clause.Column{Name: col},
			Value: gorm.Expr(
				"CASE WHEN excluded." + col + " IS NOT NULL " +
					"THEN excluded." + col + " " +
					"ELSE governance_proposal." + col + " END",
			),
		})
	}
	return set
}

// GetGovernanceProposal retrieves a governance proposal by transaction hash and action index.
// Returns nil if the proposal has been soft-deleted.
func (d *MetadataStoreSqlite) GetGovernanceProposal(
	txHash []byte,
	actionIndex uint32,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	var proposal models.GovernanceProposal
	db, err := d.resolveReadDB(txn)
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

// GetActiveGovernanceProposals retrieves all governance proposals that
// are still in the active pool: not expired past the given epoch, not
// enacted, not marked expired, not soft-deleted.
func (d *MetadataStoreSqlite) GetActiveGovernanceProposals(
	epoch uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	var proposals []*models.GovernanceProposal
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"expires_epoch >= ? AND enacted_epoch IS NULL AND expired_epoch IS NULL AND deleted_slot IS NULL",
		epoch,
	).Order(governanceProposalOrder).Find(&proposals); result.Error != nil {
		return nil, result.Error
	}
	return proposals, nil
}

// GetExpiringGovernanceProposals returns proposals whose expires_epoch is
// strictly less than the given epoch and that have not yet been enacted,
// expired, or soft-deleted. Used at epoch boundaries to mark expired
// proposals and trigger deposit returns.
func (d *MetadataStoreSqlite) GetExpiringGovernanceProposals(
	epoch uint64,
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	var proposals []*models.GovernanceProposal
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"expires_epoch < ? AND enacted_epoch IS NULL AND expired_epoch IS NULL AND deleted_slot IS NULL",
		epoch,
	).Order(governanceProposalOrder).Find(&proposals); result.Error != nil {
		return nil, result.Error
	}
	return proposals, nil
}

// governanceProposalOrder is the stable row order used when iterating
// active/expiring proposals at epoch boundaries. Consensus-critical:
// the epoch tick enforces at-most-one ratification per purpose, so
// nodes must see proposals in the same order to agree on which one
// ratifies. Sort by submission epoch, then chain position, then the
// fully-qualified action id to break any remaining ties.
const governanceProposalOrder = "proposed_epoch ASC, added_slot ASC, tx_hash ASC, action_index ASC"

// GetRatifiedGovernanceProposals returns proposals that have been ratified
// but not yet enacted. Used at epoch start to apply enactment effects.
func (d *MetadataStoreSqlite) GetRatifiedGovernanceProposals(
	txn types.Txn,
) ([]*models.GovernanceProposal, error) {
	var proposals []*models.GovernanceProposal
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"ratified_epoch IS NOT NULL AND enacted_epoch IS NULL AND deleted_slot IS NULL",
	).Order(ratifiedGovernanceProposalOrder).Find(&proposals); result.Error != nil {
		return nil, result.Error
	}
	return proposals, nil
}

// ratifiedGovernanceProposalOrder is the row order used when iterating
// ratified-but-not-yet-enacted proposals at epoch start. Consensus-critical:
// ratification time comes first so older ratifications enact first; the
// remainder is the chain-deterministic tie-break shared by
// governanceProposalOrder so all nodes (regardless of insertion order or
// restored backups) agree on enactment order within a single tick.
const ratifiedGovernanceProposalOrder = "ratified_epoch ASC, ratified_slot ASC, proposed_epoch ASC, added_slot ASC, tx_hash ASC, action_index ASC"

// GetLastEnactedGovernanceProposal returns the most recently enacted
// proposal whose action_type is in actionTypes, or nil if none exist.
// The set form lets callers group action types that share a chain
// root per CIP-1694 (e.g., NoConfidence and UpdateCommittee).
func (d *MetadataStoreSqlite) GetLastEnactedGovernanceProposal(
	actionTypes []uint8,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	if len(actionTypes) == 0 {
		return nil, nil
	}
	// GORM serialises []uint8 as a single binary blob, so we expand
	// the slice into a []int before passing it to IN ?.
	params := make([]int, len(actionTypes))
	for i, t := range actionTypes {
		params[i] = int(t)
	}
	var proposal models.GovernanceProposal
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"action_type IN ? AND enacted_epoch IS NOT NULL AND deleted_slot IS NULL",
		params,
	).Order("enacted_epoch DESC, enacted_slot DESC, id DESC").First(&proposal); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &proposal, nil
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
		// Note: added_slot is NOT updated on conflict to preserve
		// rollback safety. The original added_slot represents when
		// the proposal was first submitted. enacted_slot and
		// ratified_slot track when status changes occur for rollback.
		// gov_action_cbor uses a conditional update (see
		// govProposalUpsertAssignments) so an empty incoming payload
		// from a resumable import doesn't clobber previously-stored
		// CBOR, but a later non-empty value can still backfill.
		DoUpdates: govProposalUpsertAssignments(),
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
	db, err := d.resolveReadDB(txn)
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
//
// Transactional contract: when txn is nil this function wraps the rollback in
// its own transaction so partial failures cannot leave the table half-rolled.
// When txn is non-nil the operations execute directly against the caller's
// transaction. On error the caller MUST abort/rollback that outer transaction
// itself; this function will not undo updates that already succeeded within
// the caller-supplied txn.
func (d *MetadataStoreSqlite) DeleteGovernanceProposalsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	rollback := func(tx *gorm.DB) error {
		// Delete proposals added after the rollback slot
		if result := tx.Where("added_slot > ?", slot).Delete(&models.GovernanceProposal{}); result.Error != nil {
			return result.Error
		}
		// Clear deleted_slot for proposals soft-deleted after the rollback slot
		if result := tx.Model(&models.GovernanceProposal{}).
			Where("deleted_slot > ?", slot).
			Update("deleted_slot", nil); result.Error != nil {
			return result.Error
		}
		// Revert ratification that occurred after the rollback slot
		if result := tx.Model(&models.GovernanceProposal{}).
			Where("ratified_slot > ?", slot).
			Updates(map[string]any{
				"ratified_epoch": nil,
				"ratified_slot":  nil,
			}); result.Error != nil {
			return result.Error
		}
		// Revert enactment that occurred after the rollback slot
		if result := tx.Model(&models.GovernanceProposal{}).
			Where("enacted_slot > ?", slot).
			Updates(map[string]any{
				"enacted_epoch": nil,
				"enacted_slot":  nil,
			}); result.Error != nil {
			return result.Error
		}
		// Revert expiry that occurred after the rollback slot
		if result := tx.Model(&models.GovernanceProposal{}).
			Where("expired_slot > ?", slot).
			Updates(map[string]any{
				"expired_epoch": nil,
				"expired_slot":  nil,
			}); result.Error != nil {
			return result.Error
		}
		return nil
	}

	// If caller provided a transaction, use it directly. Otherwise wrap
	// in a single transaction so a partial failure cannot leave the
	// table half-rolled-back.
	if txn != nil {
		return rollback(db)
	}
	return db.Transaction(rollback)
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

	rollback := func(tx *gorm.DB) error {
		// Delete votes added after the rollback slot
		if result := tx.Where("added_slot > ?", slot).Delete(&models.GovernanceVote{}); result.Error != nil {
			return result.Error
		}
		// Delete votes that were updated (changed) after the rollback slot
		// Since we can't restore the previous vote value, removing the vote is safer
		// than keeping a potentially incorrect value
		if result := tx.Where("vote_updated_slot > ?", slot).Delete(&models.GovernanceVote{}); result.Error != nil {
			return result.Error
		}
		// Clear deleted_slot for votes soft-deleted after the rollback slot
		if result := tx.Model(&models.GovernanceVote{}).
			Where("deleted_slot > ?", slot).
			Update("deleted_slot", nil); result.Error != nil {
			return result.Error
		}
		return nil
	}

	if txn != nil {
		return rollback(db)
	}
	return db.Transaction(rollback)
}
