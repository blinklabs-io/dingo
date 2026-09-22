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

package database

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
)

// DeleteGovernanceProposalsAfterSlot removes governance proposals added after
// the given slot and clears deleted_slot for any that were soft-deleted after
// that slot. This is used during chain rollbacks.
func (d *Database) DeleteGovernanceProposalsAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(txn, func(txn *Txn) error {
		if err := d.governanceStore().DeleteGovernanceProposalsAfterSlot(
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to delete governance proposals after slot %d: %w",
				slot,
				err,
			)
		}
		return nil
	})
}

// DeleteGovernanceVotesAfterSlot removes governance votes added after the
// given slot and clears deleted_slot for any that were soft-deleted after
// that slot. This is used during chain rollbacks.
func (d *Database) DeleteGovernanceVotesAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(txn, func(txn *Txn) error {
		if err := d.governanceStore().DeleteGovernanceVotesAfterSlot(
			slot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to delete governance votes after slot %d: %w",
				slot,
				err,
			)
		}
		return nil
	})
}

// GetGovernanceProposal returns a governance proposal by transaction hash and action index
func (d *Database) GetGovernanceProposal(
	txHash []byte,
	actionIndex uint32,
	txn *Txn,
) (*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposal, err := d.governanceStore().GetGovernanceProposal(
		txHash,
		actionIndex,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get governance proposal: %w", err)
	}
	if proposal == nil {
		return nil, models.ErrGovernanceProposalNotFound
	}
	return proposal, nil
}

// GetActiveGovernanceProposals returns all governance proposals that are
// still in the active pool (not expired, not enacted, not soft-deleted).
func (d *Database) GetActiveGovernanceProposals(
	epoch uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetActiveGovernanceProposals(
		epoch,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get active governance proposals: %w",
			err,
		)
	}
	return proposals, nil
}

// GetExpiringGovernanceProposals returns proposals whose expires_epoch is
// strictly less than the given epoch and that have not yet been enacted,
// expired, or soft-deleted.
func (d *Database) GetExpiringGovernanceProposals(
	epoch uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetExpiringGovernanceProposals(
		epoch, txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get expiring governance proposals: %w", err,
		)
	}
	return proposals, nil
}

// GetExpiredGovernanceProposalsAt returns proposals expired at the exact
// epoch-boundary slot. Used by epoch replay to restore deposit-return effects.
func (d *Database) GetExpiredGovernanceProposalsAt(
	epoch uint64,
	slot uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetExpiredGovernanceProposalsAt(
		epoch, slot, txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get boundary-expired governance proposals: %w",
			err,
		)
	}
	return proposals, nil
}

// GetExpiredAwaitingDropGovernanceProposals returns proposals whose
// expired_epoch is strictly below the given epoch and whose deposit has not
// yet been returned. Used at epoch start, before marking any new proposals
// expired, to return the deposit and finalize proposals expired as of a prior
// boundary (dingo#4411).
func (d *Database) GetExpiredAwaitingDropGovernanceProposals(
	epoch uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().
		GetExpiredAwaitingDropGovernanceProposals(epoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get expired-awaiting-drop governance proposals: %w",
			err,
		)
	}
	return proposals, nil
}

// GetDroppedGovernanceProposalsAt returns proposals dropped (deposit
// returned) at the exact epoch-boundary slot. Used by epoch replay to
// restore deposit-return effects.
func (d *Database) GetDroppedGovernanceProposalsAt(
	epoch uint64,
	slot uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetDroppedGovernanceProposalsAt(
		epoch, slot, txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get boundary-dropped governance proposals: %w",
			err,
		)
	}
	return proposals, nil
}

// GetRatifiedGovernanceProposals returns proposals ratified but not yet
// enacted, ordered by (ratified_epoch, ratified_slot, id). Used at epoch
// start for enactment.
func (d *Database) GetRatifiedGovernanceProposals(
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetRatifiedGovernanceProposals(
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get ratified governance proposals: %w", err,
		)
	}
	return proposals, nil
}

// GetEnactedGovernanceProposalsAt returns proposals enacted at the exact
// epoch-boundary slot. Used by epoch replay to restore enactment effects.
func (d *Database) GetEnactedGovernanceProposalsAt(
	epoch uint64,
	slot uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetEnactedGovernanceProposalsAt(
		epoch, slot, txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get boundary-enacted governance proposals: %w",
			err,
		)
	}
	return proposals, nil
}

// GetLastEnactedGovernanceProposal returns the most recently enacted
// proposal whose action_type is in actionTypes, or nil if none exist.
// Callers group per-purpose action types (CIP-1694 chain roots) in
// the slice; the single-type case passes a one-element slice.
func (d *Database) GetLastEnactedGovernanceProposal(
	actionTypes []uint8,
	txn *Txn,
) (*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposal, err := d.governanceStore().GetLastEnactedGovernanceProposal(
		actionTypes, txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get last enacted governance proposal: %w", err,
		)
	}
	return proposal, nil
}

// SetGovernanceProposal creates or updates a governance proposal
func (d *Database) SetGovernanceProposal(
	proposal *models.GovernanceProposal,
	txn *Txn,
) error {
	if proposal == nil {
		return errors.New("proposal cannot be nil")
	}
	return d.withMetadataWriteTxn(txn, func(txn *Txn) error {
		if err := d.governanceStore().SetGovernanceProposal(
			proposal,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("failed to set governance proposal: %w", err)
		}
		return nil
	})
}

// ClearGovernanceProposalRatification moves a proposal back to the active,
// pending state at transitionSlot. Governance epoch processing uses it when a
// legacy ratified row fails a deterministic enactment precondition.
func (d *Database) ClearGovernanceProposalRatification(
	txHash []byte,
	actionIndex uint32,
	transitionSlot uint64,
	txn *Txn,
) error {
	return d.withMetadataWriteTxn(txn, func(txn *Txn) error {
		if err := d.governanceStore().ClearGovernanceProposalRatification(
			txHash,
			actionIndex,
			transitionSlot,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"failed to clear governance proposal ratification: %w",
				err,
			)
		}
		return nil
	})
}

// GetChildGovernanceProposals returns all active proposals whose parent is
// the given proposal (parentTxHash + parentActionIdx). Only proposals not
// yet enacted, expired, or soft-deleted are returned. Used during epoch
// boundary orphan sweeps.
func (d *Database) GetChildGovernanceProposals(
	parentTxHash []byte,
	parentActionIdx uint32,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.governanceStore().GetChildGovernanceProposals(
		parentTxHash, parentActionIdx, txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get child governance proposals: %w", err,
		)
	}
	return proposals, nil
}

// GetGovernanceVotes returns all votes for a governance proposal
func (d *Database) GetGovernanceVotes(
	proposalID uint,
	txn *Txn,
) ([]*models.GovernanceVote, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	votes, err := d.governanceStore().GetGovernanceVotes(
		proposalID,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get governance votes: %w", err)
	}
	return votes, nil
}

// SetGovernanceVote records a vote on a governance proposal
func (d *Database) SetGovernanceVote(
	vote *models.GovernanceVote,
	txn *Txn,
) error {
	if vote == nil {
		return errors.New("vote cannot be nil")
	}
	return d.withMetadataWriteTxn(txn, func(txn *Txn) error {
		if err := d.governanceStore().SetGovernanceVote(
			vote,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("failed to set governance vote: %w", err)
		}
		return nil
	})
}
