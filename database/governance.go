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
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.DeleteGovernanceProposalsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete governance proposals after slot %d: %w",
			slot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// DeleteGovernanceVotesAfterSlot removes governance votes added after the
// given slot and clears deleted_slot for any that were soft-deleted after
// that slot. This is used during chain rollbacks.
func (d *Database) DeleteGovernanceVotesAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.DeleteGovernanceVotesAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete governance votes after slot %d: %w",
			slot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
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
	proposal, err := d.metadata.GetGovernanceProposal(
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

// GetActiveGovernanceProposals returns all governance proposals that haven't expired
func (d *Database) GetActiveGovernanceProposals(
	epoch uint64,
	txn *Txn,
) ([]*models.GovernanceProposal, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	proposals, err := d.metadata.GetActiveGovernanceProposals(epoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("failed to get active governance proposals: %w", err)
	}
	return proposals, nil
}

// SetGovernanceProposal creates or updates a governance proposal
func (d *Database) SetGovernanceProposal(
	proposal *models.GovernanceProposal,
	txn *Txn,
) error {
	if proposal == nil {
		return errors.New("proposal cannot be nil")
	}
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer txn.Release()
	}
	if err := d.metadata.SetGovernanceProposal(proposal, txn.Metadata()); err != nil {
		return fmt.Errorf("failed to set governance proposal: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("failed to commit governance proposal: %w", err)
		}
	}
	return nil
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
	votes, err := d.metadata.GetGovernanceVotes(proposalID, txn.Metadata())
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
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer txn.Release()
	}
	if err := d.metadata.SetGovernanceVote(vote, txn.Metadata()); err != nil {
		return fmt.Errorf("failed to set governance vote: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("failed to commit governance vote: %w", err)
		}
	}
	return nil
}
