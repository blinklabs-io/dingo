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
	"math/big"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// GetCommitteeMember returns a committee member by cold key
func (d *Database) GetCommitteeMember(
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn *Txn,
) (*models.AuthCommitteeHot, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	ret, err := d.governanceStore().GetCommitteeMember(
		coldCredentialTag,
		coldKey,
		termStartSlot,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, models.ErrCommitteeMemberNotFound
	}
	return ret, nil
}

// GetActiveCommitteeMembers returns all active committee members
func (d *Database) GetActiveCommitteeMembers(
	txn *Txn,
) ([]*models.AuthCommitteeHot, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.governanceStore().GetActiveCommitteeMembers(txn.Metadata())
}

// IsCommitteeMemberResigned checks if a committee member has resigned
func (d *Database) IsCommitteeMemberResigned(
	coldCredentialTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn *Txn,
) (bool, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.governanceStore().IsCommitteeMemberResigned(
		coldCredentialTag,
		coldKey,
		termStartSlot,
		txn.Metadata(),
	)
}

// GetResignedCommitteeMembers returns cold credentials with a resignation
// record in each credential's selected membership term.
func (d *Database) GetResignedCommitteeMembers(
	coldCredentials []models.CommitteeCredential,
	txn *Txn,
) (map[string]bool, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.governanceStore().GetResignedCommitteeMembers(
		coldCredentials,
		txn.Metadata(),
	)
}

// GetCommitteeActiveCount returns the number of active (non-resigned)
// committee members.
func (d *Database) GetCommitteeActiveCount(
	txn *Txn,
) (int, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	return d.governanceStore().GetCommitteeActiveCount(txn.Metadata())
}

// SetCommitteeMembers upserts governance-enacted committee members. Used
// by UpdateCommittee action enactment and snapshot import.
func (d *Database) SetCommitteeMembers(
	members []*models.CommitteeMember,
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
	if err := d.governanceStore().SetCommitteeMembers(
		members, txn.Metadata(),
	); err != nil {
		return fmt.Errorf("failed to set committee members: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"failed to commit committee members: %w", err,
			)
		}
		owned = false
	}
	return nil
}

// SetCommitteeQuorum stores the quorum threshold enacted with a committee.
func (d *Database) SetCommitteeQuorum(
	quorum *big.Rat,
	slot uint64,
	txn *Txn,
) error {
	if quorum == nil {
		return errors.New("committee quorum cannot be nil")
	}
	if quorum.Sign() <= 0 {
		return errors.New("committee quorum must be positive")
	}
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
	stored := &types.Rat{Rat: new(big.Rat).Set(quorum)}
	if err := d.governanceStore().SetCommitteeQuorum(
		stored, slot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf("failed to set committee quorum: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"failed to commit committee quorum: %w", err,
			)
		}
		owned = false
	}
	return nil
}

// ClearCommitteeQuorum records at the given slot that no quorum is
// in effect (e.g. after a NoConfidence action is enacted). A later
// GetCommitteeQuorum will return nil until a subsequent
// SetCommitteeQuorum writes a new positive value.
func (d *Database) ClearCommitteeQuorum(
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
	if err := d.governanceStore().ClearCommitteeQuorum(
		slot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf("failed to clear committee quorum: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"failed to commit cleared committee quorum: %w", err,
			)
		}
		owned = false
	}
	return nil
}

// GetCommitteeQuorum returns the latest enacted committee quorum.
func (d *Database) GetCommitteeQuorum(
	txn *Txn,
) (*big.Rat, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	quorum, err := d.governanceStore().GetCommitteeQuorum(txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("failed to get committee quorum: %w", err)
	}
	if quorum == nil || quorum.Rat == nil {
		return nil, nil
	}
	return new(big.Rat).Set(quorum.Rat), nil
}

// GetCommitteeMembers returns all active (non-deleted) governance-enacted
// committee members.
func (d *Database) GetCommitteeMembers(
	txn *Txn,
) ([]*models.CommitteeMember, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	members, err := d.governanceStore().GetCommitteeMembers(txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf("failed to get committee members: %w", err)
	}
	return members, nil
}

// GetCommitteeMembersIncludeDeleted returns all committee members
// including soft-deleted rows. Used to detect whether a committee
// was ever seated — after a NoConfidence action, GetCommitteeMembers
// returns no rows, but the committee was seated previously.
func (d *Database) GetCommitteeMembersIncludeDeleted(
	txn *Txn,
) ([]*models.CommitteeMember, error) {
	if txn == nil {
		txn = d.MetadataTxn(false)
		defer txn.Release()
	}
	members, err := d.governanceStore().GetCommitteeMembersIncludeDeleted(
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get committee members (include deleted): %w", err,
		)
	}
	return members, nil
}

// DeleteCommitteeMembersAfterSlot removes committee state added after
// the given slot and clears deleted_slot for any members soft-deleted
// after that slot. Used during chain rollbacks.
func (d *Database) DeleteCommitteeMembersAfterSlot(
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
	if err := d.governanceStore().DeleteCommitteeMembersAfterSlot(
		slot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete committee members after slot %d: %w",
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

// SoftDeleteCommitteeMembers marks the given cold credential hashes as
// removed. Used by UpdateCommittee action enactment to remove members.
func (d *Database) SoftDeleteCommitteeMembers(
	coldCredentials []models.CommitteeCredential,
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
	if err := d.governanceStore().SoftDeleteCommitteeMembers(
		coldCredentials, slot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to soft-delete committee members: %w", err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"failed to commit soft-delete committee members: %w", err,
			)
		}
		owned = false
	}
	return nil
}

// SoftDeleteAllCommitteeMembers marks all active committee members as
// removed. Used by NoConfidence action enactment.
func (d *Database) SoftDeleteAllCommitteeMembers(
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
	if err := d.governanceStore().SoftDeleteAllCommitteeMembers(
		slot, txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to soft-delete all committee members: %w", err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"failed to commit soft-delete all committee members: %w",
				err,
			)
		}
		owned = false
	}
	return nil
}
