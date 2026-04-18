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

package postgres

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SetCommitteeMembers upserts committee members imported from a Mithril
// snapshot.
func (d *MetadataStorePostgres) SetCommitteeMembers(
	members []*models.CommitteeMember,
	txn types.Txn,
) error {
	if len(members) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("SetCommitteeMembers: resolve db: %w", err)
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "cold_cred_hash"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"expires_epoch",
			"added_slot",
			"deleted_slot",
		}),
	}
	if result := db.Clauses(onConflict).Create(members); result.Error != nil {
		return fmt.Errorf(
			"SetCommitteeMembers: upsert failed: %w",
			result.Error,
		)
	}
	return nil
}

// SetCommitteeQuorum stores the quorum threshold enacted with a committee.
func (d *MetadataStorePostgres) SetCommitteeQuorum(
	quorum *types.Rat,
	slot uint64,
	txn types.Txn,
) error {
	if quorum == nil || quorum.Rat == nil {
		return errors.New("committee quorum cannot be nil")
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("SetCommitteeQuorum: resolve db: %w", err)
	}
	state := &models.CommitteeQuorum{
		Quorum:    quorum,
		AddedSlot: slot,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "added_slot"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"quorum",
		}),
	}
	if result := db.Clauses(onConflict).Create(state); result.Error != nil {
		return fmt.Errorf(
			"SetCommitteeQuorum: upsert failed: %w",
			result.Error,
		)
	}
	return nil
}

// GetCommitteeQuorum retrieves the latest enacted committee quorum.
func (d *MetadataStorePostgres) GetCommitteeQuorum(
	txn types.Txn,
) (*types.Rat, error) {
	var state models.CommitteeQuorum
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetCommitteeQuorum: resolve db: %w", err,
		)
	}
	if result := db.Order("added_slot DESC, id DESC").
		First(&state); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"GetCommitteeQuorum: query failed: %w",
			result.Error,
		)
	}
	return state.Quorum, nil
}

// GetCommitteeMembers retrieves all active (non-deleted) snapshot-imported
// committee members.
func (d *MetadataStorePostgres) GetCommitteeMembers(
	txn types.Txn,
) ([]*models.CommitteeMember, error) {
	var members []*models.CommitteeMember
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetCommitteeMembers: resolve db: %w", err,
		)
	}
	if result := db.Where(
		"deleted_slot IS NULL",
	).Find(&members); result.Error != nil {
		return nil, fmt.Errorf(
			"GetCommitteeMembers: query failed: %w",
			result.Error,
		)
	}
	return members, nil
}

// GetCommitteeMembersIncludeDeleted returns every committee member
// row including soft-deleted ones. See sqlite variant for rationale.
func (d *MetadataStorePostgres) GetCommitteeMembersIncludeDeleted(
	txn types.Txn,
) ([]*models.CommitteeMember, error) {
	var members []*models.CommitteeMember
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetCommitteeMembersIncludeDeleted: resolve db: %w", err,
		)
	}
	if result := db.Find(&members); result.Error != nil {
		return nil, fmt.Errorf(
			"GetCommitteeMembersIncludeDeleted: query failed: %w",
			result.Error,
		)
	}
	return members, nil
}

// SoftDeleteCommitteeMembers marks the given cold credential hashes as
// removed by setting deleted_slot. Used by governance enactment to remove
// members listed in an UpdateCommittee action.
func (d *MetadataStorePostgres) SoftDeleteCommitteeMembers(
	coldCredHashes [][]byte,
	slot uint64,
	txn types.Txn,
) error {
	if len(coldCredHashes) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"SoftDeleteCommitteeMembers: resolve db: %w", err,
		)
	}
	if result := db.Model(&models.CommitteeMember{}).
		Where("cold_cred_hash IN ? AND deleted_slot IS NULL", coldCredHashes).
		Update("deleted_slot", slot); result.Error != nil {
		return fmt.Errorf(
			"SoftDeleteCommitteeMembers: update failed: %w",
			result.Error,
		)
	}
	return nil
}

// SoftDeleteAllCommitteeMembers marks all active committee members as
// removed. Used by governance enactment for NoConfidence actions.
func (d *MetadataStorePostgres) SoftDeleteAllCommitteeMembers(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"SoftDeleteAllCommitteeMembers: resolve db: %w", err,
		)
	}
	if result := db.Model(&models.CommitteeMember{}).
		Where("deleted_slot IS NULL").
		Update("deleted_slot", slot); result.Error != nil {
		return fmt.Errorf(
			"SoftDeleteAllCommitteeMembers: update failed: %w",
			result.Error,
		)
	}
	return nil
}

// DeleteCommitteeMembersAfterSlot removes committee state added after
// the given slot and clears deleted_slot for any members soft-deleted
// after that slot.
func (d *MetadataStorePostgres) DeleteCommitteeMembersAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"DeleteCommitteeMembersAfterSlot: resolve db: %w",
			err,
		)
	}

	rollback := func(tx *gorm.DB) error {
		if result := tx.Where(
			"added_slot > ?", slot,
		).Delete(&models.CommitteeMember{}); result.Error != nil {
			return fmt.Errorf(
				"DeleteCommitteeMembersAfterSlot: delete failed: %w",
				result.Error,
			)
		}

		if result := tx.Where(
			"added_slot > ?", slot,
		).Delete(&models.CommitteeQuorum{}); result.Error != nil {
			return fmt.Errorf(
				"DeleteCommitteeMembersAfterSlot: delete quorum failed: %w",
				result.Error,
			)
		}

		if result := tx.Model(&models.CommitteeMember{}).
			Where("deleted_slot > ?", slot).
			Update("deleted_slot", nil); result.Error != nil {
			return fmt.Errorf(
				"DeleteCommitteeMembersAfterSlot: clear deleted_slot failed: %w",
				result.Error,
			)
		}
		return nil
	}

	if txn != nil {
		return rollback(db)
	}
	return db.Transaction(rollback)
}
