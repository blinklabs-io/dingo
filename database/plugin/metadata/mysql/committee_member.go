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

package mysql

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SetCommitteeMembers upserts committee members imported from a Mithril
// snapshot.
func (d *MetadataStoreMysql) SetCommitteeMembers(
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

// GetCommitteeMembers retrieves all active (non-deleted) snapshot-imported
// committee members.
func (d *MetadataStoreMysql) GetCommitteeMembers(
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

// DeleteCommitteeMembersAfterSlot removes committee members added after
// the given slot and clears deleted_slot for any that were soft-deleted
// after that slot.
func (d *MetadataStoreMysql) DeleteCommitteeMembersAfterSlot(
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
