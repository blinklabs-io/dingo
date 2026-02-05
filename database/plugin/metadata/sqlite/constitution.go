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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetConstitution retrieves the current constitution.
// Returns nil if the constitution has been soft-deleted.
func (d *MetadataStoreSqlite) GetConstitution(
	txn types.Txn,
) (*models.Constitution, error) {
	var constitution models.Constitution
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf("resolveDB failed in GetConstitution: %w", err)
	}
	// Get the most recent non-deleted constitution by added_slot
	if result := db.Where("deleted_slot IS NULL").Order("added_slot DESC").First(&constitution); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("DB querying latest Constitution failed: %w", result.Error)
	}
	return &constitution, nil
}

// SetConstitution sets the constitution.
func (d *MetadataStoreSqlite) SetConstitution(
	constitution *models.Constitution,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("resolveDB failed in SetConstitution: %w", err)
	}
	// Use upsert: if a constitution with the same added_slot exists, update it
	// Otherwise insert a new record
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "added_slot"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"anchor_url",
			"anchor_hash",
			"policy_hash",
			"deleted_slot",
		}),
	}
	if result := db.Clauses(onConflict).Create(constitution); result.Error != nil {
		return fmt.Errorf("DB upserting Constitution failed: %w", result.Error)
	}
	return nil
}

// DeleteConstitutionsAfterSlot removes constitutions added after the given slot
// and clears deleted_slot for any that were soft-deleted after that slot.
// This is used during chain rollbacks.
func (d *MetadataStoreSqlite) DeleteConstitutionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("resolveDB failed in DeleteConstitutionsAfterSlot: %w", err)
	}

	// Delete constitutions added after the rollback slot
	if result := db.Where("added_slot > ?", slot).Delete(&models.Constitution{}); result.Error != nil {
		return fmt.Errorf("DB deleting added after slot Constitution failed: %w", result.Error)
	}

	// Clear deleted_slot for constitutions soft-deleted after the rollback slot
	if result := db.Model(&models.Constitution{}).
		Where("deleted_slot > ?", slot).
		Update("deleted_slot", nil); result.Error != nil {
		return fmt.Errorf("DB clearing deleted_slot after slot Constitution failed: %w", result.Error)
	}

	return nil
}
