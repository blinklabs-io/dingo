// Copyright 2025 Blink Labs Software
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
	"gorm.io/gorm"
)

// GetAccount gets an account
func (d *MetadataStoreSqlite) GetAccount(
	stakeKey []byte,
	includeInactive bool,
	txn *gorm.DB,
) (*models.Account, error) {
	ret := &models.Account{}
	if txn == nil {
		txn = d.DB()
	}

	query := txn.Where("staking_key = ?", stakeKey)
	if !includeInactive {
		query = query.Where("active = ?", true)
	}

	result := query.First(ret)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// SetAccount saves an account
func (d *MetadataStoreSqlite) SetAccount(
	stakeKey, pkh, drep []byte,
	slot uint64,
	active bool,
	certificateID uint,
	txn *gorm.DB,
) error {
	db := d.DB()
	if txn != nil {
		db = txn
	}

	// Find or create account for this staking key (accounts are unique per staking key)
	account := &models.Account{}
	result := db.FirstOrCreate(account, models.Account{StakingKey: stakeKey})
	if result.Error != nil {
		return fmt.Errorf("failed to find or create account: %w", result.Error)
	}

	// Update account fields
	// Note: added_slot represents the last time this account was updated by a certificate,
	// not the initial creation time
	updates := map[string]interface{}{
		"pool":           pkh,
		"drep":           drep,
		"added_slot":     slot,
		"active":         active,
		"certificate_id": certificateID,
	}
	if err := db.Model(account).Updates(updates).Error; err != nil {
		return fmt.Errorf("failed to update account: %w", err)
	}

	return nil
}
