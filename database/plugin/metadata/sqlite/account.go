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

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetAccount gets an account
func (d *MetadataStoreSqlite) GetAccount(
	stakeKey []byte,
	txn *gorm.DB,
) (*models.Account, error) {
	ret := &models.Account{}
	if txn == nil {
		txn = d.DB()
	}
	result := txn.Where("staking_key = ? AND active = ?", stakeKey, true).First(ret)
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
	txn *gorm.DB,
) error {
	tmpItem := models.Account{
		StakingKey: stakeKey,
		AddedSlot:  slot,
		Pool:       pkh,
		Drep:       drep,
		Active:     active,
	}
	onConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "staking_key"}},
		UpdateAll: true,
	}
	if txn != nil {
		if result := txn.Clauses(onConflict).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Clauses(onConflict).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
