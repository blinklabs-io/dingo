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
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetAccount gets an account
func (d *MetadataStoreSqlite) GetAccount(
	stakeKey []byte,
	txn *gorm.DB,
) (models.Account, error) {
	ret := models.Account{}
	tmpAccount := models.Account{}
	if txn != nil {
		if result := txn.First(&tmpAccount, "stakeKey = ?", stakeKey); result.Error != nil {
			return ret, result.Error
		}
	} else {
		if result := d.DB().First(&tmpAccount, "stakeKey = ?", stakeKey); result.Error != nil {
			return ret, result.Error
		}
	}
	ret = tmpAccount
	return ret, nil
}

// SetAccount saves an account
func (d *MetadataStoreSqlite) SetAccount(
	stakeKey, pkh, drep []byte,
	slot, deposit uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.Account{
		StakingKey:    stakeKey,
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if txn != nil {
		if result := txn.Clauses(clause.OnConflict{UpdateAll: true}).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Clauses(clause.OnConflict{UpdateAll: true}).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
