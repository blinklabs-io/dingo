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

package postgres

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetAccount gets an account
func (d *MetadataStorePostgres) GetAccount(
	stakeKey []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Account, error) {
	ret := &models.Account{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	query := db
	if !includeInactive {
		query = query.Where("active = ?", true)
	}
	result := query.First(ret, "staking_key = ?", stakeKey)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// SetAccount saves an account
func (d *MetadataStorePostgres) SetAccount(
	stakeKey, pkh, drep []byte,
	slot uint64,
	active bool,
	txn types.Txn,
) error {
	tmpItem := models.Account{
		StakingKey: stakeKey,
		AddedSlot:  slot,
		Pool:       pkh,
		Drep:       drep,
		Active:     active,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "staking_key"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"added_slot", "pool", "drep", "active"},
		),
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Clauses(onConflict).Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}
