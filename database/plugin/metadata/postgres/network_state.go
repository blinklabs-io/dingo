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

// SetNetworkState stores the treasury and reserves balances.
func (d *MetadataStorePostgres) SetNetworkState(
	treasury, reserves uint64,
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	state := &models.NetworkState{
		Treasury: types.Uint64(treasury),
		Reserves: types.Uint64(reserves),
		Slot:     slot,
	}
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "slot"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"treasury", "reserves"},
		),
	}).Create(state)
	if result.Error != nil {
		return fmt.Errorf(
			"set network state: %w",
			result.Error,
		)
	}
	return nil
}

// GetNetworkState retrieves the most recent network state.
func (d *MetadataStorePostgres) GetNetworkState(
	txn types.Txn,
) (*models.NetworkState, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var state models.NetworkState
	result := db.Order("slot DESC").First(&state)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"get network state: %w",
			result.Error,
		)
	}
	return &state, nil
}
