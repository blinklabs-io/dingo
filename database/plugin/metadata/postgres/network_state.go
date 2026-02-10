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
		return fmt.Errorf("set network state: %w", err)
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

// DeleteNetworkStateAfterSlot removes network state records added
// after the given slot.
func (d *MetadataStorePostgres) DeleteNetworkStateAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"delete network state after slot: %w", err,
		)
	}
	result := db.Where("slot > ?", slot).
		Delete(&models.NetworkState{})
	if result.Error != nil {
		return fmt.Errorf(
			"delete network state after slot %d: %w",
			slot,
			result.Error,
		)
	}
	return nil
}

// GetNetworkState retrieves the most recent network state.
func (d *MetadataStorePostgres) GetNetworkState(
	txn types.Txn,
) (*models.NetworkState, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf("get network state: %w", err)
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

// GetSyncState retrieves a sync state value by key.
func (d *MetadataStorePostgres) GetSyncState(
	key string,
	txn types.Txn,
) (string, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return "", fmt.Errorf(
			"resolve db for get sync state: %w", err,
		)
	}
	var state models.SyncState
	result := db.Where("sync_key = ?", key).First(&state)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", fmt.Errorf(
			"get sync state: %w", result.Error,
		)
	}
	return state.Value, nil
}

// SetSyncState stores or updates a sync state value.
func (d *MetadataStorePostgres) SetSyncState(
	key, value string,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"resolve db for set sync state: %w", err,
		)
	}
	state := &models.SyncState{
		Key:   key,
		Value: value,
	}
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "sync_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(state)
	if result.Error != nil {
		return fmt.Errorf(
			"set sync state: %w", result.Error,
		)
	}
	return nil
}

// DeleteSyncState removes a sync state key.
func (d *MetadataStorePostgres) DeleteSyncState(
	key string,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"resolve db for delete sync state: %w", err,
		)
	}
	result := db.Where(
		"sync_key = ?", key,
	).Delete(&models.SyncState{})
	if result.Error != nil {
		return fmt.Errorf(
			"delete sync state: %w", result.Error,
		)
	}
	return nil
}

// ClearSyncState removes all sync state entries.
func (d *MetadataStorePostgres) ClearSyncState(
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"resolve db for clear sync state: %w", err,
		)
	}
	result := db.Session(
		&gorm.Session{AllowGlobalUpdate: true},
	).Delete(&models.SyncState{})
	if result.Error != nil {
		return fmt.Errorf(
			"clear sync state: %w", result.Error,
		)
	}
	return nil
}
