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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package postgres

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetSyncState retrieves a sync state value by key.
func (d *MetadataStorePostgres) GetSyncState(
	key string,
	txn types.Txn,
) (string, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return "", err
	}
	var state models.SyncState
	result := db.Where("key = ?", key).First(&state)
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
		return err
	}
	state := &models.SyncState{
		Key:   key,
		Value: value,
	}
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
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
		return err
	}
	result := db.Where(
		"key = ?", key,
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
		return err
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
