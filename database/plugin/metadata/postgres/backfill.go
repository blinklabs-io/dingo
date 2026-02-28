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

// GetBackfillCheckpoint retrieves a backfill checkpoint by phase.
// Returns nil (not error) if no checkpoint exists for the phase.
func (d *MetadataStorePostgres) GetBackfillCheckpoint(
	phase string,
	txn types.Txn,
) (*models.BackfillCheckpoint, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetBackfillCheckpoint: resolve db: %w", err,
		)
	}
	var cp models.BackfillCheckpoint
	result := db.Where("phase = ?", phase).First(&cp)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"get backfill checkpoint: %w",
			result.Error,
		)
	}
	return &cp, nil
}

// SetBackfillCheckpoint creates or updates a backfill checkpoint,
// upserting on the Phase column.
func (d *MetadataStorePostgres) SetBackfillCheckpoint(
	checkpoint *models.BackfillCheckpoint,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"SetBackfillCheckpoint: resolve db: %w", err,
		)
	}
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "phase"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"last_slot",
			"total_slots",
			"updated_at",
			"completed",
		}),
	}).Create(checkpoint)
	if result.Error != nil {
		return fmt.Errorf(
			"set backfill checkpoint: %w",
			result.Error,
		)
	}
	return nil
}
