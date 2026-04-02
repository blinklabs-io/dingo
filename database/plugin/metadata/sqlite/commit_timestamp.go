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

	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	commitTimestampRowId = 1
	nodeSettingsRowId    = 1
)

// CommitTimestamp represents the sqlite table used to track the current commit timestamp
type CommitTimestamp struct {
	ID        uint `gorm:"primarykey"`
	Timestamp int64
}

func (CommitTimestamp) TableName() string {
	return "commit_timestamp"
}

func (d *MetadataStoreSqlite) GetCommitTimestamp() (int64, error) {
	// Get value from sqlite
	var tmpCommitTimestamp CommitTimestamp
	result := d.ReadDB().First(&tmpCommitTimestamp)
	if result.Error != nil {
		// It's not an error if there's no records found
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, result.Error
	}
	return tmpCommitTimestamp.Timestamp, nil
}

func (d *MetadataStoreSqlite) SetCommitTimestamp(
	timestamp int64,
	txn types.Txn,
) error {
	tmpCommitTimestamp := CommitTimestamp{
		ID:        commitTimestampRowId,
		Timestamp: timestamp,
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"timestamp"}),
	}).Create(&tmpCommitTimestamp)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// NodeSettings persists immutable node configuration so that storage mode
// and network cannot be changed after the database has been initialised.
type NodeSettings struct {
	ID          uint   `gorm:"primarykey"`
	StorageMode string `gorm:"size:16;not null"`
	Network     string `gorm:"size:64;not null"`
}

func (NodeSettings) TableName() string {
	return "node_settings"
}

// GetNodeSettings returns the persisted node settings, or nil if the
// database has never been initialised.
func (d *MetadataStoreSqlite) GetNodeSettings() (*types.NodeSettings, error) {
	var row NodeSettings
	result := d.ReadDB().
		Where("id = ?", nodeSettingsRowId).
		First(&row)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetNodeSettings failed: %w", result.Error)
	}
	return &types.NodeSettings{
		StorageMode: row.StorageMode,
		Network:     row.Network,
	}, nil
}

// SetNodeSettings persists the immutable node settings, inserting the
// singleton row if it does not already exist.
func (d *MetadataStoreSqlite) SetNodeSettings(
	s *types.NodeSettings,
) error {
	row := NodeSettings{
		ID:          nodeSettingsRowId,
		StorageMode: s.StorageMode,
		Network:     s.Network,
	}
	result := d.DB().Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(&row)
	if result.Error != nil {
		return fmt.Errorf("SetNodeSettings failed: %w", result.Error)
	}
	if result.RowsAffected > 0 || s.Network == "" {
		return nil
	}
	result = d.DB().
		Model(&NodeSettings{}).
		Where(
			"id = ? AND storage_mode = ? AND network = ?",
			nodeSettingsRowId,
			s.StorageMode,
			"",
		).
		Update("network", s.Network)
	if result.Error != nil {
		return fmt.Errorf("SetNodeSettings failed: %w", result.Error)
	}
	return nil
}
