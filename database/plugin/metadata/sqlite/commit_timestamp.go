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

	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	commitTimestampRowId = 1
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
	result := d.DB().First(&tmpCommitTimestamp)
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
