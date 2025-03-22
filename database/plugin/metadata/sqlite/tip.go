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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	tipEntryId = 1
)

// GetTip returns the current metadata Tip as ocommon.Tip
func (d *MetadataStoreSqlite) GetTip(
	txn *gorm.DB,
) (ocommon.Tip, error) {
	ret := ocommon.Tip{}
	tmpTip := models.Tip{}
	if txn != nil {
		result := txn.Where("id = ?", tipEntryId).First(&tmpTip)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return ret, nil
			}
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("id = ?", tipEntryId).First(&tmpTip)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return ret, nil
			}
			return ret, result.Error
		}
	}
	ret.Point = ocommon.Point{
		Slot: tmpTip.Slot,
		Hash: tmpTip.Hash,
	}
	ret.BlockNumber = tmpTip.BlockNumber
	return ret, nil
}

// SetTip saves a tip
func (d *MetadataStoreSqlite) SetTip(
	tip ochainsync.Tip,
	txn *gorm.DB,
) error {
	tmpItem := models.Tip{
		ID:          tipEntryId,
		Slot:        tip.Point.Slot,
		Hash:        tip.Point.Hash,
		BlockNumber: tip.BlockNumber,
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
