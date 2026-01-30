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

package mysql

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	tipEntryId = 1
)

// GetTip returns the current metadata Tip as ocommon.Tip
func (d *MetadataStoreMysql) GetTip(
	txn types.Txn,
) (ocommon.Tip, error) {
	ret := ocommon.Tip{}
	tmpTip := models.Tip{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return ret, err
	}
	result := db.Where("id = ?", tipEntryId).First(&tmpTip)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return ret, nil
		}
		return ret, result.Error
	}
	ret.Point = ocommon.Point{
		Slot: tmpTip.Slot,
		Hash: tmpTip.Hash,
	}
	ret.BlockNumber = tmpTip.BlockNumber
	return ret, nil
}

// SetTip saves a tip
func (d *MetadataStoreMysql) SetTip(
	tip ochainsync.Tip,
	txn types.Txn,
) error {
	tmpItem := models.Tip{
		ID:          tipEntryId,
		Slot:        tip.Point.Slot,
		Hash:        tip.Point.Hash,
		BlockNumber: tip.BlockNumber,
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Clauses(clause.OnConflict{UpdateAll: true}).Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}
