// Copyright 2024 Blink Labs Software
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

package models

import (
	"errors"

	"github.com/blinklabs-io/dingo/database"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	tipEntryId = 1
)

type Tip struct {
	ID          uint `gorm:"primarykey"`
	Slot        uint64
	Hash        []byte
	BlockNumber uint64
}

func (Tip) TableName() string {
	return "tip"
}

func TipGet(db database.Database) (ochainsync.Tip, error) {
	var ret ochainsync.Tip
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = TipGetTxn(txn)
		return err
	})
	return ret, err
}

func TipGetTxn(txn *database.Txn) (ochainsync.Tip, error) {
	var ret ochainsync.Tip
	var tmpTip Tip
	result := txn.Metadata().Where("id = ?", tipEntryId).First(&tmpTip)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return ret, nil
		}
		return ret, result.Error
	}
	ret = ochainsync.Tip{
		Point: ocommon.Point{
			Slot: tmpTip.Slot,
			Hash: tmpTip.Hash,
		},
		BlockNumber: tmpTip.BlockNumber,
	}
	return ret, nil
}

func TipUpdateTxn(txn *database.Txn, tip ochainsync.Tip) error {
	tmpTip := Tip{
		ID:          tipEntryId,
		Slot:        tip.Point.Slot,
		Hash:        tip.Point.Hash,
		BlockNumber: tip.BlockNumber,
	}
	result := txn.Metadata().Clauses(
		clause.OnConflict{
			UpdateAll: true,
		},
	).Create(&tmpTip)
	if result.Error != nil {
		return result.Error
	}
	return nil
}
