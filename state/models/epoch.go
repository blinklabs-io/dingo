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

import "github.com/blinklabs-io/dingo/database"

type Epoch struct {
	ID uint `gorm:"primarykey"`
	// NOTE: we would normally use this as the primary key, but GORM doesn't
	// like a primary key value of 0
	EpochId       uint `gorm:"uniqueIndex"`
	EraId         uint
	StartSlot     uint64
	SlotLength    uint
	LengthInSlots uint
	Nonce         []byte
}

func (Epoch) TableName() string {
	return "epoch"
}

func EpochLatest(db database.Database) (Epoch, error) {
	var ret Epoch
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = EpochLatestTxn(txn)
		return err
	})
	return ret, err
}

func EpochLatestTxn(txn *database.Txn) (Epoch, error) {
	var ret Epoch
	result := txn.Metadata().Order("epoch_id DESC").
		First(&ret)
	if result.Error != nil {
		return ret, result.Error
	}
	return ret, nil
}
