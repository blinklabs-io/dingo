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

import "github.com/blinklabs-io/node/database"

type Era struct {
	ID         uint `gorm:"primarykey"`
	EraId      uint
	StartEpoch uint
}

func (Era) TableName() string {
	return "era"
}

func EraLatest(db database.Database) (Era, error) {
	var ret Era
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = EraLatestTxn(txn)
		return err
	})
	return ret, err
}

func EraLatestTxn(txn *database.Txn) (Era, error) {
	var ret Era
	result := txn.Metadata().Order("era_id DESC").
		First(&ret)
	if result.Error != nil {
		return ret, result.Error
	}
	return ret, nil
}
