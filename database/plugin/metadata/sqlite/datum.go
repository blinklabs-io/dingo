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

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetDatum returns a datum by its hash
func (d *MetadataStoreSqlite) GetDatum(
	hash lcommon.Blake2b256,
	txn *gorm.DB,
) (models.Datum, error) {
	var datum models.Datum
	var result *gorm.DB
	if txn != nil {
		result = txn.Where("hash = ?", hash[:]).First(&datum)
	} else {
		result = d.DB().Where("hash = ?", hash[:]).First(&datum)
	}
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return models.Datum{}, nil
		}
		return models.Datum{}, result.Error
	}
	return datum, nil
}

// SetDatum saves a datum into the database, or updates it if it already exists
func (d *MetadataStoreSqlite) SetDatum(
	hash lcommon.Blake2b256,
	rawDatum []byte,
	addedSlot uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.Datum{
		Hash:      hash[:],
		RawDatum:  rawDatum,
		AddedSlot: addedSlot,
	}
	onConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		UpdateAll: true,
	}
	if txn != nil {
		if result := txn.Clauses(onConflict).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Clauses(onConflict).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
