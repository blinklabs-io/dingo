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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetDatum returns a datum by its hash
func (d *MetadataStoreSqlite) GetDatum(
	hash lcommon.Blake2b256,
	txn types.Txn,
) (*models.Datum, error) {
	ret := &models.Datum{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.First(ret, "hash = ?", hash[:])
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// SetDatum saves a datum
func (d *MetadataStoreSqlite) SetDatum(
	hash lcommon.Blake2b256,
	rawDatum []byte,
	addedSlot uint64,
	txn types.Txn,
) error {
	tmpItem := models.Datum{
		Hash:      hash[:],
		RawDatum:  rawDatum,
		AddedSlot: addedSlot,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "hash"}},
		DoUpdates: clause.Assignments(map[string]any{
			"raw_datum": rawDatum,
		}),
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Clauses(onConflict).Create(&tmpItem)
	if result.Error != nil {
		return result.Error
	}
	return nil
}
