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
	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetDrep gets a drep
func (d *MetadataStoreSqlite) GetDrep(
	cred []byte,
	txn *gorm.DB,
) (models.Drep, error) {
	ret := models.Drep{}
	tmpDrep := models.Drep{}
	if txn != nil {
		if result := txn.First(&tmpDrep, "credential = ?", cred); result.Error != nil {
			return ret, result.Error
		}
	} else {
		if result := d.DB().First(&tmpDrep, "credential = ?", cred); result.Error != nil {
			return ret, result.Error
		}
	}
	ret = tmpDrep
	return ret, nil
}

// SetDrep saves a drep
func (d *MetadataStoreSqlite) SetDrep(
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn *gorm.DB,
) error {
	tmpItem := models.Drep{
		Credential: cred,
		AddedSlot:  slot,
		AnchorUrl:  url,
		AnchorHash: hash,
		Active:     active,
	}
	onConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "credential"}},
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
