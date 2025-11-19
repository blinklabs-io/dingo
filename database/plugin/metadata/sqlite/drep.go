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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetDrep gets a drep. Returns (nil, nil) if not found.
func (d *MetadataStoreSqlite) GetDrep(
	cred []byte,
	txn *gorm.DB,
) (*models.Drep, error) {
	ret := &models.Drep{}
	if txn != nil {
		if result := txn.First(ret, "drep_credential = ?", cred); result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil, nil
			}
			return nil, result.Error
		}
	} else {
		if result := d.DB().First(ret, "drep_credential = ?", cred); result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil, nil
			}
			return nil, result.Error
		}
	}
	return ret, nil
}

// SetDrep saves a drep
func (d *MetadataStoreSqlite) SetDrep(
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	certificateID uint,
	txn *gorm.DB,
) error {
	tmpItem := models.Drep{
		DrepCredential: cred,
		AddedSlot:      slot,
		AnchorUrl:      url,
		AnchorHash:     hash,
		Active:         active,
		CertificateID:  certificateID,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "drep_credential"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"added_slot":     slot,
			"anchor_url":     url,
			"anchor_hash":    hash,
			"active":         active,
			"certificate_id": certificateID,
		}),
	}
	db := txn
	if db == nil {
		db = d.DB()
	}
	if result := db.Clauses(onConflict).Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}
