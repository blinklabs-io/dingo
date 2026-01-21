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
	"github.com/blinklabs-io/dingo/database/types"
)

// GetPParams returns a list of protocol parameters for a given epoch. If there are no pparams
// for the specified epoch, it will return the most recent pparams before the specified epoch
func (d *MetadataStoreSqlite) GetPParams(
	epoch uint64,
	txn types.Txn,
) ([]models.PParams, error) {
	ret := []models.PParams{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return ret, err
	}
	result := db.Where("epoch <= ?", epoch).
		Order("epoch DESC, id DESC").
		Limit(1).
		Find(&ret)
	if result.Error != nil {
		return ret, result.Error
	}
	return ret, nil
}

// GetPParamUpdates returns a list of protocol parameter updates for a given epoch
func (d *MetadataStoreSqlite) GetPParamUpdates(
	epoch uint64,
	txn types.Txn,
) ([]models.PParamUpdate, error) {
	ret := []models.PParamUpdate{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return ret, err
	}
	result := db.Where("epoch = ?", epoch).Order("id DESC").Find(&ret)
	if result.Error != nil {
		return ret, result.Error
	}
	return ret, nil
}

// SetPParams saves protocol parameters
func (d *MetadataStoreSqlite) SetPParams(
	params []byte,
	slot, epoch uint64,
	eraId uint,
	txn types.Txn,
) error {
	tmpItem := models.PParams{
		Cbor:      params,
		AddedSlot: slot,
		Epoch:     epoch,
		EraId:     eraId,
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}

// SetPParamUpdate saves a protocol parameter update
func (d *MetadataStoreSqlite) SetPParamUpdate(
	genesis, update []byte,
	slot, epoch uint64,
	txn types.Txn,
) error {
	tmpItem := models.PParamUpdate{
		GenesisHash: genesis,
		Cbor:        update,
		AddedSlot:   slot,
		Epoch:       epoch,
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}

// DeletePParamsAfterSlot removes protocol parameter records added after the given slot.
func (d *MetadataStoreSqlite) DeletePParamsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Where("added_slot > ?", slot).Delete(&models.PParams{}); result.Error != nil {
		return result.Error
	}
	return nil
}

// DeletePParamUpdatesAfterSlot removes protocol parameter update records added after the given slot.
func (d *MetadataStoreSqlite) DeletePParamUpdatesAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Where("added_slot > ?", slot).Delete(&models.PParamUpdate{}); result.Error != nil {
		return result.Error
	}
	return nil
}
