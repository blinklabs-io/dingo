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
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"gorm.io/gorm"
)

// GetPoolRegistration returns pool registration certificates
// func (d *MetadataStoreSqlite) GetPoolRegistration(
// 	cred []byte,
// 	txn *gorm.DB,
// ) ([]models.PoolRegistration, error) {
// 	ret := []models.PoolRegistration{}
// 	// Create table if it doesn't exist
// 	if err := d.DB().AutoMigrate(&models.PoolRegistration{}); err != nil {
// 		return ret, err
// 	}
// 	if txn != nil {
// 		result := txn.Where("pool_key_hash = ?", cred).Find(&ret)
// 		if result.Error != nil {
// 			return ret, result.Error
// 		}
// 	} else {
// 		result := d.DB().Where("pool_key_hash = ?", cred).Find(&ret)
// 		if result.Error != nil {
// 			return ret, result.Error
// 		}
// 	}
// 	return ret, nil
// }

// GetStakeRegistrations returns stake registration certificates
func (d *MetadataStoreSqlite) GetStakeRegistrations(
	cred []byte,
	txn *gorm.DB,
) ([]models.StakeRegistration, error) {
	ret := []models.StakeRegistration{}
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.StakeRegistration{}); err != nil {
		return ret, err
	}
	if txn != nil {
		result := txn.Where("staking_key= ?", cred).Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("staking_key= ?", cred).Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	return ret, nil
}

// SetPoolRetirement saves a pool retirement certificate
func (d *MetadataStoreSqlite) SetPoolRetirement(
	pkh []byte,
	slot, epoch uint64,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.PoolRetirement{}); err != nil {
		return err
	}
	tmpItem := models.PoolRetirement{
		PoolKeyHash: pkh,
		Epoch:       epoch,
		AddedSlot:   slot,
	}
	if txn != nil {
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetStakeDelegation saves a stake delegation certificate
func (d *MetadataStoreSqlite) SetStakeDelegation(
	cred, pkh []byte,
	slot uint64,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.StakeDelegation{}); err != nil {
		return err
	}
	tmpItem := models.StakeDelegation{
		StakingKey:  cred,
		PoolKeyHash: pkh,
		AddedSlot:   slot,
	}
	if txn != nil {
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetStakeDeregistration saves a stake deregistration certificate
func (d *MetadataStoreSqlite) SetStakeDeregistration(
	cred []byte,
	slot uint64,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.StakeDeregistration{}); err != nil {
		return err
	}
	tmpItem := models.StakeDeregistration{
		StakingKey: cred,
		AddedSlot:  slot,
	}
	if txn != nil {
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetStakeRegistration saves a stake registration certificate
func (d *MetadataStoreSqlite) SetStakeRegistration(
	cred []byte,
	slot, deposit uint64,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.StakeRegistration{}); err != nil {
		return err
	}
	tmpItem := models.StakeRegistration{
		StakingKey:    cred,
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if txn != nil {
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetPoolRegistration stores a pool registration
// func (d *MetadataStoreSqlite) SetPoolRegistration(item models.PoolRegistration) error {
// 	if result := d.DB().Create(&item); result.Error != nil {
// 		return result.Error
// 	}
// 	return nil
// }
