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
	"math/big"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetPoolRegistrations returns pool registration certificates
func (d *MetadataStoreSqlite) GetPoolRegistrations(
	pkh []byte,
	txn *gorm.DB,
) ([]models.PoolRegistration, error) {
	ret := []models.PoolRegistration{}
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.PoolRegistration{}); err != nil {
		return ret, err
	}
	if txn != nil {
		result := txn.Where("pool_key_hash = ?", pkh).
			Order("id DESC").
			Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("pool_key_hash = ?", pkh).Order("id DESC").Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	return ret, nil
}

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

// SetPoolRegistration saves a pool registration certificate
func (d *MetadataStoreSqlite) SetPoolRegistration(
	pkh, vrf []byte,
	pledge, cost, slot, deposit uint64,
	margin *big.Rat,
	owners []lcommon.AddrKeyHash,
	relays []lcommon.PoolRelay,
	metadata *lcommon.PoolMetadata,
	txn *gorm.DB,
) error {
	tables := []any{
		&models.PoolRegistration{},
		&models.PoolRegistrationOwner{},
		&models.PoolRegistrationRelay{},
	}
	for _, table := range tables {
		// Create table if it doesn't exist
		if err := d.DB().AutoMigrate(table); err != nil {
			return err
		}
	}
	tmpItem := models.PoolRegistration{
		PoolKeyHash:   pkh,
		VrfKeyHash:    vrf,
		Pledge:        models.Uint64(pledge),
		Cost:          models.Uint64(cost),
		Margin:        &models.Rat{Rat: margin},
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if metadata != nil {
		tmpItem.MetadataUrl = metadata.Url
		tmpItem.MetadataHash = metadata.Hash[:]
	}
	for _, owner := range owners {
		tmpItem.Owners = append(
			tmpItem.Owners,
			models.PoolRegistrationOwner{KeyHash: owner[:]},
		)
	}
	for _, relay := range relays {
		tmpRelay := models.PoolRegistrationRelay{
			Ipv4: relay.Ipv4,
			Ipv6: relay.Ipv6,
		}
		if relay.Port != nil {
			tmpRelay.Port = uint(*relay.Port)
		}
		if relay.Hostname != nil {
			tmpRelay.Hostname = *relay.Hostname
		}
		tmpItem.Relays = append(tmpItem.Relays, tmpRelay)
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

// SetPoolOwners updates the pool owners on a pool registration
func (d *MetadataStoreSqlite) SetPoolOwners(
	pkh []byte,
	owners [][]byte,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.PoolRegistrationOwner{}); err != nil {
		return err
	}
	poolRegistrations, err := d.GetPoolRegistrations(pkh, txn)
	if err != nil {
		return err
	}
	if len(poolRegistrations) == 0 {
		return errors.New("pool not found")
	}
	// TODO: check if we have a deregistration and act accordingly
	pool := poolRegistrations[0]
	poolOwners := []models.PoolRegistrationOwner{}
	for _, owner := range owners {
		poolOwners = append(
			poolOwners,
			models.PoolRegistrationOwner{KeyHash: owner[:]},
		)
	}
	pool.Owners = poolOwners
	if txn != nil {
		txn.Where(&models.PoolRegistrationOwner{PoolRegistrationID: pool.ID}).
			Delete(&models.PoolRegistrationOwner{})
		if result := txn.Save(&pool); result.Error != nil {
			return result.Error
		}
	} else {
		d.DB().Where(&models.PoolRegistrationOwner{PoolRegistrationID: pool.ID}).Delete(&models.PoolRegistrationOwner{})
		if result := d.DB().Save(&pool); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetPoolRelays updates the pool relays on a pool registration
func (d *MetadataStoreSqlite) SetPoolRelays(
	pkh []byte,
	relays []models.PoolRegistrationRelay,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.PoolRegistrationRelay{}); err != nil {
		return err
	}
	poolRegistrations, err := d.GetPoolRegistrations(pkh, txn)
	if err != nil {
		return err
	}
	if len(poolRegistrations) == 0 {
		return errors.New("pool not found")
	}
	// TODO: check if we have a deregistration and act accordingly
	pool := poolRegistrations[0]
	poolRelays := []models.PoolRegistrationRelay{}
	for _, relay := range relays {
		tmpRelay := models.PoolRegistrationRelay{
			Ipv4:     relay.Ipv4,
			Ipv6:     relay.Ipv6,
			Port:     relay.Port,
			Hostname: relay.Hostname,
		}
		poolRelays = append(
			poolRelays,
			tmpRelay,
		)
	}
	pool.Relays = poolRelays
	if txn != nil {
		txn.Where(&models.PoolRegistrationRelay{PoolRegistrationID: pool.ID}).
			Delete(&models.PoolRegistrationRelay{})
		if result := txn.Save(&pool); result.Error != nil {
			return result.Error
		}
	} else {
		d.DB().Where(&models.PoolRegistrationRelay{PoolRegistrationID: pool.ID}).Delete(&models.PoolRegistrationRelay{})
		if result := d.DB().Save(&pool); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
