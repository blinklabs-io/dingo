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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetAccount gets an account
func (d *MetadataStoreSqlite) GetAccount(
	stakeKey []byte,
	txn *gorm.DB,
) (models.Account, error) {
	ret := models.Account{}
	tmpAccount := models.Account{}
	if txn != nil {
		if result := txn.First(&tmpAccount, "staking_key = ?", stakeKey); result.Error != nil {
			return ret, result.Error
		}
	} else {
		if result := d.DB().First(&tmpAccount, "staking_key = ?", stakeKey); result.Error != nil {
			return ret, result.Error
		}
	}
	ret = tmpAccount
	return ret, nil
}

// SetAccount saves an account
func (d *MetadataStoreSqlite) SetAccount(
	stakeKey, pkh, drep []byte,
	slot uint64,
	active bool,
	txn *gorm.DB,
) error {
	tmpItem := models.Account{
		StakingKey: stakeKey,
		AddedSlot:  slot,
		Pool:       pkh,
		Drep:       drep,
	}
	if txn != nil {
		if result := txn.Clauses(clause.OnConflict{UpdateAll: true}).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Clauses(clause.OnConflict{UpdateAll: true}).Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetDeregistration saves a deregistration certificate
func (d *MetadataStoreSqlite) SetDeregistration(
	cert *lcommon.DeregistrationCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	stakeKey := cert.StakeCredential.Credential.Bytes()
	tmpAccount, err := d.GetAccount(stakeKey, txn)
	if err != nil {
		return err
	}
	tmpItem := models.Deregistration{
		StakingKey: stakeKey,
		AddedSlot:  slot,
	}
	tmpAccount.Active = false
	if txn != nil {
		if accountErr := txn.Save(&tmpAccount); accountErr.Error != nil {
			return accountErr.Error
		}
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if accountErr := d.DB().Save(&tmpAccount); accountErr.Error != nil {
			return accountErr.Error
		}
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetStakeRegistration saves a stake registration certificate and account
func (d *MetadataStoreSqlite) SetStakeRegistration(
	cert *lcommon.StakeRegistrationCertificate,
	slot, deposit uint64,
	txn *gorm.DB,
) error {
	stakeKey := cert.StakeRegistration.Credential.Bytes()
	tmpItem := models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if err := d.SetAccount(stakeKey, nil, nil, slot, true, txn); err != nil {
		return err
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

// SetRegistration saves a registration certificate and account
func (d *MetadataStoreSqlite) SetRegistration(
	cert *lcommon.RegistrationCertificate,
	slot, deposit uint64,
	txn *gorm.DB,
) error {
	stakeKey := cert.StakeCredential.Credential.Bytes()
	tmpItem := models.Registration{
		StakingKey:    stakeKey,
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if err := d.SetAccount(stakeKey, nil, nil, slot, true, txn); err != nil {
		return err
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
	cert *lcommon.StakeDelegationCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	stakeKey := cert.StakeCredential.Credential.Bytes()
	tmpAccount, err := d.GetAccount(stakeKey, txn)
	if err != nil {
		return err
	}
	tmpItem := models.StakeDelegation{
		StakingKey:  stakeKey,
		PoolKeyHash: cert.PoolKeyHash[:],
		AddedSlot:   slot,
	}
	tmpAccount.Pool = tmpItem.PoolKeyHash
	if txn != nil {
		if accountErr := txn.Save(&tmpAccount); accountErr.Error != nil {
			return accountErr.Error
		}
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if accountErr := d.DB().Save(&tmpAccount); accountErr.Error != nil {
			return accountErr.Error
		}
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetStakeDeregistration saves a stake deregistration certificate
func (d *MetadataStoreSqlite) SetStakeDeregistration(
	cert *lcommon.StakeDeregistrationCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	stakeKey := cert.StakeDeregistration.Credential.Bytes()
	tmpAccount, err := d.GetAccount(stakeKey, txn)
	if err != nil {
		return err
	}
	tmpItem := models.StakeDeregistration{
		StakingKey: stakeKey,
		AddedSlot:  slot,
	}
	tmpAccount.Active = false
	if txn != nil {
		if accountErr := txn.Save(&tmpAccount); accountErr.Error != nil {
			return accountErr.Error
		}
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if accountErr := d.DB().Save(&tmpAccount); accountErr.Error != nil {
			return accountErr.Error
		}
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
