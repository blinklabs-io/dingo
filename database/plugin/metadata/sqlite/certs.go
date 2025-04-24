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
)

// GetStakeRegistrations returns stake registration certificates
func (d *MetadataStoreSqlite) GetStakeRegistrations(
	stakingKey []byte,
	txn *gorm.DB,
) ([]lcommon.StakeRegistrationCertificate, error) {
	ret := []lcommon.StakeRegistrationCertificate{}
	certs := []models.StakeRegistration{}
	if txn != nil {
		result := txn.Where("staking_key = ?", stakingKey).
			Order("id DESC").
			Find(&certs)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("staking_key = ?", stakingKey).
			Order("id DESC").
			Find(&certs)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	var tmpCert lcommon.StakeRegistrationCertificate
	for _, cert := range certs {
		tmpCert = lcommon.StakeRegistrationCertificate{
			CertType: lcommon.CertificateTypeStakeRegistration,
			StakeRegistration: lcommon.Credential{
				// TODO: determine correct type
				// CredType: lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(cert.StakingKey),
			},
		}
		ret = append(ret, tmpCert)
	}
	return ret, nil
}

// SetStakeDelegation saves a stake delegation certificate
func (d *MetadataStoreSqlite) SetStakeDelegation(
	cert *lcommon.StakeDelegationCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.StakeDelegation{
		StakingKey:  cert.StakeCredential.Credential.Bytes(),
		PoolKeyHash: cert.PoolKeyHash[:],
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
	cert *lcommon.StakeDeregistrationCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.StakeDeregistration{
		StakingKey: cert.StakeDeregistration.Credential.Bytes(),
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
	if err := d.SetAccount(stakeKey, nil, nil, slot, deposit, txn); err != nil {
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

// SetRegistration saves a registration certificate
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
	if err := d.SetAccount(stakeKey, nil, nil, slot, deposit, txn); err != nil {
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
