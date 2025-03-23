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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetPoolRegistrations returns pool registration certificates
func (d *MetadataStoreSqlite) GetPoolRegistrations(
	pkh lcommon.PoolKeyHash,
	txn *gorm.DB,
) ([]lcommon.PoolRegistrationCertificate, error) {
	ret := []lcommon.PoolRegistrationCertificate{}
	certs := []models.PoolRegistration{}
	if txn != nil {
		result := txn.Where("pool_key_hash = ?", lcommon.Blake2b224(pkh).Bytes()).
			Order("id DESC").
			Find(&certs)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("pool_key_hash = ?", lcommon.Blake2b224(pkh).Bytes()).
			Order("id DESC").
			Find(&certs)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	for _, cert := range certs {
		tmpMargin := cbor.Rat{Rat: cert.Margin.Rat}
		tmpCert := lcommon.PoolRegistrationCertificate{
			CertType: lcommon.CertificateTypePoolRegistration,
			Operator: lcommon.PoolKeyHash(
				lcommon.NewBlake2b224(cert.PoolKeyHash),
			),
			VrfKeyHash: lcommon.VrfKeyHash(
				lcommon.NewBlake2b256(cert.VrfKeyHash),
			),
			Pledge: uint64(cert.Pledge),
			Cost:   uint64(cert.Cost),
			Margin: tmpMargin,
			// TODO: we do not store this anywhere
			// RewardAccount: lcommon.AddrKeyHash(lcommon.NewBlake2b256(cert.PoolOwners[0]))
		}
		for _, owner := range cert.Owners {
			addrKeyHash := lcommon.AddrKeyHash(
				lcommon.NewBlake2b224(owner.KeyHash),
			)
			tmpCert.PoolOwners = append(tmpCert.PoolOwners, addrKeyHash)
		}
		for _, relay := range cert.Relays {
			tmpRelay := lcommon.PoolRelay{}
			// Determine type
			if relay.Port != 0 {
				port := uint32(relay.Port) // #nosec G115
				tmpRelay.Port = &port
				if relay.Hostname != "" {
					hostname := relay.Hostname
					tmpRelay.Type = lcommon.PoolRelayTypeSingleHostName
					tmpRelay.Hostname = &hostname
				} else {
					tmpRelay.Type = lcommon.PoolRelayTypeSingleHostAddress
					tmpRelay.Ipv4 = relay.Ipv4
					tmpRelay.Ipv6 = relay.Ipv6
				}
			} else {
				hostname := relay.Hostname
				tmpRelay.Type = lcommon.PoolRelayTypeMultiHostName
				tmpRelay.Hostname = &hostname
			}
			tmpCert.Relays = append(tmpCert.Relays, tmpRelay)
		}
		if cert.MetadataUrl != "" {
			poolMetadata := &lcommon.PoolMetadata{
				Url: cert.MetadataUrl,
				Hash: lcommon.PoolMetadataHash(
					lcommon.NewBlake2b256(cert.MetadataHash),
				),
			}
			tmpCert.PoolMetadata = poolMetadata
		}
		ret = append(ret, tmpCert)
	}
	return ret, nil
}

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
	for _, cert := range certs {
		tmpCert := lcommon.StakeRegistrationCertificate{
			CertType: lcommon.CertificateTypeStakeRegistration,
			StakeRegistration: lcommon.StakeCredential{
				// TODO: determine correct type
				// CredType: lcommon.StakeCredentialTypeAddrKeyHash,
				Credential: cert.StakingKey,
			},
		}
		ret = append(ret, tmpCert)
	}
	return ret, nil
}

// SetPoolRegistration saves a pool registration certificate
func (d *MetadataStoreSqlite) SetPoolRegistration(
	cert *lcommon.PoolRegistrationCertificate,
	slot, deposit uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.PoolRegistration{
		PoolKeyHash:   cert.Operator[:],
		VrfKeyHash:    cert.VrfKeyHash[:],
		Pledge:        models.Uint64(cert.Pledge),
		Cost:          models.Uint64(cert.Cost),
		Margin:        &models.Rat{Rat: cert.Margin.Rat},
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if cert.PoolMetadata != nil {
		tmpItem.MetadataUrl = cert.PoolMetadata.Url
		tmpItem.MetadataHash = cert.PoolMetadata.Hash[:]
	}
	for _, owner := range cert.PoolOwners {
		tmpItem.Owners = append(
			tmpItem.Owners,
			models.PoolRegistrationOwner{KeyHash: owner[:]},
		)
	}
	for _, relay := range cert.Relays {
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
	cert *lcommon.PoolRetirementCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.PoolRetirement{
		PoolKeyHash: cert.PoolKeyHash[:],
		Epoch:       cert.Epoch,
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
	cert *lcommon.StakeDelegationCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.StakeDelegation{
		StakingKey:  cert.StakeCredential.Credential,
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
		StakingKey: cert.StakeDeregistration.Credential,
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
	tmpItem := models.StakeRegistration{
		StakingKey:    cert.StakeRegistration.Credential,
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
