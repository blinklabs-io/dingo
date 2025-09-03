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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetPool gets a pool
func (d *MetadataStoreSqlite) GetPool(
	// pkh lcommon.PoolKeyHash,
	pkh []byte,
	txn *gorm.DB,
) (models.Pool, error) {
	ret := models.Pool{}
	tmpPool := models.Pool{}
	if txn != nil {
		result := txn.
			Preload("Registration", func(db *gorm.DB) *gorm.DB { return db.Order("id DESC").Limit(1) }).
			First(
				&tmpPool,
				"pool_key_hash = ?",
				pkh,
			)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().
			Preload("Registration", func(db *gorm.DB) *gorm.DB { return db.Order("id DESC").Limit(1) }).
			First(
				&tmpPool,
				"pool_key_hash = ?",
				pkh,
			)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	ret = tmpPool
	return ret, nil
}

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
	var addrKeyHash lcommon.AddrKeyHash
	var tmpCert lcommon.PoolRegistrationCertificate
	var tmpMargin lcommon.GenesisRat
	var tmpRelay lcommon.PoolRelay
	for _, cert := range certs {
		tmpMargin = lcommon.GenesisRat{Rat: cert.Margin.Rat}
		tmpCert = lcommon.PoolRegistrationCertificate{
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
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224(cert.RewardAccount),
			),
		}
		for _, owner := range cert.Owners {
			addrKeyHash = lcommon.AddrKeyHash(
				lcommon.NewBlake2b224(owner.KeyHash),
			)
			tmpCert.PoolOwners = append(tmpCert.PoolOwners, addrKeyHash)
		}
		for _, relay := range cert.Relays {
			tmpRelay = lcommon.PoolRelay{}
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

// SetPoolRegistration saves a pool registration certificate and pool
func (d *MetadataStoreSqlite) SetPoolRegistration(
	cert *lcommon.PoolRegistrationCertificate,
	slot, deposit uint64,
	txn *gorm.DB,
) error {
	var tmpItem models.Pool
	tmpPool, err := d.GetPool(cert.Operator[:], txn)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			tmpItem = models.Pool{
				PoolKeyHash: cert.Operator[:],
				VrfKeyHash:  cert.VrfKeyHash[:],
			}
		} else {
			return err
		}
	} else {
		// Store our fetched pool
		tmpItem = tmpPool
	}
	tmpItem.Pledge = types.Uint64(cert.Pledge)
	tmpItem.Cost = types.Uint64(cert.Cost)
	tmpItem.Margin = &types.Rat{Rat: cert.Margin.Rat}
	tmpItem.RewardAccount = cert.RewardAccount[:]

	tmpReg := models.PoolRegistration{
		PoolKeyHash:   cert.Operator[:],
		VrfKeyHash:    cert.VrfKeyHash[:],
		Pledge:        types.Uint64(cert.Pledge),
		Cost:          types.Uint64(cert.Cost),
		Margin:        &types.Rat{Rat: cert.Margin.Rat},
		AddedSlot:     slot,
		DepositAmount: deposit,
	}
	if cert.PoolMetadata != nil {
		tmpReg.MetadataUrl = cert.PoolMetadata.Url
		tmpReg.MetadataHash = cert.PoolMetadata.Hash[:]
	}
	for _, owner := range cert.PoolOwners {
		tmpReg.Owners = append(
			tmpReg.Owners,
			models.PoolRegistrationOwner{KeyHash: owner[:]},
		)
	}
	tmpItem.Owners = tmpReg.Owners
	var tmpRelay models.PoolRegistrationRelay
	for _, relay := range cert.Relays {
		tmpRelay = models.PoolRegistrationRelay{
			Ipv4: relay.Ipv4,
			Ipv6: relay.Ipv6,
		}
		if relay.Port != nil {
			tmpRelay.Port = uint(*relay.Port)
		}
		if relay.Hostname != nil {
			tmpRelay.Hostname = *relay.Hostname
		}
		tmpItem.Relays = append(tmpReg.Relays, tmpRelay)
	}
	tmpItem.Registration = append(tmpItem.Registration, tmpReg)
	tmpItem.Relays = tmpReg.Relays
	onConflict := clause.OnConflict{
		Columns:   []clause.Column{{Name: "pool_key_hash"}},
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

// SetPoolRetirement saves a pool retirement certificate
func (d *MetadataStoreSqlite) SetPoolRetirement(
	cert *lcommon.PoolRetirementCertificate,
	slot uint64,
	txn *gorm.DB,
) error {
	tmpPool, err := d.GetPool(cert.PoolKeyHash[:], txn)
	if err != nil {
		return err
	}
	tmpItem := models.PoolRetirement{
		PoolKeyHash: cert.PoolKeyHash[:],
		Epoch:       cert.Epoch,
		AddedSlot:   slot,
	}
	tmpPool.Retirement = append(tmpPool.Retirement, tmpItem)
	if txn != nil {
		if result := txn.Save(&tmpPool); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Save(&tmpPool); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
