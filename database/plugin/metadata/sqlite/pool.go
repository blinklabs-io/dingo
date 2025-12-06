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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetPool gets a pool
func (d *MetadataStoreSqlite) GetPool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn *gorm.DB,
) (*models.Pool, error) {
	ret := &models.Pool{}
	if txn == nil {
		txn = d.DB()
	}
	result := txn.
		Preload(
			"Registration",
			func(db *gorm.DB) *gorm.DB { return db.Order("added_slot DESC").Limit(1) },
		).
		Preload(
			"Retirement",
			func(db *gorm.DB) *gorm.DB { return db.Order("added_slot DESC").Limit(1) },
		).
		First(
			ret,
			"pool_key_hash = ?",
			pkh.Bytes(),
		)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	if !includeInactive {
		hasReg := len(ret.Registration) > 0
		hasRet := len(ret.Retirement) > 0
		if !hasReg {
			return nil, nil
		}
		// If the latest retirement is more recent than the latest registration,
		// check whether the retirement epoch has passed. A retirement in the
		// future means the pool is still active until that epoch is reached.
		if hasRet &&
			ret.Retirement[0].AddedSlot > ret.Registration[0].AddedSlot {
			retEpoch := ret.Retirement[0].Epoch
			// Determine current epoch from tip -> epoch table. If we cannot
			// determine the current epoch, conservatively treat the pool as active.
			var tmpTip models.Tip
			if res := txn.Where("id = ?", tipEntryId).First(&tmpTip); res.Error != nil {
				// Can't get tip, assume active
				return ret, nil //nolint:nilerr
			}
			var curEpoch models.Epoch
			if res := txn.Where("start_slot <= ?", tmpTip.Slot).Order("start_slot DESC").First(&curEpoch); res.Error != nil {
				// Can't determine current epoch, assume active
				return ret, nil //nolint:nilerr
			}
			if retEpoch > curEpoch.EpochId {
				// Retirement is in the future -> pool still active
				return ret, nil
			}
			// Retirement epoch has passed -> treat as inactive
			return nil, nil
		}
	}
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
			CertType: uint(lcommon.CertificateTypePoolRegistration),
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
