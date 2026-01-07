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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetPool gets a pool
func (d *MetadataStoreSqlite) GetPool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn types.Txn,
) (*models.Pool, error) {
	ret := &models.Pool{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.
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
			if res := db.Where("id = ?", tipEntryId).First(&tmpTip); res.Error != nil {
				// Can't get tip, assume active
				return ret, nil //nolint:nilerr
			}
			var curEpoch models.Epoch
			if res := db.Where("start_slot <= ?", tmpTip.Slot).Order("start_slot DESC").First(&curEpoch); res.Error != nil {
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
	txn types.Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	ret := []lcommon.PoolRegistrationCertificate{}
	certs := []models.PoolRegistration{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return ret, err
	}
	result := db.
		Preload("Owners").
		Preload("Relays").
		Where("pool_key_hash = ?", pkh.Bytes()).
		Order("id DESC").
		Find(&certs)
	if result.Error != nil {
		return ret, result.Error
	}
	var addrKeyHash lcommon.AddrKeyHash
	var tmpCert lcommon.PoolRegistrationCertificate
	var tmpRelay lcommon.PoolRelay
	for _, cert := range certs {
		var tmpMargin lcommon.GenesisRat
		if cert.Margin == nil || cert.Margin.Rat == nil {
			return nil, fmt.Errorf(
				"pool registration margin is nil (id=%d)",
				cert.ID,
			)
		}
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

// GetActivePoolRelays returns all relays from currently active pools.
// A pool is considered active if it has a registration and either:
// - No retirement, or
// - The retirement epoch is in the future
//
// Implementation note: This function loads all registrations and retirements
// per pool and filters in Go rather than using complex SQL JOINs. This approach
// is necessary because:
//  1. GORM's Preload with Limit(1) applies the limit globally, not per-parent
//  2. Determining the "latest" registration/retirement requires comparing
//     added_slot values which is cumbersome in a single query
//  3. The filtering logic (retirement epoch vs current epoch) is clearer in Go
//
// For networks with thousands of pools, this may use significant memory.
// Future optimization could use raw SQL with window functions (ROW_NUMBER)
// or batch processing if memory becomes a concern.
func (d *MetadataStoreSqlite) GetActivePoolRelays(
	txn types.Txn,
) ([]models.PoolRegistrationRelay, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}

	// Get the current epoch from the tip
	var tmpTip models.Tip
	if res := db.Where("id = ?", tipEntryId).First(&tmpTip); res.Error != nil {
		// If we can't get the tip, return empty (no relays)
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return []models.PoolRegistrationRelay{}, nil
		}
		return nil, res.Error
	}

	var curEpoch models.Epoch
	if res := db.Where(
		"start_slot <= ?",
		tmpTip.Slot,
	).Order("start_slot DESC").First(&curEpoch); res.Error != nil {
		// If we can't determine current epoch, return empty
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return []models.PoolRegistrationRelay{}, nil
		}
		return nil, res.Error
	}

	// Query all pools with their registrations, retirements, and relays.
	// We load ALL registrations/retirements per pool and filter in Go because
	// GORM's Preload with Limit(1) applies globally, not per-parent.
	var pools []models.Pool
	result := db.
		Preload("Registration", func(db *gorm.DB) *gorm.DB {
			return db.Order("added_slot DESC")
		}).
		Preload("Registration.Relays").
		Preload("Retirement", func(db *gorm.DB) *gorm.DB {
			return db.Order("added_slot DESC")
		}).
		Find(&pools)
	if result.Error != nil {
		return nil, result.Error
	}

	// Filter active pools and collect relays from the latest registration
	var relays []models.PoolRegistrationRelay
	for _, pool := range pools {
		// Skip pools without registrations
		if len(pool.Registration) == 0 {
			continue
		}

		// Get the latest registration (already sorted by added_slot DESC)
		latestReg := pool.Registration[0]

		// Check if pool is retired
		if len(pool.Retirement) > 0 {
			// Get the latest retirement (already sorted by added_slot DESC)
			latestRet := pool.Retirement[0]
			// If retirement is more recent than registration and epoch has passed
			if latestRet.AddedSlot > latestReg.AddedSlot &&
				latestRet.Epoch <= curEpoch.EpochId {
				continue // Pool is retired
			}
		}

		// Pool is active, add relays from the latest registration
		relays = append(relays, latestReg.Relays...)
	}

	return relays, nil
}
