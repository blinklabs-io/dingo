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
	"math"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// poolRegRecord holds registration data for batch processing during pool restoration.
// Includes blockIndex, certIndex, and certID for deterministic same-slot disambiguation.
type poolRegRecord struct {
	pledge        types.Uint64
	cost          types.Uint64
	margin        *types.Rat
	vrfKeyHash    []byte
	rewardAccount []byte
	addedSlot     uint64
	blockIndex    uint32
	certIndex     uint32
	certID        uint
}

// isMoreRecent checks if this registration is more recent than the other.
// Uses lexicographic comparison of (addedSlot, blockIndex, certIndex, certID)
// for deterministic ordering when multiple registrations occur in the same slot.
func (r poolRegRecord) isMoreRecent(other poolRegRecord) bool {
	if r.addedSlot != other.addedSlot {
		return r.addedSlot > other.addedSlot
	}
	if r.blockIndex != other.blockIndex {
		return r.blockIndex > other.blockIndex
	}
	if r.certIndex != other.certIndex {
		return r.certIndex > other.certIndex
	}
	return r.certID > other.certID
}

// poolRegCache holds batch-fetched registration data for all pools being restored.
type poolRegCache struct {
	// Maps pool ID to the most recent registration
	registration map[uint]poolRegRecord
	hasReg       map[uint]bool
}

// newPoolRegCache creates an empty pool registration cache.
func newPoolRegCache(capacity int) *poolRegCache {
	return &poolRegCache{
		registration: make(map[uint]poolRegRecord, capacity),
		hasReg:       make(map[uint]bool, capacity),
	}
}

// batchFetchPoolRegs fetches all relevant registrations for the given pool IDs
// at or before the given slot. Uses one query with JOIN to get cert_index for
// same-slot disambiguation. Chunks queries to avoid exceeding SQLite bind
// variable limits.
func batchFetchPoolRegs(
	db *gorm.DB,
	poolIDs []uint,
	slot uint64,
) (*poolRegCache, error) {
	cache := newPoolRegCache(len(poolIDs))

	// Process pool IDs in chunks to avoid SQLite bind variable limits
	for start := 0; start < len(poolIDs); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(poolIDs))
		idChunk := poolIDs[start:end]

		// Fetch registration records with cert_index, block_index, and cert ID from joined tables
		type regResult struct {
			PoolID        uint
			AddedSlot     uint64
			BlockIndex    uint32
			CertIndex     uint32
			CertID        uint
			Pledge        types.Uint64
			Cost          types.Uint64
			Margin        *types.Rat
			VrfKeyHash    []byte
			RewardAccount []byte
		}
		var regRecords []regResult
		if err := db.Table("pool_registration").
			Select("pool_registration.pool_id, pool_registration.added_slot, pool_registration.pledge, pool_registration.cost, pool_registration.margin, pool_registration.vrf_key_hash, pool_registration.reward_account, certs.cert_index, certs.id AS cert_id, transaction.block_index").
			Joins("INNER JOIN certs ON certs.id = pool_registration.certificate_id").
			Joins("INNER JOIN transaction ON transaction.id = certs.transaction_id").
			Where("pool_registration.pool_id IN ? AND pool_registration.added_slot <= ?", idChunk, slot).
			Find(&regRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range regRecords {
			rec := poolRegRecord{
				addedSlot:     r.AddedSlot,
				blockIndex:    r.BlockIndex,
				certIndex:     r.CertIndex,
				certID:        r.CertID,
				pledge:        r.Pledge,
				cost:          r.Cost,
				margin:        r.Margin,
				vrfKeyHash:    r.VrfKeyHash,
				rewardAccount: r.RewardAccount,
			}
			if !cache.hasReg[r.PoolID] ||
				rec.isMoreRecent(cache.registration[r.PoolID]) {
				cache.registration[r.PoolID] = rec
				cache.hasReg[r.PoolID] = true
			}
		}
	}

	return cache, nil
}

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
			func(db *gorm.DB) *gorm.DB { return db.Order("added_slot DESC, id DESC").Limit(1) },
		).
		Preload("Registration.Owners").
		Preload("Registration.Relays").
		Preload(
			"Retirement",
			func(db *gorm.DB) *gorm.DB { return db.Order("added_slot DESC, id DESC").Limit(1) },
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
				if errors.Is(res.Error, gorm.ErrRecordNotFound) {
					// Tip not yet available (e.g., initial sync), treat pool as active
					return ret, nil
				}
				return nil, fmt.Errorf("failed to get tip entry: %w", res.Error)
			}
			var curEpoch models.Epoch
			if res := db.Where("start_slot <= ?", tmpTip.Slot).Order("start_slot DESC").First(&curEpoch); res.Error != nil {
				if errors.Is(res.Error, gorm.ErrRecordNotFound) {
					// Epoch data not yet available (e.g., initial sync), treat pool as active
					return ret, nil
				}
				return nil, fmt.Errorf(
					"failed to get current epoch: %w",
					res.Error,
				)
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

// RestorePoolStateAtSlot reverts pool state to the given slot.
//
// This function handles two cases for pools:
//  1. Pools with no registration certificates at or before the slot are deleted
//     (they didn't exist at that point in the chain).
//  2. Pools with at least one registration at or before the slot have their
//     denormalized fields (Pledge, Cost, Margin, VrfKeyHash, RewardAccount)
//     restored from the most recent such registration.
//
// Child records are automatically removed via CASCADE constraints when pools
// are deleted. PoolRegistration records after the slot are removed separately
// by DeleteCertificatesAfterSlot.
//
// This implementation uses batch fetching to avoid N+1 query patterns:
// instead of querying certificates per-pool, it fetches all relevant
// registrations for all affected pools upfront in one query with a JOIN
// to the certs table to get cert_index for deterministic same-slot ordering.
func (d *MetadataStoreSqlite) RestorePoolStateAtSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Phase 1: Delete pools with no registrations at or before the rollback slot
	poolsWithNoValidRegsSubquery := db.Model(&models.Pool{}).
		Select("pool.id").
		Where(
			"NOT EXISTS (?)",
			db.Model(&models.PoolRegistration{}).
				Select("1").
				Where("pool_registration.pool_id = pool.id AND pool_registration.added_slot <= ?", slot),
		)

	if result := db.Where(
		"id IN (?)",
		poolsWithNoValidRegsSubquery,
	).Delete(&models.Pool{}); result.Error != nil {
		return result.Error
	}

	// Phase 2: Restore denormalized fields for remaining pools that had any
	// registration after the rollback slot. These pools survive Phase 1
	// (meaning they have at least one registration at or before the slot),
	// but their denormalized fields may reflect a now-invalid later registration.
	var poolsToRestore []models.Pool
	poolsWithRegsAfterSlotSubquery := db.Model(&models.PoolRegistration{}).
		Select("DISTINCT pool_id").
		Where("added_slot > ?", slot)

	if result := db.Where(
		"id IN (?)",
		poolsWithRegsAfterSlotSubquery,
	).Find(&poolsToRestore); result.Error != nil {
		return result.Error
	}

	if len(poolsToRestore) == 0 {
		return nil
	}

	// Extract pool IDs for batch fetching
	poolIDs := make([]uint, len(poolsToRestore))
	for i, pool := range poolsToRestore {
		poolIDs[i] = pool.ID
	}

	// Batch-fetch all registrations for all affected pools
	cache, err := batchFetchPoolRegs(db, poolIDs, slot)
	if err != nil {
		return err
	}

	// Process each pool using the cached registration data
	for _, pool := range poolsToRestore {
		// Get registration from cache (must exist due to Phase 1 deletion)
		latestReg, hasRegAtSlot := cache.registration[pool.ID], cache.hasReg[pool.ID]
		if !hasRegAtSlot {
			// This indicates database inconsistency: Phase 1 should have deleted
			// any pool without a registration cert at or before the rollback slot.
			return fmt.Errorf(
				"pool %d has no registration cert at or before slot %d but wasn't deleted in Phase 1",
				pool.ID,
				slot,
			)
		}

		// Update the Pool's denormalized fields from the registration
		if result := db.Model(&pool).Updates(map[string]any{
			"pledge":         latestReg.pledge,
			"cost":           latestReg.cost,
			"margin":         latestReg.margin,
			"vrf_key_hash":   latestReg.vrfKeyHash,
			"reward_account": latestReg.rewardAccount,
		}); result.Error != nil {
			return result.Error
		}
	}

	return nil
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
				if relay.Port > math.MaxUint32 {
					return nil, fmt.Errorf(
						"pool relay port out of range: %d",
						relay.Port,
					)
				}
				port := uint32(relay.Port)
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
				// Port is 0, check if we have IP addresses first
				if relay.Ipv4 != nil || relay.Ipv6 != nil {
					tmpRelay.Type = lcommon.PoolRelayTypeSingleHostAddress
					tmpRelay.Ipv4 = relay.Ipv4
					tmpRelay.Ipv6 = relay.Ipv6
					// Port remains nil
				} else if relay.Hostname != "" {
					hostname := relay.Hostname
					tmpRelay.Type = lcommon.PoolRelayTypeMultiHostName
					tmpRelay.Hostname = &hostname
				}
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
			return db.Order("added_slot DESC, id DESC")
		}).
		Preload("Registration.Relays").
		Preload("Retirement", func(db *gorm.DB) *gorm.DB {
			return db.Order("added_slot DESC, id DESC")
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
