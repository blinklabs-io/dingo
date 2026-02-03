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

package postgres

import (
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetPool gets a pool
func (d *MetadataStorePostgres) GetPool(
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

// GetPoolRegistrations returns pool registration certificates
func (d *MetadataStorePostgres) GetPoolRegistrations(
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

// poolRegRecord holds fields from a pool registration for batch processing
// during pool state restoration.
type poolRegRecord struct {
	pledge        types.Uint64
	cost          types.Uint64
	margin        *types.Rat
	vrfKeyHash    []byte
	rewardAccount []byte
	addedSlot     uint64
	certIndex     uint32
}

// poolRegCache holds batch-fetched registration data for all pools being restored.
type poolRegCache struct {
	registration map[uint]poolRegRecord
	hasReg       map[uint]bool
}

// batchFetchPoolRegs fetches all registrations for the given pool IDs at or
// before the given slot, returning only the most recent registration per pool.
func batchFetchPoolRegs(
	db *gorm.DB,
	poolIDs []uint,
	slot uint64,
) (*poolRegCache, error) {
	cache := &poolRegCache{
		registration: make(map[uint]poolRegRecord, len(poolIDs)),
		hasReg:       make(map[uint]bool, len(poolIDs)),
	}

	type result struct {
		PoolID        uint
		Pledge        types.Uint64
		Cost          types.Uint64
		Margin        *types.Rat
		VrfKeyHash    []byte
		RewardAccount []byte
		AddedSlot     uint64
		CertIndex     uint32
	}
	var records []result

	// Use ROW_NUMBER to fetch only the latest registration per pool
	query := `
		WITH ranked AS (
			SELECT pr.pool_id, pr.pledge, pr.cost, pr.margin,
				pr.vrf_key_hash, pr.reward_account, pr.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY pr.pool_id
					ORDER BY pr.added_slot DESC, c.cert_index DESC
				) as rn
			FROM pool_registration pr
			INNER JOIN certs c ON c.id = pr.certificate_id
			WHERE pr.pool_id IN ? AND pr.added_slot <= ?
		)
		SELECT pool_id, pledge, cost, margin, vrf_key_hash, reward_account, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, poolIDs, slot).Scan(&records).Error; err != nil {
		return nil, err
	}

	for _, r := range records {
		cache.registration[r.PoolID] = poolRegRecord{
			pledge:        r.Pledge,
			cost:          r.Cost,
			margin:        r.Margin,
			vrfKeyHash:    r.VrfKeyHash,
			rewardAccount: r.RewardAccount,
			addedSlot:     r.AddedSlot,
			certIndex:     r.CertIndex,
		}
		cache.hasReg[r.PoolID] = true
	}

	return cache, nil
}

// RestorePoolStateAtSlot reverts pool state to the given slot.
// Pools that have no registrations at or before the given slot are deleted.
// Pools that have registrations at or before the given slot have their
// denormalized fields (pledge, cost, margin, etc.) restored from the most
// recent registration at or before the slot.
//
// This implementation uses batch fetching to avoid N+1 query patterns:
// instead of querying certificates per-pool, it fetches all relevant
// registrations for all affected pools upfront in one query with a JOIN
// to the certs table to get cert_index for deterministic same-slot ordering.
func (d *MetadataStorePostgres) RestorePoolStateAtSlot(
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

// GetActivePoolKeyHashes retrieves the key hashes of all currently active pools.
// A pool is active if it has a registration and either no retirement or
// the retirement epoch is in the future.
func (d *MetadataStorePostgres) GetActivePoolKeyHashes(
	txn types.Txn,
) ([][]byte, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf("GetActivePoolKeyHashes: resolve db: %w", err)
	}

	// Get the current epoch from the tip
	var tmpTip models.Tip
	if res := db.Where("id = ?", tipEntryId).First(&tmpTip); res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return [][]byte{}, nil
		}
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashes: get tip: %w",
			res.Error,
		)
	}

	var curEpoch models.Epoch
	if res := db.Where(
		"start_slot <= ?",
		tmpTip.Slot,
	).Order("start_slot DESC").First(&curEpoch); res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return [][]byte{}, nil
		}
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashes: get epoch: %w",
			res.Error,
		)
	}

	// Query all pools with their registrations and retirements
	var pools []models.Pool
	result := db.
		Preload("Registration", func(db *gorm.DB) *gorm.DB {
			return db.Order("added_slot DESC, id DESC")
		}).
		Preload("Retirement", func(db *gorm.DB) *gorm.DB {
			return db.Order("added_slot DESC, id DESC")
		}).
		Find(&pools)
	if result.Error != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashes: query pools: %w",
			result.Error,
		)
	}

	// Filter active pools and collect their key hashes
	poolKeyHashes := make([][]byte, 0, len(pools))
	for _, pool := range pools {
		if len(pool.Registration) == 0 {
			continue
		}

		latestReg := pool.Registration[0]

		// Check if pool is retired
		if len(pool.Retirement) > 0 {
			latestRet := pool.Retirement[0]
			if latestRet.AddedSlot > latestReg.AddedSlot &&
				latestRet.Epoch <= curEpoch.EpochId {
				continue // Pool is retired
			}
		}

		// Pool is active
		poolKeyHashes = append(poolKeyHashes, pool.PoolKeyHash)
	}

	return poolKeyHashes, nil
}

// GetStakeByPool returns the total delegated stake and delegator count for a pool.
func (d *MetadataStorePostgres) GetStakeByPool(
	poolKeyHash []byte,
	txn types.Txn,
) (uint64, uint64, error) {
	stakes, delegators, err := d.GetStakeByPools([][]byte{poolKeyHash}, txn)
	if err != nil {
		return 0, 0, err
	}
	return stakes[string(poolKeyHash)], delegators[string(poolKeyHash)], nil
}

// GetStakeByPools returns delegated stake for multiple pools in a single query.
func (d *MetadataStorePostgres) GetStakeByPools(
	poolKeyHashes [][]byte,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, nil, fmt.Errorf("GetStakeByPools: resolve db: %w", err)
	}

	// Initialize maps - stakeMap returns zeros since stake calculation
	// requires UTxO aggregation which is not yet implemented
	stakeMap := make(map[string]uint64, len(poolKeyHashes))
	delegatorMap := make(map[string]uint64, len(poolKeyHashes))

	// Initialize all pools with zero
	for _, hash := range poolKeyHashes {
		stakeMap[string(hash)] = 0
		delegatorMap[string(hash)] = 0
	}

	if len(poolKeyHashes) == 0 {
		return stakeMap, delegatorMap, nil
	}

	// Query accounts delegated to these pools and count
	type poolStakeResult struct {
		Pool           []byte
		DelegatorCount int64
	}

	var results []poolStakeResult
	if err := db.Model(&models.Account{}).
		Select("pool, COUNT(*) as delegator_count").
		Where("pool IN ? AND active = ?", poolKeyHashes, true).
		Group("pool").
		Scan(&results).Error; err != nil {
		return nil, nil, fmt.Errorf(
			"GetStakeByPools: query accounts: %w",
			err,
		)
	}

	// Update delegator counts from query results
	for _, r := range results {
		if r.DelegatorCount >= 0 {
			delegatorMap[string(r.Pool)] = uint64(r.DelegatorCount)
		}
	}

	// TODO: Implement full stake calculation. This requires:
	// 1. Get all staking_keys for accounts delegated to pools
	// 2. Query UTxOs by stake credential
	// 3. Sum values per pool
	// For now, stakeMap returns zeros - stake values are placeholders.

	return stakeMap, delegatorMap, nil
}
