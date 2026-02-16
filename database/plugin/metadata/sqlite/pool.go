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
// at or before the given slot. Uses one query with LEFT JOIN to get cert_index
// for same-slot disambiguation (COALESCE defaults to 0 when certificate
// reference is NULL). Chunks queries to avoid exceeding SQLite bind variable
// limits.
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
			Select("pool_registration.pool_id, pool_registration.added_slot, pool_registration.pledge, pool_registration.cost, pool_registration.margin, pool_registration.vrf_key_hash, pool_registration.reward_account, COALESCE(certs.cert_index, 0) AS cert_index, COALESCE(certs.id, 0) AS cert_id, COALESCE(\"transaction\".block_index, 0) AS block_index").
			Joins("LEFT JOIN certs ON certs.id = pool_registration.certificate_id").
			Joins("LEFT JOIN \"transaction\" ON \"transaction\".id = certs.transaction_id").
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
	db, err := d.resolveReadDB(txn)
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

// GetPoolByVrfKeyHash retrieves an active pool by its VRF key hash.
// Returns nil if no active pool uses this VRF key.
func (d *MetadataStoreSqlite) GetPoolByVrfKeyHash(
	vrfKeyHash []byte,
	txn types.Txn,
) (*models.Pool, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var pool models.Pool
	result := db.Where("vrf_key_hash = ?", vrfKeyHash).First(&pool)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &pool, nil
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
	db, err := d.resolveReadDB(txn)
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
	db, err := d.resolveReadDB(txn)
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
	// JOINs with certs and transaction tables to use block_index and cert_index
	// for consistent on-chain certificate ordering (matching GetActivePoolKeyHashesAtSlot).
	// Note: cert_index resets per transaction, so block_index must come first.
	var pools []models.Pool
	result := db.
		Preload("Registration", func(db *gorm.DB) *gorm.DB {
			return db.Select("pool_registration.*").
				Joins("LEFT JOIN certs ON certs.id = pool_registration.certificate_id").
				Joins("LEFT JOIN \"transaction\" ON \"transaction\".id = certs.transaction_id").
				Order("pool_registration.added_slot DESC, COALESCE(\"transaction\".block_index, 0) DESC, COALESCE(certs.cert_index, 0) DESC")
		}).
		Preload("Registration.Relays").
		Preload("Retirement", func(db *gorm.DB) *gorm.DB {
			return db.Select("pool_retirement.*").
				Joins("LEFT JOIN certs ON certs.id = pool_retirement.certificate_id").
				Joins("LEFT JOIN \"transaction\" ON \"transaction\".id = certs.transaction_id").
				Order("pool_retirement.added_slot DESC, COALESCE(\"transaction\".block_index, 0) DESC, COALESCE(certs.cert_index, 0) DESC")
		}).
		Find(&pools)
	if result.Error != nil {
		return nil, result.Error
	}

	// Collect certificate IDs where same-slot comparison is needed
	// to look up block_index and cert_index for tie-breaking
	var certIDs []uint
	for _, pool := range pools {
		if len(pool.Registration) == 0 || len(pool.Retirement) == 0 {
			continue
		}
		if pool.Retirement[0].AddedSlot == pool.Registration[0].AddedSlot {
			certIDs = append(
				certIDs,
				pool.Registration[0].CertificateID,
				pool.Retirement[0].CertificateID,
			)
		}
	}

	// certInfo holds block_index and cert_index for same-slot comparison
	type certInfo struct {
		blockIndex uint32
		certIndex  uint
	}
	// Use composite key (cert ID + cert type) to disambiguate when
	// a registration and retirement share the same CertificateID
	type certInfoKey struct {
		certID   uint
		certType uint
	}
	certInfoMap := make(map[certInfoKey]certInfo)
	if len(certIDs) > 0 {
		// Fetch cert_type, cert_index and block_index via join with transaction
		// Chunk to avoid exceeding SQLite bind variable limits
		type certResult struct {
			CertID     uint
			CertType   uint
			CertIndex  uint
			BlockIndex uint32
		}
		for start := 0; start < len(certIDs); start += sqliteBindVarLimit {
			end := min(start+sqliteBindVarLimit, len(certIDs))
			idChunk := certIDs[start:end]

			var certResults []certResult
			if err := db.Table("certs").
				Select("certs.id AS cert_id, certs.cert_type, certs.cert_index, COALESCE(\"transaction\".block_index, 0) AS block_index").
				Joins("LEFT JOIN \"transaction\" ON \"transaction\".id = certs.transaction_id").
				Where("certs.id IN ?", idChunk).
				Scan(&certResults).Error; err != nil {
				return nil, fmt.Errorf(
					"GetActivePoolRelays: fetch cert indexes: %w",
					err,
				)
			}
			for _, c := range certResults {
				certInfoMap[certInfoKey{certID: c.CertID, certType: c.CertType}] = certInfo{
					blockIndex: c.BlockIndex,
					certIndex:  c.CertIndex,
				}
			}
		}
	}

	// Filter active pools and collect relays from the latest registration
	var relays []models.PoolRegistrationRelay
	for _, pool := range pools {
		// Skip pools without registrations
		if len(pool.Registration) == 0 {
			continue
		}

		// Get the latest registration (already sorted by added_slot DESC, block_index DESC, cert_index DESC)
		latestReg := pool.Registration[0]

		// Check if pool is retired
		if len(pool.Retirement) > 0 {
			// Get the latest retirement (already sorted by added_slot DESC, block_index DESC, cert_index DESC)
			latestRet := pool.Retirement[0]
			// Check if retirement takes precedence over registration
			shouldCheckRetirement := false
			if latestRet.AddedSlot > latestReg.AddedSlot {
				shouldCheckRetirement = true
			} else if latestRet.AddedSlot == latestReg.AddedSlot {
				// Same-slot case: use block_index then cert_index for on-chain ordering
				// Higher block_index/cert_index means it came later in the block
				// Disambiguate by cert_type to handle potential CertificateID collisions
				retInfo := certInfoMap[certInfoKey{
					certID:   latestRet.CertificateID,
					certType: uint(lcommon.CertificateTypePoolRetirement),
				}]
				regInfo := certInfoMap[certInfoKey{
					certID:   latestReg.CertificateID,
					certType: uint(lcommon.CertificateTypePoolRegistration),
				}]
				if retInfo.blockIndex > regInfo.blockIndex {
					shouldCheckRetirement = true
				} else if retInfo.blockIndex == regInfo.blockIndex && retInfo.certIndex > regInfo.certIndex {
					shouldCheckRetirement = true
				}
			}
			// If retirement takes precedence and epoch has passed, pool is retired
			if shouldCheckRetirement && latestRet.Epoch <= curEpoch.EpochId {
				continue // Pool is retired
			}
		}

		// Pool is active, add relays from the latest registration
		relays = append(relays, latestReg.Relays...)
	}

	return relays, nil
}

// GetActivePoolKeyHashes retrieves the key hashes of all currently active pools.
// A pool is active if it has a registration and either no retirement or
// the retirement epoch is in the future.
//
// This delegates to GetActivePoolKeyHashesAtSlot using the current tip's slot,
// ensuring consistent same-slot certificate handling via block_index and cert_index
// ordering (ORDER BY added_slot DESC, block_index DESC, cert_index DESC).
func (d *MetadataStoreSqlite) GetActivePoolKeyHashes(
	txn types.Txn,
) ([][]byte, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf("GetActivePoolKeyHashes: resolve db: %w", err)
	}

	// Get the current tip slot
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

	// Delegate to slot-based query for consistent behavior
	return d.GetActivePoolKeyHashesAtSlot(tmpTip.Slot, txn)
}

// GetActivePoolKeyHashesAtSlot retrieves the key hashes of pools that were
// active at the given slot. A pool was active at a slot if:
//  1. It had a registration with added_slot <= slot
//  2. Either no retirement with added_slot <= slot, OR the retirement was
//     for an epoch that hadn't started by the given slot
//
// This implementation only fetches pools that have at least one registration
// at or before the given slot, avoiding loading all pools for early slot queries.
// Uses window functions to get the latest registration/retirement per pool
// directly in SQLite, minimizing memory usage.
func (d *MetadataStoreSqlite) GetActivePoolKeyHashesAtSlot(
	slot uint64,
	txn types.Txn,
) ([][]byte, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: resolve db: %w",
			err,
		)
	}

	// Find the epoch that contains the given slot
	var epochAtSlot models.Epoch
	if res := db.Where(
		"start_slot <= ?",
		slot,
	).Order("start_slot DESC").First(&epochAtSlot); res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			// No epoch data yet - return error so callers can
			// distinguish "no pools" from "data not synced"
			return nil, fmt.Errorf(
				"GetActivePoolKeyHashesAtSlot: %w",
				types.ErrNoEpochData,
			)
		}
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: get epoch at slot: %w",
			res.Error,
		)
	}

	// Verify the slot falls within the epoch's duration. If it doesn't,
	// we only have an older epoch and the requested slot is beyond our
	// synced data. Using a stale epoch ID could incorrectly treat retired
	// pools as active.
	if slot >= epochAtSlot.StartSlot+uint64(epochAtSlot.LengthInSlots) {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: %w",
			types.ErrNoEpochData,
		)
	}

	// Use window functions to get only the latest registration and retirement
	// per pool at or before the given slot, then filter in SQL.
	// This avoids loading all pools and their certificates into memory.
	type poolResult struct {
		PoolKeyHash []byte
	}
	var results []poolResult

	// Query explanation:
	// 1. latest_reg: Gets the most recent registration per pool at or before slot
	//    JOINs with certs and transaction to get block_index and cert_index for
	//    on-chain ordering (cert_index resets per transaction)
	// 2. latest_ret: Gets the most recent retirement per pool at or before slot
	//    JOINs with certs and transaction to get block_index and cert_index
	// 3. Join pools with their latest registration (INNER JOIN ensures only pools
	//    with registrations at or before slot are included)
	// 4. LEFT JOIN retirement to handle pools without retirements
	// 5. Filter: Pool is active if no retirement OR retirement is before registration
	//    (using slot, block_index, and cert_index for same-slot disambiguation)
	//    OR retirement epoch hasn't started yet
	// Note: block_index from transaction provides ordering across transactions,
	// cert_index from certs provides ordering within a transaction.
	// COALESCE defaults to 0 when references are NULL (e.g., tests)
	query := `
		WITH latest_reg AS (
			SELECT pr.pool_id, pr.added_slot,
				COALESCE(t.block_index, 0) as blk_idx,
				COALESCE(c.cert_index, 0) as cert_idx,
				ROW_NUMBER() OVER (
					PARTITION BY pr.pool_id
					ORDER BY pr.added_slot DESC, COALESCE(t.block_index, 0) DESC, COALESCE(c.cert_index, 0) DESC
				) as rn
			FROM pool_registration pr
			LEFT JOIN certs c ON c.id = pr.certificate_id
			LEFT JOIN "transaction" t ON t.id = c.transaction_id
			WHERE pr.added_slot <= ?
		),
		latest_ret AS (
			SELECT rt.pool_id, rt.added_slot, rt.epoch,
				COALESCE(t.block_index, 0) as blk_idx,
				COALESCE(c.cert_index, 0) as cert_idx,
				ROW_NUMBER() OVER (
					PARTITION BY rt.pool_id
					ORDER BY rt.added_slot DESC, COALESCE(t.block_index, 0) DESC, COALESCE(c.cert_index, 0) DESC
				) as rn
			FROM pool_retirement rt
			LEFT JOIN certs c ON c.id = rt.certificate_id
			LEFT JOIN "transaction" t ON t.id = c.transaction_id
			WHERE rt.added_slot <= ?
		)
		SELECT p.pool_key_hash
		FROM pool p
		INNER JOIN latest_reg lr ON lr.pool_id = p.id AND lr.rn = 1
		LEFT JOIN latest_ret lrt ON lrt.pool_id = p.id AND lrt.rn = 1
		WHERE lrt.pool_id IS NULL
			OR lrt.added_slot < lr.added_slot
			OR (lrt.added_slot = lr.added_slot AND lrt.blk_idx < lr.blk_idx)
			OR (lrt.added_slot = lr.added_slot AND lrt.blk_idx = lr.blk_idx AND lrt.cert_idx < lr.cert_idx)
			OR lrt.epoch > ?`

	if err := db.Raw(query, slot, slot, epochAtSlot.EpochId).Scan(&results).Error; err != nil {
		return nil, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: query pools: %w",
			err,
		)
	}

	// Convert results to [][]byte
	poolKeyHashes := make([][]byte, len(results))
	for i, r := range results {
		poolKeyHashes[i] = r.PoolKeyHash
	}

	return poolKeyHashes, nil
}

// GetStakeByPool returns the total delegated stake and delegator count for a pool.
func (d *MetadataStoreSqlite) GetStakeByPool(
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
// Stake is computed by joining active accounts with their live UTxOs
// (deleted_slot = 0) and summing the UTxO amounts per pool.
func (d *MetadataStoreSqlite) GetStakeByPools(
	poolKeyHashes [][]byte,
	txn types.Txn,
) (map[string]uint64, map[string]uint64, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, nil, fmt.Errorf("GetStakeByPools: resolve db: %w", err)
	}

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

	// Query delegator counts per pool from active accounts
	type poolDelegatorResult struct {
		Pool           []byte
		DelegatorCount int64
	}

	var delegatorResults []poolDelegatorResult
	for start := 0; start < len(poolKeyHashes); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]

		var chunkResults []poolDelegatorResult
		if err := db.Model(&models.Account{}).
			Select("pool, COUNT(*) as delegator_count").
			Where("pool IN ? AND active = ?", chunk, true).
			Group("pool").
			Scan(&chunkResults).Error; err != nil {
			return nil, nil, fmt.Errorf(
				"GetStakeByPools: query delegator counts: %w",
				err,
			)
		}
		delegatorResults = append(delegatorResults, chunkResults...)
	}

	for _, r := range delegatorResults {
		if r.DelegatorCount >= 0 {
			delegatorMap[string(r.Pool)] = uint64(r.DelegatorCount)
		}
	}

	// Query total delegated stake per pool by joining accounts with
	// their live UTxOs (deleted_slot = 0) and summing UTxO amounts.
	type poolStakeResult struct {
		Pool       []byte
		TotalStake uint64
	}

	var stakeResults []poolStakeResult
	for start := 0; start < len(poolKeyHashes); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]

		var chunkResults []poolStakeResult
		if err := db.Table("account").
			Select("account.pool, COALESCE(SUM(utxo.amount), 0) as total_stake").
			Joins("INNER JOIN utxo ON utxo.staking_key = account.staking_key").
			Where("account.pool IN ? AND account.active = ? AND utxo.deleted_slot = 0", chunk, true).
			Group("account.pool").
			Scan(&chunkResults).Error; err != nil {
			return nil, nil, fmt.Errorf(
				"GetStakeByPools: query stake: %w",
				err,
			)
		}
		stakeResults = append(stakeResults, chunkResults...)
	}

	for _, r := range stakeResults {
		stakeMap[string(r.Pool)] = r.TotalStake
	}

	return stakeMap, delegatorMap, nil
}
