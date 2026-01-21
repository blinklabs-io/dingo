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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetAccount gets an account
func (d *MetadataStorePostgres) GetAccount(
	stakeKey []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Account, error) {
	ret := &models.Account{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	query := db
	if !includeInactive {
		query = query.Where("active = ?", true)
	}
	result := query.First(ret, "staking_key = ?", stakeKey)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// SetAccount saves an account
func (d *MetadataStorePostgres) SetAccount(
	stakeKey, pkh, drep []byte,
	slot uint64,
	active bool,
	txn types.Txn,
) error {
	tmpItem := models.Account{
		StakingKey: stakeKey,
		AddedSlot:  slot,
		Pool:       pkh,
		Drep:       drep,
		Active:     active,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "staking_key"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"added_slot", "pool", "drep", "active"},
		),
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Clauses(onConflict).Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}

// certRecord holds common fields extracted from certificate records for
// batch processing during account state restoration.
// Includes certIndex for same-slot disambiguation.
type certRecord struct {
	pool      []byte
	drep      []byte
	addedSlot uint64
	certIndex uint32
}

// isMoreRecent checks if this certificate is more recent than the other.
// When slots are equal, certIndex determines order (higher = later in transaction).
func (r certRecord) isMoreRecent(other certRecord) bool {
	if r.addedSlot > other.addedSlot {
		return true
	}
	if r.addedSlot == other.addedSlot && r.certIndex > other.certIndex {
		return true
	}
	return false
}

// accountCertCache holds batch-fetched certificate data for all accounts
// being restored. This eliminates N+1 queries by fetching all certificates
// for all affected accounts in a small number of queries upfront.
type accountCertCache struct {
	// Maps staking key (as string) to the most recent registration
	latestReg map[string]certRecord
	hasReg    map[string]bool

	// Maps staking key to the most recent deregistration
	latestDereg map[string]certRecord
	hasDereg    map[string]bool

	// Maps staking key to the most recent pool delegation
	poolDelegation map[string]certRecord

	// Maps staking key to the most recent DRep delegation
	drepDelegation map[string]certRecord
}

// newAccountCertCache creates an empty certificate cache.
func newAccountCertCache(capacity int) *accountCertCache {
	return &accountCertCache{
		latestReg:      make(map[string]certRecord, capacity),
		hasReg:         make(map[string]bool, capacity),
		latestDereg:    make(map[string]certRecord, capacity),
		hasDereg:       make(map[string]bool, capacity),
		poolDelegation: make(map[string]certRecord, capacity),
		drepDelegation: make(map[string]certRecord, capacity),
	}
}

// updateReg updates the registration if this one is more recent.
func (c *accountCertCache) updateReg(key string, rec certRecord) {
	if !c.hasReg[key] || rec.isMoreRecent(c.latestReg[key]) {
		c.latestReg[key] = rec
		c.hasReg[key] = true
	}
}

// updateDereg updates the deregistration if this one is more recent.
func (c *accountCertCache) updateDereg(key string, rec certRecord) {
	if !c.hasDereg[key] || rec.isMoreRecent(c.latestDereg[key]) {
		c.latestDereg[key] = rec
		c.hasDereg[key] = true
	}
}

// updatePoolDelegation updates the pool delegation if this one is more recent.
func (c *accountCertCache) updatePoolDelegation(key string, rec certRecord) {
	existing, ok := c.poolDelegation[key]
	if !ok || rec.isMoreRecent(existing) {
		c.poolDelegation[key] = rec
	}
}

// updateDrepDelegation updates the DRep delegation if this one is more recent.
func (c *accountCertCache) updateDrepDelegation(key string, rec certRecord) {
	existing, ok := c.drepDelegation[key]
	if !ok || rec.isMoreRecent(existing) {
		c.drepDelegation[key] = rec
	}
}

// batchFetchCerts fetches all relevant certificates for the given staking keys
// at or before the given slot. Uses one query per certificate table.
func batchFetchCerts(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
) (*accountCertCache, error) {
	cache := newAccountCertCache(len(stakingKeys))

	// Registration certificates (5 types)
	if err := batchFetchStakeRegistration(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}
	if err := batchFetchStakeRegistrationDelegation(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}
	if err := batchFetchStakeVoteRegistrationDelegation(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}
	if err := batchFetchVoteRegistrationDelegation(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}
	if err := batchFetchRegistration(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}

	// Deregistration certificates (2 types)
	if err := batchFetchStakeDeregistration(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}
	if err := batchFetchDeregistration(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}

	// Pool delegation certificates - StakeDelegation is pool-only
	if err := batchFetchStakeDelegation(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}
	// StakeVoteDelegation has both pool and DRep
	if err := batchFetchStakeVoteDelegation(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}

	// DRep delegation certificates - VoteDelegation is DRep-only
	if err := batchFetchVoteDelegation(db, stakingKeys, slot, cache); err != nil {
		return nil, err
	}

	return cache, nil
}

func batchFetchStakeRegistration(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey []byte
		AddedSlot  uint64
		CertIndex  uint32
	}
	var records []result
	// Use ROW_NUMBER to fetch only the latest record per staking key
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM stake_registration t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		cache.updateReg(
			string(r.StakingKey),
			certRecord{addedSlot: r.AddedSlot, certIndex: r.CertIndex},
		)
	}
	return nil
}

func batchFetchStakeRegistrationDelegation(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey  []byte
		PoolKeyHash []byte
		AddedSlot   uint64
		CertIndex   uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.pool_key_hash, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM stake_registration_delegation t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, pool_key_hash, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		key := string(r.StakingKey)
		rec := certRecord{pool: r.PoolKeyHash, addedSlot: r.AddedSlot, certIndex: r.CertIndex}
		cache.updateReg(key, rec)
		cache.updatePoolDelegation(key, rec)
	}
	return nil
}

func batchFetchStakeVoteRegistrationDelegation(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey  []byte
		PoolKeyHash []byte
		Drep        []byte
		AddedSlot   uint64
		CertIndex   uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.pool_key_hash, t.drep, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM stake_vote_registration_delegation t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, pool_key_hash, drep, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		key := string(r.StakingKey)
		rec := certRecord{pool: r.PoolKeyHash, drep: r.Drep, addedSlot: r.AddedSlot, certIndex: r.CertIndex}
		cache.updateReg(key, rec)
		cache.updatePoolDelegation(key, rec)
		cache.updateDrepDelegation(key, certRecord{drep: r.Drep, addedSlot: r.AddedSlot, certIndex: r.CertIndex})
	}
	return nil
}

func batchFetchVoteRegistrationDelegation(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey []byte
		Drep       []byte
		AddedSlot  uint64
		CertIndex  uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.drep, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM vote_registration_delegation t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, drep, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		key := string(r.StakingKey)
		rec := certRecord{drep: r.Drep, addedSlot: r.AddedSlot, certIndex: r.CertIndex}
		cache.updateReg(key, rec)
		cache.updateDrepDelegation(key, rec)
	}
	return nil
}

func batchFetchRegistration(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey []byte
		AddedSlot  uint64
		CertIndex  uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM registration t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		cache.updateReg(
			string(r.StakingKey),
			certRecord{addedSlot: r.AddedSlot, certIndex: r.CertIndex},
		)
	}
	return nil
}

func batchFetchStakeDeregistration(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey []byte
		AddedSlot  uint64
		CertIndex  uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM stake_deregistration t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		cache.updateDereg(
			string(r.StakingKey),
			certRecord{addedSlot: r.AddedSlot, certIndex: r.CertIndex},
		)
	}
	return nil
}

func batchFetchDeregistration(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey []byte
		AddedSlot  uint64
		CertIndex  uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM deregistration t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		cache.updateDereg(
			string(r.StakingKey),
			certRecord{addedSlot: r.AddedSlot, certIndex: r.CertIndex},
		)
	}
	return nil
}

func batchFetchStakeDelegation(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey  []byte
		PoolKeyHash []byte
		AddedSlot   uint64
		CertIndex   uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.pool_key_hash, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM stake_delegation t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, pool_key_hash, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		cache.updatePoolDelegation(
			string(r.StakingKey),
			certRecord{pool: r.PoolKeyHash, addedSlot: r.AddedSlot, certIndex: r.CertIndex},
		)
	}
	return nil
}

func batchFetchStakeVoteDelegation(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey  []byte
		PoolKeyHash []byte
		Drep        []byte
		AddedSlot   uint64
		CertIndex   uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.pool_key_hash, t.drep, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM stake_vote_delegation t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, pool_key_hash, drep, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		key := string(r.StakingKey)
		cache.updatePoolDelegation(key, certRecord{pool: r.PoolKeyHash, addedSlot: r.AddedSlot, certIndex: r.CertIndex})
		cache.updateDrepDelegation(key, certRecord{drep: r.Drep, addedSlot: r.AddedSlot, certIndex: r.CertIndex})
	}
	return nil
}

func batchFetchVoteDelegation(
	db *gorm.DB,
	stakingKeys [][]byte,
	slot uint64,
	cache *accountCertCache,
) error {
	type result struct {
		StakingKey []byte
		Drep       []byte
		AddedSlot  uint64
		CertIndex  uint32
	}
	var records []result
	query := `
		WITH ranked AS (
			SELECT t.staking_key, t.drep, t.added_slot, c.cert_index,
				ROW_NUMBER() OVER (
					PARTITION BY t.staking_key
					ORDER BY t.added_slot DESC, c.cert_index DESC
				) as rn
			FROM vote_delegation t
			INNER JOIN certs c ON c.id = t.certificate_id
			WHERE t.staking_key IN ? AND t.added_slot <= ?
		)
		SELECT staking_key, drep, added_slot, cert_index
		FROM ranked WHERE rn = 1`
	if err := db.Raw(query, stakingKeys, slot).Scan(&records).Error; err != nil {
		return err
	}
	for _, r := range records {
		cache.updateDrepDelegation(
			string(r.StakingKey),
			certRecord{drep: r.Drep, addedSlot: r.AddedSlot, certIndex: r.CertIndex},
		)
	}
	return nil
}

// RestoreAccountStateAtSlot reverts account delegation state to the given slot.
// For accounts modified after the slot, this restores their Pool and Drep
// delegations to the state they had at the given slot, or marks them inactive
// if they were registered after that slot.
//
// This implementation uses batch fetching to avoid N+1 query patterns:
// instead of querying certificates per-account, it fetches all relevant
// certificates for all affected accounts upfront (one query per table),
// then processes them in memory.
func (d *MetadataStorePostgres) RestoreAccountStateAtSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Find all accounts that were modified after the rollback slot
	var accountsToRestore []models.Account
	if result := db.Where("added_slot > ?", slot).Find(&accountsToRestore); result.Error != nil {
		return result.Error
	}

	if len(accountsToRestore) == 0 {
		return nil
	}

	// Extract staking keys for batch fetching
	stakingKeys := make([][]byte, len(accountsToRestore))
	for i, account := range accountsToRestore {
		stakingKeys[i] = account.StakingKey
	}

	// Batch-fetch all certificates for all affected accounts
	cache, err := batchFetchCerts(db, stakingKeys, slot)
	if err != nil {
		return err
	}

	// Process each account using the cached certificate data
	for _, account := range accountsToRestore {
		key := string(account.StakingKey)

		// Check if this account had any registration before the rollback slot
		latestReg, hasReg := cache.latestReg[key], cache.hasReg[key]

		if !hasReg {
			// Account was registered after rollback slot, delete it
			if result := db.Delete(&account); result.Error != nil {
				return result.Error
			}
			continue
		}

		// Get pool delegation from cache
		var pool []byte
		var poolRec certRecord
		if rec, ok := cache.poolDelegation[key]; ok {
			pool = rec.pool
			poolRec = rec
		}

		// Get DRep delegation from cache
		var drep []byte
		var drepRec certRecord
		if rec, ok := cache.drepDelegation[key]; ok {
			drep = rec.drep
			drepRec = rec
		}

		// Get deregistration info from cache
		latestDereg, hasDereg := cache.latestDereg[key], cache.hasDereg[key]

		// Account is active if either:
		// - There is no deregistration, or
		// - The most recent registration is after the most recent deregistration
		active := !hasDereg || latestReg.isMoreRecent(latestDereg)

		// Compute the actual last modification slot as the max of all relevant events
		lastModSlot := latestReg.addedSlot
		if hasDereg && latestDereg.addedSlot > lastModSlot {
			lastModSlot = latestDereg.addedSlot
		}
		// Only consider pool/drep slots if they were found in the cache
		if _, hasPool := cache.poolDelegation[key]; hasPool && poolRec.addedSlot > lastModSlot {
			lastModSlot = poolRec.addedSlot
		}
		if _, hasDrep := cache.drepDelegation[key]; hasDrep && drepRec.addedSlot > lastModSlot {
			lastModSlot = drepRec.addedSlot
		}

		// Update the account with restored state
		if result := db.Model(&account).Updates(map[string]any{
			"pool":       pool,
			"drep":       drep,
			"active":     active,
			"added_slot": lastModSlot,
		}); result.Error != nil {
			return result.Error
		}
	}

	return nil
}
