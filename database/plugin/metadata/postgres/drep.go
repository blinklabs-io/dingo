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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetDrep gets a drep
func (d *MetadataStorePostgres) GetDrep(
	cred []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Drep, error) {
	var drep models.Drep
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	if !includeInactive {
		db = db.Where("active = ?", true)
	}
	if result := db.First(&drep, "credential = ?", cred); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &drep, nil
}

// GetActiveDreps retrieves all active DReps.
func (d *MetadataStorePostgres) GetActiveDreps(
	txn types.Txn,
) ([]*models.Drep, error) {
	var dreps []*models.Drep
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where("active = ?", true).Find(&dreps); result.Error != nil {
		return nil, result.Error
	}
	return dreps, nil
}

// SetDrep saves a drep
func (d *MetadataStorePostgres) SetDrep(
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn types.Txn,
) error {
	tmpItem := models.Drep{
		Credential: cred,
		AddedSlot:  slot,
		AnchorURL:  url,
		AnchorHash: hash,
		Active:     active,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{{Name: "credential"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"added_slot",
			"anchor_url",
			"anchor_hash",
			"active",
		}),
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

// GetDRepVotingPower calculates the voting power for a DRep by summing the
// stake of all accounts delegated to it. Uses the current live UTxO set
// (deleted_slot = 0) for the calculation.
//
// TODO: This implementation uses the current live UTxO set as an
// approximation. A future implementation should accept an epoch
// parameter and use epoch-based stake snapshots for accurate
// voting power at a specific point in time.
func (d *MetadataStorePostgres) GetDRepVotingPower(
	drepCredential []byte,
	txn types.Txn,
) (uint64, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, err
	}
	var totalStake uint64
	if err := db.Raw(`
		SELECT COALESCE(SUM(CAST(u.amount AS BIGINT)), 0)
		FROM utxo u
		WHERE u.deleted_slot = 0
		AND u.staking_key IN (
			SELECT a.staking_key
			FROM account a
			WHERE a.drep = $1 AND a.active = true
		)
	`, drepCredential).Scan(&totalStake).Error; err != nil {
		return 0, fmt.Errorf("get drep voting power: %w", err)
	}
	return totalStake, nil
}

// UpdateDRepActivity updates the DRep's last activity epoch and
// recalculates the expiry epoch.
// Returns ErrDrepActivityNotUpdated if no matching DRep record was found.
func (d *MetadataStorePostgres) UpdateDRepActivity(
	drepCredential []byte,
	activityEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	expiryEpoch := activityEpoch + inactivityPeriod
	result := db.Model(&models.Drep{}).
		Where("credential = ?", drepCredential).
		Updates(map[string]any{
			"last_activity_epoch": activityEpoch,
			"expiry_epoch":        expiryEpoch,
		})
	if result.Error != nil {
		return fmt.Errorf("update drep activity: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return models.ErrDrepActivityNotUpdated
	}
	return nil
}

// GetExpiredDReps retrieves all active DReps whose expiry epoch is at
// or before the given epoch.
func (d *MetadataStorePostgres) GetExpiredDReps(
	epoch uint64,
	txn types.Txn,
) ([]*models.Drep, error) {
	var dreps []*models.Drep
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	if result := db.Where(
		"active = ? AND expiry_epoch > 0 AND expiry_epoch <= ?",
		true,
		epoch,
	).Find(&dreps); result.Error != nil {
		return nil, fmt.Errorf("get expired dreps: %w", result.Error)
	}
	return dreps, nil
}

// GetCommitteeActiveCount returns the number of active (non-resigned)
// committee members.
func (d *MetadataStorePostgres) GetCommitteeActiveCount(
	txn types.Txn,
) (int, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, err
	}
	var count int64
	if result := db.Raw(`
		SELECT COUNT(*) FROM (
			SELECT DISTINCT a.cold_credential
			FROM auth_committee_hot a
			INNER JOIN certs c
				ON c.id = a.certificate_id
			WHERE NOT EXISTS (
				SELECT 1
				FROM auth_committee_hot a2
				INNER JOIN certs c2
					ON c2.id = a2.certificate_id
				WHERE a2.cold_credential = a.cold_credential
				AND (
					a2.added_slot > a.added_slot
					OR (a2.added_slot = a.added_slot
						AND c2.cert_index > c.cert_index)
				)
			)
			AND NOT EXISTS (
				SELECT 1
				FROM resign_committee_cold r
				INNER JOIN certs cr
					ON cr.id = r.certificate_id
				WHERE r.cold_credential = a.cold_credential
				AND (
					r.added_slot > a.added_slot
					OR (r.added_slot = a.added_slot
						AND cr.cert_index > c.cert_index)
				)
			)
		) active_members
	`).Scan(&count); result.Error != nil {
		return 0, fmt.Errorf(
			"get committee active count: %w",
			result.Error,
		)
	}
	return int(count), nil
}

// drepCertRecord holds fields from a DRep certificate for batch processing
// during DRep state restoration.
type drepCertRecord struct {
	anchorURL  string
	anchorHash []byte
	addedSlot  uint64
	certIndex  uint32
}

// drepCertCache holds batch-fetched certificate data for all DReps being restored.
type drepCertCache struct {
	registration   map[string]drepCertRecord
	hasReg         map[string]bool
	deregistration map[string]drepCertRecord
	hasDereg       map[string]bool
	update         map[string]drepCertRecord
	hasUpdate      map[string]bool
}

// batchFetchDrepCerts fetches all relevant certificates for the given DRep credentials
// at or before the given slot.
func batchFetchDrepCerts(
	db *gorm.DB,
	credentials [][]byte,
	slot uint64,
) (*drepCertCache, error) {
	cache := &drepCertCache{
		registration:   make(map[string]drepCertRecord, len(credentials)),
		hasReg:         make(map[string]bool, len(credentials)),
		deregistration: make(map[string]drepCertRecord, len(credentials)),
		hasDereg:       make(map[string]bool, len(credentials)),
		update:         make(map[string]drepCertRecord, len(credentials)),
		hasUpdate:      make(map[string]bool, len(credentials)),
	}

	// Fetch registrations
	{
		type result struct {
			DrepCredential []byte
			AnchorURL      string `gorm:"column:anchor_url"`
			AnchorHash     []byte
			AddedSlot      uint64
			CertIndex      uint32
		}
		var records []result
		query := `
			WITH ranked AS (
				SELECT t.drep_credential, t.anchor_url, t.anchor_hash, t.added_slot, c.cert_index,
					ROW_NUMBER() OVER (
						PARTITION BY t.drep_credential
						ORDER BY t.added_slot DESC, c.cert_index DESC
					) as rn
				FROM registration_drep t
				INNER JOIN certs c ON c.id = t.certificate_id
				WHERE t.drep_credential IN ? AND t.added_slot <= ?
			)
			SELECT drep_credential, anchor_url, anchor_hash, added_slot, cert_index
			FROM ranked WHERE rn = 1`
		if err := db.Raw(query, credentials, slot).Scan(&records).Error; err != nil {
			return nil, err
		}
		for _, r := range records {
			key := string(r.DrepCredential)
			cache.registration[key] = drepCertRecord{
				anchorURL:  r.AnchorURL,
				anchorHash: r.AnchorHash,
				addedSlot:  r.AddedSlot,
				certIndex:  r.CertIndex,
			}
			cache.hasReg[key] = true
		}
	}

	// Fetch deregistrations
	{
		type result struct {
			DrepCredential []byte
			AddedSlot      uint64
			CertIndex      uint32
		}
		var records []result
		query := `
			WITH ranked AS (
				SELECT t.drep_credential, t.added_slot, c.cert_index,
					ROW_NUMBER() OVER (
						PARTITION BY t.drep_credential
						ORDER BY t.added_slot DESC, c.cert_index DESC
					) as rn
				FROM deregistration_drep t
				INNER JOIN certs c ON c.id = t.certificate_id
				WHERE t.drep_credential IN ? AND t.added_slot <= ?
			)
			SELECT drep_credential, added_slot, cert_index
			FROM ranked WHERE rn = 1`
		if err := db.Raw(query, credentials, slot).Scan(&records).Error; err != nil {
			return nil, err
		}
		for _, r := range records {
			key := string(r.DrepCredential)
			cache.deregistration[key] = drepCertRecord{
				addedSlot: r.AddedSlot,
				certIndex: r.CertIndex,
			}
			cache.hasDereg[key] = true
		}
	}

	// Fetch updates
	{
		type result struct {
			Credential []byte
			AnchorURL  string `gorm:"column:anchor_url"`
			AnchorHash []byte
			AddedSlot  uint64
			CertIndex  uint32
		}
		var records []result
		query := `
			WITH ranked AS (
				SELECT t.credential, t.anchor_url, t.anchor_hash, t.added_slot, c.cert_index,
					ROW_NUMBER() OVER (
						PARTITION BY t.credential
						ORDER BY t.added_slot DESC, c.cert_index DESC
					) as rn
				FROM update_drep t
				INNER JOIN certs c ON c.id = t.certificate_id
				WHERE t.credential IN ? AND t.added_slot <= ?
			)
			SELECT credential, anchor_url, anchor_hash, added_slot, cert_index
			FROM ranked WHERE rn = 1`
		if err := db.Raw(query, credentials, slot).Scan(&records).Error; err != nil {
			return nil, err
		}
		for _, r := range records {
			key := string(r.Credential)
			cache.update[key] = drepCertRecord{
				anchorURL:  r.AnchorURL,
				anchorHash: r.AnchorHash,
				addedSlot:  r.AddedSlot,
				certIndex:  r.CertIndex,
			}
			cache.hasUpdate[key] = true
		}
	}

	return cache, nil
}

// RestoreDrepStateAtSlot reverts DRep state to the given slot.
// DReps that have no registrations at or before the given slot are deleted.
// DReps that have prior registrations have their Active status and anchor
// data restored based on the most recent certificate at or before the slot.
//
// This implementation uses batch fetching to avoid N+1 query patterns:
// instead of querying certificates per-DRep, it fetches all relevant
// certificates for all affected DReps upfront (one query per table),
// then processes them in memory.
func (d *MetadataStorePostgres) RestoreDrepStateAtSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Phase 1: Delete DReps that have no registration certificates at or before
	// the rollback slot. These DReps were first registered after the rollback
	// point and should not exist in the restored state.
	drepsWithNoValidRegsSubquery := db.Model(&models.Drep{}).
		Select("drep.id").
		Where("added_slot > ?", slot).
		Where(
			"NOT EXISTS (?)",
			db.Model(&models.RegistrationDrep{}).
				Select("1").
				Where("registration_drep.drep_credential = drep.credential AND registration_drep.added_slot <= ?", slot),
		)

	if result := db.Where(
		"id IN (?)",
		drepsWithNoValidRegsSubquery,
	).Delete(&models.Drep{}); result.Error != nil {
		return result.Error
	}

	// Phase 2: Restore state for DReps that have at least one registration
	// certificate at or before the rollback slot.
	var drepsToRestore []models.Drep
	if result := db.Where("added_slot > ?", slot).Find(&drepsToRestore); result.Error != nil {
		return result.Error
	}

	if len(drepsToRestore) == 0 {
		return nil
	}

	// Extract credentials for batch fetching
	credentials := make([][]byte, len(drepsToRestore))
	for i, drep := range drepsToRestore {
		credentials[i] = drep.Credential
	}

	// Batch-fetch all certificates for all affected DReps
	cache, err := batchFetchDrepCerts(db, credentials, slot)
	if err != nil {
		return err
	}

	// Process each DRep using the cached certificate data
	for _, drep := range drepsToRestore {
		key := string(drep.Credential)

		// Get registration from cache (must exist due to Phase 1 deletion)
		lastReg, hasRegAtSlot := cache.registration[key], cache.hasReg[key]
		if !hasRegAtSlot {
			// This indicates database inconsistency: Phase 1 should have deleted
			// any DRep without a registration cert at or before the rollback slot.
			return fmt.Errorf(
				"DRep %x has no registration cert at or before slot %d but wasn't deleted in Phase 1",
				drep.Credential,
				slot,
			)
		}

		// Determine the correct state by processing certificates in order.
		// Start with registration state (DRep is active with registration's anchor data)
		active := true
		anchorURL := lastReg.anchorURL
		anchorHash := lastReg.anchorHash
		latestSlot := lastReg.addedSlot
		latestCertIndex := lastReg.certIndex
		latestWasDereg := false

		// Apply deregistration if it's more recent than registration
		if cache.hasDereg[key] {
			lastDereg := cache.deregistration[key]
			if lastDereg.addedSlot > latestSlot ||
				(lastDereg.addedSlot == latestSlot && lastDereg.certIndex > latestCertIndex) {
				active = false
				latestSlot = lastDereg.addedSlot
				latestCertIndex = lastDereg.certIndex
				anchorURL = ""
				anchorHash = nil
				latestWasDereg = true
			}
		}

		// Apply update certificate only if it's the most recent event AND the
		// DRep is still active. Per CIP-1694 and Cardano protocol rules, an update
		// certificate is only valid for registered DReps. If a DRep deregisters,
		// their update history is effectively cleared.
		if cache.hasUpdate[key] && !latestWasDereg {
			lastUpdate := cache.update[key]
			if lastUpdate.addedSlot > latestSlot ||
				(lastUpdate.addedSlot == latestSlot && lastUpdate.certIndex > latestCertIndex) {
				anchorURL = lastUpdate.anchorURL
				anchorHash = lastUpdate.anchorHash
				latestSlot = lastUpdate.addedSlot
			}
		}

		// Update the DRep with restored state.
		// Reset LastActivityEpoch and ExpiryEpoch to 0 since we cannot
		// reliably reconstruct these values from certificate data alone
		// during rollback. They will be recalculated as new activity occurs.
		if result := db.Model(&drep).Updates(map[string]any{
			"anchor_url":          anchorURL,
			"anchor_hash":         anchorHash,
			"active":              active,
			"added_slot":          latestSlot,
			"last_activity_epoch": 0,
			"expiry_epoch":        0,
		}); result.Error != nil {
			return result.Error
		}
	}

	return nil
}
