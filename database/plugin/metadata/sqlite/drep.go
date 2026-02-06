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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// drepCertRecord holds certificate data for batch processing during DRep restoration.
// Includes cert_index for same-slot disambiguation.
type drepCertRecord struct {
	addedSlot  uint64
	certIndex  uint32
	anchorUrl  string
	anchorHash []byte
}

// isMoreRecent checks if this certificate is more recent than the other.
// When slots are equal, cert_index determines order (higher = later in transaction).
func (r drepCertRecord) isMoreRecent(other drepCertRecord) bool {
	if r.addedSlot > other.addedSlot {
		return true
	}
	if r.addedSlot == other.addedSlot && r.certIndex > other.certIndex {
		return true
	}
	return false
}

// drepCertCache holds batch-fetched certificate data for all DReps being restored.
type drepCertCache struct {
	// Maps credential (as string) to the most recent registration
	registration map[string]drepCertRecord
	hasReg       map[string]bool

	// Maps credential to the most recent deregistration
	deregistration map[string]drepCertRecord
	hasDereg       map[string]bool

	// Maps credential to the most recent update
	update    map[string]drepCertRecord
	hasUpdate map[string]bool
}

// newDrepCertCache creates an empty DRep certificate cache.
func newDrepCertCache(capacity int) *drepCertCache {
	return &drepCertCache{
		registration:   make(map[string]drepCertRecord, capacity),
		hasReg:         make(map[string]bool, capacity),
		deregistration: make(map[string]drepCertRecord, capacity),
		hasDereg:       make(map[string]bool, capacity),
		update:         make(map[string]drepCertRecord, capacity),
		hasUpdate:      make(map[string]bool, capacity),
	}
}

// batchFetchDrepCerts fetches all relevant certificates for the given credentials
// at or before the given slot. Uses one query per certificate table with JOIN
// to get cert_index for same-slot disambiguation. Chunks queries to avoid
// exceeding SQLite bind variable limits.
func batchFetchDrepCerts(
	db *gorm.DB,
	credentials [][]byte,
	slot uint64,
) (*drepCertCache, error) {
	cache := newDrepCertCache(len(credentials))

	// Process credentials in chunks to avoid SQLite bind variable limits
	for start := 0; start < len(credentials); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(credentials))
		credChunk := credentials[start:end]

		// Fetch registration certificates with cert_index
		type regResult struct {
			DrepCredential []byte
			AddedSlot      uint64
			CertIndex      uint32
			AnchorUrl      string
			AnchorHash     []byte
		}
		var regRecords []regResult
		if err := db.Table("registration_drep").
			Select("registration_drep.drep_credential, registration_drep.added_slot, registration_drep.anchor_url, registration_drep.anchor_hash, certs.cert_index").
			Joins("INNER JOIN certs ON certs.id = registration_drep.certificate_id").
			Where("drep_credential IN ? AND registration_drep.added_slot <= ?", credChunk, slot).
			Find(&regRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range regRecords {
			key := string(r.DrepCredential)
			rec := drepCertRecord{
				addedSlot:  r.AddedSlot,
				certIndex:  r.CertIndex,
				anchorUrl:  r.AnchorUrl,
				anchorHash: r.AnchorHash,
			}
			if !cache.hasReg[key] || rec.isMoreRecent(cache.registration[key]) {
				cache.registration[key] = rec
				cache.hasReg[key] = true
			}
		}

		// Fetch deregistration certificates with cert_index
		type deregResult struct {
			DrepCredential []byte
			AddedSlot      uint64
			CertIndex      uint32
		}
		var deregRecords []deregResult
		if err := db.Table("deregistration_drep").
			Select("deregistration_drep.drep_credential, deregistration_drep.added_slot, certs.cert_index").
			Joins("INNER JOIN certs ON certs.id = deregistration_drep.certificate_id").
			Where("drep_credential IN ? AND deregistration_drep.added_slot <= ?", credChunk, slot).
			Find(&deregRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range deregRecords {
			key := string(r.DrepCredential)
			rec := drepCertRecord{
				addedSlot: r.AddedSlot,
				certIndex: r.CertIndex,
			}
			if !cache.hasDereg[key] ||
				rec.isMoreRecent(cache.deregistration[key]) {
				cache.deregistration[key] = rec
				cache.hasDereg[key] = true
			}
		}

		// Fetch update certificates with cert_index
		type updateResult struct {
			Credential []byte
			AddedSlot  uint64
			CertIndex  uint32
			AnchorUrl  string
			AnchorHash []byte
		}
		var updateRecords []updateResult
		if err := db.Table("update_drep").
			Select("update_drep.credential, update_drep.added_slot, update_drep.anchor_url, update_drep.anchor_hash, certs.cert_index").
			Joins("INNER JOIN certs ON certs.id = update_drep.certificate_id").
			Where("credential IN ? AND update_drep.added_slot <= ?", credChunk, slot).
			Find(&updateRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range updateRecords {
			key := string(r.Credential)
			rec := drepCertRecord{
				addedSlot:  r.AddedSlot,
				certIndex:  r.CertIndex,
				anchorUrl:  r.AnchorUrl,
				anchorHash: r.AnchorHash,
			}
			if !cache.hasUpdate[key] || rec.isMoreRecent(cache.update[key]) {
				cache.update[key] = rec
				cache.hasUpdate[key] = true
			}
		}
	}

	return cache, nil
}

// GetDrep gets a drep
func (d *MetadataStoreSqlite) GetDrep(
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

// RestoreDrepStateAtSlot reverts DRep state to the given slot.
//
// This function handles two cases for DReps modified after the rollback slot:
//  1. DReps with no registration certificates at or before the slot are deleted
//     (they didn't exist at that point in the chain).
//  2. DReps with prior registrations have their state restored by examining
//     all relevant certificates (registration, deregistration, update) and
//     determining the correct Active status and anchor data.
//
// The Active status follows these rules:
//   - A registration certificate activates a DRep
//   - A deregistration certificate deactivates a DRep
//   - An update certificate can modify anchor data but cannot reactivate a
//     deregistered DRep (per Cardano protocol rules)
//
// The added_slot field is set to the slot of the most recent effective
// certificate that modified the DRep's state at or before the rollback slot.
//
// This implementation uses batch fetching to avoid N+1 query patterns:
// instead of querying certificates per-DRep, it fetches all relevant
// certificates for all affected DReps upfront (one query per table),
// then processes them in memory.
func (d *MetadataStoreSqlite) RestoreDrepStateAtSlot(
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
		anchorUrl := lastReg.anchorUrl
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
				anchorUrl = ""
				anchorHash = nil
				latestWasDereg = true
			}
		}

		// Apply update certificate only if it's the most recent event AND the
		// DRep is still active. Per CIP-1694 and Cardano protocol rules, an update
		// certificate is only valid for registered DReps. If a DRep deregisters,
		// any subsequent update certificates are ignored - the DRep must re-register
		// to become active again. Therefore, we skip updates when latestWasDereg is true.
		if cache.hasUpdate[key] && !latestWasDereg {
			lastUpdate := cache.update[key]
			if lastUpdate.addedSlot > latestSlot ||
				(lastUpdate.addedSlot == latestSlot && lastUpdate.certIndex > latestCertIndex) {
				anchorUrl = lastUpdate.anchorUrl
				anchorHash = lastUpdate.anchorHash
				latestSlot = lastUpdate.addedSlot
			}
		}

		// Update the DRep record with the restored state.
		if result := db.Model(&drep).Updates(map[string]any{
			"active":      active,
			"anchor_url":  anchorUrl,
			"anchor_hash": anchorHash,
			"added_slot":  latestSlot,
		}); result.Error != nil {
			return result.Error
		}
	}

	return nil
}

// GetActiveDreps retrieves all active DReps.
func (d *MetadataStoreSqlite) GetActiveDreps(
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
func (d *MetadataStoreSqlite) SetDrep(
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
		AnchorUrl:  url,
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
