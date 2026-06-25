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
// On-chain ordering is (added_slot DESC, block_index DESC, cert_index DESC)
// because cert_index resets per tx, so block_index is required to
// disambiguate across txs in the same block.
type drepCertRecord struct {
	addedSlot  uint64
	blockIndex uint32
	certIndex  uint32
	anchorURL  string
	anchorHash []byte
}

// isMoreRecent checks if this certificate is more recent than the other.
// Ordering follows (added_slot, block_index, cert_index): block_index
// orders txs within a block, cert_index orders certs within a tx.
func (r drepCertRecord) isMoreRecent(other drepCertRecord) bool {
	if r.addedSlot != other.addedSlot {
		return r.addedSlot > other.addedSlot
	}
	if r.blockIndex != other.blockIndex {
		return r.blockIndex > other.blockIndex
	}
	return r.certIndex > other.certIndex
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

const getDRepVotingPowerBatchSQL = `
	SELECT a.drep AS drep, a.drep_type AS credential_tag,
		   COALESCE(SUM(
			   COALESCE(u.utxo_sum, 0)
			   + COALESCE(CAST(a.reward AS INTEGER), 0)
		   ), 0) AS stake
	FROM account a
	LEFT JOIN (
			SELECT ax.drep_type, ax.credential_tag, ax.staking_key,
				   COALESCE(SUM(CAST(utxo.amount AS INTEGER)), 0) AS utxo_sum
			FROM account ax
			JOIN utxo INDEXED BY ` + utxoStakingLiveAmountIndex + `
			         ON utxo.credential_tag = ax.credential_tag
			         AND utxo.staking_key = ax.staking_key
			         AND utxo.deleted_slot = 0
		WHERE ax.active = 1 AND ax.drep IN ?
		GROUP BY ax.drep_type, ax.credential_tag, ax.staking_key
	) u ON u.credential_tag = a.credential_tag
		AND u.staking_key = a.staking_key
		AND u.drep_type = a.drep_type
	WHERE a.active = 1 AND a.drep IN ?
	GROUP BY a.drep, a.drep_type
`

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

// drepCertCacheKey returns a composite cache key combining credential_tag and
// credential so key-hash and script-hash DReps with the same hash are tracked
// independently.
func drepCertCacheKey(credentialTag uint8, credential []byte) string {
	return string([]byte{credentialTag}) + string(credential)
}

// batchFetchDrepCerts fetches all relevant certificates for the given credential refs
// at or before the given slot. Uses one query per certificate table with JOIN
// to get cert_index for same-slot disambiguation. Chunks queries to avoid
// exceeding SQLite bind variable limits.
//
// The SQL IN clause filters on staking_key hash only (not credential_tag) for
// simplicity; the cache is post-filtered to entries whose composite (tag, hash)
// key matches one of the input refs, so credentials sharing a 28-byte hash but
// differing in tag are kept isolated.
func batchFetchDrepCerts(
	db *gorm.DB,
	refs []models.StakeCredentialRef,
	slot uint64,
) (*drepCertCache, error) {
	cache := newDrepCertCache(len(refs))

	// Build unique credential list for the SQL IN clause and a requested
	// set (by composite cache key) for post-filtering.
	requested := make(map[string]struct{}, len(refs))
	seenHash := make(map[string]struct{}, len(refs))
	var credentials [][]byte
	for _, ref := range refs {
		requested[drepCertCacheKey(ref.Tag, ref.Key)] = struct{}{}
		h := string(ref.Key)
		if _, ok := seenHash[h]; !ok {
			seenHash[h] = struct{}{}
			credentials = append(credentials, ref.Key)
		}
	}

	if len(credentials) == 0 {
		return cache, nil
	}

	// Process credentials in chunks to avoid SQLite bind variable limits
	for start := 0; start < len(credentials); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(credentials))
		credChunk := credentials[start:end]

		// Fetch registration certificates with block_index + cert_index.
		// LEFT JOIN certs so genesis registration_drep rows (no certs
		// row, certificate_id=0) still surface; cert_index/block_index
		// default to 0, which is safe because slot 0 precedes every
		// real cert. block_index orders txs within a block; cert_index
		// orders certs within a tx.
		type regResult struct {
			DrepCredential []byte
			CredentialTag  uint8
			AddedSlot      uint64
			BlockIndex     uint32
			CertIndex      uint32
			AnchorURL      string `gorm:"column:anchor_url"`
			AnchorHash     []byte
		}
		var regRecords []regResult
		if err := db.Table("registration_drep").
			Select(`registration_drep.credential_tag, registration_drep.drep_credential, registration_drep.added_slot, registration_drep.anchor_url, registration_drep.anchor_hash, COALESCE("transaction".block_index, 0) AS block_index, COALESCE(certs.cert_index, 0) AS cert_index`).
			Joins("LEFT JOIN certs ON certs.id = registration_drep.certificate_id").
			Joins(`LEFT JOIN "transaction" ON "transaction".id = certs.transaction_id`).
			Where("drep_credential IN ? AND registration_drep.added_slot <= ?", credChunk, slot).
			Find(&regRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range regRecords {
			key := drepCertCacheKey(r.CredentialTag, r.DrepCredential)
			rec := drepCertRecord{
				addedSlot:  r.AddedSlot,
				blockIndex: r.BlockIndex,
				certIndex:  r.CertIndex,
				anchorURL:  r.AnchorURL,
				anchorHash: r.AnchorHash,
			}
			if !cache.hasReg[key] || rec.isMoreRecent(cache.registration[key]) {
				cache.registration[key] = rec
				cache.hasReg[key] = true
			}
		}

		// Fetch deregistration certificates with block_index + cert_index.
		type deregResult struct {
			DrepCredential []byte
			CredentialTag  uint8
			AddedSlot      uint64
			BlockIndex     uint32
			CertIndex      uint32
		}
		var deregRecords []deregResult
		if err := db.Table("deregistration_drep").
			Select(`deregistration_drep.credential_tag, deregistration_drep.drep_credential, deregistration_drep.added_slot, COALESCE("transaction".block_index, 0) AS block_index, certs.cert_index`).
			Joins("INNER JOIN certs ON certs.id = deregistration_drep.certificate_id").
			Joins(`LEFT JOIN "transaction" ON "transaction".id = certs.transaction_id`).
			Where("drep_credential IN ? AND deregistration_drep.added_slot <= ?", credChunk, slot).
			Find(&deregRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range deregRecords {
			key := drepCertCacheKey(r.CredentialTag, r.DrepCredential)
			rec := drepCertRecord{
				addedSlot:  r.AddedSlot,
				blockIndex: r.BlockIndex,
				certIndex:  r.CertIndex,
			}
			if !cache.hasDereg[key] ||
				rec.isMoreRecent(cache.deregistration[key]) {
				cache.deregistration[key] = rec
				cache.hasDereg[key] = true
			}
		}

		// Fetch update certificates with block_index + cert_index.
		type updateResult struct {
			Credential    []byte
			CredentialTag uint8
			AddedSlot     uint64
			BlockIndex    uint32
			CertIndex     uint32
			AnchorURL     string `gorm:"column:anchor_url"`
			AnchorHash    []byte
		}
		var updateRecords []updateResult
		if err := db.Table("update_drep").
			Select(`update_drep.credential_tag, update_drep.credential, update_drep.added_slot, update_drep.anchor_url, update_drep.anchor_hash, COALESCE("transaction".block_index, 0) AS block_index, certs.cert_index`).
			Joins("INNER JOIN certs ON certs.id = update_drep.certificate_id").
			Joins(`LEFT JOIN "transaction" ON "transaction".id = certs.transaction_id`).
			Where("credential IN ? AND update_drep.added_slot <= ?", credChunk, slot).
			Find(&updateRecords).Error; err != nil {
			return nil, err
		}
		for _, r := range updateRecords {
			key := drepCertCacheKey(r.CredentialTag, r.Credential)
			rec := drepCertRecord{
				addedSlot:  r.AddedSlot,
				blockIndex: r.BlockIndex,
				certIndex:  r.CertIndex,
				anchorURL:  r.AnchorURL,
				anchorHash: r.AnchorHash,
			}
			if !cache.hasUpdate[key] || rec.isMoreRecent(cache.update[key]) {
				cache.update[key] = rec
				cache.hasUpdate[key] = true
			}
		}
	}

	// Remove entries for DRep credential variants not present in the input refs.
	for key := range cache.registration {
		if _, ok := requested[key]; !ok {
			delete(cache.registration, key)
			delete(cache.hasReg, key)
		}
	}
	for key := range cache.deregistration {
		if _, ok := requested[key]; !ok {
			delete(cache.deregistration, key)
			delete(cache.hasDereg, key)
		}
	}
	for key := range cache.update {
		if _, ok := requested[key]; !ok {
			delete(cache.update, key)
			delete(cache.hasUpdate, key)
		}
	}

	return cache, nil
}

// GetDrep gets a drep by hash only (no tag filter). Used for the protocol
// validation path where only a Blake2b224 hash is available.
func (d *MetadataStoreSqlite) GetDrep(
	cred []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Drep, error) {
	var drep models.Drep
	db, err := d.resolveReadDB(txn)
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

// GetDrepByCredential gets a drep by the full credential identity (tag + hash).
func (d *MetadataStoreSqlite) GetDrepByCredential(
	credentialTag uint8,
	cred []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Drep, error) {
	var drep models.Drep
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	if !includeInactive {
		db = db.Where("active = ?", true)
	}
	if result := db.First(
		&drep,
		"credential_tag = ? AND credential = ?",
		credentialTag, cred,
	); result.Error != nil {
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
				Where("registration_drep.credential_tag = drep.credential_tag AND registration_drep.drep_credential = drep.credential AND registration_drep.added_slot <= ?", slot),
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

	// Extract credential refs for batch fetching
	drepRefs := make([]models.StakeCredentialRef, len(drepsToRestore))
	for i, drep := range drepsToRestore {
		drepRefs[i] = models.NewStakeCredentialRef(drep.CredentialTag, drep.Credential)
	}

	// Batch-fetch all certificates for all affected DReps
	cache, err := batchFetchDrepCerts(db, drepRefs, slot)
	if err != nil {
		return err
	}

	// Process each DRep using the cached certificate data
	for _, drep := range drepsToRestore {
		key := drepCertCacheKey(drep.CredentialTag, drep.Credential)

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
		// Start with registration state (DRep is active with registration's
		// anchor data) and track the latest event as a drepCertRecord so
		// cross-type comparisons go through isMoreRecent — i.e., use the
		// full (added_slot, block_index, cert_index) ordering. Without
		// block_index, a same-slot deregistration in a later tx could lose
		// to a registration in an earlier tx (both with cert_index=0).
		active := true
		anchorURL := lastReg.anchorURL
		anchorHash := lastReg.anchorHash
		latest := lastReg
		latestWasDereg := false

		// Apply deregistration if it's more recent than the current latest.
		if cache.hasDereg[key] {
			lastDereg := cache.deregistration[key]
			if lastDereg.isMoreRecent(latest) {
				active = false
				anchorURL = ""
				anchorHash = nil
				latest = lastDereg
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
			if lastUpdate.isMoreRecent(latest) {
				anchorURL = lastUpdate.anchorURL
				anchorHash = lastUpdate.anchorHash
				latest = lastUpdate
			}
		}

		// Activity/expiry can't be reliably reconstructed from cert
		// history alone (voting activity is tracked elsewhere and bumps
		// expiry_epoch). For on-chain registrations, reset to 0 so they
		// are re-seeded by subsequent activity. For genesis-rooted
		// DReps (most-recent registration at slot 0), preserve the
		// existing values — they were set from the Conway genesis
		// config at bootstrap, and zeroing them would silently turn
		// the DRep into a "never-expiring" one (drepActiveAtEpoch
		// treats expiry_epoch=0 as unbounded), inflating governance
		// tallies.
		expiryEpoch := uint64(0)
		lastActivityEpoch := uint64(0)
		if lastReg.addedSlot == 0 {
			expiryEpoch = drep.ExpiryEpoch
			lastActivityEpoch = drep.LastActivityEpoch
		}

		if result := db.Model(&drep).Updates(map[string]any{
			"active":              active,
			"anchor_url":          anchorURL,
			"anchor_hash":         anchorHash,
			"added_slot":          latest.addedSlot,
			"last_activity_epoch": lastActivityEpoch,
			"expiry_epoch":        expiryEpoch,
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
	db, err := d.resolveReadDB(txn)
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
	credentialTag uint8,
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn types.Txn,
) error {
	tmpItem := models.Drep{
		CredentialTag: credentialTag,
		Credential:    cred,
		AddedSlot:     slot,
		AnchorURL:     url,
		AnchorHash:    hash,
		Active:        active,
	}
	onConflict := clause.OnConflict{
		Columns: []clause.Column{
			{Name: "credential_tag"},
			{Name: "credential"},
		},
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

// InsertDrepIfAbsent inserts a DRep row only when no record exists for
// the given credential. Existing rows are left untouched so the repair
// path cannot clobber real registration metadata.
func (d *MetadataStoreSqlite) InsertDrepIfAbsent(
	credentialTag uint8,
	cred []byte,
	slot uint64,
	url string,
	hash []byte,
	active bool,
	txn types.Txn,
) error {
	tmpItem := models.Drep{
		CredentialTag: credentialTag,
		Credential:    cred,
		AddedSlot:     slot,
		AnchorURL:     url,
		AnchorHash:    hash,
		Active:        active,
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Clauses(clause.OnConflict{DoNothing: true}).
		Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}

// GetDRepVotingPower calculates the voting power for a DRep by summing the
// current stake of all accounts delegated to it, including live UTxO balance
// plus reward-account balance.
//
// TODO: This implementation uses current live balances as an
// approximation. A future implementation should accept an epoch
// parameter and use epoch-based stake snapshots for accurate
// voting power at a specific point in time.
//
// The voting power is computed by:
// 1. Finding all accounts whose drep field matches the DRep credential
// 2. Summing those accounts' UTxO values
//
// Returns 0 if the DRep has no delegators.
func (d *MetadataStoreSqlite) GetDRepVotingPower(
	credentialTag uint8,
	drepCredential []byte,
	txn types.Txn,
) (uint64, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return 0, err
	}

	var totalStake uint64
	if err := db.Raw(`
		SELECT COALESCE(SUM(
				   COALESCE(u.utxo_sum, 0)
				   + COALESCE(CAST(a.reward AS INTEGER), 0)
			   ), 0)
		FROM account a
		LEFT JOIN (
			SELECT credential_tag, staking_key,
				   COALESCE(SUM(CAST(amount AS INTEGER)), 0) AS utxo_sum
			FROM utxo
			WHERE deleted_slot = 0
			  AND EXISTS (
				  SELECT 1 FROM account ax
				  WHERE ax.credential_tag = utxo.credential_tag
				    AND ax.staking_key = utxo.staking_key
				    AND ax.drep = ? AND ax.drep_type = ? AND ax.active = 1
			  )
			GROUP BY credential_tag, staking_key
		) u ON u.credential_tag = a.credential_tag
			AND u.staking_key = a.staking_key
		WHERE a.drep = ? AND a.drep_type = ? AND a.active = 1
	`, drepCredential, credentialTag, drepCredential, credentialTag).Scan(&totalStake).Error; err != nil {
		return 0, fmt.Errorf("get drep voting power: %w", err)
	}

	return totalStake, nil
}

// GetDRepVotingPowerBatch returns a credential-to-voting-power map for
// the given DRep credentials in a single query. Credentials with no
// active delegators are omitted; credentials with active delegators whose
// delegated stake sums to zero are present with stake = 0. This is the
// batch variant of GetDRepVotingPower and avoids N+1 queries when
// tallying governance votes across many active DReps.
func (d *MetadataStoreSqlite) GetDRepVotingPowerBatch(
	drepCredentials []models.StakeCredentialRef,
	txn types.Txn,
) (map[string]uint64, error) {
	out := make(map[string]uint64, len(drepCredentials))
	if len(drepCredentials) == 0 {
		return out, nil
	}
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	type row struct {
		Drep          []byte
		CredentialTag uint64
		Stake         uint64
	}
	// Chunk credentials to stay under SQLite's default bind variable
	// limit (SQLITE_MAX_VARIABLE_NUMBER = 999). A large active DRep
	// set on mainnet can easily exceed this in a single IN clause.
	// Build separate hash and type slices for the IN clauses.
	hashes := make([][]byte, len(drepCredentials))
	requested := make(map[string]struct{}, len(drepCredentials))
	for i, ref := range drepCredentials {
		hashes[i] = ref.Key
		requested[ref.MapKey()] = struct{}{}
	}
	for start := 0; start < len(hashes); start += sqliteBindVarLimit {
		end := min(start+sqliteBindVarLimit, len(hashes))
		chunk := hashes[start:end]
		var rows []row
		// Aggregate UTxO amounts per (staking_key, drep_type) before
		// adding account.reward to avoid fan-out from the LEFT JOIN.
		if err := db.Raw(getDRepVotingPowerBatchSQL, chunk, chunk).Scan(&rows).Error; err != nil {
			return nil, fmt.Errorf("get drep voting power batch: %w", err)
		}
		for _, r := range rows {
			// account.drep_type 0=key and 1=script align with credential_tag values
			// by protocol spec; types ≥2 (ALWAYS_ABSTAIN, ALWAYS_NO_CONFIDENCE)
			// carry no credential hash and are never in the requested map.
			if r.CredentialTag > 1 {
				continue
			}
			ref := models.StakeCredentialRef{Tag: uint8(r.CredentialTag), Key: r.Drep}
			if _, ok := requested[ref.MapKey()]; !ok {
				continue
			}
			out[ref.MapKey()] = r.Stake
		}
	}
	return out, nil
}

// GetDRepVotingPowerByType returns voting power grouped by DRep delegation
// type. It is used for predefined DRep options, which carry no credential.
func (d *MetadataStoreSqlite) GetDRepVotingPowerByType(
	drepTypes []uint64,
	txn types.Txn,
) (map[uint64]uint64, error) {
	out := make(map[uint64]uint64, len(drepTypes))
	if len(drepTypes) == 0 {
		return out, nil
	}
	if err := models.ValidatePredefinedDrepTypes(drepTypes); err != nil {
		return nil, err
	}
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	type row struct {
		DrepType uint64
		Stake    uint64
	}
	var rows []row
	// Aggregate UTxO amounts per staking_key in a subquery before
	// adding account.reward, otherwise the LEFT JOIN would multiply
	// the per-account reward by the number of live UTxOs and inflate
	// the totals. Each account contributes (utxo_sum + reward) once
	// to its drep_type bucket.
	if err := db.Raw(`
		SELECT a.drep_type AS drep_type,
			   COALESCE(SUM(
				   COALESCE(u.utxo_sum, 0)
				   + COALESCE(CAST(a.reward AS INTEGER), 0)
			   ), 0) AS stake
		FROM account a
		LEFT JOIN (
			SELECT credential_tag, staking_key,
				   COALESCE(SUM(CAST(amount AS INTEGER)), 0) AS utxo_sum
			FROM utxo
			WHERE deleted_slot = 0
			  AND EXISTS (
				  SELECT 1 FROM account ax
				  WHERE ax.credential_tag = utxo.credential_tag
				    AND ax.staking_key = utxo.staking_key
				    AND ax.active = 1 AND ax.drep_type IN ?
			  )
			GROUP BY credential_tag, staking_key
		) u ON u.credential_tag = a.credential_tag
			AND u.staking_key = a.staking_key
		WHERE a.active = 1 AND a.drep_type IN ?
		GROUP BY a.drep_type
	`, drepTypes, drepTypes).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("get drep voting power by type: %w", err)
	}
	for _, r := range rows {
		out[r.DrepType] = r.Stake
	}
	return out, nil
}

// UpdateDRepActivity updates the DRep's last activity epoch and recalculates
// the expiry epoch. Called when a DRep votes, registers, or updates their
// registration. The expiryEpoch is set to activityEpoch + inactivityPeriod.
// Returns ErrDrepActivityNotUpdated if no matching DRep record was found.
func (d *MetadataStoreSqlite) UpdateDRepActivity(
	credentialTag uint8,
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
		Where("credential_tag = ? AND credential = ?", credentialTag, drepCredential).
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

// GetExpiredDReps retrieves all active DReps whose expiry epoch is at or
// before the given epoch. These DReps have been inactive for longer than
// the dRepInactivityPeriod and should be considered expired for voting
// power purposes.
func (d *MetadataStoreSqlite) GetExpiredDReps(
	epoch uint64,
	txn types.Txn,
) ([]*models.Drep, error) {
	var dreps []*models.Drep
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	// A DRep is expired if active, has a non-zero expiry epoch, and the
	// expiry epoch is at or before the current epoch.
	if result := db.Where(
		"active = ? AND expiry_epoch > 0 AND expiry_epoch <= ?",
		true,
		epoch,
	).Find(&dreps); result.Error != nil {
		return nil, fmt.Errorf("get expired dreps: %w", result.Error)
	}
	return dreps, nil
}

// GetCommitteeActiveCount returns the total number of active (non-resigned)
// committee members. This is used for CC threshold calculations.
func (d *MetadataStoreSqlite) GetCommitteeActiveCount(
	txn types.Txn,
) (int, error) {
	db, err := d.resolveReadDB(txn)
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
		return 0, fmt.Errorf("get committee active count: %w", result.Error)
	}
	return int(count), nil
}
