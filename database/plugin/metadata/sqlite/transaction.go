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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

package sqlite

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetTransactionByHash returns a transaction by its hash
func (d *MetadataStoreSqlite) GetTransactionByHash(
	hash []byte,
	txn *gorm.DB,
) (*models.Transaction, error) {
	ret := &models.Transaction{}
	if txn == nil {
		txn = d.DB()
	}
	result := txn.
		Preload(clause.Associations).
		First(ret, "hash = ?", hash)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// SetTransaction persists transaction and certificates with duplicate cleanup.
func (d *MetadataStoreSqlite) SetTransaction(
	point ocommon.Point,
	tx lcommon.Transaction,
	idx uint32,
	deposits map[int]uint64,
	txn *gorm.DB,
) error {
	txHash := tx.Hash().Bytes()
	if txn == nil {
		txn = d.DB()
	}

	tmpTx := models.Transaction{
		Hash:       txHash,
		Type:       tx.Type(),
		BlockHash:  point.Hash,
		BlockIndex: idx,
		Fee:        types.Uint64{Val: tx.Fee()},
		TTL:        types.Uint64{Val: tx.TTL()},
	}
	if tx.Metadata() != nil {
		tmpMetadata := tx.Metadata().Cbor()
		tmpTx.Metadata = tmpMetadata
	}
	collateralReturn := tx.CollateralReturn()
	for _, utxo := range tx.Produced() {
		if collateralReturn != nil && utxo.Output == collateralReturn {
			utxoModel := models.UtxoLedgerToModel(utxo, point.Slot)
			utxoModel.IsCollateralReturn = true
			tmpTx.CollateralReturn = &utxoModel
			continue
		}
		tmpTx.Outputs = append(
			tmpTx.Outputs,
			models.UtxoLedgerToModel(utxo, point.Slot),
		)
	}
	result := txn.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "hash"}}, // unique txn hash
		DoUpdates: clause.AssignmentColumns(
			[]string{"block_hash", "block_index"},
		),
	}).Create(&tmpTx)
	if result.Error != nil {
		return fmt.Errorf(
			"create transaction at slot %d, block %x, txHash %x, txIndex %d: %#v, %w",
			point.Slot,
			point.Hash,
			txHash,
			idx,
			tx,
			result.Error,
		)
	}

	// Reload transaction ID after upsert (needed when ON CONFLICT updates existing row)
	if tmpTx.ID == 0 {
		if err := txn.Where("hash = ?", txHash).First(&tmpTx).Error; err != nil {
			return fmt.Errorf("reload transaction ID after upsert: %w", err)
		}
	}
	// Add Inputs to Transaction
	for _, input := range tx.Inputs() {
		inTxId := input.Id().Bytes()
		inIdx := input.Index()
		utxo, err := d.GetUtxo(inTxId, inIdx, txn)
		if err != nil {
			return fmt.Errorf(
				"failed to fetch input %x#%d: %w",
				inTxId,
				inIdx,
				err,
			)
		}
		if utxo == nil {
			d.logger.Warn(
				"Skipping missing input UTxO",
				"hash",
				input.Id().String(),
				"index",
				inIdx,
			)
			continue
		}
		tmpTx.Inputs = append(
			tmpTx.Inputs,
			*utxo,
		)
	}
	// Add Collateral to Transaction
	if len(tx.Collateral()) > 0 {
		var caseClauses []string
		var whereConditions []string
		var caseArgs []any
		var whereArgs []any

		for _, input := range tx.Collateral() {
			inTxId := input.Id().Bytes()
			inIdx := input.Index()
			utxo, err := d.GetUtxo(inTxId, inIdx, txn)
			if err != nil {
				return fmt.Errorf(
					"failed to fetch input %x#%d: %w",
					inTxId,
					inIdx,
					err,
				)
			}
			if utxo == nil {
				d.logger.Warn(
					"Skipping missing collateral UTxO",
					"hash",
					input.Id().String(),
					"index",
					inIdx,
				)
				continue
			}
			// Found the Utxo, add it to the SQL UPDATE list
			// First, add it to the CASE statement so it's selected
			caseClauses = append(
				caseClauses,
				"WHEN tx_id = ? AND output_idx = ? THEN ?",
			)
			caseArgs = append(caseArgs, inTxId, inIdx, txHash)
			// Also add it to the WHERE clause in the SQL UPDATE
			whereConditions = append(
				whereConditions,
				"(tx_id = ? AND output_idx = ?)",
			)
			whereArgs = append(whereArgs, inTxId, inIdx)
			// Add it to the Transaction
			tmpTx.Collateral = append(
				tmpTx.Collateral,
				*utxo,
			)
		}
		// Update reference where this Utxo was used as collateral in a Transaction
		if len(caseClauses) > 0 {
			args := append(caseArgs, whereArgs...)
			sql := fmt.Sprintf(
				"UPDATE utxo SET collateral_by_tx_id = CASE %s ELSE collateral_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			result = txn.Exec(sql, args...)
			if result.Error != nil {
				return fmt.Errorf(
					"batch update collateral: %w",
					result.Error,
				)
			}
		}
	}
	// Add ReferenceInputs to Transaction
	if len(tx.ReferenceInputs()) > 0 {
		var caseClauses []string
		var whereConditions []string
		var caseArgs []any
		var whereArgs []any

		for _, input := range tx.ReferenceInputs() {
			inTxId := input.Id().Bytes()
			inIdx := input.Index()
			utxo, err := d.GetUtxo(inTxId, inIdx, txn)
			if err != nil {
				return fmt.Errorf(
					"failed to fetch input %x#%d: %w",
					inTxId,
					inIdx,
					err,
				)
			}
			if utxo == nil {
				d.logger.Warn(
					"Skipping missing reference input UTxO",
					"hash",
					input.Id().String(),
					"index",
					inIdx,
				)
				continue
			}
			// Found the Utxo, add it to the SQL UPDATE list
			// First, add it to the CASE statement so it's selected
			caseClauses = append(
				caseClauses,
				"WHEN tx_id = ? AND output_idx = ? THEN ?",
			)
			caseArgs = append(caseArgs, inTxId, inIdx, txHash)
			// Also add it to the WHERE clause in the SQL UPDATE
			whereConditions = append(
				whereConditions,
				"(tx_id = ? AND output_idx = ?)",
			)
			whereArgs = append(whereArgs, inTxId, inIdx)
			// Add it to the Transaction
			tmpTx.ReferenceInputs = append(
				tmpTx.ReferenceInputs,
				*utxo,
			)
		}
		// Update reference where this Utxo was used as a reference input in a Transaction
		if len(caseClauses) > 0 {
			args := append(caseArgs, whereArgs...)
			sql := fmt.Sprintf(
				"UPDATE utxo SET referenced_by_tx_id = CASE %s ELSE referenced_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			result = txn.Exec(sql, args...)
			if result.Error != nil {
				return fmt.Errorf(
					"batch update reference inputs: %w",
					result.Error,
				)
			}
		}
	}

	// Consume input UTxOs
	for _, input := range tx.Consumed() {
		inTxId := input.Id().Bytes()
		inIdx := input.Index()
		utxo, err := d.GetUtxo(inTxId, inIdx, txn)
		if err != nil {
			return fmt.Errorf(
				"failed to fetch input %x#%d: %w",
				inTxId,
				inIdx,
				err,
			)
		}
		if utxo == nil {
			d.logger.Warn(
				fmt.Sprintf("input UTxO not found: %x#%d", inTxId, inIdx),
			)
			continue
			// return fmt.Errorf("input UTxO not found: %x#%d", inTxId, inIdx)
		}
		// Update existing UTxOs
		result = txn.Model(&models.Utxo{}).
			Where("tx_id = ? AND output_idx = ?", inTxId, inIdx).
			Where("spent_at_tx_id IS NULL OR spent_at_tx_id = ?", txHash).
			Updates(map[string]any{
				"deleted_slot":   point.Slot,
				"spent_at_tx_id": txHash,
			})
		if result.Error != nil {
			return result.Error
		}
	}
	// Avoid updating associations
	result = txn.Omit(clause.Associations).Save(&tmpTx)
	if result.Error != nil {
		return result.Error
	}

	if !tx.IsValid() {
		return nil
	}

	certs := tx.Certificates()
	if len(certs) == 0 {
		return nil
	}
	// Validate certificate deposits are provided when certificates are present
	if deposits == nil {
		return fmt.Errorf(
			"certificate deposits required when transaction has %d certificates",
			len(certs),
		)
	}

	// Clean up certificates that are no longer present
	if err := d.cleanupObsoleteCertificates(tmpTx.ID, certs, txn); err != nil {
		return fmt.Errorf("cleanup obsolete certificates: %w", err)
	}

	for idx, cert := range certs {
		// Get deposit amount for this certificate (default to 0 if not present)
		deposit := uint64(0)
		if amt, exists := deposits[idx]; exists {
			deposit = amt
		}

		// Validate that deposit-requiring certificate types have deposits provided
		certType := cert.Type()
		if _, exists := deposits[idx]; !exists {
			switch certType {
			case uint(lcommon.CertificateTypeStakeRegistration),
				uint(lcommon.CertificateTypeRegistration),
				uint(lcommon.CertificateTypePoolRegistration),
				uint(lcommon.CertificateTypeRegistrationDrep),
				uint(lcommon.CertificateTypeStakeRegistrationDelegation),
				uint(lcommon.CertificateTypeVoteRegistrationDelegation),
				uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation):
				return fmt.Errorf(
					"certificate type %d at index %d in transaction %d requires deposit calculation",
					certType,
					idx,
					tmpTx.ID,
				)
			}
		}

		err := d.storeCertificate(
			cert,
			tmpTx.ID,
			uint32(idx), //nolint:gosec
			point,
			deposit,
			txn,
		)
		if err != nil {
			return fmt.Errorf(
				"store certificate at index %d: %w",
				idx,
				err,
			)
		}
	}

	return nil
}

// cleanupObsoleteCertificates removes certificate records that are no longer present in the current transaction
func (d *MetadataStoreSqlite) cleanupObsoleteCertificates(
	txID uint,
	currentCerts []lcommon.Certificate,
	txn *gorm.DB,
) error {
	// Get existing certificates for this transaction
	var existingCerts []models.Certificate
	if err := txn.Where("transaction_id = ?", txID).Find(&existingCerts).Error; err != nil {
		return fmt.Errorf("query existing certificates: %w", err)
	}

	// Create a map of current certificate indices
	currentIndices := make(map[uint]bool)
	for idx := range currentCerts {
		currentIndices[uint(idx)] = true // #nosec G115 - slice index is always non-negative
	}

	// Find certificates that need to be removed
	var toRemove []models.Certificate
	for _, existing := range existingCerts {
		if !currentIndices[existing.CertIndex] {
			toRemove = append(toRemove, existing)
		}
	}

	// Group certificates by type for bulk operations
	certsByType := make(map[uint][]uint) // certType -> []certificateIDs
	certIDsToRemove := make([]uint, 0, len(toRemove))

	for _, cert := range toRemove {
		certsByType[cert.CertType] = append(
			certsByType[cert.CertType],
			cert.CertificateID,
		)
		certIDsToRemove = append(certIDsToRemove, cert.ID)
	}

	// Bulk delete specialized certificate records by type
	for certType, certIDs := range certsByType {
		if err := d.bulkRemoveCertificateRecords(certType, certIDs, txn); err != nil {
			return fmt.Errorf(
				"bulk remove certificate records for type %d: %w",
				certType,
				err,
			)
		}
	}

	// Bulk delete certificate mapping records
	if len(certIDsToRemove) > 0 {
		if err := txn.Where("id IN ?", certIDsToRemove).Delete(&models.Certificate{}).Error; err != nil {
			return fmt.Errorf("bulk delete certificate mappings: %w", err)
		}
	}

	return nil
}

// bulkRemoveCertificateRecords performs bulk deletion of specialized certificate records by type
func (d *MetadataStoreSqlite) bulkRemoveCertificateRecords(
	certType uint,
	certIDs []uint,
	txn *gorm.DB,
) error {
	if len(certIDs) == 0 {
		return nil
	}

	switch certType {
	case uint(lcommon.CertificateTypeStakeRegistration):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.StakeRegistration{}).
			Error
	case uint(lcommon.CertificateTypeStakeDeregistration):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.StakeDeregistration{}).
			Error
	case uint(lcommon.CertificateTypeStakeDelegation):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.StakeDelegation{}).
			Error
	case uint(lcommon.CertificateTypePoolRegistration):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.PoolRegistration{}).
			Error
	case uint(lcommon.CertificateTypePoolRetirement):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.PoolRetirement{}).
			Error
	case uint(lcommon.CertificateTypeGenesisKeyDelegation):
		return txn.Where("id IN ?", certIDs).
			Delete(&models.GenesisKeyDelegation{}).
			Error
	case uint(lcommon.CertificateTypeMoveInstantaneousRewards):
		return txn.Where("id IN ?", certIDs).
			Delete(&models.MoveInstantaneousRewards{}).
			Error
	case uint(lcommon.CertificateTypeAuthCommitteeHot):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.AuthCommitteeHot{}).
			Error
	case uint(lcommon.CertificateTypeResignCommitteeCold):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.ResignCommitteeCold{}).
			Error
	case uint(lcommon.CertificateTypeRegistrationDrep):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.RegistrationDrep{}).
			Error
	case uint(lcommon.CertificateTypeDeregistrationDrep):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.DeregistrationDrep{}).
			Error
	case uint(lcommon.CertificateTypeUpdateDrep):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.UpdateDrep{}).
			Error
	case uint(lcommon.CertificateTypeVoteDelegation):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.VoteDelegation{}).
			Error
	case uint(lcommon.CertificateTypeVoteRegistrationDelegation):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.VoteRegistrationDelegation{}).
			Error
	case uint(lcommon.CertificateTypeStakeVoteDelegation):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.StakeVoteDelegation{}).
			Error
	case uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation):
		return txn.Where("certificate_id IN ?", certIDs).
			Delete(&models.StakeVoteRegistrationDelegation{}).
			Error
	case uint(lcommon.CertificateTypeLeiosEb):
		return txn.Where("id IN ?", certIDs).Delete(&models.LeiosEb{}).Error
	default:
		// Unknown certificate type, skip
		return nil
	}
}

// storeCertificate persists certificate to specialized table and creates mapping entry.
func (d *MetadataStoreSqlite) storeCertificate(
	cert lcommon.Certificate,
	txID uint,
	blockIndex uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
) error {
	if cert == nil {
		return errors.New("certificate cannot be nil")
	}
	if txn == nil {
		return errors.New("transaction cannot be nil")
	}

	insertCert := func(targetID uint) (uint, error) {
		certRow := models.Certificate{
			TransactionID: txID,
			Slot:          blockPoint.Slot,
			BlockHash:     blockPoint.Hash,
			CertIndex:     uint(blockIndex),
			CertType:      uint(cert.Type()),
			CertificateID: targetID,
		}
		if err := txn.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "transaction_id"},
				{Name: "cert_index"},
			},
			DoUpdates: clause.AssignmentColumns(
				[]string{"slot", "block_hash", "cert_type", "certificate_id"},
			),
		}).Create(&certRow).Error; err != nil {
			return 0, err
		}
		// If the row already existed, certRow.ID will be 0. Re-query to get the existing ID.
		if certRow.ID == 0 {
			var existingCert models.Certificate
			if err := txn.Where("transaction_id = ? AND cert_index = ?", txID, uint(blockIndex)).First(&existingCert).Error; err != nil {
				return 0, fmt.Errorf(
					"failed to find existing certificate: %w",
					err,
				)
			}
			certRow.ID = existingCert.ID
		}
		return certRow.ID, nil
	}

	switch certTyped := cert.(type) {
	case *lcommon.StakeRegistrationCertificate:
		return d.storeStakeRegistrationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.StakeDeregistrationCertificate:
		return d.storeStakeDeregistrationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.StakeDelegationCertificate:
		return d.storeStakeDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.PoolRegistrationCertificate:
		return d.storePoolRegistrationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.PoolRetirementCertificate:
		return d.storePoolRetirementCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.RegistrationCertificate:
		return d.storeRegistrationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.DeregistrationCertificate:
		return d.storeDeregistrationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.StakeRegistrationDelegationCertificate:
		return d.storeStakeRegistrationDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.VoteDelegationCertificate:
		return d.storeVoteDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.StakeVoteDelegationCertificate:
		return d.storeStakeVoteDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.VoteRegistrationDelegationCertificate:
		return d.storeVoteRegistrationDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		return d.storeStakeVoteRegistrationDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.AuthCommitteeHotCertificate:
		return d.storeAuthCommitteeHotCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.ResignCommitteeColdCertificate:
		return d.storeResignCommitteeColdCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.RegistrationDrepCertificate:
		return d.storeRegistrationDrepCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.DeregistrationDrepCertificate:
		return d.storeDeregistrationDrepCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.UpdateDrepCertificate:
		return d.storeUpdateDrepCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.GenesisKeyDelegationCertificate:
		return d.storeGenesisKeyDelegationCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.MoveInstantaneousRewardsCertificate:
		return d.storeMoveInstantaneousRewardsCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	case *lcommon.LeiosEbCertificate:
		return d.storeLeiosEbCertificate(certTyped, txID, blockIndex, blockPoint, deposit, txn, insertCert)
	default:
		return fmt.Errorf("unsupported certificate type: %T", cert)
	}
}

// Helper functions for common certificate patterns

// snakeToPascal converts snake_case to PascalCase (e.g., "genesis_hash" -> "GenesisHash")
func snakeToPascal(s string) string {
	parts := strings.Split(s, "_")
	caser := cases.Title(language.English)
	for i, part := range parts {
		parts[i] = caser.String(part)
	}
	return strings.Join(parts, "")
}

// createOrUpdateAccount creates a new account or updates an existing one
func (d *MetadataStoreSqlite) createOrUpdateAccount(
	stakeKey []byte,
	poolKey []byte,
	drepKey []byte,
	slot uint64,
	txn *gorm.DB,
) error {
	// Try to get existing active account
	tmpAccount, err := d.GetAccount(stakeKey, txn)
	if err != nil {
		return fmt.Errorf("get account: %w", err)
	}

	if tmpAccount != nil {
		// Update existing account
		if poolKey != nil {
			tmpAccount.Pool = poolKey
		}
		if drepKey != nil {
			tmpAccount.Drep = drepKey
		}
		tmpAccount.Active = true
		tmpAccount.AddedSlot = slot
		if err := txn.Save(tmpAccount).Error; err != nil {
			return fmt.Errorf("update account: %w", err)
		}
	} else {
		// Create new account
		if err := d.SetAccount(stakeKey, poolKey, drepKey, slot, true, txn); err != nil {
			return fmt.Errorf("create account: %w", err)
		}
	}
	return nil
}

// deactivateAccount marks an account as inactive
func (d *MetadataStoreSqlite) deactivateAccount(
	stakeKey []byte,
	txn *gorm.DB,
) error {
	tmpAccount, err := d.GetAccount(stakeKey, txn)
	if err != nil {
		return fmt.Errorf("get account for deactivation: %w", err)
	}
	if tmpAccount != nil {
		tmpAccount.Active = false
		if err := txn.Save(tmpAccount).Error; err != nil {
			return fmt.Errorf("deactivate account: %w", err)
		}
	}
	return nil
}

// createOrUpdateDrep creates a new drep or updates an existing one
func (d *MetadataStoreSqlite) createOrUpdateDrep(
	drepKey []byte,
	slot uint64,
	anchorUrl string,
	anchorHash []byte,
	active bool,
	txn *gorm.DB,
) error {
	// Try to get existing drep
	tmpDrep, err := d.GetDrep(drepKey, txn)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("get drep: %w", err)
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create new drep
		if err := d.SetDrep(drepKey, slot, anchorUrl, anchorHash, active, txn); err != nil {
			return fmt.Errorf("create drep: %w", err)
		}
	} else {
		// Update existing drep
		tmpDrep.Active = active
		if anchorUrl != "" {
			tmpDrep.AnchorUrl = anchorUrl
		}
		if len(anchorHash) > 0 {
			tmpDrep.AnchorHash = anchorHash
		}
		if err := txn.Save(tmpDrep).Error; err != nil {
			return fmt.Errorf("update drep: %w", err)
		}
	}
	return nil
}

// createCertificateModel creates a certificate model with conflict resolution and returns the ID
func createCertificateModel(
	model any,
	conflictColumns []string,
	updateColumns []string,
	txn *gorm.DB,
) (uint, error) {
	// If no conflict columns specified, just create the model
	if len(conflictColumns) == 0 {
		if err := txn.Create(model).Error; err != nil {
			return 0, err
		}
	} else {
		// Build conflict columns
		columns := make([]clause.Column, 0, len(conflictColumns))
		for _, col := range conflictColumns {
			columns = append(columns, clause.Column{Name: col})
		}

		// Create the model with conflict resolution
		if err := txn.Clauses(clause.OnConflict{
			Columns:   columns,
			DoUpdates: clause.AssignmentColumns(updateColumns),
		}).Create(model).Error; err != nil {
			return 0, err
		}
	}

	// Get the ID from the created model using reflection
	v := reflect.ValueOf(model).Elem()
	idField := v.FieldByName("ID")
	if idField.IsValid() && idField.Kind() == reflect.Uint {
		id := uint(idField.Uint())
		// If ID is 0, it means a conflict occurred and we updated an existing row
		// We need to re-query to get the actual ID
		if id == 0 && len(conflictColumns) > 0 {
			// Build WHERE clause using conflict columns
			whereClause := make(map[string]interface{})
			for _, col := range conflictColumns {
				fieldName := snakeToPascal(
					col,
				) // Convert snake_case to PascalCase
				field := v.FieldByName(fieldName)
				if field.IsValid() {
					whereClause[col] = field.Interface()
				}
			}

			// Re-query to get the existing record's ID
			if err := txn.Where(whereClause).First(model).Error; err != nil {
				return 0, fmt.Errorf(
					"failed to re-query for ID after conflict: %w",
					err,
				)
			}

			// Get the ID again after re-query
			id = uint(idField.Uint())
		}
		return id, nil
	}

	return 0, errors.New("unable to get ID from model")
}

// storeStakeRegistrationCertificate handles StakeRegistrationCertificate persistence
func (d *MetadataStoreSqlite) storeStakeRegistrationCertificate(
	certTyped *lcommon.StakeRegistrationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()

	// Create account for stake registration
	if err := d.createOrUpdateAccount(stakeKey, nil, nil, blockPoint.Slot, txn); err != nil {
		return fmt.Errorf("create account for stake registration: %w", err)
	}

	// Create stake registration record
	item := models.StakeRegistration{
		StakingKey:    stakeKey,
		DepositAmount: types.Uint64{Val: deposit},
		AddedSlot:     blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for stake registration
		[]string{"deposit_amount", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create stake registration: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update stake registration certificate id: %w", err)
	}

	return nil
}

// storeStakeDeregistrationCertificate handles StakeDeregistrationCertificate persistence
func (d *MetadataStoreSqlite) storeStakeDeregistrationCertificate(
	certTyped *lcommon.StakeDeregistrationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()

	// Deactivate account for stake deregistration
	if err := d.deactivateAccount(stakeKey, txn); err != nil {
		return fmt.Errorf(
			"deactivate account for stake deregistration: %w",
			err,
		)
	}

	// Create stake deregistration record
	item := models.StakeDeregistration{
		StakingKey: stakeKey,
		AddedSlot:  blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for stake deregistration
		[]string{"added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create stake deregistration: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update stake deregistration certificate id: %w", err)
	}

	return nil
}

// storeStakeDelegationCertificate handles StakeDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeStakeDelegationCertificate(
	certTyped *lcommon.StakeDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()
	poolKeyHash := certTyped.PoolKeyHash[:]

	// Update account with pool delegation
	if err := d.createOrUpdateAccount(stakeKey, poolKeyHash, nil, blockPoint.Slot, txn); err != nil {
		return fmt.Errorf("update account for stake delegation: %w", err)
	}

	// Create stake delegation record
	item := models.StakeDelegation{
		StakingKey:  stakeKey,
		PoolKeyHash: poolKeyHash,
		AddedSlot:   blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for stake delegation
		[]string{"pool_key_hash", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create stake delegation: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update stake delegation certificate id: %w", err)
	}

	return nil
}

// storePoolRegistrationCertificate handles PoolRegistrationCertificate persistence
func (d *MetadataStoreSqlite) storePoolRegistrationCertificate(
	certTyped *lcommon.PoolRegistrationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	poolKeyHash := lcommon.PoolKeyHash(certTyped.Operator[:])
	// Create or update Pool record
	tmpPool, err := d.GetPool(poolKeyHash, txn)
	if err != nil && !errors.Is(err, models.ErrPoolNotFound) {
		return fmt.Errorf("get pool for registration: %w", err)
	}
	if tmpPool == nil {
		tmpPool = &models.Pool{
			PoolKeyHash: certTyped.Operator[:],
			VrfKeyHash:  certTyped.VrfKeyHash[:],
		}
	}
	// Update pool data from certificate
	tmpPool.Pledge = types.Uint64{Val: certTyped.Pledge}
	tmpPool.Cost = types.Uint64{Val: certTyped.Cost}
	if certTyped.Margin.Rat != nil {
		tmpPool.Margin = &types.Rat{Rat: certTyped.Margin.Rat}
	}
	tmpPool.RewardAccount = certTyped.RewardAccount[:]

	item := models.PoolRegistration{
		PoolKeyHash:   certTyped.Operator[:],
		VrfKeyHash:    certTyped.VrfKeyHash[:],
		RewardAccount: certTyped.RewardAccount[:],
		Pledge:        types.Uint64{Val: certTyped.Pledge},
		Cost:          types.Uint64{Val: certTyped.Cost},
		AddedSlot:     blockPoint.Slot,
		DepositAmount: types.Uint64{Val: deposit},
	}
	// Add margin if it's valid
	if certTyped.Margin.Rat != nil {
		item.Margin = &types.Rat{Rat: certTyped.Margin.Rat}
	}
	// Add metadata if present
	if certTyped.PoolMetadata != nil {
		item.MetadataUrl = certTyped.PoolMetadata.Url
		item.MetadataHash = certTyped.PoolMetadata.Hash[:]
	}
	// Add owners
	for _, owner := range certTyped.PoolOwners {
		item.Owners = append(item.Owners, models.PoolRegistrationOwner{
			KeyHash: owner[:],
		})
	}
	// Add relays
	for _, relay := range certTyped.Relays {
		tmpRelay := models.PoolRegistrationRelay{
			Ipv4: relay.Ipv4,
			Ipv6: relay.Ipv6,
		}
		if relay.Port != nil {
			tmpRelay.Port = uint(*relay.Port)
		}
		if relay.Hostname != nil {
			tmpRelay.Hostname = *relay.Hostname
		}
		item.Relays = append(item.Relays, tmpRelay)
	}
	// Update Pool record with the new registration data
	tmpPool.Owners = item.Owners
	tmpPool.Relays = item.Relays
	if err := txn.Save(tmpPool).Error; err != nil {
		return fmt.Errorf("save pool for registration: %w", err)
	}
	item.PoolID = tmpPool.ID

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for pool registration
		[]string{
			"vrf_key_hash",
			"reward_account",
			"pledge",
			"cost",
			"margin",
			"metadata_url",
			"metadata_hash",
			"deposit_amount",
			"added_slot",
		},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create pool registration: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update pool registration certificate id: %w", err)
	}

	return nil
}

// storePoolRetirementCertificate handles PoolRetirementCertificate persistence
func (d *MetadataStoreSqlite) storePoolRetirementCertificate(
	certTyped *lcommon.PoolRetirementCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	poolKeyHash := certTyped.PoolKeyHash[:]

	// Create pool retirement record
	item := models.PoolRetirement{
		PoolKeyHash: poolKeyHash,
		Epoch:       certTyped.Epoch,
		AddedSlot:   blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{"pool_key_hash", "epoch"},
		[]string{"added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create pool retirement: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update pool retirement certificate id: %w", err)
	}

	return nil
}

// storeRegistrationCertificate handles RegistrationCertificate persistence
func (d *MetadataStoreSqlite) storeRegistrationCertificate(
	certTyped *lcommon.RegistrationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()

	// Create account for registration
	if err := d.createOrUpdateAccount(stakeKey, nil, nil, blockPoint.Slot, txn); err != nil {
		return fmt.Errorf("create account for registration: %w", err)
	}

	// Create registration record
	item := models.Registration{
		StakingKey:    stakeKey,
		DepositAmount: types.Uint64{Val: deposit},
		AddedSlot:     blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for registration
		[]string{"deposit_amount", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create registration: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update registration certificate id: %w", err)
	}

	return nil
}

// storeDeregistrationCertificate handles DeregistrationCertificate persistence
func (d *MetadataStoreSqlite) storeDeregistrationCertificate(
	certTyped *lcommon.DeregistrationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()

	// Deactivate account for deregistration
	if err := d.deactivateAccount(stakeKey, txn); err != nil {
		return fmt.Errorf("deactivate account for deregistration: %w", err)
	}

	// Create deregistration record
	item := models.Deregistration{
		StakingKey: stakeKey,
		AddedSlot:  blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for deregistration
		[]string{"added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create deregistration: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update deregistration certificate id: %w", err)
	}

	return nil
}

// storeStakeRegistrationDelegationCertificate handles StakeRegistrationDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeStakeRegistrationDelegationCertificate(
	certTyped *lcommon.StakeRegistrationDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()
	poolKeyHash := certTyped.PoolKeyHash

	// Create account for stake registration delegation
	if err := d.createOrUpdateAccount(
		stakeKey, poolKeyHash, nil, blockPoint.Slot, txn,
	); err != nil {
		return fmt.Errorf(
			"create account for stake registration delegation: %w",
			err,
		)
	}

	// Create stake registration delegation record
	item := models.StakeRegistrationDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKeyHash,
		DepositAmount: types.Uint64{Val: deposit},
		AddedSlot:     blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for stake registration delegation
		[]string{"pool_key_hash", "deposit_amount", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create stake registration delegation: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf(
			"update stake registration delegation certificate id: %w",
			err,
		)
	}

	return nil
}

// storeVoteDelegationCertificate handles VoteDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeVoteDelegationCertificate(
	certTyped *lcommon.VoteDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()
	drepCredential := certTyped.Drep.Credential[:]

	// Update account with DRep delegation
	if err := d.createOrUpdateAccount(stakeKey, nil, drepCredential, blockPoint.Slot, txn); err != nil {
		return fmt.Errorf("update account for vote delegation: %w", err)
	}

	// Create vote delegation record
	item := models.VoteDelegation{
		StakingKey: stakeKey,
		Drep:       drepCredential,
		AddedSlot:  blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for vote delegation
		[]string{"drep", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create vote delegation: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update vote delegation certificate id: %w", err)
	}

	return nil
}

// storeStakeVoteDelegationCertificate handles StakeVoteDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeStakeVoteDelegationCertificate(
	certTyped *lcommon.StakeVoteDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()
	poolKeyHash := certTyped.PoolKeyHash
	drepCredential := certTyped.Drep.Credential[:]

	// Update account with both pool and DRep delegation
	if err := d.createOrUpdateAccount(stakeKey, poolKeyHash, drepCredential, blockPoint.Slot, txn); err != nil {
		return fmt.Errorf("update account for stake vote delegation: %w", err)
	}

	// Create stake vote delegation record
	item := models.StakeVoteDelegation{
		StakingKey:  stakeKey,
		PoolKeyHash: poolKeyHash,
		Drep:        drepCredential,
		AddedSlot:   blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for stake vote delegation
		[]string{"pool_key_hash", "drep", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create stake vote delegation: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf(
			"update stake vote delegation certificate id: %w",
			err,
		)
	}

	return nil
}

// storeVoteRegistrationDelegationCertificate handles VoteRegistrationDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeVoteRegistrationDelegationCertificate(
	certTyped *lcommon.VoteRegistrationDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()
	drepCredential := certTyped.Drep.Credential[:]

	// Create account for vote registration delegation
	if err := d.createOrUpdateAccount(
		stakeKey, nil, drepCredential, blockPoint.Slot, txn,
	); err != nil {
		return fmt.Errorf(
			"create account for vote registration delegation: %w",
			err,
		)
	}

	// Create vote registration delegation record
	item := models.VoteRegistrationDelegation{
		StakingKey:    stakeKey,
		Drep:          drepCredential,
		DepositAmount: types.Uint64{Val: deposit},
		AddedSlot:     blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for vote registration delegation
		[]string{"drep", "deposit_amount", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create vote registration delegation: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf(
			"update vote registration delegation certificate id: %w",
			err,
		)
	}

	return nil
}

// storeStakeVoteRegistrationDelegationCertificate handles StakeVoteRegistrationDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeStakeVoteRegistrationDelegationCertificate(
	certTyped *lcommon.StakeVoteRegistrationDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	stakeKey := certTyped.StakeCredential.Credential.Bytes()
	poolKeyHash := certTyped.PoolKeyHash[:]
	drepCredential := certTyped.Drep.Credential[:]

	// Create account for stake vote registration delegation
	if err := d.createOrUpdateAccount(
		stakeKey, poolKeyHash, drepCredential, blockPoint.Slot, txn,
	); err != nil {
		return fmt.Errorf(
			"create account for stake vote registration delegation: %w",
			err,
		)
	}

	// Create stake vote registration delegation record
	item := models.StakeVoteRegistrationDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKeyHash,
		Drep:          drepCredential,
		DepositAmount: types.Uint64{Val: deposit},
		AddedSlot:     blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for stake vote registration delegation
		[]string{"pool_key_hash", "drep", "deposit_amount", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create stake vote registration delegation: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf(
			"update stake vote registration delegation certificate id: %w",
			err,
		)
	}

	return nil
}

// storeAuthCommitteeHotCertificate handles AuthCommitteeHotCertificate persistence
func (d *MetadataStoreSqlite) storeAuthCommitteeHotCertificate(
	certTyped *lcommon.AuthCommitteeHotCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	// Create auth committee hot record
	item := models.AuthCommitteeHot{
		ColdCredential: certTyped.ColdCredential.Credential.Bytes(),
		HostCredential: certTyped.HotCredential.Credential.Bytes(),
		AddedSlot:      blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for auth committee hot
		[]string{"host_credential", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create auth committee hot: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	// Update the item with the certificate ID
	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf(
			"update auth committee hot with certificate ID: %w",
			err,
		)
	}

	return nil
}

// storeResignCommitteeColdCertificate handles ResignCommitteeColdCertificate persistence
func (d *MetadataStoreSqlite) storeResignCommitteeColdCertificate(
	certTyped *lcommon.ResignCommitteeColdCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	// Create resign committee cold record
	item := models.ResignCommitteeCold{
		ColdCredential: certTyped.ColdCredential.Credential.Bytes(),
		AddedSlot:      blockPoint.Slot,
	}

	// Add anchor data if present
	if certTyped.Anchor != nil {
		item.AnchorUrl = certTyped.Anchor.Url
		item.AnchorHash = certTyped.Anchor.DataHash[:]
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for resign committee cold
		[]string{"anchor_url", "anchor_hash", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create resign committee cold: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf(
			"update resign committee cold certificate id: %w",
			err,
		)
	}

	return nil
}

// storeRegistrationDrepCertificate handles RegistrationDrepCertificate persistence
func (d *MetadataStoreSqlite) storeRegistrationDrepCertificate(
	certTyped *lcommon.RegistrationDrepCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	drepCredential := certTyped.DrepCredential.Credential.Bytes()

	// Create/update drep to mark as active for registration
	var anchorUrl string
	var anchorHash []byte
	if certTyped.Anchor != nil {
		anchorUrl = certTyped.Anchor.Url
		anchorHash = certTyped.Anchor.DataHash[:]
	}

	if err := d.createOrUpdateDrep(drepCredential, blockPoint.Slot, anchorUrl, anchorHash, true, txn); err != nil {
		return fmt.Errorf("create drep for registration: %w", err)
	}

	// Create registration drep record
	item := models.RegistrationDrep{
		DrepCredential: drepCredential,
		AddedSlot:      blockPoint.Slot,
		DepositAmount:  types.Uint64{Val: deposit},
		AnchorUrl:      anchorUrl,
		AnchorHash:     anchorHash,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for re-registration
		[]string{"deposit_amount", "anchor_url", "anchor_hash", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create registration drep: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update registration drep certificate id: %w", err)
	}

	return nil
}

// storeDeregistrationDrepCertificate handles DeregistrationDrepCertificate persistence
func (d *MetadataStoreSqlite) storeDeregistrationDrepCertificate(
	certTyped *lcommon.DeregistrationDrepCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	deposit uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	drepCredential := certTyped.DrepCredential.Credential.Bytes()

	// Deactivate drep for deregistration
	tmpDrep, err := d.GetDrep(drepCredential, txn)
	if err != nil {
		// If drep not found, we still allow the operation (matches original behavior)
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("get drep for deactivation: %w", err)
		}
	} else {
		tmpDrep.Active = false
		if err := txn.Save(tmpDrep).Error; err != nil {
			return fmt.Errorf("deactivate drep: %w", err)
		}
	}

	// Create deregistration drep record
	item := models.DeregistrationDrep{
		DrepCredential: drepCredential,
		AddedSlot:      blockPoint.Slot,
		DepositAmount:  types.Uint64{Val: deposit},
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for deregistration
		[]string{"deposit_amount", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create deregistration drep: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update deregistration drep certificate id: %w", err)
	}

	return nil
}

// storeUpdateDrepCertificate handles UpdateDrepCertificate persistence
func (d *MetadataStoreSqlite) storeUpdateDrepCertificate(
	certTyped *lcommon.UpdateDrepCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	// Create update drep record
	item := models.UpdateDrep{
		DrepCredential: certTyped.DrepCredential.Credential.Bytes(),
		AddedSlot:      blockPoint.Slot,
	}

	// Add anchor data if present
	if certTyped.Anchor != nil {
		item.AnchorUrl = certTyped.Anchor.Url
		item.AnchorHash = certTyped.Anchor.DataHash[:]
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for update
		[]string{"anchor_url", "anchor_hash", "added_slot"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create update drep: %w", err)
	}

	certID, err := insertCert(id)
	if err != nil {
		return err
	}

	if err := txn.Model(&item).Update("certificate_id", certID).Error; err != nil {
		return fmt.Errorf("update update drep certificate id: %w", err)
	}

	return nil
}

// storeGenesisKeyDelegationCertificate handles GenesisKeyDelegationCertificate persistence
func (d *MetadataStoreSqlite) storeGenesisKeyDelegationCertificate(
	certTyped *lcommon.GenesisKeyDelegationCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	// Create genesis key delegation record
	item := models.GenesisKeyDelegation{
		GenesisHash:         certTyped.GenesisHash,
		GenesisDelegateHash: certTyped.GenesisDelegateHash,
		VrfKeyHash:          certTyped.VrfKeyHash[:],
		AddedSlot:           blockPoint.Slot,
	}

	// Genesis key delegation is idempotent, so we use DoNothing on conflict
	if err := txn.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "genesis_hash"}, {Name: "genesis_delegate_hash"}, {Name: "vrf_key_hash"}},
		DoNothing: true,
	}).Create(&item).Error; err != nil {
		return fmt.Errorf("create genesis key delegation: %w", err)
	}

	// If ID is 0, it means a conflict occurred and DoNothing was executed
	// We need to query the existing row to get its ID
	if item.ID == 0 {
		if err := txn.Where(models.GenesisKeyDelegation{
			GenesisHash:         certTyped.GenesisHash,
			GenesisDelegateHash: certTyped.GenesisDelegateHash,
			VrfKeyHash:          certTyped.VrfKeyHash[:],
		}).First(&item).Error; err != nil {
			return fmt.Errorf(
				"failed to query existing genesis key delegation: %w",
				err,
			)
		}
	}

	_, err := insertCert(item.ID)
	return err
}

// storeMoveInstantaneousRewardsCertificate handles MoveInstantaneousRewardsCertificate persistence
func (d *MetadataStoreSqlite) storeMoveInstantaneousRewardsCertificate(
	certTyped *lcommon.MoveInstantaneousRewardsCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	// Serialize rewards map to JSON for storage - convert Credential keys to bytes
	var rewardData []byte
	var err error
	if len(certTyped.Reward.Rewards) > 0 {
		// Convert map with Credential keys to map with hex-encoded string keys for JSON serialization
		// Hex encoding provides readable, robust keys that avoid non-printable/non-UTF-8 sequences
		serializableRewards := make(map[string]uint64)
		for cred, amount := range certTyped.Reward.Rewards {
			serializableRewards[hex.EncodeToString(cred.Credential.Bytes())] = amount
		}
		rewardData, err = json.Marshal(serializableRewards)
		if err != nil {
			return fmt.Errorf("serialize MIR rewards: %w", err)
		}
	}

	// Create move instantaneous rewards record
	item := models.MoveInstantaneousRewards{
		Source:     certTyped.Reward.Source,
		OtherPot:   certTyped.Reward.OtherPot,
		RewardData: rewardData,
		AddedSlot:  blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{}, // No conflict resolution for MIR
		[]string{"reward_data"},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create move instantaneous rewards: %w", err)
	}

	_, err = insertCert(id)
	return err
}

// storeLeiosEbCertificate handles LeiosEbCertificate persistence
func (d *MetadataStoreSqlite) storeLeiosEbCertificate(
	certTyped *lcommon.LeiosEbCertificate,
	_ uint,
	_ uint32,
	blockPoint ocommon.Point,
	_ uint64,
	txn *gorm.DB,
	insertCert func(uint) (uint, error),
) error {
	// Serialize complex data structures to JSON for storage
	persistentVotersData, err := json.Marshal(certTyped.PersistentVoters)
	if err != nil {
		return fmt.Errorf("serialize persistent voters: %w", err)
	}

	// Convert map with Blake2b256 keys to map with hex-encoded string keys for JSON serialization
	// Hex encoding provides readable, robust keys that avoid non-printable/non-UTF-8 sequences
	serializableNonpersistentVoters := make(map[string]any)
	for hash, voter := range certTyped.NonpersistentVoters {
		serializableNonpersistentVoters[hex.EncodeToString(hash.Bytes())] = voter
	}
	nonpersistentVotersData, err := json.Marshal(
		serializableNonpersistentVoters,
	)
	if err != nil {
		return fmt.Errorf("serialize non-persistent voters: %w", err)
	}

	var aggregateEligSigData []byte
	if certTyped.AggregateEligSig != nil {
		aggregateEligSigData, err = json.Marshal(certTyped.AggregateEligSig)
		if err != nil {
			return fmt.Errorf("serialize aggregate elig sig: %w", err)
		}
	}

	aggregateVoteSigData, err := json.Marshal(certTyped.AggregateVoteSig)
	if err != nil {
		return fmt.Errorf("serialize aggregate vote sig: %w", err)
	}

	// Create leios eb record
	item := models.LeiosEb{
		ElectionId:              certTyped.ElectionId[:],
		EndorserBlockHash:       certTyped.EndorserBlockHash[:],
		PersistentVotersData:    persistentVotersData,
		NonpersistentVotersData: nonpersistentVotersData,
		AggregateEligSigData:    aggregateEligSigData,
		AggregateVoteSigData:    aggregateVoteSigData,
		AddedSlot:               blockPoint.Slot,
	}

	id, err := createCertificateModel(
		&item,
		[]string{"election_id", "endorser_block_hash"},
		[]string{
			"persistent_voters_data",
			"nonpersistent_voters_data",
			"aggregate_elig_sig_data",
			"aggregate_vote_sig_data",
		},
		txn,
	)
	if err != nil {
		return fmt.Errorf("create leios eb: %w", err)
	}

	_, err = insertCert(id)
	return err
}
