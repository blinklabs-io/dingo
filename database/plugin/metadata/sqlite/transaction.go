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
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

	// Determine if this transaction already existed (by hash) for idempotency.
	var existingTxCount int64
	if err := txn.Model(&models.Transaction{}).
		Where("hash = ?", txHash).
		Count(&existingTxCount).Error; err != nil {
		return fmt.Errorf("check existing transaction: %w", err)
	}
	alreadyExists := existingTxCount > 0

	// Check if block information has changed for re-inclusion handling
	blockChanged := false
	if alreadyExists {
		var existingTx models.Transaction
		if err := txn.Where("hash = ?", txHash).First(&existingTx).Error; err != nil {
			return fmt.Errorf("fetch existing transaction: %w", err)
		}
		blockChanged = !bytes.Equal(existingTx.BlockHash, point.Hash) ||
			existingTx.BlockIndex != idx
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

	// Only populate outputs if transaction is new
	if !alreadyExists {
		collateralReturn := tx.CollateralReturn()
		for _, utxo := range tx.Produced() {
			if collateralReturn != nil && utxo.Output == collateralReturn {
				utxoModel := models.UtxoLedgerToModel(utxo, point.Slot, true)
				tmpTx.CollateralReturn = &utxoModel
				continue
			}
			tmpTx.Outputs = append(
				tmpTx.Outputs,
				models.UtxoLedgerToModel(utxo, point.Slot, false),
			)
		}
	}
	// Create transaction with ON CONFLICT upsert for idempotency
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

	// If transaction already existed, skip UTXO processing since they're already stored
	if alreadyExists {
		// Go directly to certificate processing
		return d.processTransactionCertificates(
			tx,
			tmpTx,
			point,
			deposits,
			alreadyExists,
			blockChanged,
			txn,
		)
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

	return d.processTransactionCertificates(
		tx,
		tmpTx,
		point,
		deposits,
		alreadyExists,
		blockChanged,
		txn,
	)
}

func (d *MetadataStoreSqlite) processTransactionCertificates(
	tx lcommon.Transaction,
	tmpTx models.Transaction,
	point ocommon.Point,
	deposits map[int]uint64,
	alreadyExists bool,
	blockChanged bool,
	txn *gorm.DB,
) error {
	if !tx.IsValid() {
		return nil
	}

	certs := tx.Certificates()
	if len(certs) == 0 {
		return nil
	}

	// For re-inclusion with different block info, update certificate slots and block hash
	if alreadyExists && blockChanged {
		if err := d.updateCertificateSlots(tmpTx.ID, point.Slot, point.Hash, txn); err != nil {
			return fmt.Errorf(
				"update certificate slots and block hash for re-inclusion: %w",
				err,
			)
		}
		return nil
	}

	// Validate certificate deposits are provided when certificates are present AND transaction is new
	if !alreadyExists && deposits == nil {
		return fmt.Errorf(
			"certificate deposits required when transaction has %d certificates",
			len(certs),
		)
	}

	if alreadyExists {
		// Transaction already existed with identical content, skip certificate processing
		return nil
	}

	// Transaction is new, process all certificates
	for idx, cert := range certs {
		// Get deposit amount for this certificate (default to 0 if not present)
		deposit := uint64(0)
		if amt, exists := deposits[idx]; exists {
			deposit = amt
		}

		// Validate that deposit-requiring certificate types have deposits provided
		certType := models.CertificateType(cert.Type())
		if _, exists := deposits[idx]; !exists {
			// Check if this certificate type requires a deposit
			requiresDeposit := certType == models.CertificateTypeStakeRegistration ||
				certType == models.CertificateTypeRegistration ||
				certType == models.CertificateTypePoolRegistration ||
				certType == models.CertificateTypeRegistrationDrep ||
				certType == models.CertificateTypeStakeRegistrationDelegation ||
				certType == models.CertificateTypeVoteRegistrationDelegation ||
				certType == models.CertificateTypeStakeVoteRegistrationDelegation

			if requiresDeposit {
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

// updateCertificateSlots updates the slot and block hash for all certificates associated with a transaction
func (d *MetadataStoreSqlite) updateCertificateSlots(
	txID uint,
	newSlot uint64,
	newBlockHash []byte,
	txn *gorm.DB,
) error {
	if txn == nil {
		txn = d.DB()
	}

	// Update slot and block_hash in certificates table
	updates := map[string]interface{}{
		"slot":       newSlot,
		"block_hash": newBlockHash,
	}
	if err := txn.Model(&models.Certificate{}).
		Where("transaction_id = ?", txID).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("update certificate slot and block_hash: %w", err)
	}

	// Update added_slot in specialized certificate tables
	// These tables have certificate_id that references the certificates.id
	certTables := []string{
		"pool_registration",
		"pool_retirement",
		"genesis_key_delegations",
		"move_instantaneous_rewards",
		"resign_committee_cold",
		"auth_committee_hot",
		"registration",
		"deregistration",
		"stake_registration",
		"stake_deregistration",
		"stake_delegation",
		"stake_registration_delegation",
		"vote_delegation",
		"stake_vote_delegation",
		"vote_registration_delegation",
		"stake_vote_registration_delegation",
		"registration_drep",
		"deregistration_drep",
		"update_drep",
		"leios_eb",
	}

	for _, table := range certTables {
		// Update added_slot where certificate_id is in the certificates for this transaction
		if err := txn.Table(table).
			Where("certificate_id IN (SELECT id FROM certificates WHERE transaction_id = ?)", txID).
			Update("added_slot", newSlot).Error; err != nil {
			return fmt.Errorf("update %s added_slot: %w", table, err)
		}
	}

	return nil
}
