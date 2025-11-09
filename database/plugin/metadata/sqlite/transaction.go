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
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
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

// SetTransaction adds a new transaction to the database
func (d *MetadataStoreSqlite) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	txn *gorm.DB,
) error {
	txHash := tx.Hash().Bytes()
	if txn == nil {
		txn = d.DB()
	}
	tmpTx := &models.Transaction{
		Hash:       txHash,
		Type:       tx.Type(),
		BlockHash:  point.Hash,
		BlockIndex: idx,
	}
	if tx.Metadata() != nil {
		tmpMetadata := tx.Metadata().Cbor()
		tmpTx.Metadata = tmpMetadata
	}
	collateralReturn := tx.CollateralReturn()
	for _, utxo := range tx.Produced() {
		if collateralReturn != nil && utxo.Output == collateralReturn {
			utxo := models.UtxoLedgerToModel(utxo, point.Slot)
			tmpTx.CollateralReturn = &utxo
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
		return fmt.Errorf("create transaction: %w", result.Error)
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
		// Update reference where this Utxo was used as collateral in a
		// Transaction
		if len(caseClauses) > 0 {
			args := append(caseArgs, whereArgs...)
			sql := fmt.Sprintf(
				"UPDATE utxo SET collateral_by_tx_id = CASE %s ELSE collateral_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			result = txn.Exec(sql, args...)
			if result.Error != nil {
				return fmt.Errorf("batch update collateral: %w", result.Error)
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
		// Update reference where this Utxo was used as a reference input in a
		// Transaction
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
	return nil
}
