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
		Preload("Inputs").
		Preload("Outputs").
		Preload("ReferenceInputs").
		Preload("Collateral").
		Preload("CollateralReturn").
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
	collateralReturn := tx.CollateralReturn()
	for _, utxo := range tx.Produced() {
		if collateralReturn != nil && utxo.Output == collateralReturn {
			tmpTx.CollateralReturn = models.UtxoLedgerToModel(utxo, point.Slot)
			continue
		}
		tmpTx.Outputs = append(
			tmpTx.Outputs,
			models.UtxoLedgerToModel(utxo, point.Slot),
		)
	}
	result := txn.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}}, // unique txn hash
		DoUpdates: clause.AssignmentColumns([]string{"block_hash", "block_index", "type"}),
	}).Create(&tmpTx)
	if result.Error != nil {
		return fmt.Errorf("create transaction: %w", result.Error)
	}
	// Explicitly create produced Utxos with TransactionID set
	if tx.IsValid() && len(tmpTx.Outputs) > 0 {
		for o := range tmpTx.Outputs {
			tmpTx.Outputs[o].TransactionID = &tmpTx.ID
		}
		result = txn.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
			UpdateAll: true,
		}).Create(&tmpTx.Outputs)
		if result.Error != nil {
			return fmt.Errorf("create outputs: %w", result.Error)
		}
	}
	// Explicitly create collateral return Utxo with TransactionID set
	if collateralReturn != nil {
		// Verify the collateral return was actually found and populated
		if tmpTx.CollateralReturn.TxId == nil {
			return errors.New("collateral return output not found in produced outputs")
		}
		tmpTx.CollateralReturn.TransactionID = &tmpTx.ID
		result = txn.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
			UpdateAll: true,
		}).Create(&tmpTx.CollateralReturn)
		if result.Error != nil {
			return fmt.Errorf("create collateral return output: %w", result.Error)
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
			// Do nothing
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
		var args []any

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
				// Do nothing
				continue
			}
			caseClauses = append(caseClauses, "WHEN tx_id = ? AND output_idx = ? THEN ?")
			args = append(args, inTxId, inIdx, txHash)
			whereConditions = append(whereConditions, "(tx_id = ? AND output_idx = ?)")
			args = append(args, inTxId, inIdx)
			tmpTx.Collateral = append(
				tmpTx.Collateral,
				*utxo,
			)
		}
		if len(caseClauses) > 0 {
			sql := fmt.Sprintf(
				"UPDATE utxos SET collateral_by_tx_id = CASE %s ELSE collateral_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			result = txn.Exec(sql, args...)
			if result.Error!= nil {
				return fmt.Errorf("batch update collateral: %w", result.Error)
			}
		}
	}
	// Add ReferenceInputs to Transaction
	if len(tx.ReferenceInputs()) > 0 {
		var caseClauses []string
		var whereConditions []string
		var args []any
		
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
				// Do nothing
				continue
			}
			caseClauses = append(caseClauses, "WHEN tx_id = ? AND output_idx = ? THEN ?")
			args = append(args, inTxId, inIdx, txHash)
			whereConditions = append(whereConditions, "(tx_id = ? AND output_idx = ?)")
			args = append(args, inTxId, inIdx)
			tmpTx.ReferenceInputs = append(
				tmpTx.ReferenceInputs,
				*utxo,
			)
		}
		if len(caseClauses) > 0 {
			sql := fmt.Sprintf(
				"UPDATE utxos SET referenced_by_tx_id = CASE %s ELSE referenced_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			result = txn.Exec(sql, args...)
			if result.Error != nil {
				return fmt.Errorf("batch update reference inputs: %w", result.Error)
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
			return fmt.Errorf("input UTxO not found: %x#%d", inTxId, inIdx)
		}
		// Update existing UTxOs
		result = txn.Model(&models.Utxo{}).
			Where("tx_id = ? AND output_idx = ?", inTxId, inIdx).
			Where("spent_at_tx_id IS NULL OR spent_at_tx_id = ?", txHash).
			Updates(map[string]any{
				"deleted_slot":   point.Slot,
				"spent_at_tx_id": tx.Hash().Bytes(),
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
