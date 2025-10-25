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
	result := txn.First(ret, "hash = ?", hash)
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
	if txn == nil {
		txn = d.DB()
	}
	tmpTx := &models.Transaction{
		Hash:       tx.Hash().Bytes(),
		Type:       tx.Type(),
		BlockHash:  point.Hash,
		BlockIndex: idx,
	}
	collateralReturn := tx.CollateralReturn()
	if tx.IsValid() {
		for _, utxo := range tx.Produced() {
			if utxo.Output == collateralReturn {
				tmpTx.CollateralReturn = models.UtxoLedgerToModel(utxo, point.Slot)
				continue
			}
			tmpTx.Outputs = append(
				tmpTx.Outputs,
				models.UtxoLedgerToModel(utxo, point.Slot),
			)
		}
	}
	result := txn.Create(&tmpTx)
	if result.Error != nil {
		return fmt.Errorf("create transaction: %w", result.Error)
	}
	// Explicitly create produced Utxos with TransactionID set
	if tx.IsValid() && len(tmpTx.Outputs) > 0 {
		for o := range tmpTx.Outputs {
			tmpTx.Outputs[o].TransactionID = &tmpTx.ID
		}
		result = txn.Clauses(clause.OnConflict{
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
		tmpTx.Collateral = append(
			tmpTx.Collateral,
			*utxo,
		)
	}
	// Add ReferenceInputs to Transaction
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
		tmpTx.ReferenceInputs = append(
			tmpTx.ReferenceInputs,
			*utxo,
		)
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
