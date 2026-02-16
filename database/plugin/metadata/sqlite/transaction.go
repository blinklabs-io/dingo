// Copyright 2026 Blink Labs Software
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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dbFromTxn returns d.DB() only when txn is nil, unwraps known
// *sqliteTxn or provider.MetadataTxn() when available, and returns
// nil for unrecognized txn types so callers can detect errors.
func (d *MetadataStoreSqlite) dbFromTxn(txn types.Txn) *gorm.DB {
	if txn == nil {
		return d.DB()
	}
	if stx, ok := txn.(*sqliteTxn); ok && stx != nil {
		return stx.db
	}
	if provider, ok := txn.(interface {
		MetadataTxn() *gorm.DB
	}); ok {
		if db := provider.MetadataTxn(); db != nil {
			return db
		}
	}
	// Return nil for unrecognized txn types to allow callers to
	// detect errors
	return nil
}

// resolveDB returns the *gorm.DB for the given transaction, or
// d.DB() if txn is nil. Returns nil, ErrTxnWrongType if txn is
// non-nil but not the expected type.
func (d *MetadataStoreSqlite) resolveDB(
	txn types.Txn,
) (*gorm.DB, error) {
	if stx, ok := txn.(*sqliteTxn); ok {
		if stx != nil && stx.beginErr != nil {
			return nil, stx.beginErr
		}
	}
	if txn == nil {
		return d.DB(), nil
	}
	db := d.dbFromTxn(txn)
	if db == nil {
		return nil, types.ErrTxnWrongType
	}
	return db, nil
}

// resolveReadDB returns the read-optimized *gorm.DB for the given
// transaction. When txn is nil, it returns d.ReadDB() which uses
// the separate read connection pool for file-based databases.
// When txn is non-nil, it returns the transaction's DB handle
// (which is always from the write pool).
func (d *MetadataStoreSqlite) resolveReadDB(
	txn types.Txn,
) (*gorm.DB, error) {
	if stx, ok := txn.(*sqliteTxn); ok {
		if stx != nil && stx.beginErr != nil {
			return nil, stx.beginErr
		}
	}
	if txn == nil {
		return d.ReadDB(), nil
	}
	db := d.dbFromTxn(txn)
	if db == nil {
		return nil, types.ErrTxnWrongType
	}
	return db, nil
}

// addressSQLCondition builds a raw SQL WHERE fragment and
// bind args for matching UTxO rows by payment/staking key.
// The prefix is the table alias (e.g. "u"). Returns an
// empty condition if the address has neither key.
func addressSQLCondition(
	addr lcommon.Address,
	prefix string,
) (string, []any) {
	zeroHash := lcommon.NewBlake2b224(nil)
	hasPayment := addr.PaymentKeyHash() != zeroHash
	hasStake := addr.StakeKeyHash() != zeroHash

	switch {
	case hasPayment && hasStake:
		return fmt.Sprintf(
				"%s.payment_key = ? AND %s.staking_key = ?",
				prefix, prefix,
			), []any{
				addr.PaymentKeyHash().Bytes(),
				addr.StakeKeyHash().Bytes(),
			}
	case hasPayment:
		return prefix + ".payment_key = ?",
			[]any{addr.PaymentKeyHash().Bytes()}
	case hasStake:
		return prefix + ".staking_key = ?",
			[]any{addr.StakeKeyHash().Bytes()}
	default:
		return "", nil
	}
}

// GetTransactionByHash returns a transaction by its hash
func (d *MetadataStoreSqlite) GetTransactionByHash(
	hash []byte,
	txn types.Txn,
) (*models.Transaction, error) {
	ret := &models.Transaction{}
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.
		Preload(clause.Associations).
		Preload("Inputs.Assets").
		Preload("Outputs.Assets").
		Preload("Collateral.Assets").
		Preload("ReferenceInputs.Assets").
		First(ret, "hash = ?", hash)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// GetTransactionsByBlockHash returns all transactions for a given
// block hash, ordered by their position within the block.
func (d *MetadataStoreSqlite) GetTransactionsByBlockHash(
	blockHash []byte,
	txn types.Txn,
) ([]models.Transaction, error) {
	var ret []models.Transaction
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.
		Where("block_hash = ?", blockHash).
		Order("block_index ASC").
		Preload(clause.Associations).
		Preload("Inputs.Assets").
		Preload("Outputs.Assets").
		Preload("Collateral.Assets").
		Preload("ReferenceInputs.Assets").
		Find(&ret)
	if result.Error != nil {
		return nil, fmt.Errorf("get txs by block %x: %w", blockHash, result.Error)
	}
	return ret, nil
}

// GetTransactionsByAddress returns transactions that involve
// a given address as either a sender (input) or receiver
// (output). Results are ordered by slot descending with
// pagination support.
func (d *MetadataStoreSqlite) GetTransactionsByAddress(
	addr lcommon.Address,
	limit int,
	offset int,
	txn types.Txn,
) ([]models.Transaction, error) {
	var ret []models.Transaction
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}

	cond, args := addressSQLCondition(addr, "u")
	if cond == "" {
		return ret, nil
	}

	subQuery := fmt.Sprintf(
		"id IN ("+
			"SELECT DISTINCT t.id "+
			"FROM transaction t "+
			"JOIN utxo u ON "+
			"(u.transaction_id = t.id "+
			"OR u.spent_at_tx_id = t.hash) "+
			"WHERE %s"+
			")",
		cond,
	)

	query := db.
		Where(subQuery, args...).
		Order("slot DESC").
		Preload(clause.Associations).
		Preload("Inputs.Assets").
		Preload("Outputs.Assets").
		Preload("Collateral.Assets").
		Preload("ReferenceInputs.Assets")

	if limit > 0 {
		query = query.Limit(limit)
	}
	if offset > 0 {
		query = query.Offset(offset)
	}

	result := query.Find(&ret)
	if result.Error != nil {
		return nil, fmt.Errorf(
			"get txs by address: %w", result.Error,
		)
	}
	return ret, nil
}

// processScripts is a generic helper to process any script type
func processScripts[T lcommon.Script](
	db *gorm.DB,
	transactionID uint,
	scriptType uint8,
	scripts []T,
	point ocommon.Point,
) error {
	for _, script := range scripts {
		witnessScript := models.WitnessScripts{
			TransactionID: transactionID,
			Type:          scriptType,
			ScriptHash:    script.Hash().Bytes(),
		}
		if result := db.Create(&witnessScript); result.Error != nil {
			return fmt.Errorf("create witness script: %w", result.Error)
		}
		scriptContent := models.Script{
			Hash:        script.Hash().Bytes(),
			Type:        scriptType,
			Content:     script.RawScriptBytes(),
			CreatedSlot: point.Slot,
		}
		if result := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "hash"}},
			DoNothing: true,
		}).Create(&scriptContent); result.Error != nil {
			return fmt.Errorf("create script content: %w", result.Error)
		}
	}
	return nil
}

// certRequiresDeposit returns true if the certificate type requires a deposit
func certRequiresDeposit(cert lcommon.Certificate) bool {
	switch cert.(type) {
	case *lcommon.PoolRegistrationCertificate,
		*lcommon.RegistrationCertificate,
		*lcommon.RegistrationDrepCertificate,
		*lcommon.StakeRegistrationCertificate,
		*lcommon.StakeRegistrationDelegationCertificate,
		*lcommon.StakeVoteRegistrationDelegationCertificate,
		*lcommon.VoteRegistrationDelegationCertificate:
		return true
	default:
		return false
	}
}

// getOrCreateAccount retrieves an existing account or creates a new one.
// Uses a SELECT-then-INSERT pattern with OnConflict to handle race conditions
// where two goroutines could both attempt to create the same account.
func (d *MetadataStoreSqlite) getOrCreateAccount(
	stakeKey []byte,
	txn types.Txn,
) (*models.Account, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}

	// First, try to find an existing account
	var existing models.Account
	result := db.Where("staking_key = ?", stakeKey).First(&existing)
	if result.Error == nil {
		// Account exists - mark for reactivation if inactive
		if !existing.Active {
			existing.Active = true
		}
		return &existing, nil
	}
	if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, result.Error
	}

	// Account doesn't exist - try to create with OnConflict to handle races.
	// If another goroutine creates the same account concurrently, this will
	// do nothing and we'll fetch the existing record below.
	tmpAccount := &models.Account{
		StakingKey: stakeKey,
		Active:     true,
	}
	result = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "staking_key"}},
		DoNothing: true,
	}).Create(tmpAccount)
	if result.Error != nil {
		return nil, result.Error
	}

	// If no row was inserted (conflict occurred), fetch the existing record
	if result.RowsAffected == 0 {
		result = db.Where("staking_key = ?", stakeKey).First(tmpAccount)
		if result.Error != nil {
			return nil, result.Error
		}
		// Mark for reactivation if inactive
		if !tmpAccount.Active {
			tmpAccount.Active = true
		}
	}

	return tmpAccount, nil
}

// saveAccount persists the account to the database. It creates a new
// record when `account.ID == 0` (with an upsert on `staking_key`) or saves
// the existing record otherwise.
func saveAccount(account *models.Account, db *gorm.DB) error {
	if account.ID == 0 {
		result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "staking_key"}},
			DoUpdates: clause.AssignmentColumns(
				[]string{"pool", "drep"},
			),
		}).Create(account)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := db.Save(account)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// saveCertRecord saves a certificate record and returns any error
func saveCertRecord(record any, db *gorm.DB) error {
	result := db.Create(record)
	return result.Error
}

// SetTransaction adds a new transaction to the database and processes all certificates
func (d *MetadataStoreSqlite) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	txn types.Txn,
) error {
	txHash := tx.Hash().Bytes()
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	tmpTx := &models.Transaction{
		Hash:       txHash,
		Type:       tx.Type(),
		BlockHash:  point.Hash,
		BlockIndex: idx,
		Slot:       point.Slot,
		Fee:        types.Uint64(tx.Fee().Uint64()),
		TTL:        types.Uint64(tx.TTL()),
		Valid:      tx.IsValid(),
	}
	if tx.Metadata() != nil {
		tmpMetadata, err := cbor.Encode(tx.Metadata())
		if err != nil {
			return fmt.Errorf("failed to encode metadata: %w", err)
		}
		tmpTx.Metadata = tmpMetadata
	}
	collateralReturn := tx.CollateralReturn()
	// Store all produced UTxOs - tx.Produced() returns correct indices for both
	// valid transactions (regular outputs at indices 0, 1, ...) and invalid
	// transactions (collateral return at index len(Outputs()))
	for _, utxo := range tx.Produced() {
		if collateralReturn != nil && utxo.Output == collateralReturn {
			m := models.UtxoLedgerToModel(utxo, point.Slot)
			tmpTx.CollateralReturn = &m
			continue
		}
		m := models.UtxoLedgerToModel(utxo, point.Slot)
		tmpTx.Outputs = append(tmpTx.Outputs, m)
	}

	// Store outputs in a separate slice for explicit creation later
	// GORM's Create with OnConflict doesn't properly handle associations
	outputsToCreate := tmpTx.Outputs
	tmpTx.Outputs = nil

	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "hash"}}, // unique txn hash
		DoUpdates: clause.AssignmentColumns(
			[]string{"block_hash", "block_index", "slot"},
		),
	}).Create(tmpTx)

	// Restore outputs for later explicit creation
	tmpTx.Outputs = outputsToCreate

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
	// SQLite's ON CONFLICT clause doesn't return the ID of an existing row when
	// the conflict path is taken (no insert occurs). We need to fetch the ID
	// explicitly so we can associate witness records with the correct transaction.
	// Only fetch the ID column to avoid expensive association preloads.
	if tmpTx.ID == 0 {
		var existing struct{ ID uint }
		if err := db.Model(&models.Transaction{}).
			Select("id").
			Where("hash = ?", txHash).
			Take(&existing).Error; err != nil {
			return fmt.Errorf(
				"failed to fetch transaction ID after upsert: %w",
				err,
			)
		}
		tmpTx.ID = existing.ID
	}

	// Create UTxO records for outputs
	// OnConflict doesn't process associations, so we create them explicitly
	for i := range tmpTx.Outputs {
		tmpTx.Outputs[i].ID = 0 // Reset ID to let GORM auto-increment
		tmpTx.Outputs[i].TransactionID = &tmpTx.ID
		result := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
			DoNothing: true,
		}).Create(&tmpTx.Outputs[i])
		if result.Error != nil {
			return fmt.Errorf(
				"create utxo output %d for tx %x: %w",
				i,
				txHash,
				result.Error,
			)
		}
	}
	// Create CollateralReturn UTxO if present
	// Uses CollateralReturnForTxID (not TransactionID) to distinguish from regular outputs
	if tmpTx.CollateralReturn != nil {
		tmpTx.CollateralReturn.CollateralReturnForTxID = &tmpTx.ID
		if result := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
			DoNothing: true,
		}).Create(tmpTx.CollateralReturn); result.Error != nil {
			return fmt.Errorf("create collateral return utxo: %w", result.Error)
		}
	}

	// Add Inputs to Transaction - batch fetch all input UTXOs
	inputRefs := make([]UtxoRef, 0, len(tx.Inputs()))
	for _, input := range tx.Inputs() {
		inputRefs = append(inputRefs, UtxoRef{
			TxId:      input.Id().Bytes(),
			OutputIdx: input.Index(),
		})
	}
	inputUtxos, err := d.GetUtxosBatch(inputRefs, txn)
	if err != nil {
		return fmt.Errorf("failed to batch fetch input UTXOs: %w", err)
	}
	for _, input := range tx.Inputs() {
		key := fmt.Sprintf("%x:%d", input.Id().Bytes(), input.Index())
		utxo := inputUtxos[key]
		if utxo == nil {
			d.logger.Warn(
				"Skipping missing input UTxO",
				"hash",
				input.Id().String(),
				"index",
				input.Index(),
			)
			continue
		}
		tmpTx.Inputs = append(
			tmpTx.Inputs,
			*utxo,
		)
	}
	// Add Collateral to Transaction - batch fetch all collateral UTXOs
	if len(tx.Collateral()) > 0 {
		collateralRefs := make([]UtxoRef, 0, len(tx.Collateral()))
		for _, input := range tx.Collateral() {
			collateralRefs = append(collateralRefs, UtxoRef{
				TxId:      input.Id().Bytes(),
				OutputIdx: input.Index(),
			})
		}
		collateralUtxos, err := d.GetUtxosBatch(collateralRefs, txn)
		if err != nil {
			return fmt.Errorf("failed to batch fetch collateral UTXOs: %w", err)
		}

		var caseClauses []string
		var whereConditions []string
		var caseArgs []any
		var whereArgs []any

		for _, input := range tx.Collateral() {
			inTxId := input.Id().Bytes()
			inIdx := input.Index()
			key := fmt.Sprintf("%x:%d", inTxId, inIdx)
			utxo := collateralUtxos[key]
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
			result = db.Exec(sql, args...)
			if result.Error != nil {
				return fmt.Errorf("batch update collateral: %w", result.Error)
			}
		}
	}
	// Add ReferenceInputs to Transaction - batch fetch all reference input UTXOs
	if len(tx.ReferenceInputs()) > 0 {
		var refInputRefs []UtxoRef
		for _, input := range tx.ReferenceInputs() {
			refInputRefs = append(refInputRefs, UtxoRef{
				TxId:      input.Id().Bytes(),
				OutputIdx: input.Index(),
			})
		}
		refInputUtxos, err := d.GetUtxosBatch(refInputRefs, txn)
		if err != nil {
			return fmt.Errorf(
				"failed to batch fetch reference input UTXOs: %w",
				err,
			)
		}

		var caseClauses []string
		var whereConditions []string
		var caseArgs []any
		var whereArgs []any

		for _, input := range tx.ReferenceInputs() {
			inTxId := input.Id().Bytes()
			inIdx := input.Index()
			key := fmt.Sprintf("%x:%d", inTxId, inIdx)
			utxo := refInputUtxos[key]
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
			result = db.Exec(sql, args...)
			if result.Error != nil {
				return fmt.Errorf(
					"batch update reference inputs: %w",
					result.Error,
				)
			}
		}
	}

	// Consume UTxOs using optimistic locking to prevent race conditions.
	// The atomic update ensures only one transaction can successfully mark a UTXO as spent.
	for _, input := range tx.Consumed() {
		inTxId := input.Id().Bytes()
		inIdx := input.Index()

		// Use atomic check-and-update: only update if UTXO is still unspent.
		// This prevents TOCTOU race where:
		// 1. GetUtxo reads UTXO with deleted_slot=0
		// 2. Another transaction marks it spent
		// 3. First transaction commits spend anyway
		result = db.Model(&models.Utxo{}).
			Where("tx_id = ? AND output_idx = ?", inTxId, inIdx).
			Where("deleted_slot = 0").       // Only update if still unspent
			Where("spent_at_tx_id IS NULL"). // Require NULL for first spend
			Updates(map[string]any{
				"deleted_slot":   point.Slot,
				"spent_at_tx_id": txHash,
			})
		if result.Error != nil {
			return result.Error
		}

		// Check if update succeeded - if no rows affected, the UTXO was already spent
		// or doesn't exist
		if result.RowsAffected == 0 {
			// Check if UTXO exists at all (may be missing or already spent)
			var existingUtxo models.Utxo
			checkResult := db.Where("tx_id = ? AND output_idx = ?", inTxId, inIdx).
				First(&existingUtxo)
			if checkResult.Error != nil {
				if errors.Is(checkResult.Error, gorm.ErrRecordNotFound) {
					d.logger.Warn(
						"input UTxO not found",
						"hash",
						input.Id().String(),
						"index",
						inIdx,
					)
					continue
				}
				return fmt.Errorf(
					"failed to check UTXO %x#%d: %w",
					inTxId,
					inIdx,
					checkResult.Error,
				)
			}
			// UTXO exists but was already spent - check if by the same transaction (idempotent retry)
			if existingUtxo.SpentAtTxId != nil &&
				bytes.Equal(existingUtxo.SpentAtTxId, txHash) {
				// Same transaction, this is an idempotent retry - OK to continue
				continue
			}
			// UTXO was spent by a different transaction - this is a conflict
			return fmt.Errorf(
				"%w: %x:%d",
				types.ErrUtxoConflict,
				inTxId,
				inIdx,
			)
		}
	}
	// Extract and save witness set data
	// Delete existing witness records to ensure idempotency on retry.
	// Note: Caller's transaction (via txn parameter) already provides atomicity,
	// so we don't need a nested db.Transaction() which would create an unnecessary savepoint.
	if result := db.Where("transaction_id = ?", tmpTx.ID).Delete(&models.KeyWitness{}); result.Error != nil {
		return fmt.Errorf("failed to delete key witnesses: %w", result.Error)
	}
	if result := db.Where("transaction_id = ?", tmpTx.ID).Delete(&models.WitnessScripts{}); result.Error != nil {
		return fmt.Errorf("failed to delete witness scripts: %w", result.Error)
	}
	if result := db.Where("transaction_id = ?", tmpTx.ID).Delete(&models.Redeemer{}); result.Error != nil {
		return fmt.Errorf("failed to delete redeemers: %w", result.Error)
	}
	if result := db.Where("transaction_id = ?", tmpTx.ID).Delete(&models.PlutusData{}); result.Error != nil {
		return fmt.Errorf("failed to delete plutus data: %w", result.Error)
	}
	ws := tx.Witnesses()
	if ws != nil {
		// Add Vkey Witnesses
		for _, vkey := range ws.Vkey() {
			keyWitness := models.KeyWitness{
				TransactionID: tmpTx.ID,
				Type:          models.KeyWitnessTypeVkey,
				Vkey:          vkey.Vkey,
				Signature:     vkey.Signature,
			}
			if result := db.Create(&keyWitness); result.Error != nil {
				return fmt.Errorf("create vkey witness: %w", result.Error)
			}
		}

		// Add Bootstrap Witnesses
		for _, bootstrap := range ws.Bootstrap() {
			keyWitness := models.KeyWitness{
				TransactionID: tmpTx.ID,
				Type:          models.KeyWitnessTypeBootstrap,
				PublicKey:     bootstrap.PublicKey,
				Signature:     bootstrap.Signature,
				ChainCode:     bootstrap.ChainCode,
				Attributes:    bootstrap.Attributes,
			}
			if result := db.Create(&keyWitness); result.Error != nil {
				return fmt.Errorf("create bootstrap witness: %w", result.Error)
			}
		}

		// Process all script types using the generic helper
		if err := processScripts(db, tmpTx.ID, uint8(lcommon.ScriptRefTypeNativeScript), ws.NativeScripts(), point); err != nil {
			return err
		}
		if err := processScripts(db, tmpTx.ID, uint8(lcommon.ScriptRefTypePlutusV1), ws.PlutusV1Scripts(), point); err != nil {
			return err
		}
		if err := processScripts(db, tmpTx.ID, uint8(lcommon.ScriptRefTypePlutusV2), ws.PlutusV2Scripts(), point); err != nil {
			return err
		}
		if err := processScripts(db, tmpTx.ID, uint8(lcommon.ScriptRefTypePlutusV3), ws.PlutusV3Scripts(), point); err != nil {
			return err
		}

		// Add PlutusData (Datums)
		for _, datum := range ws.PlutusData() {
			plutusData := models.PlutusData{
				TransactionID: tmpTx.ID,
				Data:          datum.Cbor(),
			}
			if result := db.Create(&plutusData); result.Error != nil {
				return fmt.Errorf("create plutus data: %w", result.Error)
			}
		}

		// Add Redeemers
		if ws.Redeemers() != nil {
			for key, value := range ws.Redeemers().Iter() {
				//nolint:gosec
				redeemer := models.Redeemer{
					TransactionID: tmpTx.ID,
					Tag:           uint8(key.Tag),
					Index:         key.Index,
					Data:          value.Data.Cbor(),
					ExUnitsMemory: uint64(
						max(0, value.ExUnits.Memory),
					),
					ExUnitsCPU: uint64(
						max(0, value.ExUnits.Steps),
					),
				}
				if result := db.Create(&redeemer); result.Error != nil {
					return fmt.Errorf("create redeemer: %w", result.Error)
				}
			}
		}
	}

	// Avoid updating associations
	result = db.Omit(clause.Associations).Save(tmpTx)
	if result.Error != nil {
		return result.Error
	}

	// Process certificates - all certificate types are handled here in a consolidated manner
	// This centralizes certificate processing logic within the metadata layer following DRY principles
	if tx.IsValid() {
		certs := tx.Certificates()
		if len(certs) > 0 {
			// Delete existing specialized certificate records to ensure idempotency on retry
			// This ensures 1:1 correspondence between unified and specialized certificates
			// Wrap in transaction for atomicity - ensures all certificate data is deleted together
			unifiedIDs := []uint{}
			if result := db.Model(&models.Certificate{}).Where("transaction_id = ?", tmpTx.ID).Pluck("id", &unifiedIDs); result.Error != nil {
				return fmt.Errorf(
					"query existing unified certificates: %w",
					result.Error,
				)
			}
			if len(unifiedIDs) > 0 {
				// Delete specialized records linked to existing unified certificates.
				// Note: Caller's transaction already provides atomicity, so no nested transaction needed.
				tables := []string{
					"stake_registration", "pool_registration", "pool_retirement", "auth_committee_hot", "resign_committee_cold",
					"deregistration", "stake_delegation", "stake_registration_delegation", "stake_vote_delegation",
					"stake_vote_registration_delegation", "registration", "registration_drep", "deregistration_drep",
					"update_drep", "vote_delegation", "vote_registration_delegation", "move_instantaneous_rewards",
				}
				for _, table := range tables {
					if result := db.Table(table).Where("certificate_id IN ?", unifiedIDs).Delete(nil); result.Error != nil {
						return fmt.Errorf(
							"delete existing %s records: %w",
							table,
							result.Error,
						)
					}
				}
			}
			// Create unified certificate records first (idempotent with ON CONFLICT DO NOTHING)
			certIDMap := make(map[int]uint)
			certIDUpdates := make(map[uint]uint) // unifiedID -> specializedID
			for i, cert := range certs {
				var certType uint
				switch cert.(type) {
				case *lcommon.PoolRegistrationCertificate:
					certType = uint(lcommon.CertificateTypePoolRegistration)
				case *lcommon.StakeRegistrationCertificate:
					certType = uint(lcommon.CertificateTypeStakeRegistration)
				case *lcommon.PoolRetirementCertificate:
					certType = uint(lcommon.CertificateTypePoolRetirement)
				case *lcommon.StakeDeregistrationCertificate:
					certType = uint(lcommon.CertificateTypeStakeDeregistration)
				case *lcommon.DeregistrationCertificate:
					certType = uint(lcommon.CertificateTypeDeregistration)
				case *lcommon.StakeDelegationCertificate:
					certType = uint(lcommon.CertificateTypeStakeDelegation)
				case *lcommon.StakeRegistrationDelegationCertificate:
					certType = uint(lcommon.CertificateTypeStakeRegistrationDelegation)
				case *lcommon.StakeVoteDelegationCertificate:
					certType = uint(lcommon.CertificateTypeStakeVoteDelegation)
				case *lcommon.RegistrationCertificate:
					certType = uint(lcommon.CertificateTypeRegistration)
				case *lcommon.RegistrationDrepCertificate:
					certType = uint(lcommon.CertificateTypeRegistrationDrep)
				case *lcommon.DeregistrationDrepCertificate:
					certType = uint(lcommon.CertificateTypeDeregistrationDrep)
				case *lcommon.UpdateDrepCertificate:
					certType = uint(lcommon.CertificateTypeUpdateDrep)
				case *lcommon.StakeVoteRegistrationDelegationCertificate:
					certType = uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation)
				case *lcommon.VoteRegistrationDelegationCertificate:
					certType = uint(lcommon.CertificateTypeVoteRegistrationDelegation)
				case *lcommon.VoteDelegationCertificate:
					certType = uint(lcommon.CertificateTypeVoteDelegation)
				case *lcommon.AuthCommitteeHotCertificate:
					certType = uint(lcommon.CertificateTypeAuthCommitteeHot)
				case *lcommon.ResignCommitteeColdCertificate:
					certType = uint(lcommon.CertificateTypeResignCommitteeCold)
				case *lcommon.MoveInstantaneousRewardsCertificate:
					certType = uint(lcommon.CertificateTypeMoveInstantaneousRewards)
				default:
					d.logger.Warn("unknown certificate type", "type", fmt.Sprintf("%T", cert))
					continue
				}
				unifiedCert := models.Certificate{
					TransactionID: tmpTx.ID,
					CertIndex:     uint(i), //nolint:gosec
					CertType:      certType,
					Slot:          point.Slot,
					BlockHash:     point.Hash,
					CertificateID: 0, // Will be set to specialized record ID later if needed
				}
				// Use ON CONFLICT DO NOTHING to handle retries idempotently
				if result := db.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "transaction_id"}, {Name: "cert_index"}},
					DoNothing: true,
				}).Create(&unifiedCert); result.Error != nil {
					return fmt.Errorf(
						"create unified certificate: %w",
						result.Error,
					)
				}
				// If the record already existed, we need to fetch its ID
				if unifiedCert.ID == 0 {
					result := db.
						Where(
							"transaction_id = ? AND cert_index = ?",
							tmpTx.ID,
							uint(i), //nolint:gosec
						).
						First(&unifiedCert)
					if result.Error != nil {
						return fmt.Errorf(
							"fetch existing unified certificate: %w",
							result.Error,
						)
					}
				}
				certIDMap[i] = unifiedCert.ID
			}
			for i, cert := range certs {
				deposit := uint64(0)
				if certDeposits != nil {
					if depositVal, ok := certDeposits[i]; ok {
						deposit = depositVal
					} else if certRequiresDeposit(cert) {
						d.logger.Warn("missing deposit for deposit-bearing certificate",
							"index", i, "type", fmt.Sprintf("%T", cert))
					}
				}
				if certDeposits == nil && certRequiresDeposit(cert) {
					d.logger.Error(
						"certDeposits is nil for deposit-bearing certificate",
						"index",
						i,
						"type",
						fmt.Sprintf("%T", cert),
					)
					return fmt.Errorf(
						"missing certDeposits for deposit-bearing certificate at index %d",
						i,
					)
				}
				switch c := cert.(type) {
				case *lcommon.PoolRegistrationCertificate:
					// Include inactive pools to allow re-registration.
					tmpPool, err := d.GetPool(lcommon.PoolKeyHash(c.Operator[:]), true, txn)
					if err != nil {
						if !errors.Is(err, models.ErrPoolNotFound) {
							return fmt.Errorf("process certificate: %w", err)
						}
					}
					if tmpPool == nil {
						tmpPool = &models.Pool{
							PoolKeyHash: c.Operator[:],
							VrfKeyHash:  c.VrfKeyHash[:],
						}
					}

					// Reactivation handled by writing a registration record.

					// Update pool's current state
					tmpPool.Pledge = types.Uint64(c.Pledge)
					tmpPool.Cost = types.Uint64(c.Cost)
					tmpPool.Margin = &types.Rat{Rat: c.Margin.Rat}
					tmpPool.RewardAccount = c.RewardAccount[:]

					// Create registration record
					tmpReg := models.PoolRegistration{
						PoolKeyHash:   c.Operator[:],
						VrfKeyHash:    c.VrfKeyHash[:],
						Pledge:        types.Uint64(c.Pledge),
						Cost:          types.Uint64(c.Cost),
						Margin:        &types.Rat{Rat: c.Margin.Rat},
						RewardAccount: c.RewardAccount[:],
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}
					if c.PoolMetadata != nil {
						tmpReg.MetadataUrl = c.PoolMetadata.Url
						tmpReg.MetadataHash = c.PoolMetadata.Hash[:]
					}
					for _, owner := range c.PoolOwners {
						tmpReg.Owners = append(
							tmpReg.Owners,
							models.PoolRegistrationOwner{KeyHash: owner[:]},
						)
					}
					tmpPool.Owners = tmpReg.Owners

					var tmpRelay models.PoolRegistrationRelay
					for _, relay := range c.Relays {
						tmpRelay = models.PoolRegistrationRelay{
							Ipv4: relay.Ipv4,
							Ipv6: relay.Ipv6,
						}
						if relay.Port != nil {
							tmpRelay.Port = uint(*relay.Port)
						}
						if relay.Hostname != nil {
							tmpRelay.Hostname = *relay.Hostname
						}
						tmpReg.Relays = append(tmpReg.Relays, tmpRelay)
					}
					tmpPool.Relays = tmpReg.Relays

					// Set the PoolID for the registration record
					// Use Omit to skip creating associations - they should only be created through PoolRegistration
					if tmpPool.ID == 0 {
						result := db.Omit(clause.Associations).Create(tmpPool)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					} else {
						result := db.Omit(clause.Associations).Save(tmpPool)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}
					tmpReg.PoolID = tmpPool.ID

					// Set PoolID on Owners and Relays for FK constraint
					for i := range tmpReg.Owners {
						tmpReg.Owners[i].PoolID = tmpPool.ID
					}
					for i := range tmpReg.Relays {
						tmpReg.Relays[i].PoolID = tmpPool.ID
					}

					// Save the registration record
					result := db.Create(&tmpReg)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.StakeRegistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpReg := models.StakeRegistration{
						StakingKey:    stakeKey,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					if tmpAccount.ID == 0 {
						tmpAccount.AddedSlot = point.Slot
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.PoolRetirementCertificate:
					// Include inactive pools when retiring.
					tmpPool, err := d.GetPool(lcommon.PoolKeyHash(c.PoolKeyHash[:]), true, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					if tmpPool == nil {
						d.logger.Warn("retiring non-existent pool", "hash", c.PoolKeyHash)
						tmpPool = &models.Pool{PoolKeyHash: c.PoolKeyHash[:]}
						result := db.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "pool_key_hash"}},
							UpdateAll: true,
						}).Create(&tmpPool)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}

					tmpItem := models.PoolRetirement{
						PoolKeyHash:   c.PoolKeyHash[:],
						Epoch:         c.Epoch,
						AddedSlot:     point.Slot,
						PoolID:        tmpPool.ID,
						CertificateID: certIDMap[i],
					}

					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.StakeDeregistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.GetAccount(stakeKey, false, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					if tmpAccount == nil {
						d.logger.Warn("deregistering non-existent account", "hash", stakeKey)
						tmpAccount = &models.Account{
							StakingKey: stakeKey,
						}
						result := db.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "staking_key"}},
							UpdateAll: true,
						}).Create(tmpAccount)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}

					tmpAccount.Active = false
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.StakeDeregistration{
						StakingKey:    stakeKey,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.DeregistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.GetAccount(stakeKey, false, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					if tmpAccount == nil {
						d.logger.Warn("deregistering non-existent account", "hash", stakeKey)
						tmpAccount = &models.Account{
							StakingKey: stakeKey,
						}
						result := db.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "staking_key"}},
							UpdateAll: true,
						}).Create(tmpAccount)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}

					tmpAccount.Active = false
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.Deregistration{
						StakingKey:    stakeKey,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
						Amount:        types.Uint64(deposit),
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.StakeDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.StakeDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.StakeRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.AddedSlot = point.Slot

					tmpReg := models.StakeRegistrationDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.StakeVoteDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.Drep = c.Drep.Credential[:]
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.StakeVoteDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          c.Drep.Credential[:],
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.RegistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpReg := models.Registration{
						StakingKey:    stakeKey,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					if tmpAccount.ID == 0 {
						tmpAccount.AddedSlot = point.Slot
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.RegistrationDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]

					// Registration (re)creates/activates the DRep regardless of prior state.

					tmpReg := models.RegistrationDrep{
						DrepCredential: drepCredential,
						AddedSlot:      point.Slot,
						DepositAmount:  types.Uint64(deposit),
						CertificateID:  certIDMap[i],
					}
					if c.Anchor != nil {
						tmpReg.AnchorUrl = c.Anchor.Url
						tmpReg.AnchorHash = c.Anchor.DataHash[:]
					}

					// Persist DRep anchor and active state
					if err := d.SetDrep(drepCredential, point.Slot, tmpReg.AnchorUrl, tmpReg.AnchorHash, true, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.DeregistrationDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]

					tmpDereg := models.DeregistrationDrep{
						DrepCredential: drepCredential,
						AddedSlot:      point.Slot,
						DepositAmount:  types.Uint64(deposit),
						CertificateID:  certIDMap[i],
					}

					// Mark DRep inactive
					// Ensure we don't create a new DRep during deregistration. Check existence first.
					existingDrep, err := d.GetDrep(drepCredential, true, txn)
					if err != nil {
						if !errors.Is(err, models.ErrDrepNotFound) {
							return fmt.Errorf("process certificate: %w", err)
						}
					}
					if existingDrep == nil {
						return fmt.Errorf("process certificate: %w", models.ErrDrepNotFound)
					}
					if err := d.SetDrep(drepCredential, point.Slot, "", nil, false, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpDereg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpDereg.ID
				case *lcommon.UpdateDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]

					tmpUpdate := models.UpdateDrep{
						Credential:    drepCredential,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if c.Anchor != nil {
						tmpUpdate.AnchorUrl = c.Anchor.Url
						tmpUpdate.AnchorHash = c.Anchor.DataHash[:]
					}

					// Update DRep anchor and mark active
					// Require that the DRep already exists for updates.
					existingDrep, err := d.GetDrep(drepCredential, true, txn)
					if err != nil {
						if !errors.Is(err, models.ErrDrepNotFound) {
							return fmt.Errorf("process certificate: %w", err)
						}
					}
					if existingDrep == nil {
						return fmt.Errorf("process certificate: %w", models.ErrDrepNotFound)
					}
					if err := d.SetDrep(drepCredential, point.Slot, tmpUpdate.AnchorUrl, tmpUpdate.AnchorHash, true, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpUpdate, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpUpdate.ID
				case *lcommon.StakeVoteRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.Drep = c.Drep.Credential[:]
					tmpAccount.AddedSlot = point.Slot

					tmpReg := models.StakeVoteRegistrationDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          c.Drep.Credential[:],
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.VoteRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Drep = c.Drep.Credential[:]
					tmpAccount.AddedSlot = point.Slot

					tmpReg := models.VoteRegistrationDelegation{
						StakingKey:    stakeKey,
						Drep:          c.Drep.Credential[:],
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.VoteDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Drep = c.Drep.Credential[:]
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.VoteDelegation{
						StakingKey:    stakeKey,
						Drep:          c.Drep.Credential[:],
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.AuthCommitteeHotCertificate:
					coldCredential := c.ColdCredential.Credential[:]
					hotCredential := c.HotCredential.Credential[:]

					tmpAuth := models.AuthCommitteeHot{
						ColdCredential: coldCredential,
						HostCredential: hotCredential,
						CertificateID:  certIDMap[i],
						AddedSlot:      point.Slot,
					}

					if err := saveCertRecord(&tmpAuth, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpAuth.ID
				case *lcommon.ResignCommitteeColdCertificate:
					coldCredential := c.ColdCredential.Credential[:]

					tmpResign := models.ResignCommitteeCold{
						ColdCredential: coldCredential,
						CertificateID:  certIDMap[i],
						AddedSlot:      point.Slot,
					}
					if c.Anchor != nil {
						tmpResign.AnchorUrl = c.Anchor.Url
						tmpResign.AnchorHash = c.Anchor.DataHash[:]
					}

					if err := saveCertRecord(&tmpResign, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpResign.ID
				case *lcommon.MoveInstantaneousRewardsCertificate:
					tmpMIR := models.MoveInstantaneousRewards{
						Pot:           c.Reward.Source,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					// Save the MIR record
					result := db.Create(&tmpMIR)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpMIR.ID

					// Save individual rewards
					for credential, amount := range c.Reward.Rewards {
						tmpReward := models.MoveInstantaneousRewardsReward{
							Credential: credential.Credential[:],
							Amount:     types.Uint64(amount),
							MIRID:      tmpMIR.ID,
						}
						result := db.Create(&tmpReward)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}
				default:
					return fmt.Errorf("unsupported certificate type %T", cert)
				}
			}

			// Batch update unified certificates with specialized record IDs
			if len(certIDUpdates) > 0 {
				// Build CASE statement for batch update
				var ids []uint
				var whenClauses []string
				var values []any

				for unifiedID, specializedID := range certIDUpdates {
					ids = append(ids, unifiedID)
					whenClauses = append(whenClauses, "WHEN id = ? THEN ?")
					values = append(values, unifiedID, specializedID)
				}

				caseStmt := strings.Join(whenClauses, " ")
				query := fmt.Sprintf(
					"UPDATE certs SET certificate_id = CASE %s END WHERE id IN (?)",
					caseStmt,
				)
				values = append(values, ids)

				if result := db.Exec(query, values...); result.Error != nil {
					return fmt.Errorf(
						"batch update unified certificates: %w",
						result.Error,
					)
				}
			}
		}

		if err := d.storeTransactionDatums(tx, point.Slot, txn); err != nil {
			return fmt.Errorf("store datums failed: %w", err)
		}
	}

	return nil
}

// SetGenesisTransaction stores a genesis transaction record.
// Genesis transactions have no inputs, witnesses, or fees - just outputs.
func (d *MetadataStoreSqlite) SetGenesisTransaction(
	hash []byte,
	blockHash []byte,
	outputs []models.Utxo,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	tmpTx := &models.Transaction{
		Hash:      hash,
		Type:      0, // Byron era type
		BlockHash: blockHash,
		Slot:      0, // Genesis slot
		Valid:     true,
	}

	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoNothing: true,
	}).Create(tmpTx)
	if result.Error != nil {
		return fmt.Errorf(
			"create genesis transaction %x: %w",
			hash,
			result.Error,
		)
	}

	// Fetch ID if it was an existing record
	if tmpTx.ID == 0 {
		var existing struct{ ID uint }
		if err := db.Model(&models.Transaction{}).
			Select("id").
			Where("hash = ?", hash).
			Take(&existing).Error; err != nil {
			return fmt.Errorf("fetch genesis transaction ID: %w", err)
		}
		tmpTx.ID = existing.ID
	}

	// Create UTxO records for genesis outputs
	// Reset IDs to ensure GORM treats them as new records (prevents upsert issues
	// when outputs already have primary keys set from previous calls)
	for i := range outputs {
		outputs[i].ID = 0 // Reset ID to let GORM auto-increment
		outputs[i].TransactionID = &tmpTx.ID
	}
	if len(outputs) > 0 {
		result := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
			DoNothing: true,
		}).Create(&outputs)
		if result.Error != nil {
			return fmt.Errorf("create genesis utxos: %w", result.Error)
		}
	}

	return nil
}

// Traverse each utxo and check for inline datum & calls storeDatum
func (d *MetadataStoreSqlite) storeTransactionDatums(
	tx lcommon.Transaction,
	slot uint64,
	txn types.Txn,
) error {
	for _, utxo := range tx.Produced() {
		if err := d.storeDatum(utxo.Output.Datum(), slot, txn); err != nil {
			return err
		}
	}
	witnesses := tx.Witnesses()
	if witnesses == nil {
		return nil
	}
	// Looks over the transaction witness set & store each datum.
	for _, datum := range witnesses.PlutusData() {
		datumCopy := datum
		if err := d.storeDatum(&datumCopy, slot, txn); err != nil {
			return err
		}
	}
	return nil
}

// Marshal the raw CBOR and hashes with Blake2b256Hash & calls SetDatum of metadata store.
func (d *MetadataStoreSqlite) storeDatum(
	datum *lcommon.Datum,
	slot uint64,
	txn types.Txn,
) error {
	if datum == nil {
		return nil
	}
	rawDatum := datum.Cbor()
	if len(rawDatum) == 0 {
		var err error
		rawDatum, err = datum.MarshalCBOR()
		if err != nil {
			return fmt.Errorf("marshal datum: %w", err)
		}
	}
	if len(rawDatum) == 0 {
		return nil
	}
	datumHash := lcommon.Blake2b256Hash(rawDatum)
	if err := d.SetDatum(datumHash, rawDatum, slot, txn); err != nil {
		return err
	}
	return nil
}

// GetTransactionHashesAfterSlot returns transaction hashes for transactions added after the given slot.
// This is used for blob cleanup during rollback/truncation.
func (d *MetadataStoreSqlite) GetTransactionHashesAfterSlot(
	slot uint64,
	txn types.Txn,
) ([][]byte, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}

	var txHashes [][]byte
	if result := db.Model(&models.Transaction{}).
		Where("slot > ?", slot).
		Pluck("hash", &txHashes); result.Error != nil {
		return nil, fmt.Errorf("query transaction hashes: %w", result.Error)
	}

	return txHashes, nil
}

// DeleteTransactionsAfterSlot removes transaction records added after the given slot.
// Child records are automatically removed via CASCADE constraints.
// UTXO hash-based foreign keys (spent_at_tx_id, collateral_by_tx_id, referenced_by_tx_id)
// are NULLed out before deleting transactions to prevent orphaned references.
func (d *MetadataStoreSqlite) DeleteTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Get transaction hashes that will be deleted
	var txHashes [][]byte
	if result := db.Model(&models.Transaction{}).
		Where("slot > ?", slot).
		Pluck("hash", &txHashes); result.Error != nil {
		return fmt.Errorf("query transaction hashes: %w", result.Error)
	}

	// NULL out UTXO references to transactions being deleted
	// These fields reference transaction hashes, not IDs, so CASCADE doesn't handle them
	if len(txHashes) > 0 {
		// Clear spent_at_tx_id and reset deleted_slot to restore UTXO active state
		if result := db.Model(&models.Utxo{}).
			Where("spent_at_tx_id IN ?", txHashes).
			Updates(map[string]any{
				"spent_at_tx_id": nil,
				"deleted_slot":   0,
			}); result.Error != nil {
			return fmt.Errorf(
				"clear spent_at_tx_id references: %w",
				result.Error,
			)
		}

		if result := db.Model(&models.Utxo{}).
			Where("collateral_by_tx_id IN ?", txHashes).
			Update("collateral_by_tx_id", nil); result.Error != nil {
			return fmt.Errorf(
				"clear collateral_by_tx_id references: %w",
				result.Error,
			)
		}

		if result := db.Model(&models.Utxo{}).
			Where("referenced_by_tx_id IN ?", txHashes).
			Update("referenced_by_tx_id", nil); result.Error != nil {
			return fmt.Errorf(
				"clear referenced_by_tx_id references: %w",
				result.Error,
			)
		}
	}

	if result := db.Where("slot > ?", slot).Delete(&models.Transaction{}); result.Error != nil {
		return result.Error
	}

	return nil
}
