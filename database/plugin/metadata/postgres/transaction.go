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

package postgres

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dbFromTxn returns d.DB() only when txn is nil, unwraps known *postgresTxn or provider.MetadataTxn() when available, and returns nil for unrecognized txn types so callers can detect errors
func (d *MetadataStorePostgres) dbFromTxn(txn types.Txn) *gorm.DB {
	if txn == nil {
		return d.DB()
	}
	if stx, ok := txn.(*postgresTxn); ok && stx != nil {
		return stx.db
	}
	if provider, ok := txn.(interface{ MetadataTxn() *gorm.DB }); ok {
		if db := provider.MetadataTxn(); db != nil {
			return db
		}
	}
	return nil // Return nil for unrecognized txn types to allow callers to detect errors
}

// resolveDB returns the *gorm.DB for the given transaction, or d.DB() if txn is nil.
// Returns nil, ErrTxnWrongType if txn is non-nil but not the expected type.
func (d *MetadataStorePostgres) resolveDB(txn types.Txn) (*gorm.DB, error) {
	if stx, ok := txn.(*postgresTxn); ok {
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

// resolveReadDB returns the *gorm.DB for read-only queries.
// PostgreSQL uses a single connection pool that handles
// concurrent reads natively, so this delegates to resolveDB.
// This method exists for API consistency with the SQLite
// backend, which uses a separate read pool for WAL mode.
func (d *MetadataStorePostgres) resolveReadDB(
	txn types.Txn,
) (*gorm.DB, error) {
	return d.resolveDB(txn)
}

// GetTransactionByHash returns a transaction by its hash
func (d *MetadataStorePostgres) GetTransactionByHash(
	hash []byte,
	txn types.Txn,
) (*models.Transaction, error) {
	ret := &models.Transaction{}
	db, err := d.resolveDB(txn)
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

// GetTransactionsByBlockHash returns all transactions in a block, ordered by index
func (d *MetadataStorePostgres) GetTransactionsByBlockHash(
	blockHash []byte,
	txn types.Txn,
) ([]models.Transaction, error) {
	var ret []models.Transaction
	db, err := d.resolveDB(txn)
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

// It builds AddressTransaction rows for a single transaction.
// deduplication by (payment_key, staking_key) within the tx.
func collectAddressTransactions(
	transactionID uint,
	slot uint64,
	txIndex uint32,
	utxos []models.Utxo,
) []models.AddressTransaction {
	ret := make([]models.AddressTransaction, 0, len(utxos))
	seen := make(map[string]struct{}, len(utxos))
	for _, utxo := range utxos {
		if len(utxo.PaymentKey) == 0 && len(utxo.StakingKey) == 0 {
			continue
		}
		key := fmt.Sprintf("%x|%x", utxo.PaymentKey, utxo.StakingKey)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, models.AddressTransaction{
			PaymentKey:    append([]byte(nil), utxo.PaymentKey...),
			StakingKey:    append([]byte(nil), utxo.StakingKey...),
			TransactionID: transactionID,
			Slot:          slot,
			TxIndex:       txIndex,
		})
	}
	return ret
}

// GetTransactionsByAddress returns transactions that involve
// the given payment/staking key with pagination support.
func (d *MetadataStorePostgres) GetTransactionsByAddress(
	paymentKey []byte,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.Transaction, error) {
	var ret []models.Transaction
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}

	if len(paymentKey) == 0 && len(stakingKey) == 0 {
		return ret, nil
	}

	addrQuery := db.Model(&models.AddressTransaction{})
	switch {
	case len(paymentKey) > 0 && len(stakingKey) > 0:
		addrQuery = addrQuery.Where(
			"payment_key = ? AND staking_key = ?",
			paymentKey,
			stakingKey,
		)
	case len(paymentKey) > 0:
		addrQuery = addrQuery.Where(
			"payment_key = ? AND (staking_key IS NULL OR OCTET_LENGTH(staking_key) = 0)",
			paymentKey,
		)
	default:
		addrQuery = addrQuery.Where("staking_key = ?", stakingKey)
	}

	subQuery := addrQuery.Select("DISTINCT transaction_id")
	direction := "DESC"
	if strings.EqualFold(order, "asc") {
		direction = "ASC"
	}
	query := db.
		Where("id IN (?)", subQuery).
		Order(fmt.Sprintf("slot %s, block_index %s, id %s", direction, direction, direction)).
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

// GetAddressesByStakingKey returns distinct addresses mapped to a staking key.
func (d *MetadataStorePostgres) GetAddressesByStakingKey(
	stakingKey []byte,
	limit int,
	offset int,
	txn types.Txn,
) ([]models.AddressTransaction, error) {
	var ret []models.AddressTransaction
	if len(stakingKey) == 0 {
		return ret, nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	query := db.Model(&models.AddressTransaction{}).
		Select("MIN(id) AS id, payment_key, staking_key").
		Where("staking_key = ?", stakingKey).
		Group("payment_key, staking_key").
		Order("payment_key ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	if offset > 0 {
		query = query.Offset(offset)
	}
	if result := query.Find(&ret); result.Error != nil {
		return nil, fmt.Errorf("get addresses by staking key: %w", result.Error)
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

// getOrCreateAccount retrieves an existing account or creates a new one
func (d *MetadataStorePostgres) getOrCreateAccount(
	stakeKey []byte,
	txn types.Txn,
) (*models.Account, error) {
	// Include inactive accounts to allow reactivation on registration.
	tmpAccount, err := d.GetAccount(stakeKey, true, txn)
	if err != nil {
		if !errors.Is(err, models.ErrAccountNotFound) {
			return nil, err
		}
	}
	if tmpAccount == nil {
		tmpAccount = &models.Account{
			StakingKey: stakeKey,
		}
	} else if !tmpAccount.Active {
		tmpAccount.Active = true
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
				[]string{
					"pool",
					"drep",
					"active",
					"certificate_id",
				},
			),
		},
			clause.Returning{Columns: []clause.Column{{Name: "id"}}},
		).Create(account)
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
func (d *MetadataStorePostgres) SetTransaction(
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
	// Safely convert tx.Fee() (*big.Int) to uint64
	var feeUint uint64
	if txFee := tx.Fee(); txFee != nil {
		if txFee.BitLen() > 64 {
			feeUint = math.MaxUint64
		} else {
			feeUint = txFee.Uint64()
		}
	}
	tmpTx := &models.Transaction{
		Hash:       txHash,
		Type:       tx.Type(),
		BlockHash:  point.Hash,
		BlockIndex: idx,
		Slot:       point.Slot,
		Fee:        types.Uint64(feeUint),
		TTL:        types.Uint64(tx.TTL()),
		Valid:      tx.IsValid(),
	}
	if tx.Metadata() != nil && d.storageMode == types.StorageModeAPI {
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
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "hash"}}, // unique txn hash
		DoUpdates: clause.AssignmentColumns(
			[]string{"block_hash", "block_index", "slot"},
		),
	}).Create(tmpTx)
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
	// Defensive: when an upsert hits a conflict path, we may not have an ID for
	// the existing row. Fetch it explicitly so we can link witness records to
	// the correct transaction (behavior varies by driver/DB).
	if tmpTx.ID == 0 {
		existingTx, err := d.GetTransactionByHash(txHash, txn)
		if err != nil {
			return fmt.Errorf(
				"failed to fetch transaction ID after upsert: %w",
				err,
			)
		}
		if existingTx == nil {
			return fmt.Errorf("transaction not found after upsert: %x", txHash)
		}
		tmpTx.ID = existingTx.ID
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
			result = db.Exec(sql, args...)
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

	// Consume UTxOs
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
				"input UTxO not found",
				"hash",
				input.Id().String(),
				"index",
				inIdx,
			)
			continue
		}
		// Update existing UTxOs
		result = db.Model(&models.Utxo{}).
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
	// Address indexing, witnesses, scripts, redeemers, and plutus data only stored in API mode
	if d.storageMode == types.StorageModeAPI {
		// Index unique addresses participating in this transaction.
		// It checks both transaction input & output.
		addressUtxos := make(
			[]models.Utxo,
			0,
			len(tmpTx.Inputs)+len(tmpTx.Collateral)+len(tmpTx.Outputs)+1,
		)
		addressUtxos = append(addressUtxos, tmpTx.Inputs...)
		addressUtxos = append(addressUtxos, tmpTx.Collateral...)
		addressUtxos = append(addressUtxos, tmpTx.Outputs...)
		if tmpTx.CollateralReturn != nil {
			addressUtxos = append(addressUtxos, *tmpTx.CollateralReturn)
		}
		addressTxs := collectAddressTransactions(
			tmpTx.ID,
			point.Slot,
			idx,
			addressUtxos,
		)
		if result := db.Where("transaction_id = ?", tmpTx.ID).
			Delete(&models.AddressTransaction{}); result.Error != nil {
			return fmt.Errorf("delete existing address transactions: %w", result.Error)
		}
		if len(addressTxs) > 0 {
			if result := db.Create(&addressTxs); result.Error != nil {
				return fmt.Errorf("create address transactions: %w", result.Error)
			}
		}
		// Extract and save witness set data
		// Delete existing witness records to ensure idempotency on retry
		result := db.Where(
			"transaction_id = ?", tmpTx.ID,
		).Delete(&models.KeyWitness{})
		if result.Error != nil {
			return fmt.Errorf(
				"delete existing key witnesses: %w",
				result.Error,
			)
		}
		result = db.Where(
			"transaction_id = ?", tmpTx.ID,
		).Delete(&models.WitnessScripts{})
		if result.Error != nil {
			return fmt.Errorf(
				"delete existing witness scripts: %w",
				result.Error,
			)
		}
		result = db.Where(
			"transaction_id = ?", tmpTx.ID,
		).Delete(&models.Redeemer{})
		if result.Error != nil {
			return fmt.Errorf(
				"delete existing redeemers: %w",
				result.Error,
			)
		}
		result = db.Where(
			"transaction_id = ?", tmpTx.ID,
		).Delete(&models.PlutusData{})
		if result.Error != nil {
			return fmt.Errorf(
				"delete existing plutus data: %w",
				result.Error,
			)
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
			if err := processScripts(
				db, tmpTx.ID,
				uint8(lcommon.ScriptRefTypeNativeScript),
				ws.NativeScripts(), point,
			); err != nil {
				return fmt.Errorf(
					"process NativeScript scripts for tx %d at slot %d: %w",
					tmpTx.ID, point.Slot, err,
				)
			}
			if err := processScripts(
				db, tmpTx.ID,
				uint8(lcommon.ScriptRefTypePlutusV1),
				ws.PlutusV1Scripts(), point,
			); err != nil {
				return fmt.Errorf(
					"process PlutusV1 scripts for tx %d at slot %d: %w",
					tmpTx.ID, point.Slot, err,
				)
			}
			if err := processScripts(
				db, tmpTx.ID,
				uint8(lcommon.ScriptRefTypePlutusV2),
				ws.PlutusV2Scripts(), point,
			); err != nil {
				return fmt.Errorf(
					"process PlutusV2 scripts for tx %d at slot %d: %w",
					tmpTx.ID, point.Slot, err,
				)
			}
			if err := processScripts(
				db, tmpTx.ID,
				uint8(lcommon.ScriptRefTypePlutusV3),
				ws.PlutusV3Scripts(), point,
			); err != nil {
				return fmt.Errorf(
					"process PlutusV3 scripts for tx %d at slot %d: %w",
					tmpTx.ID, point.Slot, err,
				)
			}

			// Add PlutusData (Datums) — only for valid transactions,
			// matching storeTransactionDatums which hash-indexes them.
			if tx.IsValid() {
				for _, datum := range ws.PlutusData() {
					plutusData := models.PlutusData{
						TransactionID: tmpTx.ID,
						Data:          datum.Cbor(),
					}
					if result := db.Create(&plutusData); result.Error != nil {
						return fmt.Errorf(
							"create plutus data: %w",
							result.Error,
						)
					}
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
	} // end storageMode == types.StorageModeAPI

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
			unifiedIDs := []uint{}
			if result := db.Model(&models.Certificate{}).Where("transaction_id = ?", tmpTx.ID).Pluck("id", &unifiedIDs); result.Error != nil {
				return fmt.Errorf(
					"query existing unified certificates: %w",
					result.Error,
				)
			}
			if len(unifiedIDs) > 0 {
				// Delete specialized records linked to existing unified certificates.
				// Child tables must be deleted before parent tables due to FK constraints.
				// Note: move_instantaneous_rewards_reward is deleted via CASCADE when its
				// parent move_instantaneous_rewards is deleted (MIRID FK constraint).
				tables := []string{
					"pool_registration_owner",
					"pool_registration_relay",
					"stake_registration",
					"pool_registration",
					"pool_retirement",
					"auth_committee_hot",
					"resign_committee_cold",
					"deregistration",
					"stake_delegation",
					"stake_registration_delegation",
					"stake_vote_delegation",
					"stake_vote_registration_delegation",
					"registration",
					"registration_drep",
					"deregistration_drep",
					"update_drep",
					"vote_delegation",
					"vote_registration_delegation",
					"move_instantaneous_rewards",
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
					certIdx := uint(i) // #nosec G115
					result := db.Where(
						"transaction_id = ? AND cert_index = ?",
						tmpTx.ID,
						certIdx,
					).First(&unifiedCert)
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

					// Set the PoolID for the registration record
					if tmpPool.ID == 0 {
						result := db.Create(tmpPool)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					} else {
						result := db.Save(tmpPool)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}
					tmpReg.PoolID = tmpPool.ID

					// Save the registration record.
					// Use OnConflict to handle two registrations for the same pool
					// in the same slot (same block). The second certificate updates
					// the registration fields instead of failing on the unique index.
					result := db.Clauses(clause.OnConflict{
						Columns: []clause.Column{
							{Name: "pool_id"},
							{Name: "added_slot"},
						},
						DoUpdates: clause.AssignmentColumns([]string{
							"vrf_key_hash", "pledge", "cost", "margin",
							"reward_account", "certificate_id",
							"metadata_url", "metadata_hash",
						}),
					}, clause.Returning{Columns: []clause.Column{{Name: "id"}}}).Omit("Owners", "Relays").Create(&tmpReg)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}

					// On conflict, GORM may not populate tmpReg.ID.
					// Re-fetch if necessary so Owners/Relays get the correct FK.
					if tmpReg.ID == 0 {
						var existing models.PoolRegistration
						if err := db.Where(
							"pool_id = ? AND added_slot = ?",
							tmpReg.PoolID, tmpReg.AddedSlot,
						).First(&existing).Error; err != nil {
							return fmt.Errorf(
								"fetching pool registration ID after upsert: %w",
								err,
							)
						}
						tmpReg.ID = existing.ID
					}

					// Delete old Owners/Relays for this registration (idempotent on retry
					// or when a second cert in the same slot updates the registration)
					if res := db.Where(
						"pool_registration_id = ?", tmpReg.ID,
					).Delete(&models.PoolRegistrationOwner{}); res.Error != nil {
						return fmt.Errorf("delete pool registration owners: %w", res.Error)
					}
					if res := db.Where(
						"pool_registration_id = ?", tmpReg.ID,
					).Delete(&models.PoolRegistrationRelay{}); res.Error != nil {
						return fmt.Errorf("delete pool registration relays: %w", res.Error)
					}

					// Insert Owners and Relays with correct FKs
					if len(tmpReg.Owners) > 0 {
						for j := range tmpReg.Owners {
							tmpReg.Owners[j].PoolRegistrationID = tmpReg.ID
							tmpReg.Owners[j].PoolID = tmpPool.ID
						}
						if res := db.Create(&tmpReg.Owners); res.Error != nil {
							return fmt.Errorf("create pool registration owners: %w", res.Error)
						}
					}

					if len(tmpReg.Relays) > 0 {
						for j := range tmpReg.Relays {
							tmpReg.Relays[j].PoolRegistrationID = tmpReg.ID
							tmpReg.Relays[j].PoolID = tmpPool.ID
						}
						if res := db.Create(&tmpReg.Relays); res.Error != nil {
							return fmt.Errorf("create pool registration relays: %w", res.Error)
						}
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
						tmpAccount.CertificateID = certIDMap[i]
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
						tmpAccount.CertificateID = certIDMap[i]
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
						tmpReg.AnchorURL = c.Anchor.Url
						tmpReg.AnchorHash = c.Anchor.DataHash[:]
					}

					// Persist DRep anchor and active state
					if err := d.SetDrep(drepCredential, point.Slot, tmpReg.AnchorURL, tmpReg.AnchorHash, true, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Use OnConflict to handle two registrations for the same DRep
					// in the same slot (same block). The second certificate updates
					// the registration fields instead of failing on the unique index.
					result := db.Clauses(clause.OnConflict{
						Columns: []clause.Column{
							{Name: "drep_credential"},
							{Name: "added_slot"},
						},
						DoUpdates: clause.AssignmentColumns([]string{
							"anchor_url", "anchor_hash", "certificate_id",
						}),
					}).Create(&tmpReg)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}

					// On conflict, GORM may not populate tmpReg.ID.
					// Re-fetch if necessary so certIDUpdates gets the correct ID.
					if tmpReg.ID == 0 {
						var existing models.RegistrationDrep
						if err := db.Where(
							"drep_credential = ? AND added_slot = ?",
							tmpReg.DrepCredential, tmpReg.AddedSlot,
						).First(&existing).Error; err != nil {
							return fmt.Errorf(
								"fetching drep registration ID after upsert: %w",
								err,
							)
						}
						tmpReg.ID = existing.ID
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
						tmpUpdate.AnchorURL = c.Anchor.Url
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
					if err := d.SetDrep(drepCredential, point.Slot, tmpUpdate.AnchorURL, tmpUpdate.AnchorHash, true, txn); err != nil {
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
						tmpResign.AnchorURL = c.Anchor.Url
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
					whenClauses = append(
						whenClauses,
						"WHEN id = ? THEN CAST(? AS bigint)",
					)
					values = append(values, unifiedID, specializedID)
				}

				caseStmt := strings.Join(whenClauses, " ")
				query := fmt.Sprintf(
					"UPDATE certs SET certificate_id = CASE %s END WHERE id IN ?",
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

		if d.storageMode == types.StorageModeAPI {
			if err := d.storeTransactionDatums(tx, point.Slot, txn); err != nil {
				return fmt.Errorf("store datums failed: %w", err)
			}
		}
	}

	return nil
}

// SetGenesisTransaction stores a genesis transaction record.
// Genesis transactions have no inputs, witnesses, or fees - just outputs.
func (d *MetadataStorePostgres) SetGenesisTransaction(
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
		return fmt.Errorf("create genesis transaction %x: %w", hash, result.Error)
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
	for i := range outputs {
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
func (d *MetadataStorePostgres) storeTransactionDatums(
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
func (d *MetadataStorePostgres) storeDatum(
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
	return d.SetDatum(datumHash, rawDatum, slot, txn)
}

// GetTransactionHashesAfterSlot returns transaction hashes for transactions added after the given slot.
// This is used for blob cleanup during rollback/truncation.
func (d *MetadataStorePostgres) GetTransactionHashesAfterSlot(
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
// This also clears UTXO references (spent_at_tx_id, collateral_by_tx_id, referenced_by_tx_id)
// to transactions being deleted, effectively restoring UTXOs to their unspent state.
// UTXO hash-based foreign keys are NULLed out before deleting transactions to prevent orphaned references.
func (d *MetadataStorePostgres) DeleteTransactionsAfterSlot(
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

// SetGenesisStaking is not implemented for the PostgreSQL metadata plugin.
func (d *MetadataStorePostgres) SetGenesisStaking(
	_ map[string]lcommon.PoolRegistrationCertificate,
	_ map[string]string,
	_ []byte,
	_ types.Txn,
) error {
	return errors.New("genesis staking not implemented for postgres")
}

// DeleteAddressTransactionsAfterSlot removes address-transaction mapping records
// added after the given slot.
func (d *MetadataStorePostgres) DeleteAddressTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Where("slot > ?", slot).
		Delete(&models.AddressTransaction{}); result.Error != nil {
		return fmt.Errorf("delete address transactions after slot: %w", result.Error)
	}
	return nil
}
