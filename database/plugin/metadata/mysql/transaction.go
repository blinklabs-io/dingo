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

//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/accounthistory"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/accountsums"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/certutil"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// dbFromTxn returns d.DB() only when txn is nil, unwraps known *mysqlTxn or provider.MetadataTxn() when available, and returns nil for unrecognized txn types so callers can detect errors
func (d *MetadataStoreMysql) dbFromTxn(txn types.Txn) *gorm.DB {
	if txn == nil {
		return d.DB()
	}
	if stx, ok := txn.(*mysqlTxn); ok && stx != nil {
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
func (d *MetadataStoreMysql) resolveDB(txn types.Txn) (*gorm.DB, error) {
	if stx, ok := txn.(*mysqlTxn); ok {
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
// MySQL handles concurrent reads natively with a single connection
// pool, so this delegates to resolveDB. This method exists for API
// consistency with the SQLite backend, which uses a separate read
// pool for WAL mode.
func (d *MetadataStoreMysql) resolveReadDB(
	txn types.Txn,
) (*gorm.DB, error) {
	return d.resolveDB(txn)
}

// GetTransactionByHash returns a transaction by its hash
func (d *MetadataStoreMysql) GetTransactionByHash(
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

// GetTransactionSlotByHash returns the slot of the transaction with the
// given hash without preloading any related rows. Returns (0, false, nil)
// when no such transaction exists.
func (d *MetadataStoreMysql) GetTransactionSlotByHash(
	hash []byte,
	txn types.Txn,
) (uint64, bool, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, false, err
	}
	var row struct{ Slot uint64 }
	result := db.Model(&models.Transaction{}).
		Select("slot").
		Where("hash = ?", hash).
		Take(&row)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return 0, false, nil
		}
		return 0, false, result.Error
	}
	return row.Slot, true, nil
}

// GetTransactionIDByHash returns the primary-key ID of the transaction
// with the given hash without preloading any related rows. Returns
// (0, false, nil) when no such transaction exists.
func (d *MetadataStoreMysql) GetTransactionIDByHash(
	hash []byte,
	txn types.Txn,
) (uint, bool, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, false, err
	}
	var row struct{ ID uint }
	result := db.Model(&models.Transaction{}).
		Select("id").
		Where("hash = ?", hash).
		Take(&row)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return 0, false, nil
		}
		return 0, false, result.Error
	}
	return row.ID, true, nil
}

// GetTransactionMetadataByHash returns only the stored metadata blob for the
// transaction with the given hash without preloading any related rows. Returns
// (nil, nil) when no such transaction exists or it carries no metadata.
func (d *MetadataStoreMysql) GetTransactionMetadataByHash(
	hash []byte,
	txn types.Txn,
) ([]byte, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	var row struct{ Metadata []byte }
	result := db.Model(&models.Transaction{}).
		Select("metadata").
		Where("hash = ?", hash).
		Take(&row)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return row.Metadata, nil
}

// GetTransactionsByHashes returns transactions for the provided hashes.
func (d *MetadataStoreMysql) GetTransactionsByHashes(
	hashes [][]byte,
	txn types.Txn,
) ([]models.Transaction, error) {
	var ret []models.Transaction
	if len(hashes) == 0 {
		return ret, nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.
		Where("hash IN ?", hashes).
		Preload(clause.Associations).
		Preload("Inputs.Assets").
		Preload("Outputs.Assets").
		Preload("Collateral.Assets").
		Preload("ReferenceInputs.Assets").
		Find(&ret)
	if result.Error != nil {
		return nil, fmt.Errorf("get txs by hashes: %w", result.Error)
	}
	return ret, nil
}

// GetTransactionsByBlockHash returns all transactions in a block, ordered by index
func (d *MetadataStoreMysql) GetTransactionsByBlockHash(
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
// deduplication by (payment_key, credential_tag, staking_key) within the tx.
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
		key := fmt.Sprintf(
			"%x|%d|%x",
			utxo.PaymentKey,
			utxo.CredentialTag,
			utxo.StakingKey,
		)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, models.AddressTransaction{
			PaymentKey:    append([]byte(nil), utxo.PaymentKey...),
			CredentialTag: utxo.CredentialTag,
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
func (d *MetadataStoreMysql) GetTransactionsByAddress(
	paymentKey []byte,
	credentialTag uint8,
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
			"payment_key = ? AND credential_tag = ? AND staking_key = ?",
			paymentKey,
			credentialTag,
			stakingKey,
		)
	case len(paymentKey) > 0:
		addrQuery = addrQuery.Where(
			"payment_key = ? AND (staking_key IS NULL OR OCTET_LENGTH(staking_key) = 0)",
			paymentKey,
		)
	default:
		addrQuery = addrQuery.Where(
			"credential_tag = ? AND staking_key = ?",
			credentialTag,
			stakingKey,
		)
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

// CountTransactionsByAddress returns the total number of
// distinct transactions involving the given
// payment/staking key.
func (d *MetadataStoreMysql) CountTransactionsByAddress(
	paymentKey []byte,
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, err
	}

	if len(paymentKey) == 0 && len(stakingKey) == 0 {
		return 0, nil
	}

	addrQuery := db.Model(&models.AddressTransaction{})
	switch {
	case len(paymentKey) > 0 && len(stakingKey) > 0:
		addrQuery = addrQuery.Where(
			"payment_key = ? AND credential_tag = ? AND staking_key = ?",
			paymentKey,
			credentialTag,
			stakingKey,
		)
	case len(paymentKey) > 0:
		addrQuery = addrQuery.Where(
			"payment_key = ? AND (staking_key IS NULL OR OCTET_LENGTH(staking_key) = 0)",
			paymentKey,
		)
	default:
		addrQuery = addrQuery.Where(
			"credential_tag = ? AND staking_key = ?",
			credentialTag,
			stakingKey,
		)
	}

	var count int64
	result := addrQuery.Distinct("transaction_id").Count(&count)
	if result.Error != nil {
		return 0, fmt.Errorf(
			"count txs by address: %w",
			result.Error,
		)
	}
	return int(count), nil
}

// GetAddressesByCredential returns distinct addresses mapped to a stake credential.
func (d *MetadataStoreMysql) GetAddressesByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
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
		Select("MIN(id) AS id, payment_key, credential_tag, staking_key").
		Where(
			"credential_tag = ? AND staking_key = ? AND length(payment_key) > 0",
			credentialTag,
			stakingKey,
		).
		Group("payment_key, credential_tag, staking_key").
		Order(addressOrderClause(order))
	if limit > 0 {
		query = query.Limit(limit)
	}
	if offset > 0 {
		query = query.Offset(offset)
	}
	if result := query.Find(&ret); result.Error != nil {
		return nil, fmt.Errorf("get addresses by stake credential: %w", result.Error)
	}
	return ret, nil
}

// CountAddressesByCredential returns the total number of distinct addresses mapped to a stake credential.
func (d *MetadataStoreMysql) CountAddressesByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	if len(stakingKey) == 0 {
		return 0, nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, fmt.Errorf(
			"resolve DB for count addresses by stake credential: %w",
			err,
		)
	}
	var count int64
	if err := db.Model(&models.AddressTransaction{}).
		Where(
			"credential_tag = ? AND staking_key = ? AND length(payment_key) > 0",
			credentialTag,
			stakingKey,
		).
		Distinct("payment_key").
		Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count addresses by stake credential: %w", err)
	}
	return int(count), nil
}

func addressOrderClause(order string) string {
	if strings.EqualFold(order, "desc") {
		return "payment_key DESC"
	}
	return "payment_key ASC"
}

func (d *MetadataStoreMysql) GetAccountDelegationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountDelegationHistoryRow, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve DB for account delegation history: %w",
			err,
		)
	}
	rows, err := accounthistory.QueryDelegationHistoryByCredential(
		db,
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query account delegation history: %w",
			err,
		)
	}
	return rows, nil
}

func (d *MetadataStoreMysql) CountAccountDelegationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, fmt.Errorf(
			"resolve DB for count account delegation history: %w",
			err,
		)
	}
	count, err := accounthistory.CountDelegationHistoryByCredential(
		db,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count account delegation history: %w",
			err,
		)
	}
	return count, nil
}

func (d *MetadataStoreMysql) GetAccountRegistrationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountRegistrationHistoryRow, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve DB for account registration history: %w",
			err,
		)
	}
	rows, err := accounthistory.QueryRegistrationHistoryByCredential(
		db,
		credentialTag,
		stakingKey,
		limit,
		offset,
		order,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query account registration history: %w",
			err,
		)
	}
	return rows, nil
}

func (d *MetadataStoreMysql) CountAccountRegistrationHistoryByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (int, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, fmt.Errorf(
			"resolve DB for count account registration history: %w",
			err,
		)
	}
	count, err := accounthistory.CountRegistrationHistoryByCredential(
		db,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count account registration history: %w",
			err,
		)
	}
	return count, nil
}

func (d *MetadataStoreMysql) GetAccountSumsByCredential(
	credentialTag uint8,
	stakingKey []byte,
	txn types.Txn,
) (models.AccountSums, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return models.AccountSums{}, fmt.Errorf(
			"resolve DB for account sums: %w",
			err,
		)
	}
	sums, err := accountsums.QueryAccountSums(db, credentialTag, stakingKey)
	if err != nil {
		return models.AccountSums{}, fmt.Errorf(
			"query account sums: %w",
			err,
		)
	}
	return sums, nil
}

// GetTransactionsByMetadataLabel returns transactions containing a metadata
// entry for the requested label.
func (d *MetadataStoreMysql) GetTransactionsByMetadataLabel(
	label uint64,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.Transaction, error) {
	var ret []models.Transaction
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}

	orderClause := "slot ASC, block_index ASC, id ASC"
	if descending {
		orderClause = "slot DESC, block_index DESC, id DESC"
	}

	subQuery := db.Model(&models.TransactionMetadataLabel{}).
		Select("transaction_id").
		Where("label = ?", types.Uint64(label))

	query := db.
		Where("id IN (?)", subQuery).
		Order(orderClause).
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

	if result := query.Find(&ret); result.Error != nil {
		return nil, fmt.Errorf(
			"get txs by metadata label %d: %w",
			label,
			result.Error,
		)
	}

	return ret, nil
}

// CountTransactionsByMetadataLabel returns the total number of transactions
// that include metadata for the requested label.
func (d *MetadataStoreMysql) CountTransactionsByMetadataLabel(
	label uint64,
	txn types.Txn,
) (int, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, err
	}

	var count int64
	if result := db.Model(&models.TransactionMetadataLabel{}).
		Where("label = ?", types.Uint64(label)).
		Count(&count); result.Error != nil {
		return 0, fmt.Errorf(
			"count txs by metadata label %d: %w",
			label,
			result.Error,
		)
	}
	return int(count), nil
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
func (d *MetadataStoreMysql) getOrCreateAccount(
	credentialTag uint8,
	stakeKey []byte,
	txn types.Txn,
) (*models.Account, error) {
	// Include inactive accounts to allow reactivation on registration.
	tmpAccount, err := d.GetAccountByCredential(credentialTag, stakeKey, true, txn)
	if err != nil {
		if !errors.Is(err, models.ErrAccountNotFound) {
			return nil, err
		}
	}
	if tmpAccount == nil {
		tmpAccount = &models.Account{
			StakingKey:    stakeKey,
			CredentialTag: credentialTag,
			CreatedSlot:   models.AccountCreatedSlotUnset,
		}
	} else if !tmpAccount.Active {
		tmpAccount.Active = true
	}
	return tmpAccount, nil
}

// saveAccount persists the account to the database. It creates a new
// record when `account.ID == 0` (with an upsert on credential tag + staking key) or saves
// the existing record otherwise.
func saveAccount(account *models.Account, db *gorm.DB) error {
	if account.CreatedSlot == models.AccountCreatedSlotUnset {
		account.CreatedSlot = account.AddedSlot
	}
	if account.ID == 0 {
		updates := clause.AssignmentColumns(
			[]string{
				"added_slot",
				"pool",
				"drep",
				"drep_type",
				"active",
				"certificate_id",
			},
		)
		updates = append(updates, clause.Assignment{
			Column: clause.Column{Name: "created_slot"},
			Value: gorm.Expr(
				"LEAST(created_slot, VALUES(created_slot))",
			),
		})
		result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "credential_tag"},
				{Name: "staking_key"},
			},
			DoUpdates: updates,
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

// insertMissingDeregistrationAccount creates the placeholder used when a
// deregistration is observed before its account. A concurrent or repeated
// deregistration must not replace any fields on an account that already exists.
func insertMissingDeregistrationAccount(
	account *models.Account,
	db *gorm.DB,
) error {
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "credential_tag"},
			{Name: "staking_key"},
		},
		DoNothing: true,
	}).Create(account).Error
}

// saveCertRecord saves a certificate record and returns any error
func saveCertRecord(record any, db *gorm.DB) error {
	result := db.Create(record)
	return result.Error
}

// SetGapBlockTransaction stores a transaction record and its produced
// outputs without looking up or consuming input UTxOs. Gap blocks
// from mithril sync have their UTxO state already reflected in the
// snapshot, so input processing must be skipped entirely.
func (d *MetadataStoreMysql) SetGapBlockTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	txn types.Txn,
) error {
	txHash := tx.Hash().Bytes()
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
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
	collateralReturn := tx.CollateralReturn()
	for _, utxo := range tx.Produced() {
		if collateralReturn != nil && utxo.Output == collateralReturn {
			m := models.UtxoLedgerToModel(utxo, point.Slot)
			tmpTx.CollateralReturn = &m
			continue
		}
		m := models.UtxoLedgerToModel(utxo, point.Slot)
		tmpTx.Outputs = append(tmpTx.Outputs, m)
	}
	outputsToCreate := tmpTx.Outputs
	collateralReturnToCreate := tmpTx.CollateralReturn
	tmpTx.Outputs = nil
	tmpTx.CollateralReturn = nil
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "hash"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"block_hash", "block_index", "slot"},
		),
	}).Create(tmpTx)
	tmpTx.Outputs = outputsToCreate
	tmpTx.CollateralReturn = collateralReturnToCreate
	if result.Error != nil {
		return fmt.Errorf(
			"create gap block transaction at slot %d: %w",
			point.Slot,
			result.Error,
		)
	}
	if tmpTx.ID == 0 {
		existingTx, err := d.GetTransactionByHash(txHash, txn)
		if err != nil {
			return fmt.Errorf(
				"fetch transaction ID after upsert: %w", err,
			)
		}
		if existingTx == nil {
			return fmt.Errorf(
				"transaction not found after upsert: %x",
				txHash,
			)
		}
		tmpTx.ID = existingTx.ID
	}
	for i := range tmpTx.Outputs {
		tmpTx.Outputs[i].ID = 0
		tmpTx.Outputs[i].TransactionID = &tmpTx.ID
	}
	if len(tmpTx.Outputs) > 0 {
		if err := d.ImportUtxos(tmpTx.Outputs, txn); err != nil {
			return fmt.Errorf(
				"create gap block utxo outputs for tx %x: %w",
				txHash, err,
			)
		}
	}
	if tmpTx.CollateralReturn != nil {
		tmpTx.CollateralReturn.ID = 0
		tmpTx.CollateralReturn.CollateralReturnForTxID = &tmpTx.ID
		if err := d.ImportUtxos(
			[]models.Utxo{*tmpTx.CollateralReturn},
			txn,
		); err != nil {
			return fmt.Errorf(
				"create gap block collateral return for tx %x: %w",
				txHash, err,
			)
		}
	}
	if err := d.recordAssetMintBurn(tx, txHash, point.Slot, idx, txn); err != nil {
		return err
	}
	return nil
}

// SetTransaction adds a new transaction to the database and processes all certificates
func (d *MetadataStoreMysql) SetTransaction(
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
	var metadataLabels []labelcodec.Entry
	if tx.Metadata() != nil && d.storageMode == types.StorageModeAPI {
		tmpMetadata, tmpLabels, err := labelcodec.EncodeAndExtract(
			tx.Metadata(),
		)
		if err != nil {
			return fmt.Errorf(
				"failed to extract metadata labels: %w",
				err,
			)
		}
		tmpTx.Metadata = tmpMetadata
		metadataLabels = tmpLabels
	}
	collateralReturn := tx.CollateralReturn()
	// tx.Produced() already returns correct indices for both
	// valid transactions (regular outputs at 0, 1, ...) and
	// invalid transactions (collateral return at len(Outputs())).
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
	if tx.IsValid() {
		if err := d.applyTransactionRewardWithdrawals(
			tx.Withdrawals(),
			point.Slot,
			txHash,
			txn,
		); err != nil {
			return fmt.Errorf("apply reward withdrawals for tx %x: %w", txHash, err)
		}
	}
	if len(metadataLabels) > 0 {
		labelRecords := make(
			[]models.TransactionMetadataLabel,
			0,
			len(metadataLabels),
		)
		for _, tmpLabel := range metadataLabels {
			labelRecords = append(labelRecords, models.TransactionMetadataLabel{
				TransactionID: tmpTx.ID,
				Label:         types.Uint64(tmpLabel.Label),
				Slot:          point.Slot,
				CborValue:     tmpLabel.CborValue,
				JsonValue:     tmpLabel.JsonValue,
			})
		}
		if result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "transaction_id"},
				{Name: "label"},
			},
			DoUpdates: clause.AssignmentColumns(
				[]string{"slot", "cbor_value", "json_value"},
			),
		}).Create(&labelRecords); result.Error != nil {
			return fmt.Errorf(
				"create metadata labels for tx %x: %w",
				txHash,
				result.Error,
			)
		}
	}

	if err := d.recordAssetMintBurn(tx, txHash, point.Slot, idx, txn); err != nil {
		return err
	}
	// Add Inputs to Transaction
	if len(tx.Inputs()) > 0 {
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
			inTxId := input.Id().Bytes()
			inIdx := input.Index()
			key := fmt.Sprintf("%x:%d", inTxId, inIdx)
			utxo := inputUtxos[key]
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
			tmpTx.Inputs = append(tmpTx.Inputs, *utxo)
		}
	}
	// Add Collateral to Transaction
	if len(tx.Collateral()) > 0 {
		var caseClauses []string
		var whereConditions []string
		var caseArgs []any
		var whereArgs []any

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
	// Add ReferenceInputs to Transaction
	if len(tx.ReferenceInputs()) > 0 {
		var caseClauses []string
		var whereConditions []string
		var caseArgs []any
		var whereArgs []any

		refInputRefs := make([]UtxoRef, 0, len(tx.ReferenceInputs()))
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

	// Consume UTxOs
	if len(tx.Consumed()) > 0 {
		type consumedUtxoRef struct {
			txID []byte
			idx  uint32
			hash string
		}
		consumedRefs := make([]consumedUtxoRef, 0, len(tx.Consumed()))
		for _, input := range tx.Consumed() {
			inTxID := input.Id().Bytes()
			inIdx := input.Index()
			duplicate := false
			for i := range consumedRefs {
				if consumedRefs[i].idx == inIdx &&
					bytes.Equal(consumedRefs[i].txID, inTxID) {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			consumedRefs = append(consumedRefs, consumedUtxoRef{
				txID: inTxID,
				idx:  inIdx,
				hash: input.Id().String(),
			})
		}
		if len(consumedRefs) > 0 {
			whereConditions := make([]string, 0, len(consumedRefs))
			updateArgs := make([]any, 0, 2+(len(consumedRefs)*2))
			updateArgs = append(updateArgs, point.Slot, txHash)
			for _, ref := range consumedRefs {
				whereConditions = append(
					whereConditions,
					"(tx_id = ? AND output_idx = ?)",
				)
				updateArgs = append(updateArgs, ref.txID, ref.idx)
			}
			sql := fmt.Sprintf(
				"UPDATE utxo SET deleted_slot = ?, spent_at_tx_id = ? "+
					"WHERE deleted_slot = 0 AND spent_at_tx_id IS NULL AND (%s)",
				strings.Join(whereConditions, " OR "),
			)
			result = db.Exec(sql, updateArgs...)
			if result.Error != nil {
				return fmt.Errorf("batch consume utxos: %w", result.Error)
			}
			if result.RowsAffected != int64(len(consumedRefs)) {
				for _, ref := range consumedRefs {
					var existingUtxo models.Utxo
					checkResult := db.Where(
						"tx_id = ? AND output_idx = ?",
						ref.txID,
						ref.idx,
					).First(&existingUtxo)
					if checkResult.Error != nil {
						if errors.Is(checkResult.Error, gorm.ErrRecordNotFound) {
							d.logger.Warn(
								"input UTxO not found",
								"hash",
								ref.hash,
								"index",
								ref.idx,
							)
							continue
						}
						return fmt.Errorf(
							"failed to check UTXO %x#%d: %w",
							ref.txID,
							ref.idx,
							checkResult.Error,
						)
					}
					if existingUtxo.SpentAtTxId != nil &&
						bytes.Equal(existingUtxo.SpentAtTxId, txHash) {
						continue
					}
					if existingUtxo.DeletedSlot == 0 &&
						existingUtxo.SpentAtTxId == nil {
						return fmt.Errorf(
							"batch consume did not update UTXO %x#%d",
							ref.txID,
							ref.idx,
						)
					}
					return fmt.Errorf(
						"%w: %x:%d",
						types.ErrUtxoConflict,
						ref.txID,
						ref.idx,
					)
				}
			}
		}
	}
	// Address indexing, witnesses, scripts, redeemers, and plutus data only stored in API mode
	if d.storageMode == types.StorageModeAPI {
		// Index unique addresses participating in this transaction.
		// Includes inputs, collateral inputs, reference inputs, outputs, and collateral return.
		addressUtxos := make(
			[]models.Utxo,
			0,
			len(tmpTx.Inputs)+
				len(tmpTx.Collateral)+
				len(tmpTx.ReferenceInputs)+
				len(tmpTx.Outputs)+1,
		)
		addressUtxos = append(addressUtxos, tmpTx.Inputs...)
		addressUtxos = append(addressUtxos, tmpTx.Collateral...)
		addressUtxos = append(addressUtxos, tmpTx.ReferenceInputs...)
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
					"genesis_delegation",
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
				case *lcommon.GenesisKeyDelegationCertificate:
					certType = uint(lcommon.CertificateTypeGenesisKeyDelegation)
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
					rewardAcctTag, rewardAcctHash, err := certutil.PoolRewardAccount(c)
					if err != nil {
						return fmt.Errorf("pool reward account: %w", err)
					}
					tmpPool.RewardAccount = rewardAcctHash
					tmpPool.RewardAccountCredentialTag = rewardAcctTag

					// Create registration record
					tmpReg := models.PoolRegistration{
						PoolKeyHash:                c.Operator[:],
						VrfKeyHash:                 c.VrfKeyHash[:],
						Pledge:                     types.Uint64(c.Pledge),
						Cost:                       types.Uint64(c.Cost),
						Margin:                     &types.Rat{Rat: c.Margin.Rat},
						RewardAccount:              rewardAcctHash,
						RewardAccountCredentialTag: rewardAcctTag,
						AddedSlot:                  point.Slot,
						DepositAmount:              types.Uint64(deposit),
						CertificateID:              certIDMap[i],
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
							"reward_account", "reward_account_credential_tag", "certificate_id",
							"metadata_url", "metadata_hash",
							"deposit_amount",
						}),
					}).Omit("Owners", "Relays").Create(&tmpReg)
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpReg := models.StakeRegistration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					tmpAccount.AddedSlot = point.Slot
					if tmpAccount.ID == 0 {
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
				case *lcommon.GenesisKeyDelegationCertificate:
					tmpItem := models.GenesisDelegation{
						GenesisHash:         c.GenesisHash,
						GenesisDelegateHash: c.GenesisDelegateHash,
						VrfKeyHash:          c.VrfKeyHash[:],
						AddedSlot:           point.Slot,
						BlockIndex:          idx,
						CertIndex:           uint(i), //nolint:gosec
						CertificateID:       certIDMap[i],
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpItem.ID
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.GetAccountByCredential(credentialTag, stakeKey, true, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					if tmpAccount == nil {
						d.logger.Warn("deregistering non-existent account", "hash", stakeKey)
						tmpAccount = &models.Account{
							StakingKey:    stakeKey,
							CredentialTag: credentialTag,
							CreatedSlot:   models.AccountCreatedSlotUnset,
						}
						if err := insertMissingDeregistrationAccount(tmpAccount, db); err != nil {
							return fmt.Errorf("process certificate: %w", err)
						}
					}

					tmpAccount.Active = false
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.StakeDeregistration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.GetAccountByCredential(credentialTag, stakeKey, true, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					if tmpAccount == nil {
						d.logger.Warn("deregistering non-existent account", "hash", stakeKey)
						tmpAccount = &models.Account{
							StakingKey:    stakeKey,
							CredentialTag: credentialTag,
							CreatedSlot:   models.AccountCreatedSlotUnset,
						}
						if err := insertMissingDeregistrationAccount(tmpAccount, db); err != nil {
							return fmt.Errorf("process certificate: %w", err)
						}
					}

					tmpAccount.Active = false
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.Deregistration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.StakeDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.AddedSlot = point.Slot

					tmpReg := models.StakeRegistrationDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.StakeVoteDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          drepCredential,
						DrepType:      drepType,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpReg := models.Registration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					tmpAccount.AddedSlot = point.Slot
					if tmpAccount.ID == 0 {
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
					drepCredTag, err := models.CredentialTagFromUint(c.DrepCredential.CredType)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Registration (re)creates/activates the DRep regardless of prior state.

					tmpReg := models.RegistrationDrep{
						CredentialTag:  drepCredTag,
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
					if err := d.SetDrep(drepCredTag, drepCredential, point.Slot, tmpReg.AnchorURL, tmpReg.AnchorHash, true, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Use OnConflict to handle two registrations for the same DRep
					// in the same slot (same block). The second certificate updates
					// the registration fields instead of failing on the unique index.
					result := db.Clauses(clause.OnConflict{
						Columns: []clause.Column{
							{Name: "credential_tag"},
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
							"credential_tag = ? AND drep_credential = ? AND added_slot = ?",
							drepCredTag, tmpReg.DrepCredential, tmpReg.AddedSlot,
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
					drepCredTag, err := models.CredentialTagFromUint(c.DrepCredential.CredType)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpDereg := models.DeregistrationDrep{
						CredentialTag:  drepCredTag,
						DrepCredential: drepCredential,
						AddedSlot:      point.Slot,
						DepositAmount:  types.Uint64(deposit),
						CertificateID:  certIDMap[i],
					}

					// Mark DRep inactive
					// Ensure we don't create a new DRep during deregistration. Check existence first.
					existingDrep, err := d.GetDrepByCredential(drepCredTag, drepCredential, true, txn)
					if err != nil {
						if !errors.Is(err, models.ErrDrepNotFound) {
							return fmt.Errorf("process certificate: %w", err)
						}
					}
					if existingDrep == nil {
						return fmt.Errorf("process certificate: %w", models.ErrDrepNotFound)
					}
					if err := d.SetDrep(drepCredTag, drepCredential, point.Slot, "", nil, false, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpDereg, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpDereg.ID
				case *lcommon.UpdateDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]
					drepCredTag, err := models.CredentialTagFromUint(c.DrepCredential.CredType)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					tmpUpdate := models.UpdateDrep{
						CredentialTag: drepCredTag,
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
					existingDrep, err := d.GetDrepByCredential(drepCredTag, drepCredential, true, txn)
					if err != nil {
						if !errors.Is(err, models.ErrDrepNotFound) {
							return fmt.Errorf("process certificate: %w", err)
						}
					}
					if existingDrep == nil {
						return fmt.Errorf("process certificate: %w", models.ErrDrepNotFound)
					}
					if err := d.SetDrep(drepCredTag, drepCredential, point.Slot, tmpUpdate.AnchorURL, tmpUpdate.AnchorHash, true, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpUpdate, db); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					// Collect update for batch processing
					certIDUpdates[certIDMap[i]] = tmpUpdate.ID
				case *lcommon.StakeVoteRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}

					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot

					tmpReg := models.StakeVoteRegistrationDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          drepCredential,
						DrepType:      drepType,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}

					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot

					tmpReg := models.VoteRegistrationDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						Drep:          drepCredential,
						DrepType:      drepType,
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
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}

					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot

					tmpItem := models.VoteDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						Drep:          drepCredential,
						DrepType:      drepType,
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
						HotCredential:  hotCredential,
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
						OtherPot:      types.Uint64(c.Reward.OtherPot),
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
						credentialTag, err := models.CredentialTagFromUint(credential.CredType)
						if err != nil {
							return fmt.Errorf("process certificate: %w", err)
						}
						tmpReward := models.MoveInstantaneousRewardsReward{
							Credential:    credential.Credential[:],
							CredentialTag: credentialTag,
							Amount:        types.Uint64(amount),
							MIRID:         tmpMIR.ID,
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

func (d *MetadataStoreMysql) applyTransactionRewardWithdrawals(
	withdrawals map[*lcommon.Address]*big.Int,
	slot uint64,
	txHash []byte,
	txn types.Txn,
) error {
	for addr, amount := range withdrawals {
		if addr == nil || amount == nil || amount.Sign() == 0 {
			continue
		}
		if amount.Sign() < 0 || !amount.IsUint64() {
			return fmt.Errorf(
				"invalid reward withdrawal amount %s",
				amount.String(),
			)
		}
		zeroHash := lcommon.Blake2b224{}
		stakeKeyHash := addr.StakeKeyHash()
		if stakeKeyHash == zeroHash {
			return errors.New("reward withdrawal missing stake credential")
		}
		credentialTag, ok := models.StakeCredentialTagFromAddress(*addr)
		if !ok {
			return errors.New("derive reward withdrawal credential tag")
		}
		if err := d.ApplyAccountRewardWithdrawal(
			credentialTag,
			stakeKeyHash.Bytes(),
			amount.Uint64(),
			slot,
			txHash,
			txn,
		); err != nil {
			return err
		}
	}
	return nil
}

// SetTransactionBatched performs the same logical work as SetTransaction but
// accumulates all per-item metadata rows into acc for later bulk flushing via
// FlushBatch.  The transaction record itself is still written immediately so
// that downstream foreign-key dependencies (witness, script, etc.) can
// reference its auto-increment ID.
//
// Items written immediately (FK or ordering dependency):
//   - Transaction record (upsert)
//   - Collateral / reference-input UTXO marker UPDATEs
//   - Certificates and governance records
//   - storeTransactionDatums hash index
//
// Items deferred to acc:
//   - UTxO outputs, collateral return, UTxO spends
//   - Key witnesses, witness scripts, scripts, plutus data, redeemers
//   - Address-transaction index rows
func (d *MetadataStoreMysql) SetTransactionBatched(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
	acc types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	batch, ok := acc.(*BatchAccumulator)
	if !ok {
		return fmt.Errorf(
			"SetTransactionBatched: wrong accumulator type %T",
			acc,
		)
	}
	if batch == nil {
		return errors.New("SetTransactionBatched: acc must not be nil")
	}
	local := NewBatchAccumulator()
	txHash := tx.Hash().Bytes()
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// ------------------------------------------------------------------ //
	// 1. Write transaction record immediately (needed for FK IDs)         //
	// ------------------------------------------------------------------ //
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
	var metadataLabels []labelcodec.Entry
	if tx.Metadata() != nil && d.storageMode == types.StorageModeAPI {
		tmpMetadata, tmpLabels, err := labelcodec.EncodeAndExtract(
			tx.Metadata(),
		)
		if err != nil {
			return fmt.Errorf(
				"failed to extract metadata labels: %w",
				err,
			)
		}
		tmpTx.Metadata = tmpMetadata
		metadataLabels = tmpLabels
	}

	collateralReturn := tx.CollateralReturn()
	produced := tx.Produced()

	// Separate collateral return from regular outputs.
	// tx.Produced() already returns correct indices for both valid transactions
	// (regular outputs at 0, 1, ...) and invalid transactions (collateral return
	// at len(Outputs())), so no index rewriting is needed.
	var colRetUtxo *models.Utxo
	outputModels := make([]models.Utxo, 0, len(produced))
	for _, utxo := range produced {
		m := models.UtxoLedgerToModel(utxo, point.Slot)
		if collateralReturn != nil && utxo.Output == collateralReturn {
			colRetUtxo = &m
			continue
		}
		outputModels = append(outputModels, m)
	}

	// Clear Outputs on tmpTx so the upsert doesn't try to create them.
	tmpTx.Outputs = nil

	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "hash"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"block_hash", "block_index", "slot"},
		),
	}).Create(tmpTx)
	needsIdFetch := tmpTx.ID == 0

	if result.Error != nil {
		return fmt.Errorf(
			"create transaction (batched) at slot %d, block %x, txHash %x, txIndex %d: %w",
			point.Slot,
			point.Hash,
			txHash,
			idx,
			result.Error,
		)
	}
	if needsIdFetch {
		var existing struct{ ID uint }
		if err := db.Model(&models.Transaction{}).
			Select("id").
			Where("hash = ?", txHash).
			Take(&existing).Error; err != nil {
			return fmt.Errorf(
				"failed to fetch transaction ID after upsert (batched): %w",
				err,
			)
		}
		tmpTx.ID = existing.ID
	}

	if tx.IsValid() {
		if err := d.applyTransactionRewardWithdrawals(
			tx.Withdrawals(),
			point.Slot,
			txHash,
			txn,
		); err != nil {
			return fmt.Errorf(
				"apply reward withdrawals for tx %x: %w",
				txHash,
				err,
			)
		}
	}

	// metadata labels – small, write immediately just like SetTransaction.
	if len(metadataLabels) > 0 {
		labelRecords := make(
			[]models.TransactionMetadataLabel,
			0,
			len(metadataLabels),
		)
		for _, tmpLabel := range metadataLabels {
			labelRecords = append(labelRecords, models.TransactionMetadataLabel{
				TransactionID: tmpTx.ID,
				Label:         types.Uint64(tmpLabel.Label),
				Slot:          point.Slot,
				CborValue:     tmpLabel.CborValue,
				JsonValue:     tmpLabel.JsonValue,
			})
		}
		if result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "transaction_id"},
				{Name: "label"},
			},
			DoUpdates: clause.AssignmentColumns(
				[]string{"slot", "cbor_value", "json_value"},
			),
		}).Create(&labelRecords); result.Error != nil {
			return fmt.Errorf(
				"create metadata labels for tx %x (batched): %w",
				txHash,
				result.Error,
			)
		}
	}

	if err := d.recordAssetMintBurn(tx, txHash, point.Slot, idx, txn); err != nil {
		return err
	}

	// ------------------------------------------------------------------ //
	// 2. Accumulate UTxO outputs                                          //
	// ------------------------------------------------------------------ //
	for i := range outputModels {
		outputModels[i].ID = 0
		outputModels[i].TransactionID = &tmpTx.ID
		local.AddUtxoOutput(outputModels[i])
	}
	if colRetUtxo != nil {
		colRetUtxo.CollateralReturnForTxID = &tmpTx.ID
		local.AddCollateralReturn(*colRetUtxo)
	}

	// ------------------------------------------------------------------ //
	// 3. Collateral / reference-input marker UPDATEs (immediate)         //
	//    These update UTxOs that already exist from prior blocks.        //
	// ------------------------------------------------------------------ //
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
					"failed to fetch collateral UTxO (batched): %w",
					err,
				)
			}
			if utxo == nil {
				continue
			}
			caseClauses = append(
				caseClauses,
				"WHEN tx_id = ? AND output_idx = ? THEN ?",
			)
			caseArgs = append(caseArgs, inTxId, inIdx, txHash)
			whereConditions = append(
				whereConditions,
				"(tx_id = ? AND output_idx = ?)",
			)
			whereArgs = append(whereArgs, inTxId, inIdx)
			tmpTx.Collateral = append(tmpTx.Collateral, *utxo)
		}
		if len(caseClauses) > 0 {
			args := append(caseArgs, whereArgs...)
			sql := fmt.Sprintf(
				"UPDATE utxo SET collateral_by_tx_id = CASE %s ELSE collateral_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			if r := db.Exec(sql, args...); r.Error != nil {
				return fmt.Errorf(
					"batch update collateral (batched): %w",
					r.Error,
				)
			}
		}
	}

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
					"failed to fetch reference input UTxO (batched): %w",
					err,
				)
			}
			if utxo == nil {
				continue
			}
			caseClauses = append(
				caseClauses,
				"WHEN tx_id = ? AND output_idx = ? THEN ?",
			)
			caseArgs = append(caseArgs, inTxId, inIdx, txHash)
			whereConditions = append(
				whereConditions,
				"(tx_id = ? AND output_idx = ?)",
			)
			whereArgs = append(whereArgs, inTxId, inIdx)
			tmpTx.ReferenceInputs = append(
				tmpTx.ReferenceInputs,
				*utxo,
			)
		}
		if len(caseClauses) > 0 {
			args := append(caseArgs, whereArgs...)
			sql := fmt.Sprintf(
				"UPDATE utxo SET referenced_by_tx_id = CASE %s ELSE referenced_by_tx_id END WHERE %s",
				strings.Join(caseClauses, " "),
				strings.Join(whereConditions, " OR "),
			)
			if r := db.Exec(sql, args...); r.Error != nil {
				return fmt.Errorf(
					"batch update reference inputs (batched): %w",
					r.Error,
				)
			}
		}
	}

	// ------------------------------------------------------------------ //
	// 4. Accumulate UTxO spends (consumed inputs)                        //
	// ------------------------------------------------------------------ //
	if len(tx.Consumed()) > 0 {
		seen := make(map[string]bool, len(tx.Consumed()))
		for _, input := range tx.Consumed() {
			inTxID := input.Id().Bytes()
			inIdx := input.Index()
			key := fmt.Sprintf("%x:%d", inTxID, inIdx)
			if seen[key] {
				continue
			}
			seen[key] = true
			local.AddUtxoSpend(utxoSpend{
				TxId:          inTxID,
				OutputIdx:     inIdx,
				Slot:          point.Slot,
				SpentByTxHash: txHash,
			})
		}
	}

	// ------------------------------------------------------------------ //
	// 5. Accumulate API-mode metadata (witnesses, scripts, address txs)  //
	// ------------------------------------------------------------------ //
	if d.storageMode == types.StorageModeAPI {
		// On retry: schedule deletion of previously flushed rows for this tx.
		if needsIdFetch {
			local.AddDeleteTxID(tmpTx.ID)
		}

		// Fetch input UTxOs for address-indexing below.
		for _, input := range tx.Inputs() {
			inTxId := input.Id().Bytes()
			inIdx := input.Index()
			utxo, err := d.GetUtxo(inTxId, inIdx, txn)
			if err != nil {
				return fmt.Errorf(
					"failed to fetch input UTxO (batched): %w",
					err,
				)
			}
			if utxo == nil {
				continue
			}
			tmpTx.Inputs = append(tmpTx.Inputs, *utxo)
		}

		// Address-transaction index.
		addressUtxos := make(
			[]models.Utxo,
			0,
			len(tmpTx.Inputs)+len(tmpTx.Collateral)+len(outputModels)+1,
		)
		addressUtxos = append(addressUtxos, tmpTx.Inputs...)
		addressUtxos = append(addressUtxos, tmpTx.Collateral...)
		addressUtxos = append(addressUtxos, outputModels...)
		if colRetUtxo != nil {
			addressUtxos = append(addressUtxos, *colRetUtxo)
		}
		for _, atx := range collectAddressTransactions(
			tmpTx.ID,
			point.Slot,
			idx,
			addressUtxos,
		) {
			local.AddAddressTx(atx)
		}

		// Witnesses.
		ws := tx.Witnesses()
		if ws != nil {
			for _, vkey := range ws.Vkey() {
				local.AddKeyWitness(models.KeyWitness{
					TransactionID: tmpTx.ID,
					Type:          models.KeyWitnessTypeVkey,
					Vkey:          vkey.Vkey,
					Signature:     vkey.Signature,
				})
			}
			for _, bootstrap := range ws.Bootstrap() {
				local.AddKeyWitness(models.KeyWitness{
					TransactionID: tmpTx.ID,
					Type:          models.KeyWitnessTypeBootstrap,
					PublicKey:     bootstrap.PublicKey,
					Signature:     bootstrap.Signature,
					ChainCode:     bootstrap.ChainCode,
					Attributes:    bootstrap.Attributes,
				})
			}

			// Scripts – collect into accumulator instead of writing to DB.
			for _, s := range ws.NativeScripts() {
				local.AddWitnessScript(models.WitnessScripts{
					TransactionID: tmpTx.ID,
					Type:          uint8(lcommon.ScriptRefTypeNativeScript),
					ScriptHash:    s.Hash().Bytes(),
				})
				local.AddScript(models.Script{
					Hash:        s.Hash().Bytes(),
					Type:        uint8(lcommon.ScriptRefTypeNativeScript),
					Content:     s.RawScriptBytes(),
					CreatedSlot: point.Slot,
				})
			}
			for _, s := range ws.PlutusV1Scripts() {
				local.AddWitnessScript(models.WitnessScripts{
					TransactionID: tmpTx.ID,
					Type:          uint8(lcommon.ScriptRefTypePlutusV1),
					ScriptHash:    s.Hash().Bytes(),
				})
				local.AddScript(models.Script{
					Hash:        s.Hash().Bytes(),
					Type:        uint8(lcommon.ScriptRefTypePlutusV1),
					Content:     s.RawScriptBytes(),
					CreatedSlot: point.Slot,
				})
			}
			for _, s := range ws.PlutusV2Scripts() {
				local.AddWitnessScript(models.WitnessScripts{
					TransactionID: tmpTx.ID,
					Type:          uint8(lcommon.ScriptRefTypePlutusV2),
					ScriptHash:    s.Hash().Bytes(),
				})
				local.AddScript(models.Script{
					Hash:        s.Hash().Bytes(),
					Type:        uint8(lcommon.ScriptRefTypePlutusV2),
					Content:     s.RawScriptBytes(),
					CreatedSlot: point.Slot,
				})
			}
			for _, s := range ws.PlutusV3Scripts() {
				local.AddWitnessScript(models.WitnessScripts{
					TransactionID: tmpTx.ID,
					Type:          uint8(lcommon.ScriptRefTypePlutusV3),
					ScriptHash:    s.Hash().Bytes(),
				})
				local.AddScript(models.Script{
					Hash:        s.Hash().Bytes(),
					Type:        uint8(lcommon.ScriptRefTypePlutusV3),
					Content:     s.RawScriptBytes(),
					CreatedSlot: point.Slot,
				})
			}

			// PlutusData (datums).
			if tx.IsValid() {
				for _, datum := range ws.PlutusData() {
					local.AddPlutusData(models.PlutusData{
						TransactionID: tmpTx.ID,
						Data:          datum.Cbor(),
					})
				}
			}

			// Redeemers.
			if ws.Redeemers() != nil {
				for key, value := range ws.Redeemers().Iter() {
					//nolint:gosec
					local.AddRedeemer(models.Redeemer{
						TransactionID: tmpTx.ID,
						Tag:           uint8(key.Tag),
						Index:         key.Index,
						Data:          value.Data.Cbor(),
						ExUnitsMemory: uint64(max(0, value.ExUnits.Memory)),
						ExUnitsCPU:    uint64(max(0, value.ExUnits.Steps)),
					})
				}
			}
		}
	}

	// ------------------------------------------------------------------ //
	// 6. Certificates and governance – immediate, same as SetTransaction //
	// ------------------------------------------------------------------ //
	if tx.IsValid() {
		certs := tx.Certificates()
		if len(certs) > 0 {
			unifiedIDs := []uint{}
			if result := db.Model(&models.Certificate{}).
				Where("transaction_id = ?", tmpTx.ID).
				Pluck("id", &unifiedIDs); result.Error != nil {
				return fmt.Errorf(
					"query existing unified certificates (batched): %w",
					result.Error,
				)
			}
			if len(unifiedIDs) > 0 {
				poolRegistrationIDs := []uint{}
				if result := db.Model(&models.PoolRegistration{}).
					Where("certificate_id IN ?", unifiedIDs).
					Pluck("id", &poolRegistrationIDs); result.Error != nil {
					return fmt.Errorf(
						"query existing pool registrations (batched): %w",
						result.Error,
					)
				}
				if len(poolRegistrationIDs) > 0 {
					if result := db.Table("pool_registration_owner").
						Where("pool_registration_id IN ?", poolRegistrationIDs).
						Delete(nil); result.Error != nil {
						return fmt.Errorf(
							"delete existing pool_registration_owner records (batched): %w",
							result.Error,
						)
					}
					if result := db.Table("pool_registration_relay").
						Where("pool_registration_id IN ?", poolRegistrationIDs).
						Delete(nil); result.Error != nil {
						return fmt.Errorf(
							"delete existing pool_registration_relay records (batched): %w",
							result.Error,
						)
					}
				}
				tables := []string{
					"stake_registration", "pool_registration", "pool_retirement",
					"auth_committee_hot", "resign_committee_cold",
					"deregistration", "stake_delegation",
					"stake_registration_delegation", "stake_vote_delegation",
					"stake_vote_registration_delegation", "registration",
					"registration_drep", "deregistration_drep", "update_drep",
					"vote_delegation", "vote_registration_delegation",
					"move_instantaneous_rewards",
					"genesis_delegation",
				}
				for _, table := range tables {
					if result := db.Table(table).
						Where("certificate_id IN ?", unifiedIDs).
						Delete(nil); result.Error != nil {
						return fmt.Errorf(
							"delete existing %s records (batched): %w",
							table,
							result.Error,
						)
					}
				}
			}
			certIDMap := make(map[int]uint)
			certIDUpdates := make(map[uint]uint)
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
				case *lcommon.GenesisKeyDelegationCertificate:
					certType = uint(lcommon.CertificateTypeGenesisKeyDelegation)
				default:
					d.logger.Warn(
						"unknown certificate type (batched)",
						"type",
						fmt.Sprintf("%T", cert),
					)
					continue
				}
				unifiedCert := models.Certificate{
					TransactionID: tmpTx.ID,
					CertIndex:     uint(i), //nolint:gosec
					CertType:      certType,
					Slot:          point.Slot,
					BlockHash:     point.Hash,
				}
				if result := db.Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "transaction_id"},
						{Name: "cert_index"},
					},
					DoNothing: true,
				}).Create(&unifiedCert); result.Error != nil {
					return fmt.Errorf(
						"create unified certificate (batched): %w",
						result.Error,
					)
				}
				if unifiedCert.ID == 0 {
					if result := db.Where(
						"transaction_id = ? AND cert_index = ?",
						tmpTx.ID,
						uint(i), //nolint:gosec
					).First(&unifiedCert); result.Error != nil {
						return fmt.Errorf(
							"fetch existing unified certificate (batched): %w",
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
						d.logger.Warn(
							"missing deposit for deposit-bearing certificate (batched)",
							"index", i, "type", fmt.Sprintf("%T", cert),
						)
					}
				}
				if certDeposits == nil && certRequiresDeposit(cert) {
					return fmt.Errorf(
						"missing certDeposits for deposit-bearing certificate at index %d (batched)",
						i,
					)
				}
				switch c := cert.(type) {
				case *lcommon.PoolRegistrationCertificate:
					tmpPool, err := d.GetPool(
						lcommon.PoolKeyHash(c.Operator[:]),
						true,
						txn,
					)
					if err != nil && !errors.Is(err, models.ErrPoolNotFound) {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if tmpPool == nil {
						tmpPool = &models.Pool{
							PoolKeyHash: c.Operator[:],
							VrfKeyHash:  c.VrfKeyHash[:],
						}
					}
					tmpPool.Pledge = types.Uint64(c.Pledge)
					tmpPool.Cost = types.Uint64(c.Cost)
					tmpPool.Margin = &types.Rat{Rat: c.Margin.Rat}
					rewardAcctTag2, rewardAcctHash2, err := certutil.PoolRewardAccount(c)
					if err != nil {
						return fmt.Errorf("pool reward account: %w", err)
					}
					tmpPool.RewardAccount = rewardAcctHash2
					tmpPool.RewardAccountCredentialTag = rewardAcctTag2
					tmpReg := models.PoolRegistration{
						PoolKeyHash:                c.Operator[:],
						VrfKeyHash:                 c.VrfKeyHash[:],
						Pledge:                     types.Uint64(c.Pledge),
						Cost:                       types.Uint64(c.Cost),
						Margin:                     &types.Rat{Rat: c.Margin.Rat},
						RewardAccount:              rewardAcctHash2,
						RewardAccountCredentialTag: rewardAcctTag2,
						AddedSlot:                  point.Slot,
						DepositAmount:              types.Uint64(deposit),
						CertificateID:              certIDMap[i],
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
					for _, relay := range c.Relays {
						r := models.PoolRegistrationRelay{
							Ipv4: relay.Ipv4,
							Ipv6: relay.Ipv6,
						}
						if relay.Port != nil {
							r.Port = uint(*relay.Port)
						}
						if relay.Hostname != nil {
							r.Hostname = *relay.Hostname
						}
						tmpReg.Relays = append(tmpReg.Relays, r)
					}
					tmpPool.Relays = tmpReg.Relays
					if tmpPool.ID == 0 {
						if r := db.Omit(clause.Associations).Create(tmpPool); r.Error != nil {
							return fmt.Errorf(
								"process certificate (batched): %w",
								r.Error,
							)
						}
					} else {
						if r := db.Omit(clause.Associations).Save(tmpPool); r.Error != nil {
							return fmt.Errorf(
								"process certificate (batched): %w",
								r.Error,
							)
						}
					}
					tmpReg.PoolID = tmpPool.ID
					for j := range tmpReg.Owners {
						tmpReg.Owners[j].PoolID = tmpPool.ID
					}
					for j := range tmpReg.Relays {
						tmpReg.Relays[j].PoolID = tmpPool.ID
					}
					r2 := db.Clauses(clause.OnConflict{
						Columns: []clause.Column{
							{Name: "pool_id"},
							{Name: "added_slot"},
						},
						DoUpdates: clause.AssignmentColumns([]string{
							"vrf_key_hash", "pledge", "cost", "margin",
							"reward_account", "reward_account_credential_tag", "certificate_id",
							"metadata_url", "metadata_hash", "deposit_amount",
						}),
					}).Omit("Owners", "Relays").Create(&tmpReg)
					if r2.Error != nil {
						return fmt.Errorf(
							"process certificate (batched): %w",
							r2.Error,
						)
					}
					if tmpReg.ID == 0 {
						var existing models.PoolRegistration
						if err := db.Where(
							"pool_id = ? AND added_slot = ?",
							tmpReg.PoolID, tmpReg.AddedSlot,
						).First(&existing).Error; err != nil {
							return fmt.Errorf(
								"fetching pool registration ID after upsert (batched): %w",
								err,
							)
						}
						tmpReg.ID = existing.ID
					}
					if r := db.Where(
						"pool_registration_id = ?", tmpReg.ID,
					).Delete(&models.PoolRegistrationOwner{}); r.Error != nil {
						return fmt.Errorf(
							"delete pool registration owners (batched): %w",
							r.Error,
						)
					}
					if r := db.Where(
						"pool_registration_id = ?", tmpReg.ID,
					).Delete(&models.PoolRegistrationRelay{}); r.Error != nil {
						return fmt.Errorf(
							"delete pool registration relays (batched): %w",
							r.Error,
						)
					}
					if len(tmpReg.Owners) > 0 {
						for j := range tmpReg.Owners {
							tmpReg.Owners[j].PoolRegistrationID = tmpReg.ID
							tmpReg.Owners[j].PoolID = tmpPool.ID
						}
						if r := db.Create(&tmpReg.Owners); r.Error != nil {
							return fmt.Errorf(
								"create pool registration owners (batched): %w",
								r.Error,
							)
						}
					}
					if len(tmpReg.Relays) > 0 {
						for j := range tmpReg.Relays {
							tmpReg.Relays[j].PoolRegistrationID = tmpReg.ID
							tmpReg.Relays[j].PoolID = tmpPool.ID
						}
						if r := db.Create(&tmpReg.Relays); r.Error != nil {
							return fmt.Errorf(
								"create pool registration relays (batched): %w",
								r.Error,
							)
						}
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.StakeRegistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpReg := models.StakeRegistration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}
					tmpAccount.AddedSlot = point.Slot
					if tmpAccount.ID == 0 {
						tmpAccount.CertificateID = certIDMap[i]
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.GenesisKeyDelegationCertificate:
					tmpItem := models.GenesisDelegation{
						GenesisHash:         c.GenesisHash,
						GenesisDelegateHash: c.GenesisDelegateHash,
						VrfKeyHash:          c.VrfKeyHash[:],
						AddedSlot:           point.Slot,
						BlockIndex:          idx,
						CertIndex:           uint(i), //nolint:gosec
						CertificateID:       certIDMap[i],
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.PoolRetirementCertificate:
					tmpPool, err := d.GetPool(
						lcommon.PoolKeyHash(c.PoolKeyHash[:]),
						true,
						txn,
					)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if tmpPool == nil {
						tmpPool = &models.Pool{PoolKeyHash: c.PoolKeyHash[:]}
						r := db.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "pool_key_hash"}},
							UpdateAll: true,
						}).Create(&tmpPool)
						if r.Error != nil {
							return fmt.Errorf("process certificate (batched): %w", r.Error)
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
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.StakeDeregistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.GetAccountByCredential(credentialTag, stakeKey, true, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if tmpAccount == nil {
						tmpAccount = &models.Account{
							StakingKey:    stakeKey,
							CredentialTag: credentialTag,
							CreatedSlot:   models.AccountCreatedSlotUnset,
						}
						if err := insertMissingDeregistrationAccount(tmpAccount, db); err != nil {
							return fmt.Errorf("process certificate (batched): %w", err)
						}
					}
					tmpAccount.Active = false
					tmpAccount.AddedSlot = point.Slot
					tmpItem := models.StakeDeregistration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.DeregistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.GetAccountByCredential(credentialTag, stakeKey, true, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if tmpAccount == nil {
						tmpAccount = &models.Account{
							StakingKey:    stakeKey,
							CredentialTag: credentialTag,
							CreatedSlot:   models.AccountCreatedSlotUnset,
						}
						if err := insertMissingDeregistrationAccount(tmpAccount, db); err != nil {
							return fmt.Errorf("process certificate (batched): %w", err)
						}
					}
					tmpAccount.Active = false
					tmpAccount.AddedSlot = point.Slot
					tmpItem := models.Deregistration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
						Amount:        types.Uint64(deposit),
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.StakeDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.AddedSlot = point.Slot
					tmpItem := models.StakeDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						PoolKeyHash:   c.PoolKeyHash[:],
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.StakeRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.AddedSlot = point.Slot
					tmpReg := models.StakeRegistrationDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						PoolKeyHash:   c.PoolKeyHash[:],
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.StakeVoteDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}
					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot
					tmpItem := models.StakeVoteDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          drepCredential,
						DrepType:      drepType,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.RegistrationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpReg := models.Registration{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}
					tmpAccount.AddedSlot = point.Slot
					if tmpAccount.ID == 0 {
						tmpAccount.CertificateID = certIDMap[i]
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.RegistrationDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]
					drepCredTag2, err := models.CredentialTagFromUint(c.DrepCredential.CredType)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpReg := models.RegistrationDrep{
						CredentialTag:  drepCredTag2,
						DrepCredential: drepCredential,
						AddedSlot:      point.Slot,
						DepositAmount:  types.Uint64(deposit),
						CertificateID:  certIDMap[i],
					}
					if c.Anchor != nil {
						tmpReg.AnchorURL = c.Anchor.Url
						tmpReg.AnchorHash = c.Anchor.DataHash[:]
					}
					if err := d.SetDrep(
						drepCredTag2, drepCredential, point.Slot,
						tmpReg.AnchorURL, tmpReg.AnchorHash, true, txn,
					); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					r2 := db.Clauses(clause.OnConflict{
						Columns: []clause.Column{
							{Name: "credential_tag"},
							{Name: "drep_credential"},
							{Name: "added_slot"},
						},
						DoUpdates: clause.AssignmentColumns([]string{
							"anchor_url", "anchor_hash", "certificate_id",
						}),
					}).Create(&tmpReg)
					if r2.Error != nil {
						return fmt.Errorf("process certificate (batched): %w", r2.Error)
					}
					if tmpReg.ID == 0 {
						var existing models.RegistrationDrep
						if err := db.Where(
							"credential_tag = ? AND drep_credential = ? AND added_slot = ?",
							drepCredTag2, tmpReg.DrepCredential, tmpReg.AddedSlot,
						).First(&existing).Error; err != nil {
							return fmt.Errorf(
								"fetching drep registration ID after upsert (batched): %w",
								err,
							)
						}
						tmpReg.ID = existing.ID
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.DeregistrationDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]
					drepCredTag2, err := models.CredentialTagFromUint(c.DrepCredential.CredType)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpDereg := models.DeregistrationDrep{
						CredentialTag:  drepCredTag2,
						DrepCredential: drepCredential,
						AddedSlot:      point.Slot,
						DepositAmount:  types.Uint64(deposit),
						CertificateID:  certIDMap[i],
					}
					existingDrep, err := d.GetDrepByCredential(drepCredTag2, drepCredential, true, txn)
					if err != nil && !errors.Is(err, models.ErrDrepNotFound) {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if existingDrep == nil {
						return fmt.Errorf(
							"process certificate (batched): %w",
							models.ErrDrepNotFound,
						)
					}
					if err := d.SetDrep(
						drepCredTag2, drepCredential, point.Slot, "", nil, false, txn,
					); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpDereg, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpDereg.ID
				case *lcommon.UpdateDrepCertificate:
					drepCredential := c.DrepCredential.Credential[:]
					drepCredTag2, err := models.CredentialTagFromUint(c.DrepCredential.CredType)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					tmpUpdate := models.UpdateDrep{
						CredentialTag: drepCredTag2,
						Credential:    drepCredential,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if c.Anchor != nil {
						tmpUpdate.AnchorURL = c.Anchor.Url
						tmpUpdate.AnchorHash = c.Anchor.DataHash[:]
					}
					existingDrep, err := d.GetDrepByCredential(drepCredTag2, drepCredential, true, txn)
					if err != nil && !errors.Is(err, models.ErrDrepNotFound) {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if existingDrep == nil {
						return fmt.Errorf(
							"process certificate (batched): %w",
							models.ErrDrepNotFound,
						)
					}
					if err := d.SetDrep(
						drepCredTag2, drepCredential, point.Slot,
						tmpUpdate.AnchorURL, tmpUpdate.AnchorHash, true, txn,
					); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpUpdate, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpUpdate.ID
				case *lcommon.StakeVoteRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}
					tmpAccount.Pool = c.PoolKeyHash[:]
					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot
					tmpReg := models.StakeVoteRegistrationDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          drepCredential,
						DrepType:      drepType,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.VoteRegistrationDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}
					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot
					tmpReg := models.VoteRegistrationDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						Drep:          drepCredential,
						DrepType:      drepType,
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpReg, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpReg.ID
				case *lcommon.VoteDelegationCertificate:
					stakeKey := c.StakeCredential.Credential[:]
					credentialTag, err := models.CredentialTagFromUint(c.StakeCredential.CredType)
					if err != nil {
						return err
					}
					tmpAccount, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					drepType, err := models.DrepTypeFromInt(c.Drep.Type)
					if err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					var drepCredential []byte
					if drepType != models.DrepTypeAlwaysAbstain &&
						drepType != models.DrepTypeAlwaysNoConfidence {
						drepCredential = c.Drep.Credential[:]
					}
					tmpAccount.Drep = drepCredential
					tmpAccount.DrepType = drepType
					tmpAccount.AddedSlot = point.Slot
					tmpItem := models.VoteDelegation{
						StakingKey:    stakeKey,
						CredentialTag: credentialTag,
						Drep:          drepCredential,
						DrepType:      drepType,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if err := saveAccount(tmpAccount, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					if err := saveCertRecord(&tmpItem, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpItem.ID
				case *lcommon.AuthCommitteeHotCertificate:
					coldCredential := c.ColdCredential.Credential[:]
					hotCredential := c.HotCredential.Credential[:]
					tmpAuth := models.AuthCommitteeHot{
						ColdCredential: coldCredential,
						HotCredential:  hotCredential,
						CertificateID:  certIDMap[i],
						AddedSlot:      point.Slot,
					}
					if err := saveCertRecord(&tmpAuth, db); err != nil {
						return fmt.Errorf("process certificate (batched): %w", err)
					}
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
						return fmt.Errorf("process certificate (batched): %w", err)
					}
					certIDUpdates[certIDMap[i]] = tmpResign.ID
				case *lcommon.MoveInstantaneousRewardsCertificate:
					tmpMIR := models.MoveInstantaneousRewards{
						Pot:           c.Reward.Source,
						OtherPot:      types.Uint64(c.Reward.OtherPot),
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}
					if r := db.Create(&tmpMIR); r.Error != nil {
						return fmt.Errorf("process certificate (batched): %w", r.Error)
					}
					certIDUpdates[certIDMap[i]] = tmpMIR.ID
					for credential, amount := range c.Reward.Rewards {
						credentialTag, err := models.CredentialTagFromUint(credential.CredType)
						if err != nil {
							return fmt.Errorf("process certificate: %w", err)
						}
						tmpReward := models.MoveInstantaneousRewardsReward{
							Credential:    credential.Credential[:],
							CredentialTag: credentialTag,
							Amount:        types.Uint64(amount),
							MIRID:         tmpMIR.ID,
						}
						if r := db.Create(&tmpReward); r.Error != nil {
							return fmt.Errorf("process certificate (batched): %w", r.Error)
						}
					}
				default:
					return fmt.Errorf(
						"unsupported certificate type (batched) %T",
						cert,
					)
				}
			}

			if len(certIDUpdates) > 0 {
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
					"UPDATE certs SET certificate_id = CASE %s END WHERE id IN ?",
					caseStmt,
				)
				values = append(values, ids)
				if r := db.Exec(query, values...); r.Error != nil {
					return fmt.Errorf(
						"batch update unified certificates (batched): %w",
						r.Error,
					)
				}
			}
		}

		if d.storageMode == types.StorageModeAPI {
			if err := d.storeTransactionDatums(tx, point.Slot, txn); err != nil {
				return fmt.Errorf("store datums failed (batched): %w", err)
			}
		}
	}

	batch.MergeFrom(local)
	return nil
}

// SetGenesisTransaction stores a genesis transaction record.
// Genesis transactions have no inputs, witnesses, or fees - just outputs.
func (d *MetadataStoreMysql) SetGenesisTransaction(
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
func (d *MetadataStoreMysql) storeTransactionDatums(
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
func (d *MetadataStoreMysql) storeDatum(
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
func (d *MetadataStoreMysql) GetTransactionHashesAfterSlot(
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
func (d *MetadataStoreMysql) DeleteTransactionsAfterSlot(
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

	if result := db.Where("slot > ?", slot).
		Delete(&models.TransactionMetadataLabel{}); result.Error != nil {
		return fmt.Errorf(
			"delete transaction metadata labels after slot %d: %w",
			slot,
			result.Error,
		)
	}

	if result := db.Where("slot > ?", slot).
		Delete(&models.AssetMintBurn{}); result.Error != nil {
		return fmt.Errorf(
			"delete asset mint/burn events after slot %d: %w",
			slot,
			result.Error,
		)
	}

	if result := db.Where("slot > ?", slot).Delete(&models.Transaction{}); result.Error != nil {
		return result.Error
	}

	return nil
}

// SetGenesisStaking stores genesis pool registrations and stake delegations
// from the shelley-genesis.json staking section. It creates Pool,
// PoolRegistration, and Account records at slot 0.
//
// Idempotency notes:
//   - The slot-0 PoolRegistration is written with OnConflict{DoNothing}. Re-running
//     with mutated genesis data would leave stale historical fields on the slot-0
//     row, but a network's genesis file is immutable so this is intentional;
//     callers must not re-bootstrap with a different genesis against an existing
//     database.
//   - No synthetic slot-0 Registration / StakeDelegation history row is written
//     for the stakeDelegations entries here, unlike SetGenesisGovernance. This
//     leaves a known rollback hole: a later on-chain cert touching a
//     Shelley-genesis-delegated key, followed by a rollback past that cert, can
//     delete the genesis-rooted account because RestoreAccountStateAtSlot keys
//     off the registration table rather than Account.added_slot. Mainnet does
//     not exercise this path (its shelley-genesis declares no stake
//     delegations), so the hole has not been triaged for the test networks;
//     closing it is tracked separately.
func (d *MetadataStoreMysql) SetGenesisStaking(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	_ []byte,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Batch fetch all existing pools to avoid N+1 queries
	poolKeyHashes := make([][]byte, 0, len(pools))
	for _, cert := range pools {
		poolKeyHashes = append(poolKeyHashes, cert.Operator[:])
	}
	var existingPools []models.Pool
	if len(poolKeyHashes) > 0 {
		if result := db.Where(
			"pool_key_hash IN ?",
			poolKeyHashes,
		).Find(&existingPools); result.Error != nil {
			return fmt.Errorf(
				"batch fetch genesis pools: %w",
				result.Error,
			)
		}
	}
	existingPoolMap := make(map[string]*models.Pool, len(existingPools))
	for i := range existingPools {
		key := hex.EncodeToString(existingPools[i].PoolKeyHash)
		existingPoolMap[key] = &existingPools[i]
	}

	for _, cert := range pools {
		poolKey := hex.EncodeToString(cert.Operator[:])
		tmpPool := existingPoolMap[poolKey]
		if tmpPool == nil {
			tmpPool = &models.Pool{
				PoolKeyHash: cert.Operator[:],
				VrfKeyHash:  cert.VrfKeyHash[:],
			}
		}
		tmpPool.Pledge = types.Uint64(cert.Pledge)
		tmpPool.Cost = types.Uint64(cert.Cost)
		tmpPool.Margin = &types.Rat{Rat: cert.Margin.Rat}
		rewardAcctTag, rewardAcctHash, err := certutil.PoolRewardAccount(&cert)
		if err != nil {
			return fmt.Errorf("pool reward account: %w", err)
		}
		tmpPool.RewardAccount = rewardAcctHash
		tmpPool.RewardAccountCredentialTag = rewardAcctTag

		tmpReg := models.PoolRegistration{
			PoolKeyHash:                cert.Operator[:],
			VrfKeyHash:                 cert.VrfKeyHash[:],
			Pledge:                     types.Uint64(cert.Pledge),
			Cost:                       types.Uint64(cert.Cost),
			Margin:                     &types.Rat{Rat: cert.Margin.Rat},
			RewardAccount:              rewardAcctHash,
			RewardAccountCredentialTag: rewardAcctTag,
			AddedSlot:                  0,
		}
		if cert.PoolMetadata != nil {
			tmpReg.MetadataUrl = cert.PoolMetadata.Url
			tmpReg.MetadataHash = cert.PoolMetadata.Hash[:]
		}
		for _, owner := range cert.PoolOwners {
			tmpReg.Owners = append(
				tmpReg.Owners,
				models.PoolRegistrationOwner{KeyHash: owner[:]},
			)
		}
		tmpPool.Owners = tmpReg.Owners

		for _, relay := range cert.Relays {
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
			tmpReg.Relays = append(tmpReg.Relays, tmpRelay)
		}
		tmpPool.Relays = tmpReg.Relays

		if tmpPool.ID == 0 {
			result := db.Omit(clause.Associations).Create(tmpPool)
			if result.Error != nil {
				return fmt.Errorf(
					"create genesis pool: %w",
					result.Error,
				)
			}
		} else {
			result := db.Omit(clause.Associations).Save(tmpPool)
			if result.Error != nil {
				return fmt.Errorf(
					"save genesis pool: %w",
					result.Error,
				)
			}
		}
		tmpReg.PoolID = tmpPool.ID
		for i := range tmpReg.Owners {
			tmpReg.Owners[i].PoolID = tmpPool.ID
		}
		for i := range tmpReg.Relays {
			tmpReg.Relays[i].PoolID = tmpPool.ID
		}

		result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&tmpReg)
		if result.Error != nil {
			return fmt.Errorf(
				"create genesis pool registration: %w",
				result.Error,
			)
		}
	}

	for stakerHex, poolHex := range stakeDelegations {
		stakerBytes, err := hex.DecodeString(stakerHex)
		if err != nil {
			return fmt.Errorf(
				"decode staker hash %s: %w",
				stakerHex,
				err,
			)
		}
		poolBytes, err := hex.DecodeString(poolHex)
		if err != nil {
			return fmt.Errorf(
				"decode pool hash %s: %w",
				poolHex,
				err,
			)
		}

		account := &models.Account{
			StakingKey: stakerBytes,
			// Shelley genesis staking section encodes stake credentials as
			// raw 28-byte hashes with no type metadata. All Shelley-era
			// genesis stake credentials are key-hash by protocol design.
			CredentialTag: 0,
			Pool:          poolBytes,
			Active:        true,
			AddedSlot:     0,
		}
		// DoUpdates intentionally omits added_slot: RestoreAccountStateAtSlot
		// selects rows by `added_slot > rollback_slot`, so resetting a
		// non-zero added_slot back to 0 on a re-bootstrap (e.g. resumed
		// Mithril after partial sync) would make that row invisible to
		// every future rollback.
		result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "credential_tag"},
				{Name: "staking_key"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"pool", "active"}),
		}).Create(account)
		if result.Error != nil {
			return fmt.Errorf(
				"create genesis account: %w",
				result.Error,
			)
		}
	}

	return nil
}

// SetGenesisGovernance stores the initial DReps and stake/vote
// delegations described in the conway-genesis.json bootstrap section.
// All records are stamped at slot 0 so they appear as part of the
// initial ledger state.
func (d *MetadataStoreMysql) SetGenesisGovernance(
	initialDReps conway.ConwayGenesisInitialDReps,
	delegs conway.ConwayGenesisDelegs,
	_ []byte,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	for cred, state := range initialDReps {
		if cred == nil {
			continue
		}
		credentialTag, err := models.CredentialTagFromUint(cred.CredType)
		if err != nil {
			return fmt.Errorf("genesis drep credential type: %w", err)
		}
		drepCred := cred.Credential[:]
		drep := &models.Drep{
			CredentialTag: credentialTag,
			Credential:    drepCred,
			AddedSlot:     0,
			ExpiryEpoch:   state.Expiry,
			Active:        true,
		}
		if state.Anchor != nil {
			drep.AnchorURL = state.Anchor.Url
			drep.AnchorHash = state.Anchor.DataHash[:]
		}
		if result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "credential_tag"},
				{Name: "credential"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"added_slot",
				"anchor_url",
				"anchor_hash",
				"expiry_epoch",
				"active",
			}),
		}).Create(drep); result.Error != nil {
			return fmt.Errorf(
				"create genesis drep: %w",
				result.Error,
			)
		}

		reg := &models.RegistrationDrep{
			CredentialTag:  credentialTag,
			DrepCredential: drepCred,
			AddedSlot:      0,
			DepositAmount:  types.Uint64(state.Deposit),
		}
		if state.Anchor != nil {
			reg.AnchorURL = state.Anchor.Url
			reg.AnchorHash = state.Anchor.DataHash[:]
		}
		if result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "credential_tag"},
				{Name: "drep_credential"},
				{Name: "added_slot"},
			},
			DoNothing: true,
		}).Create(reg); result.Error != nil {
			return fmt.Errorf(
				"create genesis drep registration: %w",
				result.Error,
			)
		}
	}

	for cred, delegatee := range delegs {
		if cred == nil {
			continue
		}
		stakeKey := cred.Credential[:]
		credentialTag, err := models.CredentialTagFromUint(cred.CredType)
		if err != nil {
			return err
		}

		drepType, err := models.DrepTypeFromInt(delegatee.DRep.Type)
		if err != nil && delegatee.Type != conway.ConwayGenesisDelegateeTypeStake {
			return fmt.Errorf("genesis delegatee drep type: %w", err)
		}
		var drepCredential []byte
		if delegatee.Type != conway.ConwayGenesisDelegateeTypeStake &&
			drepType != models.DrepTypeAlwaysAbstain &&
			drepType != models.DrepTypeAlwaysNoConfidence {
			drepCredential = delegatee.DRep.Credential
		}

		account, err := d.getOrCreateAccount(credentialTag, stakeKey, txn)
		if err != nil {
			return fmt.Errorf(
				"get or create genesis delegatee account: %w",
				err,
			)
		}
		account.AddedSlot = 0

		// Conway genesis delegations implicitly register the staking
		// credential at slot 0 (no on-chain certificate, no deposit).
		// We materialize this as a synthetic Registration row so the
		// rollback path (RestoreAccountStateAtSlot) recognizes the
		// account as "existed at genesis" via its standard hasReg
		// check; without it, rolling back past a later on-chain cert
		// would delete a genesis-rooted account.
		var existingReg models.Registration
		regResult := db.Where(
			"credential_tag = ? AND staking_key = ? AND added_slot = ?",
			credentialTag, stakeKey, uint64(0),
		).Attrs(models.Registration{
			StakingKey:    stakeKey,
			CredentialTag: credentialTag,
			AddedSlot:     0,
		}).FirstOrCreate(&existingReg)
		if regResult.Error != nil {
			return fmt.Errorf(
				"create genesis registration: %w", regResult.Error,
			)
		}

		// Delegation history tables have no unique constraint that covers
		// (credential_tag, staking_key, added_slot), so we use FirstOrCreate
		// to keep the slot-0 row idempotent across retries (e.g., resumed
		// Mithril bootstrap). Each stake credential contributes at most one
		// genesis delegation row of a given type, so matching on tag + key +
		// added_slot is sufficient.
		switch delegatee.Type {
		case conway.ConwayGenesisDelegateeTypeStake:
			account.Pool = delegatee.PoolId[:]
			if err := saveAccount(account, db); err != nil {
				return fmt.Errorf(
					"save genesis stake delegatee account: %w", err,
				)
			}
			var existing models.StakeDelegation
			result := db.Where(
				"credential_tag = ? AND staking_key = ? AND added_slot = ?",
				credentialTag, stakeKey, uint64(0),
			).Attrs(models.StakeDelegation{
				StakingKey:    stakeKey,
				CredentialTag: credentialTag,
				PoolKeyHash:   delegatee.PoolId[:],
				AddedSlot:     0,
			}).FirstOrCreate(&existing)
			if result.Error != nil {
				return fmt.Errorf(
					"create genesis stake delegation: %w", result.Error,
				)
			}
		case conway.ConwayGenesisDelegateeTypeVote:
			account.Drep = drepCredential
			account.DrepType = drepType
			if err := saveAccount(account, db); err != nil {
				return fmt.Errorf(
					"save genesis vote delegatee account: %w", err,
				)
			}
			var existing models.VoteDelegation
			result := db.Where(
				"credential_tag = ? AND staking_key = ? AND added_slot = ?",
				credentialTag, stakeKey, uint64(0),
			).Attrs(models.VoteDelegation{
				StakingKey:    stakeKey,
				CredentialTag: credentialTag,
				Drep:          drepCredential,
				DrepType:      drepType,
				AddedSlot:     0,
			}).FirstOrCreate(&existing)
			if result.Error != nil {
				return fmt.Errorf(
					"create genesis vote delegation: %w", result.Error,
				)
			}
		case conway.ConwayGenesisDelegateeTypeStakeVote:
			account.Pool = delegatee.PoolId[:]
			account.Drep = drepCredential
			account.DrepType = drepType
			if err := saveAccount(account, db); err != nil {
				return fmt.Errorf(
					"save genesis stake/vote delegatee account: %w", err,
				)
			}
			var existing models.StakeVoteDelegation
			result := db.Where(
				"credential_tag = ? AND staking_key = ? AND added_slot = ?",
				credentialTag, stakeKey, uint64(0),
			).Attrs(models.StakeVoteDelegation{
				StakingKey:    stakeKey,
				CredentialTag: credentialTag,
				PoolKeyHash:   delegatee.PoolId[:],
				Drep:          drepCredential,
				DrepType:      drepType,
				AddedSlot:     0,
			}).FirstOrCreate(&existing)
			if result.Error != nil {
				return fmt.Errorf(
					"create genesis stake/vote delegation: %w", result.Error,
				)
			}
		default:
			return fmt.Errorf(
				"unknown genesis delegatee type: %d",
				delegatee.Type,
			)
		}
	}

	return nil
}

// DeleteAddressTransactionsAfterSlot removes address-transaction mapping records
// added after the given slot.
func (d *MetadataStoreMysql) DeleteAddressTransactionsAfterSlot(
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

// DeleteTransactionMetadataLabelsAfterSlot removes transaction metadata label
// index records added after the given slot.
func (d *MetadataStoreMysql) DeleteTransactionMetadataLabelsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.
		Where("slot > ?", slot).
		Delete(&models.TransactionMetadataLabel{}); result.Error != nil {
		return fmt.Errorf(
			"delete transaction metadata labels after slot %d: %w",
			slot,
			result.Error,
		)
	}
	return nil
}
