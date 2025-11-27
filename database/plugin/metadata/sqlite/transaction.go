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
func (d *MetadataStoreSqlite) getOrCreateAccount(
	stakeKey []byte,
	txn *gorm.DB,
) (*models.Account, error) {
	tmpAccount, err := d.GetAccount(stakeKey, txn)
	if err != nil {
		if !errors.Is(err, models.ErrAccountNotFound) {
			return nil, err
		}
	}
	if tmpAccount == nil {
		tmpAccount = &models.Account{
			StakingKey: stakeKey,
		}
	}
	return tmpAccount, nil
}

// saveAccountIfNew saves the account if it doesn't exist in the database yet
func saveAccountIfNew(account *models.Account, txn *gorm.DB) error {
	if account.ID == 0 {
		result := txn.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "staking_key"}},
			DoUpdates: clause.AssignmentColumns(
				[]string{"pool", "drep"},
			),
		}).Create(account)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := txn.Save(account)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// saveCertRecord saves a certificate record and returns any error
func saveCertRecord(record any, txn *gorm.DB) error {
	result := txn.Create(record)
	return result.Error
}

// SetTransaction adds a new transaction to the database and processes all certificates
func (d *MetadataStoreSqlite) SetTransaction(
	tx lcommon.Transaction,
	point ocommon.Point,
	idx uint32,
	certDeposits map[int]uint64,
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
		Fee:        types.Uint64(tx.Fee()),
		TTL:        types.Uint64(tx.TTL()),
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
			d.Logger.Warn(
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
				d.Logger.Warn(
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
				d.Logger.Warn(
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
			d.Logger.Warn(
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

	// Process certificates - all certificate types are handled here in a consolidated manner
	// This centralizes certificate processing logic within the metadata layer following DRY principles
	certs := tx.Certificates()
	if len(certs) > 0 {
		for i, cert := range certs {
			deposit := uint64(0)
			if certDeposits != nil {
				if depositVal, ok := certDeposits[i]; ok {
					deposit = depositVal
				} else if certRequiresDeposit(cert) {
					d.Logger.Warn("missing deposit for deposit-bearing certificate",
						"index", i, "type", fmt.Sprintf("%T", cert))
				}
			}
			switch c := cert.(type) {
			case *lcommon.PoolRegistrationCertificate:
				// Pool registration logic here
				tmpPool, err := d.GetPool(lcommon.PoolKeyHash(c.Operator[:]), txn)
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
					CertificateID: uint(i), //nolint:gosec
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
				if tmpPool.ID == 0 {
					result := txn.Create(tmpPool)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}
				} else {
					result := txn.Save(tmpPool)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}
				}
				tmpReg.PoolID = tmpPool.ID

				// Save the registration record
				result := txn.Create(&tmpReg)
				if result.Error != nil {
					return fmt.Errorf("process certificate: %w", result.Error)
				}
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
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpReg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.PoolRetirementCertificate:
				tmpPool, err := d.GetPool(lcommon.PoolKeyHash(c.PoolKeyHash[:]), txn)
				if err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
				if tmpPool == nil {
					d.Logger.Warn("retiring non-existent pool", "hash", c.PoolKeyHash)
					tmpPool = &models.Pool{PoolKeyHash: c.PoolKeyHash[:]}
					result := txn.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "pool_key_hash"}},
						UpdateAll: true,
					}).Create(&tmpPool)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}
				}

				tmpItem := models.PoolRetirement{
					PoolKeyHash: c.PoolKeyHash[:],
					Epoch:       c.Epoch,
					AddedSlot:   point.Slot,
					PoolID:      tmpPool.ID,
				}

				if err := saveCertRecord(&tmpItem, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.StakeDeregistrationCertificate:
				stakeKey := c.StakeCredential.Credential[:]
				tmpAccount, err := d.GetAccount(stakeKey, txn)
				if err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
				if tmpAccount == nil {
					d.Logger.Warn("deregistering non-existent account", "hash", stakeKey)
					tmpAccount = &models.Account{
						StakingKey: stakeKey,
					}
					result := txn.Clauses(clause.OnConflict{
						Columns:   []clause.Column{{Name: "staking_key"}},
						UpdateAll: true,
					}).Create(tmpAccount)
					if result.Error != nil {
						return fmt.Errorf("process certificate: %w", result.Error)
					}
				}

				tmpItem := models.StakeDeregistration{
					StakingKey:    stakeKey,
					AddedSlot:     point.Slot,
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpItem, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.StakeDelegationCertificate:
				stakeKey := c.StakeCredential.Credential[:]
				tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
				if err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				tmpAccount.Pool = c.PoolKeyHash[:]

				tmpItem := models.StakeDelegation{
					StakingKey:    stakeKey,
					PoolKeyHash:   c.PoolKeyHash[:],
					AddedSlot:     point.Slot,
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpItem, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.StakeRegistrationDelegationCertificate:
				stakeKey := c.StakeCredential.Credential[:]
				tmpAccount, err := d.getOrCreateAccount(stakeKey, txn)
				if err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				tmpAccount.Pool = c.PoolKeyHash[:]

				tmpReg := models.StakeRegistrationDelegation{
					StakingKey:    stakeKey,
					PoolKeyHash:   c.PoolKeyHash[:],
					AddedSlot:     point.Slot,
					DepositAmount: types.Uint64(deposit),
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpReg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
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
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpReg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.RegistrationDrepCertificate:
				drepCredential := c.DrepCredential.Credential[:]

				tmpReg := models.RegistrationDrep{
					DrepCredential: drepCredential,
					AddedSlot:      point.Slot,
					DepositAmount:  types.Uint64(deposit),
					CertificateID:  uint(i), //nolint:gosec
				}
				if c.Anchor != nil {
					tmpReg.AnchorUrl = c.Anchor.Url
					tmpReg.AnchorHash = c.Anchor.DataHash[:]
				}

				if err := saveCertRecord(&tmpReg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.DeregistrationDrepCertificate:
				drepCredential := c.DrepCredential.Credential[:]

				tmpDereg := models.DeregistrationDrep{
					DrepCredential: drepCredential,
					AddedSlot:      point.Slot,
					DepositAmount:  types.Uint64(deposit),
					CertificateID:  uint(i), //nolint:gosec
				}

				if err := saveCertRecord(&tmpDereg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.UpdateDrepCertificate:
				drepCredential := c.DrepCredential.Credential[:]

				tmpUpdate := models.UpdateDrep{
					Credential:    drepCredential,
					AddedSlot:     point.Slot,
					CertificateID: uint(i), //nolint:gosec
				}
				if c.Anchor != nil {
					tmpUpdate.AnchorUrl = c.Anchor.Url
					tmpUpdate.AnchorHash = c.Anchor.DataHash[:]
				}

				if err := saveCertRecord(&tmpUpdate, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
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
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpReg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
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
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpReg, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
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
					CertificateID: uint(i), //nolint:gosec
				}

				if err := saveAccountIfNew(tmpAccount, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}

				if err := saveCertRecord(&tmpItem, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.AuthCommitteeHotCertificate:
				coldCredential := c.ColdCredential.Credential[:]
				hotCredential := c.HotCredential.Credential[:]

				tmpAuth := models.AuthCommitteeHot{
					ColdCredential: coldCredential,
					HostCredential: hotCredential,
					AddedSlot:      point.Slot,
				}

				if err := saveCertRecord(&tmpAuth, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			case *lcommon.ResignCommitteeColdCertificate:
				coldCredential := c.ColdCredential.Credential[:]

				tmpResign := models.ResignCommitteeCold{
					ColdCredential: coldCredential,
					AddedSlot:      point.Slot,
				}
				if c.Anchor != nil {
					tmpResign.AnchorUrl = c.Anchor.Url
					tmpResign.AnchorHash = c.Anchor.DataHash[:]
				}

				if err := saveCertRecord(&tmpResign, txn); err != nil {
					return fmt.Errorf("process certificate: %w", err)
				}
			default:
				return fmt.Errorf("unsupported certificate type %T", cert)
			}
		}
	}

	if err := d.storeTransactionDatums(tx, point.Slot, txn); err != nil {
		return fmt.Errorf("store datums failed: %w", err)
	}

	return nil
}

// Traverse each utxo and check for inline datum & calls storeDatum
func (d *MetadataStoreSqlite) storeTransactionDatums(
	tx lcommon.Transaction,
	slot uint64,
	txn *gorm.DB,
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
	txn *gorm.DB,
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
