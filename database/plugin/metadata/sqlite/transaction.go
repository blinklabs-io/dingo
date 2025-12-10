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
	"github.com/blinklabs-io/gouroboros/cbor"
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

// processScripts is a generic helper to process any script type
func processScripts[T lcommon.Script](
	txn *gorm.DB,
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
		if result := txn.Create(&witnessScript); result.Error != nil {
			return fmt.Errorf("create witness script: %w", result.Error)
		}
		scriptContent := models.Script{
			Hash:        script.Hash().Bytes(),
			Type:        scriptType,
			Content:     script.RawScriptBytes(),
			CreatedSlot: point.Slot,
		}
		if result := txn.Clauses(clause.OnConflict{
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
func (d *MetadataStoreSqlite) getOrCreateAccount(
	stakeKey []byte,
	txn *gorm.DB,
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
	// SQLite's ON CONFLICT clause doesn't return the ID of an existing row when
	// the conflict path is taken (no insert occurs). We need to fetch the ID
	// explicitly so we can associate witness records with the correct transaction.
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
	// Extract and save witness set data
	// Delete existing witness records to ensure idempotency on retry
	if result := txn.Where("transaction_id = ?", tmpTx.ID).Delete(&models.KeyWitness{}); result.Error != nil {
		return fmt.Errorf("delete existing key witnesses: %w", result.Error)
	}
	if result := txn.Where("transaction_id = ?", tmpTx.ID).Delete(&models.WitnessScripts{}); result.Error != nil {
		return fmt.Errorf("delete existing witness scripts: %w", result.Error)
	}
	if result := txn.Where("transaction_id = ?", tmpTx.ID).Delete(&models.Redeemer{}); result.Error != nil {
		return fmt.Errorf("delete existing redeemers: %w", result.Error)
	}
	if result := txn.Where("transaction_id = ?", tmpTx.ID).Delete(&models.PlutusData{}); result.Error != nil {
		return fmt.Errorf("delete existing plutus data: %w", result.Error)
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
			if result := txn.Create(&keyWitness); result.Error != nil {
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
			if result := txn.Create(&keyWitness); result.Error != nil {
				return fmt.Errorf("create bootstrap witness: %w", result.Error)
			}
		}

		// Process all script types using the generic helper
		if err := processScripts(txn, tmpTx.ID, uint8(lcommon.ScriptRefTypeNativeScript), ws.NativeScripts(), point); err != nil {
			return err
		}
		if err := processScripts(txn, tmpTx.ID, uint8(lcommon.ScriptRefTypePlutusV1), ws.PlutusV1Scripts(), point); err != nil {
			return err
		}
		if err := processScripts(txn, tmpTx.ID, uint8(lcommon.ScriptRefTypePlutusV2), ws.PlutusV2Scripts(), point); err != nil {
			return err
		}
		if err := processScripts(txn, tmpTx.ID, uint8(lcommon.ScriptRefTypePlutusV3), ws.PlutusV3Scripts(), point); err != nil {
			return err
		}

		// Add PlutusData (Datums)
		for _, datum := range ws.PlutusData() {
			plutusData := models.PlutusData{
				TransactionID: tmpTx.ID,
				Data:          datum.Cbor(),
			}
			if result := txn.Create(&plutusData); result.Error != nil {
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
				if result := txn.Create(&redeemer); result.Error != nil {
					return fmt.Errorf("create redeemer: %w", result.Error)
				}
			}
		}
	}

	// Avoid updating associations
	result = txn.Omit(clause.Associations).Save(&tmpTx)
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
			if result := txn.Model(&models.Certificate{}).Where("transaction_id = ?", tmpTx.ID).Pluck("id", &unifiedIDs); result.Error != nil {
				return fmt.Errorf(
					"query existing unified certificates: %w",
					result.Error,
				)
			}
			if len(unifiedIDs) > 0 {
				// Delete specialized records linked to existing unified certificates
				tables := []string{
					"stake_registration", "pool_registration", "pool_retirement", "auth_committee_hot", "resign_committee_cold",
					"deregistration", "stake_delegation", "stake_registration_delegation", "stake_vote_delegation",
					"stake_vote_registration_delegation", "registration", "registration_drep", "deregistration_drep",
					"update_drep", "vote_delegation", "vote_registration_delegation", "move_instantaneous_rewards",
				}
				for _, table := range tables {
					if result := txn.Table(table).Where("certificate_id IN ?", unifiedIDs).Delete(nil); result.Error != nil {
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
				if result := txn.Clauses(clause.OnConflict{
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
					if result := txn.Where("transaction_id = ? AND cert_index = ?", tmpTx.ID, uint(i)).First(&unifiedCert); result.Error != nil { //nolint:gosec
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

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, txn); err != nil {
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
						result := txn.Clauses(clause.OnConflict{
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

					if err := saveCertRecord(&tmpItem, txn); err != nil {
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
						result := txn.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "staking_key"}},
							UpdateAll: true,
						}).Create(tmpAccount)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}

					tmpAccount.Active = false

					tmpItem := models.StakeDeregistration{
						StakingKey:    stakeKey,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, txn); err != nil {
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
						result := txn.Clauses(clause.OnConflict{
							Columns:   []clause.Column{{Name: "staking_key"}},
							UpdateAll: true,
						}).Create(tmpAccount)
						if result.Error != nil {
							return fmt.Errorf("process certificate: %w", result.Error)
						}
					}

					tmpAccount.Active = false

					tmpItem := models.Deregistration{
						StakingKey:    stakeKey,
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
						Amount:        types.Uint64(deposit),
					}

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, txn); err != nil {
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

					tmpItem := models.StakeDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, txn); err != nil {
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

					tmpReg := models.StakeRegistrationDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						AddedSlot:     point.Slot,
						DepositAmount: types.Uint64(deposit),
						CertificateID: certIDMap[i],
					}

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, txn); err != nil {
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

					tmpItem := models.StakeVoteDelegation{
						StakingKey:    stakeKey,
						PoolKeyHash:   c.PoolKeyHash[:],
						Drep:          c.Drep.Credential[:],
						AddedSlot:     point.Slot,
						CertificateID: certIDMap[i],
					}

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, txn); err != nil {
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

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, txn); err != nil {
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

					if err := saveCertRecord(&tmpReg, txn); err != nil {
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

					if err := saveCertRecord(&tmpDereg, txn); err != nil {
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

					if err := saveCertRecord(&tmpUpdate, txn); err != nil {
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

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, txn); err != nil {
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

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpReg, txn); err != nil {
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

					if err := saveAccountIfNew(tmpAccount, txn); err != nil {
						return fmt.Errorf("process certificate: %w", err)
					}

					if err := saveCertRecord(&tmpItem, txn); err != nil {
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

					if err := saveCertRecord(&tmpAuth, txn); err != nil {
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

					if err := saveCertRecord(&tmpResign, txn); err != nil {
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
					result := txn.Create(&tmpMIR)
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
						result := txn.Create(&tmpReward)
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

				if result := txn.Exec(query, values...); result.Error != nil {
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
