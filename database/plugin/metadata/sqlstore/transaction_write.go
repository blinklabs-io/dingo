// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//nolint:gosec,rowserrcheck,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type immediateBatchAccumulator struct{}

func (*immediateBatchAccumulator) Reset() {}

func (s *Store) NewBatchAccumulator() types.MetadataBatchAccumulator {
	return &immediateBatchAccumulator{}
}

func (s *Store) FlushBatch(
	accumulator types.MetadataBatchAccumulator,
	_ types.Txn,
) error {
	if _, ok := accumulator.(*immediateBatchAccumulator); !ok {
		return fmt.Errorf(
			"sqlstore FlushBatch: wrong accumulator type %T",
			accumulator,
		)
	}
	accumulator.Reset()
	return nil
}

func (s *Store) SetTransactionBatched(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	if _, ok := accumulator.(*immediateBatchAccumulator); !ok {
		return fmt.Errorf(
			"SetTransactionBatched: wrong accumulator type %T",
			accumulator,
		)
	}
	return s.SetTransaction(
		transaction,
		point,
		index,
		certDeposits,
		skipWithdrawalWitness,
		txn,
	)
}

func (s *Store) SetTransaction(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
) error {
	if transaction == nil {
		return errors.New("set transaction: nil transaction")
	}
	hash := transaction.Hash().Bytes()
	var (
		metadataValue  []byte
		metadataLabels []labelcodec.Entry
	)
	if transaction.Metadata() != nil &&
		s.storageMode == types.StorageModeAPI {
		var err error
		metadataValue, metadataLabels, err = labelcodec.EncodeAndExtract(
			transaction.Metadata(),
		)
		if err != nil {
			return fmt.Errorf("extract transaction metadata: %w", err)
		}
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			collateralFee, err := collateralFeeForTransaction(
				ctx,
				db,
				transaction,
			)
			if err != nil {
				return err
			}
			transactionID, err := queryReturnedID(ctx, db, `
INSERT INTO "transaction" (
    hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hash) DO UPDATE SET
    block_hash = excluded.block_hash,
    block_index = excluded.block_index,
    slot = excluded.slot,
    collateral_fee = excluded.collateral_fee
RETURNING id`,
				hash,
				point.Hash,
				metadataValue,
				point.Slot,
				transaction.Type(),
				decimalUint64(types.Uint64(transaction.Fee().Uint64())),
				decimalUint64(types.Uint64(collateralFee)),
				decimalUint64(types.Uint64(transaction.TTL())),
				index,
				transaction.IsValid(),
			)
			if err != nil {
				return fmt.Errorf("create transaction %x: %w", hash, err)
			}
			if err := s.applyTransactionMetadataLabels(
				ctx,
				db,
				transactionID,
				point.Slot,
				metadataLabels,
			); err != nil {
				return err
			}
			if err := s.applyTransactionAssetMintBurn(
				ctx,
				db,
				transaction,
				hash,
				point.Slot,
				index,
			); err != nil {
				return err
			}
			if transaction.IsValid() {
				if err := s.applyTransactionWithdrawals(
					ctx,
					db,
					transaction,
					point.Slot,
					hash,
					skipWithdrawalWitness,
				); err != nil {
					return err
				}
				certificateRefs, err := s.applyTransactionCertificates(
					ctx,
					db,
					transactionID,
					transaction.Certificates(),
					point,
					index,
					certDeposits,
				)
				if err != nil {
					return err
				}
				if err := s.refreshRewardLiveStakeRefs(
					ctx,
					db,
					certificateRefs,
					point.Slot,
				); err != nil {
					return err
				}
			}
			collateralReturn := transaction.CollateralReturn()
			producedModels := make(
				[]models.Utxo,
				0,
				len(transaction.Produced()),
			)
			producedStakeRefs := make([]models.StakeCredentialRef, 0)
			for _, produced := range transaction.Produced() {
				model, err := models.UtxoLedgerToModel(produced, point.Slot)
				if err != nil {
					return fmt.Errorf(
						"convert output %d: %w",
						produced.Id.Index(),
						err,
					)
				}
				if collateralReturn != nil &&
					produced.Output == collateralReturn {
					id := uint(transactionID)
					model.CollateralReturnForTxID = &id
				} else {
					id := uint(transactionID)
					model.TransactionID = &id
				}
				if err := s.insertUtxoModel(ctx, db, &model, true); err != nil {
					return fmt.Errorf(
						"create output %x#%d: %w",
						model.TxId,
						model.OutputIdx,
						err,
					)
				}
				producedModels = append(producedModels, model)
				if len(model.StakingKey) > 0 {
					producedStakeRefs = append(producedStakeRefs,
						models.NewStakeCredentialRef(
							model.CredentialTag,
							model.StakingKey,
						),
					)
				}
			}
			if err := s.applyTransactionAPIDetails(
				ctx,
				db,
				transactionID,
				transaction,
				point.Slot,
				index,
				producedModels,
			); err != nil {
				return err
			}
			refs := make([]models.UtxoId, 0, len(transaction.Consumed()))
			seenConsumed := make(
				map[string]struct{},
				len(transaction.Consumed()),
			)
			for _, input := range transaction.Consumed() {
				refKey := fmt.Sprintf(
					"%x:%d",
					input.Id().Bytes(),
					input.Index(),
				)
				if _, ok := seenConsumed[refKey]; ok {
					continue
				}
				seenConsumed[refKey] = struct{}{}
				refs = append(refs, models.UtxoId{
					Hash: input.Id().Bytes(),
					Idx:  input.Index(),
				})
				result, err := db.ExecContext(ctx, `
UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE tx_id = ? AND output_idx = ?
  AND deleted_slot = 0 AND spent_at_tx_id IS NULL`,
					point.Slot,
					hash,
					input.Id().Bytes(),
					input.Index(),
				)
				if err != nil {
					return err
				}
				affected, err := result.RowsAffected()
				if err != nil {
					return err
				}
				if affected > 0 {
					continue
				}
				var (
					deletedSlot uint64
					spentBy     []byte
				)
				err = db.QueryRowContext(ctx, `
SELECT deleted_slot, spent_at_tx_id
FROM utxo WHERE tx_id = ? AND output_idx = ?`,
					input.Id().Bytes(),
					input.Index(),
				).Scan(&deletedSlot, &spentBy)
				if errors.Is(err, sql.ErrNoRows) {
					// Gap/partial-history ingestion intentionally tolerates a
					// missing producer output.
					continue
				}
				if err != nil {
					return err
				}
				if bytes.Equal(spentBy, hash) {
					continue
				}
				if deletedSlot == 0 && len(spentBy) == 0 {
					return fmt.Errorf(
						"consume UTxO %x#%d: row was not updated",
						input.Id().Bytes(),
						input.Index(),
					)
				}
				return fmt.Errorf(
					"%w: %x:%d",
					types.ErrUtxoConflict,
					input.Id().Bytes(),
					input.Index(),
				)
			}
			stakeRefs, err := queryUtxoStakeRefs(ctx, db, refs, false)
			if err != nil {
				return err
			}
			stakeRefs = append(stakeRefs, producedStakeRefs...)
			return s.refreshRewardLiveStakeRefs(ctx, db, stakeRefs, point.Slot)
		},
	)
}

func (s *Store) SetGapBlockTransaction(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	txn types.Txn,
) error {
	// Gap ingestion intentionally has no available input state. Persisting only
	// the transaction and produced outputs is equivalent to SetTransaction
	// after suppressing the consumed-input update.
	if transaction == nil {
		return errors.New("set gap transaction: nil transaction")
	}
	hash := transaction.Hash().Bytes()
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			collateralFee, err := collateralFeeForTransaction(
				ctx,
				db,
				transaction,
			)
			if err != nil {
				return err
			}
			transactionID, err := queryReturnedID(ctx, db, `
INSERT INTO "transaction" (
    hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hash) DO UPDATE SET
    block_hash = excluded.block_hash, block_index = excluded.block_index,
    slot = excluded.slot, collateral_fee = excluded.collateral_fee
RETURNING id`,
				hash,
				point.Hash,
				point.Slot,
				transaction.Type(),
				decimalUint64(types.Uint64(transaction.Fee().Uint64())),
				decimalUint64(types.Uint64(collateralFee)),
				decimalUint64(types.Uint64(transaction.TTL())),
				index,
				transaction.IsValid(),
			)
			if err != nil {
				return err
			}
			collateralReturn := transaction.CollateralReturn()
			stakeRefs := make([]models.StakeCredentialRef, 0)
			for _, produced := range transaction.Produced() {
				model, err := models.UtxoLedgerToModel(produced, point.Slot)
				if err != nil {
					return fmt.Errorf(
						"convert output %d: %w",
						produced.Id.Index(),
						err,
					)
				}
				id := uint(transactionID)
				if collateralReturn != nil &&
					produced.Output == collateralReturn {
					model.CollateralReturnForTxID = &id
				} else {
					model.TransactionID = &id
				}
				if err := s.insertUtxoModel(ctx, db, &model, true); err != nil {
					return err
				}
				if len(model.StakingKey) > 0 {
					stakeRefs = append(stakeRefs, models.NewStakeCredentialRef(
						model.CredentialTag,
						model.StakingKey,
					))
				}
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, stakeRefs, point.Slot)
		},
	)
}

func (s *Store) RecomputeGapCollateralFee(
	transaction lcommon.Transaction,
	_ ocommon.Point,
	txn types.Txn,
) error {
	if transaction.IsValid() {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			fee, err := collateralFeeForTransaction(ctx, db, transaction)
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, `
UPDATE "transaction" SET collateral_fee = ? WHERE hash = ?`,
				decimalUint64(types.Uint64(fee)),
				transaction.Hash().Bytes(),
			)
			return err
		},
	)
}

func (s *Store) SetGenesisTransaction(
	hash []byte,
	blockHash []byte,
	outputs []models.Utxo,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			id, err := queryReturnedID(ctx, db, `
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, 0, 0, '0', '0', '0', 0, TRUE)
ON CONFLICT (hash) DO UPDATE SET hash = excluded.hash
RETURNING id`,
				hash,
				blockHash,
			)
			if err != nil {
				return fmt.Errorf(
					"create genesis transaction %x: %w",
					hash,
					err,
				)
			}
			transactionID := uint(id)
			refs := make([]models.StakeCredentialRef, 0, len(outputs))
			for i := range outputs {
				outputs[i].ID = 0
				outputs[i].TransactionID = &transactionID
				if err := s.insertUtxoModel(ctx, db, &outputs[i], true); err != nil {
					return err
				}
				refs = append(refs, models.NewStakeCredentialRef(
					outputs[i].CredentialTag,
					outputs[i].StakingKey,
				))
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, 0)
		},
	)
}

func (s *Store) DeleteTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			rows, err := db.QueryContext(ctx, `
SELECT hash FROM "transaction" WHERE slot > ?`,
				slot,
			)
			if err != nil {
				return err
			}
			hashes := [][]byte{}
			for rows.Next() {
				var hash []byte
				if err := rows.Scan(&hash); err != nil {
					rows.Close()
					return err
				}
				hashes = append(hashes, hash)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("scan transactions for rollback: %w", err)
			}
			refs := []models.StakeCredentialRef{}
			for start := 0; start < len(hashes); start += 400 {
				end := min(start+400, len(hashes))
				args := make([]any, end-start)
				for i, hash := range hashes[start:end] {
					args[i] = hash
				}
				stakeRows, err := db.QueryContext(ctx, `
SELECT DISTINCT credential_tag, staking_key FROM utxo
WHERE spent_at_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				)
				if err != nil {
					return err
				}
				for stakeRows.Next() {
					var ref models.StakeCredentialRef
					if err := stakeRows.Scan(&ref.Tag, &ref.Key); err != nil {
						stakeRows.Close()
						return err
					}
					refs = append(refs, ref)
				}
				if err := stakeRows.Close(); err != nil {
					return err
				}
				if err := stakeRows.Err(); err != nil {
					return fmt.Errorf(
						"scan affected stake credentials for rollback: %w",
						err,
					)
				}
				if _, err := db.ExecContext(ctx, `
UPDATE utxo SET spent_at_tx_id = NULL, deleted_slot = 0
WHERE spent_at_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
UPDATE utxo SET collateral_by_tx_id = NULL
WHERE collateral_by_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
UPDATE utxo SET referenced_by_tx_id = NULL
WHERE referenced_by_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
DELETE FROM utxo_reference_input
WHERE transaction_hash IN (`+bindPlaceholders(len(args))+`)`, args...); err != nil {
					return err
				}
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM transaction_metadata_label WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM asset_mint_burn WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(
				ctx,
				`DELETE FROM "transaction" WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

func (s *Store) insertUtxoModel(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	ignoreConflict bool,
) error {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return err
	}
	conflict := ""
	if ignoreConflict {
		conflict = " ON CONFLICT (tx_id, output_idx) DO NOTHING"
	}
	var id int64
	err = db.QueryRowContext(ctx, `
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`+conflict+`
RETURNING id`,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) && ignoreConflict {
		err = db.QueryRowContext(ctx, `
SELECT id FROM utxo WHERE tx_id = ? AND output_idx = ?`,
			params.TxID,
			params.OutputIdx,
		).Scan(&id)
		if err == nil {
			// Snapshot imports can create an output before its producer
			// transaction is replayed. Once that transaction is known, fill in
			// the provenance without overwriting an already-linked output.
			_, err = db.ExecContext(ctx, `
UPDATE utxo
SET transaction_id = COALESCE(transaction_id, ?),
    collateral_return_for_tx_id = COALESCE(collateral_return_for_tx_id, ?)
WHERE id = ?`,
				params.TransactionID,
				params.CollateralReturnForTxID,
				id,
			)
		}
	}
	if err != nil {
		return err
	}
	utxo.ID = uint(id)
	q := s.operationalQueries(db)
	for i := range utxo.Assets {
		asset := &utxo.Assets[i]
		asset.UtxoID = utxo.ID
		err := q.ImportAsset(
			ctx,
			sqlitequery.ImportAssetParams{
				Name:        asset.Name,
				NameHex:     asset.NameHex,
				PolicyID:    asset.PolicyId,
				Fingerprint: asset.Fingerprint,
				UtxoID:      sql.NullInt64{Int64: id, Valid: true},
				Amount: sql.NullString{
					String: decimalUint64(asset.Amount),
					Valid:  true,
				},
			},
		)
		if err != nil {
			return err
		}
		var assetID uint
		if err := db.QueryRowContext(ctx, `
SELECT id FROM asset
WHERE utxo_id = ? AND policy_id = ? AND name = ?
ORDER BY id DESC LIMIT 1`,
			id,
			asset.PolicyId,
			asset.Name,
		).Scan(&assetID); err != nil {
			return err
		}
		asset.ID = assetID
	}
	return nil
}

func collateralFeeForTransaction(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
) (uint64, error) {
	if transaction.IsValid() {
		return 0, nil
	}
	if total := transaction.TotalCollateral(); total != nil &&
		total.Sign() > 0 {
		if !total.IsUint64() {
			return 0, errors.New("total collateral exceeds uint64")
		}
		return total.Uint64(), nil
	}
	var total uint64
	seen := make(map[string]struct{})
	for _, input := range transaction.Collateral() {
		key := fmt.Sprintf("%x:%d", input.Id().Bytes(), input.Index())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var amount string
		err := db.QueryRowContext(ctx, `
SELECT amount FROM utxo WHERE tx_id = ? AND output_idx = ?`,
			input.Id().Bytes(),
			input.Index(),
		).Scan(&amount)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		value, err := parseUint64("collateral input", amount)
		if err != nil {
			return 0, err
		}
		if value > math.MaxUint64-total {
			return 0, errors.New("collateral input sum overflow")
		}
		total += value
	}
	if output := transaction.CollateralReturn(); output != nil {
		amount := output.Amount()
		if amount != nil && amount.Sign() > 0 {
			if !amount.IsUint64() || amount.Uint64() > total {
				return 0, nil
			}
			total -= amount.Uint64()
		}
	}
	return total, nil
}

func nullBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func (s *Store) applyTransactionWithdrawals(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
	slot uint64,
	txHash []byte,
	skipWithdrawalWitness bool,
) error {
	for address, amount := range transaction.Withdrawals() {
		if address == nil || amount == nil {
			continue
		}
		if amount.Sign() < 0 || !amount.IsUint64() {
			return fmt.Errorf(
				"invalid reward withdrawal amount %s",
				amount.String(),
			)
		}
		stakeKey := address.StakeKeyHash()
		if stakeKey == (lcommon.Blake2b224{}) {
			return errors.New("reward withdrawal missing stake credential")
		}
		tag, ok := models.StakeCredentialTagFromAddress(*address)
		if !ok {
			return errors.New("derive reward withdrawal credential tag")
		}
		if !skipWithdrawalWitness {
			// CIP-0163: only the delegator-inactivity gate's rollback/renewal
			// paths read this table (see BatchedTxIngestOpts.
			// SkipWithdrawalWitnessWrite), so gate-off callers elide the
			// insert rather than growing an unbounded, never-pruned table
			// nothing reads (issue #2919).
			if _, err := db.ExecContext(ctx, `
INSERT INTO account_withdrawal_witness (
    staking_key, credential_tag, tx_hash, added_slot
) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_hash, credential_tag, staking_key) DO NOTHING`,
				stakeKey.Bytes(),
				tag,
				txHash,
				slot,
			); err != nil {
				return err
			}
		}
		var accountID uint
		var reward sql.NullString
		err := db.QueryRowContext(ctx, `
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`,
			tag,
			stakeKey.Bytes(),
		).Scan(&accountID, &reward)
		if errors.Is(err, sql.ErrNoRows) {
			return models.ErrAccountNotFound
		}
		if err != nil {
			return err
		}
		var exists bool
		if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM account_reward_delta
    WHERE withdrawal = TRUE AND tx_hash = ?
      AND credential_tag = ? AND staking_key = ?
)`,
			txHash,
			tag,
			stakeKey.Bytes(),
		).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		previous, err := parseNullUint64("account reward", reward)
		if err != nil {
			return err
		}
		if amount.Uint64() > previous {
			return fmt.Errorf(
				"reward withdrawal amount %s exceeds account balance %d: %w",
				amount.String(),
				previous,
				models.ErrRewardWithdrawalExceedsBalance,
			)
		}
		if amount.Sign() == 0 {
			continue
		}
		rewardAfter := previous - amount.Uint64()
		if _, err := db.ExecContext(ctx, `
UPDATE account SET reward = ? WHERE id = ?`,
			strconv.FormatUint(rewardAfter, 10),
			accountID,
		); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal
) VALUES (?, ?, ?, ?, ?, ?, TRUE)
ON CONFLICT (
    withdrawal, tx_hash, credential_tag, staking_key, added_slot
) DO NOTHING`,
			stakeKey.Bytes(),
			tag,
			txHash,
			strconv.FormatUint(amount.Uint64(), 10),
			strconv.FormatUint(previous, 10),
			slot,
		); err != nil {
			return err
		}
		if err := s.refreshRewardLiveStakeAggregate(
			ctx,
			db,
			models.NewStakeCredentialRef(tag, stakeKey.Bytes()),
			slot,
		); err != nil {
			return err
		}
	}
	return nil
}
