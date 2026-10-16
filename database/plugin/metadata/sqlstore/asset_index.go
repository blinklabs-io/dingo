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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlstore

import (
	"database/sql"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func orderDirection(descending bool) string {
	if descending {
		return "DESC"
	}
	return "ASC"
}

// nonNilName keeps an empty asset name a real empty value: a nil slice binds
// as NULL, which matches no row, while the empty name is a legal asset name.
func nonNilName(name []byte) []byte {
	if name == nil {
		return []byte{}
	}
	return name
}

// ListMintedAssets implements metadata.MetadataStore. Assets are ordered by
// the smallest mint/burn row id they own: ids follow application order, so
// that is the position of the asset's first recorded event.
func (s *Store) ListMintedAssets(
	policyID []byte,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.MintedAsset, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query := `SELECT policy_id, name FROM asset_mint_burn`
	args := []any{}
	if policyID != nil {
		query += ` WHERE policy_id = ?`
		args = append(args, policyID)
	}
	query += ` GROUP BY policy_id, name ORDER BY MIN(id) ` +
		orderDirection(descending)
	query, args = addLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("list minted assets: %w", err)
	}
	defer rows.Close()
	ret := []models.MintedAsset{}
	for rows.Next() {
		var asset models.MintedAsset
		if err := rows.Scan(&asset.PolicyId, &asset.Name); err != nil {
			return nil, fmt.Errorf("scan minted asset: %w", err)
		}
		ret = append(ret, asset)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list minted assets: %w", err)
	}
	return ret, nil
}

// CountMintedAssets implements metadata.MetadataStore.
func (s *Store) CountMintedAssets(
	policyID []byte,
	txn types.Txn,
) (int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	query := `SELECT COUNT(*) FROM (SELECT 1 FROM asset_mint_burn`
	args := []any{}
	if policyID != nil {
		query += ` WHERE policy_id = ?`
		args = append(args, policyID)
	}
	query += ` GROUP BY policy_id, name) AS minted`
	var count int
	if err := db.QueryRowContext(
		ctx, s.dialect.Rebind(query), args...,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count minted assets: %w", err)
	}
	return count, nil
}

// GetAssetMintBurnHistory implements metadata.MetadataStore.
func (s *Store) GetAssetMintBurnHistory(
	policyID lcommon.Blake2b224,
	assetName []byte,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.AssetMintBurn, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	dir := orderDirection(descending)
	query := `
SELECT id, tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index
FROM asset_mint_burn
WHERE policy_id = ? AND name = ?
ORDER BY slot ` + dir + `, tx_index ` + dir + `, id ` + dir
	query, args := addLimitOffset(
		query,
		[]any{policyID[:], nonNilName(assetName)},
		limit,
		offset,
	)
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("get asset mint burn history: %w", err)
	}
	defer rows.Close()
	ret := []models.AssetMintBurn{}
	for rows.Next() {
		var (
			row      models.AssetMintBurn
			id       int64
			slot     sql.NullInt64
			quantity sql.NullString
			txIndex  sql.NullInt64
		)
		if err := rows.Scan(
			&id,
			&row.TxHash,
			&row.PolicyId,
			&row.Name,
			&row.Fingerprint,
			&slot,
			&quantity,
			&txIndex,
		); err != nil {
			return nil, fmt.Errorf("scan asset mint burn row: %w", err)
		}
		row.ID = uint(id) // #nosec G115 -- autoincrement ids are positive
		if slot.Valid {
			row.Slot, err = checkedUint64(slot.Int64)
			if err != nil {
				return nil, fmt.Errorf("asset mint burn slot: %w", err)
			}
		}
		if txIndex.Valid {
			index, err := checkedUint64(txIndex.Int64)
			if err != nil || index > uint64(^uint32(0)) {
				return nil, fmt.Errorf(
					"asset mint burn tx index %d out of range",
					txIndex.Int64,
				)
			}
			row.TxIndex = uint32(index) // #nosec G115 -- range checked above
		}
		row.Quantity = quantity.String
		ret = append(ret, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get asset mint burn history: %w", err)
	}
	return ret, nil
}

// CountAssetMintBurnHistory implements metadata.MetadataStore.
func (s *Store) CountAssetMintBurnHistory(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int
	if err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			`SELECT COUNT(*) FROM asset_mint_burn WHERE policy_id = ? AND name = ?`,
		),
		policyID[:], nonNilName(assetName),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count asset mint burn history: %w", err)
	}
	return count, nil
}

// assetTransactionsSource selects every transaction involving one asset as a
// derived table of (hash, block_hash, slot, block_index, id), with the range
// predicate applied by the caller. Each branch starts from an indexed asset
// or mint/burn lookup. Branches are UNIONed rather than OR-ed so that none
// loses its index and a transaction matching several branches appears once.
const assetTransactionsSource = `
SELECT t.hash AS hash, t.block_hash AS block_hash, t.slot AS slot,
       t.block_index AS block_index, t.id AS id
FROM "transaction" AS t
WHERE t.id IN (
    SELECT u.transaction_id FROM asset AS a
    JOIN utxo AS u ON u.id = a.utxo_id
    WHERE a.policy_id = ? AND a.name = ? AND u.transaction_id IS NOT NULL
    UNION
    SELECT u.collateral_return_for_tx_id FROM asset AS a
    JOIN utxo AS u ON u.id = a.utxo_id
    WHERE a.policy_id = ? AND a.name = ?
      AND u.collateral_return_for_tx_id IS NOT NULL
)
UNION
SELECT t.hash, t.block_hash, t.slot, t.block_index, t.id
FROM "transaction" AS t
WHERE t.hash IN (
    SELECT u.spent_at_tx_id FROM asset AS a
    JOIN utxo AS u ON u.id = a.utxo_id
    WHERE a.policy_id = ? AND a.name = ? AND u.spent_at_tx_id IS NOT NULL
    UNION
    SELECT m.tx_hash FROM asset_mint_burn AS m
    WHERE m.policy_id = ? AND m.name = ?
)`

func assetTransactionsQuery(
	policyID lcommon.Blake2b224,
	assetName []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
) (string, []any, error) {
	name := nonNilName(assetName)
	args := make([]any, 0, 12)
	for range 4 {
		args = append(args, policyID[:], name)
	}
	predicate := ""
	if from != nil {
		slot, err := checkedInt64(from.Slot)
		if err != nil {
			return "", nil, err
		}
		predicate += ` AND (matched.slot > ? OR (matched.slot = ? AND COALESCE(matched.block_index, 0) >= ?))`
		args = append(args, slot, slot, from.TxIndex)
	}
	if to != nil {
		slot, err := checkedInt64(to.Slot)
		if err != nil {
			return "", nil, err
		}
		predicate += ` AND (matched.slot < ? OR (matched.slot = ? AND COALESCE(matched.block_index, 0) <= ?))`
		args = append(args, slot, slot, to.TxIndex)
	}
	return `(` + assetTransactionsSource + `) AS matched WHERE 1 = 1` + predicate,
		args, nil
}

// GetAssetTransactions implements metadata.MetadataStore.
func (s *Store) GetAssetTransactions(
	policyID lcommon.Blake2b224,
	assetName []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.Transaction, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	source, args, err := assetTransactionsQuery(policyID, assetName, from, to)
	if err != nil {
		return nil, fmt.Errorf("asset transactions range: %w", err)
	}
	dir := orderDirection(descending)
	query := `SELECT matched.hash, matched.block_hash, matched.slot,
       matched.block_index, matched.id FROM ` + source +
		` ORDER BY matched.slot ` + dir +
		`, COALESCE(matched.block_index, 0) ` + dir +
		`, matched.id ` + dir
	query, args = addLimitOffset(query, args, limit, offset)
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("get asset transactions: %w", err)
	}
	defer rows.Close()
	ret := []models.Transaction{}
	for rows.Next() {
		var (
			tx         models.Transaction
			slot       sql.NullInt64
			blockIndex sql.NullInt64
			id         int64
		)
		if err := rows.Scan(
			&tx.Hash, &tx.BlockHash, &slot, &blockIndex, &id,
		); err != nil {
			return nil, fmt.Errorf("scan asset transaction: %w", err)
		}
		tx.ID = uint(id) // #nosec G115 -- autoincrement ids are positive
		if slot.Valid {
			tx.Slot, err = checkedUint64(slot.Int64)
			if err != nil {
				return nil, fmt.Errorf("asset transaction slot: %w", err)
			}
		}
		if blockIndex.Valid {
			index, err := checkedUint64(blockIndex.Int64)
			if err != nil || index > uint64(^uint32(0)) {
				return nil, fmt.Errorf(
					"asset transaction block index %d out of range",
					blockIndex.Int64,
				)
			}
			tx.BlockIndex = uint32(index) // #nosec G115 -- range checked above
		}
		ret = append(ret, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get asset transactions: %w", err)
	}
	return ret, nil
}

// CountAssetTransactions implements metadata.MetadataStore.
func (s *Store) CountAssetTransactions(
	policyID lcommon.Blake2b224,
	assetName []byte,
	from *models.AddressTransactionPosition,
	to *models.AddressTransactionPosition,
	txn types.Txn,
) (int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	source, args, err := assetTransactionsQuery(policyID, assetName, from, to)
	if err != nil {
		return 0, fmt.Errorf("asset transactions range: %w", err)
	}
	var count int
	if err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(`SELECT COUNT(*) FROM `+source),
		args...,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count asset transactions: %w", err)
	}
	return count, nil
}
