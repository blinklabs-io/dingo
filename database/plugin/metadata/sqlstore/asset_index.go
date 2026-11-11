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
	"context"
	"database/sql"
	"fmt"
	"strings"

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

// mintedAssetsQuery builds the asset listing statement with `?` placeholders.
// minted_asset holds one row per asset keyed by its first event id, so a page
// reads limit+offset index entries in order and never sorts.
func mintedAssetsQuery(
	policyID []byte,
	limit int,
	offset int,
	descending bool,
) (string, []any) {
	query := `SELECT policy_id, name FROM minted_asset`
	args := []any{}
	if policyID != nil {
		query += ` WHERE policy_id = ?`
		args = append(args, policyID)
	}
	query += ` ORDER BY first_event_id ` + orderDirection(descending)
	return addLimitOffset(query, args, limit, offset)
}

// mintedAssetsCountQuery builds the asset count statement with `?`
// placeholders.
func mintedAssetsCountQuery(policyID []byte) (string, []any) {
	query := `SELECT COUNT(*) FROM minted_asset`
	args := []any{}
	if policyID != nil {
		query += ` WHERE policy_id = ?`
		args = append(args, policyID)
	}
	return query, args
}

// indexMintedAssets records, for every asset among the just-inserted
// asset_mint_burn rows, its first event in minted_asset. rows are
// assetMintBurnShape rows. An asset that already has a row keeps it: later
// events carry larger ids, so its first event cannot change on insert.
// Events are located through the unique (tx_hash, policy_id, name) key, which
// stays in place during bulk import while the lookup index may be deferred.
func indexMintedAssets(
	ctx context.Context,
	db queryer,
	parameterLimit int,
	rows [][]any,
) error {
	const keyWidth = 3
	perStatement := max(1, parameterLimit/keyWidth)
	for start := 0; start < len(rows); start += perStatement {
		end := min(start+perStatement, len(rows))
		args := make([]any, 0, (end-start)*keyWidth)
		for _, row := range rows[start:end] {
			// assetMintBurnShape columns: tx_hash, policy_id, name, ...
			args = append(args, row[0], row[1], row[2])
		}
		query := `-- name: IndexMintedAssets :exec
INSERT INTO minted_asset (policy_id, name, first_event_id)
SELECT e.policy_id, e.name, MIN(e.id)
FROM asset_mint_burn AS e
WHERE (e.tx_hash, e.policy_id, e.name) IN (` +
			strings.TrimSuffix(
				strings.Repeat("(?, ?, ?), ", end-start), ", ",
			) + `)
  AND NOT EXISTS (
      SELECT 1 FROM minted_asset AS m
      WHERE m.policy_id = e.policy_id AND m.name = e.name
  )
GROUP BY e.policy_id, e.name`
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("index minted assets: %w", err)
		}
	}
	return nil
}

// rollbackMintedAssets removes the effect of asset_mint_burn rows past slot
// on minted_asset. It must run before those rows are deleted, because it
// finds the assets whose first event is going away. An asset left with no
// event disappears; one that keeps older events takes the smallest remaining
// id as its first event.
func rollbackMintedAssets(
	ctx context.Context,
	db queryer,
	slot uint64,
) error {
	if _, err := db.ExecContext(ctx, `-- name: DeleteRolledBackMintedAssets :exec
DELETE FROM minted_asset
WHERE first_event_id IN (SELECT id FROM asset_mint_burn WHERE slot > ?)
  AND NOT EXISTS (
      SELECT 1 FROM asset_mint_burn AS e
      WHERE e.policy_id = minted_asset.policy_id
        AND e.name = minted_asset.name
        AND e.slot <= ?
  )`, slot, slot); err != nil {
		return fmt.Errorf("delete rolled back minted assets: %w", err)
	}
	if _, err := db.ExecContext(ctx, `-- name: RepairRolledBackMintedAssets :exec
UPDATE minted_asset
SET first_event_id = (
    SELECT MIN(e.id) FROM asset_mint_burn AS e
    WHERE e.policy_id = minted_asset.policy_id
      AND e.name = minted_asset.name
      AND e.slot <= ?
)
WHERE first_event_id IN (SELECT id FROM asset_mint_burn WHERE slot > ?)`,
		slot, slot); err != nil {
		return fmt.Errorf("repair rolled back minted assets: %w", err)
	}
	return nil
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
	query, args := mintedAssetsQuery(policyID, limit, offset, descending)
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
	query, args := mintedAssetsCountQuery(policyID)
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
