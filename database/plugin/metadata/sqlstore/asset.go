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

//nolint:gosec // SQL INTEGER mappings preserve the existing unsigned domain API.
package sqlstore

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const assetHolderGroups = `
FROM asset AS a
INNER JOIN utxo AS u ON u.id = a.utxo_id
INNER JOIN utxo_address AS ua ON ua.utxo_id = u.id
WHERE a.policy_id = ? AND a.name = ? AND u.deleted_slot = 0
GROUP BY ua.address`

func (s *Store) GetAssetHolders(
	query models.AssetHolderQuery,
	txn types.Txn,
) ([]models.AssetHolder, int, error) {
	if query.Limit <= 0 || query.Offset < 0 {
		return nil, 0, errors.New("asset holder page bounds must be positive")
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	args := []any{query.PolicyID, query.AssetName}
	var missing int
	err = db.QueryRowContext(ctx, s.dialect.Rebind(`
SELECT 1 FROM asset AS a
INNER JOIN utxo AS u ON u.id = a.utxo_id
LEFT JOIN utxo_address AS ua ON ua.utxo_id = u.id
WHERE a.policy_id = ? AND a.name = ? AND u.deleted_slot = 0
  AND ua.utxo_id IS NULL
LIMIT 1`), args...).Scan(&missing)
	if err == nil {
		return nil, 0, errors.New("live UTxO address is unavailable")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, 0, fmt.Errorf("check asset holder addresses: %w", err)
	}
	var total int
	if err := db.QueryRowContext(ctx, s.dialect.Rebind(
		`SELECT COUNT(*) FROM (SELECT 1`+assetHolderGroups+`) AS holders`,
	), args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count asset holders: %w", err)
	}
	if total == 0 || query.Offset >= total {
		return nil, total, nil
	}
	direction := "ASC"
	if query.Descending {
		direction = "DESC"
	}
	sumExpression := "SUM(CAST(a.amount AS DECIMAL(20, 0)))"
	if s.dialect.Name() == "sqlite" {
		sumExpression = "dingo_uint64_sum(a.amount)"
	} else {
		overflowSQL := `SELECT 1` + assetHolderGroups +
			` HAVING ` + sumExpression +
			` > CAST('18446744073709551615' AS DECIMAL(20, 0)) LIMIT 1`
		var overflow int
		err := db.QueryRowContext(
			ctx,
			s.dialect.Rebind(overflowSQL),
			args...,
		).Scan(&overflow)
		if err == nil {
			return nil, 0, errors.New("asset holder quantity overflow")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, 0, fmt.Errorf("check asset holder overflow: %w", err)
		}
	}
	pageSQL := `SELECT ua.address, ` + sumExpression + ` AS quantity` +
		assetHolderGroups + ` ORDER BY quantity ` + direction +
		`, ua.address ` + direction + ` LIMIT ? OFFSET ?`
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(pageSQL),
		query.PolicyID,
		query.AssetName,
		query.Limit,
		query.Offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query asset holders: %w", err)
	}
	defer rows.Close()
	ret := make([]models.AssetHolder, 0, query.Limit)
	for rows.Next() {
		var holder models.AssetHolder
		var quantity string
		if err := rows.Scan(&holder.Address, &quantity); err != nil {
			return nil, 0, fmt.Errorf("scan asset holder: %w", err)
		}
		holder.Quantity, err = parseUint64("asset holder quantity", quantity)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, holder)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return ret, total, nil
}

func (s *Store) GetAssetByPolicyAndName(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (models.Asset, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return models.Asset{}, err
	}
	q := s.operationalQueries(db)
	row, err := q.GetAssetByPolicyAndName(
		ctx,
		sqlitequery.GetAssetByPolicyAndNameParams{
			PolicyID: policyID[:],
			Name:     assetName,
		},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return models.Asset{}, nil
	}
	if err != nil {
		return models.Asset{}, err
	}
	amount := uint64(0)
	if row.Amount.Valid {
		amount, err = parseUint64("asset amount", row.Amount.String)
		if err != nil {
			return models.Asset{}, err
		}
	}
	return models.Asset{
		Name:        row.Name,
		PolicyId:    row.PolicyID,
		Fingerprint: row.Fingerprint,
		ID:          uint(row.ID),
		UtxoID:      uint(row.UtxoID.Int64),
		Amount:      types.Uint64(amount),
	}, nil
}

// GetAssetQuantityByPolicyAndName sums the live holdings of an asset in the
// database, so one row crosses the driver however many UTxOs hold it. It reads
// at most models.MaxAssetQuantityHoldings holdings and fails with
// models.ErrUtxoQueryBudgetExceeded for an asset held by more, so one request
// cannot make the database scan without limit. The scan runs under the
// transaction's context, so it stops when the caller's request does.
func (s *Store) GetAssetQuantityByPolicyAndName(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (uint64, error) {
	return s.assetQuantity(
		policyID,
		assetName,
		models.MaxAssetQuantityHoldings,
		txn,
	)
}

func (s *Store) assetQuantity(
	policyID lcommon.Blake2b224,
	assetName []byte,
	maxHoldings int,
	txn types.Txn,
) (uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	sumExpression := "SUM(CAST(amount AS DECIMAL(20, 0)))"
	if s.dialect.Name() == "sqlite" {
		sumExpression = "dingo_uint64_sum(amount)"
	}
	// The inner LIMIT is what bounds the scan: one row past the cap shows
	// the asset has more holdings than are allowed to be read.
	var (
		count int
		raw   sql.NullString
	)
	err = db.QueryRowContext(ctx, s.dialect.Rebind(`
SELECT COUNT(*), `+sumExpression+`
FROM (
  SELECT asset.amount AS amount
  FROM asset
  INNER JOIN utxo ON asset.utxo_id = utxo.id
  WHERE asset.policy_id = ? AND asset.name = ? AND utxo.deleted_slot = 0
    AND asset.amount IS NOT NULL
  LIMIT ?
) AS holdings`),
		policyID[:], assetName, maxHoldings+1,
	).Scan(&count, &raw)
	if err != nil {
		// The sqlite aggregate refuses a sum beyond 64 bits itself.
		return 0, err
	}
	if count > maxHoldings {
		return 0, models.ErrUtxoQueryBudgetExceeded
	}
	if !raw.Valid {
		return 0, nil
	}
	total, err := strconv.ParseUint(raw.String, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, errors.New("asset quantity overflow")
		}
		return 0, fmt.Errorf("decode asset quantity: %w", err)
	}
	return total, nil
}

func (s *Store) GetAssetMintBurnInfo(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) ([]byte, int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, 0, err
	}
	q := s.operationalQueries(db)
	row, err := q.GetAssetMintBurnInfo(
		ctx,
		sqlitequery.GetAssetMintBurnInfoParams{
			PolicyID:   policyID[:],
			Name:       assetName,
			PolicyID_2: policyID[:],
			Name_2:     assetName,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	return row.InitialTxHash, int(row.EventCount), nil
}
