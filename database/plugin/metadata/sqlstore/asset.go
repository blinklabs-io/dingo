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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// liveAssetUtxoPredicate selects the live outputs among an asset's holdings.
// On SQLite the unary plus keeps the liveness index out of the plan: it
// matches nearly every row, yet statistics can rank it above the asset's
// holdings, and driving from it probes the holdings once per live output.
func liveAssetUtxoPredicate(dialect, utxoAlias string) string {
	if dialect == "sqlite" {
		return "+" + utxoAlias + ".deleted_slot = 0"
	}
	return utxoAlias + ".deleted_slot = 0"
}

func assetHolderGroups(dialect string) string {
	return `
FROM asset_identity AS i
INNER JOIN asset_holding AS a ON a.asset_id = i.id
INNER JOIN utxo AS u ON u.id = a.utxo_id
INNER JOIN utxo_address AS ua ON ua.utxo_id = u.id
WHERE i.policy_id = ? AND i.name = ? AND ` +
		liveAssetUtxoPredicate(dialect, "u") + `
GROUP BY ua.address`
}

func assetHolderAddressCheckQuery(dialect string) string {
	return `
SELECT 1 FROM asset_identity AS i
INNER JOIN asset_holding AS a ON a.asset_id = i.id
INNER JOIN utxo AS u ON u.id = a.utxo_id
LEFT JOIN utxo_address AS ua ON ua.utxo_id = u.id
WHERE i.policy_id = ? AND i.name = ? AND ` +
		liveAssetUtxoPredicate(dialect, "u") + `
  AND ua.utxo_id IS NULL
LIMIT 1`
}

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
	args := []any{query.PolicyID, nonNilName(query.AssetName)}
	var missing int
	dialect := s.dialect.Name()
	holderGroups := assetHolderGroups(dialect)
	err = db.QueryRowContext(ctx, s.dialect.Rebind(
		assetHolderAddressCheckQuery(dialect),
	), args...).Scan(&missing)
	if err == nil {
		return nil, 0, errors.New("live UTxO address is unavailable")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, 0, fmt.Errorf("check asset holder addresses: %w", err)
	}
	var total int
	if err := db.QueryRowContext(ctx, s.dialect.Rebind(
		`SELECT COUNT(*) FROM (SELECT 1`+holderGroups+`) AS holders`,
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
		overflowSQL := `SELECT 1` + holderGroups +
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
		holderGroups + ` ORDER BY quantity ` + direction +
		`, ua.address ` + direction + ` LIMIT ? OFFSET ?`
	rows, err := db.QueryContext(
		ctx,
		s.dialect.Rebind(pageSQL),
		query.PolicyID,
		nonNilName(query.AssetName),
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

// AssetHeld reports whether any retained holding of the asset exists, live or
// spent and not yet pruned. An identity whose holdings are all gone does not
// count: the identity row outlives its holdings, so the answer comes from the
// holdings.
func (s *Store) AssetHeld(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, err
	}
	return s.operationalQueries(db).AssetHeld(
		ctx,
		sqlitequery.AssetHeldParams{
			PolicyID: policyID[:],
			Name:     nonNilName(assetName),
		},
	)
}

// assetQuantityQuery totals the live holdings of one asset with
// sumExpression, which reads h.amount.
func assetQuantityQuery(dialect, sumExpression string) string {
	return `
SELECT ` + sumExpression + `
FROM asset_identity AS i
INNER JOIN asset_holding AS h ON h.asset_id = i.id
INNER JOIN utxo ON h.utxo_id = utxo.id
WHERE i.policy_id = ? AND i.name = ? AND ` +
		liveAssetUtxoPredicate(dialect, "utxo") + `
  AND h.amount IS NOT NULL`
}

// assetQuantitySum is the dialect's aggregate over h.amount. The sqlite
// aggregate refuses a total past 64 bits itself; the others sum DECIMAL(20, 0)
// and parseAssetQuantity rejects a total past 64 bits.
func (s *Store) assetQuantitySum() string {
	if s.dialect.Name() == "sqlite" {
		return "dingo_uint64_sum(h.amount)"
	}
	return "SUM(CAST(h.amount AS DECIMAL(20, 0)))"
}

func parseAssetQuantity(raw sql.NullString) (uint64, error) {
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

// GetAssetQuantityByPolicyAndName sums the live holdings of an asset in the
// database, so one row crosses the driver however many UTxOs hold it. The scan
// runs under the transaction's context, so it stops when the caller's request
// does.
func (s *Store) GetAssetQuantityByPolicyAndName(
	policyID lcommon.Blake2b224,
	assetName []byte,
	txn types.Txn,
) (uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var raw sql.NullString
	err = db.QueryRowContext(ctx, s.dialect.Rebind(
		assetQuantityQuery(s.dialect.Name(), s.assetQuantitySum())),
		policyID[:], nonNilName(assetName),
	).Scan(&raw)
	if err != nil {
		return 0, err
	}
	return parseAssetQuantity(raw)
}

// GetAssetQuantitiesByPolicyAndName sums the live holdings of every asset in
// assets with one grouped statement per bind-parameter chunk, rather than one
// statement per asset. The result is aligned with assets; an asset with no
// live holding totals zero.
func (s *Store) GetAssetQuantitiesByPolicyAndName(
	assets []models.MintedAsset,
	txn types.Txn,
) ([]uint64, error) {
	ret := make([]uint64, len(assets))
	if len(assets) == 0 {
		return ret, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	// policy_id is a fixed 28 bytes, so policy || name is an unambiguous key.
	key := func(policyID, name []byte) string {
		return string(policyID) + string(name)
	}
	unique := make([]models.MintedAsset, 0, len(assets))
	seen := make(map[string]struct{}, len(assets))
	for _, asset := range assets {
		k := key(asset.PolicyId, asset.Name)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		unique = append(unique, asset)
	}
	// Each asset binds its policy ID and name.
	chunkSize := max(s.dialect.ParameterLimit()/2, 1)
	totals := make(map[string]uint64, len(unique))
	sum := s.assetQuantitySum()
	for start := 0; start < len(unique); start += chunkSize {
		chunk := unique[start:min(start+chunkSize, len(unique))]
		query, args := assetQuantitiesQuery(s.dialect.Name(), sum, chunk)
		if err := s.sumAssetQuantityChunk(
			ctx, db, query, args, totals, key,
		); err != nil {
			return nil, err
		}
	}
	for i, asset := range assets {
		ret[i] = totals[key(asset.PolicyId, asset.Name)]
	}
	return ret, nil
}

// assetQuantitiesQuery is the grouped quantity statement for assets, with
// its arguments, totalling each asset's live holdings with sumExpression.
// Grouping leads with h.asset_id: grouping by (policy_id, name) alone lets
// SQLite scan the asset_identity unique index for its order instead of
// searching it once per requested key.
func assetQuantitiesQuery(
	dialect, sumExpression string,
	assets []models.MintedAsset,
) (string, []any) {
	var where strings.Builder
	args := make([]any, 0, 2*len(assets))
	for i, asset := range assets {
		if i > 0 {
			where.WriteString(" OR ")
		}
		where.WriteString("(i.policy_id = ? AND i.name = ?)")
		args = append(args, asset.PolicyId, nonNilName(asset.Name))
	}
	return `
SELECT i.policy_id, i.name, ` + sumExpression + `
FROM asset_identity AS i
INNER JOIN asset_holding AS h ON h.asset_id = i.id
INNER JOIN utxo ON h.utxo_id = utxo.id
WHERE ` + liveAssetUtxoPredicate(dialect, "utxo") + ` AND h.amount IS NOT NULL
  AND (` + where.String() + `)
GROUP BY h.asset_id, i.policy_id, i.name`, args
}

// sumAssetQuantityChunk runs one grouped quantity statement and records each
// total in totals under key.
func (s *Store) sumAssetQuantityChunk(
	ctx context.Context,
	db queryer,
	query string,
	args []any,
	totals map[string]uint64,
	key func(policyID, name []byte) string,
) error {
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var policyID, name []byte
		var raw sql.NullString
		if err := rows.Scan(&policyID, &name, &raw); err != nil {
			return err
		}
		total, err := parseAssetQuantity(raw)
		if err != nil {
			return err
		}
		totals[key(policyID, name)] = total
	}
	return rows.Err()
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
