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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
)

// An asset is stored once in asset_identity and each output's quantity of it
// in asset_holding. Identity ids are node-local surrogates, and an identity is
// never deleted: a rollback or prune removes holdings only, so every read
// joins holdings and an identity with none is invisible.
//
// No identity id is cached across statements. The store has no hook for a
// rolled-back transaction or savepoint, so a cached id could name an identity
// that was never committed.

// insertAssetHoldingQuery stores one holding when the asset's identity
// already exists. It affects no row when the identity is missing or the
// holding is already present. The cast types utxo_id for PostgreSQL, which
// otherwise infers text for a bare bind parameter in a SELECT list.
const insertAssetHoldingQuery = `-- name: InsertAssetHolding :exec
INSERT INTO asset_holding (utxo_id, asset_id, amount)
SELECT CAST(? AS BIGINT), id, ? FROM asset_identity
WHERE policy_id = ? AND name = ?
ON CONFLICT (utxo_id, asset_id) DO NOTHING`

const insertAssetIdentityQuery = `-- name: InsertAssetIdentity :exec
INSERT INTO asset_identity (policy_id, name) VALUES (?, ?)
ON CONFLICT (policy_id, name) DO NOTHING`

// insertAssetHolding records that utxoID holds asset. A nil name is stored as
// the empty name, the same asset a zero-length name means.
func (s *Store) insertAssetHolding(
	ctx context.Context,
	db queryer,
	utxoID int64,
	asset *models.Asset,
) error {
	policy := asset.PolicyId
	name := nonNilName(asset.Name)
	amount := validString(decimalUint64(asset.Amount))
	// The first statement writes nothing when the identity is new, and
	// again when the holding already exists, so the retry after creating the
	// identity may also write nothing.
	for attempt := range 2 {
		result, err := s.execCached(ctx, db, insertAssetHoldingQuery,
			utxoID, amount, policy, name,
		)
		if err != nil {
			return fmt.Errorf("insert asset holding: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("insert asset holding: %w", err)
		}
		if affected > 0 || attempt == 1 {
			return nil
		}
		if _, err := s.execCached(ctx, db, insertAssetIdentityQuery,
			policy, name,
		); err != nil {
			return fmt.Errorf("insert asset identity: %w", err)
		}
	}
	return nil
}

// assetHoldingShape queues holdings for a batched write. Rows carry the
// asset's (policy_id, name) rather than an identity id: the id is resolved
// when the batch flushes, against the identities that exist then.
var assetHoldingShape = rowShape{
	name:          "InsertAssetHoldingBatch",
	table:         "asset_holding",
	columns:       []string{"utxo_id", "policy_id", "name", "amount"},
	resolveAssets: true,
}

// queueAssetHolding queues one holding on rows.
func queueAssetHolding(
	rows *rowBatch,
	utxoID int64,
	asset *models.Asset,
) {
	rows.add(
		assetHoldingShape,
		utxoID,
		asset.PolicyId,
		nonNilName(asset.Name),
		validString(decimalUint64(asset.Amount)),
	)
}

const (
	holdingRowUtxo = iota
	holdingRowPolicy
	holdingRowName
	holdingRowAmount
)

const (
	mintBurnRowTransaction = iota
	mintBurnRowPolicy
	mintBurnRowName
	mintBurnRowSlot
	mintBurnRowQuantity
	mintBurnRowTxIndex
)

type assetIdentityKey struct {
	policy string
	name   string
}

// ensureAssetIdentities returns the id of every asset in keys, creating the
// identities that are missing. It resolves the distinct assets with chunked
// selects, inserts the missing ones in key order and resolves those again.
func ensureAssetIdentities(
	ctx context.Context,
	db queryer,
	all []assetIdentityKey,
	parameterLimit int,
) (map[assetIdentityKey]int64, error) {
	seen := make(map[assetIdentityKey]struct{}, len(all))
	keys := make([]assetIdentityKey, 0, len(all))
	for _, key := range all {
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	ids := make(map[assetIdentityKey]int64, len(keys))
	if err := resolveAssetIdentities(
		ctx, db, keys, ids, parameterLimit, false,
	); err != nil {
		return nil, err
	}
	var missing []assetIdentityKey
	for _, key := range keys {
		if _, ok := ids[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		// Writers that insert overlapping identities must take the unique
		// key's locks in one order, or two of them can wait on each other.
		slices.SortFunc(missing, func(a, b assetIdentityKey) int {
			return cmp.Or(
				strings.Compare(a.policy, b.policy),
				strings.Compare(a.name, b.name),
			)
		})
		if err := insertAssetIdentities(
			ctx, db, missing, parameterLimit,
		); err != nil {
			return nil, err
		}
		// A concurrent writer may have created an identity this call's
		// insert skipped. On the engines with snapshot reads that row is
		// invisible to a plain select, so the second resolve locks.
		wrapped, wrappedOK := unwrapDialectQueryer(db)
		lock := wrappedOK && wrapped.dialect != "sqlite"
		if err := resolveAssetIdentities(
			ctx, db, missing, ids, parameterLimit, lock,
		); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// flushAssetMintBurn writes queued mint and burn events keyed by transaction
// and asset id, in key order so they append to the primary key. A transaction
// that names an asset twice keeps its first event, as the unique key did.
func flushAssetMintBurn(
	ctx context.Context,
	db queryer,
	rows [][]any,
	parameterLimit int,
) error {
	if len(rows) == 0 {
		return nil
	}
	keys := make([]assetIdentityKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, assetIdentityKey{
			policy: string(
				row[mintBurnRowPolicy].([]byte),
			), //nolint:forcetypeassert
			name: string(
				row[mintBurnRowName].([]byte),
			), //nolint:forcetypeassert
		})
	}
	ids, err := ensureAssetIdentities(ctx, db, keys, parameterLimit)
	if err != nil {
		return err
	}
	type event struct {
		transaction int64
		asset       int64
		row         []any
	}
	events := make([]event, len(rows))
	for i, row := range rows {
		id, ok := ids[keys[i]]
		if !ok {
			return fmt.Errorf(
				"resolve asset identity %x/%x: not found after insert",
				keys[i].policy, keys[i].name,
			)
		}
		transaction, _ := row[mintBurnRowTransaction].(int64)
		events[i] = event{transaction: transaction, asset: id, row: row}
	}
	slices.SortStableFunc(events, func(a, b event) int {
		return cmp.Or(
			cmp.Compare(a.transaction, b.transaction),
			cmp.Compare(a.asset, b.asset),
		)
	})
	const width = 5
	perStatement := max(1, parameterLimit/width)
	for start := 0; start < len(events); start += perStatement {
		end := min(start+perStatement, len(events))
		args := make([]any, 0, (end-start)*width)
		for _, item := range events[start:end] {
			args = append(args,
				item.transaction, item.asset,
				item.row[mintBurnRowSlot],
				item.row[mintBurnRowTxIndex],
				item.row[mintBurnRowQuantity],
			)
		}
		query := "-- name: InsertAssetMintBurnBatch :exec\n" +
			"INSERT INTO asset_mint_burn " +
			"(transaction_id, asset_id, slot, tx_index, quantity) VALUES " +
			strings.TrimSuffix(
				strings.Repeat("(?, ?, ?, ?, ?), ", end-start), ", ",
			) +
			"\nON CONFLICT (transaction_id, asset_id) DO NOTHING"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert asset_mint_burn rows: %w", err)
		}
	}
	return nil
}

// flushAssetHoldings writes queued holdings. It resolves the distinct assets
// with chunked selects, inserts the identities that are missing, resolves
// those again, then inserts the holdings in (utxo_id, asset_id) order so they
// append to the primary key.
func flushAssetHoldings(
	ctx context.Context,
	db queryer,
	rows [][]any,
	parameterLimit int,
) error {
	if len(rows) == 0 {
		return nil
	}
	keys := make([]assetIdentityKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, assetIdentityKey{
			policy: string(
				row[holdingRowPolicy].([]byte),
			), //nolint:forcetypeassert
			name: string(
				row[holdingRowName].([]byte),
			), //nolint:forcetypeassert
		})
	}
	ids, err := ensureAssetIdentities(ctx, db, keys, parameterLimit)
	if err != nil {
		return err
	}
	type holding struct {
		utxo   any
		utxoID int64
		asset  int64
		amount any
	}
	holdings := make([]holding, 0, len(rows))
	for _, row := range rows {
		key := assetIdentityKey{
			policy: string(
				row[holdingRowPolicy].([]byte),
			), //nolint:forcetypeassert
			name: string(
				row[holdingRowName].([]byte),
			), //nolint:forcetypeassert
		}
		id, ok := ids[key]
		if !ok {
			return fmt.Errorf(
				"resolve asset identity %x/%x: not found after insert",
				key.policy, key.name,
			)
		}
		utxoID, _ := row[holdingRowUtxo].(int64)
		holdings = append(holdings, holding{
			utxo:   row[holdingRowUtxo],
			utxoID: utxoID,
			asset:  id,
			amount: row[holdingRowAmount],
		})
	}
	slices.SortStableFunc(holdings, func(a, b holding) int {
		if a.utxoID != b.utxoID {
			if a.utxoID < b.utxoID {
				return -1
			}
			return 1
		}
		switch {
		case a.asset < b.asset:
			return -1
		case a.asset > b.asset:
			return 1
		}
		return 0
	})
	const width = 3
	perStatement := max(1, parameterLimit/width)
	for start := 0; start < len(holdings); start += perStatement {
		end := min(start+perStatement, len(holdings))
		args := make([]any, 0, (end-start)*width)
		for _, item := range holdings[start:end] {
			args = append(args, item.utxo, item.asset, item.amount)
		}
		query := "-- name: InsertAssetHoldingBatch :exec\n" +
			"INSERT INTO asset_holding (utxo_id, asset_id, amount) VALUES " +
			strings.TrimSuffix(
				strings.Repeat("(?, ?, ?), ", end-start), ", ",
			) +
			"\nON CONFLICT (utxo_id, asset_id) DO NOTHING"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert asset_holding rows: %w", err)
		}
	}
	return nil
}

// resolveAssetIdentities adds the id of each key that exists to ids. With lock
// set the select reads the latest committed identities, not the transaction's
// snapshot.
func resolveAssetIdentities(
	ctx context.Context,
	db queryer,
	keys []assetIdentityKey,
	ids map[assetIdentityKey]int64,
	parameterLimit int,
	lock bool,
) error {
	perStatement := max(1, parameterLimit/2)
	for start := 0; start < len(keys); start += perStatement {
		end := min(start+perStatement, len(keys))
		args := make([]any, 0, (end-start)*2)
		for _, key := range keys[start:end] {
			args = append(args, []byte(key.policy), []byte(key.name))
		}
		query := "-- name: ResolveAssetIdentityBatch :many\n" +
			"SELECT id, policy_id, name FROM asset_identity WHERE " +
			strings.TrimSuffix(
				strings.Repeat("(policy_id = ? AND name = ?) OR ", end-start),
				" OR ",
			)
		if lock {
			query += "\nFOR SHARE"
		}
		if err := func() error {
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("resolve asset identities: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var (
					id           int64
					policy, name []byte
				)
				if err := rows.Scan(&id, &policy, &name); err != nil {
					return fmt.Errorf("scan asset identity: %w", err)
				}
				ids[assetIdentityKey{
					policy: string(policy), name: string(name),
				}] = id
			}
			return rows.Err()
		}(); err != nil {
			return err
		}
	}
	return nil
}

func insertAssetIdentities(
	ctx context.Context,
	db queryer,
	keys []assetIdentityKey,
	parameterLimit int,
) error {
	perStatement := max(1, parameterLimit/2)
	for start := 0; start < len(keys); start += perStatement {
		end := min(start+perStatement, len(keys))
		args := make([]any, 0, (end-start)*2)
		for _, key := range keys[start:end] {
			args = append(args, []byte(key.policy), []byte(key.name))
		}
		query := "-- name: InsertAssetIdentityBatch :exec\n" +
			"INSERT INTO asset_identity (policy_id, name) VALUES " +
			strings.TrimSuffix(strings.Repeat("(?, ?), ", end-start), ", ") +
			"\nON CONFLICT (policy_id, name) DO NOTHING"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert asset_identity rows: %w", err)
		}
	}
	return nil
}
