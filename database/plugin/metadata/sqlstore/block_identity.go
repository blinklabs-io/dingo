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
	"errors"
	"fmt"
	"strings"
)

// A block's hash is held once, in block_identity, and every transaction of the
// block refers to it by integer id. Dictionary rows are never deleted, like
// asset_identity: a row left by a rolled-back block is unreachable because
// every read starts from applied_transaction, and the same hash gets the same
// row if the block is applied again. Ids are therefore resolved against the
// database on each write and never remembered, so a rolled-back insert cannot
// leave a stale id behind.
const blockIdentityIDSQL = `-- name: GetBlockIdentityID :one
SELECT id FROM block_identity WHERE hash = ?`

const blockIdentityInsertSQL = `-- name: InsertBlockIdentity :exec
INSERT INTO block_identity (hash) VALUES (?)
ON CONFLICT (hash) DO NOTHING`

// appliedBlockHashSQL is the SQL expression for the hash of the block of the
// applied_transaction row aliased as alias. It is NULL for a transaction with
// no block.
func appliedBlockHashSQL(alias string) string {
	return "(SELECT bi.hash FROM block_identity AS bi WHERE bi.id = " +
		alias + ".block_id)"
}

// resolveBlockID returns the block_identity id of hash, inserting the row if
// absent, as the value to bind to applied_transaction.block_id. A transaction
// with no block hash binds NULL.
func resolveBlockID(
	queryRow func(string, ...any) *sql.Row,
	exec func(string, ...any) (sql.Result, error),
	hash any,
) (any, error) {
	value, ok := hash.([]byte)
	if !ok || len(value) == 0 {
		if hash == nil || ok {
			return nil, nil
		}
		return nil, fmt.Errorf("block hash has type %T, want []byte", hash)
	}
	var id int64
	err := queryRow(blockIdentityIDSQL, value).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err = exec(blockIdentityInsertSQL, value); err != nil {
			return nil, fmt.Errorf("insert block identity: %w", err)
		}
		err = queryRow(blockIdentityIDSQL, value).Scan(&id)
	}
	if err != nil {
		return nil, fmt.Errorf("resolve block identity: %w", err)
	}
	return id, nil
}

// blockIDSet maps the block hashes of one batch to their dictionary ids.
type blockIDSet map[string]int64

// id returns the value to bind to applied_transaction.block_id for hash.
func (set blockIDSet) id(hash any) (any, error) {
	value, ok := hash.([]byte)
	if !ok || len(value) == 0 {
		if hash == nil || ok {
			return nil, nil
		}
		return nil, fmt.Errorf("block hash has type %T, want []byte", hash)
	}
	id, ok := set[string(value)]
	if !ok || id == 0 {
		return nil, fmt.Errorf("block identity of %x was not resolved", value)
	}
	return id, nil
}

// resolveBlockIDs gives every distinct block hash of hashes a dictionary row
// and returns their ids, in two statements however many blocks the batch
// holds. Empty and nil hashes name no block.
func resolveBlockIDs(
	ctx context.Context,
	db queryer,
	hashes []any,
) (blockIDSet, error) {
	ids := make(blockIDSet)
	var distinct []any
	for _, hash := range hashes {
		value, ok := hash.([]byte)
		if !ok || len(value) == 0 {
			continue
		}
		if _, seen := ids[string(value)]; !seen {
			ids[string(value)] = 0
			distinct = append(distinct, value)
		}
	}
	if len(distinct) == 0 {
		return ids, nil
	}
	if err := execChunked(ctx, db, distinct, 1, func(rows int) string {
		return `-- name: BatchInsertBlockIdentities :exec
INSERT INTO block_identity (hash) VALUES ` + strings.TrimSuffix(
			strings.Repeat("(?), ", rows), ", ") + `
ON CONFLICT (hash) DO NOTHING`
	}); err != nil {
		return nil, fmt.Errorf("insert block identities: %w", err)
	}
	const perStatement = 999
	for start := 0; start < len(distinct); start += perStatement {
		chunk := distinct[start:min(start+perStatement, len(distinct))]
		if err := readBlockIdentityIDs(ctx, db, chunk, ids); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func readBlockIdentityIDs(
	ctx context.Context,
	db queryer,
	hashes []any,
	ids blockIDSet,
) error {
	rows, err := db.QueryContext(ctx, `-- name: BatchBlockIdentityIDs :many
SELECT id, hash FROM block_identity WHERE hash IN (`+
		strings.TrimSuffix(strings.Repeat("?, ", len(hashes)), ", ")+`)`,
		hashes...)
	if err != nil {
		return fmt.Errorf("read block identities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id   int64
			hash []byte
		)
		if err := rows.Scan(&id, &hash); err != nil {
			return err
		}
		ids[string(hash)] = id
	}
	return rows.Err()
}
