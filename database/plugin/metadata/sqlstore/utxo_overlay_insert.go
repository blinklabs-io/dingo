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

//nolint:gosec // SQL INTEGER mappings preserve the unsigned domain API.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

const maxSQLiteOverlayInsertRows = 2047

const sqliteOverlayInsertGuard = `-- name: SQLiteOverlayInsertGuard :one
SELECT EXISTS (
 SELECT 1 FROM main.sqlite_schema
 WHERE type = 'trigger' AND tbl_name = 'utxo' COLLATE NOCASE
) OR EXISTS (
 SELECT 1 FROM temp.sqlite_schema
 WHERE (type = 'trigger' AND tbl_name = 'utxo' COLLATE NOCASE)
    OR (type IN ('table', 'view') AND name = 'utxo' COLLATE NOCASE)
)`

func (s *Store) overlayInsertRows(
	ctx context.Context,
	db queryer,
	count int,
) (int, error) {
	legacy := max(1, s.dialect.ParameterLimit()/overlayUtxoInsertColumns)
	preferred := min(s.sqliteOverlayInsertRows, count)
	if s.dialect.Name() != "sqlite" || preferred <= legacy ||
		overlayInsertTransaction(db) == nil {
		return legacy, nil
	}
	// Triggers can observe statement boundaries through changes(). Recheck
	// both schemas for every flush, including DDL within this transaction.
	guarded, err := sqliteOverlayInsertHasTriggers(ctx, db)
	if err != nil {
		return overlayInsertFallback(ctx, legacy, err)
	}
	if guarded {
		return legacy, nil
	}
	// The provider certifies eager preparation. This checks the current
	// connection's runtime limit without binding thousands of dummy values.
	query := "-- name: SQLiteOverlayInsertLimit :one\nSELECT ?" +
		strconv.Itoa(preferred*overlayUtxoInsertColumns)
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return overlayInsertFallback(ctx, legacy, err)
	}
	if stmt == nil {
		return legacy, nil
	}
	defer stmt.Close()
	if err := stmt.Close(); err != nil {
		return overlayInsertFallback(ctx, legacy, err)
	}
	if err := ctx.Err(); err != nil {
		return legacy, err
	}
	return preferred, nil
}

func overlayInsertFallback(
	ctx context.Context,
	legacy int,
	err error,
) (int, error) {
	if contextErr := ctx.Err(); contextErr != nil {
		return legacy, contextErr
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return legacy, err
	}
	return legacy, nil
}

func sqliteOverlayInsertHasTriggers(
	ctx context.Context,
	db queryer,
) (bool, error) {
	rows, err := db.QueryContext(ctx, sqliteOverlayInsertGuard)
	if err != nil {
		return true, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return true, err
		}
		return true, sql.ErrNoRows
	}
	var guarded bool
	if err := rows.Scan(&guarded); err != nil {
		return true, err
	}
	if rows.Next() {
		return true, errors.New("overlay insert guard returned multiple rows")
	}
	if err := rows.Err(); err != nil {
		return true, err
	}
	if err := rows.Close(); err != nil {
		return true, err
	}
	return guarded, nil
}

// A pool can hand preparation and insertion different connections; only
// known transaction wrappers establish that the capability check applies.
func overlayInsertTransaction(db queryer) *sql.Tx {
	switch value := db.(type) {
	case *sql.Tx:
		return value
	case countingQueryer:
		return overlayInsertTransaction(value.queryer)
	case dialectQueryer:
		return overlayInsertTransaction(value.queryer)
	case barrierQueryer:
		return overlayInsertTransaction(value.queryer)
	default:
		return nil
	}
}
