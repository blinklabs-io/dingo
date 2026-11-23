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
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
)

// dictionary is a table that holds each distinct value of a key column once
// and names it by an integer id. A dictionary row is never deleted, and ids
// are resolved inside the caller's transaction and never cached across it, so
// a rollback cannot leave a stale id behind.
type dictionary struct {
	// name labels the dictionary's statements; table and column name the
	// dictionary table and its unique key column.
	name, table, column string
	// insertColumns and insertRow describe the row written for a missing key.
	// The key is the row's first argument, followed by extraArgs.
	insertColumns string
	insertRow     string
	extraArgs     []any
}

var (
	addressDictionary = dictionary{
		name: "Address", table: "address", column: "address",
		insertColumns: "address", insertRow: "(?)",
	}
	credentialDictionary = dictionary{
		name: "Credential", table: "credential", column: "hash",
		insertColumns: "hash", insertRow: "(?)",
	}
	// datumDictionary adds an unrevealed datum row for a hash no transaction
	// has revealed yet: an empty raw_datum, which no revealed datum has. A
	// later reveal fills the row in (see datumShape).
	datumDictionary = dictionary{
		name: "Datum", table: "datum", column: "hash",
		insertColumns: "hash, raw_datum, added_slot", insertRow: "(?, ?, 0)",
		extraArgs: []any{[]byte{}},
	}
)

// ensureAddresses returns the dictionary id of every address in all, adding
// the addresses that are missing.
func ensureAddresses(
	ctx context.Context,
	db queryer,
	all [][]byte,
	parameterLimit int,
) (map[string]int64, error) {
	return addressDictionary.ensure(ctx, db, all, parameterLimit)
}

// ensure returns the id of every key in all, adding the keys that are
// missing. It resolves the distinct keys with chunked selects, inserts the
// missing ones in key order and resolves those again.
func (d dictionary) ensure(
	ctx context.Context,
	db queryer,
	all [][]byte,
	parameterLimit int,
) (map[string]int64, error) {
	seen := make(map[string]struct{}, len(all))
	keys := make([][]byte, 0, len(all))
	for _, key := range all {
		if len(key) == 0 {
			continue
		}
		if _, ok := seen[string(key)]; !ok {
			seen[string(key)] = struct{}{}
			keys = append(keys, key)
		}
	}
	ids := make(map[string]int64, len(keys))
	if err := d.resolve(ctx, db, keys, ids, parameterLimit, false); err != nil {
		return nil, err
	}
	var missing [][]byte
	for _, key := range keys {
		if _, ok := ids[string(key)]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return ids, nil
	}
	// Writers that insert overlapping keys must take the unique key's locks
	// in one order, or two of them can wait on each other.
	slices.SortFunc(missing, bytes.Compare)
	if err := d.insert(ctx, db, missing, parameterLimit); err != nil {
		return nil, err
	}
	// A concurrent writer may have created a key this call's insert skipped.
	// On the engines with snapshot reads that row is invisible to a plain
	// select, so the second resolve locks.
	wrapped, wrappedOK := unwrapDialectQueryer(db)
	lock := wrappedOK && wrapped.dialect != "sqlite"
	if err := d.resolve(
		ctx, db, missing, ids, parameterLimit, lock,
	); err != nil {
		return nil, err
	}
	return ids, nil
}

func (d dictionary) resolve(
	ctx context.Context,
	db queryer,
	keys [][]byte,
	ids map[string]int64,
	parameterLimit int,
	lock bool,
) error {
	perStatement := max(1, parameterLimit)
	for start := 0; start < len(keys); start += perStatement {
		end := min(start+perStatement, len(keys))
		args := make([]any, 0, end-start)
		for _, key := range keys[start:end] {
			args = append(args, key)
		}
		query := "-- name: Resolve" + d.name + "Batch :many\n" +
			"SELECT id, " + d.column + " FROM " + d.table + " WHERE " +
			d.column + " IN (" +
			strings.TrimSuffix(strings.Repeat("?, ", end-start), ", ") + ")"
		if lock {
			query += "\nFOR SHARE"
		}
		if err := func() error {
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("resolve %s rows: %w", d.table, err)
			}
			defer rows.Close()
			for rows.Next() {
				var (
					id  int64
					key []byte
				)
				if err := rows.Scan(&id, &key); err != nil {
					return fmt.Errorf("scan %s row: %w", d.table, err)
				}
				ids[string(key)] = id
			}
			return rows.Err()
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (d dictionary) insert(
	ctx context.Context,
	db queryer,
	keys [][]byte,
	parameterLimit int,
) error {
	width := 1 + len(d.extraArgs)
	perStatement := max(1, parameterLimit/width)
	for start := 0; start < len(keys); start += perStatement {
		end := min(start+perStatement, len(keys))
		args := make([]any, 0, (end-start)*width)
		for _, key := range keys[start:end] {
			args = append(args, key)
			args = append(args, d.extraArgs...)
		}
		query := "-- name: Insert" + d.name + "Batch :exec\n" +
			"INSERT INTO " + d.table + " (" + d.insertColumns + ") VALUES " +
			strings.TrimSuffix(
				strings.Repeat(d.insertRow+", ", end-start),
				", ",
			) +
			"\nON CONFLICT (" + d.column + ") DO NOTHING"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert %s rows: %w", d.table, err)
		}
	}
	return nil
}

// resolveAddressRows returns rows with the address in column 1 replaced by its
// dictionary id, adding the addresses that are missing. The queued rows are
// left as they are.
func resolveAddressRows(
	ctx context.Context,
	db queryer,
	rows [][]any,
	parameterLimit int,
) ([][]any, error) {
	addresses := make([][]byte, 0, len(rows))
	for _, row := range rows {
		address, _ := row[1].([]byte)
		addresses = append(addresses, address)
	}
	ids, err := ensureAddresses(ctx, db, addresses, parameterLimit)
	if err != nil {
		return nil, err
	}
	resolved := make([][]any, len(rows))
	for i, row := range rows {
		out := slices.Clone(row)
		out[1] = ids[string(addresses[i])]
		resolved[i] = out
	}
	return resolved, nil
}

// resolveCredentialRows returns address-transaction rows with the payment
// credential hash in column 0 and the stake credential hash in column 2
// replaced by their dictionary ids, adding the credentials that are missing.
// A row with no credential in a column holds NULL there. The queued rows are
// left as they are.
func resolveCredentialRows(
	ctx context.Context,
	db queryer,
	rows [][]any,
	parameterLimit int,
) ([][]any, error) {
	hashes := make([][]byte, 0, 2*len(rows))
	for _, row := range rows {
		payment, _ := row[0].([]byte)
		staking, _ := row[2].([]byte)
		hashes = append(hashes, payment, staking)
	}
	ids, err := credentialDictionary.ensure(ctx, db, hashes, parameterLimit)
	if err != nil {
		return nil, err
	}
	idOf := func(value any) any {
		hash, _ := value.([]byte)
		if id, ok := ids[string(hash)]; ok && len(hash) > 0 {
			return id
		}
		return nil
	}
	resolved := make([][]any, len(rows))
	for i, row := range rows {
		out := slices.Clone(row)
		out[0], out[2] = idOf(row[0]), idOf(row[2])
		resolved[i] = out
	}
	return resolved, nil
}
