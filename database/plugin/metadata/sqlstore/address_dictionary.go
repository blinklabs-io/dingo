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

// ensureAddresses returns the dictionary id of every address in all, adding
// the addresses that are missing. It resolves the distinct addresses with
// chunked selects, inserts the missing ones in key order and resolves those
// again. Ids are resolved inside the caller's transaction and never cached
// across it, so a rollback cannot leave a stale id behind. Dictionary rows are
// never deleted.
func ensureAddresses(
	ctx context.Context,
	db queryer,
	all [][]byte,
	parameterLimit int,
) (map[string]int64, error) {
	seen := make(map[string]struct{}, len(all))
	keys := make([][]byte, 0, len(all))
	for _, address := range all {
		if _, ok := seen[string(address)]; !ok {
			seen[string(address)] = struct{}{}
			keys = append(keys, address)
		}
	}
	ids := make(map[string]int64, len(keys))
	if err := resolveAddresses(ctx, db, keys, ids, parameterLimit, false); err != nil {
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
	// Writers that insert overlapping addresses must take the unique key's
	// locks in one order, or two of them can wait on each other.
	slices.SortFunc(missing, bytes.Compare)
	if err := insertAddresses(ctx, db, missing, parameterLimit); err != nil {
		return nil, err
	}
	// A concurrent writer may have created an address this call's insert
	// skipped. On the engines with snapshot reads that row is invisible to a
	// plain select, so the second resolve locks.
	wrapped, wrappedOK := unwrapDialectQueryer(db)
	lock := wrappedOK && wrapped.dialect != "sqlite"
	if err := resolveAddresses(
		ctx, db, missing, ids, parameterLimit, lock,
	); err != nil {
		return nil, err
	}
	return ids, nil
}

func resolveAddresses(
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
		query := "-- name: ResolveAddressBatch :many\n" +
			"SELECT id, address FROM address WHERE address IN (" +
			strings.TrimSuffix(strings.Repeat("?, ", end-start), ", ") + ")"
		if lock {
			query += "\nFOR SHARE"
		}
		if err := func() error {
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("resolve addresses: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var (
					id      int64
					address []byte
				)
				if err := rows.Scan(&id, &address); err != nil {
					return fmt.Errorf("scan address: %w", err)
				}
				ids[string(address)] = id
			}
			return rows.Err()
		}(); err != nil {
			return err
		}
	}
	return nil
}

func insertAddresses(
	ctx context.Context,
	db queryer,
	keys [][]byte,
	parameterLimit int,
) error {
	perStatement := max(1, parameterLimit)
	for start := 0; start < len(keys); start += perStatement {
		end := min(start+perStatement, len(keys))
		args := make([]any, 0, end-start)
		for _, key := range keys[start:end] {
			args = append(args, key)
		}
		query := "-- name: InsertAddressBatch :exec\n" +
			"INSERT INTO address (address) VALUES " +
			strings.TrimSuffix(strings.Repeat("(?), ", end-start), ", ") +
			"\nON CONFLICT (address) DO NOTHING"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert address rows: %w", err)
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
