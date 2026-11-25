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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.FactRowStore = (*Store)(nil)

// factTable validates that t is a table the store knows as a fact table and
// returns the store's own definition, so a caller cannot make the store build
// SQL over a table or column it did not list.
func factTable(t metadata.FactTable) (metadata.FactTable, error) {
	candidates := append(
		slices.Clone(metadata.FactTables), metadata.AccountRewardCreditTable,
	)
	for _, c := range candidates {
		if c.Name == t.Name {
			// The reward journal is also read whole, withdrawals included,
			// by a caller that clears the filter.
			if c.Name == metadata.AccountRewardCreditTable.Name &&
				t.FilterColumn == "" {
				c.FilterColumn, c.FilterValue = "", nil
			}
			return c, nil
		}
	}
	return metadata.FactTable{}, fmt.Errorf(
		"sqlstore: %q is not a fact table", t.Name,
	)
}

func (s *Store) slotPredicate(t metadata.FactTable) string {
	parts := make([]string, len(t.SlotColumns))
	for i, c := range t.SlotColumns {
		q := s.dialect.QuoteIdentifier(c)
		parts[i] = "(" + q + " >= ? AND " + q + " <= ?)"
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// ReadFactRows implements metadata.FactRowStore.
func (s *Store) ReadFactRows(
	ctx context.Context,
	table metadata.FactTable,
	from, through uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) (metadata.FactRows, int64, error) {
	t, err := factTable(table)
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	lo, err := checkedInt64(from)
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	hi, err := checkedInt64(through)
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	// A nil txn reads on the pool and takes ctx below; a transaction carries
	// its own context.
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return metadata.FactRows{}, 0, fmt.Errorf(
			"read %s facts: %w",
			t.Name,
			err,
		)
	}
	if txn == nil {
		txCtx = ctx
	}
	where := s.slotPredicate(t)
	args := make([]any, 0, 2*len(t.SlotColumns)+2)
	for range t.SlotColumns {
		args = append(args, lo, hi)
	}
	if t.FilterColumn != "" {
		where += " AND " + s.dialect.QuoteIdentifier(t.FilterColumn) + " = ?"
		args = append(args, t.FilterValue)
	}
	query := "SELECT * FROM " + s.dialect.QuoteIdentifier(t.Name) +
		" WHERE " + where
	if t.Paged {
		if limit <= 0 {
			return metadata.FactRows{}, 0, errors.New(
				"sqlstore: paged fact read needs a positive limit",
			)
		}
		query += " AND " + s.dialect.QuoteIdentifier("id") + " > ? ORDER BY " +
			s.dialect.QuoteIdentifier("id") + " LIMIT ?"
		args = append(args, cursor, limit)
	}
	rs, err := db.QueryContext(txCtx, query, args...)
	if err != nil {
		return metadata.FactRows{}, 0, fmt.Errorf(
			"read %s facts: %w",
			t.Name,
			err,
		)
	}
	defer rs.Close()
	cols, err := rs.Columns()
	if err != nil {
		return metadata.FactRows{}, 0, err
	}
	idIdx := -1
	out := metadata.FactRows{}
	for i, c := range cols {
		if t.Paged && c == "id" {
			idIdx = i
			continue
		}
		out.Columns = append(out.Columns, c)
	}
	var next int64
	for rs.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return metadata.FactRows{}, 0, err
		}
		row := make([]any, 0, len(out.Columns))
		for i, v := range vals {
			if i == idIdx {
				id, ok := v.(int64)
				if !ok {
					return metadata.FactRows{}, 0, fmt.Errorf(
						"sqlstore: %s id is %T", t.Name, v,
					)
				}
				next = id
				continue
			}
			row = append(row, v)
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rs.Err(); err != nil {
		return metadata.FactRows{}, 0, err
	}
	out.LastID = next
	if !t.Paged || len(out.Rows) < limit {
		next = 0
	}
	return out, next, nil
}

// DeleteFactRowsAfter implements metadata.FactRowStore.
func (s *Store) DeleteFactRowsAfter(
	table metadata.FactTable,
	slot uint64,
	txn types.Txn,
) error {
	t, err := factTable(table)
	if err != nil {
		return err
	}
	if t.BlockPatched || t.FilterColumn != "" {
		return nil
	}
	if txn == nil {
		return errors.New(
			"sqlstore: fact rows are deleted inside a transaction",
		)
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	db, txCtx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete %s facts: %w", t.Name, err)
	}
	parts := make([]string, len(t.SlotColumns))
	args := make([]any, len(t.SlotColumns))
	for i, c := range t.SlotColumns {
		parts[i] = s.dialect.QuoteIdentifier(c) + " > ?"
		args[i] = sqlSlot
	}
	query := "DELETE FROM " + s.dialect.QuoteIdentifier(t.Name) +
		" WHERE " + strings.Join(parts, " OR ")
	if _, err := db.ExecContext(txCtx, query, args...); err != nil {
		return fmt.Errorf("delete %s facts: %w", t.Name, err)
	}
	return nil
}
