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
	"math"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
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

func (s *Store) quoteAll(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = s.dialect.QuoteIdentifier(n)
	}
	return out
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
	if t.Companion != nil {
		return s.readCompanionFacts(txCtx, db, t, lo, hi, cursor, limit)
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
	if err := asOfRows(t, &out, through); err != nil {
		return metadata.FactRows{}, 0, err
	}
	out.LastID = next
	if !t.Paged || len(out.Rows) < limit {
		next = 0
	}
	return out, next, nil
}

// WriteFactRows implements metadata.FactRowStore.
func (s *Store) WriteFactRows(
	table metadata.FactTable,
	rows metadata.FactRows,
	txn types.Txn,
) error {
	t, err := factTable(table)
	if err != nil {
		return err
	}
	if len(rows.Rows) == 0 {
		return nil
	}
	if txn == nil {
		return errors.New(
			"sqlstore: fact rows are written inside a transaction",
		)
	}
	db, txCtx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("write %s facts: %w", t.Name, err)
	}
	if t.Companion != nil {
		return s.writeCompanionFacts(txCtx, db, t, rows)
	}
	colIdx := make(map[string]int, len(rows.Columns))
	for i, c := range rows.Columns {
		colIdx[c] = i
	}
	for _, k := range t.Key {
		if _, ok := colIdx[k]; !ok {
			return fmt.Errorf("sqlstore: %s rows lack key column %s", t.Name, k)
		}
	}
	keyPred := make([]string, len(t.Key))
	for i, k := range t.Key {
		keyPred[i] = s.dialect.QuoteIdentifier(k) + " = ?"
	}
	keyWhere := strings.Join(keyPred, " AND ")
	keyArgs := func(row []any) []any {
		args := make([]any, len(t.Key))
		for i, k := range t.Key {
			args[i] = row[colIdx[k]]
		}
		return args
	}
	quotedTable := s.dialect.QuoteIdentifier(t.Name)
	if len(t.Patch) > 0 {
		sets := make([]string, len(t.Patch))
		for i, c := range t.Patch {
			if _, ok := colIdx[c]; !ok {
				return fmt.Errorf(
					"sqlstore: %s rows lack patch column %s",
					t.Name,
					c,
				)
			}
			sets[i] = s.dialect.QuoteIdentifier(c) + " = ?"
		}
		update := "UPDATE " + quotedTable + " SET " +
			strings.Join(sets, ", ") + " WHERE " + keyWhere
		for _, row := range rows.Rows {
			args := make([]any, 0, len(t.Patch)+len(t.Key))
			for _, c := range t.Patch {
				args = append(args, row[colIdx[c]])
			}
			args = append(args, keyArgs(row)...)
			res, err := db.ExecContext(txCtx, update, args...)
			if err != nil {
				return fmt.Errorf("patch %s facts: %w", t.Name, err)
			}
			if t.Name != "account" {
				continue
			}
			if n, err := res.RowsAffected(); err != nil || n == 0 {
				continue
			}
			if err := s.refreshPatchedAccountLiveStake(
				txCtx, db, colIdx, row,
			); err != nil {
				return fmt.Errorf("patch %s facts: %w", t.Name, err)
			}
		}
		return nil
	}
	del := "DELETE FROM " + quotedTable + " WHERE " + keyWhere
	marks := strings.TrimSuffix(strings.Repeat("?, ", len(rows.Columns)), ", ")
	insert := "INSERT INTO " + quotedTable + " (" +
		strings.Join(
			s.quoteAll(rows.Columns),
			", ",
		) + ") VALUES (" + marks + ")"
	for _, row := range rows.Rows {
		if _, err := db.ExecContext(txCtx, del, keyArgs(row)...); err != nil {
			return fmt.Errorf("replace %s facts: %w", t.Name, err)
		}
		if _, err := db.ExecContext(txCtx, insert, row...); err != nil {
			return fmt.Errorf("insert %s facts: %w", t.Name, err)
		}
	}
	return nil
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
	if len(t.Patch) > 0 || t.FilterColumn != "" {
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

// asOfRows nulls the columns of each AsOf group whose slot lies past through.
func asOfRows(
	t metadata.FactTable,
	rows *metadata.FactRows,
	through uint64,
) error {
	for _, group := range t.AsOf {
		slotIdx := slices.Index(rows.Columns, group.SlotColumn)
		if slotIdx < 0 {
			return fmt.Errorf(
				"sqlstore: %s rows lack as-of slot column %s",
				t.Name, group.SlotColumn,
			)
		}
		idx := make([]int, 0, len(group.Columns))
		for _, c := range group.Columns {
			i := slices.Index(rows.Columns, c)
			if i < 0 {
				return fmt.Errorf("sqlstore: %s rows lack column %s", t.Name, c)
			}
			idx = append(idx, i)
		}
		for _, row := range rows.Rows {
			slot, ok := row[slotIdx].(int64)
			if !ok || slot < 0 || uint64(slot) <= through {
				continue
			}
			for _, i := range idx {
				row[i] = nil
			}
		}
	}
	return nil
}

var _ metadata.DrepExpiryAsOfStore = (*Store)(nil)

// ApplyDrepExpiryAsOf implements metadata.DrepExpiryAsOfStore.
func (s *Store) ApplyDrepExpiryAsOf(slot uint64, txn types.Txn) error {
	if txn == nil {
		return errors.New(
			"sqlstore: DRep expiry is applied inside a transaction",
		)
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("apply DRep expiry: %w", err)
	}
	latest := func(column string) string {
		return `(SELECT history.` + column + ` FROM drep_expiry_history history
  WHERE history.credential_tag = drep.credential_tag
    AND history.credential = drep.credential
    AND history.added_slot <= ?
  ORDER BY history.added_slot DESC, history.id DESC LIMIT 1)`
	}
	query := `UPDATE drep SET
  last_activity_epoch = ` + latest("last_activity_epoch") + `,
  expiry_epoch = ` + latest("expiry_epoch") + `
WHERE EXISTS (
  SELECT 1 FROM drep_expiry_history history
  WHERE history.credential_tag = drep.credential_tag
    AND history.credential = drep.credential
    AND history.added_slot <= ?)`
	if _, err := db.ExecContext(
		ctx, s.dialect.Rebind(query), sqlSlot, sqlSlot, sqlSlot,
	); err != nil {
		return fmt.Errorf("apply DRep expiry as of slot %d: %w", slot, err)
	}
	return nil
}

func (s *Store) readCompanionFacts(
	ctx context.Context,
	db queryer,
	t metadata.FactTable,
	lo, hi int64,
	cursor int64,
	limit int,
) (metadata.FactRows, int64, error) {
	if limit <= 0 {
		return metadata.FactRows{}, 0, errors.New(
			"sqlstore: paged fact read needs a positive limit",
		)
	}
	c, p := t.Companion, s.dialect.QuoteIdentifier(t.Companion.Parent)
	own := slices.Clone(t.Companion.Columns)
	cols := make([]string, 0, len(t.Key)+len(own))
	selects := make([]string, 0, len(t.Key)+len(own))
	for _, k := range t.Key {
		cols = append(cols, k)
		selects = append(selects, p+"."+s.dialect.QuoteIdentifier(k))
	}
	for _, o := range own {
		cols = append(cols, o)
		selects = append(
			selects,
			s.dialect.QuoteIdentifier(t.Name)+"."+s.dialect.QuoteIdentifier(o),
		)
	}
	tbl, id := s.dialect.QuoteIdentifier(t.Name),
		s.dialect.QuoteIdentifier(c.ParentID)
	slot := tbl + "." + s.dialect.QuoteIdentifier(t.SlotColumns[0])
	query := "SELECT " + strings.Join(selects, ", ") + ", " + tbl + "." + id +
		" FROM " + tbl + " JOIN " + p + " ON " + p + "." +
		s.dialect.QuoteIdentifier("id") + " = " + tbl + "." + id +
		" WHERE " + slot + " >= ? AND " + slot + " <= ? AND " + tbl + "." +
		id + " > ? ORDER BY " + tbl + "." + id + " LIMIT ?"
	rs, err := db.QueryContext(ctx, query, lo, hi, cursor, limit)
	if err != nil {
		return metadata.FactRows{}, 0, fmt.Errorf(
			"read %s facts: %w", t.Name, err,
		)
	}
	defer rs.Close()
	out := metadata.FactRows{Columns: cols}
	var next int64
	for rs.Next() {
		vals := make([]any, len(cols)+1)
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return metadata.FactRows{}, 0, err
		}
		pid, ok := vals[len(cols)].(int64)
		if !ok {
			return metadata.FactRows{}, 0, fmt.Errorf(
				"sqlstore: %s %s is %T", t.Name, c.ParentID, vals[len(cols)],
			)
		}
		next = pid
		out.Rows = append(out.Rows, vals[:len(cols)])
	}
	if err := rs.Err(); err != nil {
		return metadata.FactRows{}, 0, err
	}
	if len(out.Rows) < limit {
		next = 0
	}
	return out, next, nil
}

// writeCompanionFacts replaces the companion row of each parent found by its
// Key. A parent the store does not hold is skipped by the insert's own select.
func (s *Store) writeCompanionFacts(
	ctx context.Context,
	db queryer,
	t metadata.FactTable,
	rows metadata.FactRows,
) error {
	colIdx := make(map[string]int, len(rows.Columns))
	for i, c := range rows.Columns {
		colIdx[c] = i
	}
	own := slices.Clone(t.Companion.Columns)
	for _, c := range append(slices.Clone(t.Key), own...) {
		if _, ok := colIdx[c]; !ok {
			return fmt.Errorf("sqlstore: %s rows lack column %s", t.Name, c)
		}
	}
	tbl := s.dialect.QuoteIdentifier(t.Name)
	parent := s.dialect.QuoteIdentifier(t.Companion.Parent)
	id := s.dialect.QuoteIdentifier(t.Companion.ParentID)
	keyPred := make([]string, len(t.Key))
	for i, k := range t.Key {
		keyPred[i] = s.dialect.QuoteIdentifier(k) + " = ?"
	}
	keyWhere := strings.Join(keyPred, " AND ")
	del := "DELETE FROM " + tbl + " WHERE " + id + " IN (SELECT " +
		s.dialect.QuoteIdentifier("id") + " FROM " + parent + " WHERE " +
		keyWhere + ")"
	marks := strings.TrimSuffix(strings.Repeat("?, ", len(own)), ", ")
	insert := "INSERT INTO " + tbl + " (" + id + ", " +
		strings.Join(s.quoteAll(own), ", ") + ") SELECT " +
		s.dialect.QuoteIdentifier("id") + ", " + marks + " FROM " + parent +
		" WHERE " + keyWhere
	for _, row := range rows.Rows {
		keyArgs := make([]any, len(t.Key))
		for i, k := range t.Key {
			keyArgs[i] = row[colIdx[k]]
		}
		if _, err := db.ExecContext(ctx, del, keyArgs...); err != nil {
			return fmt.Errorf("replace %s facts: %w", t.Name, err)
		}
		args := make([]any, 0, len(own)+len(t.Key))
		for _, o := range own {
			args = append(args, row[colIdx[o]])
		}
		args = append(args, keyArgs...)
		if _, err := db.ExecContext(ctx, insert, args...); err != nil {
			return fmt.Errorf("insert %s facts: %w", t.Name, err)
		}
	}
	return nil
}

// refreshPatchedAccountLiveStake brings the live-stake row of a patched
// account in step with it. The row mirrors the account's pool and
// registration, and only a write of the account through the ledger's own path
// refreshes it, which a patch does not go through. The row is dated at the
// slot the patched account carries, where that write dates it.
func (s *Store) refreshPatchedAccountLiveStake(
	ctx context.Context,
	db queryer,
	colIdx map[string]int,
	row []any,
) error {
	tag, ok := row[colIdx["credential_tag"]].(int64)
	if !ok || tag < 0 || tag > math.MaxUint8 {
		return fmt.Errorf(
			"account fact credential tag is %v", row[colIdx["credential_tag"]],
		)
	}
	key, ok := row[colIdx["staking_key"]].([]byte)
	if !ok {
		return fmt.Errorf(
			"account fact staking key is %T", row[colIdx["staking_key"]],
		)
	}
	added, ok := row[colIdx["added_slot"]].(int64)
	if !ok {
		return fmt.Errorf(
			"account fact slot is %T", row[colIdx["added_slot"]],
		)
	}
	slot, err := checkedUint64(added)
	if err != nil {
		return err
	}
	return s.refreshRewardLiveStakeAggregateDelta(
		ctx, db, models.NewStakeCredentialRef(uint8(tag), key), slot, 0,
	)
}
