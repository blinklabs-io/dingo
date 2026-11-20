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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
)

// CoreCopyCompleteKey is written after the consensus namespace holds every
// copied row and the adopted IndexerDB's tip and offsets are durable. Its presence prevents a later startup copying stale API state
// over consensus state that has advanced independently.
const CoreCopyCompleteKey = "dingo:storage:core-copy:complete"

// CoreCopyAdoptedKey is written to the adopted source store once the
// consensus namespace beside it holds CoreCopyCompleteKey. A source carrying
// it beside a namespace without that key lost the namespace after adoption:
// copying the source again would rebuild consensus state from the source's
// tip, which the IndexerDB writer has since moved past the ledger's history.
const CoreCopyAdoptedKey = "dingo:storage:core-copy:adopted"

const (
	coreCopyPrefix = "dingo:storage:core-copy:"
	// CoreCopyCursorPrefix starts the sync_state key of each table's copy
	// cursor in the consensus namespace.
	CoreCopyCursorPrefix = coreCopyPrefix + "cursor:"
	coreCopyBatchRows    = 500
)

// coreCopyAPITables are tables only API storage mode writes or reads. They
// stay in the adopted IndexerDB, which serves them; the consensus namespace
// never reads them.
var coreCopyAPITables = []string{
	"address_transaction",
	"address_transaction_rebuild_source",
	"redeemer_script",
	"transaction_metadata_label",
}

// coreCopyFixedExclusions are tables a consensus copy never carries: the
// destination's own migration bookkeeping. Every other table of the schema
// that is not API-only is copied, so a table a later release adds reaches the
// consensus namespace by default.
var coreCopyFixedExclusions = []string{"schema_migrations"}

type coreCopyCursor struct {
	After string `json:"after"`
	Int   bool   `json:"int,omitempty"`
	Done  bool   `json:"done"`
}

// CopyConsensusStateTo copies the rows a core-mode store reads from this
// API store into dst, a separate consensus namespace whose schema is at the
// same version. It is the server counterpart of the SQLite adoption, where the
// released file keeps every table and becomes the consensus store: the copy
// carries every table except the API-only ones and every table whose foreign
// keys point into one of those. The UTxO rows and the ledger-state families
// are copied too: the node reverts the namespace below its tip before it
// moves them into the LedgerDB, and the revert reads them. Tables are copied
// parents first. Every bounded batch and
// its cursor commit together, so interruption never advances past missing
// rows; the first batch of a table also clears what dst's own migrations
// seeded there. afterBatch is an optional fault-injection hook called after a
// durable batch. The caller writes CoreCopyCompleteKey only after completing
// adoption.
func (s *Store) CopyConsensusStateTo(
	ctx context.Context,
	dst *Store,
	afterBatch func(string) error,
) error {
	if dst == nil || dst == s || dst.writeDB == s.writeDB {
		return errors.New(
			"consensus state needs a separate destination store",
		)
	}
	if dst.dialect.Name() != s.dialect.Name() {
		return errors.New(
			"consensus state copies only between stores of one provider",
		)
	}
	tables, err := dst.coreCopyTables(ctx)
	if err != nil {
		return fmt.Errorf("plan consensus copy: %w", err)
	}
	for _, table := range tables {
		if err := s.copyConsensusTable(ctx, dst, table, afterBatch); err != nil {
			return fmt.Errorf("copy consensus table %s: %w", table, err)
		}
	}
	return nil
}

// coreCopyTables returns the tables of this store's schema a consensus copy
// carries, parents before the tables whose foreign keys reference them.
func (s *Store) coreCopyTables(
	ctx context.Context,
) ([]string, error) {
	all, err := s.schemaTables(ctx)
	if err != nil {
		return nil, err
	}
	edges, err := s.foreignKeyEdges(ctx)
	if err != nil {
		return nil, err
	}
	skipped := map[string]bool{}
	for _, list := range [][]string{
		coreCopyFixedExclusions, coreCopyAPITables,
	} {
		for _, name := range list {
			skipped[name] = true
		}
	}
	// A row of a copied table must not reference one left behind.
	for changed := true; changed; {
		changed = false
		for _, e := range edges {
			if skipped[e.parent] && !skipped[e.child] {
				skipped[e.child] = true
				changed = true
			}
		}
	}
	remaining := make([]string, 0, len(all))
	for _, name := range all {
		if !skipped[name] {
			remaining = append(remaining, name)
		}
	}
	slices.Sort(remaining)
	ordered := make([]string, 0, len(remaining))
	placed := map[string]bool{}
	for len(remaining) != 0 {
		next := remaining[:0:0]
		progress := false
		for _, name := range remaining {
			ready := true
			for _, e := range edges {
				if e.child == name && e.parent != name &&
					!skipped[e.parent] && !placed[e.parent] {
					ready = false
					break
				}
			}
			if ready {
				ordered = append(ordered, name)
				placed[name] = true
				progress = true
			} else {
				next = append(next, name)
			}
		}
		if !progress {
			return nil, fmt.Errorf(
				"foreign keys among %s form a cycle",
				strings.Join(next, ", "),
			)
		}
		remaining = next
	}
	return ordered, nil
}

func (s *Store) schemaTables(ctx context.Context) ([]string, error) {
	var query string
	switch s.dialect.Name() {
	case "postgres":
		query = `SELECT table_name FROM information_schema.tables
WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'`
	case "mysql":
		query = `SELECT table_name FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'`
	default:
		query = `SELECT name FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`
	}
	return queryStrings(ctx, s.writeDB, query)
}

type foreignKeyEdge struct{ child, parent string }

func (s *Store) foreignKeyEdges(ctx context.Context) ([]foreignKeyEdge, error) {
	var query string
	switch s.dialect.Name() {
	case "postgres":
		query = `SELECT c.relname, p.relname FROM pg_constraint k
JOIN pg_class c ON c.oid = k.conrelid
JOIN pg_class p ON p.oid = k.confrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE k.contype = 'f' AND n.nspname = current_schema()`
	case "mysql":
		query = `SELECT table_name, referenced_table_name
FROM information_schema.key_column_usage
WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL`
	default:
		query = `SELECT m.name, f."table" FROM sqlite_master m,
pragma_foreign_key_list(m.name) f WHERE m.type = 'table'`
	}
	rows, err := s.writeDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list foreign keys: %w", err)
	}
	defer rows.Close()
	var edges []foreignKeyEdge
	for rows.Next() {
		var e foreignKeyEdge
		if err := rows.Scan(&e.child, &e.parent); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	return edges, rows.Err()
}

// primaryKey returns the single column of table's primary key, or "" when the
// table has none or a composite one.
func (s *Store) primaryKey(ctx context.Context, table string) (string, error) {
	var query string
	switch s.dialect.Name() {
	case "postgres":
		query = `SELECT a.attname FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = to_regclass(quote_ident($1)) AND i.indisprimary`
	case "mysql":
		query = `SELECT column_name FROM information_schema.key_column_usage
WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = 'PRIMARY'`
	default:
		query = `SELECT name FROM pragma_table_info(?) WHERE pk > 0`
	}
	columns, err := queryStrings(ctx, s.writeDB, query, table)
	if err != nil {
		return "", err
	}
	if len(columns) != 1 {
		return "", nil
	}
	return columns[0], nil
}

func queryStrings(
	ctx context.Context,
	db *sql.DB,
	query string,
	args ...any,
) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

func (s *Store) copyConsensusTable(
	ctx context.Context,
	dst *Store,
	name string,
	afterBatch func(string) error,
) error {
	marker := CoreCopyCursorPrefix + name
	var raw string
	err := dst.writeDB.QueryRowContext(ctx, dst.dialect.Rebind("SELECT value FROM sync_state WHERE sync_key = ?"), marker).
		Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var cursor coreCopyCursor
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
			return fmt.Errorf("read migration cursor: %w", err)
		}
	}
	if cursor.Done {
		return nil
	}
	key, err := dst.primaryKey(ctx, name)
	if err != nil {
		return fmt.Errorf("read primary key: %w", err)
	}
	if key == "" {
		if err := s.copyConsensusTableWhole(ctx, dst, name, marker); err != nil {
			return err
		}
		if afterBatch != nil {
			return afterBatch(name)
		}
		return nil
	}
	sourceTable := s.dialect.QuoteIdentifier(name)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		query := "SELECT * FROM " + sourceTable
		var args []any
		sourceKey := s.dialect.QuoteIdentifier(key)
		if cursor.After != "" {
			var bound any = cursor.After
			if cursor.Int {
				bound, err = strconv.ParseInt(cursor.After, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid migration cursor: %w", err)
				}
			}
			query += " WHERE " + sourceKey + " > ?"
			args = append(args, bound)
		}
		query += fmt.Sprintf(
			" ORDER BY %s LIMIT %d",
			sourceKey,
			coreCopyBatchRows,
		)
		columns, batch, err := s.readCopyBatch(ctx, query, args, name, key)
		if err != nil {
			return err
		}
		next := cursor
		if len(batch.rows) != 0 {
			switch value := batch.lastKey.(type) {
			case int64:
				next.After = strconv.FormatInt(value, 10)
				next.Int = true
			case string:
				next.After = value
			case []byte:
				next.After = string(value)
			default:
				return fmt.Errorf(
					"unexpected migration key type %T",
					value,
				)
			}
		}
		next.Done = batch.read < coreCopyBatchRows
		if err := dst.writeCopyBatch(ctx, name, columns, batch.rows, cursor.After == "", next, marker); err != nil {
			return err
		}
		if afterBatch != nil {
			if err := afterBatch(name); err != nil {
				return err
			}
		}
		if next.Done {
			return nil
		}
		cursor = next
	}
}

// copyConsensusTableWhole copies a table without a single-column key, which
// has no cursor to resume from, in one destination transaction: a stop leaves
// the table absent rather than partial. The rows are read and written
// coreCopyBatchRows at a time, so the table is never held in memory.
func (s *Store) copyConsensusTableWhole(
	ctx context.Context,
	dst *Store,
	name, marker string,
) (err error) {
	encoded, err := json.Marshal(coreCopyCursor{Done: true})
	if err != nil {
		return err
	}
	rows, err := s.writeDB.QueryContext(
		ctx,
		"SELECT * FROM "+s.dialect.QuoteIdentifier(name), //nolint:gosec // G202: name is a quoted schema identifier
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	tx, err := dst.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	if _, err := tx.ExecContext(
		ctx,
		"DELETE FROM "+dst.dialect.QuoteIdentifier(name), //nolint:gosec // G202: name is a quoted schema identifier
	); err != nil {
		return err
	}
	batch := make([][]any, 0, coreCopyBatchRows)
	flush := func() error {
		err := dst.insertCopyRows(ctx, tx, name, columns, batch)
		batch = batch[:0]
		return err
	}
	for rows.Next() {
		values, err := scanCopyRow(rows, len(columns))
		if err != nil {
			return err
		}
		batch = append(batch, values)
		if len(batch) == coreCopyBatchRows {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := dst.advanceSequences(ctx, tx, name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		dst.dialect.Rebind(
			consensusUpsert(
				dst.dialect,
				"sync_state",
				[]string{"sync_key", "value"},
				"sync_key",
			),
		),
		marker,
		string(encoded),
	); err != nil {
		return err
	}
	return tx.Commit()
}

// scanCopyRow reads the current row of rows, copying the byte slices the
// driver reuses.
func scanCopyRow(rows *sql.Rows, columns int) ([]any, error) {
	values := make([]any, columns)
	targets := make([]any, columns)
	for i := range values {
		targets[i] = &values[i]
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	for i, value := range values {
		if raw, ok := value.([]byte); ok {
			values[i] = append([]byte(nil), raw...)
		}
	}
	return values, nil
}

type copyBatch struct {
	rows    [][]any
	read    int
	lastKey any
}

func (s *Store) readCopyBatch(
	ctx context.Context,
	query string,
	args []any,
	name, key string,
) ([]string, copyBatch, error) {
	var batch copyBatch
	rows, err := s.writeDB.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return nil, batch, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, batch, err
	}
	keyIndex, syncKeyIndex := -1, -1
	for i, column := range columns {
		if key != "" && column == key {
			keyIndex = i
		}
		if name == "sync_state" && column == "sync_key" {
			syncKeyIndex = i
		}
	}
	if key != "" && keyIndex < 0 {
		return nil, batch, fmt.Errorf("missing migration key %q", key)
	}
	for rows.Next() {
		values, err := scanCopyRow(rows, len(columns))
		if err != nil {
			return nil, batch, err
		}
		batch.read++
		if keyIndex >= 0 {
			batch.lastKey = values[keyIndex]
		}
		if syncKeyIndex >= 0 &&
			strings.HasPrefix(stringValue(values[syncKeyIndex]), coreCopyPrefix) {
			continue
		}
		batch.rows = append(batch.rows, values)
	}
	return columns, batch, rows.Err()
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

// writeCopyBatch inserts one batch and records its cursor in a single
// transaction. The first batch of a table first removes the rows dst's
// migrations seeded, keeping the copy's own cursors.
func (s *Store) writeCopyBatch(
	ctx context.Context,
	name string,
	columns []string,
	batch [][]any,
	first bool,
	next coreCopyCursor,
	marker string,
) error {
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	writeErr := func() error {
		table := s.dialect.QuoteIdentifier(name)
		if first {
			clear := "DELETE FROM " + table //nolint:gosec // G202: table is a quoted schema identifier
			if name == "sync_state" {
				clear += " WHERE sync_key NOT LIKE 'dingo:storage:core-copy:%'"
			}
			if _, err := tx.ExecContext(ctx, clear); err != nil {
				return err
			}
		}
		if err := s.insertCopyRows(ctx, tx, name, columns, batch); err != nil {
			return err
		}
		if next.Done {
			if err := s.advanceSequences(ctx, tx, name); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(
			ctx,
			s.dialect.Rebind(
				consensusUpsert(
					s.dialect,
					"sync_state",
					[]string{"sync_key", "value"},
					"sync_key",
				),
			),
			marker,
			string(encoded),
		)
		return err
	}()
	if writeErr != nil {
		return errors.Join(writeErr, tx.Rollback())
	}
	return tx.Commit()
}

// insertCopyRows inserts copied rows of table name through tx.
func (s *Store) insertCopyRows(
	ctx context.Context,
	tx *sql.Tx,
	name string,
	columns []string,
	batch [][]any,
) error {
	if len(batch) == 0 {
		return nil
	}
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = s.dialect.QuoteIdentifier(column)
	}
	row := "(" + strings.TrimSuffix(
		strings.Repeat("?,", len(columns)), ",",
	) + ")"
	perStatement := max(1, s.dialect.ParameterLimit()/len(columns))
	for start := 0; start < len(batch); start += perStatement {
		end := min(start+perStatement, len(batch))
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, (end-start)*len(columns))
		for _, values := range batch[start:end] {
			placeholders = append(placeholders, row)
			args = append(args, values...)
		}
		insert := "INSERT INTO " + s.dialect.QuoteIdentifier(name) + " (" +
			strings.Join(quoted, ", ") + ") VALUES " +
			strings.Join(placeholders, ", ")
		if _, err := tx.ExecContext(ctx, s.dialect.Rebind(insert), args...); err != nil {
			return err
		}
	}
	return nil
}

// advanceSequences moves each PostgreSQL serial sequence of table past the
// copied keys, which were inserted explicitly and so never drew from it.
// MySQL and SQLite advance their counters on an explicit insert.
func (s *Store) advanceSequences(
	ctx context.Context,
	tx *sql.Tx,
	name string,
) error {
	if s.dialect.Name() != "postgres" {
		return nil
	}
	columns, err := serialColumns(ctx, tx, name)
	if err != nil {
		return err
	}
	table := s.dialect.QuoteIdentifier(name)
	for _, column := range columns {
		quoted := s.dialect.QuoteIdentifier(column)
		if _, err := tx.ExecContext(
			ctx,
			//nolint:gosec // G202: quoted and table are quoted schema identifiers
			"SELECT setval(pg_get_serial_sequence($1, $2), COALESCE(MAX("+
				quoted+"), 1), MAX("+quoted+") IS NOT NULL) FROM "+table,
			table,
			column,
		); err != nil {
			return fmt.Errorf("advance sequence of %s.%s: %w", name, column, err)
		}
	}
	return nil
}

func consensusUpsert(
	dialect Dialect,
	name string,
	columns []string,
	key string,
) string {
	quoted := make([]string, len(columns))
	updates := make([]string, 0, len(columns)-1)
	for i, column := range columns {
		quoted[i] = dialect.QuoteIdentifier(column)
		if column != key {
			value := "excluded." + quoted[i]
			if dialect.Name() == "mysql" {
				value = "VALUES(" + quoted[i] + ")"
			}
			updates = append(updates, quoted[i]+" = "+value)
		}
	}
	q := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		dialect.QuoteIdentifier(name),
		strings.Join(quoted, ", "),
		strings.TrimSuffix(strings.Repeat("?,", len(columns)), ","),
	)
	if dialect.Name() == "mysql" {
		return q + " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
	}
	return q + " ON CONFLICT (" + dialect.QuoteIdentifier(
		key,
	) + ") DO UPDATE SET " + strings.Join(
		updates,
		", ",
	)
}

// FinishConsensusSettingsCopy records the independent core role only after
// the IndexerDB is durable. The role latches and the completion marker commit
// together. The namespace is left as a released consensus store is: its UTxO
// rows and ledger-state families are still in SQL, for the node to revert
// below the tip and move into the LedgerDB.
func (s *Store) FinishConsensusSettingsCopy(
	ctx context.Context,
	epoch, slot uint64,
) error {
	if epoch > math.MaxInt64 || slot > math.MaxInt64 {
		return errors.New(
			"consensus migration epoch or slot exceeds SQL integer range",
		)
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "UPDATE node_settings SET storage_mode = 'core' WHERE id = 1"); err != nil {
		return err
	}
	gates := map[string]string{
		"storage_mode":   "core",
		"indexer_layout": "split",
	}
	for key, value := range gates {
		_, err := tx.ExecContext(
			ctx,
			s.dialect.Rebind(
				consensusUpsert(
					s.dialect,
					"node_settings_gate",
					[]string{
						"name",
						"value",
						"recorded_epoch",
						"recorded_slot",
					},
					"name",
				),
			),
			key,
			value,
			int64(epoch),
			int64(slot),
		)
		if err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sync_state WHERE sync_key = 'ledger.utxo_set_migration'"); err != nil {
		return err
	}
	_, err = tx.ExecContext(
		ctx,
		s.dialect.Rebind(
			consensusUpsert(
				s.dialect,
				"sync_state",
				[]string{"sync_key", "value"},
				"sync_key",
			),
		),
		CoreCopyCompleteKey,
		"1",
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// serialColumns returns the PostgreSQL columns of table that draw defaults
// from a sequence.
func serialColumns(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT column_name FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = $1
AND column_default LIKE 'nextval(%'`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

// CoreCopyPlan describes a consensus copy of the full schema.
type CoreCopyPlan struct {
	// Copied lists the copied tables in copy order.
	Copied []string
	// DroppedByForeignKey lists tables left behind only because a foreign
	// key references a table left behind.
	DroppedByForeignKey []string
	// CopiedWhole lists copied tables without a single-column primary key,
	// which are copied in one transaction.
	CopiedWhole []string
}

// PlanCoreCopy plans a consensus copy of the full schema, on a temporary
// in-memory SQLite store migrated to the current version.
func PlanCoreCopy(ctx context.Context) (CoreCopyPlan, error) {
	var plan CoreCopyPlan
	db, err := sql.Open("sqlite", "file:core_copy_plan?mode=memory&cache=shared")
	if err != nil {
		return plan, err
	}
	defer db.Close()
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return plan, err
	}
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	if err != nil {
		return plan, err
	}
	if err := store.Start(ctx); err != nil {
		return plan, err
	}
	plan.Copied, err = store.coreCopyTables(ctx)
	if err != nil {
		return plan, err
	}
	all, err := store.schemaTables(ctx)
	if err != nil {
		return plan, err
	}
	named := map[string]bool{}
	for _, list := range [][]string{coreCopyFixedExclusions, coreCopyAPITables} {
		for _, name := range list {
			named[name] = true
		}
	}
	for _, name := range all {
		if !named[name] && !slices.Contains(plan.Copied, name) {
			plan.DroppedByForeignKey = append(plan.DroppedByForeignKey, name)
		}
	}
	for _, name := range plan.Copied {
		key, err := store.primaryKey(ctx, name)
		if err != nil {
			return plan, err
		}
		if key == "" {
			plan.CopiedWhole = append(plan.CopiedWhole, name)
		}
	}
	return plan, nil
}
