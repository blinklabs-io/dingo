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

package migrations

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// migrationSQL contains immutable, versioned migration resources.
//
//go:embed v1/*/*.sql
var migrationSQL embed.FS

const InitialSchemaRelease = "v1alpha1"

// SQLiteRegistry returns the checked-in SQLite migration registry.
func SQLiteRegistry() ([]Migration, error) {
	return registryForDialect("sqlite")
}

// PostgresRegistry and MySQLRegistry expose the same v1alpha1 schema contract
// with backend-native type and identity syntax. Keeping the migration
// registry shared prevents the three providers from drifting at the schema
// boundary while allowing each engine to execute its own DDL.
func PostgresRegistry() ([]Migration, error) {
	return registryForDialect("postgres")
}

func MySQLRegistry() ([]Migration, error) {
	return registryForDialect("mysql")
}

func registryForDialect(dialect string) ([]Migration, error) {
	expand, err := loadSQL("v1/sqlite/expand.sql")
	if err != nil {
		return nil, err
	}
	contract, err := loadSQL("v1/sqlite/contract.sql")
	if err != nil {
		return nil, err
	}
	sqlForDialect := SQL{Expand: expand, Contract: contract}
	if dialect != "sqlite" {
		sqlForDialect.Expand = translateSchemaSQL(expand, dialect)
		sqlForDialect.Contract = translateSchemaSQL(contract, dialect)
	}
	return []Migration{{
		Version:          1,
		Name:             InitialSchemaRelease,
		BackfillRevision: "none",
		SQL: map[string]SQL{
			dialect: sqlForDialect,
		},
		Adopt: adoptionForDialect(dialect),
	}}, nil
}

func adoptionForDialect(dialect string) Adoption {
	if dialect == "sqlite" {
		return adoptSQLiteV1
	}
	return adoptExistingV1
}

// adoptExistingV1 validates an unversioned PostgreSQL/MySQL database that was
// created by the previous metadata implementation. Those backends already
// enforce their table constraints through the provider's normal DDL; the
// database/sql cutover must still refuse a partial or incompatible schema
// rather than silently treating it as fresh. The subsequent v1 expand phase
// is idempotent and fills in any missing non-destructive indexes.
func adoptExistingV1(ctx context.Context, conn *sql.Conn, dialect string) error {
	if dialect != "postgres" && dialect != "mysql" {
		return fmt.Errorf("%w: unsupported adoption dialect %q", ErrLegacySchema, dialect)
	}
	expected, err := sqliteV1Columns()
	if err != nil {
		return err
	}
	for table, columns := range expected {
		found, err := existingDialectTableColumns(ctx, conn, dialect, table)
		if err != nil {
			return err
		}
		if len(found) == 0 {
			return fmt.Errorf("%w: missing required table %q in %s database", ErrLegacySchema, table, dialect)
		}
		for column := range columns {
			if _, ok := found[column]; !ok {
				return fmt.Errorf(
					"%w: table %q is missing column %q in %s database",
					ErrLegacySchema,
					table,
					column,
					dialect,
				)
			}
		}
	}
	// Copy legacy reference-input edges only after validating the complete
	// contract. An incomplete utxo table must report ErrLegacySchema rather
	// than failing with a misleading missing-column error from the copy query.
	if err := ensureExistingReferenceInputs(ctx, conn, dialect); err != nil {
		return err
	}
	return nil
}

func ensureExistingReferenceInputs(ctx context.Context, conn *sql.Conn, dialect string) error {
	utxoColumns, err := existingDialectTableColumns(ctx, conn, dialect, "utxo")
	if err != nil {
		return err
	}
	if len(utxoColumns) == 0 {
		return nil
	}
	if _, ok := utxoColumns["referenced_by_tx_id"]; !ok {
		// Let the complete contract validation report the missing legacy
		// column instead of issuing a copy query that cannot run.
		return nil
	}
	statements, err := loadSQL("v1/sqlite/expand.sql")
	if err != nil {
		return err
	}
	var create string
	for _, statement := range statements {
		if schemaTableName(statement) == "utxo_reference_input" {
			create = translateSchemaSQL([]string{statement}, dialect)[0]
			break
		}
	}
	if create == "" {
		return fmt.Errorf("%w: missing v1 reference-input DDL", ErrLegacySchema)
	}
	if _, err := conn.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("create reference-input association table: %w", err)
	}
	insert := `INSERT INTO utxo_reference_input (utxo_id, transaction_hash)
SELECT id, referenced_by_tx_id FROM utxo
WHERE referenced_by_tx_id IS NOT NULL AND length(referenced_by_tx_id) > 0`
	if dialect == "mysql" {
		insert = strings.Replace(insert, "INSERT INTO", "INSERT IGNORE INTO", 1)
	} else {
		insert += " ON CONFLICT DO NOTHING"
	}
	if dialect == "postgres" {
		insert = strings.Replace(insert, "?", "$1", 1)
	}
	if _, err := conn.ExecContext(ctx, insert); err != nil {
		return fmt.Errorf("adopt legacy reference inputs: %w", err)
	}
	return nil
}

func existingDialectTableColumns(
	ctx context.Context,
	conn *sql.Conn,
	dialect, table string,
) (map[string]struct{}, error) {
	query := `SELECT column_name FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = ?`
	if dialect == "postgres" {
		query = `SELECT column_name FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = ?`
		query = strings.Replace(query, "?", "$1", 1)
	}
	rows, err := conn.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("inspect %s table %q: %w", dialect, table, err)
	}
	defer rows.Close()
	ret := make(map[string]struct{})
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan %s table %q columns: %w", dialect, table, err)
		}
		ret[column] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s table %q columns: %w", dialect, table, err)
	}
	return ret, nil
}

var (
	autoIncrementType = regexp.MustCompile(
		`(?i)integer PRIMARY KEY AUTOINCREMENT`,
	)
	wordType = func(word string) *regexp.Regexp {
		return regexp.MustCompile(`(?i)\b` + word + `\b`)
	}
	mysqlIndexPattern = regexp.MustCompile(
		`(?is)^(CREATE\s+(?:UNIQUE\s+)?INDEX\s+.*?\s+ON\s+` +
			"`?([a-zA-Z0-9_]+)`?" + `\s*)\(([^)]*)\)$`,
	)
	mysqlBlobColumnPattern = regexp.MustCompile(
		"(?i)`([^`]+)`\\s+(?:blob|text)\\b",
	)
	mysqlForeignKeyPattern = regexp.MustCompile(
		"(?i)FOREIGN\\s+KEY\\s*\\(`([^`]+)`\\)\\s+REFERENCES\\s+`([^`]+)`\\s*\\(`([^`]+)`\\)",
	)
	mysqlInlineKeyPattern = regexp.MustCompile(
		`(?is)(PRIMARY\s+KEY|UNIQUE(?:\s+KEY)?)\s*\(([^)]*)\)`,
	)
	mysqlTextDefaultPattern = regexp.MustCompile(
		`(?i)\btext(\s+NOT\s+NULL)?\s+DEFAULT\s+'0'`,
	)
)

func translateSchemaSQL(statements []string, dialect string) []string {
	translated := make([]string, len(statements))
	mysqlBlobColumns := make(map[string]map[string]struct{})
	mysqlForeignKeyColumns := make(map[string]map[string]struct{})
	if dialect == "mysql" {
		for _, statement := range statements {
			if !strings.HasPrefix(strings.ToUpper(statement), "CREATE TABLE") {
				continue
			}
			table := schemaTableName(statement)
			if table == "" {
				continue
			}
			columns := make(map[string]struct{})
			for _, match := range mysqlBlobColumnPattern.FindAllStringSubmatch(statement, -1) {
				columns[match[1]] = struct{}{}
			}
			mysqlBlobColumns[table] = columns
			for _, match := range mysqlForeignKeyPattern.FindAllStringSubmatch(statement, -1) {
				addColumn(mysqlForeignKeyColumns, table, match[1])
				addColumn(mysqlForeignKeyColumns, match[2], match[3])
			}
		}
		for table, columns := range mysqlForeignKeyColumns {
			for column := range columns {
				delete(mysqlBlobColumns[table], column)
			}
		}
	}
	for index, statement := range statements {
		value := statement
		switch dialect {
		case "postgres":
			value = strings.ReplaceAll(value, "`", `"`)
			value = autoIncrementType.ReplaceAllString(
				value,
				"BIGSERIAL PRIMARY KEY",
			)
			// SQLite's INTEGER columns are used for every metadata ID and
			// foreign-key column.  AUTOINCREMENT IDs become BIGSERIAL above;
			// widen the remaining integer columns as well so PostgreSQL accepts
			// foreign keys that reference those bigint IDs (PostgreSQL requires
			// matching integer types for FK constraints).
			value = wordType("integer").ReplaceAllString(value, "BIGINT")
			value = wordType("blob").ReplaceAllString(value, "BYTEA")
			value = wordType("datetime").ReplaceAllString(
				value,
				"TIMESTAMPTZ",
			)
			value = wordType("numeric").ReplaceAllString(value, "BOOLEAN")
			value = strings.ReplaceAll(value, `DEFAULT "0"`, "DEFAULT '0'")
		case "mysql":
			value = autoIncrementType.ReplaceAllString(
				value,
				"BIGINT AUTO_INCREMENT PRIMARY KEY",
			)
			value = wordType("integer").ReplaceAllString(value, "BIGINT")
			value = wordType("numeric").ReplaceAllString(value, "BOOLEAN")
			value = strings.ReplaceAll(value, `DEFAULT "0"`, "DEFAULT '0'")
			value = mysqlTextDefaultPattern.ReplaceAllString(
				value,
				"VARCHAR(255)$1 DEFAULT '0'",
			)
			value = strings.ReplaceAll(value, "CREATE INDEX IF NOT EXISTS", "CREATE INDEX")
			value = strings.ReplaceAll(
				value,
				"CREATE UNIQUE INDEX IF NOT EXISTS",
				"CREATE UNIQUE INDEX",
			)
			if strings.HasPrefix(strings.ToUpper(statement), "CREATE TABLE") {
				for column := range mysqlForeignKeyColumns[schemaTableName(statement)] {
					value = replaceMySQLBlobType(value, column)
				}
			}
			value = translateMySQLInlineKeys(
				value,
				schemaTableName(statement),
				mysqlBlobColumns,
			)
			value = translateMySQLIndexColumns(value, mysqlBlobColumns)
		}
		translated[index] = value
	}
	return translated
}

func addColumn(columns map[string]map[string]struct{}, table, column string) {
	if columns[table] == nil {
		columns[table] = make(map[string]struct{})
	}
	columns[table][column] = struct{}{}
}

func replaceMySQLBlobType(statement, column string) string {
	pattern := regexp.MustCompile(
		"(?i)(`" + regexp.QuoteMeta(column) + "`\\s+)blob\\b",
	)
	return pattern.ReplaceAllString(statement, "${1}VARBINARY(255)")
}

func translateMySQLInlineKeys(
	statement string,
	table string,
	blobColumns map[string]map[string]struct{},
) string {
	return mysqlInlineKeyPattern.ReplaceAllStringFunc(statement, func(value string) string {
		match := mysqlInlineKeyPattern.FindStringSubmatch(value)
		columns := strings.Split(match[2], ",")
		for index, column := range columns {
			trimmed := strings.TrimSpace(column)
			name := strings.Trim(trimmed, "`")
			if _, ok := blobColumns[table][name]; ok {
				columns[index] = trimmed + "(255)"
			}
		}
		return match[1] + " (" + strings.Join(columns, ",") + ")"
	})
}

func schemaTableName(statement string) string {
	parts := strings.Fields(statement)
	if len(parts) < 6 {
		return ""
	}
	return strings.Trim(parts[5], "`")
}

func translateMySQLIndexColumns(
	statement string,
	blobColumns map[string]map[string]struct{},
) string {
	match := mysqlIndexPattern.FindStringSubmatch(statement)
	if len(match) != 4 {
		return statement
	}
	columns := strings.Split(match[3], ",")
	for index, column := range columns {
		trimmed := strings.TrimSpace(column)
		name := strings.Trim(trimmed, "`")
		if _, ok := blobColumns[match[2]][name]; !ok {
			continue
		}
		columns[index] = trimmed + "(255)"
	}
	return match[1] + "(" + strings.Join(columns, ",") + ")"
}

// adoptSQLiteV1 recognizes the final unversioned SQLite schema from before the
// database/sql cutover. Older experimental layouts are deliberately not
// reconstructed here: v1alpha1 is the big-bang database/sql boundary, and
// the current production shape is the sole adoption contract.
func adoptSQLiteV1(
	ctx context.Context,
	conn *sql.Conn,
	dialect string,
) error {
	if dialect != "sqlite" {
		return fmt.Errorf("SQLite version-1 adoption called for %q", dialect)
	}
	// The GORM provider performed these repairs before AutoMigrate created
	// v1's unique indexes and cascade foreign keys.  CREATE ... IF NOT EXISTS
	// cannot repair an existing index or constraint, so keep the compatibility
	// work in the adoption path while the migration lock is held.
	if err := ensureSQLiteAdoptionColumns(ctx, conn); err != nil {
		return err
	}
	required := map[string][]string{
		"transaction": {
			"id", "hash", "block_hash", "metadata", "slot", "type",
			"fee", "collateral_fee", "ttl", "block_index", "valid",
		},
		"utxo": {
			"id", "transaction_id", "collateral_return_for_tx_id",
			"tx_id", "output_idx", "added_slot", "deleted_slot",
			"spent_at_tx_id", "referenced_by_tx_id", "collateral_by_tx_id",
		},
		"account": {
			"id", "credential_tag", "staking_key", "created_slot",
			"added_slot", "reward", "active",
		},
		"pool": {"id", "pool_key_hash"},
		"drep": {"id", "credential_tag", "credential"},
		"certs": {
			"id", "transaction_id", "certificate_id", "cert_index",
			"cert_type", "slot",
		},
		"tip":              {"id", "hash", "slot", "block_number"},
		"node_settings":    {"id", "storage_mode", "network"},
		"commit_timestamp": {"id", "timestamp"},
		"pool_stake_snapshot": {
			"id", "epoch", "snapshot_type", "pool_key_hash",
			"calculation_version",
		},
		"reward_live_stake": {
			"id", "credential_tag", "staking_key", "calculation_version",
		},
		"reward_snapshot": {
			"id", "epoch", "snapshot_type", "calculation_version",
		},
		"reward_account_output": {
			"id", "epoch", "credential_tag", "staking_key", "guarded",
		},
	}
	// Validate every table and column in the released v1 schema, not only the
	// handful of tables used by the adoption backfills below.  A partially
	// created or damaged database must never be marked migrated: the expand
	// phase intentionally uses IF NOT EXISTS and therefore cannot repair a
	// table that exists with an incompatible shape.
	expected, err := sqliteV1Columns()
	if err != nil {
		return err
	}
	for table, columns := range expected {
		for column := range columns {
			required[table] = append(required[table], column)
		}
	}
	for table, columns := range required {
		found, err := sqliteTableColumns(ctx, conn, table)
		if err != nil {
			return err
		}
		if len(found) == 0 {
			return fmt.Errorf(
				"%w: missing required table %q",
				ErrLegacySchema,
				table,
			)
		}
		var missing []string
		for _, column := range columns {
			if _, ok := found[column]; !ok {
				missing = append(missing, column)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf(
				"%w: table %q is missing columns %s",
				ErrLegacySchema,
				table,
				strings.Join(missing, ", "),
			)
		}
	}
	// Reference inputs used to be represented by the single
	// utxo.referenced_by_tx_id column. Preserve that legacy edge while adding
	// the many-to-many association required when multiple transactions reuse a
	// reference UTxO. Do this only after validating the full legacy contract so
	// unsupported schemas report ErrLegacySchema instead of a misleading SQL
	// "no such table/column" error.
	if _, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS utxo_reference_input (
    utxo_id INTEGER NOT NULL,
    transaction_hash BLOB NOT NULL,
    PRIMARY KEY (utxo_id, transaction_hash),
    FOREIGN KEY (utxo_id) REFERENCES utxo(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_utxo_reference_input_tx
    ON utxo_reference_input(transaction_hash);`); err != nil {
		return fmt.Errorf("create reference-input association table: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `
INSERT OR IGNORE INTO utxo_reference_input (utxo_id, transaction_hash)
SELECT id, referenced_by_tx_id FROM utxo
WHERE referenced_by_tx_id IS NOT NULL AND length(referenced_by_tx_id) > 0`); err != nil {
		return fmt.Errorf("adopt legacy reference inputs: %w", err)
	}
	if err := repairSQLiteV1Indexes(ctx, conn); err != nil {
		return err
	}
	if err := dedupeSQLiteV1Rows(ctx, conn); err != nil {
		return err
	}
	if err := purgeSQLiteV1Orphans(ctx, conn); err != nil {
		return err
	}
	if err := repairSQLiteV1ForeignKeys(ctx, conn); err != nil {
		return err
	}
	return backfillSQLiteV1CreatedSlot(ctx, conn)
}

// ensureSQLiteAdoptionColumns contains only additive columns known to have
// existed in supported pre-v1 databases.  The full contract is still checked
// below; unknown/damaged schemas are rejected rather than guessed at.
func ensureSQLiteAdoptionColumns(ctx context.Context, conn *sql.Conn) error {
	additions := []struct {
		table, column, definition string
	}{
		{"account", "created_slot", "INTEGER NOT NULL DEFAULT 0"},
		{"account", "credential_tag", "INTEGER NOT NULL DEFAULT 0"},
		{"account_reward_delta", "credential_tag", "INTEGER NOT NULL DEFAULT 0"},
		{"account_reward_delta", "tx_hash", "BLOB NOT NULL DEFAULT X''"},
		{"account_reward_delta", "previous_reward", "TEXT"},
		{"drep", "credential_tag", "INTEGER NOT NULL DEFAULT 0"},
		{"governance_vote", "voter_credential_tag", "INTEGER NOT NULL DEFAULT 0"},
		{"utxo", "transaction_id", "INTEGER"},
		{"utxo", "collateral_return_for_tx_id", "INTEGER"},
		{"plutus_data", "transaction_id", "INTEGER"},
		{"certs", "transaction_id", "INTEGER"},
		{"key_witness", "transaction_id", "INTEGER"},
		{"witness_scripts", "transaction_id", "INTEGER"},
		{"redeemer", "transaction_id", "INTEGER"},
		{"asset", "utxo_id", "INTEGER"},
		{"pool_registration", "pool_id", "INTEGER"},
		{"pool_retirement", "pool_id", "INTEGER"},
		{"pool_registration_owner", "pool_registration_id", "INTEGER"},
		{"pool_registration_relay", "pool_registration_id", "INTEGER"},
		{"move_instantaneous_rewards_reward", "mir_id", "INTEGER"},
	}
	for _, addition := range additions {
		exists, err := sqliteTableExists(ctx, conn, addition.table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		columns, err := sqliteTableColumns(ctx, conn, addition.table)
		if err != nil {
			return err
		}
		if _, ok := columns[addition.column]; ok {
			continue
		}
		if _, err := conn.ExecContext(ctx,
			"ALTER TABLE `"+addition.table+"` ADD COLUMN `"+addition.column+"` "+addition.definition,
		); err != nil {
			return fmt.Errorf("add %s.%s during adoption: %w", addition.table, addition.column, err)
		}
	}
	return nil
}

// sqliteV1Columns derives the released table/column contract from the
// immutable expand resource. Keeping this in one place avoids a second,
// inevitably drifting list of dozens of metadata columns.
func sqliteV1Columns() (map[string]map[string]struct{}, error) {
	statements, err := loadSQL("v1/sqlite/expand.sql")
	if err != nil {
		return nil, err
	}
	ret := make(map[string]map[string]struct{})
	for _, statement := range statements {
		upper := strings.ToUpper(strings.TrimSpace(statement))
		if !strings.HasPrefix(upper, "CREATE TABLE") {
			continue
		}
		open := strings.IndexByte(statement, '(')
		if open < 0 || !strings.HasSuffix(strings.TrimSpace(statement), ")") {
			return nil, fmt.Errorf("%w: malformed CREATE TABLE statement", ErrLegacySchema)
		}
		header := strings.Fields(strings.TrimSpace(statement[:open]))
		if len(header) == 0 {
			return nil, fmt.Errorf("%w: missing table name", ErrLegacySchema)
		}
		table := strings.Trim(header[len(header)-1], "`\"")
		body := strings.TrimSpace(statement[open+1:])
		body = strings.TrimSuffix(body, ")")
		columns := make(map[string]struct{})
		for _, definition := range splitSQLList(body) {
			definition = strings.TrimSpace(definition)
			if definition == "" {
				continue
			}
			first := strings.Fields(definition)
			if len(first) == 0 {
				continue
			}
			keyword := strings.ToUpper(strings.Trim(first[0], "`\""))
			switch keyword {
			case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK":
				continue
			}
			columns[strings.Trim(first[0], "`\"")] = struct{}{}
		}
		ret[table] = columns
	}
	return ret, nil
}

// splitSQLList splits a CREATE TABLE body on top-level commas while retaining
// commas nested in type declarations and constraints.
func splitSQLList(value string) []string {
	var ret []string
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(value); index++ {
		char := value[index]
		if quote != 0 {
			if char == quote {
				if index+1 < len(value) && value[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}
		switch char {
		case '\'', '"', '`':
			quote = char
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				ret = append(ret, value[start:index])
				start = index + 1
			}
		}
	}
	return append(ret, value[start:])
}

func repairSQLiteV1Indexes(ctx context.Context, conn *sql.Conn) error {
	// Legacy releases enforced a slot-less unique index on reward deltas.  It
	// cannot represent multiple NULL-hash credits for the same account (for
	// example MIR/POOLREAP rows), and normalizing those NULLs to the canonical
	// empty hash below would make the duplicate values collide immediately.
	// Remove that obsolete index before rewriting the data; the v1 expand phase
	// installs the slot-aware replacement afterwards.
	if exists, err := sqliteIndexExists(ctx, conn, "idx_account_reward_delta_w_tx_s"); err != nil {
		return err
	} else if exists {
		if _, err := conn.ExecContext(
			ctx,
			"DROP INDEX IF EXISTS `idx_account_reward_delta_w_tx_s`",
		); err != nil {
			return fmt.Errorf("drop legacy SQLite reward-delta index: %w", err)
		}
	}
	// Legacy reward deltas used NULL for credits without a source hash. Treat
	// that value as the canonical empty hash before the slot-aware unique index
	// is installed; SQLite otherwise permits every replay because NULLs do not
	// collide in a unique index.
	if _, err := conn.ExecContext(ctx,
		`UPDATE account_reward_delta SET tx_hash = X'' WHERE tx_hash IS NULL`); err != nil {
		return fmt.Errorf("normalize legacy reward delta hashes: %w", err)
	}
	if err := rewriteSQLiteRewardDeltaNotNull(ctx, conn); err != nil {
		return err
	}
	// Older releases created a non-unique hash_slot index. Merge duplicates
	// deterministically before replacing it: checkpoint flags are ORed, and
	// conflicting nonces are rejected rather than silently discarded.
	type nonceRow struct {
		id, slot    int64
		hash, nonce []byte
		checkpoint  bool
	}
	rows, err := conn.QueryContext(ctx,
		`SELECT id, hash, slot, nonce, is_checkpoint FROM block_nonce ORDER BY hash, slot, id`)
	if err != nil {
		return fmt.Errorf("inspect block_nonce duplicates: %w", err)
	}
	defer rows.Close()
	kept := make(map[string]nonceRow)
	var remove []int64
	for rows.Next() {
		var row nonceRow
		var checkpoint int64
		if err := rows.Scan(&row.id, &row.hash, &row.slot, &row.nonce, &checkpoint); err != nil {
			return fmt.Errorf("scan block_nonce duplicates: %w", err)
		}
		row.checkpoint = checkpoint != 0
		key := fmt.Sprintf("%x/%d", row.hash, row.slot)
		previous, exists := kept[key]
		if !exists {
			kept[key] = row
			continue
		}
		if len(previous.nonce) > 0 && len(row.nonce) > 0 && !bytes.Equal(previous.nonce, row.nonce) {
			return fmt.Errorf("conflicting block_nonce values for hash %x slot %d", row.hash, row.slot)
		}
		if len(previous.nonce) == 0 && len(row.nonce) > 0 {
			previous.nonce = row.nonce
		}
		previous.checkpoint = previous.checkpoint || row.checkpoint
		kept[key] = previous
		remove = append(remove, row.id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate block_nonce duplicates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close block_nonce cursor: %w", err)
	}
	for _, row := range kept {
		if _, err := conn.ExecContext(ctx,
			`UPDATE block_nonce SET nonce = ?, is_checkpoint = ? WHERE id = ?`,
			row.nonce, row.checkpoint, row.id); err != nil {
			return fmt.Errorf("merge block_nonce row: %w", err)
		}
	}
	for _, id := range remove {
		if _, err := conn.ExecContext(ctx, `DELETE FROM block_nonce WHERE id = ?`, id); err != nil {
			return fmt.Errorf("remove duplicate block_nonce row: %w", err)
		}
	}
	// Every index below either changed uniqueness/columns or was superseded by
	// a wider replacement in the final GORM schema. Drop it before v1 expand so
	// CREATE INDEX IF NOT EXISTS cannot preserve an incompatible definition.
	for _, legacy := range []struct {
		table string
		name  string
	}{
		{"block_nonce", "hash_slot"},
		{"account", "idx_account_staking_key"},
		{"drep", "idx_drep_credential"},
		{"governance_vote", "idx_vote_unique"},
		{"reward_live_stake", "idx_reward_live_stake_pool"},
		{"reward_account_output", "idx_reward_account_output_credential"},
		{"address_transaction", "idx_addr_tx_staking"},
	} {
		exists, err := sqliteIndexExistsOnTable(ctx, conn, legacy.table, legacy.name)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if _, err := conn.ExecContext(ctx, "DROP INDEX IF EXISTS `"+legacy.name+"`"); err != nil {
			return fmt.Errorf("drop legacy SQLite index %q: %w", legacy.name, err)
		}
	}
	if _, err := conn.ExecContext(
		ctx,
		"CREATE UNIQUE INDEX IF NOT EXISTS `hash_slot` ON `block_nonce`(`hash`,`slot`)",
	); err != nil {
		return fmt.Errorf("create block_nonce unique index: %w", err)
	}
	return nil
}

func rewriteSQLiteRewardDeltaNotNull(ctx context.Context, conn *sql.Conn) error {
	columns, err := sqliteTableColumnsInfo(ctx, conn, "account_reward_delta")
	if err != nil {
		return err
	}
	txHashInfo, ok := columns["tx_hash"]
	if !ok || txHashInfo.notNull {
		return nil
	}
	// SQLite cannot add NOT NULL to an existing column in place. Rebuild only
	// the released table shape, preserving primary IDs and every supported
	// value. Extra columns are rejected rather than silently discarded.
	expected := map[string]struct{}{
		"staking_key": {}, "credential_tag": {}, "tx_hash": {}, "amount": {},
		"previous_reward": {}, "id": {}, "added_slot": {}, "withdrawal": {},
	}
	for column := range columns {
		if _, ok := expected[column]; !ok {
			return fmt.Errorf("%w: account_reward_delta has unsupported column %q", ErrLegacySchema, column)
		}
	}
	credentialTag := "0"
	if _, ok := columns["credential_tag"]; ok {
		credentialTag = "credential_tag" //nolint:gosec // fixed SQL column identifier
	}
	previousReward := "NULL"
	if _, ok := columns["previous_reward"]; ok {
		previousReward = "previous_reward"
	}
	txHashExpr := "X''"
	if _, ok := columns["tx_hash"]; ok {
		txHashExpr = "tx_hash"
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin account_reward_delta rewrite: %w", err)
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return fmt.Errorf("rewrite account_reward_delta tx_hash constraint: %w", cause)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS account_reward_delta_v1`); err != nil {
		return rollback(err)
	}
	if _, err := tx.ExecContext(ctx, `
CREATE TABLE account_reward_delta_v1 (
 staking_key BLOB NOT NULL,
 credential_tag INTEGER NOT NULL DEFAULT 0,
 tx_hash BLOB NOT NULL,
 amount TEXT NOT NULL,
 previous_reward TEXT,
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 added_slot INTEGER NOT NULL,
 withdrawal NUMERIC NOT NULL DEFAULT FALSE
)`); err != nil {
		return rollback(err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO account_reward_delta_v1
 (staking_key, credential_tag, tx_hash, amount, previous_reward, id, added_slot, withdrawal)
SELECT staking_key, `+credentialTag+`, `+txHashExpr+`, amount, `+previousReward+`, id, added_slot, withdrawal
FROM account_reward_delta`,
	); err != nil {
		return rollback(err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE account_reward_delta`); err != nil {
		return rollback(err)
	}
	if _, err := tx.ExecContext(ctx,
		`ALTER TABLE account_reward_delta_v1 RENAME TO account_reward_delta`,
	); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit account_reward_delta rewrite: %w", err)
	}
	return nil
}

type sqliteColumnInfo struct {
	notNull bool
}

func sqliteTableColumnsInfo(
	ctx context.Context,
	conn *sql.Conn,
	table string,
) (map[string]sqliteColumnInfo, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA table_info(`"+table+"`)")
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite table %q: %w", table, err)
	}
	defer rows.Close()
	ret := make(map[string]sqliteColumnInfo)
	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultVal sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultVal, &primaryKey); err != nil {
			return nil, err
		}
		ret[name] = sqliteColumnInfo{notNull: notNull != 0}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}

func sqliteIndexExistsOnTable(
	ctx context.Context,
	conn *sql.Conn,
	table string,
	name string,
) (bool, error) {
	var found int
	err := conn.QueryRowContext(
		ctx,
		`SELECT 1 FROM pragma_index_list(?) WHERE name = ? LIMIT 1`,
		table,
		name,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect SQLite index %q on %s: %w", name, table, err)
	}
	return true, nil
}

type sqliteDuplicateSpec struct {
	table   string
	columns []string
	keep    string
}

// dedupeSQLiteV1Rows repairs duplicate data that would make v1's unique
// indexes fail.  Queries intentionally avoid DELETE self-subqueries, which
// are rejected by MySQL in the equivalent legacy path and are less portable
// than selecting IDs then deleting by primary key.
func dedupeSQLiteV1Rows(ctx context.Context, conn *sql.Conn) error {
	specs := []sqliteDuplicateSpec{
		{table: "pool_stake_snapshot", columns: []string{"epoch", "snapshot_type", "pool_key_hash"}, keep: "MAX"},
		{table: "reward_live_stake", columns: []string{"credential_tag", "staking_key"}, keep: "MIN"},
	}
	for _, spec := range specs {
		if exists, err := sqliteTableExists(ctx, conn, spec.table); err != nil {
			return err
		} else if !exists {
			continue
		}
		groupColumns := strings.Join(spec.columns, ", ")
		rows, err := conn.QueryContext(ctx,
			"SELECT "+groupColumns+" FROM `"+spec.table+"` GROUP BY "+groupColumns+" HAVING COUNT(*) > 1", //nolint:gosec // table and columns come from fixed migration specs
		)
		if err != nil {
			return fmt.Errorf("inspect duplicate %s rows: %w", spec.table, err)
		}
		groups, err := scanDuplicateGroups(rows, len(spec.columns))
		if err != nil {
			return fmt.Errorf("scan duplicate %s rows: %w", spec.table, err)
		}
		for _, values := range groups {
			where := make([]string, len(spec.columns))
			args := make([]any, len(spec.columns), len(spec.columns)+1)
			for index, column := range spec.columns {
				where[index] = "`" + column + "` = ?"
				args[index] = values[index]
			}
			var keepID int64
			query := "SELECT " + spec.keep + "(id) FROM `" + spec.table + "` WHERE " + strings.Join(where, " AND ")
			if err := conn.QueryRowContext(ctx, query, args...).Scan(&keepID); err != nil {
				return fmt.Errorf("select retained %s row: %w", spec.table, err)
			}
			deleteArgs := make([]any, len(args)+1)
			copy(deleteArgs, args)
			deleteArgs[len(args)] = keepID
			if _, err := conn.ExecContext(ctx,
				"DELETE FROM `"+spec.table+"` WHERE "+strings.Join(where, " AND ")+" AND id <> ?", //nolint:gosec // identifiers come from fixed migration specs
				deleteArgs...,
			); err != nil { //nolint:gosec // table and columns come from fixed migration specs
				return fmt.Errorf("deduplicate %s rows: %w", spec.table, err)
			}
		}
	}
	return nil
}

func scanDuplicateGroups(rows *sql.Rows, width int) ([][]any, error) {
	defer rows.Close()
	groups := make([][]any, 0)
	for rows.Next() {
		values := make([]any, width)
		dest := make([]any, width)
		for index := range values {
			dest[index] = &values[index]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		groups = append(groups, values)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groups, nil
}

func sqliteTableExists(ctx context.Context, conn *sql.Conn, table string) (bool, error) {
	var found int
	err := conn.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect SQLite table %q: %w", table, err)
	}
	return true, nil
}

func purgeSQLiteV1Orphans(ctx context.Context, conn *sql.Conn) error {
	type orphanSpec struct {
		child, parent, column string
		nullable              bool
	}
	specs := []orphanSpec{
		{"utxo", "transaction", "transaction_id", true},
		{"utxo", "transaction", "collateral_return_for_tx_id", true},
		{"plutus_data", "transaction", "transaction_id", false},
		{"certs", "transaction", "transaction_id", false},
		{"key_witness", "transaction", "transaction_id", false},
		{"witness_scripts", "transaction", "transaction_id", false},
		{"redeemer", "transaction", "transaction_id", false},
		{"asset", "utxo", "utxo_id", false},
		{"pool_registration", "pool", "pool_id", false},
		{"pool_retirement", "pool", "pool_id", false},
		{"pool_registration_owner", "pool_registration", "pool_registration_id", false},
		{"pool_registration_relay", "pool_registration", "pool_registration_id", false},
		{"move_instantaneous_rewards_reward", "move_instantaneous_rewards", "mir_id", false},
	}
	for _, spec := range specs {
		childExists, err := sqliteTableExists(ctx, conn, spec.child)
		if err != nil {
			return err
		}
		if !childExists {
			continue
		}
		parentExists, err := sqliteTableExists(ctx, conn, spec.parent)
		if err != nil {
			return err
		}
		if !parentExists {
			return fmt.Errorf("%w: orphan parent table %q is missing", ErrLegacySchema, spec.parent)
		}
		columns, err := sqliteTableColumns(ctx, conn, spec.child)
		if err != nil {
			return err
		}
		if _, ok := columns[spec.column]; !ok {
			continue
		}
		predicate := "`" + spec.column + "` IS NOT NULL AND "
		if !spec.nullable {
			predicate = ""
		}
		query := "DELETE FROM `" + spec.child + "` WHERE " + predicate + //nolint:gosec // identifiers come from fixed migration specs
			"NOT EXISTS (SELECT 1 FROM `" + spec.parent + "` p WHERE p.id = `" + spec.child + "`.`" + spec.column + "`)"
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("purge orphan %s.%s rows: %w", spec.child, spec.column, err)
		}
	}
	return nil
}

type sqliteForeignKeySpec struct {
	child, column  string
	parent, target string
	onDelete       string
}

var sqliteV1ForeignKeys = []sqliteForeignKeySpec{
	{child: "utxo", column: "collateral_return_for_tx_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
	{child: "utxo", column: "transaction_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
	{child: "utxo", column: "collateral_by_tx_id", parent: "transaction", target: "hash", onDelete: "NO ACTION"},
	{child: "utxo", column: "spent_at_tx_id", parent: "transaction", target: "hash", onDelete: "NO ACTION"},
	{child: "utxo", column: "referenced_by_tx_id", parent: "transaction", target: "hash", onDelete: "NO ACTION"},
	{child: "utxo_reference_input", column: "utxo_id", parent: "utxo", target: "id", onDelete: "CASCADE"},
	{child: "asset", column: "utxo_id", parent: "utxo", target: "id", onDelete: "CASCADE"},
	{child: "certs", column: "transaction_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
	{child: "key_witness", column: "transaction_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
	{child: "move_instantaneous_rewards_reward", column: "mir_id", parent: "move_instantaneous_rewards", target: "id", onDelete: "CASCADE"},
	{child: "pool_registration", column: "pool_id", parent: "pool", target: "id", onDelete: "CASCADE"},
	{child: "pool_registration_owner", column: "pool_id", parent: "pool", target: "id", onDelete: "NO ACTION"},
	{child: "pool_registration_owner", column: "pool_registration_id", parent: "pool_registration", target: "id", onDelete: "CASCADE"},
	{child: "pool_registration_relay", column: "pool_id", parent: "pool", target: "id", onDelete: "NO ACTION"},
	{child: "pool_registration_relay", column: "pool_registration_id", parent: "pool_registration", target: "id", onDelete: "CASCADE"},
	{child: "pool_retirement", column: "pool_id", parent: "pool", target: "id", onDelete: "CASCADE"},
	{child: "plutus_data", column: "transaction_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
	{child: "redeemer", column: "transaction_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
	{child: "witness_scripts", column: "transaction_id", parent: "transaction", target: "id", onDelete: "CASCADE"},
}

func repairSQLiteV1ForeignKeys(ctx context.Context, conn *sql.Conn) error {
	byTable := make(map[string][]sqliteForeignKeySpec)
	for _, spec := range sqliteV1ForeignKeys {
		byTable[spec.child] = append(byTable[spec.child], spec)
	}
	var rebuild []string
	for table, expected := range byTable {
		exists, err := sqliteTableExists(ctx, conn, table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		actual, err := sqliteForeignKeys(ctx, conn, table)
		if err != nil {
			return err
		}
		if sameSQLiteForeignKeys(actual, expected) {
			continue
		}
		if hasUnexpectedSQLiteForeignKeys(actual, expected) {
			return fmt.Errorf("%w: table %q has unsupported foreign-key definitions", ErrLegacySchema, table)
		}
		rebuild = append(rebuild, table)
	}
	if len(rebuild) == 0 {
		return nil
	}
	// SQLite cannot toggle foreign_keys inside a transaction. Disable it only
	// around this short, locked rebuild and restore the prior setting after the
	// transaction commits or rolls back.
	var foreignKeys int
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("inspect SQLite foreign-key enforcement: %w", err)
	}
	if foreignKeys != 0 {
		if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
			return fmt.Errorf("disable SQLite foreign-key enforcement for adoption: %w", err)
		}
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		if foreignKeys != 0 {
			_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		}
		return fmt.Errorf("begin SQLite foreign-key adoption: %w", err)
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		if foreignKeys != 0 {
			_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		}
		return fmt.Errorf("rebuild SQLite foreign keys: %w", cause)
	}
	for _, table := range rebuild {
		if err := rebuildSQLiteV1Table(ctx, tx, table); err != nil {
			return rollback(err)
		}
	}
	if err := tx.Commit(); err != nil {
		if foreignKeys != 0 {
			_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		}
		return fmt.Errorf("commit SQLite foreign-key adoption: %w", err)
	}
	if foreignKeys != 0 {
		if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
			return fmt.Errorf("restore SQLite foreign-key enforcement: %w", err)
		}
	}
	return nil
}

type sqliteForeignKey struct {
	column, parent, target, onDelete string
}

func sqliteForeignKeys(
	ctx context.Context,
	conn *sql.Conn,
	table string,
) ([]sqliteForeignKey, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA foreign_key_list(`"+table+"`)") //nolint:gosec // table comes from fixed migration specs
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite foreign keys on %s: %w", table, err)
	}
	defer rows.Close()
	ret := make([]sqliteForeignKey, 0)
	for rows.Next() {
		var (
			id, sequence                                      int
			parent, column, target, onUpdate, onDelete, match string
		)
		if err := rows.Scan(&id, &sequence, &parent, &column, &target, &onUpdate, &onDelete, &match); err != nil {
			return nil, fmt.Errorf("scan SQLite foreign keys on %s: %w", table, err)
		}
		ret = append(ret, sqliteForeignKey{
			column: column, parent: parent, target: target,
			onDelete: strings.ToUpper(onDelete),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite foreign keys on %s: %w", table, err)
	}
	return ret, nil
}

func sameSQLiteForeignKeys(actual []sqliteForeignKey, expected []sqliteForeignKeySpec) bool {
	if len(actual) != len(expected) {
		return false
	}
	matched := make([]bool, len(expected))
	for _, row := range actual {
		found := false
		for index, spec := range expected {
			if matched[index] || row.column != spec.column ||
				row.parent != spec.parent || row.target != spec.target ||
				row.onDelete != spec.onDelete {
				continue
			}
			matched[index] = true
			found = true
			break
		}
		if !found {
			return false
		}
	}
	return true
}

func hasUnexpectedSQLiteForeignKeys(actual []sqliteForeignKey, expected []sqliteForeignKeySpec) bool {
	for _, row := range actual {
		matched := false
		for _, spec := range expected {
			if row.column == spec.column && row.parent == spec.parent &&
				row.target == spec.target && row.onDelete == spec.onDelete {
				matched = true
				break
			}
		}
		if !matched {
			return true
		}
	}
	return false
}

func rebuildSQLiteV1Table(ctx context.Context, tx *sql.Tx, table string) error {
	statements, err := loadSQL("v1/sqlite/expand.sql")
	if err != nil {
		return err
	}
	var create string
	prefix := "CREATE TABLE IF NOT EXISTS `" + table + "`"
	for _, statement := range statements {
		if strings.HasPrefix(statement, prefix) {
			create = statement
			break
		}
	}
	if create == "" {
		return fmt.Errorf("%w: no v1 CREATE TABLE statement for %q", ErrLegacySchema, table)
	}
	columns, err := sqliteTableColumnNamesTx(ctx, tx, table)
	if err != nil {
		return err
	}
	contract, err := sqliteV1Columns()
	if err != nil {
		return err
	}
	allowed := contract[table]
	for _, column := range columns {
		if _, ok := allowed[column]; !ok {
			return fmt.Errorf(
				"%w: table %q has unsupported column %q",
				ErrLegacySchema,
				table,
				column,
			)
		}
	}
	temp := table + "__v1_fk"
	recreated := "CREATE TABLE `" + temp + "`" + strings.TrimPrefix(create, prefix) //nolint:gosec // temp/table names are derived from validated v1 schema
	var tempExists int
	if err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, temp,
	).Scan(&tempExists); err == nil {
		return fmt.Errorf("%w: stale SQLite FK rebuild table %q exists", ErrLegacySchema, temp)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("inspect stale SQLite FK rebuild table %q: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, recreated); err != nil {
		return fmt.Errorf("create SQLite FK rebuild table %q: %w", table, err)
	}
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = "`" + strings.ReplaceAll(column, "`", "``") + "`"
	}
	columnList := strings.Join(quoted, ", ")
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO `"+temp+"` ("+columnList+") SELECT "+columnList+" FROM `"+table+"`", //nolint:gosec // table/columns come from validated v1 schema
	); err != nil {
		return fmt.Errorf("copy %s rows during SQLite FK rebuild: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE `"+table+"`"); err != nil {
		return fmt.Errorf("drop old SQLite FK table %q: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, "ALTER TABLE `"+temp+"` RENAME TO `"+table+"`"); err != nil {
		return fmt.Errorf("rename SQLite FK table %q: %w", table, err)
	}
	return nil
}

func sqliteTableColumnNamesTx(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info(`"+table+"`)") //nolint:gosec // table comes from fixed migration specs
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite columns on %s: %w", table, err)
	}
	defer rows.Close()
	ret := make([]string, 0)
	for rows.Next() {
		var (
			cid, notNull, primaryKey int
			name, columnType         string
			defaultValue             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan SQLite columns on %s: %w", table, err)
		}
		ret = append(ret, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite columns on %s: %w", table, err)
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("%w: table %q has no columns", ErrLegacySchema, table)
	}
	return ret, nil
}

const sqliteCreatedSlotBackfillPhase = "account_created_slot"

// backfillSQLiteV1CreatedSlot is intentionally checkpointed in the legacy
// backfill_checkpoint table.  It runs during adoption, before schema_migrations
// version 1 is recorded, and therefore remains resumable if a process exits
// between batches.  Each batch commits account updates and its cursor together.
func backfillSQLiteV1CreatedSlot(ctx context.Context, conn *sql.Conn) error {
	if exists, err := sqliteTableExists(ctx, conn, "account"); err != nil {
		return err
	} else if !exists {
		return nil
	}
	if exists, err := sqliteTableExists(ctx, conn, "backfill_checkpoint"); err != nil {
		return err
	} else if !exists {
		if _, err := conn.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS backfill_checkpoint (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 phase TEXT NOT NULL,
 last_slot INTEGER,
 total_slots INTEGER,
 started_at DATETIME,
 updated_at DATETIME,
 completed NUMERIC
)`); err != nil {
			return fmt.Errorf("create account created-slot checkpoint: %w", err)
		}
	}
	// Older checkpoint tables were not always created with the phase unique
	// index. Keep one deterministic row per phase before v1 creates it.
	if _, err := conn.ExecContext(ctx, `
DELETE FROM backfill_checkpoint
WHERE id NOT IN (
 SELECT MIN(id) FROM backfill_checkpoint GROUP BY phase
)`); err != nil {
		return fmt.Errorf("deduplicate account backfill checkpoints: %w", err)
	}
	var completed bool
	var cursor sql.NullInt64
	err := conn.QueryRowContext(ctx,
		`SELECT completed, last_slot FROM backfill_checkpoint WHERE phase = ? LIMIT 1`,
		sqliteCreatedSlotBackfillPhase,
	).Scan(&completed, &cursor)
	if err == nil && completed {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read account created-slot checkpoint: %w", err)
	}
	lastID := int64(0)
	if cursor.Valid {
		lastID = cursor.Int64
	}
	if _, err := conn.ExecContext(ctx, `
INSERT OR IGNORE INTO backfill_checkpoint (phase, last_slot, started_at, updated_at, completed)
VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, FALSE)`,
		sqliteCreatedSlotBackfillPhase, lastID); err != nil {
		return fmt.Errorf("initialize account created-slot checkpoint: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		`UPDATE backfill_checkpoint SET updated_at = CURRENT_TIMESTAMP WHERE phase = ?`,
		sqliteCreatedSlotBackfillPhase,
	); err != nil {
		return fmt.Errorf("refresh account created-slot checkpoint: %w", err)
	}
	registrationTables := []string{
		"stake_registration",
		"stake_registration_delegation",
		"stake_vote_registration_delegation",
		"vote_registration_delegation",
		"registration",
	}
	genesisTables := []string{
		"registration", "stake_delegation", "stake_vote_delegation", "vote_delegation",
	}
	registrationTables, err = existingSQLiteTables(ctx, conn, registrationTables)
	if err != nil {
		return err
	}
	genesisTables, err = existingSQLiteTables(ctx, conn, genesisTables)
	if err != nil {
		return err
	}
	const batchSize = 400
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin account created-slot batch: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
SELECT id, credential_tag, staking_key
FROM account
WHERE id > ? AND staking_key IS NOT NULL
ORDER BY id
LIMIT ?`, lastID, batchSize)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("scan account created-slot batch: %w", err)
		}
		type accountKey struct {
			id  int64
			tag int64
			key []byte
		}
		accounts := make([]accountKey, 0, batchSize)
		scanErr := func() error {
			defer rows.Close()
			for rows.Next() {
				var account accountKey
				if err := rows.Scan(&account.id, &account.tag, &account.key); err != nil {
					return fmt.Errorf("scan account created-slot row: %w", err)
				}
				accounts = append(accounts, account)
			}
			return rows.Err()
		}()
		if scanErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("iterate account created-slot rows: %w", scanErr)
		}
		for _, account := range accounts {
			var minimum sql.NullInt64
			for _, table := range registrationTables {
				var slot sql.NullInt64
				err := tx.QueryRowContext(ctx,
					"SELECT MIN(added_slot) FROM `"+table+"` WHERE credential_tag = ? AND staking_key = ?",
					account.tag, account.key).Scan(&slot)
				if err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("read %s registration history: %w", table, err)
				}
				if slot.Valid && (!minimum.Valid || slot.Int64 < minimum.Int64) {
					minimum = slot
				}
			}
			if !minimum.Valid || minimum.Int64 == 0 {
				continue
			}
			var genesis bool
			for _, table := range genesisTables {
				var found int
				err := tx.QueryRowContext(ctx,
					"SELECT 1 FROM `"+table+"` WHERE credential_tag = ? AND staking_key = ? AND added_slot = 0 LIMIT 1",
					account.tag, account.key).Scan(&found)
				if err == nil {
					genesis = true
					break
				}
				if !errors.Is(err, sql.ErrNoRows) {
					_ = tx.Rollback()
					return fmt.Errorf("read %s genesis evidence: %w", table, err)
				}
			}
			if genesis {
				continue
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE account SET created_slot = ? WHERE id = ? AND (created_slot = 0 OR created_slot > ?) AND certificate_id <> 0`,
				minimum.Int64, account.id, minimum.Int64); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("update account created_slot: %w", err)
			}
		}
		if len(accounts) == 0 {
			if _, err := tx.ExecContext(ctx, `
UPDATE backfill_checkpoint SET completed = TRUE, updated_at = CURRENT_TIMESTAMP WHERE phase = ?`, sqliteCreatedSlotBackfillPhase); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("complete account created-slot checkpoint: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit account created-slot completion: %w", err)
			}
			return nil
		}
		lastID = accounts[len(accounts)-1].id
		if _, err := tx.ExecContext(ctx, `
UPDATE backfill_checkpoint SET last_slot = ?, updated_at = CURRENT_TIMESTAMP WHERE phase = ?`, lastID, sqliteCreatedSlotBackfillPhase); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("checkpoint account created-slot batch: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit account created-slot batch: %w", err)
		}
	}
}

func existingSQLiteTables(ctx context.Context, conn *sql.Conn, tables []string) ([]string, error) {
	existing := make([]string, 0, len(tables))
	for _, table := range tables {
		ok, err := sqliteTableExists(ctx, conn, table)
		if err != nil {
			return nil, err
		}
		if ok {
			existing = append(existing, table)
		}
	}
	return existing, nil
}

func sqliteIndexExists(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	var found int
	err := conn.QueryRowContext(
		ctx,
		`SELECT 1 FROM pragma_index_list(?) WHERE name = ? LIMIT 1`,
		"block_nonce", name,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		// Account and reward-delta indexes live on different tables. Query the
		// catalog directly so the caller need not carry table-specific names.
		for _, table := range []string{"account", "account_reward_delta"} {
			err = conn.QueryRowContext(
				ctx,
				`SELECT 1 FROM pragma_index_list(?) WHERE name = ? LIMIT 1`,
				table,
				name,
			).Scan(&found)
			if err == nil {
				return true, nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return false, fmt.Errorf("inspect SQLite index %q: %w", name, err)
			}
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect SQLite index %q: %w", name, err)
	}
	return true, nil
}

func sqliteTableColumns(
	ctx context.Context,
	conn *sql.Conn,
	table string,
) (map[string]struct{}, error) {
	rows, err := conn.QueryContext(
		ctx,
		`SELECT name FROM pragma_table_info(?)`,
		table,
	)
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite table %q: %w", table, err)
	}
	defer rows.Close()
	ret := make(map[string]struct{})
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		ret[column] = struct{}{}
	}
	return ret, rows.Err()
}

func loadSQL(path string) ([]string, error) {
	content, err := migrationSQL.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read embedded migration %s: %w", path, err)
	}
	statements, err := splitSQL(string(content))
	if err != nil {
		return nil, fmt.Errorf("parse embedded migration %s: %w", path, err)
	}
	return statements, nil
}

// splitSQL is intentionally small and strict. Migration resources may contain
// semicolons in quoted values and comments, but not backend client directives.
func splitSQL(content string) ([]string, error) {
	var (
		statements   []string
		current      []byte
		quote        byte
		lineComment  bool
		blockComment bool
	)
	flush := func() {
		statement := string(current)
		current = nil
		for len(statement) > 0 &&
			(statement[0] == ' ' || statement[0] == '\n' ||
				statement[0] == '\r' || statement[0] == '\t') {
			statement = statement[1:]
		}
		for len(statement) > 0 {
			last := statement[len(statement)-1]
			if last != ' ' && last != '\n' && last != '\r' && last != '\t' {
				break
			}
			statement = statement[:len(statement)-1]
		}
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	for idx := 0; idx < len(content); idx++ {
		character := content[idx]
		if lineComment {
			if character == '\n' {
				lineComment = false
				current = append(current, character)
			}
			continue
		}
		if blockComment {
			if character == '*' && idx+1 < len(content) &&
				content[idx+1] == '/' {
				blockComment = false
				idx++
			}
			continue
		}
		if quote != 0 {
			current = append(current, character)
			if character == quote {
				if idx+1 < len(content) && content[idx+1] == quote {
					current = append(current, content[idx+1])
					idx++
				} else {
					quote = 0
				}
			}
			continue
		}
		if character == '-' && idx+1 < len(content) &&
			content[idx+1] == '-' {
			appendCommentSpace(&current)
			lineComment = true
			idx++
			continue
		}
		if character == '/' && idx+1 < len(content) &&
			content[idx+1] == '*' {
			appendCommentSpace(&current)
			blockComment = true
			idx++
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
			current = append(current, character)
		case ';':
			flush()
		default:
			current = append(current, character)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %q string", quote)
	}
	if blockComment {
		return nil, errors.New("unterminated block comment")
	}
	flush()
	return statements, nil
}

func appendCommentSpace(current *[]byte) {
	if len(*current) == 0 {
		return
	}
	last := (*current)[len(*current)-1]
	if last != ' ' && last != '\n' && last != '\r' && last != '\t' {
		*current = append(*current, ' ')
	}
}
