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
	return func(
		context.Context,
		*sql.Conn,
		string,
	) error {
		return fmt.Errorf(
			"%w: unversioned %s databases require an explicit export/import",
			ErrLegacySchema,
			dialect,
		)
	}
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
	return repairSQLiteV1Indexes(ctx, conn)
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
	// Older releases created a non-unique hash_slot index. Deduplicate before
	// replacing it so the v1 ON CONFLICT (hash, slot) upsert is guaranteed to
	// have a matching unique constraint.
	if _, err := conn.ExecContext(ctx, `
DELETE FROM block_nonce
WHERE id NOT IN (
    SELECT MIN(id) FROM block_nonce GROUP BY hash, slot
)`); err != nil {
		return fmt.Errorf("repair block_nonce duplicates: %w", err)
	}
	for _, index := range []string{
		"hash_slot",
		"idx_account_staking_key",
		"idx_account_reward_delta_w_tx_s",
	} {
		exists, err := sqliteIndexExists(ctx, conn, index)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if _, err := conn.ExecContext(
			ctx,
			"DROP INDEX IF EXISTS `"+index+"`",
		); err != nil {
			return fmt.Errorf("drop legacy SQLite index %q: %w", index, err)
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
			lineComment = true
			idx++
			continue
		}
		if character == '/' && idx+1 < len(content) &&
			content[idx+1] == '*' {
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
