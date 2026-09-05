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
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

// migrationSQL contains immutable, versioned migration resources.
//
//go:embed v*/*/*.sql
var migrationSQL embed.FS

const (
	initialSchemaRelease                       = "v1alpha1"
	leiosKeySchemaRelease                      = "leios-key-registration"
	tokenRegistrySchemaRelease                 = "token-registry-metadata"
	accountBaselineSchemaRelease               = "account-import-baseline"
	leiosSnapshotKeySchemaRelease              = "leios-snapshot-keys"
	governanceRatificationHistorySchemaRelease = "governance-ratification-history"
	accountDepositSchemaRelease                = "account-import-deposit"
	committeeCredentialTagsSchemaRelease       = "committee-credential-tags"
	committeeTermStartPresenceSchemaRelease    = "committee-term-start-presence"
	pointerAddressStakeSchemaRelease           = "pointer-address-stake"
)

// schemaVersions names every migration in ascending version order.
var schemaVersions = []struct {
	Version int
	Name    string
	Dir     string
}{
	{Version: 1, Name: initialSchemaRelease, Dir: "v1"},
	{Version: 2, Name: leiosKeySchemaRelease, Dir: "v2"},
	{Version: 3, Name: tokenRegistrySchemaRelease, Dir: "v3"},
	{Version: 4, Name: accountBaselineSchemaRelease, Dir: "v4"},
	{Version: 5, Name: leiosSnapshotKeySchemaRelease, Dir: "v5"},
	{
		Version: 6,
		Name:    governanceRatificationHistorySchemaRelease,
		Dir:     "v6",
	},
	{Version: 7, Name: accountDepositSchemaRelease, Dir: "v7"},
	{Version: 8, Name: committeeCredentialTagsSchemaRelease, Dir: "v8"},
	{
		Version: 9,
		Name:    committeeTermStartPresenceSchemaRelease,
		Dir:     "v9",
	},
	{Version: 10, Name: pointerAddressStakeSchemaRelease, Dir: "v10"},
}

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
	loaded := make([]SQL, 0, len(schemaVersions))
	// Every CREATE TABLE lives in the initial version, and MySQL's translation
	// reads them to learn which columns are blobs. A later version indexing an
	// existing blob column carries no CREATE TABLE of its own, so it is
	// translated against the schema as a whole rather than against its own
	// statements -- otherwise MySQL would be handed a BLOB key with no prefix
	// length, which it rejects.
	var wholeSchema []string
	for _, version := range schemaVersions {
		expand, err := loadSQL(version.Dir + "/sqlite/expand.sql")
		if err != nil {
			return nil, err
		}
		contract, err := loadOptionalSQL(version.Dir + "/sqlite/contract.sql")
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, SQL{Expand: expand, Contract: contract})
		wholeSchema = append(wholeSchema, expand...)
		wholeSchema = append(wholeSchema, contract...)
	}

	ret := make([]Migration, 0, len(schemaVersions))
	for index, version := range schemaVersions {
		sqlForDialect := loaded[index]
		if dialect != "sqlite" {
			sqlForDialect.Expand = translateSchemaSQLInSchema(
				loaded[index].Expand,
				dialect,
				wholeSchema,
			)
			sqlForDialect.Contract = translateSchemaSQLInSchema(
				loaded[index].Contract,
				dialect,
				wholeSchema,
			)
		}
		migration := Migration{
			Version:          version.Version,
			Name:             version.Name,
			BackfillRevision: "none",
			SQL: map[string]SQL{
				dialect: sqlForDialect,
			},
		}
		if version.Name == committeeTermStartPresenceSchemaRelease {
			migration.BackfillRevision = "1"
			migration.Backfill = committeeTermStartBackfill
		}
		ret = append(ret, migration)
	}
	return ret, nil
}

// committeeTermStartBackfill is deliberately data-driven rather than a
// migration SQL statement. The expand phase can therefore be retried after a
// process dies immediately after adding the column; rows are selected by ID
// and each committed batch advances the durable migration cursor.
func committeeTermStartBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	lastID := int64(0)
	if batch.Cursor != "" {
		parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"parse committee backfill cursor: %w",
				err,
			)
		}
		lastID = parsed
	}
	rows, err := batch.Tx.QueryContext(ctx,
		batch.Rebind(
			"SELECT id FROM committee_member WHERE id > ? AND NOT term_start_slot_set ORDER BY id LIMIT ?",
		),
		lastID, batch.Limit,
	)
	if err != nil {
		return BatchResult{}, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return BatchResult{}, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return BatchResult{}, err
	}
	if len(ids) == 0 {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	for _, id := range ids {
		if _, err := batch.Tx.ExecContext(ctx,
			batch.Rebind(
				"UPDATE committee_member SET term_start_slot_set = TRUE WHERE id = ?",
			),
			id,
		); err != nil {
			return BatchResult{}, err
		}
	}
	return BatchResult{
		Cursor: strconv.FormatInt(ids[len(ids)-1], 10),
		Rows:   int64(len(ids)),
	}, nil
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

// translateSchemaSQLInSchema translates statements for a dialect, deriving
// MySQL's column typing from schemaStatements rather than from the statements
// being translated. The two differ for any migration that alters a table it
// did not create; see registryForDialect.
func translateSchemaSQLInSchema(
	statements []string,
	dialect string,
	schemaStatements []string,
) []string {
	translated := make([]string, len(statements))
	mysqlBlobColumns := make(map[string]map[string]struct{})
	mysqlForeignKeyColumns := make(map[string]map[string]struct{})
	if dialect == "mysql" {
		for _, statement := range schemaStatements {
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
			// SQLite INTEGER is used for metadata IDs and foreign-key columns.
			// Widen all integer columns so PostgreSQL foreign keys retain matching
			// BIGINT types after the AUTOINCREMENT rewrite above.
			value = wordType("integer").ReplaceAllString(value, "BIGINT")
			value = wordType("blob").ReplaceAllString(value, "BYTEA")
			value = wordType("datetime").ReplaceAllString(value, "TIMESTAMPTZ")
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
			value = strings.ReplaceAll(
				value,
				"CREATE INDEX IF NOT EXISTS",
				"CREATE INDEX",
			)
			value = strings.ReplaceAll(
				value,
				"CREATE UNIQUE INDEX IF NOT EXISTS",
				"CREATE UNIQUE INDEX",
			)
			value = strings.ReplaceAll(
				value,
				"DROP INDEX IF EXISTS `idx_committee_member_cold_cred_hash`",
				"DROP INDEX `idx_committee_member_cold_cred_hash` ON `committee_member`",
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
	return mysqlInlineKeyPattern.ReplaceAllStringFunc(
		statement,
		func(value string) string {
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
		},
	)
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

// loadOptionalSQL loads a migration resource that a version need not ship,
// returning no statements when the file is absent. Only a missing file is
// tolerated: an unreadable or unparseable one is still an error.
//
// Existence is tested with fs.Stat rather than Open so the probe does not open
// a handle the caller then has to remember to close.
func loadOptionalSQL(path string) ([]string, error) {
	if _, err := fs.Stat(migrationSQL, path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read embedded migration %s: %w", path, err)
	}
	return loadSQL(path)
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
		statement := strings.TrimSpace(string(current))
		current = nil
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
		if character == '-' && idx+1 < len(content) && content[idx+1] == '-' {
			appendCommentSpace(&current)
			lineComment = true
			idx++
			continue
		}
		if character == '/' && idx+1 < len(content) && content[idx+1] == '*' {
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
