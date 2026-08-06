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

const initialSchemaRelease = "v1alpha1"

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
		Name:             initialSchemaRelease,
		BackfillRevision: "none",
		SQL: map[string]SQL{
			dialect: sqlForDialect,
		},
	}}, nil
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
