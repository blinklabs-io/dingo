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

//go:build !dingo_extra_plugins

package migrations

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
)

// Keep the default SQLite build free of optional MySQL driver dependencies.
func isMySQLDDLAlreadyApplied(context.Context, *sql.Conn, string, error) bool {
	return false
}

var mysqlDuplicateDDLPattern = regexp.MustCompile(
	"(?is)^CREATE\\s+(?:UNIQUE\\s+)?INDEX(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?\\s+ON\\s+[`\\\"]?([a-zA-Z0-9_]+)",
)
var mysqlIndexDefinitionPatternDefault = regexp.MustCompile(
	"(?is)^CREATE\\s+(UNIQUE\\s+)?INDEX(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?\\s+ON\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?\\s*\\((.*)\\)$",
)
var mysqlIndexSortPatternDefault = regexp.MustCompile(`(?i)\s+(?:ASC|DESC)\b`)
var mysqlIndexPrefixPatternDefault = regexp.MustCompile(`\(\d+\)?`)

// isMySQLDDLAlreadyAppliedOnConn is deliberately driver-independent. The
// default build cannot import go-sql-driver/mysql, but migration setup still
// runs against MySQL in tagged integration tests. MySQL exposes duplicate
// index errors as text through database/sql; verify that the named index is
// present before treating the error as an idempotent replay.
func isMySQLDDLAlreadyAppliedOnConn(
	ctx context.Context,
	conn *sql.Conn,
	statement string,
	err error,
) bool {
	if conn == nil || err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	// 1060 is a duplicate column, which an ADD COLUMN expand statement raises
	// when the phase replays after a crash between the DDL and the phase
	// advance. 1061/1826 are duplicate index/constraint.
	if strings.Contains(message, "error 1060") {
		return mysqlColumnAlreadyPresentDefault(ctx, conn, statement)
	}
	if !strings.Contains(message, "error 1061") &&
		!strings.Contains(message, "error 1826") {
		return false
	}
	definition := mysqlIndexDefinitionPatternDefault.FindStringSubmatch(
		strings.TrimSpace(statement),
	)
	if len(definition) != 5 {
		match := mysqlDuplicateDDLPattern.FindStringSubmatch(
			strings.TrimSpace(statement),
		)
		if len(match) != 3 {
			return false
		}
		return mysqlIndexExistsDefault(ctx, conn, match[2], match[1])
	}
	var actual sql.NullString
	if err := conn.QueryRowContext(ctx, `
SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',')
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		definition[3], definition[2]).Scan(&actual); err != nil || !actual.Valid {
		return false
	}
	requested := normalizeMySQLIndexColumnsDefault(definition[4])
	if requested != normalizeMySQLIndexColumnsDefault(actual.String) {
		return false
	}
	var nonUnique int
	if err := conn.QueryRowContext(ctx, `
SELECT non_unique
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
LIMIT 1`, definition[3], definition[2]).Scan(&nonUnique); err != nil {
		return false
	}
	return (definition[1] != "") == (nonUnique == 0)
}

// mysqlColumnAlreadyPresentDefault confirms the column an ADD COLUMN
// statement names is already in the schema before the duplicate-column error
// is treated as an idempotent replay.
func mysqlColumnAlreadyPresentDefault(
	ctx context.Context,
	conn *sql.Conn,
	statement string,
) bool {
	table, column, definition, ok := parseAddColumnStatement(statement)
	if !ok {
		return false
	}
	var reported sql.NullString
	return conn.QueryRowContext(ctx, `
SELECT data_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
LIMIT 1`, table, column).Scan(&reported) == nil &&
		mysqlColumnTypeMatches(reported, definition)
}

func mysqlIndexExistsDefault(
	ctx context.Context,
	conn *sql.Conn,
	table, name string,
) bool {
	var exists int
	return conn.QueryRowContext(ctx, `
SELECT 1
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
LIMIT 1`, table, name).Scan(&exists) == nil && exists == 1
}

func normalizeMySQLIndexColumnsDefault(value string) string {
	value = strings.ReplaceAll(value, "`", "")
	value = mysqlIndexPrefixPatternDefault.ReplaceAllString(value, "")
	value = mysqlIndexSortPatternDefault.ReplaceAllString(value, "")
	return strings.ReplaceAll(value, " ", "")
}
