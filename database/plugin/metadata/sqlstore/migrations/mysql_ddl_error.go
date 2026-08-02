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

//go:build dingo_extra_plugins

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

var mysqlDDLObjectPattern = regexp.MustCompile(
	"(?is)(?:INDEX|KEY)(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?|CONSTRAINT\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?",
)
var mysqlIndexDefinitionPattern = regexp.MustCompile("(?is)^CREATE\\s+(UNIQUE\\s+)?INDEX(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?\\s+ON\\s+[`\\\"]?([a-zA-Z0-9_]+)[`\\\"]?\\s*\\((.*)\\)$")

// isMySQLDDLAlreadyApplied verifies that the duplicate object named by the
// server error actually exists in the current schema. This prevents masking a
// duplicate-name error for a different object definition.
func isMySQLDDLAlreadyAppliedOnConn(ctx context.Context, conn *sql.Conn, statement string, err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	if !errors.As(err, &mysqlErr) || (mysqlErr.Number != 1061 && mysqlErr.Number != 1826) {
		return false
	}
	if conn == nil {
		// Without a connection the existing object cannot be inspected safely;
		// never turn an unrelated duplicate-definition error into a no-op.
		return false
	}
	match := mysqlDDLObjectPattern.FindStringSubmatch(statement)
	if len(match) != 3 {
		return false
	}
	name := match[1]
	if name == "" {
		name = match[2]
	}
	var schema string
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil || schema == "" {
		return false
	}
	var exists int
	indexMatch := mysqlIndexDefinitionPattern.FindStringSubmatch(statement)
	if len(indexMatch) == 5 {
		match := indexMatch
		columns := strings.TrimSpace(match[4])
		var actual string
		if err := conn.QueryRowContext(ctx, `SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = ?`, schema, match[3], match[2]).Scan(&actual); err != nil {
			return false
		}
		// Normalize quoting and deferred prefix lengths before comparing the
		// definition. A same-named index on different columns must not be
		// treated as a successful migration.
		columns = strings.ReplaceAll(columns, "`", "")
		columns = regexp.MustCompile(`\(\d+\)?`).ReplaceAllString(columns, "")
		columns = regexp.MustCompile(`(?i)\s+(?:ASC|DESC)\b`).ReplaceAllString(columns, "")
		actual = strings.ReplaceAll(actual, "`", "")
		if strings.ReplaceAll(columns, " ", "") != strings.ReplaceAll(actual, " ", "") {
			return false
		}
		var nonUnique int
		if err := conn.QueryRowContext(ctx, `SELECT non_unique FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? AND index_name = ? LIMIT 1`, schema, match[3], match[2]).Scan(&nonUnique); err != nil {
			return false
		}
		if (match[1] != "") == (nonUnique == 0) {
			exists = 1
		}
		return exists == 1
	}
	query := `SELECT 1 FROM information_schema.statistics WHERE table_schema = ? AND index_name = ? LIMIT 1`
	if mysqlErr.Number == 1826 {
		query = `SELECT 1 FROM information_schema.table_constraints WHERE table_schema = ? AND constraint_name = ? LIMIT 1`
	}
	return conn.QueryRowContext(ctx, query, schema, name).Scan(&exists) == nil
}
