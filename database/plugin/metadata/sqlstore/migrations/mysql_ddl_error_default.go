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

// isMySQLDDLAlreadyAppliedOnConn is deliberately driver-independent. The
// default build cannot import go-sql-driver/mysql, but migration adoption still
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
	if !strings.Contains(message, "error 1061") &&
		!strings.Contains(message, "error 1826") {
		return false
	}
	match := mysqlDuplicateDDLPattern.FindStringSubmatch(strings.TrimSpace(statement))
	if len(match) != 3 {
		return false
	}
	var exists int
	return conn.QueryRowContext(ctx, `
SELECT 1
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
LIMIT 1`, match[2], match[1]).Scan(&exists) == nil && exists == 1
}
