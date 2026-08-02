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
	"fmt"
)

// sumUint64Rows sums a decimal column in Go instead of asking SQL to coerce
// it to a signed integer. Metadata amounts are persisted as decimal text and
// may occupy the full uint64 range, while SUM/INTEGER is signed (and differs
// across SQLite, PostgreSQL, and MySQL).
func sumUint64Rows(db queryer, query string, args ...any) (uint64, error) {
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total uint64
	for rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		value, err := parseUint64("amount", raw.String)
		if err != nil {
			return 0, err
		}
		if ^uint64(0)-total < value {
			return 0, fmt.Errorf("amount sum overflow")
		}
		total += value
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return total, nil
}
