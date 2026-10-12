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
	"testing"

	"github.com/stretchr/testify/require"
)

// testLimitOffset executes addLimitOffset's output on store's backend. A
// positive offset with no limit is the case an unbounded LIMIT literal has to
// make valid, and PostgreSQL and MySQL reject the SQLite-only spelling.
func testLimitOffset(t *testing.T, store *Store) {
	t.Helper()
	for _, test := range []struct {
		name          string
		limit, offset int
		want          []int
	}{
		{name: "no limit no offset", want: []int{1, 2, 3}},
		{name: "limit only", limit: 2, want: []int{1, 2}},
		{name: "offset only", offset: 1, want: []int{2, 3}},
		{name: "limit and offset", limit: 1, offset: 1, want: []int{2}},
		{name: "offset past end", offset: 3, want: []int{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			query, args := addLimitOffset(
				"SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY n",
				nil,
				test.limit,
				test.offset,
			)
			rows, err := store.writeDB.QueryContext(
				t.Context(),
				store.dialect.Rebind(query),
				args...,
			)
			require.NoError(t, err)
			defer rows.Close()
			got := []int{}
			for rows.Next() {
				var n int
				require.NoError(t, rows.Scan(&n))
				got = append(got, n)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, test.want, got)
		})
	}
}

func TestAddLimitOffsetSQLite(t *testing.T) {
	t.Parallel()
	testLimitOffset(t, newMigratedTestStore(t))
}
