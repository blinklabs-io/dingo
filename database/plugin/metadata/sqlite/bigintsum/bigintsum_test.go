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

package bigintsum

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBigintSum(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:?_pragma=synchronous(OFF)")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`CREATE TABLE amounts (grp integer, amount text)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO amounts (grp, amount) VALUES
		(1, '18446744073709551615'), (1, '18446744073709551615'), (1, NULL),
		(2, '-30000000000000000000'), (2, '7'),
		(3, NULL),
		(4, 'not a number')`)
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "sums past uint64 and skips NULL",
			query: `SELECT ` + Name + `(amount) FROM amounts WHERE grp = 1`,
			want:  "36893488147419103230",
		},
		{
			name:  "keeps the sign",
			query: `SELECT ` + Name + `(amount) FROM amounts WHERE grp = 2`,
			want:  "-29999999999999999993",
		},
		{
			name:  "only NULL inputs total zero",
			query: `SELECT ` + Name + `(amount) FROM amounts WHERE grp = 3`,
			want:  "0",
		},
		{
			name:  "no rows total zero",
			query: `SELECT ` + Name + `(amount) FROM amounts WHERE grp = 9`,
			want:  "0",
		},
		{
			name:  "integer input",
			query: `SELECT ` + Name + `(grp) FROM amounts WHERE grp = 2`,
			want:  "4",
		},
	} {
		var got string
		require.NoError(t, db.QueryRow(tc.query).Scan(&got), tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}

	var got string
	err = db.QueryRow(
		`SELECT ` + Name + `(amount) FROM amounts WHERE grp = 4`,
	).Scan(&got)
	require.ErrorContains(t, err, "invalid decimal")
}
