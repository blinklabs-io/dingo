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
	"database/sql/driver"
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

func TestBigintSumSlidingMixedInputs(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`CREATE TABLE amounts(seq INTEGER PRIMARY KEY, amount)`)
	require.NoError(t, err)
	values := []any{
		nil,
		int64(7),
		"18446744073709551616",
		"-9223372036854775808",
		[]byte("+0007"),
		"-36893488147419103232",
		int64(0),
	}
	for i, value := range values {
		_, err = db.Exec(`INSERT INTO amounts VALUES(?,?)`, i, value)
		require.NoError(t, err)
	}
	rows, err := db.Query(
		`SELECT ` + Name + `(amount) OVER(ORDER BY seq ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM amounts ORDER BY seq`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		got = append(got, value)
	}
	require.NoError(t, rows.Err())
	require.Equal(
		t,
		[]string{
			"0",
			"7",
			"18446744073709551623",
			"9223372036854775815",
			"9223372036854775815",
			"-46116860184273879033",
			"-36893488147419103225",
		},
		got,
	)
}

func TestBigintSumRejectsMalformedInputs(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, value := range []any{"", " ", "12x", "1_000", 1.5} {
		var total string
		err = db.QueryRow(`SELECT `+Name+`(?)`, value).Scan(&total)
		require.Error(t, err, "input %v", value)
	}
}

func BenchmarkBigintSumStep(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value driver.Value
	}{
		{"integer", int64(1000000)},
		{"decimal", "39169836000000"},
		{"wide", "184467440737095516160000"},
		{"negative", "-9223372036854775808"},
		{"blob", []byte("39169836000000")},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var sum bigintSum
			args := []driver.Value{tc.value}
			b.ReportAllocs()
			for b.Loop() {
				if err := sum.Step(nil, args); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestBigintSumOwnsParsedBlobDigits(t *testing.T) {
	t.Parallel()
	var sum bigintSum
	value := []byte("184467440737095516160000")
	require.NoError(t, sum.Step(nil, []driver.Value{value}))
	for i := range value {
		value[i] = 'x'
	}
	got, err := sum.WindowValue(nil)
	require.NoError(t, err)
	require.Equal(t, "184467440737095516160000", got)
	inverse := []byte("-1")
	require.NoError(t, sum.WindowInverse(nil, []driver.Value{inverse}))
	for i := range inverse {
		inverse[i] = 'x'
	}
	got, err = sum.WindowValue(nil)
	require.NoError(t, err)
	require.Equal(t, "184467440737095516160001", got)
}

func BenchmarkBigintSumSQL(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"decimal", "39169836000000", "391698360000000000"},
		{"wide", "184467440737095516160000", "1844674407370955161600000000"},
		{"blob", []byte("39169836000000"), "391698360000000000"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("sqlite", ":memory:")
			require.NoError(b, err)
			db.SetMaxOpenConns(1)
			b.Cleanup(func() { require.NoError(b, db.Close()) })
			_, err = db.Exec("CREATE TABLE amounts(amount)")
			require.NoError(b, err)
			_, err = db.Exec(`WITH RECURSIVE sequence(n) AS (
    VALUES(1) UNION ALL SELECT n+1 FROM sequence WHERE n<10000
   ) INSERT INTO amounts SELECT ? FROM sequence`, tc.value)
			require.NoError(b, err)
			stmt, err := db.Prepare("SELECT " + Name + "(amount) FROM amounts")
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, stmt.Close()) })
			b.ReportAllocs()
			for b.Loop() {
				var got string
				require.NoError(b, stmt.QueryRow().Scan(&got))
				require.Equal(b, tc.want, got)
			}
		})
	}
}
