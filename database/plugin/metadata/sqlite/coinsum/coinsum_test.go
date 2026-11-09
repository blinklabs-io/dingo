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

package coinsum

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoinSumInputDomain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		values  []driver.Value
		want    uint64
		invalid bool
	}{
		{"empty", nil, 0, false},
		{"nullable", []driver.Value{nil, "", []byte{}}, 0, false},
		{"mixed", []driver.Value{int64(3), "0004", []byte("5")}, 12, false},
		{"full range", []driver.Value{"18446744073709551615"}, math.MaxUint64, false},
		{"overflow", []driver.Value{"18446744073709551615", "1"}, 0, true},
		{"oversized", []driver.Value{"18446744073709551616"}, 0, true},
		{"negative integer", []driver.Value{int64(-1)}, 0, true},
		{"negative cancellation", []driver.Value{"1", "-1"}, 0, true},
		{"leading sign", []driver.Value{"+1"}, 0, true},
		{"space", []driver.Value{" 1"}, 0, true},
		{"malformed", []driver.Value{[]byte("12x")}, 0, true},
		{"float", []driver.Value{float64(1)}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sum coinSum
			var err error
			for _, value := range tc.values {
				if err = sum.Step(nil, []driver.Value{value}); err != nil {
					break
				}
			}
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			got, err := sum.WindowValue(nil)
			require.NoError(t, err)
			require.Equal(t, strconv.FormatUint(tc.want, 10), got)
		})
	}
}

func TestCoinSumOwnsBlobAndErrorValues(t *testing.T) {
	t.Parallel()
	var sum coinSum
	value := []byte("123")
	require.NoError(t, sum.Step(nil, []driver.Value{value}))
	copy(value, "999")
	got, err := sum.WindowValue(nil)
	require.NoError(t, err)
	require.Equal(t, "123", got)
	bad := []byte("18446744073709551616")
	err = sum.Step(nil, []driver.Value{bad})
	require.Error(t, err)
	message := err.Error()
	for i := range bad {
		bad[i] = 'x'
	}
	require.Equal(t, message, err.Error())
	require.NoError(t, sum.WindowInverse(nil, []driver.Value{"23"}))
	got, err = sum.WindowValue(nil)
	require.NoError(t, err)
	require.Equal(t, "100", got)
	require.Error(t, sum.WindowInverse(nil, []driver.Value{"101"}))
	require.Error(t, sum.WindowInverse(nil, []driver.Value{"-1"}))
}

func TestCoinSumSQLiteWindowsAndEmptyRange(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	_, err = db.Exec("CREATE TABLE amounts (id INTEGER PRIMARY KEY, amount)")
	require.NoError(t, err)
	var empty string
	require.NoError(
		t,
		db.QueryRow("SELECT "+Name+"(amount) FROM amounts").Scan(&empty),
	)
	require.Equal(t, "0", empty)
	_, err = db.Exec(
		"INSERT INTO amounts VALUES (1, '1'), (2, 2), (3, NULL), (4, X'34')",
	)
	require.NoError(t, err)
	rows, err := db.Query(
		"SELECT " + Name + "(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM amounts ORDER BY id",
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
	require.NoError(t, rows.Close())
	require.Equal(t, []string{"1", "3", "2", "4"}, got)
	for i, value := range []any{"-1", "18446744073709551616", 1.5, "bad"} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, err = db.Exec("DELETE FROM amounts")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO amounts(amount) VALUES (?)", value)
			require.NoError(t, err)
			require.Error(
				t,
				db.QueryRow("SELECT "+Name+"(amount) FROM amounts").
					Scan(&empty),
			)
		})
	}
}
