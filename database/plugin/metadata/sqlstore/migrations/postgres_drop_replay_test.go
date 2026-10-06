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
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type postgresDropReplayConnector struct {
	columnPresent bool
}

func (c postgresDropReplayConnector) Connect(context.Context) (driver.Conn, error) {
	return &postgresDropReplayConn{columnPresent: c.columnPresent}, nil
}

func (c postgresDropReplayConnector) Driver() driver.Driver {
	return postgresDropReplayDriver{}
}

type postgresDropReplayDriver struct{}

func (postgresDropReplayDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type postgresDropReplayConn struct {
	columnPresent bool
}

func (*postgresDropReplayConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is unsupported")
}

func (*postgresDropReplayConn) Close() error { return nil }

func (*postgresDropReplayConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are unsupported")
}

func (c *postgresDropReplayConn) QueryContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "SELECT to_regclass($1)::text"):
		return &postgresDropReplayRows{
			columns: []string{"to_regclass"},
			values:  [][]driver.Value{{"public.asset"}},
		}, nil
	case strings.TrimSpace(query) == strings.TrimSpace(postgresColumnTypeQuery):
		rows := &postgresDropReplayRows{
			columns: []string{"data_type", "is_nullable", "column_default"},
		}
		if c.columnPresent {
			rows.values = [][]driver.Value{{"text", "YES", nil}}
		}
		return rows, nil
	default:
		return nil, errors.New("unexpected query")
	}
}

type postgresDropReplayRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *postgresDropReplayRows) Columns() []string { return r.columns }
func (*postgresDropReplayRows) Close() error        { return nil }

func (r *postgresDropReplayRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func TestPostgresDropColumnReplayChecksNoRowsBeforeScanArity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		columnPresent bool
		wantReplay    bool
	}{
		{name: "column absent", wantReplay: true},
		{name: "column present", columnPresent: true, wantReplay: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := sql.OpenDB(postgresDropReplayConnector{
				columnPresent: tc.columnPresent,
			})
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })

			require.Equal(t, tc.wantReplay, isPostgresDropColumnAlreadyAppliedOnConn(
				context.Background(),
				conn,
				"ALTER TABLE asset DROP COLUMN name_hex",
				errors.New(`column "name_hex" does not exist`),
			))
		})
	}
}
