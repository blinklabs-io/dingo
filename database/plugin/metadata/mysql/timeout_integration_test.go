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

//go:build dingo_extra_plugins && dingo_db_integration

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// withSessionParams parses dsn and adds the given session variables to its
// Params map, mirroring exactly what assembleDSN does for
// StatementTimeout/LockTimeout -- this validates the real mechanism
// assembleDSN relies on (the driver issuing a SET statement on every new
// connection for any Config.Params entry) against a live server,
// independent of Config/openStore plumbing.
func withSessionParams(
	t *testing.T,
	dsn string,
	params map[string]string,
) string {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	if parsed.Params == nil {
		parsed.Params = make(map[string]string, len(params))
	}
	for k, v := range params {
		parsed.Params[k] = v
	}
	return parsed.FormatDSN()
}

// TestStatementTimeoutIntegration guards that a max_execution_time session
// variable (as assembleDSN sets when Config.StatementTimeout is non-zero)
// actually aborts a long-running read-only statement server-side, using a
// context with no deadline of its own -- so a passing test can only be
// explained by the server enforcing the variable, not by Go-level
// cancellation. max_execution_time only bounds top-level read-only SELECT
// statements, so the blocked statement here is a SELECT, not a write.
//
// The blocking statement is a self-join over information_schema.columns,
// not "SELECT SLEEP(n)": when max_execution_time interrupts SLEEP()
// specifically, MySQL has SLEEP() return 1 and the statement complete
// successfully rather than raising ER_QUERY_TIMEOUT -- confirmed live
// against mysql:8 (see PR #3373 review). A statement doing real per-row
// work is genuinely aborted with error 3024 instead. A 3-way cross join
// over a system view large enough to take far longer than 200ms to
// complete guarantees that regardless of how many rows this particular
// server's information_schema.columns happens to hold.
func TestStatementTimeoutIntegration(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "mysqltimeout_stmt")
	dsn = withSessionParams(t, dsn, map[string]string{
		"max_execution_time": "200",
	})

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	start := time.Now()
	var count int64
	err = db.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM information_schema.columns a, "+
			"information_schema.columns b, information_schema.columns c",
	).Scan(&count)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(
		t, elapsed, 3*time.Second,
		"max_execution_time=200ms must abort the cross join well before it "+
			"could ever finish",
	)
	var mysqlErr *mysqldriver.MySQLError
	require.True(
		t, errors.As(err, &mysqlErr),
		"expected a *mysqldriver.MySQLError, got %T: %v", err, err,
	)
	require.Equal(
		t, uint16(3024), mysqlErr.Number,
		"ER_QUERY_TIMEOUT",
	)
}

// TestLockTimeoutIntegration guards that an innodb_lock_wait_timeout
// session variable aborts a statement that's waiting on a row lock held by
// another session, rather than waiting for it indefinitely.
func TestLockTimeoutIntegration(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "mysqltimeout_lock")

	holder, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	require.NoError(t, holder.PingContext(context.Background()))
	_, err = holder.Exec(
		"CREATE TABLE mysqltimeout_lock_row (id int primary key, v int) " +
			"ENGINE=InnoDB",
	)
	require.NoError(t, err)
	_, err = holder.Exec(
		"INSERT INTO mysqltimeout_lock_row (id, v) VALUES (1, 1)",
	)
	require.NoError(t, err)

	holderTx, err := holder.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holderTx.Rollback() })
	_, err = holderTx.Exec(
		"SELECT * FROM mysqltimeout_lock_row WHERE id = 1 FOR UPDATE",
	)
	require.NoError(t, err)

	waiterDSN := withSessionParams(t, dsn, map[string]string{
		"innodb_lock_wait_timeout": "1",
	})
	waiter, err := sql.Open("mysql", waiterDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = waiter.Close() })

	start := time.Now()
	_, err = waiter.ExecContext(
		context.Background(),
		"UPDATE mysqltimeout_lock_row SET v = 2 WHERE id = 1",
	)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(
		t, elapsed, 3*time.Second,
		"innodb_lock_wait_timeout=1s must abort the wait well before the "+
			"holder's transaction ever commits or rolls back",
	)
	var mysqlErr *mysqldriver.MySQLError
	require.True(
		t, errors.As(err, &mysqlErr),
		"expected a *mysqldriver.MySQLError, got %T: %v", err, err,
	)
	require.Equal(
		t, uint16(1205), mysqlErr.Number,
		"ER_LOCK_WAIT_TIMEOUT",
	)
}
