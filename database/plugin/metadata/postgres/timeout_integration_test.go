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

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// withRuntimeParams appends the given DSN query parameters to dsn,
// mirroring exactly what assembleDSN does for StatementTimeout/LockTimeout
// (query.Set on the parsed URL) -- this validates the real mechanism
// assembleDSN relies on (pgx applying an unrecognized DSN query key as a
// session runtime parameter) against a live server, independent of
// Config/openStore plumbing.
func withRuntimeParams(
	t *testing.T,
	dsn string,
	params map[string]string,
) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	query := parsed.Query()
	for k, v := range params {
		query.Set(k, v)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// TestStatementTimeoutIntegration guards that a statement_timeout DSN
// runtime parameter (as assembleDSN sets when Config.StatementTimeout is
// non-zero) actually aborts a long-running statement server-side, using a
// context with no deadline of its own -- so a passing test can only be
// explained by the server enforcing the GUC, not by Go-level cancellation.
func TestStatementTimeoutIntegration(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "pgtimeout_stmt")
	dsn = withRuntimeParams(t, dsn, map[string]string{
		"statement_timeout": "200",
	})

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	start := time.Now()
	_, err = db.ExecContext(context.Background(), "SELECT pg_sleep(5)")
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(
		t,
		elapsed,
		3*time.Second,
		"statement_timeout=200ms must abort a 5s pg_sleep well before it finishes",
	)
	var pgErr *pgconn.PgError
	require.True(
		t, errors.As(err, &pgErr),
		"expected a *pgconn.PgError, got %T: %v", err, err,
	)
	require.Equal(t, "57014", pgErr.Code, "57014 is query_canceled")
}

// TestLockTimeoutIntegration guards that a lock_timeout DSN runtime
// parameter aborts a statement that's waiting on a row lock held by
// another session, rather than waiting for it indefinitely.
func TestLockTimeoutIntegration(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "pgtimeout_lock")

	holder, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	require.NoError(t, holder.PingContext(context.Background()))
	_, err = holder.Exec(
		"CREATE TABLE pgtimeout_lock_row (id int primary key, v int)",
	)
	require.NoError(t, err)
	_, err = holder.Exec(
		"INSERT INTO pgtimeout_lock_row (id, v) VALUES (1, 1)",
	)
	require.NoError(t, err)

	holderTx, err := holder.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holderTx.Rollback() })
	_, err = holderTx.Exec(
		"SELECT * FROM pgtimeout_lock_row WHERE id = 1 FOR UPDATE",
	)
	require.NoError(t, err)

	waiterDSN := withRuntimeParams(t, dsn, map[string]string{
		"lock_timeout": "200",
	})
	waiter, err := sql.Open("pgx", waiterDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = waiter.Close() })

	start := time.Now()
	_, err = waiter.ExecContext(
		context.Background(),
		"UPDATE pgtimeout_lock_row SET v = 2 WHERE id = 1",
	)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(
		t, elapsed, 3*time.Second,
		"lock_timeout=200ms must abort the wait well before the holder's "+
			"transaction ever commits or rolls back",
	)
	var pgErr *pgconn.PgError
	require.True(
		t, errors.As(err, &pgErr),
		"expected a *pgconn.PgError, got %T: %v", err, err,
	)
	require.Equal(t, "55P03", pgErr.Code, "55P03 is lock_not_available")
}
