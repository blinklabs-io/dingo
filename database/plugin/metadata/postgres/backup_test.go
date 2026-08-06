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

package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConnEnvParsesURIStyleDSN validates that a URI-style postgres DSN is
// correctly decomposed into the PG* environment variables pg_dump/
// pg_restore read, and that the resolved database name is also returned.
func TestConnEnvParsesURIStyleDSN(t *testing.T) {
	env, database, err := connEnv(
		"postgres://alice:s3cret@db.internal:6543/mydb?sslmode=disable",
	)
	require.NoError(t, err)
	require.Contains(t, env, "PGHOST=db.internal")
	require.Contains(t, env, "PGPORT=6543")
	require.Contains(t, env, "PGUSER=alice")
	require.Contains(t, env, "PGDATABASE=mydb")
	require.Contains(t, env, "PGPASSWORD=s3cret")
	require.Contains(t, env, "PGSSLMODE=disable")
	require.Equal(t, "mydb", database)
}

// TestConnEnvFoldsNonKeywordParamsIntoPGOPTIONS guards against a real gap:
// the generated DSN in provider.go's openStore can include a "timezone"
// query param, which pgx/pgconn happily treats as a runtime session
// parameter but real pg_dump/pg_restore (linked against actual libpq)
// would reject outright as "invalid connection option" if forwarded as a
// raw PG<NAME> variable. connEnv must fold it into PGOPTIONS instead (the
// one libpq-sanctioned way to apply an arbitrary session GUC), not drop it
// or forward it as an invalid top-level keyword.
func TestConnEnvFoldsNonKeywordParamsIntoPGOPTIONS(t *testing.T) {
	env, _, err := connEnv(
		"postgres://alice:s3cret@db.internal:5432/mydb?timezone=UTC",
	)
	require.NoError(t, err)
	for _, kv := range env {
		require.NotEqual(t, "PGTZ", strings.SplitN(kv, "=", 2)[0])
		require.NotEqual(t, "timezone", strings.SplitN(kv, "=", 2)[0])
	}
	require.Contains(t, env, "PGOPTIONS=-c timezone=UTC")
}

// TestConnEnvPassesThroughRawOptionsParam guards the search_path-isolation
// case dialect_integration_test.go's postgresDSNWithSearchPath relies on:
// an explicit "options=-c..." DSN parameter is pgconn's own placeholder for
// a raw, already-formatted PGOPTIONS fragment and must be forwarded as-is,
// not re-wrapped as "-c options=...".
func TestConnEnvPassesThroughRawOptionsParam(t *testing.T) {
	env, _, err := connEnv(
		"postgres://alice@db.internal:5432/mydb?options=-csearch_path%3Dpgbackup_test",
	)
	require.NoError(t, err)
	require.Contains(t, env, "PGOPTIONS=-csearch_path=pgbackup_test")
}

// TestConnEnvOmitsPasswordWhenAbsent validates that no PGPASSWORD entry is
// emitted at all when the DSN carries no password, rather than an empty
// "PGPASSWORD=" that could mask a real .pgpass/PGPASSFILE lookup.
func TestConnEnvOmitsPasswordWhenAbsent(t *testing.T) {
	env, _, err := connEnv("postgres://alice@db.internal:5432/mydb")
	require.NoError(t, err)
	for _, kv := range env {
		require.NotContains(t, kv, "PGPASSWORD=")
	}
}

// TestConnEnvRejectsInvalidDSN validates that a malformed connection
// string is reported as an error up front, rather than producing a
// partially-populated (and silently wrong) PG* environment.
func TestConnEnvRejectsInvalidDSN(t *testing.T) {
	_, _, err := connEnv("::not a dsn::")
	require.Error(t, err)
}

// TestConnEnvInheritsParentEnvironment validates that connEnv extends the
// process's real environment (so PATH etc. survive and pg_dump/pg_restore
// can actually be found) instead of replacing it with just the PG*
// variables it adds.
func TestConnEnvInheritsParentEnvironment(t *testing.T) {
	t.Setenv("DINGO_TEST_MARKER_VAR", "present")
	env, _, err := connEnv("postgres://alice@db.internal:5432/mydb")
	require.NoError(t, err)
	require.True(
		t,
		slices.ContainsFunc(env, func(kv string) bool {
			return kv == "DINGO_TEST_MARKER_VAR=present"
		}),
		"connEnv must inherit the parent process environment (PATH, etc.), "+
			"not replace it -- otherwise pg_dump/pg_restore can't even be found",
	)
}

// TestBackupPostgresFailureDoesNotTouchDestination mirrors sqlite's
// TestBackupFailureDoesNotTouchDestination: a failed pg_dump must not leave
// a partial or clobbered file at dstPath, even if something else creates a
// file there concurrently. Uses the runPgDump seam so this needs no real
// postgres server.
func TestBackupPostgresFailureDoesNotTouchDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.dump")
	original := runPgDump
	t.Cleanup(func() { runPgDump = original })
	runPgDump = func(_ context.Context, _ []string, staged string) error {
		require.NoError(t, os.WriteFile(staged, []byte("partial"), 0o600))
		require.NoError(t, os.WriteFile(dst, []byte("concurrent"), 0o600))
		return errors.New("simulated pg_dump failure")
	}
	err := backupPostgres(
		context.Background(),
		"postgres://alice@db.internal:5432/mydb",
		dst,
	)
	require.Error(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("concurrent"), data)
}

// TestPgRestoreArgsIncludesExplicitDatabase guards a real bug found via a
// live integration run: pg_restore, unlike pg_dump, does not fall back to
// PGDATABASE alone to pick a connection target -- omitting -d/--dbname
// made every restore fail with "one of -d/--dbname and -f/--file must be
// specified" even though every other connection parameter was correctly
// set via env. This is exactly the kind of bug a seam-injected unit test
// (which replaces the whole runPgRestore body) cannot catch, since it
// never exercises the real argv construction -- pgRestoreArgs is factored
// out specifically so this can be checked without running pg_restore.
func TestPgRestoreArgsIncludesExplicitDatabase(t *testing.T) {
	args := pgRestoreArgs("mydb", "/tmp/backup.dump")
	require.Contains(t, args, "--dbname=mydb")
	require.Contains(t, args, "/tmp/backup.dump")
}

// TestBackupPostgresRejectsExistingDestination validates that BackupTo
// refuses to run pg_dump at all when dstPath already exists, rather than
// overwriting or racing whatever created it.
func TestBackupPostgresRejectsExistingDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.dump")
	require.NoError(t, os.WriteFile(dst, []byte("existing"), 0o600))
	called := false
	original := runPgDump
	t.Cleanup(func() { runPgDump = original })
	runPgDump = func(_ context.Context, _ []string, staged string) error {
		called = true
		return os.WriteFile(staged, []byte("new"), 0o600)
	}
	err := backupPostgres(
		context.Background(),
		"postgres://alice@db.internal:5432/mydb",
		dst,
	)
	require.Error(t, err)
	require.False(
		t,
		called,
		"pg_dump must not run against an already-existing destination",
	)
}
