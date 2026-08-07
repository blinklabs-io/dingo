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

package mysql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConnArgsParsesDSN validates that a mysql DSN is correctly decomposed
// into the --host/--port/--user CLI flags mysqldump/mysql expect and a
// MYSQL_PWD environment entry for the password, with the resolved
// database name also returned.
func TestConnArgsParsesDSN(t *testing.T) {
	env, args, database, err := connArgs(
		"alice:s3cret@tcp(db.internal:3307)/mydb",
	)
	require.NoError(t, err)
	require.Equal(t, "mydb", database)
	require.Contains(t, args, "--host=db.internal")
	require.Contains(t, args, "--port=3307")
	require.Contains(t, args, "--user=alice")
	require.True(
		t,
		slices.ContainsFunc(
			env,
			func(kv string) bool { return kv == "MYSQL_PWD=s3cret" },
		),
	)
}

// TestConnArgsOmitsPasswordWhenAbsent validates that no MYSQL_PWD entry is
// emitted at all when the DSN carries no password, rather than an empty
// value that could still be treated as "authenticate with an empty
// password" by the client tools.
//
// connArgs extends the real process environment (see
// TestConnArgsInheritsParentEnvironment), so this only proves anything if
// the test runner itself doesn't already have MYSQL_PWD set -- clear it
// first and restore whatever was there afterward, rather than trusting
// the ambient environment to be clean (CI/integration setups for this
// exact backend commonly export it).
func TestConnArgsOmitsPasswordWhenAbsent(t *testing.T) {
	t.Setenv("MYSQL_PWD", "")
	require.NoError(t, os.Unsetenv("MYSQL_PWD"))
	env, _, _, err := connArgs("alice@tcp(db.internal:3306)/mydb")
	require.NoError(t, err)
	for _, kv := range env {
		require.NotContains(t, kv, "MYSQL_PWD=")
	}
}

// TestConnArgsRejectsNonTCPNetwork validates that a unix-socket DSN is
// rejected up front with a clear error, since backup/restore's --host/
// --port CLI flags only make sense for a TCP connection.
func TestConnArgsRejectsNonTCPNetwork(t *testing.T) {
	_, _, _, err := connArgs("alice@unix(/tmp/mysql.sock)/mydb")
	require.Error(t, err)
}

// TestConnArgsRejectsInvalidDSN validates that a malformed connection
// string is reported as an error up front, rather than producing
// partially-populated (and silently wrong) connection args.
func TestConnArgsRejectsInvalidDSN(t *testing.T) {
	_, _, _, err := connArgs("::not a dsn::")
	require.Error(t, err)
}

// TestConnArgsInheritsParentEnvironment validates that connArgs extends
// the process's real environment (so PATH etc. survive and mysqldump/
// mysql can actually be found) instead of replacing it with just the
// MYSQL_PWD variable it adds.
func TestConnArgsInheritsParentEnvironment(t *testing.T) {
	t.Setenv("DINGO_TEST_MARKER_VAR", "present")
	env, _, _, err := connArgs("alice@tcp(db.internal:3306)/mydb")
	require.NoError(t, err)
	require.True(
		t,
		slices.ContainsFunc(env, func(kv string) bool {
			return kv == "DINGO_TEST_MARKER_VAR=present"
		}),
		"connArgs must inherit the parent process environment (PATH, etc.), "+
			"not replace it -- otherwise mysqldump/mysql can't even be found",
	)
}

// TestConnArgsMapsSSLMode guards a real gap: connArgs used to drop the
// DSN's TLS setting entirely, so mysqldump/mysql always connected in
// plaintext (or failed outright against a server enforcing TLS) no
// matter what the app's own connection pool was configured with. Every
// recognized go-sql-driver/mysql TLSConfig value must map to the
// corresponding --ssl-mode flag -- "true" (verify CA and hostname) in
// particular must not collapse to a weaker mode.
func TestConnArgsMapsSSLMode(t *testing.T) {
	tests := []struct {
		tlsConfig string
		wantFlag  string
	}{
		{"", "--ssl-mode=DISABLED"},
		{"false", "--ssl-mode=DISABLED"},
		{"true", "--ssl-mode=VERIFY_IDENTITY"},
		{"skip-verify", "--ssl-mode=REQUIRED"},
		{"preferred", "--ssl-mode=PREFERRED"},
	}
	for _, tt := range tests {
		t.Run(tt.tlsConfig, func(t *testing.T) {
			dsn := "alice@tcp(db.internal:3306)/mydb"
			if tt.tlsConfig != "" {
				dsn += "?tls=" + tt.tlsConfig
			}
			_, args, _, err := connArgs(dsn)
			require.NoError(t, err)
			require.Contains(t, args, tt.wantFlag)
		})
	}
}

// TestConnArgsRejectsCustomNamedTLSConfig validates that a custom
// registered TLS config name (via mysql.RegisterTLSConfig, referencing an
// arbitrary *tls.Config the driver resolves internally) is rejected with
// a clear error rather than guessing an --ssl-mode that could silently
// under- or over-verify relative to what that custom config actually does.
func TestConnArgsRejectsCustomNamedTLSConfig(t *testing.T) {
	_, _, _, err := connArgs("alice@tcp(db.internal:3306)/mydb?tls=my-custom-config")
	require.Error(t, err)
}

// TestBackupMySQLFailureDoesNotTouchDestination mirrors sqlite's
// TestBackupFailureDoesNotTouchDestination: a failed mysqldump must not
// leave a partial or clobbered file at dstPath, even if something else
// creates a file there concurrently. Uses the runMysqldump seam so this
// needs no real mysql server.
func TestBackupMySQLFailureDoesNotTouchDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.sql")
	original := runMysqldump
	t.Cleanup(func() { runMysqldump = original })
	runMysqldump = func(_ context.Context, _ []string, _ []string, staged string) error {
		require.NoError(t, os.WriteFile(staged, []byte("partial"), 0o600))
		require.NoError(t, os.WriteFile(dst, []byte("concurrent"), 0o600))
		return errors.New("simulated mysqldump failure")
	}
	err := backupMySQL(
		context.Background(),
		"alice@tcp(db.internal:3306)/mydb",
		dst,
	)
	require.Error(t, err)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, []byte("concurrent"), data)
}

// TestBackupMySQLRejectsExistingDestination validates that BackupTo
// refuses to run mysqldump at all when dstPath already exists, rather
// than overwriting or racing whatever created it.
func TestBackupMySQLRejectsExistingDestination(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.sql")
	require.NoError(t, os.WriteFile(dst, []byte("existing"), 0o600))
	called := false
	original := runMysqldump
	t.Cleanup(func() { runMysqldump = original })
	runMysqldump = func(_ context.Context, _ []string, _ []string, staged string) error {
		called = true
		return os.WriteFile(staged, []byte("new"), 0o600)
	}
	err := backupMySQL(
		context.Background(),
		"alice@tcp(db.internal:3306)/mydb",
		dst,
	)
	require.Error(t, err)
	require.False(
		t,
		called,
		"mysqldump must not run against an already-existing destination",
	)
}

// TestBackupMySQLUsesSingleTransaction guards a real gap: without
// --single-transaction, mysqldump reads InnoDB tables one at a time with
// no shared snapshot, so a node actively writing metadata during the dump
// can produce a backup mixing rows from different points in time.
func TestBackupMySQLUsesSingleTransaction(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.sql")
	var gotArgs []string
	original := runMysqldump
	t.Cleanup(func() { runMysqldump = original })
	runMysqldump = func(_ context.Context, _ []string, args []string, staged string) error {
		gotArgs = args
		return os.WriteFile(staged, []byte("dump"), 0o600)
	}
	require.NoError(t, backupMySQL(
		context.Background(),
		"alice@tcp(db.internal:3306)/mydb",
		dst,
	))
	require.Contains(t, gotArgs, "--single-transaction")
}

// TestBackupMySQLDoesNotUseDatabasesFlag guards a real bug found via a live
// restore test: "mysqldump --databases <db>" embeds CREATE DATABASE/USE
// <db> statements naming the SOURCE database into the dump itself, and
// restoreMySQL's mysql invocation can't override that embedded USE via its
// own connection target -- restoring such a dump into a differently-named
// target database silently landed the data in a new database matching the
// source's name instead, while both mysqldump and mysql reported success.
// The dump must instead pass the database as a single trailing positional
// argument (no --databases flag), which omits the embedded CREATE
// DATABASE/USE and always restores into whatever database the mysql
// client's own connection args select.
func TestBackupMySQLDoesNotUseDatabasesFlag(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "backup.sql")
	var gotArgs []string
	original := runMysqldump
	t.Cleanup(func() { runMysqldump = original })
	runMysqldump = func(_ context.Context, _ []string, args []string, staged string) error {
		gotArgs = args
		return os.WriteFile(staged, []byte("dump"), 0o600)
	}
	require.NoError(t, backupMySQL(
		context.Background(),
		"alice@tcp(db.internal:3306)/mydb",
		dst,
	))
	require.NotContains(t, gotArgs, "--databases")
	require.Contains(t, gotArgs, "mydb")
}

// TestBackupMySQLRequiresConfiguredDatabase validates that BackupTo
// errors cleanly when the DSN names no database, instead of running
// mysqldump with no target and producing a confusing tool-level failure.
func TestBackupMySQLRequiresConfiguredDatabase(t *testing.T) {
	err := backupMySQL(
		context.Background(),
		"alice@tcp(db.internal:3306)/",
		filepath.Join(t.TempDir(), "backup.sql"),
	)
	require.Error(t, err)
}
