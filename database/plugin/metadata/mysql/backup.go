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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// runMysqldump invokes mysqldump, indirected through a variable so a test
// can inject a failure at this exact point deterministically, mirroring
// sqlite's runVacuumInto seam.
var runMysqldump = func(
	ctx context.Context,
	env []string,
	args []string,
	dstPath string,
) error {
	cmd := exec.CommandContext( //nolint:gosec // G204: args are built internally from validated provider config, dstPath is our own staging temp path
		ctx,
		"mysqldump",
		append(args, "--result-file="+dstPath)...,
	)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysqldump: %w: %s", err, stderr.String())
	}
	return nil
}

// runMysqlRestore pipes srcPath into the mysql client against an
// already-empty target database, indirected the same way runMysqldump is.
var runMysqlRestore = func(
	ctx context.Context,
	env []string,
	args []string,
	srcPath string,
) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open dump file: %w", err)
	}
	defer src.Close() //nolint:errcheck
	cmd := exec.CommandContext(ctx, "mysql", args...)
	cmd.Env = env
	cmd.Stdin = src
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysql restore: %w: %s", err, stderr.String())
	}
	return nil
}

// connArgs resolves dsn into the host/port/user/database CLI flags shared by
// mysqldump and mysql, plus the MYSQL_PWD-carrying environment for the
// password -- kept out of argv so it never appears in ps/procfs the way a
// "-p<password>" flag would.
func connArgs(dsn string) (env, args []string, database string, err error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return nil, nil, "", fmt.Errorf("parse connection string: %w", err)
	}
	// tcp4/tcp6 (Go's net.Dial address-family-restricted variants, which
	// the driver's own Config.Net accepts same as the metadata store's own
	// connection pool does -- this isn't a provider-specific restriction to
	// mirror) are accepted alongside plain tcp: mysqldump/mysql have no CLI
	// flag to force IPv4-only or IPv6-only dialing the way the driver's Net
	// setting does, so --host/--port here resolves however the host name
	// naturally does, which in the common single-A/AAAA-record case matches
	// what tcp4/tcp6 would have forced anyway. unix remains rejected: it
	// needs an entirely different --socket flag, not --host/--port, a real
	// scope difference rather than a fidelity gap this can paper over.
	switch cfg.Net {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, nil, "", fmt.Errorf(
			"unsupported network %q: backup/restore requires a tcp connection",
			cfg.Net,
		)
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, nil, "", fmt.Errorf("parse address %q: %w", cfg.Addr, err)
	}
	sslArgs, err := mysqlSSLArgs(cfg.TLSConfig)
	if err != nil {
		return nil, nil, "", err
	}
	args = make([]string, 0, 4+len(sslArgs))
	args = append(args,
		"--host="+host,
		"--port="+port,
		"--user="+cfg.User,
		"--protocol=tcp",
	)
	args = append(args, sslArgs...)
	// A stale MYSQL_PWD already set in this process's own environment (an
	// operator's shell/systemd unit using that variable for some unrelated
	// tool, or a leftover from a previous deployment) must not survive into
	// a DSN that specifies no password -- exec.Cmd.Env only keeps the last
	// duplicate key, so appending our own value below already wins when
	// the DSN has one, but omitting it entirely (the empty-password case)
	// would otherwise silently leave that ambient value in effect,
	// authenticating with different credentials than the DSN itself
	// specifies rather than the empty password the app's own connection
	// pool actually uses.
	env = slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, "MYSQL_PWD=")
	})
	if cfg.Passwd != "" {
		env = append(env, "MYSQL_PWD="+cfg.Passwd)
	}
	return env, args, cfg.DBName, nil
}

// mysqlSSLArgs maps go-sql-driver/mysql's TLSConfig setting (dingo's own
// sslMode provider config field, per mysql/provider.go's openStore) to CLI
// flags for the mysqldump/mysql client tools actually shipped in this
// repo's Docker image: Debian's default-mysql-client package, confirmed
// live (running the actual bookworm image) to be MariaDB's client
// (mariadb-client-10.11), not real MySQL. MariaDB's mysql/mysqldump have
// never adopted MySQL 5.7+'s --ssl-mode flag at all -- every value fails
// outright with "unknown variable 'ssl-mode=...'" -- so this must speak
// MariaDB's older, coarser --ssl/--skip-ssl/--ssl-verify-server-cert flags
// instead of the newer --ssl-mode=X form.
//
// That older flag set can only really express two things: whether TLS is
// attempted at all, and whether the server's certificate is verified once
// it is. There is no client-side flag to make an unverified TLS attempt a
// hard requirement the way MySQL's own REQUIRED mode does, so
// "skip-verify" (required, unverified) and "preferred" (opportunistic)
// necessarily collapse to the same "--ssl" here -- an accepted, documented
// fidelity gap forced by the shipped client, not a design choice. "true"
// (verify CA and hostname, the driver's strictest mode) still maps to its
// own distinct, fully verified flags, since that's the one case a silent
// weakening would be a real regression from what the app's own connection
// pool is configured to require.
//
// A custom registered TLS config name (via mysql.RegisterTLSConfig,
// referencing an arbitrary *tls.Config) can't be mapped at all -- there is
// no fixed meaning to translate, and guessing could just as easily under-
// or over-verify relative to what that custom config actually does. This
// fails loudly rather than silently picking flags that might weaken it.
func mysqlSSLArgs(tlsConfig string) ([]string, error) {
	switch tlsConfig {
	case "", "false":
		// Matches the driver's own default: no TLS is attempted at all
		// (mysqldump/mysql's own default of --ssl on would instead try
		// opportunistic TLS the app's own connection never uses).
		return []string{"--skip-ssl"}, nil
	case "true":
		return []string{"--ssl", "--ssl-verify-server-cert"}, nil
	case "skip-verify", "preferred":
		return []string{"--ssl"}, nil
	default:
		return nil, fmt.Errorf(
			"mysql backup/restore: unsupported tls config %q -- mysqldump/"+
				"mysql cannot be given a custom named TLS config "+
				"(mysql.RegisterTLSConfig); only \"\", \"false\", \"true\", "+
				"\"skip-verify\", or \"preferred\" are supported",
			tlsConfig,
		)
	}
}

// backupMySQL writes a mysqldump SQL archive of the database dsn points at
// to dstPath (which must not already exist).
//
// Deliberately dumped as "mysqldump [options] database" (a single trailing
// positional database name), not "mysqldump --databases database": the
// --databases form embeds CREATE DATABASE/USE <database> statements naming
// the SOURCE database into the dump itself, which restoreMySQL's mysql
// client invocation can't override via its own connection target -- the
// dump's own embedded USE statement always wins, so restoring it into a
// differently-named target database silently lands the data in a new
// database matching the SOURCE's name instead (found via a live restore
// test using differently-named source/target databases -- the restore
// reported success but the target read back empty). The plain single-
// database form dumps only table DDL/data with no embedded database
// selection, so it always lands in whatever database mysql's own
// connection args select.
func backupMySQL(ctx context.Context, dsn, dstPath string) error {
	env, args, database, err := connArgs(dsn)
	if err != nil {
		return fmt.Errorf("mysql backup: %w", err)
	}
	if database == "" {
		return errors.New("mysql backup: no database configured to back up")
	}
	// Without --single-transaction, mysqldump reads InnoDB tables one at a
	// time with no shared snapshot: a node actively writing metadata during
	// the dump can leave the backup with rows from different points in
	// time, producing ledger state that never actually existed at any
	// single instant. --single-transaction opens one REPEATABLE READ
	// transaction for the whole dump, giving a consistent snapshot without
	// blocking concurrent writers (InnoDB-only; dingo's metadata schema is
	// InnoDB throughout).
	args = append(args, "--single-transaction", database)
	err = sqlstore.PublishBackupFile(dstPath, func(stagedPath string) error {
		return runMysqldump(ctx, env, args, stagedPath)
	})
	if err != nil {
		return fmt.Errorf("mysql backup: %w", err)
	}
	return nil
}

// validateMySQLBackup is the sqlstore.Config.ValidateBackup hook (see
// metadata.BackupValidator's doc comment). Deliberately weaker than
// postgres's validatePostgresBackup: mysqldump's plain-SQL output has no
// equivalent to pg_restore --list -- MySQL ships no tool that parses or
// validates a SQL dump's structure without actually executing it against a
// real server -- so this only confirms the file opens and is non-empty,
// catching a missing, unreadable, or zero-byte backup. A truncated-mid-
// statement or otherwise corrupted-but-non-empty dump is still only caught
// later, by restoreMySQL's own mysql client invocation, after Reset has
// already run rather than before it.
func validateMySQLBackup(ctx context.Context, srcPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(srcPath)
	if err != nil {
		return fmt.Errorf("mysql backup validation: %w", err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("mysql backup validation: %q is empty", srcPath)
	}
	return nil
}

// restoreMySQL loads a mysqldump SQL archive at srcPath into the database
// dsn points at. The target must not already contain any dingo tables --
// mysqldump/mysql have no equivalent of a fresh, never-created data
// directory to enforce this the way badger/sqlite's file-based Restore
// contracts do, so this checks explicitly instead of silently merging into
// (or partially overwriting) a populated database.
func restoreMySQL(ctx context.Context, dsn, srcPath string) error {
	env, args, database, err := connArgs(dsn)
	if err != nil {
		return fmt.Errorf("mysql restore: %w", err)
	}
	if database == "" {
		return errors.New(
			"mysql restore: no database configured to restore into",
		)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql restore: open connection: %w", err)
	}
	defer db.Close() //nolint:errcheck
	empty, err := databaseIsEmpty(ctx, db, database)
	if err != nil {
		return fmt.Errorf("mysql restore: %w", err)
	}
	if !empty {
		return errors.New(
			"mysql restore: target database already contains tables -- " +
				"restore must only be called against an empty database",
		)
	}
	if err := runMysqlRestore(ctx, env, append(args, database), srcPath); err != nil {
		return fmt.Errorf("mysql restore: %w", err)
	}
	return nil
}

// databaseIsEmpty reports whether database has no base tables yet.
// Restricted to table_type = 'BASE TABLE' so a view doesn't count as
// occupying the database: resetDatabase below only drops tables, so
// counting a view here would make an otherwise-empty database wrongly
// report "not empty" with no way to fix it.
func databaseIsEmpty(
	ctx context.Context,
	db *sql.DB,
	database string,
) (bool, error) {
	var count int
	err := db.QueryRowContext(
		ctx,
		"SELECT count(*) FROM information_schema.tables "+
			"WHERE table_schema = ? AND table_type = 'BASE TABLE'",
		database,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check target database: %w", err)
	}
	return count == 0, nil
}

// resetDatabase drops every table in database, using the same scope as
// databaseIsEmpty. This is the sqlstore.Config.Reset hook (see
// metadata.Resettable's doc comment): database/lifecycle/restore.go's
// restore orchestration briefly resolves and starts the metadata plugin
// against the real target server just to type-assert it, which for mysql
// means running real migrations against a real remote database -- a plain
// directory wipe (the mechanism that works for sqlite/badger) does
// nothing to undo that, so this clears the tables those migrations
// created instead. Foreign key checks are disabled for the duration so
// tables can be dropped in any order regardless of FK dependencies,
// restored via defer before returning either way.
//
// SET FOREIGN_KEY_CHECKS is session-scoped, but *sql.DB is a connection
// pool -- ExecContext on db directly can run each call on a different
// pooled connection, so the DROP TABLE statements could land on a
// connection where foreign key checks are still enabled and fail on the
// first parent table with a real FK dependent still present (order from
// information_schema.tables is not guaranteed to be FK-safe). Pinning
// everything to one dedicated *sql.Conn makes the disable/restore and
// every drop share the exact session the SET statement changed.
//
// Restricted to table_type = 'BASE TABLE', matching databaseIsEmpty: MySQL
// doesn't error on "DROP TABLE" naming a view, it just emits a note and
// leaves the view in place, which would silently defeat the reset (a
// later restore recreating that view then fails because it already
// exists) instead of failing loudly or actually clearing it.
func resetDatabase(ctx context.Context, db *sql.DB, database string) error {
	rows, err := db.QueryContext(
		ctx,
		"SELECT table_name FROM information_schema.tables "+
			"WHERE table_schema = ? AND table_type = 'BASE TABLE'",
		database,
	)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	if len(tables) == 0 {
		return nil
	}
	if err := refuseIfTargetHasData(ctx, db, database, tables); err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("disable foreign key checks: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1")
	}()
	for _, name := range tables {
		quoted := mysqlQuoteIdentifier(
			database,
		) + "." + mysqlQuoteIdentifier(
			name,
		)
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted); err != nil {
			return fmt.Errorf("drop table %s: %w", quoted, err)
		}
	}
	return nil
}

// migrationsTableName is sqlstore/migrations' own bookkeeping table --
// every real Start() populates it with one row per applied migration, so
// it's the one table a freshly migrated, otherwise-empty database is
// expected to already have data in.
const migrationsTableName = "schema_migrations"

// refuseIfTargetHasData errors out, before resetDatabase drops anything,
// if any table other than migrationsTableName already contains a row.
// dingo's own migrations never insert into a domain table -- only
// schema_migrations records bookkeeping rows (verified: no migration file
// under sqlstore/migrations/v1 contains an INSERT INTO a domain table) --
// so a database restoreMetadataStore's brief resolve-and-start just
// finished migrating has zero rows in everything else. A nonzero count
// anywhere else means this target isn't that: most plausibly a live
// node's own database, pointed at by a reused or misconfigured DSN, whose
// accumulated real data resetDatabase's unconditional DROP TABLE would
// otherwise destroy with no way back.
//
// Skipped entirely when metadata.ResetOfPopulatedTargetAllowed(ctx) -- see
// its doc comment: a live node restoring itself from its own earlier
// snapshot always targets exactly the database it already owns, real
// accumulated data and all, which is precisely what this guard would
// otherwise (incorrectly) treat as the misconfigured-DSN case it exists to
// catch.
func refuseIfTargetHasData(
	ctx context.Context,
	db *sql.DB,
	database string,
	tables []string,
) error {
	if metadata.ResetOfPopulatedTargetAllowed(ctx) {
		return nil
	}
	for _, name := range tables {
		if name == migrationsTableName {
			continue
		}
		quoted := mysqlQuoteIdentifier(
			database,
		) + "." + mysqlQuoteIdentifier(
			name,
		)
		var hasData int
		err := db.QueryRowContext(
			ctx, "SELECT EXISTS (SELECT 1 FROM "+quoted+")",
		).Scan(&hasData)
		if err != nil {
			return fmt.Errorf("check table %s for data: %w", quoted, err)
		}
		if hasData != 0 {
			return fmt.Errorf(
				"mysql reset: table %s already contains data -- refusing "+
					"to reset a target that isn't a freshly migrated, empty "+
					"database (this looks like a live database's own data, "+
					"not something restoreMetadataStore's brief "+
					"resolve-and-start just created)",
				quoted,
			)
		}
	}
	return nil
}

// mysqlQuoteIdentifier backtick-quotes a MySQL identifier, doubling any
// embedded backtick -- table names here come from information_schema
// itself (already-valid identifiers reflected back by the server), and
// the database name comes from provider config, not external input, but
// quoting defensively costs nothing.
func mysqlQuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
