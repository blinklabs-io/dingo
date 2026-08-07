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
	"strings"

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
	if cfg.Net != "tcp" {
		return nil, nil, "", fmt.Errorf(
			"unsupported network %q: backup/restore requires a tcp connection",
			cfg.Net,
		)
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, nil, "", fmt.Errorf("parse address %q: %w", cfg.Addr, err)
	}
	sslMode, err := mysqlSSLMode(cfg.TLSConfig)
	if err != nil {
		return nil, nil, "", err
	}
	args = []string{
		"--host=" + host,
		"--port=" + port,
		"--user=" + cfg.User,
		"--protocol=tcp",
		"--ssl-mode=" + sslMode,
	}
	env = os.Environ()
	if cfg.Passwd != "" {
		env = append(env, "MYSQL_PWD="+cfg.Passwd)
	}
	return env, args, cfg.DBName, nil
}

// mysqlSSLMode maps go-sql-driver/mysql's TLSConfig setting (dingo's own
// sslMode provider config field, per mysql/provider.go's openStore) to the
// mysqldump/mysql client tools' --ssl-mode flag. Left unmapped, these
// tools default to PREFERRED (opportunistic, unverified TLS) regardless
// of what the app's own connection pool is actually configured with --
// silently using a different, weaker transport for backup/restore than
// the one guarding every other connection this process makes. "true"
// (verify CA and hostname, the driver's strictest mode) must map to
// VERIFY_IDENTITY specifically, not a weaker mode, or a verify-required
// deployment's dump/restore connection would go out unverified.
//
// A custom registered TLS config name (via mysql.RegisterTLSConfig,
// referencing an arbitrary *tls.Config) can't be mapped at all -- there is
// no fixed meaning to translate, and guessing a specific --ssl-mode could
// just as easily under- or over-verify relative to what that custom
// config actually does. This fails loudly rather than silently picking a
// mode that might weaken it.
func mysqlSSLMode(tlsConfig string) (string, error) {
	switch tlsConfig {
	case "", "false":
		// Matches the driver's own default: no TLS is attempted at all
		// (mysqldump/mysql's own default of PREFERRED would instead try
		// opportunistic TLS the app's own connection never uses).
		return "DISABLED", nil
	case "true":
		return "VERIFY_IDENTITY", nil
	case "skip-verify":
		return "REQUIRED", nil
	case "preferred":
		return "PREFERRED", nil
	default:
		return "", fmt.Errorf(
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

// mysqlQuoteIdentifier backtick-quotes a MySQL identifier, doubling any
// embedded backtick -- table names here come from information_schema
// itself (already-valid identifiers reflected back by the server), and
// the database name comes from provider config, not external input, but
// quoting defensively costs nothing.
func mysqlQuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
