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
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/jackc/pgx/v5/pgconn"
)

// runPgDump invokes pg_dump, indirected through a variable so a test can
// inject a failure at this exact point deterministically, mirroring
// sqlite's runVacuumInto seam.
var runPgDump = func(ctx context.Context, env []string, dstPath string) error {
	cmd := exec.CommandContext( //nolint:gosec // G204: dstPath is our own staging temp path, not user input
		ctx,
		"pg_dump",
		"--format=custom",
		"--no-password",
		"--file="+dstPath,
	)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_dump: %w: %s", err, stderr.String())
	}
	return nil
}

// pgRestoreArgs builds pg_restore's argv, factored out of runPgRestore so a
// unit test can check the built flags directly without actually running
// pg_restore (which needs a real server). Unlike pg_dump, pg_restore does
// not fall back to PGDATABASE alone to pick a connection target -- it
// requires one of -d/--dbname or -f/--file explicitly on the command line,
// and previously omitting it here made every restore fail outright with
// "one of -d/--dbname and -f/--file must be specified" -- so the database
// name is passed as an argument even though every other connection
// parameter travels via env (see connEnv).
func pgRestoreArgs(database, srcPath string) []string {
	return []string{
		"--no-password", "--exit-on-error", "--dbname=" + database, srcPath,
	}
}

// runPgRestore invokes pg_restore against an already-empty target database,
// indirected the same way runPgDump is.
var runPgRestore = func(ctx context.Context, env []string, database, srcPath string) error {
	cmd := exec.CommandContext( //nolint:gosec // G204: database/srcPath come from validated provider config and our own staging path, not user input
		ctx,
		"pg_restore",
		pgRestoreArgs(database, srcPath)...,
	)
	cmd.Env = env
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_restore: %w: %s", err, stderr.String())
	}
	return nil
}

// connEnv resolves dsn into the PG* environment variables pg_dump/pg_restore
// read, so credentials never appear in a subprocess's argv (visible to any
// other user on the host via ps/procfs) the way embedding them in a
// connection-string argument would. pgconn.ParseConfig accepts either DSN
// style (URI or keyword=value) dingo's own postgres provider might be
// configured with, unlike a bespoke parser tied to one style.
//
// Any DSN query parameter pgconn doesn't recognize as one of the fields
// above (e.g. an operator-set "timezone", or a "search_path" isolating
// tests -- see dialect_integration_test.go's postgresDSNWithSearchPath) is
// preserved in cfg.RuntimeParams, not silently dropped: real libpq (which
// pg_dump/pg_restore link against, unlike pgx's own lenient Go parser)
// rejects an unrecognized top-level connection keyword outright, so
// forwarding it as a raw PG<NAME> variable would break the dump/restore
// call for any DSN using one. PGOPTIONS is libpq's own sanctioned
// mechanism for exactly this: a "-c name=value" flag per session GUC,
// applied after connecting rather than treated as a connection keyword.
func connEnv(dsn string) (env []string, database string, err error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, "", fmt.Errorf("parse connection string: %w", err)
	}
	sslmode, err := postgresSSLMode(cfg.TLSConfig)
	if err != nil {
		return nil, "", err
	}
	env = append(os.Environ(),
		"PGHOST="+cfg.Host,
		"PGPORT="+strconv.Itoa(int(cfg.Port)),
		"PGUSER="+cfg.User,
		"PGDATABASE="+cfg.Database,
		"PGSSLMODE="+sslmode,
	)
	if cfg.Password != "" {
		env = append(env, "PGPASSWORD="+cfg.Password)
	}
	if len(cfg.RuntimeParams) > 0 {
		env = append(
			env,
			"PGOPTIONS="+runtimeParamsToOptions(cfg.RuntimeParams),
		)
	}
	return env, cfg.Database, nil
}

// postgresSSLMode recovers the PGSSLMODE pg_dump/pg_restore should use from
// pgconn's already-resolved *tls.Config, which -- unlike the original DSN
// -- no longer carries the operator's own sslmode string directly.
// Collapsing every non-nil TLSConfig to a blanket "require" silently
// downgrades an operator's verify-ca/verify-full configuration to
// "encrypted but unverified," a real MITM exposure for exactly the
// deployments that asked for stronger verification. pgconn's own
// configTLS sets these tls.Config fields distinctly per mode, so the
// original mode can be recovered exactly: ServerName only for
// verify-full, a custom VerifyPeerCertificate only for verify-ca (and the
// require-with-sslrootcert-present case, which it treats identically to
// verify-ca), and neither for a bare "require"/"prefer"/"allow".
//
// A custom root CA or client certificate configured for verify-ca/
// verify-full cannot be forwarded, though: pgconn parses sslrootcert/
// sslcert/sslkey into in-memory certificate material (x509.CertPool /
// tls.Certificate) and discards the original file paths entirely, and
// pg_dump/pg_restore's PGSSLROOTCERT/PGSSLCERT/PGSSLKEY need real files
// on disk. Rather than silently falling back to weaker verification (or
// the system trust store instead of the operator's own CA) in that case,
// this fails loudly -- the caller learns backup/restore can't run safely
// against this connection instead of getting one that quietly skipped
// identity verification. (This does mean a legitimate sslrootcert=system
// configuration is also rejected here, since a system-trust CertPool and
// a custom-file CertPool are indistinguishable once resolved -- an
// accepted, documented false-positive in favor of never weakening a
// misclassified custom CA.)
func postgresSSLMode(tlsConfig *tls.Config) (string, error) {
	if tlsConfig == nil {
		return "disable", nil
	}
	if len(tlsConfig.Certificates) > 0 {
		return "", errors.New(
			"postgres backup/restore: connection is configured with a client " +
				"TLS certificate (sslcert/sslkey), which pg_dump/pg_restore " +
				"cannot be given -- the original key/certificate files are " +
				"not recoverable from the parsed connection",
		)
	}
	switch {
	case tlsConfig.ServerName != "":
		if tlsConfig.RootCAs != nil {
			return "", errors.New(
				"postgres backup/restore: connection uses sslmode=verify-full " +
					"with a custom root CA (sslrootcert), which pg_dump/pg_restore " +
					"cannot be given -- the original CA file is not recoverable " +
					"from the parsed connection",
			)
		}
		return "verify-full", nil
	case tlsConfig.VerifyPeerCertificate != nil:
		if tlsConfig.RootCAs != nil {
			return "", errors.New(
				"postgres backup/restore: connection uses sslmode=verify-ca " +
					"with a custom root CA (sslrootcert), which pg_dump/pg_restore " +
					"cannot be given -- the original CA file is not recoverable " +
					"from the parsed connection",
			)
		}
		return "verify-ca", nil
	default:
		return "require", nil
	}
}

// runtimeParamsToOptions renders pgconn's parsed RuntimeParams as a PGOPTIONS
// value. An "options" entry is already a raw "-c name=value"-style fragment
// (pgconn stores a DSN's own "options=" parameter under this key verbatim,
// presumably already correctly escaped by whoever wrote it) and is passed
// through as-is; every other key is a plain GUC name/value pair rendered
// as its own "-c" flag, with the value escaped via escapePGOption. Sorted
// for deterministic output.
func runtimeParamsToOptions(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "options" {
			parts = append(parts, params[key])
			continue
		}
		parts = append(
			parts,
			fmt.Sprintf("-c %s=%s", key, escapePGOption(params[key])),
		)
	}
	return strings.Join(parts, " ")
}

// escapePGOption backslash-escapes a PGOPTIONS value's embedded spaces and
// backslashes. The server parses the whole options startup parameter with
// its own whitespace-splitting tokenizer (pg_split_opts), which treats a
// backslash as an escape for the following character -- without this, a
// value containing a literal space (e.g. a search_path or a quoted
// identifier) would be read as two separate tokens, and a value containing
// a literal backslash (e.g. a Windows-style path) would be misinterpreted
// as escaping whatever character follows it.
func escapePGOption(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r == '\\' || r == ' ' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// backupPostgres writes a pg_dump custom-format archive of the database dsn
// points at to dstPath (which must not already exist).
func backupPostgres(ctx context.Context, dsn, dstPath string) error {
	env, _, err := connEnv(dsn)
	if err != nil {
		return fmt.Errorf("postgres backup: %w", err)
	}
	err = sqlstore.PublishBackupFile(dstPath, func(stagedPath string) error {
		return runPgDump(ctx, env, stagedPath)
	})
	if err != nil {
		return fmt.Errorf("postgres backup: %w", err)
	}
	return nil
}

// restorePostgres loads a pg_dump custom-format archive at srcPath into the
// database dsn points at. The target must not already contain any dingo
// tables -- pg_restore has no equivalent of a fresh, never-created data
// directory to enforce this the way badger/sqlite's file-based Restore
// contracts do, so this checks explicitly instead of silently merging into
// (or partially overwriting) a populated schema.
func restorePostgres(ctx context.Context, dsn, srcPath string) error {
	env, database, err := connEnv(dsn)
	if err != nil {
		return fmt.Errorf("postgres restore: %w", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("postgres restore: open connection: %w", err)
	}
	defer db.Close() //nolint:errcheck
	empty, err := databaseIsEmpty(ctx, db)
	if err != nil {
		return fmt.Errorf("postgres restore: %w", err)
	}
	if !empty {
		return errors.New(
			"postgres restore: target database already contains tables -- " +
				"restore must only be called against an empty database",
		)
	}
	if err := runPgRestore(ctx, env, database, srcPath); err != nil {
		return fmt.Errorf("postgres restore: %w", err)
	}
	return nil
}

// databaseIsEmpty reports whether the connected database has no tables in
// any user schema. pg_dump/pg_restore operate on the whole database dsn
// points at, not just the caller's default search_path schema (a plain
// DSN with no --schema flag dumps every schema), so this checks across all
// of them -- checking only the search_path-visible schemas would let a
// table sitting in some other schema go undetected here, surfacing as a
// raw pg_restore conflict instead of this package's own clean error.
// Restricted to table_type = 'BASE TABLE' so a view (or foreign table)
// doesn't count as occupying the database: resetDatabase below can't
// clear one with DROP TABLE, so counting it here would make an
// otherwise-empty database wrongly report "not empty" with no way to fix it.
func databaseIsEmpty(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
	`).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check target database: %w", err)
	}
	return count == 0, nil
}

// resetDatabase drops every table in every user schema of the database db
// is connected to, using the same schema scope as databaseIsEmpty. This is
// the sqlstore.Config.Reset hook (see metadata.Resettable's doc comment):
// database/lifecycle/restore.go's restore orchestration briefly resolves
// and starts the metadata plugin against the real target server just to
// type-assert it, which for postgres means running real migrations
// against a real remote database -- a plain directory wipe (the mechanism
// that works for sqlite/badger) does nothing to undo that, so this clears
// the tables those migrations created instead. Dropping tables rather
// than the schema itself avoids assuming the connection's search_path is
// exactly "public" (an operator-configured non-default search_path is
// exactly what postgresDSNWithSearchPath-style DSNs use). CASCADE handles
// FK dependency ordering without needing a specific drop order.
//
// Restricted to table_type = 'BASE TABLE', matching databaseIsEmpty: a
// view or foreign table sitting alongside dingo's own tables would make
// DROP TABLE fail outright (postgres rejects dropping a view that way),
// aborting the whole reset over something dingo's migrations never
// created and has no reason to touch.
func resetDatabase(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_schema, table_name FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	var tables []qualifiedTable
	for rows.Next() {
		var t qualifiedTable
		if err := rows.Scan(&t.schema, &t.name); err != nil {
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	if err := refuseIfTargetHasData(ctx, db, tables); err != nil {
		return err
	}
	for _, t := range tables {
		quoted := pgQuoteIdentifier(t.schema) + "." + pgQuoteIdentifier(t.name)
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+" CASCADE"); err != nil {
			return fmt.Errorf("drop table %s: %w", quoted, err)
		}
	}
	return nil
}

type qualifiedTable struct{ schema, name string }

// migrationsTableName is sqlstore/migrations' own bookkeeping table --
// every real Start() populates it with one row per applied migration, so
// it's the one table a freshly migrated, otherwise-empty database is
// expected to already have data in.
const migrationsTableName = "schema_migrations"

// refuseIfTargetHasData errors out, before resetDatabase drops anything,
// if any table other than the migrations runner's own bookkeeping table
// already contains a row. dingo's own migrations never insert into a
// domain table -- only schema_migrations records bookkeeping rows
// (verified: no migration file under sqlstore/migrations/v1 contains an
// INSERT INTO a domain table) -- so a database restoreMetadataStore's
// brief resolve-and-start just finished migrating has zero rows in
// everything else. A nonzero count anywhere else means this target isn't
// that: most plausibly a live node's own database, pointed at by a reused
// or misconfigured DSN, whose accumulated real data resetDatabase's
// unconditional DROP TABLE would otherwise destroy with no way back.
//
// The exemption is scoped to (schema, name), not name alone: resetDatabase
// scans every non-system schema, and migrations/runner.go's own
// hasUserTables query creates schema_migrations in current_schema()
// (typically "public", but operator-configurable via search_path) -- a
// same-named table an operator's own tooling created in some OTHER schema
// is not dingo's bookkeeping table, and a name-only exemption would let a
// populated one of those slip past this check and then get dropped anyway.
func refuseIfTargetHasData(
	ctx context.Context,
	db *sql.DB,
	tables []qualifiedTable,
) error {
	var migrationsSchema string
	if err := db.QueryRowContext(
		ctx, "SELECT current_schema()",
	).Scan(&migrationsSchema); err != nil {
		return fmt.Errorf("determine current schema: %w", err)
	}
	for _, t := range tables {
		if t.name == migrationsTableName && t.schema == migrationsSchema {
			continue
		}
		quoted := pgQuoteIdentifier(t.schema) + "." + pgQuoteIdentifier(t.name)
		var hasData bool
		err := db.QueryRowContext(
			ctx, "SELECT EXISTS (SELECT 1 FROM "+quoted+")",
		).Scan(&hasData)
		if err != nil {
			return fmt.Errorf("check table %s for data: %w", quoted, err)
		}
		if hasData {
			return fmt.Errorf(
				"postgres reset: table %s already contains data -- "+
					"refusing to reset a target that isn't a freshly "+
					"migrated, empty database (this looks like a live "+
					"database's own data, not something restoreMetadataStore's "+
					"brief resolve-and-start just created)",
				quoted,
			)
		}
	}
	return nil
}

// pgQuoteIdentifier double-quotes a Postgres identifier, doubling any
// embedded quote character -- table/schema names here come from
// information_schema itself (already-valid identifiers reflected back by
// the server), not external input, but quoting defensively costs nothing.
func pgQuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
