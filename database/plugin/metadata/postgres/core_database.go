//go:build dingo_extra_plugins

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

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// coreDSN selects schema as the only schema of dsn's search_path, keeping
// every connection, TLS and session option. The search_path startup
// parameter is applied after any options='-c search_path=...', so it wins.
// The schema is the only one on the path so that a table missing from it is
// an error rather than a silent read of the IndexerDB's table of that name.
func coreDSN(dsn, schema string) (string, error) {
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return "", fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	// libpq keyword strings use the last occurrence of a keyword.
	escaped := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(schema)
	return dsn + " search_path='" + escaped + "'", nil
}

// prepareCoreNamespace selects the consensus schema beside the adopted API
// schema in the same database, creating it when absent. Creating a schema
// needs only CREATE on the database: no CREATEDB and no new pg_hba.conf entry.
func prepareCoreNamespace(ctx context.Context, cfg Config) (Config, string, error) {
	dsn := assembleDSN(cfg)
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return Config{}, "", fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	source, err := sqlstore.OpenDB("pgx", dsn, "postgresql", false)
	if err != nil {
		return Config{}, "", fmt.Errorf("open API PostgreSQL database: %w", err)
	}
	defer source.Close()
	var database, schema, role string
	if err := source.QueryRowContext(ctx, "SELECT current_database(), COALESCE(current_schema(), ''), current_user").
		Scan(&database, &schema, &role); err != nil {
		return Config{}, "", describeConnectError(err, parsed.User, parsed.Database)
	}
	if schema == "" {
		return Config{}, "", fmt.Errorf(
			"the search_path of role %q on PostgreSQL database %q names no existing schema, so the API schema the consensus schema is derived from is unknown",
			role, database,
		)
	}
	name := metadata.CoreNamespaceName(database, schema)
	var exists bool
	if err := source.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", name).Scan(&exists); err != nil {
		return Config{}, "", fmt.Errorf("check PostgreSQL consensus schema %q: %w", name, err)
	}
	if !exists {
		if _, err := source.ExecContext(ctx, "CREATE SCHEMA "+pgQuoteIdentifier(name)); err != nil {
			var pgErr *pgconn.PgError
			switch {
			case errors.As(err, &pgErr) && pgErr.Code == "42P06":
				// Created concurrently by another process.
			case errors.As(err, &pgErr) && pgErr.Code == "42501":
				return Config{}, "", fmt.Errorf(
					"create the consensus schema %q in PostgreSQL database %q: role %q lacks CREATE on the database; run GRANT CREATE ON DATABASE %s TO %s and restart, the API schema %q is unchanged: %w",
					name, database, role,
					pgQuoteIdentifier(database), pgQuoteIdentifier(role),
					schema, err,
				)
			default:
				return Config{}, "", fmt.Errorf(
					"create the consensus schema %q in PostgreSQL database %q: %w",
					name, database, err,
				)
			}
		}
	}
	selected, err := coreDSN(dsn, name)
	if err != nil {
		return Config{}, "", err
	}
	cfg.DSN = selected
	return cfg, name, nil
}

// describeConnectError names the role and database of a failed connection
// and what the server refused: the credentials, or CONNECT on the database.
func describeConnectError(err error, role, database string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "28"):
			return fmt.Errorf(
				"authenticate role %q to PostgreSQL database %q: check the configured credentials and the server's pg_hba.conf: %w",
				role, database, err,
			)
		case pgErr.Code == "42501":
			return fmt.Errorf(
				"connect role %q to PostgreSQL database %q: run GRANT CONNECT ON DATABASE %s TO %s: %w",
				role, database,
				pgQuoteIdentifier(database), pgQuoteIdentifier(role), err,
			)
		case pgErr.Code == "3D000":
			return fmt.Errorf(
				"PostgreSQL database %q does not exist: %w", database, err,
			)
		}
	}
	return fmt.Errorf(
		"resolve the API PostgreSQL database and schema of role %q: %w",
		role, err,
	)
}
