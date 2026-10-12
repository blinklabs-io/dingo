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

package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// addColumnDefinitionCases pair the definition an interrupted migration left
// behind with the one the replayed ADD COLUMN declares.
var addColumnDefinitionCases = []struct {
	name     string
	existing string
	declared string
	wantErr  bool
}{
	{"identical", "bigint NOT NULL DEFAULT 0", "bigint NOT NULL DEFAULT 0", false},
	{"boolean", "boolean NOT NULL DEFAULT FALSE", "boolean NOT NULL DEFAULT FALSE", false},
	{"nullable no default", "text", "text", false},
	{"declared not null, existing nullable", "bigint DEFAULT 0", "bigint NOT NULL DEFAULT 0", true},
	{"declared nullable, existing not null", "bigint NOT NULL DEFAULT 0", "bigint DEFAULT 0", true},
	{"declared default, existing none", "bigint NOT NULL", "bigint NOT NULL DEFAULT 0", true},
	{"declared none, existing default", "bigint NOT NULL DEFAULT 0", "bigint NOT NULL", true},
	{"different default", "bigint NOT NULL DEFAULT 1", "bigint NOT NULL DEFAULT 0", true},
	{"different boolean default", "boolean NOT NULL DEFAULT TRUE", "boolean NOT NULL DEFAULT FALSE", true},
	{"unverifiable constraint", "bigint UNIQUE", "bigint UNIQUE", true},
}

func runAddColumnReplay(
	db *sql.DB,
	dialect string,
	table string,
	existing string,
	declared string,
) error {
	runner := migrations.Runner{
		DB:      db,
		Dialect: dialect,
		Registry: []migrations.Migration{{
			Version:          1,
			Name:             "add_column_definition",
			BackfillRevision: "1",
			SQL: map[string]migrations.SQL{
				dialect: {Expand: []string{
					"CREATE TABLE " + table + " (id bigint PRIMARY KEY, n " + existing + ")",
					"ALTER TABLE " + table + " ADD COLUMN n " + declared,
				}},
			},
		}},
		Locker: migrations.NewProcessLocker(),
	}
	return runner.Run(context.Background())
}

func requireAddColumnReplayOutcome(
	t *testing.T,
	err error,
	wantErr bool,
) {
	t.Helper()
	if wantErr {
		require.ErrorContains(t, err, "statement 2")
		return
	}
	require.NoError(t, err)
}

func TestAddColumnReplayVerifiesDefinitionOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := postgresEngineTestDSN(t)
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping())

	for i, tc := range addColumnDefinitionCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema := fmt.Sprintf("dingo_addcol_def_%d", i)
			decoy := schema + "_decoy"
			for _, s := range []string{schema, decoy} {
				_, err := admin.Exec(`DROP SCHEMA IF EXISTS "` + s + `" CASCADE`)
				require.NoError(t, err)
				_, err = admin.Exec(`CREATE SCHEMA "` + s + `"`)
				require.NoError(t, err)
			}
			t.Cleanup(func() {
				for _, s := range []string{schema, decoy} {
					_, _ = admin.Exec(`DROP SCHEMA IF EXISTS "` + s + `" CASCADE`)
				}
			})
			// A same-named table elsewhere in the database whose column
			// already has the declared definition: the lookup must be bound
			// to the migration's own schema or it would accept this one.
			_, err = admin.Exec(
				`CREATE TABLE "` + decoy + `".item (id bigint PRIMARY KEY, n ` +
					tc.declared + `)`,
			)
			require.NoError(t, err)
			db, err := sql.Open(
				"pgx",
				storagetest.PostgresDSNWithSearchPath(dsn, schema),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			requireAddColumnReplayOutcome(
				t,
				runAddColumnReplay(db, "postgres", "item", tc.existing, tc.declared),
				tc.wantErr,
			)
		})
	}
}

func TestAddColumnReplayVerifiesDefinitionOnMySQL(t *testing.T) {
	t.Parallel()
	rootDSN := mysqlEngineTestRootDSN(t)
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping())

	for i, tc := range addColumnDefinitionCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			database := fmt.Sprintf("dingo_addcol_def_%d", i)
			decoy := fmt.Sprintf("dingo_addcol_decoy_%d", i)
			_, err := admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
			require.NoError(t, err)
			_, err = admin.Exec("CREATE DATABASE `" + database + "`")
			require.NoError(t, err)
			_, err = admin.Exec("DROP DATABASE IF EXISTS `" + decoy + "`")
			require.NoError(t, err)
			_, err = admin.Exec("CREATE DATABASE `" + decoy + "`")
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
				_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + decoy + "`")
			})
			_, err = admin.Exec(
				"CREATE TABLE `" + decoy + "`.item (id bigint PRIMARY KEY, n " +
					tc.declared + ")",
			)
			require.NoError(t, err)
			cfg, err := mysqldriver.ParseDSN(rootDSN)
			require.NoError(t, err)
			cfg.DBName = database
			db, err := sql.Open("mysql", cfg.FormatDSN())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			requireAddColumnReplayOutcome(
				t,
				runAddColumnReplay(db, "mysql", "item", tc.existing, tc.declared),
				tc.wantErr,
			)
		})
	}
}
