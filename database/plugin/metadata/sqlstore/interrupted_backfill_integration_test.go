//go:build dingo_db_integration

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

package sqlstore

import (
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func TestPostgresRefusesInterruptedBackfillBeforeUpgrade(t *testing.T) {
	t.Parallel()
	dsn, schema := newPostgresIntegrationSchema(t)
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	testRefusesInterruptedBackfillBeforeUpgrade(t, backfillStore{
		dialect:  PostgresDialect(),
		registry: registry,
		locker:   integrationMigrationLocker("postgres", schema),
		open: func(t *testing.T) *sql.DB {
			t.Helper()
			db, err := OpenDB("pgx", dsn, "postgres", false)
			require.NoError(t, err)
			return db
		},
		fingerprint: []string{
			`SELECT table_name || ' ' || column_name || ' ' || data_type || ' ' || is_nullable FROM information_schema.columns WHERE table_schema = current_schema() ORDER BY 1`,
			`SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() ORDER BY 1`,
		},
	})
}

func TestMySQLRefusesInterruptedBackfillBeforeUpgrade(t *testing.T) {
	t.Parallel()
	dsn, database := newMySQLIntegrationDatabase(t)
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	testRefusesInterruptedBackfillBeforeUpgrade(t, backfillStore{
		dialect:  MySQLDialect(),
		registry: registry,
		locker:   integrationMigrationLocker("mysql", database),
		open: func(t *testing.T) *sql.DB {
			t.Helper()
			db, err := OpenDB("mysql", dsn, "mysql", false)
			require.NoError(t, err)
			return db
		},
		fingerprint: []string{
			`SELECT CONCAT(table_name, ' ', column_name, ' ', column_type, ' ', is_nullable) FROM information_schema.columns WHERE table_schema = DATABASE() ORDER BY 1`,
			`SELECT CONCAT(table_name, ' ', index_name, ' ', seq_in_index, ' ', column_name) FROM information_schema.statistics WHERE table_schema = DATABASE() ORDER BY 1`,
		},
	})
}
