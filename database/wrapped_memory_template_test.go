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

package database

import (
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/stretchr/testify/require"
)

type allowAllStatementGuard struct{}

func (allowAllStatementGuard) CheckStatement(string) error { return nil }

func migratedVersionCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var tables int
	require.NoError(t, db.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sqlite_master "+
			"WHERE type = 'table' AND name = 'schema_migrations'",
	).Scan(&tables))
	if tables == 0 {
		return 0
	}
	var versions int
	require.NoError(t, db.QueryRowContext(
		t.Context(), "SELECT COUNT(*) FROM schema_migrations",
	).Scan(&versions))
	return versions
}

// The template seed restores through the driver connection's restore
// interface. A traced or guarded connection hides it, and the seed must then
// leave the database empty without failing, so the store opening it runs its
// own migrations. A plain connection is the control: the same seed restores
// the migrated schema into it.
func TestSeedMigratedMemorySkipsWrappedDrivers(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		tracing  bool
		guard    metadata.StatementGuard
		restored bool
	}{
		"plain":          {restored: true},
		"tracing":        {tracing: true},
		"statementGuard": {guard: allowAllStatementGuard{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, err := sqlstore.OpenGuardedDB(
				"sqlite",
				"file:database_pkg_wrapped_seed_"+name+
					"?mode=memory&cache=shared",
				"sqlite", tc.tracing, tc.guard,
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			db.SetMaxOpenConns(1)

			require.NoError(t, seedMigratedMemory(db, ""))
			versions := migratedVersionCount(t, db)
			if tc.restored {
				require.Positive(t, versions, "plain driver was not restored")
			} else {
				require.Zero(t, versions, "wrapped driver was restored")
			}
		})
	}
}
