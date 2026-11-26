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

package sqlite

import (
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

type allowAllGuard struct{}

func (allowAllGuard) CheckStatement(string) error { return nil }

// A traced or guarded connection hides the driver's restore interface, so
// the template seed cannot restore into it. Such an in-memory store must
// still open and reach the migrated schema by running every migration
// itself, which the upgrade check reports as starting from version 0.
func TestWrappedInMemoryStoreMigratesItself(t *testing.T) {
	t.Parallel()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	target := registry[len(registry)-1].Version

	for name, deps := range map[string]metadata.ProviderDependencies{
		"tracing":        {TracingEnabled: true},
		"statementGuard": {StatementGuard: allowAllGuard{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var logs syncBuffer
			deps.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			store, writeDB, _, err := openSQLStore(Config{}, deps)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			require.NoError(t, store.Start(t.Context()))

			var check string
			for line := range strings.SplitSeq(logs.String(), "\n") {
				if strings.Contains(line, "metadata schema upgrade check") {
					check = line
				}
			}
			require.NotEmpty(t, check, "migration runner never reported")
			require.Contains(
				t, check, "completed_migrations=0 ",
				"wrapped store was restored from the template",
			)

			var complete int
			var highest int64
			require.NoError(t, writeDB.QueryRowContext(
				t.Context(),
				"SELECT COUNT(*), COALESCE(MAX(version), 0) "+
					"FROM schema_migrations WHERE phase = ?",
				string(migrations.PhaseComplete),
			).Scan(&complete, &highest))
			require.Equal(t, len(registry), complete)
			require.Equal(t, target, highest,
				"schema not migrated to version "+strconv.FormatInt(target, 10))
		})
	}
}
