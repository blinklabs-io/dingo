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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// TestLedgerStateDrainSchemaStopsAnOlderBinary migrates a database to the
// version that carries the drain latch and requires a runner holding only the
// previous registry to refuse it with ErrNewerSchema, while the full registry
// opens it and replays cleanly.
func TestLedgerStateDrainSchemaStopsAnOlderBinary(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 68)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 77)
	runner := func(reg []migrations.Migration) migrations.Runner {
		return migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: reg,
			Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
		}
	}

	previous := runner(registry[:68])
	require.NoError(
		t,
		previous.Run(context.Background()),
		"v68 binary on a v68 database",
	)

	current := runner(registry)
	require.NoError(t, current.Run(context.Background()))
	require.NoError(t, current.Run(context.Background()), "replay")

	err = previous.Run(context.Background())
	require.ErrorIs(t, err, migrations.ErrNewerSchema)
}
