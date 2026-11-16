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

// TestImmutableDBSchemaReleaseStopsOlderBinaries covers the downgrade guard: a
// database migrated through the ImmutableDB release is refused, as a newer
// schema, by a binary whose registry ends one version earlier.
func TestImmutableDBSchemaReleaseStopsOlderBinaries(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, databasePath, 1)
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Equal(t, "immutable-db", registry[len(registry)-1].Name)

	run := func(registry []migrations.Migration) error {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		return runner.Run(context.Background())
	}
	require.NoError(t, run(registry))
	require.ErrorIs(
		t, run(registry[:len(registry)-1]), migrations.ErrNewerSchema,
	)
}
