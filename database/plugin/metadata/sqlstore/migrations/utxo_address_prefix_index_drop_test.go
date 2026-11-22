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

func TestUtxoAddressPrefixIndexDropMigration(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 74)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	hasIndex := func(name string) bool {
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master
WHERE type = 'index' AND name = ?`, name,
		).Scan(&count))
		return count == 1
	}
	require.True(t, hasIndex("idx_utxo_address_address"))

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(ctx))

	require.False(t, hasIndex("idx_utxo_address_address"))
	require.True(t, hasIndex("idx_utxo_address_order"))
}
