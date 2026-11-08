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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func TestUtxoStakingIndexesUpgrade(t *testing.T) {
	t.Parallel()
	for _, pending := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "normal", true: "deferred"}[pending],
			func(t *testing.T) {
				t.Parallel()
				path := filepath.Join(t.TempDir(), "metadata.sqlite")
				migrations.SeedMigratedPrefix(t, path, 52)
				db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				registry, err := migrations.SQLiteRegistry()
				require.NoError(t, err)
				run := func(registry []migrations.Migration) {
					runner := migrations.Runner{
						DB:       db,
						Dialect:  "sqlite",
						Registry: registry,
						Locker: migrations.NewFileLocker(
							path + ".migrate.lock",
						),
					}
					require.NoError(t, runner.Run(context.Background()))
				}
				for _, name := range []string{"idx_utxo_staking_key", "idx_utxo_staking_deleted_amount"} {
					definition, ok := indexSQL(t, db, name)
					require.True(t, ok)
					require.NotContains(t, definition, "WHERE")
				}
				if pending {
					_, err = db.Exec(
						"INSERT INTO sync_state(sync_key,value) VALUES (?,?)",
						deferred.SyncStateKey,
						deferred.SyncStateValue,
					)
					require.NoError(t, err)
					_, err = db.Exec(
						"DROP INDEX idx_utxo_staking_deleted_amount",
					)
					require.NoError(t, err)
				}
				run(registry)
				for _, name := range []string{"idx_utxo_staking_key", "idx_utxo_staking_deleted_amount"} {
					definition, ok := indexSQL(t, db, name)
					if pending && name == "idx_utxo_staking_key" {
						require.False(t, ok)
						continue
					}
					require.True(t, ok)
					require.Contains(
						t,
						definition,
						"WHERE staking_key IS NOT NULL",
					)
				}
				definition, ok := indexSQL(t, db, "idx_utxo_deleted_staking_amount")
				require.True(t, ok)
				require.NotContains(t, definition, "WHERE")
				// The running script-locked aggregate replaced this index.
				_, ok = indexSQL(t, db, "idx_utxo_deleted_payment_script")
				require.False(t, ok)
				run(registry)
			},
		)
	}
}
