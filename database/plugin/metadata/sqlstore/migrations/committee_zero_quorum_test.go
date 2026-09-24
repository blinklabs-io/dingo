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
	_ "modernc.org/sqlite"
)

func TestCommitteeZeroQuorumMigrationConvertsLegacyClearMarkers(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB: db, Dialect: "sqlite", Registry: versions,
			Locker: migrations.NewFileLocker(databasePath + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	run(registry[:18])
	_, err = db.Exec(
		"INSERT INTO committee_quorum (quorum, added_slot) VALUES ('0', 10)",
	)
	require.NoError(t, err)
	run(registry)

	var quorum sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT quorum FROM committee_quorum WHERE added_slot = 10",
	).Scan(&quorum))
	require.False(t, quorum.Valid, "legacy zero was a clear marker")
}
