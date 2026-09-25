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

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func alonzoPParamsUnitBackfillDB(
	t *testing.T,
) (*sql.DB, func([]migrations.Migration)) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 27)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(ctx))
	}
	runTo(registry[:19])
	return db, runTo
}

func alonzoPParamsUnitMarker(t *testing.T, db *sql.DB) string {
	t.Helper()
	var got string
	require.NoError(t, db.QueryRowContext(context.Background(), `
SELECT value FROM node_settings_gate WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitGateName,
	).Scan(&got))
	return got
}

func seedPParamsEra(t *testing.T, db *sql.DB, eraID uint) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO pparams (id, cbor, added_slot, epoch, era_id)
VALUES (?, X'80', 0, 0, ?)`, eraID, eraID)
	require.NoError(t, err)
}

func TestAlonzoPParamsUnitBackfillMarksFreshDatabaseWordV1(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillMarksLegacyAlonzoRows(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	// Seeded through gouroboros' own era id, so a renumbering upstream
	// diverges from the era_id the backfill hardcodes and fails here rather
	// than silently leaving Alonzo rows unclassified.
	seedPParamsEra(t, db, alonzo.EraIdAlonzo)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitLegacyByteV0,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillDoesNotMisclassifyBabbageRows(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	seedPParamsEra(t, db, 5)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillPreservesExistingMarker(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	_, err := db.ExecContext(context.Background(), `
INSERT INTO node_settings_gate (name, value, recorded_epoch, recorded_slot)
VALUES (?, ?, 7, 9)`,
		nodesettings.AlonzoPParamsUnitGateName,
		nodesettings.AlonzoPParamsUnitWordV1,
	)
	require.NoError(t, err)
	seedPParamsEra(t, db, 4)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillReplayIsIdempotent(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)
	_, err = db.ExecContext(context.Background(), `
UPDATE schema_migrations
SET phase = 'backfill', cursor = '', dirty = 1, completed_at = NULL
WHERE version = 20`)
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}
