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
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// releasedSchemaVersion is the metadata schema version of the v0.94.2
// release.
const releasedSchemaVersion = 68

// backfillStore is one database a test opens as different binaries do.
type backfillStore struct {
	dialect  Dialect
	registry []migrations.Migration
	locker   migrations.Locker
	// open returns a new pool on the database.
	open func(t *testing.T) *sql.DB
	// fingerprint lists single-column queries over the schema objects.
	fingerprint []string
}

// start opens the database with the first versions migrations of the
// registry, as a binary whose schema ends there does, and returns the store
// and the error of its Start.
func (b backfillStore) start(
	t *testing.T,
	versions int,
	mode string,
) (*Store, error) {
	t.Helper()
	db := b.open(t)
	store, err := New(Config{
		WriteDB:           db,
		Dialect:           b.dialect,
		StorageMode:       mode,
		Migrations:        b.registry[:versions],
		MigrationLocker:   b.locker,
		LazyHotStatements: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, store.Start(context.Background())
}

// state returns the schema objects, applied migrations, sync state and node
// settings of the database.
func (b backfillStore) state(t *testing.T) string {
	t.Helper()
	db := b.open(t)
	defer db.Close()
	var out strings.Builder
	for _, query := range append(
		b.fingerprint,
		`SELECT name FROM schema_migrations ORDER BY version`,
		`SELECT phase FROM schema_migrations ORDER BY version`,
		`SELECT sync_key FROM sync_state ORDER BY sync_key`,
		`SELECT value FROM sync_state ORDER BY sync_key`,
		`SELECT name FROM node_settings_gate ORDER BY name`,
		`SELECT value FROM node_settings_gate ORDER BY name`,
	) {
		rows, err := db.Query(query)
		require.NoError(t, err, query)
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			out.WriteString(line + "\n")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		out.WriteString("--\n")
	}
	return out.String()
}

// testRefusesInterruptedBackfillBeforeUpgrade writes the API-mode store the
// v0.94.2 release leaves when its Mithril bootstrap stops during the metadata
// backfill, and opens it with this binary's schema. The open refuses before
// it migrates anything, so the store is unchanged and the release, which
// refuses a schema newer than its own, still opens it.
func testRefusesInterruptedBackfillBeforeUpgrade(
	t *testing.T,
	b backfillStore,
) {
	t.Helper()
	ctx := context.Background()
	released, err := b.start(t, releasedSchemaVersion, types.StorageModeAPI)
	require.NoError(t, err)
	require.NoError(t, released.SetNodeSettingsGates(
		ctx, nodesettings.Values{"storage_mode": "api"}, 0, 0,
	))
	require.NoError(t, released.SetSyncState("sync_status", "backfill", nil))
	require.NoError(t, released.Close())
	before := b.state(t)

	_, err = b.start(t, len(b.registry), types.StorageModeAPI)
	require.ErrorIs(t, err, metadata.ErrInterruptedBackfill)
	require.ErrorContains(t, err, "start dingo v0.94.2")
	require.Equal(t, before, b.state(t),
		"the refusal changed the released store")

	released, err = b.start(t, releasedSchemaVersion, types.StorageModeAPI)
	require.NoError(t, err, "the release no longer opens the store")
	status, err := released.GetSyncState("sync_status", nil)
	require.NoError(t, err)
	require.Equal(t, "backfill", status)

	// The release finishes the backfill and clears the status; the store
	// then upgrades, after which the release refuses it.
	require.NoError(t, released.DeleteSyncState("sync_status", nil))
	require.NoError(t, released.Close())
	upgraded, err := b.start(t, len(b.registry), types.StorageModeAPI)
	require.NoError(t, err)
	require.NoError(t, upgraded.Close())
	_, err = b.start(t, releasedSchemaVersion, types.StorageModeAPI)
	require.ErrorIs(t, err, migrations.ErrNewerSchema)
}

func TestSQLiteRefusesInterruptedBackfillBeforeUpgrade(t *testing.T) {
	t.Parallel()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	testRefusesInterruptedBackfillBeforeUpgrade(t, backfillStore{
		dialect:  SQLiteDialect(),
		registry: registry,
		locker:   migrations.NewProcessLocker(),
		open: func(t *testing.T) *sql.DB {
			t.Helper()
			db, err := sql.Open(
				"sqlite",
				path+"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
			)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			return db
		},
		fingerprint: []string{
			`SELECT type || ' ' || name || ' ' || COALESCE(sql, '') FROM sqlite_master ORDER BY type, name`,
		},
	})
}

// A store that left API mode keeps no API history to finish, and one already
// at this binary's schema is past the point the release can open it; neither
// is refused by the open.
func TestInterruptedBackfillRefusalLeavesOtherStoresAlone(t *testing.T) {
	t.Parallel()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		versions int
		mode     string
	}{
		{name: "core mode", versions: releasedSchemaVersion, mode: "core"},
		{name: "current schema", versions: len(registry), mode: "api"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "metadata.sqlite")
			b := backfillStore{
				dialect:  SQLiteDialect(),
				registry: registry,
				locker:   migrations.NewProcessLocker(),
				open: func(t *testing.T) *sql.DB {
					t.Helper()
					db, err := sql.Open(
						"sqlite",
						path+"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
					)
					require.NoError(t, err)
					db.SetMaxOpenConns(1)
					return db
				},
			}
			store, err := b.start(t, tc.versions, tc.mode)
			require.NoError(t, err)
			require.NoError(t, store.SetNodeSettingsGates(
				t.Context(),
				nodesettings.Values{"storage_mode": tc.mode}, 0, 0,
			))
			require.NoError(t, store.SetSyncState("sync_status", "backfill", nil))
			require.NoError(t, store.Close())
			_, err = b.start(t, len(registry), tc.mode)
			require.NoError(t, err)
		})
	}
}
