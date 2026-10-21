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
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// legacyEpochStore returns a store migrated through the schema version before
// the registration epoch backfill, the raw database behind it, and a function
// applying the rest of the registry.
func legacyEpochStore(t *testing.T) (*Store, *sql.DB, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open(
		"sqlite",
		"file:"+path+"?_pragma=busy_timeout(5000)&_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	last := len(registry) - 1
	require.Equal(t, "leios-key-registration-epoch-backfill", registry[last].Name)
	locker := migrations.NewProcessLocker()
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry[:last],
		MigrationLocker: locker,
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
		require.NoError(t, db.Close())
	})
	return store, db, func() {
		t.Helper()
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker:   locker,
		}
		require.NoError(t, runner.Run(context.Background()))
	}
}

type leiosEpochRow struct {
	id         int64
	epoch      sql.NullInt64
	ageUnknown bool
}

func (r leiosEpochRow) String() string {
	return fmt.Sprintf("{id=%d epoch=%v/%d ageUnknown=%v}",
		r.id, r.epoch.Valid, r.epoch.Int64, r.ageUnknown)
}

func readLeiosEpochRows(t *testing.T, db *sql.DB) []leiosEpochRow {
	t.Helper()
	rows, err := db.Query(`
SELECT id, leios_key_registration_epoch, leios_key_registration_age_unknown
FROM pool_registration ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var out []leiosEpochRow
	for rows.Next() {
		var row leiosEpochRow
		require.NoError(t, rows.Scan(&row.id, &row.epoch, &row.ageUnknown))
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func leiosRowsString(rows []leiosEpochRow) string {
	parts := make([]string, len(rows))
	for i, row := range rows {
		parts[i] = row.String()
	}
	return strings.Join(parts, "\n")
}

// A database written before the registration epoch followed the ledger's
// replay holds NULL for every live keyed registration, and rotation then falls
// back to the transaction epoch plus one. The backfill recomputes each such
// row with the write path's own replay, so a migrated database and one synced
// from scratch store the same column for every registration.
func TestPoolLeiosKeyEpochBackfillMatchesWritePath(t *testing.T) {
	t.Parallel()
	store, db, upgrade := legacyEpochStore(t)
	depositHeldEpochs(t, store, 12)

	genesis := depositHeldPoolKey(0xe1)
	fresh := depositHeldPoolKey(0xe2)
	imported := depositHeldPoolKey(0xe3)
	reaped := depositHeldPoolKey(0xe4)
	keyless := depositHeldPoolKey(0xe5)

	genesisKeyedPool(t, store, genesis)
	importLeiosKeyPool(t, store, imported, 4_060, nil, true)
	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(genesis, 0x11, 0x21), 0)
	writeDepositHeldCert(t, store, 8_100, 0, leiosKeyEpochCert(genesis, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(fresh, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(fresh, 0x11, 0x21), 0)
	writeDepositHeldCert(t, store, 6_300, 0, leiosKeyEpochCert(fresh, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 7_300, 0, leiosKeyEpochCert(fresh, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 6_200, 0, leiosKeyEpochCert(imported, 0x11, 0x21), 0)
	writeDepositHeldCert(t, store, 2_200, 0, leiosKeyEpochCert(reaped, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_200, 0, depositHeldRetirement(reaped, 4), 0)
	writeDepositHeldCert(t, store, 5_200, 0, leiosKeyEpochCert(reaped, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_300, 0, leiosKeyEpochCert(keyless, 0, 0), 500)

	want := readLeiosEpochRows(t, db)
	// The write path stored something for every case that matters here.
	require.Contains(t, leiosRowsString(want), "epoch=true/9", "rotation must be recorded")
	require.Contains(t, leiosRowsString(want), "ageUnknown=true", "imported age must be carried")

	// Put the database back in the shape a release before the replay left it:
	// live keyed rows carry no epoch and no unknown-age mark, and the genesis
	// row was written without an epoch.
	_, err := db.Exec(`
UPDATE pool_registration
SET leios_key_registration_epoch = NULL,
    leios_key_registration_age_unknown = FALSE
WHERE certificate_id <> 0`)
	require.NoError(t, err)
	_, err = db.Exec(`
UPDATE pool_registration SET leios_key_registration_epoch = NULL
WHERE certificate_id = 0 AND added_slot = 0
  AND leios_key_registration_age_unknown = FALSE`)
	require.NoError(t, err)
	require.NotEqual(t, leiosRowsString(want), leiosRowsString(readLeiosEpochRows(t, db)))

	upgrade()

	got := readLeiosEpochRows(t, db)
	require.Equal(t, leiosRowsString(want), leiosRowsString(got))
}

// Networks that never carried a BLS key hold no keyed registration, so the
// backfill must leave their rows exactly as they were.
func TestPoolLeiosKeyEpochBackfillLeavesKeylessDatabaseUnchanged(t *testing.T) {
	t.Parallel()
	store, db, upgrade := legacyEpochStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xe6)
	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0, 0), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0, 0), 0)
	require.NoError(t, store.SetGenesisStakingWithDeposits(
		map[string]lcommon.PoolRegistrationCertificate{
			"aa": *leiosKeyEpochCert(depositHeldPoolKey(0xe7), 0, 0),
		},
		map[string]string{}, 2_000_000, 500, nil, nil,
	))
	before := leiosRowsString(readLeiosEpochRows(t, db))
	require.NotEmpty(t, before)

	upgrade()

	require.Equal(t, before, leiosRowsString(readLeiosEpochRows(t, db)))
}
