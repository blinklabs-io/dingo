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
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/gouroboros/cbor"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func rewardUpgradeStartFixture(
	t *testing.T,
	emptyGenesis bool,
) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		filepath.Join(
			t.TempDir(),
			"metadata.sqlite",
		)+"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(registry), 41)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := migrations.Runner{
		DB: db, Dialect: "sqlite", Registry: registry[:34],
		Locker: migrations.NewProcessLocker(), Logger: logger,
	}
	require.NoError(t, runner.Run(t.Context()))
	for _, query := range []string{
		`INSERT INTO epoch (epoch_id, start_slot, era_id, slot_length, length_in_slots) VALUES (658, 280000000, 6, 1, 432000)`,
		`INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash, total_stake, delegator_count, captured_slot, calculation_version) VALUES (658, 'mark', X'010203', '123456789', 1, 279999999, 2)`,
		`INSERT INTO reward_snapshot (epoch, snapshot_type, total_active_stake, total_pool_count, total_delegators, captured_slot, boundary_slot, protocol_version, calculation_version) VALUES (658, 'mark', '123456789', 1, 1, 279999999, 280000000, 10, 2)`,
	} {
		_, err := db.ExecContext(t.Context(), query)
		require.NoError(t, err)
	}
	params := mockledger.NewMockConwayProtocolParams()
	params.ProtocolVersion.Major = 9
	raw, err := cbor.Encode(&params)
	require.NoError(t, err)
	_, err = db.ExecContext(
		t.Context(),
		`INSERT INTO pparams (era_id, epoch, added_slot, cbor) VALUES (6, 658, 280000000, ?)`,
		raw,
	)
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB: db, Dialect: SQLiteDialect(), Migrations: registry,
		MigrationLocker: migrations.NewProcessLocker(), Logger: logger,
		GenesisStakingEmpty: emptyGenesis,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, db
}

func rewardUpgradeDatabaseState(t *testing.T, db *sql.DB) []string {
	t.Helper()
	var state []string
	for _, query := range []string{
		`SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_master ORDER BY type, name`,
		`SELECT version, name, checksum, phase, cursor, dirty, started_at, updated_at, completed_at FROM schema_migrations ORDER BY version`,
		`SELECT epoch, snapshot_type, total_stake, calculation_version FROM pool_stake_snapshot ORDER BY id`,
		`SELECT epoch, snapshot_type, total_active_stake, calculation_version, protocol_version FROM reward_snapshot ORDER BY id`,
		`SELECT * FROM epoch ORDER BY id`,
		`SELECT * FROM pparams ORDER BY id`,
	} {
		rows, err := db.QueryContext(t.Context(), query)
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for index := range values {
				pointers[index] = &values[index]
			}
			require.NoError(t, rows.Scan(pointers...))
			state = append(state, fmt.Sprint(values))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return state
}

func TestRewardStakeUpgradeStartCertifiesBeforeMigrations(t *testing.T) {
	t.Parallel()
	store, db := rewardUpgradeStartFixture(t, true)
	require.NoError(t, store.Start(t.Context()))
	require.True(t, store.Ready())
	var version int
	require.NoError(
		t,
		db.QueryRowContext(t.Context(), `SELECT MAX(version) FROM schema_migrations`).
			Scan(&version),
	)
	require.Equal(t, 42, version)
	for _, table := range []string{"pool_stake_snapshot", "reward_snapshot"} {
		require.NoError(
			t,
			db.QueryRowContext(t.Context(), "SELECT calculation_version FROM "+table).
				Scan(&version),
		)
		require.Equal(t, 3, version)
	}
	var totalStake string
	require.NoError(
		t,
		db.QueryRowContext(t.Context(), `SELECT total_stake FROM pool_stake_snapshot`).
			Scan(&totalStake),
	)
	require.Equal(t, "123456789", totalStake)
}

func TestRewardStakeUpgradeStartCertifiesCurrentSchema(t *testing.T) {
	t.Parallel()
	store, db := rewardUpgradeStartFixture(t, true)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB: db, Dialect: "sqlite", Registry: registry,
		Locker: migrations.NewProcessLocker(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	require.NoError(t, runner.Run(t.Context()))
	var version int
	require.NoError(
		t,
		db.QueryRowContext(t.Context(), `SELECT MAX(version) FROM schema_migrations`).
			Scan(&version),
	)
	require.Equal(t, len(registry), version)
	require.NoError(t, store.Start(t.Context()))
	for _, table := range []string{"pool_stake_snapshot", "reward_snapshot"} {
		require.NoError(
			t,
			db.QueryRowContext(t.Context(), "SELECT calculation_version FROM "+table).
				Scan(&version),
		)
		require.Equal(t, 3, version)
	}
}

func TestRewardStakeUpgradeStartRefusalPreservesSchema(t *testing.T) {
	t.Parallel()
	for _, emptyGenesis := range []bool{false, true} {
		t.Run(
			fmt.Sprintf("empty_genesis_%t", emptyGenesis),
			func(t *testing.T) {
				t.Parallel()
				store, db := rewardUpgradeStartFixture(t, emptyGenesis)
				if emptyGenesis {
					_, err := db.ExecContext(
						t.Context(),
						`UPDATE reward_snapshot SET protocol_version = 12`,
					)
					require.NoError(t, err)
				}
				before := rewardUpgradeDatabaseState(t, db)
				require.Error(t, store.Start(t.Context()))
				require.False(t, store.Ready())
				require.Equal(
					t,
					before,
					rewardUpgradeDatabaseState(t, db),
					"failed startup must preserve schema, migration journal and snapshot provenance",
				)
			},
		)
	}
}
