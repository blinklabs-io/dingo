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

// restampBackfillDB returns a database migrated through the version before
// the reward-stake calculation-version restamp backfill runs, so a test can
// seed legacy (version-1) snapshot rows for it to see.
func restampBackfillDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, databasePath, 13)
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 65)

	runRestamp := func() {
		full := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, full.Run(ctx))
	}
	return db, runRestamp
}

func seedEpochSummary(
	t *testing.T,
	db *sql.DB,
	epoch uint64,
	totalActiveStake string,
) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO epoch_summary (
    epoch, total_active_stake, total_pool_count, total_delegators,
    boundary_slot, snapshot_ready
) VALUES (?, ?, 0, 0, 0, TRUE)`,
		epoch, totalActiveStake,
	)
	require.NoError(t, err)
}

func seedLegacyPoolStakeSnapshot(
	t *testing.T,
	db *sql.DB,
	epoch uint64,
	poolKeyHash []byte,
) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO pool_stake_snapshot (
    epoch, snapshot_type, pool_key_hash, total_stake, stake_denominator,
    delegator_count, captured_slot, calculation_version
) VALUES (?, 'mark', ?, '0', '0', 0, 0, 1)`,
		epoch, poolKeyHash,
	)
	require.NoError(t, err)
}

func seedLegacyRewardSnapshot(
	t *testing.T,
	db *sql.DB,
	epoch uint64,
	totalActiveStake string,
	authoritative bool,
) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO reward_snapshot (
    epoch, snapshot_type, total_active_stake, total_pool_count,
    total_delegators, captured_slot, boundary_slot, protocol_version,
    authoritative, calculation_version
) VALUES (?, 'mark', ?, 0, 0, 0, 0, 9, ?, 1)`,
		epoch, totalActiveStake, authoritative,
	)
	require.NoError(t, err)
}

func calculationVersion(
	t *testing.T,
	db *sql.DB,
	table string,
	epoch uint64,
) int {
	t.Helper()
	var version int
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT calculation_version FROM "+table+" WHERE epoch = ?",
		epoch,
	).Scan(&version))
	return version
}

// The restamp reads every legacy snapshot in one pass, so the four cases share
// one database and one run of the backfill: pool stake snapshots are always
// restamped, a reward snapshot only when its total agrees with the epoch
// summary, whether it is authoritative or a fallback.
func TestRewardStakeVersionRestamp(t *testing.T) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedLegacyPoolStakeSnapshot(t, db, 100, []byte("legacy-pool-key-hash-01"))
	seedEpochSummary(t, db, 200, "1000")
	seedLegacyRewardSnapshot(t, db, 200, "1000", true)
	seedEpochSummary(t, db, 300, "1000")
	seedLegacyRewardSnapshot(t, db, 300, "900", true)
	seedEpochSummary(t, db, 400, "1000")
	seedLegacyRewardSnapshot(t, db, 400, "1000", false)

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "pool_stake_snapshot", 100),
		"pool stake snapshots are always restamped")
	require.Equal(t, 2, calculationVersion(t, db, "reward_snapshot", 200),
		"an agreeing reward snapshot is restamped")
	require.Equal(t, 1, calculationVersion(t, db, "reward_snapshot", 300),
		"a disagreeing reward snapshot stays stale")
	require.Equal(t, 2, calculationVersion(t, db, "reward_snapshot", 400),
		"an agreeing fallback reward snapshot is restamped")
}
