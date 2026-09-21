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
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 19)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry[:13],
		Locker: migrations.NewFileLocker(
			databasePath + ".migrate.lock",
		),
	}
	require.NoError(t, runner.Run(ctx))
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

// A stale pool_stake_snapshot row is always safe to re-stamp: its stored
// totals never depended on calculation version (dingo #4026).
func TestRewardStakeVersionRestampAlwaysFixesPoolStakeSnapshot(t *testing.T) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedLegacyPoolStakeSnapshot(t, db, 100, []byte("legacy-pool-key-hash-01"))

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "pool_stake_snapshot", 100))
}

// A stale Mark reward_snapshot row whose total_active_stake already agrees
// with the epoch's epoch_summary is re-stamped: that agreement is exactly
// what identifies an epoch the version bump did not actually change (dingo
// #4026 finding 1).
func TestRewardStakeVersionRestampFixesAgreeingRewardSnapshot(t *testing.T) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedEpochSummary(t, db, 200, "1000")
	seedLegacyRewardSnapshot(t, db, 200, "1000", true)

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "reward_snapshot", 200))
}

// A stale Mark reward_snapshot row whose total_active_stake disagrees with
// epoch_summary names an epoch the version bump actually changed. The
// backfill must not paper over that with a re-stamp; it is left for
// StaleConsensusStakeSnapshotsExist to keep failing closed on.
func TestRewardStakeVersionRestampLeavesDisagreeingRewardSnapshotStale(
	t *testing.T,
) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedEpochSummary(t, db, 300, "1000")
	seedLegacyRewardSnapshot(t, db, 300, "900", true)

	runRestamp()

	require.Equal(t, 1, calculationVersion(t, db, "reward_snapshot", 300))
}

// The non-authoritative fallback row is restamped by the same rule as an
// authoritative one: agreement with epoch_summary, not the authoritative
// flag, is what the backfill keys on.
func TestRewardStakeVersionRestampFixesAgreeingFallbackRewardSnapshot(
	t *testing.T,
) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedEpochSummary(t, db, 400, "1000")
	seedLegacyRewardSnapshot(t, db, 400, "1000", false)

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "reward_snapshot", 400))
}
