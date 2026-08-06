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

package koiosparity

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// newTestDatabaseSourceDB creates a real, in-process *database.Database
// (Badger blob + SQLite metadata, matching ledger/snapshot's own test
// pattern) for exercising DatabaseSource against the same storage stack a
// live node uses, rather than a bare raw connection.
func newTestDatabaseSourceDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return db
}

// sourceGormDB reaches into the metadata store's underlying SQLite file so
// the test can seed rows directly via raw SQL, mirroring
// ledger/snapshot/calculator_test.go's snapshotSQLDB helper. Post-#3054 (the
// GORM metadata store rewrite to sqlc-generated sqlstore), the metadata
// store no longer exposes a *gorm.DB, so this reaches through
// dbtest.RawSQLiteMetadata instead and wraps it in the same testDB seeding
// helper dingo_db_test.go uses. The name is kept (rather than renamed to
// sourceSQLDB) to minimize churn in call sites that still read naturally as
// "the test's handle for seeding the source's DB".
func sourceGormDB(t *testing.T, db *database.Database) *testDB {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return &testDB{db: raw}
}

func TestNewDatabaseSourceRejectsNilDatabase(t *testing.T) {
	_, err := NewDatabaseSource(nil)
	require.Error(t, err)
}

// TestDatabaseSourceGetEpochData exercises commit visibility end to end: it
// commits epoch_summary/reward_ada_pots through the same *database.Database
// a live node would write through, then reads them back via DatabaseSource
// (a separate read-only transaction against the same live database, not a
// second connection) and confirms every field lands exactly as committed.
func TestDatabaseSourceGetEpochData(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)

	require.NoError(t, gormDB.Create(&models.EpochSummary{
		Epoch:            5,
		TotalActiveStake: types.Uint64(123_456_789),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gormDB.Create(&models.RewardAdaPots{
		Epoch:    5,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(3_000),
		Rewards:  types.Uint64(4_000),
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	data, err := source.GetEpochData(context.Background(), 5)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, "123456789", data.TotalActiveStake)
	require.True(t, data.RewardAdaPotsPresent)
	require.Equal(t, "1000", data.Treasury)
	require.Equal(t, "2000", data.Reserves)
	require.Equal(t, "3000", data.Fees)
	require.Equal(t, "4000", data.TotalRewards)
}

// TestDatabaseSourceGetEpochDataMissingOrNotReady covers both "no row at
// all" and "row exists but SnapshotReady is still false" (an in-progress
// write Dingo will repair later) -- both must read back as (nil, nil), never
// an error and never a spurious zero-value comparison.
func TestDatabaseSourceGetEpochDataMissingOrNotReady(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	data, err := source.GetEpochData(context.Background(), 9)
	require.NoError(t, err)
	require.Nil(t, data)

	require.NoError(t, gormDB.Create(&models.EpochSummary{
		Epoch:         9,
		SnapshotReady: false,
	}).Error)
	data, err = source.GetEpochData(context.Background(), 9)
	require.NoError(t, err)
	require.Nil(t, data)
}

// TestDatabaseSourceGetEpochDataRewardAdaPotsAbsent guards the
// RewardAdaPotsPresent distinction documented on DingoEpochData: a
// bootstrap-imported epoch can have epoch_summary.SnapshotReady=true with no
// reward_ada_pots row at all, which must surface as RewardAdaPotsPresent ==
// false (a real dingo_db_missing mismatch upstream), not as legitimately
// empty/zero pots.
func TestDatabaseSourceGetEpochDataRewardAdaPotsAbsent(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)
	require.NoError(t, gormDB.Create(&models.EpochSummary{
		Epoch:         3,
		SnapshotReady: true,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	data, err := source.GetEpochData(context.Background(), 3)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.False(t, data.RewardAdaPotsPresent)
	require.Empty(t, data.Fees)
	require.Empty(t, data.Treasury)
}

// TestDatabaseSourceGetPoolEpochDataMap mirrors DingoDB's own
// GetPoolEpochDataMap semantics: DelegatedStake/DelegatorCount come from
// stakeEpoch, BlocksProduced/FixedCost/Margin from paramEpoch, and
// MemberRewardTotal from stakeEpoch's reward_pool_output -- each field
// group's *Present flag reflects only whether its own row existed.
func TestDatabaseSourceGetPoolEpochDataMap(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	require.Len(t, poolKeyHash, 28)

	require.NoError(t, gormDB.Create(&models.RewardPoolInput{
		Epoch:          10,
		PoolKeyHash:    poolKeyHash,
		DelegatedStake: types.Uint64(500_000),
		DelegatorCount: 3,
	}).Error)
	blocksProduced := uint64(7)
	require.NoError(t, gormDB.Create(&models.RewardPoolInput{
		Epoch:          12,
		PoolKeyHash:    poolKeyHash,
		BlocksProduced: &blocksProduced,
		Cost:           types.Uint64(340_000_000),
	}).Error)
	require.NoError(t, gormDB.Create(&models.RewardPoolOutput{
		Epoch:             10,
		PoolKeyHash:       poolKeyHash,
		MemberRewardTotal: types.Uint64(999_999),
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	m, err := source.GetPoolEpochDataMap(context.Background(), 10, 12)
	require.NoError(t, err)
	require.Len(t, m, 1)

	var key string
	for k := range m {
		key = k
	}
	entry := m[key]
	require.NotNil(t, entry)
	require.True(t, entry.StakePresent)
	require.Equal(t, "500000", entry.DelegatedStake)
	require.Equal(t, uint64(3), entry.DelegatorCount)
	require.True(t, entry.ParamsPresent)
	require.Equal(t, uint64(7), entry.BlocksProduced)
	require.Equal(t, "340000000", entry.FixedCost)
	require.True(t, entry.MemberRewardPresent)
	require.Equal(t, "999999", entry.MemberRewardTotal)
}

// TestDatabaseSourceGetPoolEpochDataMapPartialPresence covers the case where
// only stakeEpoch's RewardPoolInput row exists for a pool (no paramEpoch row,
// no RewardPoolOutput row yet) -- e.g. a pool captured at the stake snapshot
// whose reward_pool_output hasn't been computed and written yet. Per
// dingo_db.go's doc comment (~lines 89-134) and the fallback stub-
// construction logic in source.go (~lines 211-241), each field group's
// *Present flag must reflect only whether its own row actually exists, not
// whether any row exists for the pool at all.
func TestDatabaseSourceGetPoolEpochDataMapPartialPresence(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	require.Len(t, poolKeyHash, 28)

	require.NoError(t, gormDB.Create(&models.RewardPoolInput{
		Epoch:          10,
		PoolKeyHash:    poolKeyHash,
		DelegatedStake: types.Uint64(500_000),
		DelegatorCount: 3,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	m, err := source.GetPoolEpochDataMap(context.Background(), 10, 12)
	require.NoError(t, err)
	require.Len(t, m, 1)

	var key string
	for k := range m {
		key = k
	}
	entry := m[key]
	require.NotNil(t, entry)
	require.True(t, entry.StakePresent)
	require.Equal(t, "500000", entry.DelegatedStake)
	require.Equal(t, uint64(3), entry.DelegatorCount)
	require.False(t, entry.ParamsPresent)
	require.False(t, entry.MemberRewardPresent)
}

func TestDatabaseSourceGetLatestEpoch(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	_, err = source.GetLatestEpoch(context.Background())
	require.Error(t, err, "no epoch_summary rows yet")

	gormDB := sourceGormDB(t, db)
	require.NoError(t, gormDB.Create(&models.EpochSummary{Epoch: 2}).Error)
	require.NoError(t, gormDB.Create(&models.EpochSummary{Epoch: 7}).Error)
	require.NoError(t, gormDB.Create(&models.EpochSummary{Epoch: 4}).Error)

	latest, err := source.GetLatestEpoch(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(7), latest)
}

func TestDatabaseSourceGetRewardAccountOutputs(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)
	stakingKey := []byte("STAKING-KEY-28-BYTES-LONG!!!")
	require.Len(t, stakingKey, 28)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	require.NoError(t, gormDB.Create(&models.RewardAccountOutput{
		Epoch:       6,
		StakingKey:  stakingKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(42),
		Spendable:   true,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	rows, err := source.GetRewardAccountOutputs(context.Background(), 6)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, types.Uint64(42), rows[0].Amount)
	require.True(t, rows[0].Spendable)
}

// TestDatabaseSourceCoreModePruningTiming demonstrates the retention-window
// property DatabaseSource's doc comment relies on for reward_account_output
// (the table #3097's per-account parity check will read): it remains fully
// readable through DatabaseSource until something actually deletes it
// (ledger/snapshot/rotation.go's cleanupOldSnapshots calls
// MetadataStore.DeleteRewardStateBeforeEpoch on a rolling window in core
// storage mode; API mode retains it without bound instead — see
// rotation.go's doc comment). Before that deletion, the data is present;
// simulating exactly the deletion cleanupOldSnapshots performs makes the
// epoch read back as "not present" afterward — the same signal
// GetRewardAccountOutputs already uses for "not yet computed", not an
// error. This is why the in-process observer must process a newly closed
// epoch promptly (within that multi-epoch window) rather than relying on
// some same-transaction race with the write that produced the data: reading
// late is indistinguishable from reading an epoch that was simply never
// computed.
func TestDatabaseSourceCoreModePruningTiming(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	gormDB := sourceGormDB(t, db)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	stakingKey := []byte("STAKING-KEY-28-BYTES-LONG!!!")

	const epoch = 20
	require.NoError(t, gormDB.Create(&models.RewardAccountOutput{
		Epoch:       epoch,
		StakingKey:  stakingKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(1),
		Spendable:   true,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	// Before pruning: fully readable.
	accounts, err := source.GetRewardAccountOutputs(context.Background(), epoch)
	require.NoError(t, err)
	require.Len(t, accounts, 1)

	// Simulate the same core-mode retention cleanup
	// ledger/snapshot/rotation.go's cleanupOldSnapshots performs once epoch
	// falls out of the rolling window (deleteBeforeEpoch = currentEpoch-3).
	txn := db.Transaction(true)
	require.NoError(
		t,
		db.Metadata().DeleteRewardStateBeforeEpoch(epoch+1, txn.Metadata()),
	)
	require.NoError(t, txn.Commit())

	// After pruning: reads back as "not present", not an error -- the same
	// signal as an epoch whose reward calculation never ran.
	accounts, err = source.GetRewardAccountOutputs(context.Background(), epoch)
	require.NoError(t, err)
	require.Empty(t, accounts)
}
