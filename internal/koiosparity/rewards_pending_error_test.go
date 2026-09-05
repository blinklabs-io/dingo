package koiosparity

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestDingoDBPropagatesApplyingEpochLookupError prevents a failed E+3 lookup
// from being treated as evidence that rewards are pending. The pending flag is
// only valid for a missing epoch row; a database failure must reach the caller.
func TestDingoDBPropagatesApplyingEpochLookupError(t *testing.T) {
	db, gdb := openTestDingoDB(t)
	pool := testPoolKeyHash(t, 0x41)
	require.NoError(t, gdb.Exec(
		`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
		 VALUES (?, ?, ?, ?)`, pool, 9, "1000", 1,
	).Error)
	require.NoError(t, gdb.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		[]byte{0x01}, 100, 1,
	).Error)
	require.NoError(t, gdb.Exec(`DROP TABLE epoch`).Error)

	_, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
	require.ErrorContains(t, err, "epoch lookup")
}

// TestDatabaseSourcePropagatesApplyingEpochLookupError covers the same
// contract through the in-process source. Both RewardParitySource
// implementations must fail closed when their E+3 lookup cannot run.
func TestDatabaseSourcePropagatesApplyingEpochLookupError(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	pool := testPoolKeyHash(t, 0x42)
	require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
		PoolKeyHash:    pool,
		Epoch:          9,
		DelegatedStake: types.Uint64(1000),
		DelegatorCount: 1,
	}).Error)
	require.NoError(t, sqlDB.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		[]byte{0x01}, 100, 1,
	).Error)
	require.NoError(t, sqlDB.Exec(`DROP TABLE epoch`).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	_, err = source.GetPoolEpochDataMap(context.Background(), 9, 10)
	require.ErrorContains(t, err, "epoch lookup 12")
}

func TestMissingApplyingEpochIsThePendingCase(t *testing.T) {
	t.Run("standalone source", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		pool := testPoolKeyHash(t, 0x43)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`, pool, 9, "1000", 1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 100, 1,
		).Error)

		m, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		data, ok := m[hex.EncodeToString(pool)]
		require.True(t, ok)
		require.True(t, data.RewardsPending)
	})

	t.Run("in-process source", func(t *testing.T) {
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		pool := testPoolKeyHash(t, 0x44)
		require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
			PoolKeyHash:    pool,
			Epoch:          9,
			DelegatedStake: types.Uint64(1000),
			DelegatorCount: 1,
		}).Error)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 100, 1,
		).Error)

		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		data, ok := m[hex.EncodeToString(pool)]
		require.True(t, ok)
		require.True(t, data.RewardsPending)
	})
}

func TestPositiveSlotWithoutTipHashIsNotPending(t *testing.T) {
	t.Run("standalone source", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		pool := testPoolKeyHash(t, 0x45)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`, pool, 9, "1000", 1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{}, 100, 1,
		).Error)

		m, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})

	t.Run("in-process source", func(t *testing.T) {
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		pool := testPoolKeyHash(t, 0x46)
		require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
			PoolKeyHash: pool, Epoch: 9, DelegatedStake: types.Uint64(1000), DelegatorCount: 1,
		}).Error)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{}, 100, 1,
		).Error)

		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})
}

func TestZeroSlotWithTipHashIsNotPending(t *testing.T) {
	t.Run("standalone source", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		pool := testPoolKeyHash(t, 0x47)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`, pool, 9, "1000", 1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 0, 1,
		).Error)
		m, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})

	t.Run("in-process source", func(t *testing.T) {
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		pool := testPoolKeyHash(t, 0x48)
		require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
			PoolKeyHash: pool, Epoch: 9, DelegatedStake: types.Uint64(1000), DelegatorCount: 1,
		}).Error)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 0, 1,
		).Error)
		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})
}

func TestComparePoolEpochUsesRewardsPending(t *testing.T) {
	memberRewardMismatch := func(mismatches []CheckMismatch) CheckMismatch {
		for _, mismatch := range mismatches {
			if mismatch.Field == "member_rewards" {
				return mismatch
			}
		}
		t.Fatal("member_rewards mismatch not found")
		return CheckMismatch{}
	}

	koios := &KoiosPoolEpoch{MemberRewards: "1"}
	dingo := &DingoPoolEpochData{
		MemberRewardPresent:          true,
		SpendableMemberRewardPresent: true,
		SpendableMemberRewardTotal:   "2",
	}

	dingo.RewardsPending = true
	mismatches := ComparePoolEpoch(
		"preview", 96, koios, dingo, time.Now(), 0, time.Time{}, false,
	)
	require.Equal(t, CategoryReferenceLag, memberRewardMismatch(mismatches).Category)

	dingo.RewardsPending = false
	mismatches = ComparePoolEpoch(
		"preview", 96, koios, dingo, time.Now(), 0, time.Time{}, false,
	)
	require.Equal(t, CategoryValueMismatch, memberRewardMismatch(mismatches).Category)
}
