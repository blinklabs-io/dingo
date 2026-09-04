package koiosparity

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDatabaseSourceMissingMemberRewardsUsesChainPosition covers the
// in-process source half of dingo #3857. The missing applying epoch is a
// meaningful replay state; malformed boundary or tip metadata is not evidence
// of pending work and must keep the comparison strict.
func TestDatabaseSourceMissingMemberRewardsUsesChainPosition(t *testing.T) {
	const (
		stakeEpoch = uint64(9)
		paramEpoch = uint64(10)
		applyStart = int64(500_000)
	)
	poolHash := bytes.Repeat([]byte{0x42}, 28)

	seed := func(
		t *testing.T,
		tipSlot *int64,
		applyStart *int64,
		withTipHash bool,
	) *DingoPoolEpochData {
		t.Helper()
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO reward_pool_input
			 (pool_key_hash, epoch, pledge, delegated_stake, owner_stake, cost,
			  delegator_count, captured_slot, boundary_slot)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			poolHash, stakeEpoch, "0", "1000", "0", "0", 1, 0, 0,
		).Error)
		if tipSlot != nil {
			var hash []byte
			if withTipHash {
				hash = []byte{0x01}
			}
			require.NoError(t, sqlDB.Exec(
				`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
				hash, *tipSlot, 1,
			).Error)
		}
		if applyStart != nil {
			require.NoError(t, sqlDB.Exec(
				`INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
				 VALUES (?, ?, ?)`,
				stakeEpoch+3, *applyStart, 86_400,
			).Error)
		}

		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		require.NoError(t, err)
		data, ok := m[hex.EncodeToString(poolHash)]
		require.True(t, ok, "pool missing from the map")
		require.False(t, data.MemberRewardPresent,
			"fixture must have no reward_pool_output row")
		return data
	}

	t.Run("before the applying boundary is pending", func(t *testing.T) {
		tip := applyStart - 1
		boundary := applyStart
		assert.True(t, seed(t, &tip, &boundary, true).RewardsPending)
	})

	t.Run("at the applying boundary is a real gap", func(t *testing.T) {
		tip := applyStart
		boundary := applyStart
		assert.False(t, seed(t, &tip, &boundary, true).RewardsPending)
	})

	t.Run("an applying epoch not reached is pending", func(t *testing.T) {
		tip := applyStart
		assert.True(t, seed(t, &tip, nil, true).RewardsPending)
	})

	t.Run("a missing tip is strict", func(t *testing.T) {
		boundary := applyStart
		assert.False(t, seed(t, nil, &boundary, true).RewardsPending)
	})

	t.Run("an incomplete tip is strict", func(t *testing.T) {
		tip := applyStart - 1
		boundary := applyStart
		assert.False(t, seed(t, &tip, &boundary, false).RewardsPending)
	})

	t.Run("an unreadable boundary is strict", func(t *testing.T) {
		tip := applyStart - 1
		badBoundary := int64(-1)
		assert.False(t, seed(t, &tip, &badBoundary, true).RewardsPending)
	})
}

// TestDingoDBMissingMemberRewardsUsesChainPosition covers the standalone
// source with the same boundary cases as the in-process source. In particular,
// a SQL error while reading boundary metadata must not become a grace signal.
func TestDingoDBMissingMemberRewardsUsesChainPosition(t *testing.T) {
	const (
		stakeEpoch = uint64(9)
		paramEpoch = uint64(10)
		applyStart = int64(500_000)
	)
	poolHash := bytes.Repeat([]byte{0x42}, 28)

	seed := func(
		t *testing.T,
		tipSlot *int64,
		applyStart *int64,
		withTipHash bool,
		dropEpoch bool,
	) *DingoPoolEpochData {
		t.Helper()
		db, sqlDB := openTestDingoDB(t)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO reward_pool_input
			 (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`,
			poolHash, stakeEpoch, "1000", 1,
		).Error)
		if tipSlot != nil {
			var hash []byte
			if withTipHash {
				hash = []byte{0x01}
			}
			require.NoError(t, sqlDB.Exec(
				`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
				hash, *tipSlot, 1,
			).Error)
		}
		if applyStart != nil {
			require.NoError(t, sqlDB.Exec(
				`INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
				 VALUES (?, ?, ?)`,
				stakeEpoch+3, *applyStart, 86_400,
			).Error)
		}
		if dropEpoch {
			require.NoError(t, sqlDB.Exec(`DROP TABLE epoch`).Error)
		}

		m, err := db.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		require.NoError(t, err)
		data, ok := m[hex.EncodeToString(poolHash)]
		require.True(t, ok, "pool missing from the map")
		require.False(t, data.MemberRewardPresent,
			"fixture must have no reward_pool_output row")
		return data
	}

	t.Run("before the applying boundary is pending", func(t *testing.T) {
		tip := applyStart - 1
		boundary := applyStart
		assert.True(t, seed(t, &tip, &boundary, true, false).RewardsPending)
	})

	t.Run("at the applying boundary is a real gap", func(t *testing.T) {
		tip := applyStart
		boundary := applyStart
		assert.False(t, seed(t, &tip, &boundary, true, false).RewardsPending)
	})

	t.Run("an applying epoch not reached is pending", func(t *testing.T) {
		tip := applyStart
		assert.True(t, seed(t, &tip, nil, true, false).RewardsPending)
	})

	t.Run("a missing tip is strict", func(t *testing.T) {
		boundary := applyStart
		assert.False(t, seed(t, nil, &boundary, true, false).RewardsPending)
	})

	t.Run("an incomplete tip is strict", func(t *testing.T) {
		tip := applyStart - 1
		boundary := applyStart
		assert.False(t, seed(t, &tip, &boundary, false, false).RewardsPending)
	})

	t.Run("an unreadable boundary is strict", func(t *testing.T) {
		tip := applyStart - 1
		boundary := applyStart
		assert.False(t, seed(t, &tip, &boundary, true, true).RewardsPending)
	})
}
