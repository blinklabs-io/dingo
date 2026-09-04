package koiosparity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPoolEpochDataMapReportsRewardsPending covers the standalone source's
// half of dingo #3852. DingoDB reads the applying boundary and tip from SQL;
// the comparison can only classify a pending forfeiture correctly if this
// source carries that fact forward.
func TestGetPoolEpochDataMapReportsRewardsPending(t *testing.T) {
	const (
		stakeEpoch   = uint64(9)
		paramEpoch   = uint64(10)
		boundarySlot = 1_000_000
	)
	pool := testPoolKeyHash(t, 0x42)

	seed := func(t *testing.T, tipSlot int64, tipHash []byte) map[string]*DingoPoolEpochData {
		t.Helper()
		db, gdb := openTestDingoDB(t)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_output
			 (pool_key_hash, epoch, member_reward_total, unspendable, boundary_slot)
			 VALUES (?, ?, ?, ?, ?)`,
			pool, stakeEpoch, "4006269", "1857", boundarySlot,
		).Error)
		if tipHash != nil {
			require.NoError(t, gdb.Exec(
				`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
				tipHash, tipSlot, 1,
			).Error)
		}
		m, err := db.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		require.NoError(t, err)
		return m
	}

	find := func(t *testing.T, m map[string]*DingoPoolEpochData) *DingoPoolEpochData {
		t.Helper()
		for k, v := range m {
			if len(k) >= 2 && k[:2] == "42" {
				return v
			}
		}
		require.FailNow(t, "pool row missing from the map")
		return nil
	}

	t.Run("tip before the boundary is pending", func(t *testing.T) {
		d := find(t, seed(t, boundarySlot-1, []byte{0x01}))
		assert.True(t, d.RewardsPending,
			"rewards are not applied yet, so a difference is a lag")
	})

	t.Run("tip at the boundary is applied", func(t *testing.T) {
		d := find(t, seed(t, boundarySlot, []byte{0x01}))
		assert.False(t, d.RewardsPending,
			"at the boundary the spendable flags are final")
	})

	t.Run("no tip row compares strictly", func(t *testing.T) {
		d := find(t, seed(t, 0, nil))
		assert.False(t, d.RewardsPending,
			"an unreadable tip must not downgrade a real divergence")
	})

	t.Run("positive slot without hash compares strictly", func(t *testing.T) {
		d := find(t, seed(t, boundarySlot-1, []byte{}))
		assert.False(t, d.RewardsPending,
			"an incomplete tip must not downgrade a real divergence")
	})
}
