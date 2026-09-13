package koiosparity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPoolEpochDataMapReportsRewardsPending covers the standalone source's
// half of dingo #3852. The in-process source resolves the applying boundary
// from the ledger tip; DingoDB reads the same two values out of SQL, and if it
// does not, every pre-boundary forfeiture is still reported as a value_mismatch
// no matter what the comparison does with the flag.
func TestGetPoolEpochDataMapReportsRewardsPending(t *testing.T) {
	const (
		stakeEpoch   = uint64(9)
		paramEpoch   = uint64(10)
		boundarySlot = 1_000_000
	)
	pool := testPoolKeyHash(t, 0x42)

	seed := func(t *testing.T, tipSlot int64, withTipRow bool) map[string]*DingoPoolEpochData {
		t.Helper()
		db, gdb := openTestDingoDB(t)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_output
			 (pool_key_hash, epoch, member_reward_total, unspendable, boundary_slot)
			 VALUES (?, ?, ?, ?, ?)`,
			pool, stakeEpoch, "4006269", "1857", boundarySlot,
		).Error)
		if withTipRow {
			require.NoError(t, gdb.Exec(
				`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
				[]byte{0x01}, tipSlot, 1,
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
		d := find(t, seed(t, boundarySlot-1, true))
		assert.True(t, d.RewardsPending,
			"rewards are not applied yet, so a difference is a lag")
	})

	t.Run("tip at the boundary is applied", func(t *testing.T) {
		d := find(t, seed(t, boundarySlot, true))
		assert.False(t, d.RewardsPending,
			"at the boundary the spendable flags are final")
	})

	t.Run("no tip row compares strictly", func(t *testing.T) {
		d := find(t, seed(t, 0, false))
		assert.False(t, d.RewardsPending,
			"an unreadable tip must not downgrade a real divergence")
	})

	t.Run(
		"a missing reward row before the boundary is pending",
		func(t *testing.T) {
			db, gdb := openTestDingoDB(t)
			// A reward_pool_input row so the pool is in the map at all, but no
			// reward_pool_output row: this is the not-yet-computed case.
			require.NoError(t, gdb.Exec(
				`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`,
				pool,
				stakeEpoch,
				"1000",
				1,
			).Error)
			require.NoError(t, gdb.Exec(
				`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
				[]byte{0x01}, 100, 1).Error)
			// Epoch stakeEpoch+3 exists and starts well ahead of the tip.
			require.NoError(t, gdb.Exec(
				`INSERT INTO epoch (epoch_id, start_slot, length_in_slots) VALUES (?, ?, ?)`,
				stakeEpoch+3,
				500_000,
				86_400,
			).Error)

			m, err := db.GetPoolEpochDataMap(
				context.Background(), stakeEpoch, paramEpoch,
			)
			require.NoError(t, err)
			d := find(t, m)
			require.False(
				t,
				d.MemberRewardPresent,
				"fixture must have no output row",
			)
			assert.True(t, d.RewardsPending,
				"a row Dingo has not computed yet is a lag, not a gap")
		},
	)
}

// TestGetPoolEpochDataMapRejectsAnUnusableStartSlot covers the applying-epoch
// rows the standalone source cannot derive a boundary from. The row exists, so
// this is not the pending case, but a NULL or negative start slot is not a
// representable boundary and the lookup fails closed rather than guessing.
//
// rewards_pending_error_test.go covers the two cases either side of this one:
// an absent row is the pending case, and a failed read is an error.
func TestGetPoolEpochDataMapRejectsAnUnusableStartSlot(t *testing.T) {
	const (
		stakeEpoch = uint64(9)
		paramEpoch = uint64(10)
		tipSlot    = 100
	)
	pool := testPoolKeyHash(t, 0x42)

	// No reward_pool_output row, so the pool's own boundary cannot answer and
	// the epoch-level lookup is what decides.
	run := func(t *testing.T, startSlot any) error {
		t.Helper()
		db, gdb := openTestDingoDB(t)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`,
			pool,
			stakeEpoch,
			"1000",
			1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, tipSlot, 1).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
			 VALUES (?, ?, ?)`, stakeEpoch+3, startSlot, 86_400).Error)
		_, err := db.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		return err
	}

	t.Run("a NULL start slot", func(t *testing.T) {
		require.ErrorContains(t, run(t, nil), "invalid start slot")
	})

	t.Run("a negative start slot", func(t *testing.T) {
		require.ErrorContains(t, run(t, -1), "invalid start slot")
	})
}
