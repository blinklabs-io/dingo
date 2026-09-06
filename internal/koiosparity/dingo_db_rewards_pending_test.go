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

	t.Run("a missing reward row before the boundary is pending", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		// A reward_pool_input row so the pool is in the map at all, but no
		// reward_pool_output row: this is the not-yet-computed case.
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`, pool, stakeEpoch, "1000", 1).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 100, 1).Error)
		// Epoch stakeEpoch+3 exists and starts well ahead of the tip.
		require.NoError(t, gdb.Exec(
			`INSERT INTO epoch (epoch_id, start_slot, length_in_slots) VALUES (?, ?, ?)`,
			stakeEpoch+3, 500_000, 86_400).Error)

		m, err := db.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		require.NoError(t, err)
		d := find(t, m)
		require.False(t, d.MemberRewardPresent, "fixture must have no output row")
		assert.True(t, d.RewardsPending,
			"a row Dingo has not computed yet is a lag, not a gap")
	})

	t.Run("a slot without a hash is not a tip", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_output
			 (pool_key_hash, epoch, member_reward_total, unspendable, boundary_slot)
			 VALUES (?, ?, ?, ?, ?)`,
			pool, stakeEpoch, "4006269", "1857", boundarySlot,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			nil, boundarySlot-1, 1,
		).Error)
		m, err := db.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		require.NoError(t, err)
		assert.False(t, find(t, m).RewardsPending,
			"incomplete tip metadata must not downgrade a real divergence")
	})
}

// TestGetPoolEpochDataMapSeparatesAbsentBoundaryFromUnreadableOne pins the
// three claims the applying-epoch lookup can make, which shared one branch.
//
// Only an absent row asserts "the node has not reached E+3". A failed read and
// an unusable start slot assert nothing, and per DingoPoolEpochData.
// RewardsPending a source that cannot establish the boundary must leave the
// comparison strict rather than downgrade a real divergence to a lag — the
// same direction the tip read takes when it cannot establish a tip.
func TestGetPoolEpochDataMapSeparatesAbsentBoundaryFromUnreadableOne(t *testing.T) {
	const (
		stakeEpoch = uint64(9)
		paramEpoch = uint64(10)
		tipSlot    = 100
	)
	pool := testPoolKeyHash(t, 0x42)

	// No reward_pool_output row, so the pool's own boundary cannot answer and
	// the epoch-level lookup is what decides.
	seed := func(t *testing.T, mutate func(gdb *testDB)) *DingoPoolEpochData {
		t.Helper()
		db, gdb := openTestDingoDB(t)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`, pool, stakeEpoch, "1000", 1).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, tipSlot, 1).Error)
		mutate(gdb)
		m, err := db.GetPoolEpochDataMap(
			context.Background(), stakeEpoch, paramEpoch,
		)
		require.NoError(t, err)
		for k, v := range m {
			if len(k) >= 2 && k[:2] == "42" {
				require.False(t, v.MemberRewardPresent,
					"fixture must have no reward_pool_output row")
				return v
			}
		}
		require.FailNow(t, "pool row missing from the map")
		return nil
	}

	t.Run("no row for the applying epoch is pending", func(t *testing.T) {
		assert.True(t, seed(t, func(*testDB) {}).RewardsPending,
			"the node has plainly not reached an epoch it has no row for")
	})

	t.Run("an unreadable epoch table compares strictly", func(t *testing.T) {
		assert.False(t, seed(t, func(gdb *testDB) {
			require.NoError(t, gdb.Exec(`DROP TABLE epoch`).Error)
		}).RewardsPending,
			"a failed read establishes no boundary, so it must not downgrade")
	})

	t.Run("a NULL start slot compares strictly", func(t *testing.T) {
		assert.False(t, seed(t, func(gdb *testDB) {
			require.NoError(t, gdb.Exec(
				`INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
				 VALUES (?, NULL, ?)`, stakeEpoch+3, 86_400).Error)
		}).RewardsPending,
			"a row with no start slot establishes no boundary")
	})

	t.Run("a negative start slot compares strictly", func(t *testing.T) {
		assert.False(t, seed(t, func(gdb *testDB) {
			require.NoError(t, gdb.Exec(
				`INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
				 VALUES (?, ?, ?)`, stakeEpoch+3, -1, 86_400).Error)
		}).RewardsPending,
			"a slot that is not representable establishes no boundary")
	})
}
