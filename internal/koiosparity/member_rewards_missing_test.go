package koiosparity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComparePoolEpochMissingMemberRewardsDuringReplay is the dingo #3857
// regression. A replay's epoch closed long ago, but Dingo may not have
// reached the boundary at which it writes reward_pool_output yet. The missing
// row must be a reference lag in that case, not a dingo_db_missing failure.
func TestComparePoolEpochMissingMemberRewardsDuringReplay(t *testing.T) {
	const pool = "pool1l5u4zh84na80xr56d342d32rsdw62qycwaw97hy9wwsc6axdwla"
	koios := &KoiosPoolEpoch{PoolBech32: pool, MemberRewards: "4004412"}
	now := time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)
	longClosed := now.Add(-1388 * 24 * time.Hour)

	find := func(t *testing.T, mismatches []CheckMismatch) CheckMismatch {
		t.Helper()
		for _, mismatch := range mismatches {
			if mismatch.Field == "member_rewards" {
				return mismatch
			}
		}
		require.FailNow(t, "no member_rewards mismatch produced")
		return CheckMismatch{}
	}

	t.Run("chain position keeps a replay row in the grace state", func(t *testing.T) {
		mismatch := find(t, ComparePoolEpoch(
			"preview", 44, koios,
			&DingoPoolEpochData{RewardsPending: true},
			now, 24, longClosed, false,
		))
		assert.Equal(t, CategoryReferenceLag, mismatch.Category,
			"a missing row before its applying boundary is not a database gap")
	})

	t.Run("past the applying boundary remains fail closed", func(t *testing.T) {
		mismatch := find(t, ComparePoolEpoch(
			"preview", 44, koios,
			&DingoPoolEpochData{},
			now, 24, longClosed, false,
		))
		assert.Equal(t, CategoryDBMissing, mismatch.Category,
			"without chain-position evidence a missing row stays a failure")
	})
}
