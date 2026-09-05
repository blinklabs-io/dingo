package koiosparity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComparePoolEpochMemberRewardsBeforeApplication is the dingo #3852
// regression, built from the divergence a Preview replay reported at epoch 96.
// A difference in Dingo's provisional spendable sum before the applying
// boundary is reference lag; the same difference after that boundary is real.
func TestComparePoolEpochMemberRewardsBeforeApplication(t *testing.T) {
	const (
		koiosPaid     = "4004412"
		dingoComputed = "4006269"
	)
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1l5u4zh84na80xr56d342d32rsdw62qycwaw97hy9wwsc6axdwla",
		MemberRewards: koiosPaid,
	}
	base := func() *DingoPoolEpochData {
		return &DingoPoolEpochData{
			MemberRewardPresent:          true,
			MemberRewardTotal:            dingoComputed,
			SpendableMemberRewardPresent: true,
			SpendableMemberRewardTotal:   dingoComputed,
			PoolUnspendable:              1857,
		}
	}
	now := time.Now()

	find := func(t *testing.T, ms []CheckMismatch) CheckMismatch {
		t.Helper()
		for _, m := range ms {
			if m.Field == "member_rewards" {
				return m
			}
		}
		require.FailNow(t, "no member_rewards mismatch produced")
		return CheckMismatch{}
	}

	t.Run("before application it is a timing statement", func(t *testing.T) {
		d := base()
		d.RewardsPending = true
		m := find(t, ComparePoolEpoch(
			"preview", 96, koios, d, now, 0, time.Time{}, false,
		))
		assert.Equal(t, CategoryReferenceLag, m.Category,
			"a pending forfeiture must not be reported as a divergence")
	})

	t.Run("after application it is a real divergence", func(t *testing.T) {
		d := base()
		d.RewardsPending = false
		m := find(t, ComparePoolEpoch(
			"preview", 96, koios, d, now, 0, time.Time{}, false,
		))
		assert.Equal(t, CategoryValueMismatch, m.Category,
			"once applied, a difference is a genuine mismatch")
	})

	t.Run("agreement after application reports nothing", func(t *testing.T) {
		d := base()
		d.RewardsPending = false
		d.SpendableMemberRewardTotal = koiosPaid
		for _, m := range ComparePoolEpoch(
			"preview", 96, koios, d, now, 0, time.Time{}, false,
		) {
			assert.NotEqual(t, "member_rewards", m.Field,
				"the spendable sum equals Koios, so nothing to report")
		}
	})
}
