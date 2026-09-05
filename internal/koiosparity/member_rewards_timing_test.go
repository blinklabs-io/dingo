package koiosparity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComparePoolEpochMemberRewardsBeforeApplication is the dingo #3852
// regression, built from the divergence a Preview replay reported at epoch 96.
//
// Dingo computed 4006269 in member rewards for the pool and Koios reported
// 4004412. The 1857 difference was a reward computed for a stake credential
// that deregistered before the boundary which applies it, so the ledger never
// credited it. Dingo was right; the comparison simply ran 72 seconds before
// that boundary, while the per-account spendable flags were still provisional.
//
// Recomputed after the boundary the two agree exactly, so the difference has to
// be classified by whether the rewards have been applied, not by its size.
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

// TestComparePoolEpochMissingRewardsBeforeApplication is the dingo #3857
// regression.
//
// A reward_pool_output row for a stake epoch is not written until well after
// that epoch closes, so an observer running near the tip asks about epochs
// Dingo has not computed yet. The grace window that exists for exactly this is
// measured in wall-clock time against the epoch's real close time, so during a
// from-genesis replay -- where every epoch closed years ago -- it can never
// fire, and the absence was reported as dingo_db_missing against a node that
// was simply not there yet.
func TestComparePoolEpochMissingRewardsBeforeApplication(t *testing.T) {
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1l5u4zh84na80xr56d342d32rsdw62qycwaw97hy9wwsc6axdwla",
		MemberRewards: "4004412",
	}
	// No reward_pool_output row: MemberRewardPresent is false.
	missing := func(pending bool) *DingoPoolEpochData {
		return &DingoPoolEpochData{RewardsPending: pending}
	}
	now := time.Now()
	// An epoch that closed long ago, as every epoch of a replay has.
	longClosed := now.Add(-1388 * 24 * time.Hour)

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

	t.Run("not computed yet is a lag even long after the epoch closed", func(t *testing.T) {
		m := find(t, ComparePoolEpoch(
			"preview", 44, koios, missing(true), now, 24, longClosed, false,
		))
		assert.Equal(t, CategoryReferenceLag, m.Category,
			"the wall-clock window cannot fire in a replay; chain position must")
	})

	t.Run("past the boundary a missing row is a real gap", func(t *testing.T) {
		m := find(t, ComparePoolEpoch(
			"preview", 44, koios, missing(false), now, 24, longClosed, false,
		))
		assert.Equal(t, CategoryDBMissing, m.Category,
			"once Dingo has had its chance, absence is a genuine gap")
	})

	t.Run("the wall-clock window still works at the tip", func(t *testing.T) {
		m := find(t, ComparePoolEpoch(
			"preview", 44, koios, missing(false), now, 24,
			now.Add(-1*time.Hour), false,
		))
		assert.Equal(t, CategoryReferenceLag, m.Category,
			"a recently closed epoch keeps the existing grace behaviour")
	})
}

// TestCompareAccountEpochPendingRewardsAreALag is the account-granularity half
// of dingo #3857.
//
// When Dingo has not computed an epoch's rewards yet, every account Koios
// reports a reward for is absent on the Dingo side. That is timing, not
// divergence, and at replay speed it dominates everything else: a Preview run
// produced 15879 acct_only_koios entries, and the epochs carrying them were
// exactly the epochs with no reward row.
func TestCompareAccountEpochPendingRewardsAreALag(t *testing.T) {
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake_test1a", RewardType: "member", Earned: "1000000"},
		{StakeAddress: "stake_test1b", RewardType: "member", Earned: "2000000"},
	}
	now := time.Now()
	// An epoch that closed long ago, as every epoch of a replay has, so the
	// wall-clock grace window cannot fire.
	longClosed := now.Add(-1388 * 24 * time.Hour)

	categories := func(ms []CheckMismatch) []string {
		var out []string
		for _, m := range ms {
			if m.Field == "account_reward_presence" {
				out = append(out, m.Category)
			}
		}
		return out
	}

	t.Run("not computed yet is a lag", func(t *testing.T) {
		ms := CompareAccountEpoch(
			"preview", 100, koios, nil, now, 24, longClosed, true,
		)
		got := categories(ms)
		require.Len(t, got, 2)
		for _, c := range got {
			assert.Equal(t, CategoryReferenceLag, c,
				"an epoch Dingo has not computed cannot be a divergence")
		}
	})

	t.Run("a differing amount while pending is also a lag", func(t *testing.T) {
		dingoRows := []DingoAccountReward{
			{StakeAddress: "stake_test1a", RewardType: "member", Amount: "999999"},
		}
		ms := CompareAccountEpoch(
			"preview", 100, koios[:1], dingoRows, now, 24, longClosed, true,
		)
		found := false
		for _, m := range ms {
			if m.Field == "account_reward_amount" {
				found = true
				assert.Equal(t, CategoryReferenceLag, m.Category,
					"an amount that can still change is not a divergence")
			}
		}
		// Without this the assertions above never run if the comparison stops
		// emitting the mismatch at all, and the case passes vacuously.
		require.True(t, found,
			"the differing amount must still be reported, as a lag")
	})

	t.Run("a differing amount once applied is a mismatch", func(t *testing.T) {
		dingoRows := []DingoAccountReward{
			{StakeAddress: "stake_test1a", RewardType: "member", Amount: "999999"},
		}
		ms := CompareAccountEpoch(
			"preview", 100, koios[:1], dingoRows, now, 24, longClosed, false,
		)
		found := false
		for _, m := range ms {
			if m.Field == "account_reward_amount" {
				found = true
				assert.Equal(t, CategoryValueMismatch, m.Category)
			}
		}
		assert.True(t, found, "a differing amount must still be reported")
	})

	t.Run("computed and still absent is a real finding", func(t *testing.T) {
		ms := CompareAccountEpoch(
			"preview", 100, koios, nil, now, 24, longClosed, false,
		)
		got := categories(ms)
		require.Len(t, got, 2)
		for _, c := range got {
			assert.Equal(t, CategoryAcctOnlyKoios, c,
				"once Dingo has had its chance, absence is worth reporting")
		}
	})
}
