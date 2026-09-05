package koiosparity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const zeroRewardAddr = "stake_test1uzf5lwsf37wsxmq9rdpq0v9tepk0g36vqmxr974lenzwchcszrsss"

// TestZeroEarnedKoiosRowIsNotDivergence pins that a Koios reward row worth
// zero, with no Dingo counterpart, is agreement rather than acct_only_koios.
//
// The category doc comment asserted that "Koios never emits a row for zero
// reward". Preview disproves it: Koios publishes zero-earned leader rows, and
// Dingo writes no reward_account_output row at all for a zero reward. Nothing
// was credited on either side, so no lovelace differs — but the presence test
// read the row as a reward Dingo had missed and failed epoch 222.
func TestZeroEarnedKoiosRowIsNotDivergence(t *testing.T) {
	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		[]KoiosAccountRewards{
			{
				StakeAddress: zeroRewardAddr,
				RewardType:   "leader",
				Earned:       "0",
			},
		},
		nil,
		now, 0, time.Time{},
	)
	require.Len(t, out, 1, "the row should still be reported, not dropped")
	assert.Equal(t, CategoryAcctZeroRewardRow, out[0].Category)
	assert.Equal(t, StatusPass, DetermineStatus(out),
		"a zero-earned row must never fail an epoch")
}

// TestZeroAmountDingoRowIsNotDivergence is the mirror. Dingo emits no
// zero-amount rows today, but the two presence branches are deliberately
// symmetric and a future zero row must not fail an epoch for the same reason.
func TestZeroAmountDingoRowIsNotDivergence(t *testing.T) {
	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		nil,
		[]DingoAccountReward{
			{
				StakeAddress: zeroRewardAddr,
				RewardType:   "leader",
				Amount:       "0",
			},
		},
		now, 0, time.Time{},
	)
	require.Len(t, out, 1)
	assert.Equal(t, CategoryAcctZeroRewardRow, out[0].Category)
	assert.Equal(t, StatusPass, DetermineStatus(out))
}

// TestNonZeroKoiosOnlyRowStillFails is the discrimination check: the change
// must turn off only the zero case, never one-sided rows generally.
func TestNonZeroKoiosOnlyRowStillFails(t *testing.T) {
	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		[]KoiosAccountRewards{
			{
				StakeAddress: zeroRewardAddr,
				RewardType:   "leader",
				Earned:       "1",
			},
		},
		nil,
		now, 0, time.Time{},
	)
	require.Len(t, out, 1)
	assert.Equal(t, CategoryAcctOnlyKoios, out[0].Category)
	assert.Equal(t, StatusFail, DetermineStatus(out))
}

// A zero-earned row on both sides is an ordinary match and reports nothing —
// the zero handling must not start manufacturing rows for agreeing pairs.
func TestZeroOnBothSidesReportsNothing(t *testing.T) {
	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		[]KoiosAccountRewards{
			{StakeAddress: zeroRewardAddr, RewardType: "leader", Earned: "0"},
		},
		[]DingoAccountReward{
			{StakeAddress: zeroRewardAddr, RewardType: "leader", Amount: "0"},
		},
		now, 0, time.Time{},
	)
	assert.Empty(t, out)
}
