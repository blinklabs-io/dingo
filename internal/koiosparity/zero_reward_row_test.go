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
		now, 0, time.Time{}, false,
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
		now, 0, time.Time{}, false,
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
		now, 0, time.Time{}, false,
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
		now, 0, time.Time{}, false,
	)
	assert.Empty(t, out)
}

// TestZeroRewardRowAmountSpellings pins both halves of isZeroRewardAmount's
// contract at the level that matters — CompareAccountEpoch's verdict on a
// one-sided row — rather than on the helper in isolation.
//
// Without these, replacing the helper's body with
// `strings.TrimSpace(amount) == "0"` leaves the whole package green: every
// other case in this file spells the amount "0" or "1", so neither "parsed,
// not compared to the literal" nor "an unparseable amount is not zero" is
// pinned. A waived row is the one outcome this category can produce that a
// parity checker must never produce by accident.
func TestZeroRewardRowAmountSpellings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		earned   string
		category string
		status   string
	}{
		// Zero however it is spelled: the two sides format independently.
		{"zero", "0", CategoryAcctZeroRewardRow, StatusPass},
		{"leading zeros", "00", CategoryAcctZeroRewardRow, StatusPass},
		// Not zero, and not waivable: each of these is malformed data, and a
		// malformed amount is a real value the comparison must keep
		// reporting.
		{"empty", "", CategoryAcctOnlyKoios, StatusFail},
		{"non-numeric", "abc", CategoryAcctOnlyKoios, StatusFail},
		{"negative zero", "-0", CategoryAcctOnlyKoios, StatusFail},
		{"signed zero", "+0", CategoryAcctOnlyKoios, StatusFail},
		{"padded zero", " 0", CategoryAcctOnlyKoios, StatusFail},
		{"nonzero", "1", CategoryAcctOnlyKoios, StatusFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := CompareAccountEpoch(
				"preview", 222,
				[]KoiosAccountRewards{{
					StakeAddress: zeroRewardAddr,
					RewardType:   "leader",
					Earned:       tc.earned,
				}},
				nil,
				time.Now(), 0, time.Time{}, false,
			)
			require.Len(t, out, 1)
			assert.Equal(t, tc.category, out[0].Category)
			assert.Equal(t, tc.status, DetermineStatus(out))
		})
	}
}

// TestZeroRewardRowAgreesWithValueComparison is the property behind
// parseLovelace: the presence path and the value path must not read the same
// string two different ways.
//
// A spelling isZeroRewardAmount waives on a one-sided row is a spelling
// lovelaceEqual must also call zero when both sides carry it, and one it
// rejects must be rejected there too. Before the shared parse, " 0" was
// agreement one-sided and value_mismatch two-sided, and "+0" was the reverse
// — the same input, two verdicts, depending only on whether the other side
// happened to have a row.
func TestZeroRewardRowAgreesWithValueComparison(t *testing.T) {
	for _, amount := range []string{
		"0", "00", "", "abc", "-0", "+0", " 0", "0 ", "1",
	} {
		t.Run(amount, func(t *testing.T) {
			waivedOneSided := isZeroRewardAmount(amount)
			agreesWithZero := lovelaceEqual(amount, "0")
			assert.Equal(t, waivedOneSided, agreesWithZero,
				"presence and value paths must read %q the same way", amount)
		})
	}
}
