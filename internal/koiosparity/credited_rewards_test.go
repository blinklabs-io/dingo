package koiosparity

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreditedAccountRewardsSkipsUncredited pins that the per-account
// comparison sees only the rewards the ledger actually credited.
//
// reward_account_output holds every reward the calculation produced, credited
// or not. The ledger's own application skips a row that is not spendable and
// one whose reward account is guarded by CIP-0163 expiry, so Koios never
// reports either — feeding them to the comparison makes Dingo look like it
// paid a reward nobody received.
func TestCreditedAccountRewardsSkipsUncredited(t *testing.T) {
	// Real credentials from Preview epoch 197, where all three of the epoch's
	// unspendable rows were reported as acct_only_dingo.
	unspendable := mustDecodeHex(
		t,
		"72A4EA5A1B4B170052E279055B6C2B75773006B1062C749376C9D68B",
	)
	guarded := mustDecodeHex(
		t,
		"E392F348B98E66A84389463BA547C2C551586D9973B3DE8C8B044388",
	)
	credited := mustDecodeHex(
		t,
		"F8ADA2B9A94FDD95D35D482BDDDF5A66FFA5B330B539B4613255C1DC",
	)

	rows, errs := creditedAccountRewards([]*models.RewardAccountOutput{
		{StakingKey: unspendable, RewardType: "member", Amount: 69019, Spendable: false},
		{StakingKey: guarded, RewardType: "member", Amount: 1409915, Spendable: true, Guarded: true},
		{StakingKey: credited, RewardType: "member", Amount: 500, Spendable: true},
	})
	require.Empty(t, errs)
	require.Len(t, rows, 1,
		"only the credited row belongs in the comparison")
	assert.Equal(t, "500", rows[0].Amount)
}

// TestCreditedAccountRewardsKeepsLeaderRewards guards the obvious overreach.
// A leader reward is credited to the pool's reward account and Koios reports
// it, so the filter must be about crediting, not about reward type — unlike
// the pool-level member-total path, which filters by type because it is
// summing member stake rewards specifically.
func TestCreditedAccountRewardsKeepsLeaderRewards(t *testing.T) {
	key := mustDecodeHex(
		t,
		"F8ADA2B9A94FDD95D35D482BDDDF5A66FFA5B330B539B4613255C1DC",
	)
	rows, errs := creditedAccountRewards([]*models.RewardAccountOutput{
		{StakingKey: key, RewardType: "leader", Amount: 1515378117, Spendable: true},
	})
	require.Empty(t, errs)
	require.Len(t, rows, 1)
	assert.Equal(t, "leader", rows[0].RewardType)
}

// TestCreditedAccountRewardsReportsDecodeFailure keeps the decode error
// surfacing that the inline loop had: a credential that cannot be turned into
// a stake address is a database problem worth reporting, not a row to drop.
func TestCreditedAccountRewardsReportsDecodeFailure(t *testing.T) {
	rows, errs := creditedAccountRewards([]*models.RewardAccountOutput{
		{StakingKey: []byte{0x01, 0x02}, RewardType: "member", Amount: 1, Spendable: true},
	})
	assert.Empty(t, rows)
	require.Len(t, errs, 1)
}

// An uncredited row with an undecodable credential must not be reported: the
// comparison never would have used it.
func TestCreditedAccountRewardsIgnoresUncreditedDecodeFailure(t *testing.T) {
	rows, errs := creditedAccountRewards([]*models.RewardAccountOutput{
		{StakingKey: []byte{0x01, 0x02}, RewardType: "member", Amount: 1, Spendable: false},
	})
	assert.Empty(t, rows)
	assert.Empty(t, errs)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
