// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package governance

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ada = uint64(1_000_000)

// Conway seeds each RATIFY pass from the treasury the EPOCH rule leaves
// behind (Governance.hs setFreshDRepPulsingState:
// `ensTreasuryL .~ epochState ^. treasuryL`). By then EPOCH has applied the
// boundary's enacted withdrawals to registered accounts only
// (applyEnactedWithdrawals), and moved the epoch's donations and unclaimed
// proposal deposits into the treasury
// (`casTreasuryL <>~ (utxosDonation <> fold unclaimed)`).

func seedTestRunEpoch(
	t *testing.T,
	db *database.Database,
	newEpoch uint64,
	donations uint64,
) *EpochOutput {
	t.Helper()
	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:                       db,
		Txn:                      txn,
		PrevEpoch:                newEpoch - 1,
		NewEpoch:                 newEpoch,
		BoundarySlot:             newEpoch * 100,
		PParams:                  chainTestPParams(),
		UpdateFn:                 eras.PParamsUpdateConway,
		PendingTreasuryDonations: donations,
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
	return out
}

func seedTestWithdrawalTo(
	t *testing.T,
	destination []byte,
	amount uint64,
) []byte {
	t.Helper()
	address, err := lcommon.NewAddressFromBytes(destination)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        uint(lcommon.GovActionTypeTreasuryWithdrawal),
		Withdrawals: map[*lcommon.Address]uint64{&address: amount},
	})
	require.NoError(t, err)
	return encoded
}

func seedTestRatified(proposal *models.GovernanceProposal) {
	ratifiedEpoch := stabilityTestEpoch - 1
	ratifiedSlot := ratifiedEpoch * 100
	proposal.RatifiedEpoch = &ratifiedEpoch
	proposal.RatifiedSlot = &ratifiedSlot
}

// TestProcessEpochRatifyTreasuryIncludesBoundaryDonations is the dingo#4467
// case: 100 ADA of treasury plus a 50 ADA donation from the ended epoch lets
// a 120 ADA withdrawal ratify, and the donation does not move the treasury
// inside ProcessEpoch (the caller credits it afterwards).
func TestProcessEpochRatifyTreasuryIncludesBoundaryDonations(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		donations    uint64
		wantRatified bool
	}{
		{name: "with donation", donations: 50 * ada, wantRatified: true},
		{name: "without donation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(100*ada, 20*ada, 1, nil))
			withdrawal := chainTestProposal(
				lcommon.GovActionTypeTreasuryWithdrawal,
				testBytes(32, 0xE1), nil, 400, 0, testBytes(29, 0),
				chainTestWithdrawal(t, 0xE2, 120*ada),
			)
			stored := chainTestStore(t, db, withdrawal)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := seedTestRunEpoch(t, db, stabilityTestEpoch, test.donations)
			assert.Equal(t, test.wantRatified,
				chainTestReload(t, db, withdrawal).RatifiedEpoch != nil)
			assert.Equal(t, test.wantRatified, out.RatifiedCount == 1)
			treasury, reserves := chainTestTreasury(t, store)
			assert.Equal(t, 100*ada, treasury)
			assert.Equal(t, 20*ada, reserves)
		})
	}
}

// TestProcessEpochRatifyTreasuryFollowsEnactedWithdrawalOutcome covers the
// boundary's own ENACT. A withdrawal to a registered account leaves the
// treasury; one to an unregistered account stays in it, and so does an
// unclaimed deposit refund, including a withdrawal whose return account is
// also its destination. The next RATIFY sees exactly what remains.
func TestProcessEpochRatifyTreasuryFollowsEnactedWithdrawalOutcome(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name string
		// registered selects whether the enacted withdrawal's destination
		// (and, with sameReturn, its return account) is a registered account.
		registered   bool
		sameReturn   bool
		pending      uint64
		wantRatified bool
		wantTreasury uint64
		wantReward   uint64
	}{
		{
			name:         "registered destination leaves 40",
			registered:   true,
			pending:      45 * ada,
			wantTreasury: 40 * ada,
			wantReward:   60 * ada,
		},
		{
			name:         "unclaimed withdrawal stays in treasury",
			pending:      95 * ada,
			wantRatified: true,
			wantTreasury: 100 * ada,
		},
		{
			name:         "return account is registered destination",
			registered:   true,
			sameReturn:   true,
			pending:      45 * ada,
			wantTreasury: 40 * ada,
			wantReward:   60*ada + 30*ada,
		},
		{
			name:         "return account is unregistered destination",
			sameReturn:   true,
			pending:      125 * ada,
			wantRatified: true,
			wantTreasury: 130 * ada,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(100*ada, 20*ada, 1, nil))
			destinationCred := testBytes(28, 0xE3)
			destination := buildRewardAddr(t, destinationCred)
			if test.registered {
				destinationCred, destination = chainTestRegisteredAccount(
					t, store, 0xE3,
				)
			}
			returnAddress, deposit := testBytes(29, 0), uint64(0)
			if test.sameReturn {
				returnAddress, deposit = destination, 30*ada
			}
			enacted := chainTestProposal(
				lcommon.GovActionTypeTreasuryWithdrawal,
				testBytes(32, 0xE4), nil, 300, deposit, returnAddress,
				seedTestWithdrawalTo(t, destination, 60*ada),
			)
			seedTestRatified(enacted)
			pending := chainTestProposal(
				lcommon.GovActionTypeTreasuryWithdrawal,
				testBytes(32, 0xE5), nil, 400, 0, testBytes(29, 0),
				chainTestWithdrawal(t, 0xE6, test.pending),
			)
			stored := chainTestStore(t, db, enacted, pending)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := seedTestRunEpoch(t, db, stabilityTestEpoch, 0)
			require.Equal(t, 1, out.EnactedCount)
			assert.Equal(t, test.wantRatified,
				chainTestReload(t, db, pending).RatifiedEpoch != nil)
			treasury, reserves := chainTestTreasury(t, store)
			assert.Equal(t, test.wantTreasury, treasury)
			assert.Equal(t, 20*ada, reserves)
			if test.registered {
				assert.Equal(t, test.wantReward,
					chainTestReward(t, store, destinationCred))
			}
		})
	}
}

// TestProcessEpochRatifyTreasuryIncludesUnclaimedRefunds covers deposit
// refunds that land in the treasury at the boundary: an enacted proposal's
// and an expired proposal's (the DROP step), each returned to an
// unregistered account.
func TestProcessEpochRatifyTreasuryIncludesUnclaimedRefunds(t *testing.T) {
	t.Parallel()

	for _, expired := range []bool{false, true} {
		name := "enacted proposal"
		if expired {
			name = "expired proposal"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(100*ada, 20*ada, 1, nil))
			unregistered := buildRewardAddr(t, testBytes(28, 0xE7))
			refunded := chainTestProposal(
				lcommon.GovActionTypeParameterChange,
				testBytes(32, 0xE8), nil, 300, 50*ada, unregistered,
				chainTestParameterChange(t, 61),
			)
			if expired {
				expiredEpoch := stabilityTestEpoch - 1
				expiredSlot := expiredEpoch * 100
				refunded.ExpiresEpoch = stabilityTestEpoch - 2
				refunded.ExpiredEpoch = &expiredEpoch
				refunded.ExpiredSlot = &expiredSlot
			} else {
				seedTestRatified(refunded)
			}
			pending := chainTestProposal(
				lcommon.GovActionTypeTreasuryWithdrawal,
				testBytes(32, 0xE9), nil, 400, 0, testBytes(29, 0),
				chainTestWithdrawal(t, 0xEA, 120*ada),
			)
			stored := chainTestStore(t, db, refunded, pending)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := seedTestRunEpoch(t, db, stabilityTestEpoch, 0)
			if expired {
				require.Equal(t, 1, out.DroppedCount)
			} else {
				require.Equal(t, 1, out.EnactedCount)
			}
			assert.NotNil(t, chainTestReload(t, db, pending).RatifiedEpoch)
			treasury, reserves := chainTestTreasury(t, store)
			assert.Equal(t, 150*ada, treasury)
			assert.Equal(t, 20*ada, reserves)
		})
	}
}

// TestProcessEpochRatifyTreasuryCompetingWithdrawals shows that the seeded
// budget is a running one: against 100 ADA plus a 50 ADA donation, a 120 ADA
// withdrawal ratifies and leaves too little for the later 40 ADA one. A
// rollback of the boundary followed by a replay gives the same result.
func TestProcessEpochRatifyTreasuryCompetingWithdrawals(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(100*ada, 20*ada, 1, nil))
	first := chainTestProposal(
		lcommon.GovActionTypeTreasuryWithdrawal,
		testBytes(32, 0xEB), nil, 400, 0, testBytes(29, 0),
		chainTestWithdrawal(t, 0xEC, 120*ada),
	)
	second := chainTestProposal(
		lcommon.GovActionTypeTreasuryWithdrawal,
		testBytes(32, 0xED), nil, 500, 0, testBytes(29, 0),
		chainTestWithdrawal(t, 0xEE, 40*ada),
	)
	stored := chainTestStore(t, db, first, second)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

	check := func() {
		t.Helper()
		out := seedTestRunEpoch(t, db, stabilityTestEpoch, 50*ada)
		assert.Equal(t, 1, out.RatifiedCount)
		assert.NotNil(t, chainTestReload(t, db, first).RatifiedEpoch)
		assert.Nil(t, chainTestReload(t, db, second).RatifiedEpoch)
	}
	check()
	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(
		stabilityTestEpoch*100-1, nil,
	))
	assert.Nil(t, chainTestReload(t, db, first).RatifiedEpoch)
	check()
}
