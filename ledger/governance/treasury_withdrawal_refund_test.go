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

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessEpochEnactedWithdrawalToReturnAccountRefundsDeposit proves the
// Conway EPOCH accounting for a TreasuryWithdrawal whose deposit return
// account also receives a withdrawal: applyEnactedWithdrawals and
// returnProposalDeposits both credit that account, so it gains the
// withdrawal plus the deposit while the treasury loses only the withdrawal.
// A crash-replayed boundary must not credit either amount twice, and a
// rollback must undo both.
func TestProcessEpochEnactedWithdrawalToReturnAccountRefundsDeposit(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name             string
		separateReturn   bool
		wantDestination  uint64
		wantReturnReward uint64
	}{
		{
			name:            "return account is the destination",
			wantDestination: 1_000 + 1_600,
		},
		{
			name:             "separate return account",
			separateReturn:   true,
			wantDestination:  1_000,
			wantReturnReward: 1_600,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			const (
				treasury   = uint64(10_000)
				reserves   = uint64(20)
				withdrawal = uint64(1_000)
				deposit    = uint64(1_600)
			)
			require.NoError(
				t,
				store.SetNetworkState(treasury, reserves, 1, nil),
			)
			destinationCred, destinationAddr := chainTestRegisteredAccount(
				t, store, 0x41,
			)
			returnCred, returnAddr := destinationCred, destinationAddr
			if test.separateReturn {
				returnCred, returnAddr = chainTestRegisteredAccount(
					t, store, 0x42,
				)
			}
			address, err := lcommon.NewAddressFromBytes(destinationAddr)
			require.NoError(t, err)
			actionCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
				Type:        uint(lcommon.GovActionTypeTreasuryWithdrawal),
				Withdrawals: map[*lcommon.Address]uint64{&address: withdrawal},
			})
			require.NoError(t, err)
			ratifiedEpoch := uint64(4)
			ratifiedSlot := uint64(400)
			proposal := chainTestProposal(
				lcommon.GovActionTypeTreasuryWithdrawal, testBytes(32, 0x43),
				nil, 100, deposit, returnAddr, actionCbor,
			)
			proposal.RatifiedEpoch = &ratifiedEpoch
			proposal.RatifiedSlot = &ratifiedSlot
			require.NoError(t, db.SetGovernanceProposal(proposal, nil))

			assertPots := func(
				wantTreasury, wantDestination, wantReturn uint64,
			) {
				t.Helper()
				gotTreasury, gotReserves := chainTestTreasury(t, store)
				assert.Equal(t, wantTreasury, gotTreasury, "treasury")
				assert.Equal(t, reserves, gotReserves, "reserves")
				assert.Equal(
					t,
					wantDestination,
					chainTestReward(t, store, destinationCred),
					"destination reward",
				)
				if test.separateReturn {
					assert.Equal(
						t,
						wantReturn,
						chainTestReward(t, store, returnCred),
						"return reward",
					)
				}
			}

			out := chainTestRunEpoch(t, db, 5, conwayPParamsFixture(10))
			require.Equal(t, 1, out.EnactedCount)
			assertPots(
				treasury-withdrawal,
				test.wantDestination,
				test.wantReturnReward,
			)
			// Value is conserved: the treasury and the held deposit become
			// the treasury remainder and the credited rewards.
			assert.Equal(
				t,
				treasury+deposit,
				treasury-withdrawal+test.wantDestination+
					test.wantReturnReward,
			)

			// Replay the committed boundary after the stake-reward step
			// rewrote the pot row, as a restart before the tip advance does.
			require.NoError(
				t,
				store.SetNetworkState(treasury, reserves, 500, nil),
			)
			out = chainTestRunEpoch(t, db, 5, conwayPParamsFixture(10))
			require.Equal(t, 0, out.EnactedCount)
			assertPots(
				treasury-withdrawal,
				test.wantDestination,
				test.wantReturnReward,
			)

			require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(499, nil))
			require.NoError(t, db.DeleteAccountRewardsAfterSlot(499, nil))
			require.NoError(t, db.DeleteNetworkStateAfterSlot(499, nil))
			assertPots(treasury, 0, 0)
			restored := chainTestReload(t, db, proposal)
			assert.Nil(t, restored.EnactedEpoch)
			require.NotNil(t, restored.RatifiedEpoch)

			out = chainTestRunEpoch(t, db, 5, conwayPParamsFixture(10))
			require.Equal(t, 1, out.EnactedCount)
			assertPots(
				treasury-withdrawal,
				test.wantDestination,
				test.wantReturnReward,
			)
			require.NotNil(t, chainTestReload(t, db, proposal).EnactedEpoch)
		})
	}
}
