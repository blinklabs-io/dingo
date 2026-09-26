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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessEpochDelayingActionEndsRatifyPass pins Conway RATIFY's delay:
// once NoConfidence, UpdateCommittee, NewConstitution, or HardForkInitiation
// is accepted, no later action is accepted in the same pass, even an
// otherwise eligible ParameterChange or TreasuryWithdrawal. The control case
// shows those two actions are eligible without the delaying action.
func TestProcessEpochDelayingActionEndsRatifyPass(t *testing.T) {
	t.Parallel()

	constitution := &lcommon.NewConstitutionGovAction{
		Type: uint(lcommon.GovActionTypeNewConstitution),
	}
	constitution.Constitution.Anchor.Url = "https://example.invalid/c"
	hardFork := &lcommon.HardForkInitiationGovAction{
		Type: uint(lcommon.GovActionTypeHardForkInitiation),
	}
	hardFork.ProtocolVersion.Major = 11
	tests := []struct {
		name       string
		actionType lcommon.GovActionType
		action     any
	}{
		{name: "none"},
		{
			name:       "no confidence",
			actionType: lcommon.GovActionTypeNoConfidence,
			action: &lcommon.NoConfidenceGovAction{
				Type: uint(lcommon.GovActionTypeNoConfidence),
			},
		},
		{
			name:       "update committee",
			actionType: lcommon.GovActionTypeUpdateCommittee,
			action: &lcommon.UpdateCommitteeGovAction{
				Type:   uint(lcommon.GovActionTypeUpdateCommittee),
				Quorum: newRat(2, 3),
			},
		},
		{
			name:       "new constitution",
			actionType: lcommon.GovActionTypeNewConstitution,
			action:     constitution,
		},
		{
			name:       "hard fork initiation",
			actionType: lcommon.GovActionTypeHardForkInitiation,
			action:     hardFork,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
			withdrawalAddr, err := lcommon.NewAddressFromBytes(
				buildRewardAddr(t, testBytes(28, 0xC5)),
			)
			require.NoError(t, err)
			withdrawalCbor, err := cbor.Encode(
				&lcommon.TreasuryWithdrawalGovAction{
					Type: uint(lcommon.GovActionTypeTreasuryWithdrawal),
					Withdrawals: map[*lcommon.Address]uint64{
						&withdrawalAddr: 1,
					},
				},
			)
			require.NoError(t, err)
			proposals := []*models.GovernanceProposal{
				chainTestProposal(
					lcommon.GovActionTypeParameterChange,
					testBytes(32, 0xC1), nil, 100, 0, testBytes(29, 0),
					chainTestParameterChange(t, 61),
				),
				chainTestProposal(
					lcommon.GovActionTypeTreasuryWithdrawal,
					testBytes(32, 0xC2), nil, 100, 0, testBytes(29, 0),
					withdrawalCbor,
				),
			}
			var delaying *models.GovernanceProposal
			if test.action != nil {
				actionCbor, err := cbor.Encode(test.action)
				require.NoError(t, err)
				// Submitted last, so only priority puts it first.
				delaying = chainTestProposal(
					test.actionType, testBytes(32, 0xC3), nil, 900, 0,
					testBytes(29, 0), actionCbor,
				)
				proposals = append(proposals, delaying)
			}
			stored := chainTestStore(t, db, proposals...)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := chainTestRunEpoch(
				t, db, stabilityTestEpoch, chainTestPParams(),
			)
			if delaying == nil {
				assert.Equal(t, 2, out.RatifiedCount)
				return
			}
			assert.Equal(t, 1, out.RatifiedCount)
			assert.NotNil(t, chainTestReload(t, db, delaying).RatifiedEpoch)
			for _, proposal := range proposals[:2] {
				assert.Nil(t, chainTestReload(t, db, proposal).RatifiedEpoch)
			}
		})
	}
}
