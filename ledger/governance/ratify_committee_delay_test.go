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
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessEpochCommitteeActionEndsPassUntilEnacted pins how a
// committee-purpose action's staged state reaches later actions. Conway
// RATIFY stages NoConfidence and UpdateCommittee in its enact state, but both
// are delaying, so nothing else is accepted in that pass: neither an eligible
// ParameterChange nor the committee action's own chained successor. The
// successor is accepted at the next boundary, once ENACT has made its parent
// the committee root, and it ends that pass in turn. Neither waiting action is
// expired while the pass is delayed.
func TestProcessEpochCommitteeActionEndsPassUntilEnacted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		actionType lcommon.GovActionType
		action     any
	}{
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
				Quorum: newRat(1, 2),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(10, 20, 1, nil))

			parentCbor, err := cbor.Encode(test.action)
			require.NoError(t, err)
			parent := chainTestProposal(
				test.actionType, testBytes(32, 0xE1), nil, 400, 0,
				testBytes(29, 0), parentCbor,
			)
			childCbor, err := cbor.Encode(&lcommon.UpdateCommitteeGovAction{
				Type:   uint(lcommon.GovActionTypeUpdateCommittee),
				Quorum: newRat(2, 3),
			})
			require.NoError(t, err)
			child := chainTestProposal(
				lcommon.GovActionTypeUpdateCommittee, testBytes(32, 0xE2),
				parent, 500, 0, testBytes(29, 0), childCbor,
			)
			parameterChange := chainTestProposal(
				lcommon.GovActionTypeParameterChange, testBytes(32, 0xE3), nil,
				300, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
			)
			stored := chainTestStore(t, db, parent, child, parameterChange)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)
			// The second boundary tallies against its own mark snapshot.
			require.NoError(t, store.SavePoolStakeSnapshot(
				&models.PoolStakeSnapshot{
					Epoch: predictedBoundaryStakeEpochFor(
						stabilityTestEpoch + 1,
					),
					SnapshotType: "mark",
					PoolKeyHash:  testBytes(28, 0xD3),
					TotalStake:   types.Uint64(100),
				}, nil,
			))

			first := chainTestRunEpoch(
				t, db, stabilityTestEpoch, chainTestPParams(),
			)
			assert.Equal(t, 1, first.RatifiedCount)
			assert.Zero(t, first.ExpiredCount)
			assert.NotNil(t, chainTestReload(t, db, parent).RatifiedEpoch)
			for _, waiting := range []*models.GovernanceProposal{
				child, parameterChange,
			} {
				reloaded := chainTestReload(t, db, waiting)
				assert.Nil(t, reloaded.RatifiedEpoch)
				assert.Nil(t, reloaded.ExpiredEpoch)
			}

			second := chainTestRunEpoch(
				t, db, stabilityTestEpoch+1, chainTestPParams(),
			)
			assert.Equal(t, 1, second.EnactedCount)
			assert.NotNil(t, chainTestReload(t, db, parent).EnactedEpoch)
			assert.Equal(t, 1, second.RatifiedCount)
			assert.NotNil(
				t,
				chainTestReload(t, db, child).RatifiedEpoch,
				"the enacted parent is the committee root for its successor",
			)
			assert.Nil(
				t,
				chainTestReload(t, db, parameterChange).RatifiedEpoch,
				"the accepted successor ends this pass too",
			)
		})
	}
}
