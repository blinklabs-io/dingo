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

func chainTestDelayingAction(
	t *testing.T,
	actionType lcommon.GovActionType,
) []byte {
	t.Helper()
	var action any
	switch actionType {
	case lcommon.GovActionTypeNoConfidence:
		action = &lcommon.NoConfidenceGovAction{Type: uint(actionType)}
	case lcommon.GovActionTypeUpdateCommittee:
		action = &lcommon.UpdateCommitteeGovAction{
			Type:   uint(actionType),
			Quorum: newRat(2, 3),
		}
	case lcommon.GovActionTypeNewConstitution:
		constitution := &lcommon.NewConstitutionGovAction{
			Type: uint(actionType),
		}
		constitution.Constitution.Anchor.Url = "https://example.invalid/c"
		action = constitution
	case lcommon.GovActionTypeHardForkInitiation:
		hardFork := &lcommon.HardForkInitiationGovAction{
			Type: uint(actionType),
		}
		hardFork.ProtocolVersion.Major = 11
		action = hardFork
	default:
		t.Fatalf("not a delaying action type: %d", actionType)
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	return encoded
}

// TestProcessEpochDelayingActionPriorityBoundaries pins the Conway RATIFY
// order between adjacent delaying priorities (NoConfidence 0, UpdateCommittee
// 1, NewConstitution 2, HardForkInitiation 3). With both actions eligible and
// the lower-priority one submitted first, the higher-priority action is
// accepted and its delay leaves the other pending.
func TestProcessEpochDelayingActionPriorityBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		first, second lcommon.GovActionType
	}{
		{
			name:   "no confidence before update committee",
			first:  lcommon.GovActionTypeNoConfidence,
			second: lcommon.GovActionTypeUpdateCommittee,
		},
		{
			name:   "update committee before new constitution",
			first:  lcommon.GovActionTypeUpdateCommittee,
			second: lcommon.GovActionTypeNewConstitution,
		},
		{
			name:   "new constitution before hard fork",
			first:  lcommon.GovActionTypeNewConstitution,
			second: lcommon.GovActionTypeHardForkInitiation,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
			// The lower-priority action is submitted first, so only
			// priority can put the other one ahead of it.
			lower := chainTestProposal(
				test.second, testBytes(32, 0xD8), nil, 100, 0,
				testBytes(29, 0), chainTestDelayingAction(t, test.second),
			)
			higher := chainTestProposal(
				test.first, testBytes(32, 0xD9), nil, 200, 0,
				testBytes(29, 0), chainTestDelayingAction(t, test.first),
			)
			stored := chainTestStore(t, db, lower, higher)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := chainTestRunEpoch(
				t, db, stabilityTestEpoch, chainTestPParams(),
			)
			assert.Equal(t, 1, out.RatifiedCount)
			assert.NotNil(t, chainTestReload(t, db, higher).RatifiedEpoch)
			assert.Nil(t, chainTestReload(t, db, lower).RatifiedEpoch)
		})
	}
}
