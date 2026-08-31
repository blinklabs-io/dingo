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

package ledger

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// TestLocalStateQueryItemLimitBoundary verifies that a request at the maximum
// item count is accepted and that the first over-limit size returns both the
// stable sentinel error and the structured request details.
func TestLocalStateQueryItemLimitBoundary(t *testing.T) {
	require.NoError(t, checkLocalStateQueryItemLimit(
		"boundary",
		MaxLocalStateQueryItems,
	))

	err := checkLocalStateQueryItemLimit(
		"boundary",
		MaxLocalStateQueryItems+1,
	)
	require.ErrorIs(t, err, ErrLocalStateQueryLimitExceeded)

	var limitErr *LocalStateQueryLimitError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "boundary", limitErr.Query)
	require.Equal(t, MaxLocalStateQueryItems+1, limitErr.Items)
	require.Equal(t, MaxLocalStateQueryItems, limitErr.Limit)
}

// TestLocalStateQueryPerItemHandlersRejectOverLimitBeforeWork verifies that
// every query handler with per-item database work rejects oversized input
// before accessing database or consensus state.
func TestLocalStateQueryPerItemHandlersRejectOverLimitBeforeWork(t *testing.T) {
	ls := &LedgerState{}
	itemCount := MaxLocalStateQueryItems + 1

	stakeCredentials := make(
		[]olocalstatequery.StakeCredential,
		itemCount,
	)
	credentials := make([]lcommon.Credential, itemCount)
	poolIds := make([]gledger.PoolId, itemCount)
	stakeSnapshots := &olocalstatequery.ShelleyStakeSnapshotsQuery{
		Pools: []cbor.SetType[gledger.PoolId]{
			cbor.NewSetType(poolIds, true),
		},
	}

	tests := []struct {
		name  string
		query string
		run   func() (any, error)
	}{
		{
			name:  "stake snapshots",
			query: "GetStakeSnapshots",
			run: func() (any, error) {
				return ls.queryShelleyStakeSnapshots(stakeSnapshots)
			},
		},
		{
			name:  "DRep state",
			query: "GetDRepState",
			run: func() (any, error) {
				return ls.queryShelleyDRepState(credentials)
			},
		},
		{
			name:  "stake delegation deposits",
			query: "GetStakeDelegDeposits",
			run: func() (any, error) {
				return ls.queryShelleyStakeDelegDeposits(stakeCredentials)
			},
		},
		{
			name:  "filtered vote delegatees",
			query: "GetFilteredVoteDelegatees",
			run: func() (any, error) {
				return ls.queryShelleyFilteredVoteDelegatees(credentials)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.run()
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrLocalStateQueryLimitExceeded)

			var limitErr *LocalStateQueryLimitError
			require.True(t, errors.As(err, &limitErr))
			require.Equal(t, test.query, limitErr.Query)
			require.Equal(t, itemCount, limitErr.Items)
			require.Equal(t, MaxLocalStateQueryItems, limitErr.Limit)
		})
	}
}

// TestLocalStateQueryRepeatedOverLimitRequestsRemainBounded verifies that
// repeated oversized requests are rejected consistently and do not prevent a
// subsequent normal-sized request from being accepted.
func TestLocalStateQueryRepeatedOverLimitRequestsRemainBounded(t *testing.T) {
	for range 100 {
		err := checkLocalStateQueryItemLimit(
			"repeated",
			MaxLocalStateQueryItems+1,
		)
		require.ErrorIs(t, err, ErrLocalStateQueryLimitExceeded)
	}

	// Rejected requests do not poison later normal-sized requests.
	require.NoError(t, checkLocalStateQueryItemLimit("normal", 3))
}
