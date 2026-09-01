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
	"encoding/binary"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
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
	tests := []struct {
		name  string
		query string
		run   func() (any, error)
	}{
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

// TestLocalStateQueryEmptyDRepStateRemainsUnrestricted verifies that the
// empty-filter form can return more DReps than the caller-list limit because
// its delegators are loaded in batches instead of with one read per DRep.
func TestLocalStateQueryEmptyDRepStateRemainsUnrestricted(t *testing.T) {
	db := newTestDB(t)
	txn := db.MetadataTxn(true)
	t.Cleanup(func() { txn.Rollback() }) //nolint:errcheck
	itemCount := MaxLocalStateQueryItems + 1
	for i := range itemCount {
		credential := make([]byte, 28)
		binary.BigEndian.PutUint64(credential[20:], uint64(i))
		require.NoError(t, db.CreateDrep(txn, &models.Drep{
			Credential: credential,
			Active:     true,
			AddedSlot:  1,
		}))
	}
	require.NoError(t, txn.Commit())

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()
	result, err := ls.queryShelleyDRepState(nil)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	dreps, ok := outer[0].(olocalstatequery.DRepStateResult)
	require.True(t, ok)
	require.Len(t, dreps, itemCount)
}

// TestLocalStateQueryLargeBatchHandlers verifies that handlers backed by batch
// database primitives accept collections larger than the per-item work limit.
func TestLocalStateQueryLargeBatchHandlers(t *testing.T) {
	db := newTestDB(t)
	itemCount := MaxLocalStateQueryItems + 1

	credentials := make([]lcommon.Credential, itemCount)
	for i := range credentials {
		binary.BigEndian.PutUint64(
			credentials[i].Credential[20:],
			uint64(i),
		)
	}
	result, err := (&LedgerState{db: db}).
		queryShelleyFilteredVoteDelegatees(credentials)
	require.NoError(t, err)
	require.NotNil(t, result)

	poolIds := make([]gledger.PoolId, itemCount)
	for i := range poolIds {
		binary.BigEndian.PutUint64(poolIds[i][20:], uint64(i))
	}
	query := &olocalstatequery.ShelleyStakeSnapshotsQuery{
		Pools: []cbor.SetType[gledger.PoolId]{
			cbor.NewSetType(poolIds, true),
		},
	}
	ls := &LedgerState{db: db}
	ls.consensus.Store(
		&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}},
	)
	result, err = ls.queryShelleyStakeSnapshots(query)
	require.NoError(t, err)
	require.NotNil(t, result)
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
