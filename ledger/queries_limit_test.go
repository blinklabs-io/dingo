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
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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
	require.Equal(t, "boundary", limitErr.QueryName)
	require.Equal(t, MaxLocalStateQueryItems+1, limitErr.SubmittedItemCount)
	require.Equal(t, MaxLocalStateQueryItems, limitErr.MaximumAllowedItemCount)
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
			require.Equal(t, test.query, limitErr.QueryName)
			require.Equal(t, itemCount, limitErr.SubmittedItemCount)
			require.Equal(
				t,
				MaxLocalStateQueryItems,
				limitErr.MaximumAllowedItemCount,
			)
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

// TestLocalStateQueryEmptyDRepStateMatchesPerDRepDelegators verifies that the
// batched empty-filter form (allDRepDelegators) returns the same delegator
// list, in the same order, as the per-DRep read (drepDelegators) it replaced.
// Both a DRep with several delegators and a table with more than one DRep are
// needed to exercise the grouping this form does that the per-DRep loop
// never had to: an assertion built from a single DRep or delegator can't tell
// a correct group-by from one that drops or misattributes a row.
func TestLocalStateQueryEmptyDRepStateMatchesPerDRepDelegators(t *testing.T) {
	db := newTestDB(t)
	txn := db.MetadataTxn(true)
	t.Cleanup(func() { txn.Rollback() }) //nolint:errcheck

	const numDreps = 5
	const delegatorsPerDrep = 4
	drepCredentials := make([][]byte, numDreps)
	for d := range numDreps {
		credential := make([]byte, 28)
		binary.BigEndian.PutUint64(credential[20:], uint64(d))
		drepCredentials[d] = credential
		require.NoError(t, db.CreateDrep(txn, &models.Drep{
			Credential: credential,
			Active:     true,
			AddedSlot:  1,
		}))
	}
	require.NoError(t, txn.Commit())

	for d := range numDreps {
		for k := range delegatorsPerDrep {
			stakingKey := make([]byte, 28)
			binary.BigEndian.PutUint32(stakingKey[16:], uint32(d))
			binary.BigEndian.PutUint32(stakingKey[24:], uint32(k))
			require.NoError(t, db.CreateAccount(nil, &models.Account{
				StakingKey:    stakingKey,
				CredentialTag: 0,
				Drep:          drepCredentials[d],
				DrepType:      models.DrepTypeAddrKeyHash,
				Active:        true,
			}))
		}
	}

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()
	result, err := ls.queryShelleyDRepState(nil)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	emptyForm, ok := outer[0].(olocalstatequery.DRepStateResult)
	require.True(t, ok)
	require.Len(t, emptyForm, numDreps)

	dreps, err := db.GetActiveDreps(nil)
	require.NoError(t, err)
	require.Len(t, dreps, numDreps)
	for _, drep := range dreps {
		want, err := ls.drepDelegators(drep)
		require.NoError(t, err)
		require.Len(t, want, delegatorsPerDrep)
		key := olocalstatequery.StakeCredential{
			Tag:   uint64(drep.CredentialTag),
			Bytes: gledger.NewBlake2b224(drep.Credential),
		}
		entry, ok := emptyForm[key]
		require.True(
			t,
			ok,
			"drep %x missing from empty-form result",
			drep.Credential,
		)
		require.Equal(t, want, entry.Delegators)
	}
}

// TestLocalStateQueryLargeBatchHandlers verifies that handlers backed by batch
// database primitives accept collections larger than the per-item work limit,
// and that the batched reads return the right value for every requested item
// rather than merely succeeding. Delegated/seeded indices straddle the
// chunk boundaries GetAccountsByCredential and GetPoolStakeSnapshotsForPools
// use internally, so a chunk that drops, duplicates, or cross-contaminates
// results would fail these assertions.
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
	delegatedIdx := []int{0, 997, 998, 999, itemCount - 1}
	drepCredential := bytes.Repeat([]byte{0xAB}, 28)
	for _, idx := range delegatedIdx {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey:    credentials[idx].Credential[:],
			CredentialTag: 0,
			Drep:          drepCredential,
			DrepType:      models.DrepTypeAddrKeyHash,
			Active:        true,
		}))
	}
	result, err := (&LedgerState{db: db}).
		queryShelleyFilteredVoteDelegatees(credentials)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	delegatees, ok := outer[0].(olocalstatequery.FilteredVoteDelegateesResult)
	require.True(t, ok)
	require.Len(t, delegatees, len(delegatedIdx))
	for _, idx := range delegatedIdx {
		key := olocalstatequery.StakeCredential{
			Tag:   0,
			Bytes: gledger.NewBlake2b224(credentials[idx].Credential[:]),
		}
		drep, ok := delegatees[key]
		require.True(t, ok, "credential %d missing a delegation", idx)
		require.Equal(t, drepCredential, []byte(drep.Credential))
	}

	poolIds := make([]gledger.PoolId, itemCount)
	for i := range poolIds {
		binary.BigEndian.PutUint64(poolIds[i][20:], uint64(i))
	}
	snapshotIdx := []int{0, 996, 997, 998, itemCount - 1}
	snapshotStake := make(map[int]uint64, len(snapshotIdx))
	snapshots := make([]*models.PoolStakeSnapshot, 0, len(snapshotIdx))
	for n, idx := range snapshotIdx {
		stake := uint64(n+1) * 1_000_000
		snapshotStake[idx] = stake
		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:          2,
			SnapshotType:   "mark",
			PoolKeyHash:    poolIds[idx][:],
			TotalStake:     types.Uint64(stake),
			DelegatorCount: 1,
			CapturedSlot:   200,
		})
	}
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(snapshots, nil))

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
	outer, ok = result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	snapshotResult, ok := outer[0].(olocalstatequery.StakeSnapshotsResult)
	require.True(t, ok)
	// The below-PV11 explicit-filter form always returns every requested
	// pool, seeded or not.
	require.Len(t, snapshotResult.PoolSnapshots, itemCount)
	for i, poolId := range poolIds {
		snapshot, ok := snapshotResult.PoolSnapshots[gledger.NewBlake2b224(poolId[:])]
		require.True(t, ok, "pool %d missing from result", i)
		require.Equal(
			t,
			snapshotStake[i],
			snapshot.StakeMark,
			"pool %d has the wrong mark stake",
			i,
		)
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
