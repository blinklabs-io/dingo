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

package utxoset_test

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func derivedAccounts(
	epoch uint64,
	n int,
	slot uint64,
) []*models.RewardAccountOutput {
	rows := make([]*models.RewardAccountOutput, 0, n)
	for i := range n {
		key := make([]byte, 28)
		binary.BigEndian.PutUint32(
			key,
			uint32(i),
		) //nolint:gosec // a test index
		rows = append(rows, &models.RewardAccountOutput{
			StakingKey: key, PoolKeyHash: bytes.Repeat([]byte{byte(i % 3)}, 28),
			RewardType: "member", Epoch: epoch,
			CredentialTag: uint8(i % 2), //nolint:gosec // 0 or 1
			Amount:        types.Uint64(uint64(i) + 1),
			Spendable:     i%4 != 0, Guarded: i%9 == 0,
			CapturedSlot: slot, BoundarySlot: slot + 2,
		})
	}
	return rows
}

func derivedPool(epoch, slot uint64) *models.RewardPoolOutput {
	return &models.RewardPoolOutput{
		PoolKeyHash: bytes.Repeat([]byte{7}, 28), Epoch: epoch,
		ApparentPerformance: &types.Rat{Rat: big.NewRat(3, 4)},
		OptimalReward:       10, TotalReward: 9, LeaderReward: 4,
		MemberRewardTotal: 5, OwnerStake: 100, Unspendable: 1,
		CapturedSlot: slot, BoundarySlot: slot + 2,
	}
}

// The derived rows of an epoch are written in chunks under one tombstone,
// read back whole, by credential and as of a slot, kept by a rollback to or
// above their captured slot, dropped by one below it, wiped by a replace and
// pruned below the window, and a replay writes the rows the first pass did.
func TestDerivedRewardRowsAreChunkedRolledBackWipedAndPruned(t *testing.T) {
	t.Parallel()
	testutil.SkipUnderRace(
		t,
		"single-goroutine write, read and rollback of 150k rows "+
			"across store transactions; no other test covers the "+
			"derived-row chunking, so it runs in the un-instrumented "+
			"jobs",
	)
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	const rows = 150_000
	apply := func(slot uint64, changes ...ledger.RewardDerivedChange) {
		tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyLedgerChanges(
			&tip, ledger.LedgerChanges{RewardDerived: changes},
		))
	}
	cursor := []byte(`{"done":true}`)
	write := func(slot uint64, epoch uint64) {
		apply(slot,
			ledger.RewardDerivedChange{
				Kind: ledger.RewardDerivedPools, Epoch: epoch,
				Pools: []*models.RewardPoolOutput{derivedPool(epoch, slot)},
			},
			ledger.RewardDerivedChange{
				Kind: ledger.RewardDerivedAccounts, Epoch: epoch,
				Accounts: derivedAccounts(epoch, rows, slot),
			},
			ledger.RewardDerivedChange{
				Kind: ledger.RewardDerivedCursor, Epoch: epoch,
				Slot: slot, Cursor: cursor,
			},
		)
	}
	apply(5, ledger.RewardDerivedChange{
		Kind: ledger.RewardDerivedPools, Epoch: 4,
		Pools: []*models.RewardPoolOutput{derivedPool(4, 5)},
	})
	write(20, 6)

	pools, err := set.RewardPoolOutputRows(6, nil)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	require.Equal(t, derivedPool(6, 20), pools[0])
	accounts, err := set.RewardAccountOutputRows(6, nil)
	require.NoError(t, err)
	require.Len(t, accounts, rows)
	want := derivedAccounts(6, rows, 20)
	byKey := map[string]*models.RewardAccountOutput{}
	for _, a := range want {
		byKey[string([]byte{a.CredentialTag})+string(a.StakingKey)] = a
	}
	for _, got := range accounts {
		require.Equal(
			t,
			byKey[string([]byte{got.CredentialTag})+string(got.StakingKey)],
			got,
		)
	}
	one := want[5]
	byCredential, err := set.RewardCredentialOutputRows(
		6, one.CredentialTag, one.StakingKey, nil,
	)
	require.NoError(t, err)
	require.Equal(t, []*models.RewardAccountOutput{one}, byCredential)
	raw, ok, err := set.RewardCursorRow(6, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, cursor, raw)
	epochs, err := set.RewardDerivedEpochs()
	require.NoError(t, err)
	require.Equal(t, []uint64{4, 6}, epochs)

	// A rollback to the captured slot or above keeps the epoch.
	_, err = set.Rollback(ocommon.NewPoint(20, bytes.Repeat([]byte{20}, 32)))
	require.NoError(t, err)
	kept, err := set.RewardAccountOutputRows(6, nil)
	require.NoError(t, err)
	require.Len(t, kept, rows)
	seq, err := set.RewardRollbackSeq()
	require.NoError(t, err)
	require.NotZero(t, seq, "a rollback bumps the durable counter")

	// An as-of read below the capture finds none, and a rollback drops all.
	below := uint64(19)
	hidden, err := set.RewardAccountOutputRows(6, &below)
	require.NoError(t, err)
	require.Empty(t, hidden)
	_, err = set.Rollback(ocommon.NewPoint(5, bytes.Repeat([]byte{5}, 32)))
	require.NoError(t, err)
	for _, fn := range []func() (int, error){
		func() (int, error) {
			r, err := set.RewardAccountOutputRows(6, nil)
			return len(r), err
		},
		func() (int, error) {
			r, err := set.RewardPoolOutputRows(6, nil)
			return len(r), err
		},
	} {
		n, err := fn()
		require.NoError(t, err)
		require.Zero(t, n, "one tombstone drops every chunk's rows")
	}
	_, ok, err = set.RewardCursorRow(6, nil)
	require.NoError(t, err)
	require.False(t, ok)
	older, err := set.RewardPoolOutputRows(4, nil)
	require.NoError(t, err)
	require.Len(t, older, 1, "an epoch captured earlier stays")

	// The replay writes what the first pass did, a replace wipes it, and a
	// prune drops the epochs below the window.
	write(20, 6)
	replayed, err := set.RewardAccountOutputRows(6, nil)
	require.NoError(t, err)
	require.Len(t, replayed, rows)
	apply(
		21,
		ledger.RewardDerivedChange{Kind: ledger.RewardDerivedReplace, Epoch: 6},
	)
	for _, epoch := range []uint64{6} {
		r, err := set.RewardAccountOutputRows(epoch, nil)
		require.NoError(t, err)
		require.Empty(t, r)
		_, ok, err := set.RewardCursorRow(epoch, nil)
		require.NoError(t, err)
		require.False(t, ok)
	}
	write(22, 7)
	apply(
		23,
		ledger.RewardDerivedChange{
			Kind:       ledger.RewardDerivedPrune,
			PruneBelow: 7,
		},
	)
	r4, err := set.RewardPoolOutputRows(4, nil)
	require.NoError(t, err)
	require.Empty(t, r4)
	r7, err := set.RewardAccountOutputRows(7, nil)
	require.NoError(t, err)
	require.Len(t, r7, rows)
}
