// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// An epoch whose precompute was interrupted and resumed at another captured
// slot holds rows at both slots, and a range holding only the later slot
// still copies the later chunk's pool and account rows. The earlier chunk
// sorts first, so a decision taken from the epoch's first row alone would
// skip the epoch.
func TestRewardOutputFactCopiesKeepMixedSlotEpochRows(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })

	const (
		epoch     = uint64(4)
		earlySlot = uint64(100)
		lateSlot  = uint64(150)
	)
	chunk := func(pool byte, slot uint64) {
		poolKey := bytes.Repeat([]byte{pool}, 28)
		tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
		require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
			RewardDerived: []ledger.RewardDerivedChange{
				{
					Kind: ledger.RewardDerivedPools, Epoch: epoch,
					Pools: []*models.RewardPoolOutput{{
						PoolKeyHash: poolKey, Epoch: epoch, TotalReward: 9,
						CapturedSlot: slot, BoundarySlot: 1_200,
					}},
				},
				{
					Kind: ledger.RewardDerivedAccounts, Epoch: epoch,
					Accounts: []*models.RewardAccountOutput{{
						StakingKey:  bytes.Repeat([]byte{pool + 0x10}, 28),
						PoolKeyHash: poolKey, RewardType: "member",
						Epoch: epoch, Amount: 5, Spendable: true,
						CapturedSlot: slot, BoundarySlot: 1_200,
					}},
				},
			},
		}))
	}
	chunk(1, earlySlot)
	chunk(2, lateSlot)

	epochs, err := set.RewardDerivedEpochs()
	require.NoError(t, err)
	require.Equal(t, []uint64{epoch}, epochs)

	pools, err := ledger.PoolOutputRowsOver(set, epochs, 120, 200)
	require.NoError(t, err)
	require.Len(t, pools.Rows, 1, "the later chunk's pool row was dropped")
	accounts, cursor, err := ledger.AccountOutputPageOver(
		set, epochs, 120, 200, 0, 100,
	)
	require.NoError(t, err)
	require.Zero(t, cursor)
	require.Len(t, accounts.Rows, 1, "the later chunk's account row was dropped")

	// Every chunk is outside a range below the earliest slot.
	pools, err = ledger.PoolOutputRowsOver(set, epochs, 0, earlySlot-1)
	require.NoError(t, err)
	require.Empty(t, pools.Rows)
	accounts, _, err = ledger.AccountOutputPageOver(
		set, epochs, 0, earlySlot-1, 0, 100,
	)
	require.NoError(t, err)
	require.Empty(t, accounts.Rows)
}
