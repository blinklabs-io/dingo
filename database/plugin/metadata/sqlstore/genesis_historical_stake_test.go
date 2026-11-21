// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlstore

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// Genesis stake delegations must be visible to the historical boundary stake
// reads at a slot after genesis, whichever genesis section wrote them.
func TestHistoricalStakeCountsGenesisDelegation(t *testing.T) {
	t.Parallel()
	testHistoricalStakeCountsGenesisDelegation(t, newDepositHeldStore)
}

// testHistoricalStakeCountsGenesisDelegation runs the scenarios against stores
// from newStore, so the same assertions cover each SQL dialect.
func testHistoricalStakeCountsGenesisDelegation(
	t *testing.T,
	newStore func(*testing.T) *Store,
) {
	t.Helper()
	const readSlot = 1_500
	pool := depositHeldPoolKey(0xd1)
	credential := stakeCredential(0xd2)

	setup := func(t *testing.T) *Store {
		t.Helper()
		store := newStore(t)
		depositHeldEpochs(t, store, 5)
		require.NoError(t, store.ImportUtxos([]models.Utxo{{
			TxId:          bytes.Repeat([]byte{0xd3}, 32),
			OutputIdx:     0,
			AddedSlot:     10,
			Amount:        types.Uint64(7_000_000),
			StakingKey:    credential.Credential.Bytes(),
			CredentialTag: 0,
		}}, nil))
		return store
	}
	check := func(t *testing.T, store *Store) {
		t.Helper()
		key := string(pool.Bytes())
		stakes, delegators, err := store.GetStakeByPoolsAtSlot(
			[][]byte{pool.Bytes()}, readSlot, 0, 0, nil,
		)
		require.NoError(t, err)
		require.Equal(t, uint64(1), delegators[key], "GetStakeByPoolsAtSlot delegators")
		require.Equal(t, uint64(7_000_000), stakes[key], "GetStakeByPoolsAtSlot stake")

		stakes, delegators, err = store.GetEpochBoundaryStakeByPools(
			[][]byte{pool.Bytes()}, readSlot, 0, nil, 0, 0, nil,
		)
		require.NoError(t, err)
		require.Equal(t, uint64(1), delegators[key], "GetEpochBoundaryStakeByPools delegators")
		require.Equal(t, uint64(7_000_000), stakes[key], "GetEpochBoundaryStakeByPools stake")

		pools, err := store.GetEpochBoundaryDelegatedPoolKeyHashes(readSlot, 0, nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{pool.Bytes()}, pools, "GetEpochBoundaryDelegatedPoolKeyHashes")

		inputs, err := store.GetEpochBoundaryRewardStakeInputsForPools(
			[][]byte{pool.Bytes()}, readSlot, 0, nil, 0, 0, nil,
		)
		require.NoError(t, err)
		require.Len(t, inputs, 1, "GetEpochBoundaryRewardStakeInputsForPools")
		require.Equal(t, types.Uint64(7_000_000), inputs[0].Stake)
		require.Equal(t, credential.Credential.Bytes(), inputs[0].StakingKey)
	}

	t.Run("conway_genesis_stake_delegation", func(t *testing.T) {
		t.Parallel()
		store := setup(t)
		cred := credential
		require.NoError(t, store.SetGenesisGovernance(
			nil,
			conway.ConwayGenesisDelegs{
				&cred: conway.ConwayGenesisDelegatee{
					Type:   conway.ConwayGenesisDelegateeTypeStake,
					PoolId: lcommon.PoolId(pool),
				},
			},
			nil,
			nil,
		))
		check(t, store)
	})

	t.Run("conway_genesis_stake_vote_delegation", func(t *testing.T) {
		t.Parallel()
		store := setup(t)
		cred := credential
		require.NoError(t, store.SetGenesisGovernance(
			nil,
			conway.ConwayGenesisDelegs{
				&cred: conway.ConwayGenesisDelegatee{
					Type:   conway.ConwayGenesisDelegateeTypeStakeVote,
					PoolId: lcommon.PoolId(pool),
					DRep: lcommon.Drep{
						Type: lcommon.DrepTypeAbstain,
					},
				},
			},
			nil,
			nil,
		))
		check(t, store)
	})

	// Control: the Shelley-genesis path (account row, no history rows) must
	// keep counting.
	t.Run("shelley_genesis_staking_control", func(t *testing.T) {
		t.Parallel()
		store := setup(t)
		require.NoError(t, store.SetGenesisStaking(
			nil,
			map[string]string{
				hex.EncodeToString(credential.Credential.Bytes()): hex.EncodeToString(pool.Bytes()),
			},
			0,
			nil,
			nil,
		))
		check(t, store)
	})
}
