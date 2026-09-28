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
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// The first RUPD reads an empty nesBprev, not epoch 0's nesBcur.
// With d=0 this gives eta=0; the same 180 blocks enter the next update,
// giving eta=180/(500*0.4)=0.9. Fees collected in epoch 0 enter that update
// too. These are the reference devnet inputs and pots from issue #4502.
func TestApplyStakeRewardsConwayGenesisPerformance(t *testing.T) {
	t.Parallel()
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.ConwayEraDesc
	require.NoError(t, ls.config.CardanoNodeConfig.
		LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.4,
		"epochLength": 500,
		"maxLovelaceSupply": 6000000000000,
		"securityParam": 40,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	pp := mockledger.NewMockConwayProtocolParams()
	pp.NOpt = 150
	pp.A0 = rewardCalcRat(3, 10)
	pp.Rho = rewardCalcRat(3, 1_000)
	pp.Tau = rewardCalcRat(1, 5)
	pp.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 10}
	encoded, err := cbor.Encode(&pp)
	require.NoError(t, err)
	meta := db.Metadata()
	for epoch := range uint64(3) {
		require.NoError(t, meta.SetEpoch(
			epoch*500, epoch, nil, nil, nil, nil,
			eras.ConwayEraDesc.Id, 1, 500, nil,
		))
		require.NoError(t, db.SetPParams(
			encoded, epoch*500, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(t, meta.SetNetworkState(0, 2_000_000_000_000, 0, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 0, Reserves: 2_000_000_000_000,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch: 0, SnapshotType: "mark", ProtocolVersion: 10,
		TotalActiveStake: 2_000_000_000_000,
		TotalPoolCount:   2, TotalDelegators: 2,
	}, nil))
	for _, key := range []byte{0x11, 0x22} {
		poolKey := rewardCalcHash(key)
		poolID := seedLiveStakeFixture(
			t, db, poolKey, bytes.Repeat([]byte{key}, 32),
			1_000_000_000_000, 0,
		)
		require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
			Epoch: 0, PoolKeyHash: poolKey, RewardAccount: poolKey,
			Margin:         &types.Rat{Rat: big.NewRat(0, 1)},
			DelegatedStake: 1_000_000_000_000, DelegatorCount: 1,
		}}, nil))
		require.NoError(
			t,
			meta.SaveRewardStakeInputs([]*models.RewardStakeInput{{
				Epoch: 0, PoolKeyHash: poolKey, StakingKey: poolKey,
				Stake: 1_000_000_000_000, Registered: true,
			}}, nil),
		)
		for i := range uint64(90) {
			require.NoError(t, db.UpdatePoolOpCertSequence(
				poolID, i+1, 1+2*i+uint64(key), nil,
			))
		}
	}
	_, err = rewardCalcSQLDB(t, db).Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (1, ?, ?, 60, 7, '400000', '0', '0', 0, TRUE)`,
		[]byte("genesis-performance-tx"), []byte("genesis-performance-block"))
	require.NoError(t, err)

	for _, tc := range []struct {
		epoch    uint64
		treasury uint64
		reserves uint64
		fraction *big.Rat
	}{
		{1, 0, 2_000_000_000_000, big.NewRat(1, 4)},
		{2, 1_080_080_000, 1_998_920_320_000, big.NewRat(1_562_500, 6_251_687)},
	} {
		boundary := tc.epoch * 500
		ended, err := meta.GetEpoch(tc.epoch-1, nil)
		require.NoError(t, err)
		require.NotNil(t, ended)
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			if err := ls.applyStakeRewards(txn, tc.epoch, boundary); err != nil {
				return err
			}
			return ls.saveRewardAdaPotsForEpoch(txn, tc.epoch, *ended, boundary)
		}))
		state, err := meta.GetNetworkState(nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		require.Equal(t, tc.treasury, uint64(state.Treasury),
			"treasury at boundary into epoch %d", tc.epoch)
		require.Equal(t, tc.reserves, uint64(state.Reserves),
			"reserves at boundary into epoch %d", tc.epoch)
		pots, err := meta.GetRewardAdaPots(tc.epoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, state.Treasury, pots.Treasury)
		require.Equal(t, state.Reserves, pots.Reserves)
		if tc.epoch == 1 {
			require.Equal(t, uint64(400_000), uint64(pots.Fees))
		}

		hash := bytes.Repeat([]byte{byte(tc.epoch)}, 32)
		seedBlockAtSlot(t, ls, boundary, hash)
		require.NoError(t, db.SetTip(ochainsync.Tip{
			Point: ocommon.NewPoint(boundary, hash),
		}, nil))
		result, err := ls.Query(stakeDistributionQuery(), QueryPoint{})
		require.NoError(t, err)
		dist := decodeStakeDistributionResult(t, result)
		require.Len(t, dist.Results, 2)
		for _, entry := range dist.Results {
			require.Equal(t, tc.fraction, entry.StakeFraction.Rat,
				"stake fraction at boundary into epoch %d", tc.epoch)
		}
	}
}
