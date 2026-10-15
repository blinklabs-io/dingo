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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// On a network that starts in Byron, the update built during the first
// Shelley epoch is applied at the next boundary with empty block counts. With
// d >= 0.8 eta is 1, so reserves pay out floor(rho * reserves) and the
// treasury takes floor(tau * that); nothing is distributed to pools.
func TestApplyStakeRewardsAppliesFirstShelleyRoundOnByronStart(t *testing.T) {
	t.Parallel()

	const (
		byronEpochs  = 4
		firstShelley = uint64(byronEpochs)
		reserves     = uint64(100_000_000)
		epochLength  = 21_600
		// floor(1/5 * floor(3/1000 * reserves))
		treasuryShare = uint64(60_000)
	)

	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	for epoch := range uint64(byronEpochs) {
		require.NoError(t, meta.SetEpoch(
			epoch*epochLength, epoch, nil, nil, nil, nil,
			eras.ByronEraDesc.Id, 1, epochLength, nil,
		))
	}
	require.NoError(t, meta.SetEpoch(
		firstShelley*epochLength, firstShelley, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, epochLength, nil,
	))
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(3, 1000),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(1, 1),
		ProtocolMajor:    2,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, firstShelley*epochLength, firstShelley,
		eras.ShelleyEraDesc.Id, nil,
	))
	require.NoError(t, meta.SetNetworkState(
		0, reserves, firstShelley*epochLength, nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        firstShelley,
		Reserves:     types.Uint64(reserves),
		CapturedSlot: firstShelley * epochLength,
	}, nil))
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			context.Background(), txn, firstShelley+1,
			(firstShelley+1)*epochLength,
		)
	}))
	settleRewardCredits(t, ls)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, treasuryShare, uint64(state.Treasury))
	require.Equal(t, reserves-treasuryShare, uint64(state.Reserves))
}
