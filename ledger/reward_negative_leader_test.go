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
	"math/big"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

const (
	negativeLeaderNewEpoch     = uint64(4)
	negativeLeaderSnapshot     = uint64(1)
	negativeLeaderPerformance  = uint64(2)
	negativeLeaderPotsEpoch    = uint64(3)
	negativeLeaderBoundarySlot = uint64(400)
)

// seedNegativeLeaderRewardRound stores a Dijkstra reward round whose only pool
// has a negative leader reward: maxPledgeLeverage = 0, a0 = 1/2, z0 = 1/10,
// pool stake 1000 and pledge 500 of a 10000 circulation. With R = 1000000,
// sigma' = 0 and p' = 1/20, so maxPool' = floor((2000000/3) * (-1/80)) =
// floor(-8333.33) = -8334 and, at apparent performance 1, the leader reward is
// -8334. The reward account is registered only when accountRegistered is set.
func seedNegativeLeaderRewardRound(
	t *testing.T,
	accountRegistered bool,
) (*LedgerState, *database.Database, []byte) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.activeEras = append(
		append([]eras.EraDesc(nil), eras.Eras...),
		eras.DijkstraEraDesc,
	)
	meta := db.Metadata()

	poolKey := rewardCalcHash(0x11)
	rewardAccount := rewardCalcHash(0x22)
	member := rewardCalcHash(0x33)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	conwayPParams := mockledger.NewMockConwayProtocolParams()
	conwayPParams.NOpt = 10
	conwayPParams.A0 = rewardCalcRat(1, 2)
	conwayPParams.Rho = rewardCalcRat(1, 100)
	conwayPParams.Tau = rewardCalcRat(0, 1)
	conwayPParams.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 12}
	pparamsCbor, err := cbor.Encode(&dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters:  conwayPParams,
		RefScriptCostMultiplier:   rewardCalcRat(1, 1),
		MaxPledgeLeverage:         rewardCalcRat(0, 1),
		LeiosQuorumStakeThreshold: rewardCalcRat(1, 2),
		CommitteeStakeCoverage:    rewardCalcRat(1, 2),
		QuorumStakeThreshold:      rewardCalcRat(1, 2),
	})
	require.NoError(t, err)

	for epoch, startSlot := range map[uint64]uint64{
		negativeLeaderSnapshot:    0,
		negativeLeaderPerformance: 100,
		negativeLeaderPotsEpoch:   200,
	} {
		require.NoError(t, meta.SetEpoch(
			startSlot, epoch, nil, nil, nil, nil,
			eras.DijkstraEraDesc.Id, 1, 100, nil,
		))
		require.NoError(t, db.SetPParams(
			pparamsCbor, startSlot, epoch, eras.DijkstraEraDesc.Id, nil,
		))
	}
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        negativeLeaderPotsEpoch,
		Reserves:     100_000_000,
		Treasury:     1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            negativeLeaderSnapshot,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  12,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      negativeLeaderSnapshot,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         negativeLeaderSnapshot,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    accountRegistered,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         negativeLeaderSnapshot,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(t, db.ImportPool(nil, &pool, &models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKey,
		AddedSlot:   0,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: member,
		Pool:       poolKey,
		Active:     true,
	}))
	rewardCalcSeedStakeCert(
		t, db, 1, member, 0, 250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	if accountRegistered {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: rewardAccount,
			Pool:       poolKey,
			Active:     true,
		}))
		rewardCalcSeedStakeCert(
			t, db, 2, rewardAccount, 0, 250,
			uint(lcommon.CertificateTypeStakeRegistration),
		)
	}
	return ls, db, rewardAccount
}

// With an unregistered reward account the negative leader reward moves the
// pots exactly as applyRUpdFiltered does: deltaR2 = R - (-8334) = 1008334, so
// reserves go from 100000000 to 100000000 - 1000000 + 1008334 = 100008334,
// and frTotalUnregistered = -8334 takes the treasury from 1000000 to 991666.
// Applying the boundary again reaches the same state: the persisted outputs
// cannot carry the negative reward, so reuse falls back to a fresh round.
func TestApplyStakeRewardsNegativeLeaderRewardUnregisteredAccount(t *testing.T) {
	t.Parallel()

	ls, db, _ := seedNegativeLeaderRewardRound(t, false)
	meta := db.Metadata()
	for range 2 {
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.applyStakeRewards(
				txn, negativeLeaderNewEpoch, negativeLeaderBoundarySlot,
			)
		}))
		state, err := meta.GetNetworkState(nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		require.Equal(t, uint64(100_008_334), uint64(state.Reserves))
		require.Equal(t, uint64(991_666), uint64(state.Treasury))
	}
	accountOutputs, err := meta.GetRewardAccountOutputs(
		negativeLeaderSnapshot,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, accountOutputs)
}

// With a registered reward account cardano-ledger fails applying the update
// (compactCoinOrError on the negative Coin), so the boundary stops the node:
// the error halts the pipeline instead of being retried, FatalErrorFunc fires,
// and nothing from the round is written.
func TestApplyStakeRewardsNegativeLeaderRewardRegisteredAccountStops(t *testing.T) {
	t.Parallel()

	ls, db, rewardAccount := seedNegativeLeaderRewardRound(t, true)
	var (
		fatalMu   sync.Mutex
		fatalErrs []error
	)
	ls.config.FatalErrorFunc = func(err error) {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		fatalErrs = append(fatalErrs, err)
	}
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(
			txn, negativeLeaderNewEpoch, negativeLeaderBoundarySlot,
		)
	})
	require.ErrorIs(t, err, rewards.ErrNegativeLeaderReward)
	require.ErrorIs(t, err, errHaltLedgerPipeline)
	require.ErrorContains(t, err, "-8334")
	fatalMu.Lock()
	require.Len(t, fatalErrs, 1)
	require.True(t, errors.Is(fatalErrs[0], rewards.ErrNegativeLeaderReward))
	fatalMu.Unlock()

	meta := db.Metadata()
	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	if state != nil {
		require.NotEqual(t, negativeLeaderBoundarySlot, state.Slot)
	}
	outputs, err := meta.GetRewardPoolOutputs(negativeLeaderSnapshot, nil)
	require.NoError(t, err)
	require.Empty(t, outputs)
	account, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Zero(t, uint64(account.Reward))
}

// The output tables cannot hold a negative leader reward, so the precompute
// leaves such a round to the boundary instead of persisting a partial one.
func TestPrecomputeStakeRewardsSkipsNegativeLeaderReward(t *testing.T) {
	t.Parallel()

	ls, db, _ := seedNegativeLeaderRewardRound(t, false)
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.precomputeStakeRewards(
			txn, negativeLeaderNewEpoch, 300, negativeLeaderBoundarySlot,
		)
	}))
	outputs, err := db.Metadata().GetRewardPoolOutputs(
		negativeLeaderSnapshot,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, outputs)
}

// A bootstrap round pays nothing, so a negative leader reward computed for it
// must not stop the boundary or reach the treasury.
func TestSuppressBootstrapStakeRewardsClearsNegativeLeaderRewards(t *testing.T) {
	t.Parallel()

	result := &rewards.Result{
		AvailableRewards: 1_000,
		NegativeLeaderRewards: []rewards.NegativeLeaderReward{
			{Amount: 10, Spendable: true},
			{Amount: 20},
		},
		UnspendableDeficit: 20,
	}
	suppressBootstrapStakeRewards(result)
	require.NoError(t, result.NegativeLeaderRewardError())
	require.Zero(t, result.UnspendableDeficit)
	require.Equal(t, uint64(1_000), result.Undistributed)
}
