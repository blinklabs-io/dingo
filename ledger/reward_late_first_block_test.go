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
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestLateFirstRewardBlock(t *testing.T) {
	t.Parallel()
	// RUPD waits while slot <= start + window.
	require.False(t, lateFirstRewardBlock(340, 300, 40))
	require.True(t, lateFirstRewardBlock(341, 300, 40))
	require.False(t, lateFirstRewardBlock(500, 300, 0))
}

func TestReplayedStakeRewardFees(t *testing.T) {
	t.Parallel()
	got, err := replayedStakeRewardFees(0, 3_000, 2_400)
	require.NoError(t, err)
	require.Equal(t, uint64(600), got)
	got, err = replayedStakeRewardFees(500, 3_000, 2_400)
	require.NoError(t, err)
	require.Equal(t, uint64(1_100), got)
	_, err = replayedStakeRewardFees(100, 200, 301)
	require.Error(t, err)
}

// Fixture: epoch e spans [100e, 100e+100), k=1 and f=0.1 give a 4k/f
// stability window of 40 slots, so a first block is late above 100e+40.
// Pool: pledge 500 (owner stake 500, member stake 500), cost 1000, margin
// 1/10, a0 = 1/2, nOpt = 10, rho = 1/100, tau = 0, d = 0, circulation
// 100_010_000 - reserves = 10_000, 10 blocks per performance epoch
// (expected blocks 0.1*100 = 10, so eta = 1).
//
// startStep: deltaR1 = floor(1 * 1/100 * 100_000_000) = 1_000_000 for every
// round (reserves of the pots epoch). rPot = fees + deltaR1, deltaT1 = 0,
// R = rPot. In the first round, with sigma = 1000/10000 = z0 and p = 500/10000 = 1/20:
// maxPool = floor(R/(1+a0) * (z0 + p*a0*1)) = floor(R/12), apparent
// performance 1. leader = 1000 + floor((r-1000)*(1/10 + 9/10*1/2)),
// member = floor((r-1000)*9/10*1/2). deltaR = -deltaR1 + (R - leader -
// member).
//
//	round of epochs (snapshot 1, performance 2, pots 3), fee seed 2400:
//	  R = 1_002_400, r = 83_533, leader 46_393, member 37_139,
//	  deltaR = -1_000_000 + 918_868 = -81_132
//	round of epochs (snapshot 2, performance 3, pots 4), fee seed 3000,
//	reserves 99_918_868 so circulation 91_132 and sigma = 1000/91132 < z0:
//	  deltaR1 = floor(99_918_868/100) = 999_188, R = 1_002_188,
//	  maxPool 7_442, leader 4_543, member 2_898,
//	  deltaR = fees - deltaT - leader - member = 3_000 - 7_441 = -4_441
const (
	lateRoundOwnerReward  = uint64(46_393)
	lateRoundMemberReward = uint64(37_139)
)

func newLateFirstBlockTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, completeTestShelleyGenesis(cfg).LoadShelleyGenesisFromReader(strings.NewReader(
		completeTestShelleyGenesisJSON(t, map[string]any{
			"activeSlotsCoeff":  0.1,
			"epochLength":       100,
			"maxLovelaceSupply": 100010000,
			"securityParam":     1,
			"slotLength":        1,
			"systemStart":       "2022-10-25T00:00:00Z",
		}, nil),
	)))
	ls.config.CardanoNodeConfig = cfg
	require.Equal(t, uint64(40), ls.rewardUpdateStabilityWindow())
	return ls, db
}

type lateFirstBlockKeys struct {
	pool, rewardAccount, member []byte
}

func seedLateFirstBlockRound(
	t *testing.T,
	db *database.Database,
	k lateFirstBlockKeys,
	snapshotEpoch, performanceEpoch uint64,
	opCertBase uint64,
) {
	t.Helper()
	meta := db.Metadata()
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], k.pool)
	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, performanceEpoch*100, performanceEpoch,
		eras.ShelleyEraDesc.Id, nil,
	))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			context.Background(), poolID,
			opCertBase+i, performanceEpoch*100+40+i, nil,
		))
	}
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            snapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     snapshotEpoch * 100,
		BoundarySlot:     snapshotEpoch * 100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
		Epoch:                      snapshotEpoch,
		PoolKeyHash:                k.pool,
		RewardAccount:              k.rewardAccount,
		RewardAccountCredentialTag: 0,
		Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
		Pledge:                     500,
		Cost:                       1_000,
		DelegatedStake:             1_000,
		OwnerStake:                 500,
		DelegatorCount:             2,
		CapturedSlot:               snapshotEpoch * 100,
		BoundarySlot:               snapshotEpoch * 100,
	}}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch: snapshotEpoch, PoolKeyHash: k.pool,
			StakingKey: k.rewardAccount, Stake: 500, Owner: true,
			Registered: true, CapturedSlot: snapshotEpoch * 100,
			BoundarySlot: snapshotEpoch * 100,
		},
		{
			Epoch: snapshotEpoch, PoolKeyHash: k.pool,
			StakingKey: k.member, Stake: 500,
			Registered: true, CapturedSlot: snapshotEpoch * 100,
			BoundarySlot: snapshotEpoch * 100,
		},
	}, nil))
}

func seedLateFirstBlockBlocks(t *testing.T, db *database.Database, slots ...uint64) {
	t.Helper()
	for i, slot := range slots {
		hash := make([]byte, 32)
		hash[0] = byte(i + 1)
		hash[1] = byte(slot)
		require.NoError(t, db.BlockCreate(models.Block{
			ID: uint64(i + 1), Slot: slot, Hash: hash,
			Number: uint64(i + 1), Type: 1, Cbor: []byte{0x80},
		}, nil))
	}
}

// setupLateFirstBlockBoundaryFive seeds the rounds for the boundaries into
// epochs 4 and 5, applies the boundary into epoch 4 (an ordinary round), and
// leaves the pots as the boundary into 5 finds them. firstBlockOfEpoch4 is
// the slot of the block that opens epoch 4.
func setupLateFirstBlockBoundaryFive(
	t *testing.T,
	firstBlockOfEpoch4 uint64,
) (*LedgerState, *database.Database, lateFirstBlockKeys) {
	t.Helper()
	ls, db := newLateFirstBlockTestLedger(t)
	meta := db.Metadata()
	k := lateFirstBlockKeys{
		pool:          rewardCalcHash(0x11),
		rewardAccount: rewardCalcHash(0x22),
		member:        rewardCalcHash(0x33),
	}
	for e := uint64(1); e <= 5; e++ {
		require.NoError(t, meta.SetEpoch(
			e*100, e, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, 100, nil,
		))
	}
	seedLateFirstBlockRound(t, db, k, 1, 2, 1)
	seedLateFirstBlockRound(t, db, k, 2, 3, 11)
	pool := models.Pool{PoolKeyHash: k.pool}
	require.NoError(t, db.ImportPool(
		context.Background(), nil, &pool, &models.PoolRegistration{
			PoolID: pool.ID, PoolKeyHash: k.pool, AddedSlot: 0,
		},
	))
	for i, key := range [][]byte{k.rewardAccount, k.member} {
		require.NoError(t, db.CreateAccount(
			context.Background(), nil, &models.Account{
				StakingKey: key, Pool: k.pool, Active: true,
			},
		))
		rewardCalcSeedStakeCert(
			t, db, uint(i+1), key, 0, 250,
			uint(lcommon.CertificateTypeStakeRegistration),
		)
	}
	// Epoch 2 and 3 have blocks; epoch 3 opens early, so the boundary into
	// 4 applies its own round.
	seedLateFirstBlockBlocks(t, db, 250, 310, firstBlockOfEpoch4)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 3, Reserves: 100_000_000, Treasury: 5_000, Fees: 2_400,
		CapturedSlot: 300,
	}, nil))
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 4, 400, false)
	}))
	settleRewardCredits(t, ls)
	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(99_918_868), uint64(state.Reserves))
	require.Equal(t, uint64(5_000), uint64(state.Treasury))
	// The boundary into 4 also moved the treasury by 700 (for example a
	// donation) and fees of epoch 3 were 3000.
	require.NoError(t, meta.SetNetworkState(5_700, 99_918_868, 400, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 4, Reserves: 99_918_868, Treasury: 5_700, Fees: 3_000,
		CapturedSlot: 400,
	}, nil))
	return ls, db, k
}

func accountRewardBalance(
	t *testing.T,
	db *database.Database,
	key []byte,
) uint64 {
	t.Helper()
	acct, err := db.GetAccountByCredential(
		context.Background(), 0, key, false, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, acct)
	return uint64(acct.Reward)
}

func applyBoundaryFive(t *testing.T, ls *LedgerState) {
	t.Helper()
	txn := ls.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 5, 500, false)
	}))
	settleRewardCredits(t, ls)
}

func TestApplyStakeRewardsReplaysRoundAfterLateFirstBlock(t *testing.T) {
	t.Parallel()
	// 450 > 400 + 40: the tick that opens epoch 4 starts the update from the
	// state at the end of epoch 3, the same inputs the boundary into 4 used.
	ls, db, k := setupLateFirstBlockBoundaryFive(t, 450)
	require.Equal(t, lateRoundOwnerReward,
		accountRewardBalance(t, db, k.rewardAccount))
	require.Equal(t, lateRoundMemberReward,
		accountRewardBalance(t, db, k.member))

	applyBoundaryFive(t, ls)

	// Paid a second time, on the same snapshot.
	require.Equal(t, 2*lateRoundOwnerReward,
		accountRewardBalance(t, db, k.rewardAccount))
	require.Equal(t, 2*lateRoundMemberReward,
		accountRewardBalance(t, db, k.member))
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	// Applied to the pots of this boundary: 99_918_868 - 81_132 and
	// 5_700 + 0. The pots row of epoch 3 would give 99_918_868 again.
	require.Equal(t, uint64(99_837_736), uint64(state.Reserves))
	require.Equal(t, uint64(5_700), uint64(state.Treasury))
	pots, err := db.Metadata().GetRewardAdaPots(4, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1_002_400), uint64(pots.Rewards))

	// The boundary into 5 carries 0 + 3_000 - 2_400 into the next round's
	// fee seed, not the ended epoch's own 0.
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ended, err := db.Metadata().GetEpoch(4, txn.Metadata())
		require.NoError(t, err)
		return ls.saveRewardAdaPotsForEpoch(txn, 5, *ended, 500)
	}))
	pots, err = db.Metadata().GetRewardAdaPots(5, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(600), uint64(pots.Fees))
}

func TestApplyStakeRewardsEarlyFirstBlockAppliesOwnRound(t *testing.T) {
	t.Parallel()
	// 430 <= 400 + 40: the update is started later in epoch 4 from the
	// state of epoch 4, so the boundary into 5 applies the round of
	// (snapshot 2, performance 3, pots 4) to the pots of this boundary.
	ls, db, k := setupLateFirstBlockBoundaryFive(t, 430)
	applyBoundaryFive(t, ls)
	require.Equal(t, lateRoundOwnerReward+4_543,
		accountRewardBalance(t, db, k.rewardAccount))
	require.Equal(t, lateRoundMemberReward+2_898,
		accountRewardBalance(t, db, k.member))
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(99_914_427), uint64(state.Reserves))
	require.Equal(t, uint64(5_700), uint64(state.Treasury))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ended, err := db.Metadata().GetEpoch(4, txn.Metadata())
		require.NoError(t, err)
		return ls.saveRewardAdaPotsForEpoch(txn, 5, *ended, 500)
	}))
	pots, err := db.Metadata().GetRewardAdaPots(5, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), uint64(pots.Fees))
}

func TestReplaysPreviousStakeRewardRoundNeedsContiguousEpochs(t *testing.T) {
	t.Parallel()
	ls, db := newLateFirstBlockTestLedger(t)
	meta := db.Metadata()
	for e := uint64(1); e <= 5; e++ {
		require.NoError(t, meta.SetEpoch(
			e*100, e, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, 100, nil,
		))
	}
	// Epoch 4 opens late but epoch 3 has no block, so cardano-ledger ran no
	// NEWEPOCH at that block.
	seedLateFirstBlockBlocks(t, db, 250, 450)
	txn := db.Transaction(context.Background(), false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		replay, err := ls.replaysPreviousStakeRewardRound(txn, 5)
		require.NoError(t, err)
		require.False(t, replay)
		return nil
	}))
}

func TestReplaysPreviousStakeRewardRoundReadsBlocksInTransaction(
	t *testing.T,
) {
	t.Parallel()
	ls, db := newLateFirstBlockTestLedger(t)
	meta := db.Metadata()
	for e := uint64(1); e <= 5; e++ {
		require.NoError(t, meta.SetEpoch(
			e*100, e, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, 100, nil,
		))
	}
	// The read transaction opens before the blocks exist and must not see
	// them, even though a fresh transaction would.
	rtxn := db.Transaction(context.Background(), false)
	require.NoError(t, rtxn.Do(func(txn *database.Txn) error {
		require.NotNil(t, txn.Blob())
		seedLateFirstBlockBlocks(t, db, 350, 450)
		replay, err := ls.replaysPreviousStakeRewardRound(txn, 5)
		require.NoError(t, err)
		require.False(t, replay)
		return nil
	}))
	ftxn := db.Transaction(context.Background(), false)
	require.NoError(t, ftxn.Do(func(txn *database.Txn) error {
		replay, err := ls.replaysPreviousStakeRewardRound(txn, 5)
		require.NoError(t, err)
		require.True(t, replay)
		return nil
	}))
}

// On a network that starts in Byron, the late first block of the epoch after
// the first Shelley epoch replays the first Shelley round, whose performance
// epoch is Byron. The replay must not look for Byron pparams or a stake
// snapshot, and applies the same reserve expansion to the pots of its own
// boundary.
func TestApplyStakeRewardsReplaysFirstShelleyRoundAfterLateFirstBlock(
	t *testing.T,
) {
	t.Parallel()
	ls, db := newLateFirstBlockTestLedger(t)
	meta := db.Metadata()
	const byronEpochs = uint64(4)
	for e := range byronEpochs {
		require.NoError(t, meta.SetEpoch(
			e*100, e, nil, nil, nil, nil,
			eras.ByronEraDesc.Id, 1, 100, nil,
		))
	}
	for e := byronEpochs; e <= byronEpochs+2; e++ {
		require.NoError(t, meta.SetEpoch(
			e*100, e, nil, nil, nil, nil,
			eras.ShelleyEraDesc.Id, 1, 100, nil,
		))
	}
	pparamsCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(1, 1),
		ProtocolMajor:    2,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, byronEpochs*100, byronEpochs,
		eras.ShelleyEraDesc.Id, nil,
	))
	// The first Shelley epoch has a block; the next opens at 550, after
	// 500 + 40.
	seedLateFirstBlockBlocks(t, db, 410, 550)
	const reserves = uint64(100_000_000)
	require.NoError(t, meta.SetNetworkState(0, reserves, 400, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: byronEpochs, Reserves: types.Uint64(reserves),
		CapturedSlot: 400,
	}, nil))

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 5, 500, false)
	}))
	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	reserveDelta := reserves - uint64(state.Reserves)
	treasuryDelta := uint64(state.Treasury)
	require.NotZero(t, reserveDelta)
	require.Equal(t, reserveDelta, treasuryDelta)

	// The boundary into 5 leaves different pots than the ones the first
	// round was built from.
	const reserves5 = uint64(99_000_000)
	const treasury5 = uint64(7_000)
	require.NoError(t, meta.SetNetworkState(treasury5, reserves5, 500, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch: 5, Reserves: types.Uint64(reserves5),
		Treasury: types.Uint64(treasury5), Fees: 3_000, CapturedSlot: 500,
	}, nil))

	replay := false
	rtxn := db.Transaction(context.Background(), false)
	require.NoError(t, rtxn.Do(func(txn *database.Txn) error {
		var err error
		replay, err = ls.replaysPreviousStakeRewardRound(txn, 6)
		return err
	}))
	require.True(t, replay)

	txn = db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(context.Background(), txn, 6, 600, false)
	}))
	state, err = meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, reserves5-reserveDelta, uint64(state.Reserves))
	require.Equal(t, treasury5+treasuryDelta, uint64(state.Treasury))

	// The boundary into 6 carries the fees of 5, less the replayed seed.
	txn = db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ended, err := meta.GetEpoch(5, txn.Metadata())
		require.NoError(t, err)
		return ls.saveRewardAdaPotsForEpoch(txn, 6, *ended, 600)
	}))
	pots, err := meta.GetRewardAdaPots(6, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000), uint64(pots.Fees))
}
