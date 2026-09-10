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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func TestMithrilImportProvidesPreview1398RewardPParams(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	seedEligiblePreviewGoRewardBasis(t, db)

	currentParams := mithrilRewardConwayPParams()
	previousParams := *currentParams
	previousParams.MinFeeA++
	currentData, err := cbor.Encode(currentParams)
	require.NoError(t, err)
	previousData, err := cbor.Encode(&previousParams)
	require.NoError(t, err)

	eraBounds := make([]ledgerstate.EraBound, ledgerstate.EraConway+1)
	nonce := make([]byte, 32)
	require.NoError(t, ledgerstate.ImportLedgerState(
		context.Background(),
		ledgerstate.ImportConfig{
			Database: db,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			State: &ledgerstate.RawLedgerState{
				PParamsData:         currentData,
				PrevPParamsData:     previousData,
				Epoch:               1397,
				EraIndex:            ledgerstate.EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Reserves:            100_000_000,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      1_397_799,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		},
	))
	require.NoError(t, db.Metadata().SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        1397,
			Reserves:     100_000_000,
			CapturedSlot: 1_397_799,
		},
		nil,
	))
	prefilterSlot, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 1397)
	require.NoError(t, err)
	require.LessOrEqual(t, prefilterSlot, uint64(1_397_799))

	epochs, ok := stakeRewardEpochsForNewEpoch(1398)
	require.True(t, ok)
	require.Equal(t, uint64(1395), epochs.snapshot)
	require.Equal(t, uint64(1396), epochs.performance)
	require.Equal(t, uint64(1397), epochs.pots)

	currentEpoch, err := db.Metadata().GetEpoch(1397, nil)
	require.NoError(t, err)
	require.NotNil(t, currentEpoch)
	ls.currentEpoch = *currentEpoch
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = currentParams

	var rollover *EpochRolloverResult
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		rollover, rolloverErr = ls.processEpochRollover(
			txn,
			*currentEpoch,
			eras.ConwayEraDesc,
			currentParams,
			false,
		)
		return rolloverErr
	}))
	require.NotNil(t, rollover)
	require.Equal(t, uint64(1398), rollover.NewCurrentEpoch.EpochId)
	poolOutputs, err := db.Metadata().GetRewardPoolOutputs(1395, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Positive(t, uint64(poolOutputs[0].TotalReward))
	accountOutputs, err := db.Metadata().GetRewardAccountOutputs(1395, nil)
	require.NoError(t, err)
	require.NotEmpty(t, accountOutputs)
	var credited uint64
	for _, output := range accountOutputs {
		credited += uint64(output.Amount)
	}
	require.Positive(t, credited)
}

func seedEligiblePreviewGoRewardBasis(
	t *testing.T,
	db *database.Database,
) {
	t.Helper()
	const (
		rewardSnapshotEpoch = uint64(1395)
		capturedSlot        = uint64(1_397_799)
		boundarySlot        = uint64(1_395_000)
	)
	poolKey := rewardCalcHash(0x71)
	rewardAccount := rewardCalcHash(0x72)
	member := rewardCalcHash(0x73)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			1_396_640+i,
			nil,
		))
	}
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     capturedSlot,
		BoundarySlot:     boundarySlot,
		ProtocolVersion:  10,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs(
		[]*models.RewardPoolInput{{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               capturedSlot,
			BoundarySlot:               boundarySlot,
		}},
		nil,
	))
	require.NoError(t, meta.SaveRewardStakeInputs(
		[]*models.RewardStakeInput{
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				Stake:         500,
				Owner:         true,
				Registered:    true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				CredentialTag: 0,
				StakingKey:    member,
				Stake:         500,
				Registered:    true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
		},
		nil,
	))

	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(t, db.ImportPool(nil, &pool, &models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKey,
		AddedSlot:   boundarySlot,
	}))
	for _, account := range [][]byte{rewardAccount, member} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: account,
			Pool:       poolKey,
			Active:     true,
		}))
	}
	rewardCalcSeedStakeCert(
		t,
		db,
		1,
		rewardAccount,
		0,
		boundarySlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		2,
		member,
		0,
		boundarySlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
}

func mithrilRewardConwayPParams() *conway.ConwayProtocolParameters {
	params := donationTestConwayPParams(10)
	params.MinFeeA = 44
	params.NOpt = 500
	params.A0 = &cbor.Rat{Rat: big.NewRat(3, 10)}
	params.Rho = &cbor.Rat{Rat: big.NewRat(3, 1000)}
	params.Tau = &cbor.Rat{Rat: big.NewRat(1, 5)}
	return params
}
