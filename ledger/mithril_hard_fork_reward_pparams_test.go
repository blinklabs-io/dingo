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
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// hardForkRewardRound is the observable result of the Go reward round that a
// Mithril import at epoch 1397 hands to the rollover into 1398.
type hardForkRewardRound struct {
	goParams  rewardParamsView
	setParams rewardParamsView
	pools     []models.RewardPoolOutput
	accounts  []models.RewardAccountOutput
	pots      models.RewardAdaPots
}

type rewardParamsView struct {
	rho, tau, a0, d string
	nOpt, major     uint64
}

// TestMithrilImportAfterHardForkUsesSnapshotPrevPParams bootstraps from a
// Conway snapshot positioned in the first, second and third Conway epoch.
//
// cardano-ledger's startStep binds every protocol-parameter input of the
// reward update to `es ^. prevPParamsEpochStateL`, and the hard-fork
// translation (translateGovState) carries prevPParams into the new era with
// the reward fields unchanged. So the Go round the rollover into 1398 computes
// must use the snapshot's prevPParams whatever era epoch 1396 belongs to, and
// must equal the same round imported with no hard fork in the window.
func TestMithrilImportAfterHardForkUsesSnapshotPrevPParams(t *testing.T) {
	t.Parallel()

	current := mithrilRewardConwayPParams()
	previous := *current
	previous.ProtocolVersion.Major = 8
	previous.NOpt = 400
	previous.A0 = &cbor.Rat{Rat: big.NewRat(1, 5)}
	previous.Rho = &cbor.Rat{Rat: big.NewRat(1, 250)}
	previous.Tau = &cbor.Rat{Rat: big.NewRat(3, 20)}
	previous.MinFeeA++

	control := runHardForkRewardRound(t, current, &previous, nil)
	require.Equal(t, rewardParamsView{
		rho: "1/250", tau: "3/20", a0: "1/5", d: "0",
		nOpt: 400, major: 8,
	}, control.goParams,
		"the Go round must read the snapshot prevPParams (startStep's pr)")
	require.Equal(t, rewardParamsView{
		rho: "3/1000", tau: "1/5", a0: "3/10", d: "0",
		nOpt: 500, major: 10,
	}, control.setParams,
		"the Set round must read the snapshot curPParams")
	// startStep with pr = the snapshot prevPParams: eta is the pool's 10
	// blocks over floor(0.1 * 1000) expected, 1/10; deltaR1 =
	// floor(1/10 * 1/250 * 100_000_000) = 40_000; deltaT1 =
	// floor(3/20 * 40_000) = 6_000; R = 34_000. The pool's stake and pledge
	// shares both cap at z0 = 1/400, so maxPool = floor(34_000 / (6/5) *
	// 3/1000) = 85, all of it the leader's since the pool cost exceeds it.
	// Reading curPParams instead gives 48.
	require.Len(t, control.pools, 1)
	require.Equal(t, uint64(85), uint64(control.pools[0].TotalReward))
	require.Len(t, control.accounts, 1)
	require.Equal(t, uint64(85), uint64(control.accounts[0].Amount))
	require.Equal(t, uint64(6_000), uint64(control.pots.Treasury))
	require.Equal(t,
		uint64(100_000_000-40_000+(34_000-85)),
		uint64(control.pots.Reserves))

	for _, tc := range []struct {
		name        string
		conwayEpoch uint64
	}{
		{name: "first Conway epoch", conwayEpoch: 1397},
		{name: "second Conway epoch", conwayEpoch: 1396},
		{name: "third Conway epoch", conwayEpoch: 1395},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := runHardForkRewardRound(
				t, current, &previous, &tc.conwayEpoch,
			)
			require.Equal(t, control, got)
		})
	}
}

func runHardForkRewardRound(
	t *testing.T,
	currentParams *conway.ConwayProtocolParameters,
	previousParams *conway.ConwayProtocolParameters,
	conwayEpoch *uint64,
) hardForkRewardRound {
	t.Helper()
	const (
		snapshotEpoch = uint64(1397)
		epochLength   = uint64(1_000)
		tipSlot       = uint64(1_397_799)
	)
	ls, db := newRewardCalculationTestLedger(t)
	seedEligiblePreviewGoRewardBasis(t, db)

	currentData, err := cbor.Encode(currentParams)
	require.NoError(t, err)
	previousData, err := cbor.Encode(previousParams)
	require.NoError(t, err)

	eraBounds := make([]ledgerstate.EraBound, ledgerstate.EraConway+1)
	if conwayEpoch != nil {
		eraBounds[ledgerstate.EraConway] = ledgerstate.EraBound{
			Slot:  *conwayEpoch * epochLength,
			Epoch: *conwayEpoch,
		}
	}
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
				Epoch:               snapshotEpoch,
				EraIndex:            ledgerstate.EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Reserves:            100_000_000,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      tipSlot,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, uint(epochLength), nil
			},
		},
	))
	require.NoError(t, db.Metadata().SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        snapshotEpoch,
			Reserves:     100_000_000,
			CapturedSlot: tipSlot,
		},
		nil,
	))

	var round hardForkRewardRound
	pots := &models.RewardAdaPots{Reserves: 100_000_000}
	round.goParams = rewardParamsFor(
		t, ls, db, snapshotEpoch-1, snapshotEpoch, pots,
	)

	currentEpoch, err := db.Metadata().GetEpoch(snapshotEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, currentEpoch)
	ls.currentEpoch = *currentEpoch
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = currentParams

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		rollover, rolloverErr := ls.processEpochRollover(
			txn,
			*currentEpoch,
			eras.ConwayEraDesc,
			currentParams,
			false,
		)
		if rolloverErr == nil {
			require.NotNil(t, rollover)
			require.Equal(
				t, snapshotEpoch+1, rollover.NewCurrentEpoch.EpochId,
			)
		}
		return rolloverErr
	}))
	round.setParams = rewardParamsFor(
		t, ls, db, snapshotEpoch, snapshotEpoch+1, pots,
	)

	pools, err := db.Metadata().GetRewardPoolOutputs(snapshotEpoch-2, nil)
	require.NoError(t, err)
	for _, pool := range pools {
		row := *pool
		row.ID = 0
		round.pools = append(round.pools, row)
	}
	accounts, err := db.Metadata().GetRewardAccountOutputs(
		snapshotEpoch-2, nil,
	)
	require.NoError(t, err)
	for _, account := range accounts {
		row := *account
		row.ID = 0
		round.accounts = append(round.accounts, row)
	}
	nextPots, err := db.Metadata().GetRewardAdaPots(snapshotEpoch+1, nil)
	require.NoError(t, err)
	require.NotNil(t, nextPots)
	round.pots = *nextPots
	round.pots.ID = 0
	return round
}

func rewardParamsFor(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	performanceEpoch uint64,
	calculationEpoch uint64,
	pots *models.RewardAdaPots,
) rewardParamsView {
	t.Helper()
	var view rewardParamsView
	txn := db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, params, _, err := ls.rewardParameters(
			txn, performanceEpoch, calculationEpoch, pots,
		)
		if err != nil {
			return err
		}
		view = rewardParamsView{
			rho:   params.MonetaryExpansion.RatString(),
			tau:   params.TreasuryExpansion.RatString(),
			a0:    params.PledgeInfluence.RatString(),
			d:     params.Decentralization.RatString(),
			nOpt:  params.OptimalPoolCount,
			major: params.ProtocolMajorVersion,
		}
		return nil
	}))
	return view
}
