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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRollbackRequeuesRewardPrecompute(t *testing.T) {
	t.Parallel()

	for _, crossEpoch := range []bool{false, true} {
		name := "same epoch"
		if crossEpoch {
			name = "previous epoch"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			nonce := testHashBytes("reward-epoch")
			epochLength := uint(100)
			if crossEpoch {
				epochLength = 15
			}
			require.NoError(t, ls.db.SetEpoch(
				0, 3, nonce, nil, nil, nil,
				eras.ShelleyEraDesc.Id, 1000, epochLength, nil,
			))
			pp, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 7,
			})
			require.NoError(t, err)
			require.NoError(t, ls.db.SetPParams(
				pp, 0, 3, eras.ShelleyEraDesc.Id, nil,
			))
			epochID := uint64(3)
			if crossEpoch {
				epochID = 4
				require.NoError(t, ls.db.SetEpoch(
					15, epochID, testHashBytes("rolled-away-epoch"),
					nil, nil, nil, eras.ShelleyEraDesc.Id, 1000, 15, nil,
				))
			}
			epoch, err := ls.db.Metadata().GetEpoch(epochID, nil)
			require.NoError(t, err)
			ls.currentEpoch = *epoch
			ls.currentEra = eras.ShelleyEraDesc
			// Keep the worker occupied so the replacement event remains
			// observable after the real rollback path returns.
			ls.rewardPrecomputeRunning = true
			ls.rewardPrecomputePending = &event.EpochTransitionEvent{
				NewEpoch: epochID + 1,
			}
			ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
				epochEvent: event.EpochTransitionEvent{NewEpoch: epochID + 1},
				cutoffSlot: 100,
			}

			require.NoError(t, ls.rollbackWithBlocks(
				fixture.ancestorTip.Point, nil, false,
			))

			require.Zero(t, ls.rewardInputRollbackActive.Load())
			require.Equal(t, uint64(2), ls.rewardInputGeneration.Load())
			ls.rewardPrecomputeMu.Lock()
			defer ls.rewardPrecomputeMu.Unlock()
			pending := ls.rewardPrecomputePending
			require.NotNil(t, pending, "rollback must replace invalidated work")
			require.Equal(t, uint64(3), pending.NewEpoch,
				"replacement must calculate the surviving epoch's rewards")
			require.Equal(
				t,
				fixture.ancestorTip.Point.Slot,
				pending.BoundarySlot,
				"capture must use the surviving applied tip",
			)
			require.Equal(t, nonce, pending.EpochNonce)
			require.Nil(t, ls.rewardPrecomputeRetry,
				"a rolled-away prefilter retry must not replace fresh work")
		})
	}
}

func TestRollbackTransactionFailureRestoresRewardPrecompute(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, ls.db.SetEpoch(
		0, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1000, 100, nil,
	))
	epoch, err := ls.db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	ls.rewardPrecomputeRunning = true
	queued := &event.EpochTransitionEvent{
		NewEpoch:     4,
		BoundarySlot: 300,
		EpochNonce:   nonce,
	}
	ls.rewardPrecomputePending = queued
	retry := &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{NewEpoch: 4},
		cutoffSlot: 300,
		generation: ls.rewardInputGeneration.Load(),
	}
	ls.rewardPrecomputeRetry = retry
	transactionErr := errors.New("injected rollback transaction failure")
	failLedgerRollbackAfterChainTruncation(t, ls, transactionErr)

	err = ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, transactionErr)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	require.NotNil(t, ls.rewardPrecomputePending,
		"a failed rollback must restore the queued transition")
	require.Equal(t, queued.NewEpoch, ls.rewardPrecomputePending.NewEpoch)
	require.Equal(t, queued.BoundarySlot, ls.rewardPrecomputePending.BoundarySlot)
	require.Equal(t, queued.EpochNonce, ls.rewardPrecomputePending.EpochNonce)
	require.NotNil(t, ls.rewardPrecomputeRetry,
		"a failed rollback must restore the deferred prefilter retry")
	require.Equal(t, retry.cutoffSlot, ls.rewardPrecomputeRetry.cutoffSlot)
	require.Equal(t, retry.epochEvent.NewEpoch,
		ls.rewardPrecomputeRetry.epochEvent.NewEpoch)
	require.Equal(t, ls.rewardInputGeneration.Load(),
		ls.rewardPrecomputeRetry.generation,
		"the restored retry must use the new stable generation")
}

func TestRollbackRewardPrecomputePersistsReusableOutputs(t *testing.T) {
	t.Parallel()

	for _, protocolMajor := range []uint{6, 7} {
		t.Run(fmt.Sprintf("protocol %d", protocolMajor), func(t *testing.T) {
			t.Parallel()
			seed, db := seedRewardPrecomputeTimingState(t, protocolMajor)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			require.NoError(
				t,
				cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
			)
			nonce := testHashBytes("reward-epoch")
			require.NoError(t, db.SetEpoch(
				200, 3, nonce, nil, nil, nil,
				eras.ShelleyEraDesc.Id, 1, 1_000, nil,
			))
			cfg := seed.config
			cfg.Database = db
			cfg.ChainManager = cm
			ls, err := NewLedgerState(cfg)
			require.NoError(t, err)
			ls.metrics.init(prometheus.NewRegistry())
			t.Cleanup(func() { require.NoError(t, ls.Close()) })
			cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
			require.NoError(t, err)
			ancestor := chain.RawBlock{
				Slot: cutoff + 1, Hash: testHashBytes("reward-ancestor"),
				BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
			}
			current := chain.RawBlock{
				Slot: cutoff + 2, Hash: testHashBytes("reward-current"),
				PrevHash:    ancestor.Hash,
				BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
			}
			require.NoError(t, cm.PrimaryChain().AddRawBlocks(
				[]chain.RawBlock{ancestor, current},
			))
			for _, block := range []chain.RawBlock{ancestor, current} {
				require.NoError(t, db.SetBlockNonce(
					block.Hash, block.Slot, nonce, true, nil,
				))
			}
			ls.currentTip = ochainsync.Tip{
				Point:       ocommon.NewPoint(current.Slot, current.Hash),
				BlockNumber: current.BlockNumber,
			}
			require.NoError(t, db.SetTip(ls.currentTip, nil))

			require.NoError(t, ls.rollbackWithBlocks(
				ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
			))
			ls.rewardPrecomputeWG.Wait()

			outputs, err := db.Metadata().GetRewardPoolOutputs(1, nil)
			require.NoError(t, err)
			require.Len(t, outputs, 1,
				"rollback must replace discarded work before the next boundary")
			require.Equal(t, ancestor.Slot, outputs[0].CapturedSlot)
			require.Equal(t, uint64(1_200), outputs[0].BoundarySlot)
			txn := db.Transaction(false)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				app, ok, err := ls.precomputedStakeRewardApplication(
					txn,
					4,
					1_200,
				)
				require.NoError(t, err)
				require.True(t, ok, "next boundary must reuse the replacement")
				require.NotNil(t, app)
				return nil
			}))
		})
	}
}

func TestRewardPrecomputeRetryRejectsAbandonedGeneration(t *testing.T) {
	t.Parallel()

	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback active %t", active), func(t *testing.T) {
			t.Parallel()
			ls := &LedgerState{rewardPrecomputeRunning: true}
			if active {
				ls.rewardInputRollbackActive.Add(1)
			} else {
				ls.rewardInputGeneration.Add(2)
			}
			ls.deferStakeRewardPrecompute(4, 100, 0)
			require.Nil(t, ls.rewardPrecomputeRetry,
				"an old calculation must not reinstall an abandoned retry")

			ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
				epochEvent: event.EpochTransitionEvent{NewEpoch: 3},
				cutoffSlot: 100,
			}
			ls.maybeQueueStakeRewardPrecomputeRetry(100)
			require.Nil(t, ls.rewardPrecomputePending,
				"an abandoned retry must not replace the current pending epoch")
			require.Nil(t, ls.rewardPrecomputeRetry)
		})
	}
}

func TestRollbackDoesNotRestartRewardsWithoutRestoredState(t *testing.T) {
	t.Parallel()

	for _, noop := range []bool{false, true} {
		t.Run(fmt.Sprintf("no-op %t", noop), func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			ls := fixture.ls
			// An unknown surviving era forces the post-commit state reload
			// to fail; a no-op rollback must not reach that reload at all.
			require.NoError(t, ls.db.SetEpoch(
				0, 3, testHashBytes("unknown-era"), nil, nil, nil,
				255, 1000, 100, nil,
			))
			ls.rewardPrecomputeRunning = true
			pending := &event.EpochTransitionEvent{NewEpoch: 3}
			ls.rewardPrecomputePending = pending
			point := fixture.ancestorTip.Point
			if noop {
				point = fixture.currentTip.Point
			}

			err := ls.rollbackWithBlocks(point, nil, false)
			if noop {
				require.NoError(t, err)
				require.Same(t, pending, ls.rewardPrecomputePending)
				require.Zero(t, ls.rewardInputGeneration.Load())
			} else {
				require.ErrorContains(t, err, "unknown era ID 255")
				require.Nil(t, ls.rewardPrecomputePending,
					"failed reload must not schedule rewards against stale state")
				require.Equal(t, uint64(2), ls.rewardInputGeneration.Load())
			}
			require.Zero(t, ls.rewardInputRollbackActive.Load())
		})
	}
}
