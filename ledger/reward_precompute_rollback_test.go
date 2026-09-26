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
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	require.Equal(
		t,
		queued.BoundarySlot,
		ls.rewardPrecomputePending.BoundarySlot,
	)
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

func TestCommittedRollbackWithFloorFailureRequeuesRewardPrecompute(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, ls.db.SetEpoch(
		0, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1000, 100, nil,
	))
	pp, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 7,
	})
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		pp, 0, 3, eras.ShelleyEraDesc.Id, nil,
	))
	epoch, err := ls.db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	// Keep the worker occupied so the replacement stays observable.
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputePending = &event.EpochTransitionEvent{NewEpoch: 4}
	floorErr := errors.New("injected durable floor lookup failure")
	base := ls.db
	failing, err := database.New(
		base.Config(),
		database.Stores{
			Blob: base.Blob(),
			Metadata: floorLookupFailingMetadataStore{
				MetadataStore: base.Metadata(),
				err:           floorErr,
			},
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, failing.Close()) })
	ls.db = failing

	err = ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, floorErr)
	_, committed := errors.AsType[*rollbackCommittedError](err)
	require.True(
		t,
		committed,
		"the truncation committed before the floor check",
	)
	require.Equal(t, fixture.ancestorTip, ls.currentTip)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	pending := ls.rewardPrecomputePending
	require.NotNil(t, pending,
		"a committed rollback must replace the work it invalidated")
	require.Equal(t, uint64(3), pending.NewEpoch)
	require.Equal(t, fixture.ancestorTip.Point.Slot, pending.BoundarySlot)
	require.Equal(t, nonce, pending.EpochNonce)
}

func TestRollbackFailureDuringCloseDoesNotRestoreRewardPrecompute(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputePending = &event.EpochTransitionEvent{
		NewEpoch:   4,
		EpochNonce: testHashBytes("reward-epoch"),
	}
	ls.rewardPrecomputeRetry = &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{NewEpoch: 4},
		cutoffSlot: 300,
	}
	injected := errors.New("injected rollback failure during close")
	ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		// Close marks the ledger closed and discards queued precompute
		// work while this transaction is still open.
		ls.closed.Store(true)
		ls.rewardPrecomputeMu.Lock()
		ls.rewardPrecomputePending = nil
		ls.rewardPrecomputeRetry = nil
		ls.rewardPrecomputeMu.Unlock()
		return ochainsync.Tip{}, nil, injected
	}

	err := ls.rollbackWithBlocks(fixture.ancestorTip.Point, nil, false)

	require.ErrorIs(t, err, injected)
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	require.Nil(t, ls.rewardPrecomputePending,
		"a closed ledger must not re-arm the transition Close discarded")
	require.Nil(t, ls.rewardPrecomputeRetry,
		"a closed ledger must not re-arm the retry Close discarded")
}

// The pre-Babbage prefilter reads account registration at the RUPD slot, so a
// rollback across that slot can change which delegators are paid. The
// replacement must be derived from the surviving certificate history and match
// the authoritative boundary calculation exactly.
func TestRollbackRewardPrecomputeDropsAbandonedPrefilterHistory(
	t *testing.T,
) {
	t.Parallel()

	seed, db := seedRewardPrecomputeTimingState(t, 6)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
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
	epoch, err := db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
	require.NoError(t, err)
	member := rewardCalcHash(0x6a)
	// member is registered before the epoch; the abandoned chain deregisters
	// it after the rollback point and before the RUPD slot.
	rewardCalcSeedStakeCert(
		t, db, 21, member, 0, 150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t, db, 22, member, 0, cutoff-5,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	ancestor := chain.RawBlock{
		Slot: cutoff - 10, Hash: testHashBytes("prefilter-ancestor"),
		BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
	}
	abandoned := chain.RawBlock{
		Slot: cutoff + 1, Hash: testHashBytes("prefilter-abandoned"),
		PrevHash:    ancestor.Hash,
		BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		[]chain.RawBlock{ancestor, abandoned},
	))
	for _, block := range []chain.RawBlock{ancestor, abandoned} {
		require.NoError(t, db.SetBlockNonce(
			block.Hash, block.Slot, nonce, true, nil,
		))
	}
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(abandoned.Slot, abandoned.Hash),
		BlockNumber: abandoned.BlockNumber,
	}
	require.NoError(t, db.SetTip(ls.currentTip, nil))

	require.NoError(t, ls.precomputeStakeRewardsAfterEpochTransition(
		event.EpochTransitionEvent{
			NewEpoch:     3,
			BoundarySlot: abandoned.Slot,
			EpochNonce:   nonce,
		},
	))
	require.False(t, rewardOutputsPayKey(t, db, member),
		"control: the abandoned chain's prefilter excludes member")

	require.NoError(t, ls.rollbackWithBlocks(
		ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
	))
	ls.rewardPrecomputeWG.Wait()

	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, outputs,
		"no output computed from the abandoned history may survive")
	ls.rewardPrecomputeMu.Lock()
	retry := ls.rewardPrecomputeRetry
	ls.rewardPrecomputeMu.Unlock()
	require.NotNil(t, retry,
		"the replacement must wait for the RUPD slot on the surviving chain")
	require.Equal(t, uint64(3), retry.epochEvent.NewEpoch)
	require.Equal(t, cutoff, retry.cutoffSlot)

	replacement := ocommon.NewPoint(
		cutoff+1, testHashBytes("prefilter-replacement"),
	)
	ls.Lock()
	ls.currentTip = ochainsync.Tip{Point: replacement, BlockNumber: 2}
	ls.Unlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(replacement.Slot)
	ls.rewardPrecomputeWG.Wait()

	require.True(t, rewardOutputsPayKey(t, db, member),
		"the replacement must use the surviving registration history")
	txn := db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		want, ok, err := ls.calculateStakeRewardApplication(
			txn, 4, replacement.Slot, 1_200, false,
		)
		require.NoError(t, err)
		require.True(t, ok)
		poolOutputs, err := db.Metadata().GetRewardPoolOutputs(
			1, txn.Metadata(),
		)
		require.NoError(t, err)
		accountOutputs, err := db.Metadata().GetRewardAccountOutputs(
			1, txn.Metadata(),
		)
		require.NoError(t, err)
		require.Equal(t,
			rewardPoolOutputAmounts(want.poolOutputs),
			rewardPoolOutputAmounts(poolOutputs),
		)
		require.Equal(t,
			rewardAccountOutputAmounts(want.accountOutputs),
			rewardAccountOutputAmounts(accountOutputs),
		)
		pots, err := db.Metadata().GetRewardAdaPots(3, txn.Metadata())
		require.NoError(t, err)
		require.Equal(t, want.totalRewardPot, uint64(pots.Rewards))
		_, reusable, err := ls.precomputedStakeRewardApplication(
			txn, 4, 1_200,
		)
		require.NoError(t, err)
		require.True(
			t,
			reusable,
			"the next boundary must reuse the replacement",
		)
		return nil
	}))
	account, err := db.GetAccountByCredential(0, member, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Zero(t, uint64(account.Reward),
		"precomputation must not credit rewards before the boundary")
}

func rewardOutputsPayKey(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
) bool {
	t.Helper()
	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, outputs)
	for _, output := range outputs {
		if bytes.Equal(output.StakingKey, stakingKey) && output.Amount > 0 {
			return true
		}
	}
	return false
}

func rewardPoolOutputAmounts(outputs []*models.RewardPoolOutput) []string {
	ret := make([]string, 0, len(outputs))
	for _, output := range outputs {
		ret = append(ret, fmt.Sprintf(
			"%x total=%d leader=%d members=%d undistributed=%d unspendable=%d",
			output.PoolKeyHash,
			output.TotalReward,
			output.LeaderReward,
			output.MemberRewardTotal,
			output.Undistributed,
			output.Unspendable,
		))
	}
	slices.Sort(ret)
	return ret
}

func rewardAccountOutputAmounts(
	outputs []*models.RewardAccountOutput,
) []string {
	ret := make([]string, 0, len(outputs))
	for _, output := range outputs {
		ret = append(ret, fmt.Sprintf(
			"%d:%x %s pool=%x amount=%d spendable=%t",
			output.CredentialTag,
			output.StakingKey,
			output.RewardType,
			output.PoolKeyHash,
			output.Amount,
			output.Spendable,
		))
	}
	slices.Sort(ret)
	return ret
}
