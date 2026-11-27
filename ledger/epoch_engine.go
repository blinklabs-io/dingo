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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// engine returns the epoch-boundary engine over ls. It is built per call from
// the current configuration, so it holds no state of its own.
func (ls *LedgerState) engine() *epochengine.Engine {
	return epochengine.New(
		epochengine.Config{
			Logger:                      ls.config.Logger,
			CardanoNodeConfig:           ls.config.CardanoNodeConfig,
			MinPoolMargin:               ls.config.MinPoolMargin,
			PledgeLeverageEnabled:       ls.config.PledgeLeverageEnabled,
			PledgeLeverage:              ls.config.PledgeLeverage,
			FullPotRewardsEnabled:       ls.config.FullPotRewardsEnabled,
			DelegatorInactivityEnabled:  ls.config.DelegatorInactivityEnabled,
			DelegatorInactivity:         ls.config.DelegatorInactivity,
			WarmGenesisStake:            ls.config.WarmGenesisStake,
			RewardUpdateStabilityWindow: ls.rewardUpdateWindowOverride,
			FatalErrorFunc:              ls.config.FatalErrorFunc,
			HaltError:                   errHaltLedgerPipeline,
		},
		engineHost{ls: ls},
	)
}

// engineHost is the LedgerState side of epochengine.Host.
type engineHost struct {
	ls *LedgerState
}

func (h engineHost) LedgerDB() ledgerdb.LedgerDB {
	return h.ls.ledgerDB()
}

func (h engineHost) EraByID(id uint) (*eras.EraDesc, bool) {
	return h.ls.eraById(id)
}

func (h engineHost) SlotToEpoch(slot uint64) (models.Epoch, error) {
	return h.ls.SlotToEpoch(slot)
}

func (h engineHost) StagePoolBoundary(
	txn ledgerdb.Txn,
	newEpoch, boundarySlot uint64,
) {
	h.ls.stagePoolBoundary(
		txn,
		newEpoch,
		boundarySlot,
	) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
}

func (h engineHost) QueueRewardCreditCompaction(ctx context.Context) {
	h.ls.queueRewardCreditCompaction(ctx)
}

func (h engineHost) IncSkippedStakeRewardRounds() {
	h.ls.metrics.incSkippedStakeRewardRounds()
}

// RewardBlockCounts answers from the live set. The metadata store has no rows
// to fall back to: an epoch whose counts the set never held is reported
// unknown, which the reward round declines rather than distributes.
func (h engineHost) RewardBlockCounts(
	txn ledgerdb.Txn,
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
) (map[string]uint64, uint64, bool, error) {
	ls := h.ls
	if r := ls.rewardBlocksServing(txn); r != nil {
		counts, total, ok, err := ls.rewardBlockCountsFromSet(
			r, txn, epoch, poolKeys,
		)
		if err != nil {
			return nil, 0, false, err
		}
		if ok {
			return counts, total, true, nil
		}
		return nil, 0, false, nil
	}
	return nil, 0, false, ledgerStateUnserved(
		nodesettings.FamilyRewardBlocksFees,
	)
}

func (h engineHost) RewardEpochFees(
	txn ledgerdb.Txn,
	epoch models.Epoch,
) (uint64, error) {
	return h.ls.rewardEpochFeesFor(txn, epoch)
}

func (h engineHost) RewardRollbackSeq(txn ledgerdb.Txn) (uint64, error) {
	r := h.ls.rewardDerivedReader(txn)
	if r == nil {
		return 0, nil
	}
	return r.RewardRollbackSeq()
}

func (h engineHost) AccountInactivityActivationFloors(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
) (map[string]uint64, error) {
	ls := h.ls
	floors := make(map[string]uint64)
	r := ls.accountServing(txn)
	if r == nil {
		return nil, ledgerStateUnserved(nodesettings.FamilyAccounts)
	}
	activationReader, ok := r.(AccountInactivityActivationReader)
	if !ok {
		return nil, errors.New(
			"set account activation provenance is unavailable",
		)
	}
	loaded, err := activationReader.AccountInactivityActivationLoaded()
	if err != nil {
		return nil, fmt.Errorf(
			"load set account activation provenance: %w",
			err,
		)
	}
	if !loaded {
		return nil, errors.New(
			"set account activation provenance is not loaded",
		)
	}
	for _, ref := range refs {
		a, err := ls.accountView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, fmt.Errorf(
				"load set account activation membership: %w",
				err,
			)
		}
		if a != nil && a.InactivityActivation {
			floors[ref.MapKey()] = a.InactivityActivationEpoch
		}
	}
	return floors, nil
}

func (h engineHost) IterateLiveUtxos(
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return h.ls.iterateLiveUtxos(txn, fn)
}

func (h engineHost) PublishAvvmRemoval(
	ctx context.Context,
	txn ledgerdb.Txn,
	boundarySlot uint64,
	removed []models.UtxoId,
) {
	h.ls.publishUtxoEventAfterCommit(ctx, txn, UtxoEvent{
		Point:   ocommon.Point{Slot: boundarySlot},
		Cause:   UtxoChangeAvvmRemoval,
		Removed: removed,
	}, nil)
}

func (h engineHost) RewardLikelihoodDB() ledgerdb.RewardLikelihoodStore {
	return h.ls.rewardLikelihoodDB()
}

func (h engineHost) RewardInputGuards() epochengine.RewardInputGuards {
	return epochengine.RewardInputGuards{
		Generation:      &h.ls.rewardInputGeneration,
		RollbackActive:  &h.ls.rewardInputRollbackActive,
		PrecomputeFence: &h.ls.rewardPrecomputeFence,
	}
}

func (h engineHost) DeferStakeRewardPrecompute(
	newEpoch, cutoffSlot, generation uint64,
) {
	h.ls.deferStakeRewardPrecompute(newEpoch, cutoffSlot, generation)
}

func (h engineHost) PrecomputeChunkPools() int {
	return h.ls.rewardPrecomputeChunkPoolsOverride
}

func (h engineHost) RewardPrecomputeChunkDone(completed, total int) {
	if h.ls.rewardPrecomputeChunkHook != nil {
		h.ls.rewardPrecomputeChunkHook(completed, total)
	}
}

func (h engineHost) RewardPrecomputeBeforeSave() {
	if h.ls.rewardPrecomputeBeforeSaveHook != nil {
		h.ls.rewardPrecomputeBeforeSaveHook()
	}
}

func (h engineHost) TipNonceState() (uint64, []byte) {
	h.ls.RLock()
	defer h.ls.RUnlock()
	return h.ls.currentTip.Point.Slot, h.ls.currentTipBlockNonce
}

func (h engineHost) BlockNoncesInSlotRange(
	start, end uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	return h.ls.blockNoncesInSlotRange(start, end, txn)
}

func (h engineHost) ComputeCandidateNonce(
	ctx context.Context,
	txn ledgerdb.Txn,
	eraID uint,
	prevEvolvingNonce []byte,
	prevCandidateNonce []byte,
	epochStartSlot uint64,
	epochLength uint64,
) ([]byte, []byte, error) {
	return h.ls.computeCandidateNonce(
		ctx, txn, eraID, prevEvolvingNonce, prevCandidateNonce,
		epochStartSlot, epochLength,
	)
}

func (h engineHost) EpochLabNonce(
	ctx context.Context,
	txn ledgerdb.Txn,
	startSlot, endSlot uint64,
	carried []byte,
) ([]byte, error) {
	return h.ls.epochLabNonce(ctx, txn, startSlot, endSlot, carried)
}
