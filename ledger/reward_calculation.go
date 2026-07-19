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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// The rollover stack consumes the application entry points below.
const stakeRewardSourcePrefix = "dingo:stake-reward:"

func (ls *LedgerState) applyStakeRewards(
	txn *database.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	if err != nil {
		return err
	}
	if !ok {
		app, ok, err = ls.calculateStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
			boundarySlot,
		)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return ls.applyStakeRewardApplication(txn, app, boundarySlot)
}

type stakeRewardApplication struct {
	pparams          lcommon.ProtocolParameters
	params           rewards.Parameters
	pots             *models.RewardAdaPots
	poolOutputs      []*models.RewardPoolOutput
	accountOutputs   []*models.RewardAccountOutput
	epochs           stakeRewardEpochs
	totalRewardPot   uint64
	availableRewards uint64
	effectiveRewards uint64
	undistributed    uint64
	unspendable      uint64
	precomputed      bool
	outputsUpdated   bool
	// snapshotCapturedSlot and snapshotBoundarySlot record the reward_snapshot
	// row's own captured/boundary slots as observed by
	// calculateStakeRewardApplication. Callers that compute this application
	// in a read-only transaction and persist it later in a separate write
	// transaction (see precomputeStakeRewardsAfterEpochTransition) use these
	// to re-verify the snapshot has not moved (e.g. via a rollback) before
	// writing; see stakeRewardPrecomputeSnapshotGuardOK.
	snapshotCapturedSlot uint64
	snapshotBoundarySlot uint64
	// snapshotEpochNonce and the snapshot* totals below record the rest of the
	// reward_snapshot row's content so the deferred-persist guard can reject a
	// same-slot snapshot whose content changed (a rollback that deletes and
	// re-derives the snapshot keeps deterministic slots but can alter its
	// contents). EpochNonce is fork-specific randomness and acts as the
	// content fingerprint; the totals are cheap belt-and-suspenders checks.
	snapshotEpochNonce       []byte
	snapshotTotalActiveStake types.Uint64
	snapshotTotalPoolCount   uint64
	snapshotTotalDelegators  uint64
	snapshotProtocolVersion  uint
	// rewardInputGeneration covers every rollback-sensitive input that is read
	// outside the eventual write transaction: performance blocks, ADA pots,
	// protocol state, and account certificate history. A rollback brackets its
	// mutations with generation changes, so the write guard can reject work
	// calculated before or during that rollback even when the Mark snapshot did
	// not change.
	rewardInputGeneration       uint64
	rewardInputGenerationSource *atomic.Uint64
	rewardInputRollbackActive   *atomic.Int64
}

type stakeRewardPrecomputeRetry struct {
	epochEvent event.EpochTransitionEvent
	cutoffSlot uint64
}

func (ls *LedgerState) calculateStakeRewardApplication(
	txn *database.Txn,
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) (*stakeRewardApplication, bool, error) {
	rewardInputGeneration := ls.rewardInputGeneration.Load()
	epochs, ok := stakeRewardEpochsForApplication(newEpoch)
	if !ok {
		return nil, false, nil
	}

	rewardSnapshotEpoch := epochs.snapshot
	performanceEpoch := epochs.performance
	potsEpoch := epochs.pots
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()

	pots, err := meta.GetRewardAdaPots(potsEpoch, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf("get reward ADA pots for epoch %d: %w", potsEpoch, err)
	}
	if pots == nil {
		ls.config.Logger.Debug(
			"skipping stake rewards: missing ADA pots",
			"component", "ledger",
			"new_epoch", newEpoch,
			"pots_epoch", potsEpoch,
		)
		return nil, false, nil
	}

	rewardSnapshot, err := meta.GetRewardSnapshot(
		rewardSnapshotEpoch, "mark", metaTxn,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward snapshot for epoch %d: %w",
			rewardSnapshotEpoch, err,
		)
	}
	if rewardSnapshot == nil {
		ls.config.Logger.Debug(
			"skipping stake rewards: missing reward snapshot",
			"component", "ledger",
			"new_epoch", newEpoch,
			"reward_snapshot_epoch", rewardSnapshotEpoch,
		)
		return nil, false, nil
	}

	poolInputs, err := meta.GetRewardPoolInputs(
		rewardSnapshotEpoch, metaTxn,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward pool inputs for epoch %d: %w",
			rewardSnapshotEpoch, err,
		)
	}
	stakeInputs, err := meta.GetRewardStakeInputs(
		rewardSnapshotEpoch, metaTxn,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward stake inputs for epoch %d: %w",
			rewardSnapshotEpoch, err,
		)
	}

	pparams, params, performanceDecentralization, err := ls.rewardParameters(
		txn,
		performanceEpoch,
		potsEpoch,
		pots,
	)
	if err != nil {
		return nil, false, err
	}
	blockCounts, totalBlocks, err := ls.rewardBlockCounts(
		meta,
		metaTxn,
		performanceEpoch,
		poolInputs,
		performanceDecentralization,
	)
	if err != nil {
		return nil, false, err
	}
	prefilterSlot, err := ls.rewardPrefilterSlot(meta, metaTxn, potsEpoch)
	if err != nil {
		return nil, false, err
	}
	if params.RequiresRewardPrefilter() && capturedSlot < prefilterSlot {
		ls.config.Logger.Debug(
			"skipping stake reward precompute before reward prefilter slot",
			"component", "ledger",
			"new_epoch", newEpoch,
			"reward_snapshot_epoch", rewardSnapshotEpoch,
			"captured_slot", capturedSlot,
			"prefilter_slot", prefilterSlot,
		)
		ls.deferStakeRewardPrecompute(newEpoch, prefilterSlot)
		return nil, false, nil
	}

	snapshot, err := ls.rewardCalculatorSnapshot(
		meta,
		metaTxn,
		rewardSnapshot,
		poolInputs,
		stakeInputs,
		blockCounts,
		totalBlocks,
		prefilterSlot,
		params.RequiresRewardPrefilter(),
	)
	if err != nil {
		return nil, false, err
	}
	result, err := rewards.Calculate(
		rewards.Pots{
			Reserves: uint64(pots.Reserves),
			Treasury: uint64(pots.Treasury),
			Fees:     uint64(pots.Fees),
		},
		snapshot,
		params,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"calculate stake rewards for snapshot epoch %d: %w",
			rewardSnapshotEpoch, err,
		)
	}
	if epochs.bootstrap {
		suppressBootstrapStakeRewards(result)
	}

	poolOutputs := rewardPoolOutputs(
		rewardSnapshotEpoch,
		capturedSlot,
		boundarySlot,
		result.PoolRewards,
	)
	accountOutputs := rewardAccountOutputs(
		rewardSnapshotEpoch,
		capturedSlot,
		boundarySlot,
		result.AccountRewards,
	)
	return &stakeRewardApplication{
		epochs:                      epochs,
		pparams:                     pparams,
		params:                      params,
		pots:                        pots,
		poolOutputs:                 poolOutputs,
		accountOutputs:              accountOutputs,
		totalRewardPot:              result.TotalRewardPot,
		availableRewards:            result.AvailableRewards,
		effectiveRewards:            result.EffectiveRewards,
		undistributed:               result.Undistributed,
		unspendable:                 result.Unspendable,
		snapshotCapturedSlot:        rewardSnapshot.CapturedSlot,
		snapshotBoundarySlot:        rewardSnapshot.BoundarySlot,
		snapshotEpochNonce:          rewardSnapshot.EpochNonce,
		snapshotTotalActiveStake:    rewardSnapshot.TotalActiveStake,
		snapshotTotalPoolCount:      rewardSnapshot.TotalPoolCount,
		snapshotTotalDelegators:     rewardSnapshot.TotalDelegators,
		snapshotProtocolVersion:     rewardSnapshot.ProtocolVersion,
		rewardInputGeneration:       rewardInputGeneration,
		rewardInputGenerationSource: &ls.rewardInputGeneration,
		rewardInputRollbackActive:   &ls.rewardInputRollbackActive,
	}, true, nil
}

func (ls *LedgerState) applyStakeRewardApplication(
	txn *database.Txn,
	app *stakeRewardApplication,
	boundarySlot uint64,
) error {
	if app == nil {
		return errors.New("missing stake reward application")
	}
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()

	if !app.precomputed || app.outputsUpdated {
		if err := saveStakeRewardOutputs(meta, metaTxn, app); err != nil {
			return err
		}
	}

	for _, output := range app.accountOutputs {
		reward, err := rewardFromAccountOutput(output)
		if err != nil {
			return err
		}
		if !reward.Spendable {
			continue
		}
		if err := ls.db.AddAccountRewardByCredential(
			reward.Credential.Tag,
			reward.Credential.Hash[:],
			reward.Amount,
			boundarySlot,
			stakeRewardSourceHash(app.epochs.snapshot, reward),
			txn,
		); err != nil {
			return fmt.Errorf(
				"credit stake reward epoch %d credential %x: %w",
				app.epochs.snapshot,
				reward.Credential.Hash,
				err,
			)
		}
	}

	reserves, treasury, err := stakeRewardUpdatedPots(app)
	if err != nil {
		return err
	}
	if err := meta.SetNetworkState(
		treasury,
		reserves,
		boundarySlot,
		metaTxn,
	); err != nil {
		return fmt.Errorf("set network state after stake rewards: %w", err)
	}
	app.pots.Rewards = types.Uint64(app.totalRewardPot)
	if err := meta.SaveRewardAdaPots(app.pots, metaTxn); err != nil {
		return fmt.Errorf("update reward ADA pots: %w", err)
	}

	ls.config.Logger.Info(
		"applied stake rewards",
		"component", "ledger",
		"reward_snapshot_epoch", app.epochs.snapshot,
		"performance_epoch", app.epochs.performance,
		"pots_epoch", app.epochs.pots,
		"pparams_type", fmt.Sprintf("%T", app.pparams),
		"precomputed", app.precomputed,
		"total_reward_pot", app.totalRewardPot,
		"available_rewards", app.availableRewards,
		"effective_rewards", app.effectiveRewards,
		"undistributed_rewards", app.undistributed,
		"unspendable_rewards", app.unspendable,
	)
	return nil
}

func (ls *LedgerState) precomputedStakeRewardApplication(
	txn *database.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) (*stakeRewardApplication, bool, error) {
	epochs, ok := stakeRewardEpochsForApplication(newEpoch)
	if !ok {
		return nil, false, nil
	}
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()

	pots, err := meta.GetRewardAdaPots(epochs.pots, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward ADA pots for epoch %d: %w",
			epochs.pots,
			err,
		)
	}
	if pots == nil || pots.Rewards == 0 {
		return nil, false, nil
	}
	// The bootstrap calculation has no durable output rows with which to
	// validate the precompute against rollbacks. Always recalculate it at the
	// epoch-2 boundary rather than treating pots.Rewards alone as provenance.
	if epochs.bootstrap {
		return nil, false, nil
	}

	rewardSnapshot, err := meta.GetRewardSnapshot(
		epochs.snapshot,
		"mark",
		metaTxn,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get precomputed reward snapshot for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	if rewardSnapshot == nil {
		return nil, false, nil
	}

	poolOutputs, err := meta.GetRewardPoolOutputs(epochs.snapshot, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get precomputed reward pool outputs for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	if uint64(len(poolOutputs)) != rewardSnapshot.TotalPoolCount {
		return nil, false, nil
	}
	poolInputs, err := meta.GetRewardPoolInputs(epochs.snapshot, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get precomputed reward pool inputs for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	inputsMatchSnapshot, err := precomputedRewardPoolInputsMatchSnapshot(
		rewardSnapshot,
		poolInputs,
	)
	if err != nil {
		return nil, false, err
	}
	if !inputsMatchSnapshot {
		return nil, false, nil
	}
	stakeInputs, err := meta.GetRewardStakeInputs(epochs.snapshot, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get precomputed reward stake inputs for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	inputsValid := validateRewardCalculatorInputs(
		rewardSnapshot,
		poolInputs,
		stakeInputs,
	) == nil
	if !inputsValid {
		return nil, false, nil
	}
	if !precomputedRewardPoolOutputsMatchInputs(poolOutputs, poolInputs) {
		return nil, false, nil
	}

	accountOutputs, err := meta.GetRewardAccountOutputs(epochs.snapshot, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get precomputed reward account outputs for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	if !precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		accountOutputs,
	) {
		return nil, false, nil
	}
	pparams, params, performanceDecentralization, err := ls.rewardParameters(
		txn,
		epochs.performance,
		epochs.pots,
		pots,
	)
	if err != nil {
		return nil, false, err
	}
	if !precomputedRewardAccountAmountsMatchInputs(
		poolInputs,
		poolOutputs,
		stakeInputs,
		accountOutputs,
		params,
	) {
		return nil, false, nil
	}
	if !precomputedRewardOutputsMatchBoundary(
		poolOutputs,
		accountOutputs,
		boundarySlot,
	) {
		return nil, false, nil
	}
	outputsComplete, err := precomputedRewardOutputsComplete(
		poolOutputs,
		accountOutputs,
		params.RequiresRewardPrefilter(),
	)
	if err != nil {
		return nil, false, err
	}
	if !outputsComplete {
		return nil, false, nil
	}
	availableRewards, err := rewardAvailableRewards(uint64(pots.Rewards), params)
	if err != nil {
		return nil, false, err
	}
	if !precomputedRewardPotFitsAdaPots(pots) {
		return nil, false, nil
	}
	outputsFit, err := precomputedRewardPoolOutputsFitAvailable(
		poolOutputs,
		availableRewards,
	)
	if err != nil {
		return nil, false, err
	}
	if !outputsFit {
		return nil, false, nil
	}
	// Re-derive each pool's reward from the frozen inputs and reject any
	// precomputed pool output whose stored PoolReward/LeaderReward does not
	// match. The account-amount and completeness checks above validate the
	// account rows only against these stored pool totals, so this is what ties
	// them back to the snapshot inputs; see
	// precomputedRewardPoolRewardsMatchInputs.
	if uint64(pots.Reserves) > params.MaxLovelaceSupply {
		return nil, false, nil
	}
	totalCirculation := params.MaxLovelaceSupply - uint64(pots.Reserves)
	blockCounts, totalBlocks, err := ls.rewardBlockCounts(
		meta,
		metaTxn,
		epochs.performance,
		poolInputs,
		performanceDecentralization,
	)
	if err != nil {
		return nil, false, err
	}
	poolRewardsMatch, err := precomputedRewardPoolRewardsMatchInputs(
		poolInputs,
		poolOutputs,
		blockCounts,
		availableRewards,
		uint64(rewardSnapshot.TotalActiveStake),
		totalCirculation,
		totalBlocks,
		params,
	)
	if err != nil {
		return nil, false, err
	}
	if !poolRewardsMatch {
		return nil, false, nil
	}
	if params.RequiresRewardPrefilter() {
		prefilterSlot, err := ls.rewardPrefilterSlot(
			meta,
			metaTxn,
			epochs.pots,
		)
		if err != nil {
			return nil, false, err
		}
		if precomputedOutputsBeforeSlot(
			poolOutputs,
			accountOutputs,
			prefilterSlot,
		) {
			return nil, false, nil
		}
		prefilterAccounts, err := rewardPrefilterAccounts(
			meta,
			metaTxn,
			poolInputs,
			nil,
			prefilterSlot,
			true,
			nil,
		)
		if err != nil {
			return nil, false, err
		}
		missingRegisteredLeader, err := precomputedMissingPrefilterLeaderOutput(
			poolInputs,
			poolOutputs,
			accountOutputs,
			prefilterAccounts,
		)
		if err != nil {
			return nil, false, err
		}
		if missingRegisteredLeader {
			return nil, false, nil
		}
	}
	outputsUpdated, err := finalizePrecomputedRewardOutputs(
		meta,
		metaTxn,
		poolOutputs,
		accountOutputs,
	)
	if err != nil {
		return nil, false, err
	}
	app := &stakeRewardApplication{
		epochs:         epochs,
		pparams:        pparams,
		params:         params,
		pots:           pots,
		poolOutputs:    poolOutputs,
		accountOutputs: accountOutputs,
		totalRewardPot: uint64(pots.Rewards),
		precomputed:    true,
		outputsUpdated: outputsUpdated,
	}
	if err := deriveStakeRewardApplicationTotals(app); err != nil {
		return nil, false, err
	}
	return app, true, nil
}

func precomputedRewardPoolOutputsMatchInputs(
	poolOutputs []*models.RewardPoolOutput,
	poolInputs []*models.RewardPoolInput,
) bool {
	if len(poolInputs) == 0 {
		return len(poolOutputs) == 0
	}
	if len(poolOutputs) != len(poolInputs) {
		return false
	}
	expectedOwnerStake := make(map[string]types.Uint64, len(poolInputs))
	for _, input := range poolInputs {
		if input == nil {
			return false
		}
		expectedOwnerStake[string(input.PoolKeyHash)] = input.OwnerStake
	}
	if len(expectedOwnerStake) != len(poolInputs) {
		return false
	}
	for _, output := range poolOutputs {
		if output == nil {
			return false
		}
		key := string(output.PoolKeyHash)
		ownerStake, ok := expectedOwnerStake[key]
		if !ok {
			return false
		}
		if output.OwnerStake != ownerStake {
			return false
		}
		delete(expectedOwnerStake, key)
	}
	return len(expectedOwnerStake) == 0
}

// precomputedRewardPoolRewardsMatchInputs re-derives each persisted pool's
// PoolReward (stored as TotalReward) and LeaderReward from the frozen snapshot
// inputs using the canonical reward arithmetic, and rejects the reuse if either
// diverges. Without it the reuse path would trust the stored pool reward:
// precomputedRewardAccountAmountsMatchInputs pins each applied leader amount to
// poolOutput.LeaderReward and re-derives each member amount from
// poolOutput.TotalReward, and precomputedRewardOutputsComplete only proves the
// account rows sum to those stored totals. A stale or corrupted pool output
// paired with account outputs consistent with it therefore passes every other
// check and is credited to accounts unverified. The block counts and reward
// globals come from the same sources the authoritative calculation reads
// (rewardBlockCounts over the immutable performance epoch, availableRewards from
// the persisted reward pot, total active/circulating stake from the snapshot and
// ADA pots), so a legitimate precompute reproduces its stored values exactly and
// only a divergent output is rejected.
func precomputedRewardPoolRewardsMatchInputs(
	poolInputs []*models.RewardPoolInput,
	poolOutputs []*models.RewardPoolOutput,
	blockCounts map[string]uint64,
	availableRewards uint64,
	totalActiveStake uint64,
	totalCirculation uint64,
	totalBlocks uint64,
	params rewards.Parameters,
) (bool, error) {
	poolOutputByKey := make(map[string]*models.RewardPoolOutput, len(poolOutputs))
	for _, output := range poolOutputs {
		if output == nil {
			return false, nil
		}
		poolOutputByKey[string(output.PoolKeyHash)] = output
	}
	for _, input := range poolInputs {
		if input == nil {
			return false, nil
		}
		key := string(input.PoolKeyHash)
		output, ok := poolOutputByKey[key]
		if !ok {
			return false, nil
		}
		// The pool ID is not needed here: it only labels the result and the pool
		// keys were already validated upstream by validateRewardCalculatorInputs.
		reward, err := rewards.CalculatePoolReward(
			rewards.Pool{
				Margin:         ratOrZero(input.Margin),
				Pledge:         uint64(input.Pledge),
				Cost:           uint64(input.Cost),
				DelegatedStake: uint64(input.DelegatedStake),
				OwnerStake:     uint64(input.OwnerStake),
				BlocksProduced: blockCounts[key],
				TotalBlocks:    totalBlocks,
			},
			availableRewards,
			totalActiveStake,
			totalCirculation,
			totalBlocks,
			params,
		)
		if err != nil {
			return false, err
		}
		if uint64(output.TotalReward) != reward.PoolReward ||
			uint64(output.LeaderReward) != reward.LeaderReward {
			return false, nil
		}
	}
	return true, nil
}

// precomputedRewardAccountAmountsMatchInputs re-derives each persisted account
// reward amount from the frozen pool/stake inputs and rejects the reuse if any
// output's amount does not match.
//
// precomputedRewardAccountOutputsMatchInputs proves each output maps to a valid
// leader/member of its pool, and precomputedRewardOutputsComplete proves the
// global, per-pool, and per-pool-per-type totals. None of those pin an
// individual recipient's share, so a reward redistributed among the members of
// a single pool (member A's row carrying member B's amount, B's row absent)
// preserves every checked aggregate and would otherwise be accepted and applied
// to the wrong accounts. Re-deriving each amount closes that gap: a redistributed
// or absorbed share fails this per-recipient check, while a share dropped without
// an absorber still fails the per-pool total check. A leader row's amount is
// pinned directly to the pool output's stored leader reward.
//
// Only the amounts of rows that exist are re-derived here; which rows must exist
// (prefilter/zero-reward omission, era-specific dedup) is already governed by the
// membership, per-pool-total, and prefilter-leader checks. The amount of a row
// that exists is the stake-derived value in every era, because era dedup only
// drops rows, it never rewrites amounts.
func precomputedRewardAccountAmountsMatchInputs(
	poolInputs []*models.RewardPoolInput,
	poolOutputs []*models.RewardPoolOutput,
	stakeInputs []*models.RewardStakeInput,
	accountOutputs []*models.RewardAccountOutput,
	params rewards.Parameters,
) bool {
	poolInputByKey := make(map[string]*models.RewardPoolInput, len(poolInputs))
	for _, input := range poolInputs {
		if input == nil {
			return false
		}
		poolInputByKey[string(input.PoolKeyHash)] = input
	}
	poolOutputByKey := make(map[string]*models.RewardPoolOutput, len(poolOutputs))
	for _, output := range poolOutputs {
		if output == nil {
			return false
		}
		poolOutputByKey[string(output.PoolKeyHash)] = output
	}
	// Member stake keyed by pool + credential, matching the account output
	// identity. Owners never receive member rewards, so they are excluded.
	type memberStakeKey struct {
		pool string
		cred string
	}
	memberStakeByKey := make(map[memberStakeKey]uint64, len(stakeInputs))
	for _, input := range stakeInputs {
		if input == nil {
			return false
		}
		if input.Owner {
			continue
		}
		credential, err := rewards.NewCredential(
			input.CredentialTag,
			input.StakingKey,
		)
		if err != nil {
			return false
		}
		memberStakeByKey[memberStakeKey{
			pool: string(input.PoolKeyHash),
			cred: credential.Key(),
		}] = uint64(input.Stake)
	}
	// Any decode error, missing input, or amount overflow rejects reuse and
	// defers to the authoritative fresh calculation, which surfaces a real error
	// rather than swallowing it here.
	for _, output := range accountOutputs {
		reward, err := rewardFromAccountOutput(output)
		if err != nil {
			return false
		}
		poolKey := string(output.PoolKeyHash)
		poolOutput, ok := poolOutputByKey[poolKey]
		if !ok {
			return false
		}
		switch reward.Type {
		case rewards.RewardTypeLeader:
			if reward.Amount != uint64(poolOutput.LeaderReward) {
				return false
			}
		case rewards.RewardTypeMember:
			poolInput, ok := poolInputByKey[poolKey]
			if !ok {
				return false
			}
			stake, ok := memberStakeByKey[memberStakeKey{
				pool: poolKey,
				cred: reward.Credential.Key(),
			}]
			if !ok {
				return false
			}
			expected, err := rewards.MemberRewardWithParameters(
				uint64(poolOutput.TotalReward),
				uint64(poolInput.Cost),
				ratOrZero(poolInput.Margin),
				stake,
				uint64(poolInput.DelegatedStake),
				params,
			)
			if err != nil || reward.Amount != expected {
				return false
			}
		}
	}
	return true
}

func precomputedRewardAccountOutputsMatchInputs(
	poolInputs []*models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
	accountOutputs []*models.RewardAccountOutput,
) bool {
	rewardAccountsByPool := make(map[string]rewards.Credential, len(poolInputs))
	for _, input := range poolInputs {
		if input == nil {
			return false
		}
		if _, err := rewards.NewPoolID(input.PoolKeyHash); err != nil {
			return false
		}
		credential, err := rewards.NewCredential(
			input.RewardAccountCredentialTag,
			input.RewardAccount,
		)
		if err != nil {
			return false
		}
		key := string(input.PoolKeyHash)
		if _, ok := rewardAccountsByPool[key]; ok {
			return false
		}
		rewardAccountsByPool[key] = credential
	}
	memberCredentialsByPool := make(
		map[string]map[rewards.Credential]struct{},
		len(poolInputs),
	)
	for _, input := range stakeInputs {
		if input == nil {
			return false
		}
		if input.Owner {
			continue
		}
		if _, err := rewards.NewPoolID(input.PoolKeyHash); err != nil {
			return false
		}
		credential, err := rewards.NewCredential(
			input.CredentialTag,
			input.StakingKey,
		)
		if err != nil {
			return false
		}
		key := string(input.PoolKeyHash)
		members := memberCredentialsByPool[key]
		if members == nil {
			members = make(map[rewards.Credential]struct{})
			memberCredentialsByPool[key] = members
		}
		members[credential] = struct{}{}
	}
	for _, output := range accountOutputs {
		reward, err := rewardFromAccountOutput(output)
		if err != nil {
			return false
		}
		expectedRewardAccount, ok := rewardAccountsByPool[string(output.PoolKeyHash)]
		if !ok {
			return false
		}
		switch reward.Type {
		case rewards.RewardTypeLeader:
			if reward.Credential != expectedRewardAccount {
				return false
			}
		case rewards.RewardTypeMember:
			// A member reward output must correspond to a delegator captured in
			// the stake inputs. When stakeInputs is empty, memberCredentialsByPool
			// is empty and any member output is rejected, forcing recomputation of
			// stale/corrupted precomputed outputs.
			members := memberCredentialsByPool[string(output.PoolKeyHash)]
			if _, ok := members[reward.Credential]; !ok {
				return false
			}
		}
	}
	return true
}

func precomputedRewardOutputsMatchBoundary(
	poolOutputs []*models.RewardPoolOutput,
	accountOutputs []*models.RewardAccountOutput,
	boundarySlot uint64,
) bool {
	for _, output := range poolOutputs {
		if output == nil {
			return false
		}
		if output.BoundarySlot != boundarySlot ||
			output.CapturedSlot > boundarySlot {
			return false
		}
	}
	for _, output := range accountOutputs {
		if output == nil {
			return false
		}
		if output.BoundarySlot != boundarySlot ||
			output.CapturedSlot > boundarySlot {
			return false
		}
	}
	return true
}

func precomputedRewardPoolInputsMatchSnapshot(
	snapshot *models.RewardSnapshot,
	poolInputs []*models.RewardPoolInput,
) (bool, error) {
	if snapshot == nil {
		return false, nil
	}
	if uint64(len(poolInputs)) != snapshot.TotalPoolCount {
		return false, nil
	}
	seen := make(map[string]struct{}, len(poolInputs))
	var totalDelegated uint64
	var totalDelegators uint64
	for _, input := range poolInputs {
		if input == nil {
			return false, nil
		}
		if input.CapturedSlot != snapshot.CapturedSlot ||
			input.BoundarySlot != snapshot.BoundarySlot {
			return false, nil
		}
		if !validRewardPoolID(input.PoolKeyHash) {
			return false, nil
		}
		if !validRewardCredential(
			input.RewardAccountCredentialTag,
			input.RewardAccount,
		) {
			return false, nil
		}
		if input.Margin == nil || input.Margin.Rat == nil {
			return false, nil
		}
		if input.Margin.Sign() < 0 ||
			input.Margin.Cmp(big.NewRat(1, 1)) > 0 {
			return false, nil
		}
		if input.OwnerStake > input.DelegatedStake {
			return false, nil
		}
		var overflow bool
		totalDelegators, overflow = addRewardUint64(
			totalDelegators,
			input.DelegatorCount,
		)
		if overflow {
			return false, errors.New(
				"precomputed pool input delegator count overflow",
			)
		}
		key := string(input.PoolKeyHash)
		if _, ok := seen[key]; ok {
			return false, nil
		}
		seen[key] = struct{}{}
		totalDelegated, overflow = addRewardUint64(
			totalDelegated,
			uint64(input.DelegatedStake),
		)
		if overflow {
			return false, errors.New(
				"precomputed pool input delegated stake overflow",
			)
		}
	}
	if totalDelegated != uint64(snapshot.TotalActiveStake) {
		return false, nil
	}
	if totalDelegators != snapshot.TotalDelegators {
		return false, nil
	}
	return true, nil
}

func precomputedRewardOutputsComplete(
	poolOutputs []*models.RewardPoolOutput,
	accountOutputs []*models.RewardAccountOutput,
	allowMissingLeaderOutputs bool,
) (bool, error) {
	expectedByPool := make(map[string]uint64, len(poolOutputs))
	expectedLeaderByPool := make(map[string]uint64, len(poolOutputs))
	expectedMemberByPool := make(map[string]uint64, len(poolOutputs))
	var expected uint64
	for _, output := range poolOutputs {
		if output == nil {
			return false, nil
		}
		if !validRewardPoolID(output.PoolKeyHash) {
			return false, nil
		}
		totalReward := uint64(output.TotalReward)
		undistributed := uint64(output.Undistributed)
		if undistributed > totalReward {
			return false, nil
		}
		poolExpected := totalReward - undistributed
		leaderReward := uint64(output.LeaderReward)
		memberReward := uint64(output.MemberRewardTotal)
		distributed, overflow := addRewardUint64(leaderReward, memberReward)
		if overflow || distributed > totalReward {
			return false, nil
		}
		poolKey := string(output.PoolKeyHash)
		expectedLeaderByPool[poolKey] = leaderReward
		expectedMemberByPool[poolKey] = memberReward
		expectedByPool[poolKey], overflow = addRewardUint64(
			expectedByPool[poolKey],
			poolExpected,
		)
		if overflow {
			return false, errors.New("precomputed pool output total overflow")
		}
		expected, overflow = addRewardUint64(expected, poolExpected)
		if overflow {
			return false, errors.New("precomputed pool output total overflow")
		}
	}

	actualByPool := make(map[string]uint64, len(expectedByPool))
	actualLeaderByPool := make(map[string]uint64, len(expectedLeaderByPool))
	actualMemberByPool := make(map[string]uint64, len(expectedMemberByPool))
	seenAccountOutputs := make(map[string]struct{}, len(accountOutputs))
	var actual uint64
	for _, output := range accountOutputs {
		if output == nil {
			return false, nil
		}
		if !validRewardAccountOutput(output) {
			return false, nil
		}
		identityKey := rewardAccountOutputIdentityKey(output)
		if _, ok := seenAccountOutputs[identityKey]; ok {
			return false, nil
		}
		seenAccountOutputs[identityKey] = struct{}{}
		amount := uint64(output.Amount)
		if amount == 0 {
			return false, nil
		}
		poolKey := string(output.PoolKeyHash)
		var overflow bool
		actualByPool[poolKey], overflow = addRewardUint64(
			actualByPool[poolKey],
			amount,
		)
		if overflow {
			return false, errors.New("precomputed account output total overflow")
		}
		switch rewards.RewardType(output.RewardType) {
		case rewards.RewardTypeLeader:
			actualLeaderByPool[poolKey], overflow = addRewardUint64(
				actualLeaderByPool[poolKey],
				amount,
			)
		case rewards.RewardTypeMember:
			actualMemberByPool[poolKey], overflow = addRewardUint64(
				actualMemberByPool[poolKey],
				amount,
			)
		}
		if overflow {
			return false, errors.New("precomputed reward type total overflow")
		}
		actual, overflow = addRewardUint64(actual, amount)
		if overflow {
			return false, errors.New("precomputed account output total overflow")
		}
	}
	if actual != expected {
		return false, nil
	}
	for poolKey, expectedAmount := range expectedByPool {
		if actualByPool[poolKey] != expectedAmount {
			return false, nil
		}
		delete(actualByPool, poolKey)
	}
	if len(actualByPool) != 0 {
		return false, nil
	}
	for poolKey, expectedAmount := range expectedLeaderByPool {
		actualAmount := actualLeaderByPool[poolKey]
		if actualAmount != expectedAmount &&
			(!allowMissingLeaderOutputs || actualAmount != 0) {
			return false, nil
		}
		delete(actualLeaderByPool, poolKey)
	}
	if len(actualLeaderByPool) != 0 {
		return false, nil
	}
	for poolKey, expectedAmount := range expectedMemberByPool {
		if actualMemberByPool[poolKey] != expectedAmount {
			return false, nil
		}
		delete(actualMemberByPool, poolKey)
	}
	return len(actualMemberByPool) == 0, nil
}

func precomputedMissingPrefilterLeaderOutput(
	poolInputs []*models.RewardPoolInput,
	poolOutputs []*models.RewardPoolOutput,
	accountOutputs []*models.RewardAccountOutput,
	prefilterAccounts map[string]struct{},
) (bool, error) {
	rewardAccountByPool := make(map[string]string, len(poolInputs))
	for _, input := range poolInputs {
		if input == nil {
			return false, nil
		}
		if !validRewardPoolID(input.PoolKeyHash) {
			return false, nil
		}
		if !validRewardCredential(
			input.RewardAccountCredentialTag,
			input.RewardAccount,
		) {
			return false, nil
		}
		poolKey := string(input.PoolKeyHash)
		if _, exists := rewardAccountByPool[poolKey]; exists {
			return false, nil
		}
		rewardAccountByPool[poolKey] = models.NewStakeCredentialRef(
			input.RewardAccountCredentialTag,
			input.RewardAccount,
		).MapKey()
	}

	actualLeaderByPool := make(map[string]uint64, len(poolOutputs))
	for _, output := range accountOutputs {
		if output == nil {
			return false, nil
		}
		if rewards.RewardType(output.RewardType) != rewards.RewardTypeLeader {
			continue
		}
		amount := uint64(output.Amount)
		var overflow bool
		poolKey := string(output.PoolKeyHash)
		actualLeaderByPool[poolKey], overflow = addRewardUint64(
			actualLeaderByPool[poolKey],
			amount,
		)
		if overflow {
			return false, errors.New("precomputed leader output total overflow")
		}
	}

	for _, output := range poolOutputs {
		if output == nil || output.LeaderReward == 0 {
			continue
		}
		poolKey := string(output.PoolKeyHash)
		if actualLeaderByPool[poolKey] != 0 {
			continue
		}
		rewardAccount, ok := rewardAccountByPool[poolKey]
		if !ok {
			return false, nil
		}
		if _, ok := prefilterAccounts[rewardAccount]; ok {
			return true, nil
		}
	}
	return false, nil
}

func rewardAccountOutputIdentityKey(output *models.RewardAccountOutput) string {
	return string([]byte{output.CredentialTag}) +
		string(output.StakingKey) +
		string(output.PoolKeyHash) +
		"\x00" +
		output.RewardType
}

func rewardAvailableRewards(totalRewardPot uint64, params rewards.Parameters) (uint64, error) {
	treasuryTax, err := rewardFloorMul(params.TreasuryExpansion, totalRewardPot)
	if err != nil {
		return 0, fmt.Errorf("calculate treasury tax: %w", err)
	}
	if treasuryTax > totalRewardPot {
		return 0, fmt.Errorf(
			"invalid treasury tax %d above reward pot %d",
			treasuryTax,
			totalRewardPot,
		)
	}
	return totalRewardPot - treasuryTax, nil
}

func precomputedRewardPotFitsAdaPots(pots *models.RewardAdaPots) bool {
	if pots == nil {
		return false
	}
	totalRewardPot := uint64(pots.Rewards)
	fees := uint64(pots.Fees)
	if totalRewardPot < fees {
		return false
	}
	incentives := totalRewardPot - fees
	return uint64(pots.Reserves) >= incentives
}

func precomputedRewardPoolOutputsFitAvailable(
	poolOutputs []*models.RewardPoolOutput,
	availableRewards uint64,
) (bool, error) {
	var total uint64
	for _, output := range poolOutputs {
		if output == nil {
			continue
		}
		next, overflow := addRewardUint64(total, uint64(output.TotalReward))
		if overflow {
			return false, errors.New("precomputed pool reward total overflow")
		}
		total = next
	}
	return total <= availableRewards, nil
}

func precomputedOutputsBeforeSlot(
	poolOutputs []*models.RewardPoolOutput,
	accountOutputs []*models.RewardAccountOutput,
	slot uint64,
) bool {
	for _, output := range poolOutputs {
		if output != nil && output.CapturedSlot < slot {
			return true
		}
	}
	for _, output := range accountOutputs {
		if output != nil && output.CapturedSlot < slot {
			return true
		}
	}
	return false
}

func finalizePrecomputedRewardOutputs(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	poolOutputs []*models.RewardPoolOutput,
	accountOutputs []*models.RewardAccountOutput,
) (bool, error) {
	refsByKey := make(map[string]models.StakeCredentialRef)
	for _, output := range accountOutputs {
		if output == nil || len(output.StakingKey) != rewards.CredentialHashSize {
			continue
		}
		ref := models.NewStakeCredentialRef(
			output.CredentialTag,
			output.StakingKey,
		)
		refsByKey[ref.MapKey()] = ref
	}
	refs := make([]models.StakeCredentialRef, 0, len(refsByKey))
	for _, ref := range refsByKey {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].MapKey() < refs[j].MapKey()
	})

	activeAccounts, err := meta.GetAccountsByCredential(refs, false, metaTxn)
	if err != nil {
		return false, fmt.Errorf("get final reward account eligibility: %w", err)
	}

	updated := false
	unspendableByPool := make(map[string]uint64, len(poolOutputs))
	for _, output := range accountOutputs {
		if output == nil {
			continue
		}
		ref := models.NewStakeCredentialRef(
			output.CredentialTag,
			output.StakingKey,
		)
		_, spendable := activeAccounts[ref.MapKey()]
		if output.Spendable != spendable {
			output.Spendable = spendable
			updated = true
		}
		if !output.Spendable {
			key := string(output.PoolKeyHash)
			next, overflow := addRewardUint64(
				unspendableByPool[key],
				uint64(output.Amount),
			)
			if overflow {
				return false, errors.New("precomputed unspendable total overflow")
			}
			unspendableByPool[key] = next
		}
	}

	for _, output := range poolOutputs {
		if output == nil {
			continue
		}
		unspendable := types.Uint64(unspendableByPool[string(output.PoolKeyHash)])
		if output.Unspendable != unspendable {
			output.Unspendable = unspendable
			updated = true
		}
	}
	return updated, nil
}

func (ls *LedgerState) handleRewardPrecomputeEpochTransition(evt event.Event) {
	ls.handleRewardPrecomputeEpochTransitionWith(
		evt,
		ls.precomputeStakeRewardsAfterEpochTransition,
	)
}

func (ls *LedgerState) handleRewardPrecomputeEpochTransitionWith(
	evt event.Event,
	precompute func(event.EpochTransitionEvent) error,
) {
	epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
	if !ok || epochEvent.EpochNonce == nil {
		return
	}
	ls.queueRewardPrecompute(epochEvent, precompute)
}

// queueRewardPrecompute keeps EventBus delivery independent of the expensive
// reward calculation. While a calculation is active, each transition replaces
// the pending one so bulk sync retains the newest useful epoch without filling
// and overflowing the EventBus subscriber buffer. The active calculation is
// allowed to finish because its snapshot guard drops stale output; the worker
// then processes the latest transition observed in the meantime.
func (ls *LedgerState) queueRewardPrecompute(
	epochEvent event.EpochTransitionEvent,
	precompute func(event.EpochTransitionEvent) error,
) {
	ls.rewardPrecomputeMu.Lock()
	if ls.closed.Load() {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	// Store an independent copy because EventBus callbacks do not own the
	// publisher's payload after returning.
	epochEvent.EpochNonce = slices.Clone(epochEvent.EpochNonce)
	ls.rewardPrecomputePending = &epochEvent
	if ls.rewardPrecomputeRunning {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputeWG.Add(1)
	ls.rewardPrecomputeMu.Unlock()

	go ls.runRewardPrecompute(precompute)
}

// deferStakeRewardPrecompute records the pre-Babbage RUPD cutoff. The retry is
// queued only after the applied ledger tip reaches that slot, so its
// BoundarySlot reflects the actual state captured by the calculation rather
// than a future slot whose certificate history is not available yet.
func (ls *LedgerState) deferStakeRewardPrecompute(
	newEpoch uint64,
	cutoffSlot uint64,
) {
	if newEpoch == 0 {
		return
	}
	retry := &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{
			NewEpoch: newEpoch - 1,
		},
		cutoffSlot: cutoffSlot,
	}
	ls.rewardPrecomputeMu.Lock()
	if ls.rewardPrecomputeRetry == nil ||
		ls.rewardPrecomputeRetry.epochEvent.NewEpoch <= retry.epochEvent.NewEpoch {
		ls.rewardPrecomputeRetry = retry
	}
	ls.rewardPrecomputeMu.Unlock()

	ls.RLock()
	capturedSlot := ls.currentTip.Point.Slot
	ls.RUnlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(capturedSlot)
}

// maybeQueueStakeRewardPrecomputeRetry releases a deferred pre-Babbage
// calculation once committed ledger state has reached its RUPD cutoff.
func (ls *LedgerState) maybeQueueStakeRewardPrecomputeRetry(capturedSlot uint64) {
	ls.rewardPrecomputeMu.Lock()
	retry := ls.rewardPrecomputeRetry
	if retry == nil || capturedSlot < retry.cutoffSlot || ls.closed.Load() {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	ls.rewardPrecomputeRetry = nil
	epochEvent := retry.epochEvent
	epochEvent.BoundarySlot = capturedSlot
	ls.rewardPrecomputeMu.Unlock()
	ls.queueRewardPrecompute(
		epochEvent,
		ls.precomputeStakeRewardsAfterEpochTransition,
	)
}

func (ls *LedgerState) runRewardPrecompute(
	precompute func(event.EpochTransitionEvent) error,
) {
	defer ls.rewardPrecomputeWG.Done()
	for {
		ls.rewardPrecomputeMu.Lock()
		if ls.closed.Load() || ls.rewardPrecomputePending == nil {
			ls.rewardPrecomputePending = nil
			ls.rewardPrecomputeRunning = false
			ls.rewardPrecomputeMu.Unlock()
			return
		}
		epochEvent := *ls.rewardPrecomputePending
		ls.rewardPrecomputePending = nil
		ls.rewardPrecomputeMu.Unlock()

		if err := callRewardPrecompute(precompute, epochEvent); err != nil {
			ls.config.Logger.Warn(
				"failed to precompute stake rewards",
				"component", "ledger",
				"new_epoch", epochEvent.NewEpoch,
				"error", err,
			)
		}
	}
}

// callRewardPrecompute preserves the panic isolation that EventBus provided
// when the calculation ran directly inside its callback. A failed calculation
// must not crash the process or prevent the worker from consuming the newest
// transition that arrived while it was running.
func callRewardPrecompute(
	precompute func(event.EpochTransitionEvent) error,
	epochEvent event.EpochTransitionEvent,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("reward precompute panicked: %v", recovered)
		}
	}()
	return precompute(epochEvent)
}

// precomputeStakeRewardsAfterEpochTransition runs off the EventBus after an
// epoch transition to opportunistically precompute the next boundary's stake
// rewards ahead of time, so that the synchronous rollover application
// (applyStakeRewards, called from processEpochRollover with its own short
// transaction) can reuse the precomputed result instead of computing rewards
// inline.
//
// The calculation reads a large amount of reward-input state (every pool and
// per-credential stake input for the mark snapshot epoch, plus per-credential
// cert-history prefilter queries) and, on a mainnet-scale database, can run
// for minutes. It must never hold SQLite's single writer for that long, so
// it is split into a read-only calculation phase and a short write phase:
// see precomputeStakeRewardsCalculate and saveStakeRewardPrecompute. Between
// the two phases the world may have moved on (a rollback replacing the
// reward snapshot, or the boundary-application path already having run);
// the write phase re-validates before persisting and drops a stale result
// rather than writing it -- see stakeRewardPrecomputeSnapshotGuardOK.
func (ls *LedgerState) precomputeStakeRewardsAfterEpochTransition(
	evt event.EpochTransitionEvent,
) error {
	if evt.NewEpoch == math.MaxUint64 {
		return nil
	}

	var (
		haveEpoch               bool
		applicationBoundarySlot uint64
	)
	epochTxn := ls.db.Transaction(false)
	if err := epochTxn.Do(func(txn *database.Txn) error {
		epoch, err := ls.db.Metadata().GetEpoch(evt.NewEpoch, txn.Metadata())
		if err != nil {
			return fmt.Errorf(
				"get epoch %d for reward precompute: %w",
				evt.NewEpoch,
				err,
			)
		}
		if epoch == nil || epoch.LengthInSlots == 0 {
			return nil
		}
		applicationBoundarySlot = epoch.StartSlot + uint64(epoch.LengthInSlots)
		haveEpoch = true
		return nil
	}); err != nil {
		return err
	}
	if !haveEpoch {
		return nil
	}

	newEpoch := evt.NewEpoch + 1
	capturedSlot := evt.BoundarySlot

	// Read phase: compute (but do not persist) the reward application. This
	// is the expensive part -- it scans every pool/stake reward input for
	// the mark snapshot epoch -- so it runs in a read-only transaction,
	// which SQLite can serve concurrently with block-application writes.
	var app *stakeRewardApplication
	readTxn := ls.db.Transaction(false)
	if err := readTxn.Do(func(txn *database.Txn) error {
		computed, ok, err := ls.precomputeStakeRewardsCalculate(
			txn,
			newEpoch,
			capturedSlot,
			applicationBoundarySlot,
		)
		if err != nil || !ok {
			return err
		}
		app = computed
		return nil
	}); err != nil {
		return err
	}
	if app == nil {
		// Either a valid precomputed application already exists, or there
		// is nothing to compute yet (missing snapshot/pots/prefilter slot
		// not reached). Nothing to write.
		return nil
	}

	// Write phase: re-verify the calculation is still valid, then persist.
	// This holds the single SQLite writer only for a guard check plus a
	// handful of upserts, not for the calculation above.
	writeTxn := ls.db.Transaction(true)
	return writeTxn.Do(func(txn *database.Txn) error {
		meta := ls.db.Metadata()
		metaTxn := txn.Metadata()

		if _, alreadyApplied, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			applicationBoundarySlot,
		); err != nil {
			return err
		} else if alreadyApplied {
			ls.config.Logger.Debug(
				"dropping precomputed stake rewards: already written",
				"component", "ledger",
				"reward_snapshot_epoch", app.epochs.snapshot,
				"application_epoch", newEpoch,
				"captured_slot", capturedSlot,
				"boundary_slot", applicationBoundarySlot,
			)
			return nil
		}

		guardOK, err := stakeRewardPrecomputeSnapshotGuardOK(meta, metaTxn, app)
		if err != nil {
			return err
		}
		if !guardOK {
			ls.config.Logger.Debug(
				"dropping precomputed stake rewards: calculation inputs changed",
				"component", "ledger",
				"reward_snapshot_epoch", app.epochs.snapshot,
				"application_epoch", newEpoch,
				"captured_slot", capturedSlot,
				"boundary_slot", applicationBoundarySlot,
			)
			return nil
		}

		return ls.saveStakeRewardPrecompute(
			meta,
			metaTxn,
			app,
			newEpoch,
			capturedSlot,
			applicationBoundarySlot,
		)
	})
}

// stakeRewardPrecomputeSnapshotGuardOK reports whether every rollback-sensitive
// input consumed by an already-computed stakeRewardApplication is still valid.
// The rollback generation covers performance blocks, ADA pots, protocol state,
// and account certificate history; the row comparison additionally detects a
// direct replacement of the owning Mark snapshot. A mismatch means the
// application must be dropped rather than persisted; the synchronous boundary
// path recomputes authoritatively when it runs.
func stakeRewardPrecomputeSnapshotGuardOK(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	app *stakeRewardApplication,
) (bool, error) {
	if app == nil {
		return false, nil
	}
	if app.rewardInputGenerationSource != nil &&
		(app.rewardInputRollbackActive == nil ||
			app.rewardInputRollbackActive.Load() != 0 ||
			app.rewardInputGenerationSource.Load() != app.rewardInputGeneration) {
		return false, nil
	}
	snapshot, err := meta.GetRewardSnapshot(app.epochs.snapshot, "mark", metaTxn)
	if err != nil {
		return false, fmt.Errorf(
			"re-check reward snapshot for epoch %d: %w",
			app.epochs.snapshot,
			err,
		)
	}
	if snapshot == nil {
		return false, nil
	}
	if snapshot.CapturedSlot != app.snapshotCapturedSlot ||
		snapshot.BoundarySlot != app.snapshotBoundarySlot ||
		snapshot.TotalActiveStake != app.snapshotTotalActiveStake ||
		snapshot.TotalPoolCount != app.snapshotTotalPoolCount ||
		snapshot.TotalDelegators != app.snapshotTotalDelegators ||
		snapshot.ProtocolVersion != app.snapshotProtocolVersion ||
		!bytes.Equal(snapshot.EpochNonce, app.snapshotEpochNonce) {
		return false, nil
	}
	return true, nil
}

// precomputeStakeRewardsCalculate performs the read-only portion of stake
// reward precomputation: checking whether a valid precomputed application
// already exists for newEpoch and, if not, computing one. It issues no
// writes and is safe to run inside a read-only transaction.
func (ls *LedgerState) precomputeStakeRewardsCalculate(
	txn *database.Txn,
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) (*stakeRewardApplication, bool, error) {
	if epochs, ok := stakeRewardEpochsForApplication(newEpoch); ok &&
		epochs.bootstrap {
		// Epoch 2 is deliberately synchronous: its empty Go distribution leaves
		// no output rows that can safely prove an async precompute survived a
		// rollback on the same epoch-1 pots row.
		return nil, false, nil
	}
	if _, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	); err != nil || ok {
		return nil, false, err
	}
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		newEpoch,
		capturedSlot,
		boundarySlot,
	)
	if err != nil || !ok {
		return nil, false, err
	}
	if app == nil {
		return nil, false, errors.New("missing stake reward application")
	}
	return app, true, nil
}

// saveStakeRewardPrecompute persists a computed stakeRewardApplication as a
// precompute result: the reward output rows plus the updated RewardAdaPots
// rewards total. Callers must run it inside a write transaction.
func (ls *LedgerState) saveStakeRewardPrecompute(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	app *stakeRewardApplication,
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) error {
	if err := saveStakeRewardOutputs(meta, metaTxn, app); err != nil {
		return err
	}
	app.pots.Rewards = types.Uint64(app.totalRewardPot)
	if err := meta.SaveRewardAdaPots(app.pots, metaTxn); err != nil {
		return fmt.Errorf("save precomputed reward ADA pots: %w", err)
	}
	ls.config.Logger.Info(
		"precomputed stake rewards",
		"component", "ledger",
		"reward_snapshot_epoch", app.epochs.snapshot,
		"performance_epoch", app.epochs.performance,
		"pots_epoch", app.epochs.pots,
		"application_epoch", newEpoch,
		"captured_slot", capturedSlot,
		"boundary_slot", boundarySlot,
		"total_reward_pot", app.totalRewardPot,
		"available_rewards", app.availableRewards,
		"effective_rewards", app.effectiveRewards,
		"undistributed_rewards", app.undistributed,
		"unspendable_rewards", app.unspendable,
	)
	return nil
}

// precomputeStakeRewards computes and immediately persists stake rewards for
// newEpoch within the given transaction. It is kept for callers that already
// hold a single read-write transaction spanning both phases; the async
// EventBus path (precomputeStakeRewardsAfterEpochTransition) instead splits
// the same two steps (precomputeStakeRewardsCalculate,
// saveStakeRewardPrecompute) across separate read-only and write
// transactions so it never holds SQLite's single writer for the full
// calculation.
//
//nolint:unused // Used by tests
func (ls *LedgerState) precomputeStakeRewards(
	txn *database.Txn,
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) error {
	app, ok, err := ls.precomputeStakeRewardsCalculate(
		txn,
		newEpoch,
		capturedSlot,
		boundarySlot,
	)
	if err != nil || !ok {
		return err
	}
	if app == nil {
		return errors.New("missing stake reward application")
	}
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()
	return ls.saveStakeRewardPrecompute(
		meta,
		metaTxn,
		app,
		newEpoch,
		capturedSlot,
		boundarySlot,
	)
}

func saveStakeRewardOutputs(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	app *stakeRewardApplication,
) error {
	if app == nil {
		return errors.New("missing stake reward application")
	}
	if err := meta.DeleteRewardOutputsForEpoch(app.epochs.snapshot, metaTxn); err != nil {
		return fmt.Errorf(
			"replace reward outputs for epoch %d: %w",
			app.epochs.snapshot,
			err,
		)
	}
	if err := meta.SaveRewardPoolOutputs(app.poolOutputs, metaTxn); err != nil {
		return fmt.Errorf("save reward pool outputs: %w", err)
	}
	if err := meta.SaveRewardAccountOutputs(app.accountOutputs, metaTxn); err != nil {
		return fmt.Errorf("save reward account outputs: %w", err)
	}
	return nil
}

func deriveStakeRewardApplicationTotals(app *stakeRewardApplication) error {
	if app == nil || app.pots == nil {
		return errors.New("missing stake reward application")
	}
	totalRewardPot := app.totalRewardPot
	fees := uint64(app.pots.Fees)
	if totalRewardPot < fees {
		return fmt.Errorf(
			"invalid precomputed reward pot %d below fees %d",
			totalRewardPot,
			fees,
		)
	}
	availableRewards, err := rewardAvailableRewards(
		app.totalRewardPot,
		app.params,
	)
	if err != nil {
		return err
	}
	app.availableRewards = availableRewards
	app.effectiveRewards = 0
	app.unspendable = 0
	for _, output := range app.accountOutputs {
		if output == nil {
			continue
		}
		amount := uint64(output.Amount)
		var overflow bool
		if output.Spendable {
			app.effectiveRewards, overflow = addRewardUint64(app.effectiveRewards, amount)
		} else {
			app.unspendable, overflow = addRewardUint64(app.unspendable, amount)
		}
		if overflow {
			return errors.New("stake reward output total overflow")
		}
	}
	accounted, overflow := addRewardUint64(app.effectiveRewards, app.unspendable)
	if overflow || accounted > app.availableRewards {
		return fmt.Errorf(
			"precomputed rewards exceed available pot: effective=%d unspendable=%d available=%d",
			app.effectiveRewards,
			app.unspendable,
			app.availableRewards,
		)
	}
	app.undistributed = app.availableRewards - accounted
	return nil
}

func stakeRewardUpdatedPots(app *stakeRewardApplication) (uint64, uint64, error) {
	if app == nil || app.pots == nil {
		return 0, 0, errors.New("missing stake reward application")
	}
	if err := deriveStakeRewardApplicationTotals(app); err != nil {
		return 0, 0, err
	}
	incentives := app.totalRewardPot - uint64(app.pots.Fees)
	if uint64(app.pots.Reserves) < incentives {
		return 0, 0, fmt.Errorf(
			"stake reward incentives %d exceed reserves %d",
			incentives,
			app.pots.Reserves,
		)
	}
	reserves := uint64(app.pots.Reserves) - incentives
	var overflow bool
	reserves, overflow = addRewardUint64(reserves, app.undistributed)
	if overflow {
		return 0, 0, errors.New("stake reward reserves overflow")
	}
	treasuryTax, err := rewardFloorMul(
		app.params.TreasuryExpansion,
		app.totalRewardPot,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("stake reward treasury tax: %w", err)
	}
	treasury, overflow := addRewardUint64(uint64(app.pots.Treasury), treasuryTax)
	if overflow {
		return 0, 0, errors.New("stake reward treasury tax overflow")
	}
	treasury, overflow = addRewardUint64(treasury, app.unspendable)
	if overflow {
		return 0, 0, errors.New("stake reward treasury overflow")
	}
	return reserves, treasury, nil
}

func rewardFloorMul(r *big.Rat, amount uint64) (uint64, error) {
	if r == nil || r.Sign() <= 0 || amount == 0 {
		return 0, nil
	}
	value := new(big.Rat).Mul(
		new(big.Rat).Set(r),
		new(big.Rat).SetInt(new(big.Int).SetUint64(amount)),
	)
	num := new(big.Int).Set(value.Num())
	q := new(big.Int).Quo(num, value.Denom())
	if !q.IsUint64() {
		return 0, fmt.Errorf("reward multiplication overflow: floor %s", q.String())
	}
	return q.Uint64(), nil
}

func addRewardUint64(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return 0, true
	}
	return a + b, false
}

type stakeRewardEpochs struct {
	snapshot    uint64
	performance uint64
	pots        uint64
	bootstrap   bool
}

func stakeRewardEpochsForApplication(newEpoch uint64) (stakeRewardEpochs, bool) {
	// The first RUPD calculation is made during epoch 1 from epoch 0's block
	// performance and the epoch 1 ADA pots. The Go stake distribution is
	// still empty, so the epoch 2 NEWEPOCH rule applies monetary expansion
	// and treasury tax but distributes no pool or account rewards.
	if newEpoch == 2 {
		return stakeRewardEpochs{
			snapshot:    0,
			performance: 0,
			pots:        1,
			bootstrap:   true,
		}, true
	}
	return stakeRewardEpochsForNewEpoch(newEpoch)
}

func stakeRewardEpochsForNewEpoch(newEpoch uint64) (stakeRewardEpochs, bool) {
	if newEpoch < 3 {
		return stakeRewardEpochs{}, false
	}
	// At the boundary into epoch N, cardano-ledger's NEWEPOCH rule applies
	// the reward update that was computed during epoch N-1 before that
	// epoch's SNAP rotation. In Dingo's persisted model the pre-SNAP Go
	// snapshot is the mark snapshot captured for epoch N-3. The completed
	// block-performance epoch is N-2, and the ADA pots row captured at the
	// boundary into N-1 carries the reserves/treasury state plus fees for
	// epoch N-2 that seed the delayed calculation.
	return stakeRewardEpochs{
		snapshot:    newEpoch - 3,
		performance: newEpoch - 2,
		pots:        newEpoch - 1,
	}, true
}

func suppressBootstrapStakeRewards(result *rewards.Result) {
	if result == nil {
		return
	}
	result.PoolRewards = nil
	result.AccountRewards = nil
	result.EffectiveRewards = 0
	result.Unspendable = 0
	result.Undistributed = result.AvailableRewards
}

func (ls *LedgerState) saveRewardAdaPotsForEpoch(
	txn *database.Txn,
	newEpoch uint64,
	endedEpoch models.Epoch,
	boundarySlot uint64,
) error {
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()
	fees, err := rewardEpochFees(meta, metaTxn, endedEpoch)
	if err != nil {
		return fmt.Errorf("sum reward fees for epoch %d: %w", endedEpoch.EpochId, err)
	}
	state, err := meta.GetNetworkState(metaTxn)
	if err != nil {
		return fmt.Errorf("get network state for reward ADA pots: %w", err)
	}
	var treasury, reserves uint64
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	if err := meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        newEpoch,
		Treasury:     types.Uint64(treasury),
		Reserves:     types.Uint64(reserves),
		Fees:         types.Uint64(fees),
		CapturedSlot: boundarySlot,
	}, metaTxn); err != nil {
		return fmt.Errorf("save reward ADA pots for epoch %d: %w", newEpoch, err)
	}
	return nil
}

// minPoolMarginRat converts a CIP-23 minPoolMargin basis-points value to a
// rational in [0, 1], returning nil when the value is 0 (feature disabled).
// 150 bp => 150/10000. Uses SetUint64 (not int64(basisPoints)) to avoid a
// gosec G115 uint->int64 conversion finding.
func minPoolMarginRat(basisPoints uint) *big.Rat {
	if basisPoints == 0 {
		return nil
	}
	return new(big.Rat).SetFrac(
		new(big.Int).SetUint64(uint64(basisPoints)),
		big.NewInt(10_000),
	)
}

// applyMinPoolMarginConfig overlays the CIP-23 minimum pool margin operator
// setting onto the reward parameters. It sets params.MinPoolMargin only when the
// configured value is nonzero AND the calculation is for Dijkstra or later
// (protocol major version >= dijkstra.MinProtocolVersionDijkstra); otherwise it
// leaves the field nil so pre-Dijkstra reward calculation is byte-for-byte
// unchanged. This is the reward-path half of the whole-feature Dijkstra+ gate;
// the certificate half is that checkPoolMarginFloor is wired only into
// ValidateTxDijkstra.
func applyMinPoolMarginConfig(params *rewards.Parameters, cfg LedgerStateConfig) {
	if cfg.MinPoolMargin == 0 {
		return
	}
	if params.ProtocolMajorVersion < dijkstra.MinProtocolVersionDijkstra {
		return
	}
	params.MinPoolMargin = minPoolMarginRat(cfg.MinPoolMargin)
}

// MinPoolMargin returns the CIP-23 minimum pool margin as a rational in [0, 1],
// or nil when disabled (config value 0). It satisfies the eras package
// MinPoolMarginProvider interface used by the Dijkstra pool-margin-floor
// certificate rule; the Dijkstra-only era gate is inherent because only
// ValidateTxDijkstra consults it.
func (ls *LedgerState) MinPoolMargin() *big.Rat {
	return minPoolMarginRat(ls.config.MinPoolMargin)
}

func (ls *LedgerState) rewardParameters(
	txn *database.Txn,
	performanceEpoch uint64,
	calculationEpoch uint64,
	pots *models.RewardAdaPots,
) (lcommon.ProtocolParameters, rewards.Parameters, *big.Rat, error) {
	performanceEpochRow, err := ls.db.Metadata().GetEpoch(
		performanceEpoch,
		txn.Metadata(),
	)
	if err != nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"get performance epoch %d: %w",
			performanceEpoch, err,
		)
	}
	if performanceEpochRow == nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"performance epoch %d not found",
			performanceEpoch,
		)
	}
	performanceEraDesc, ok := ls.eraById(performanceEpochRow.EraId)
	if !ok || performanceEraDesc == nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"unknown era ID %d for reward performance epoch %d",
			performanceEpochRow.EraId, performanceEpoch,
		)
	}
	performancePParams, err := ls.db.GetPParams(
		performanceEpoch,
		performanceEraDesc.Id,
		performanceEraDesc.DecodePParamsFunc,
		txn,
	)
	if err != nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"get pparams for reward performance epoch %d: %w",
			performanceEpoch, err,
		)
	}
	if performancePParams == nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"missing pparams for reward performance epoch %d",
			performanceEpoch,
		)
	}
	performanceDecentralization, err := rewardDecentralizationFromPParams(
		performancePParams,
	)
	if err != nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"get decentralization for reward performance epoch %d: %w",
			performanceEpoch, err,
		)
	}
	calculationEpochRow, err := ls.db.Metadata().GetEpoch(
		calculationEpoch,
		txn.Metadata(),
	)
	if err != nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"get reward calculation epoch %d: %w",
			calculationEpoch,
			err,
		)
	}
	if calculationEpochRow == nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"reward calculation epoch %d not found",
			calculationEpoch,
		)
	}
	eraDesc, ok := ls.eraById(calculationEpochRow.EraId)
	if !ok || eraDesc == nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"unknown era ID %d for reward calculation epoch %d",
			calculationEpochRow.EraId, calculationEpoch,
		)
	}
	pparams, err := ls.db.GetPParams(
		calculationEpoch,
		eraDesc.Id,
		eraDesc.DecodePParamsFunc,
		txn,
	)
	if err != nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"get pparams for reward calculation epoch %d: %w",
			calculationEpoch, err,
		)
	}
	if pparams == nil {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"missing pparams for reward calculation epoch %d",
			calculationEpoch,
		)
	}
	params, err := rewardParametersFromPParams(
		pparams,
		ls.config.CardanoNodeConfig,
		uint64(calculationEpochRow.LengthInSlots),
	)
	if err != nil {
		return nil, rewards.Parameters{}, nil, err
	}
	// CIP-23: overlay the operator-configured minimum pool margin, gated to
	// Dijkstra and later. Single chokepoint feeding both the boundary apply and
	// the async precompute, so both agree.
	applyMinPoolMarginConfig(&params, ls.config)
	if params.MaxLovelaceSupply < uint64(pots.Reserves) {
		return nil, rewards.Parameters{}, nil, fmt.Errorf(
			"invalid reward pots: reserves %d exceed max supply %d",
			pots.Reserves,
			params.MaxLovelaceSupply,
		)
	}
	return pparams, params, performanceDecentralization, nil
}

func (ls *LedgerState) rewardBlockCounts(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	performanceEpoch uint64,
	poolInputs []*models.RewardPoolInput,
	decentralization *big.Rat,
) (map[string]uint64, uint64, error) {
	epoch, err := meta.GetEpoch(performanceEpoch, metaTxn)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get reward block-count epoch %d: %w",
			performanceEpoch, err,
		)
	}
	if epoch == nil || epoch.LengthInSlots == 0 {
		return nil, 0, nil
	}
	startSlot := epoch.StartSlot
	endSlot := startSlot + uint64(epoch.LengthInSlots) - 1
	poolKeys := make([]lcommon.PoolKeyHash, 0, len(poolInputs))
	for _, input := range poolInputs {
		if input == nil || len(input.PoolKeyHash) != rewards.CredentialHashSize {
			continue
		}
		var poolKey lcommon.PoolKeyHash
		copy(poolKey[:], input.PoolKeyHash)
		poolKeys = append(poolKeys, poolKey)
	}
	if len(poolKeys) == 0 {
		return nil, 0, nil
	}
	if decentralization != nil && decentralization.Sign() > 0 {
		return rewardBlockCountsExcludingOverlaySlots(
			meta,
			metaTxn,
			poolKeys,
			startSlot,
			endSlot,
			decentralization,
		)
	}
	counts, total, err := meta.CountPoolBlocksInSlotRange(
		poolKeys,
		startSlot,
		endSlot,
		metaTxn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count reward pool blocks in epoch %d: %w",
			performanceEpoch, err,
		)
	}
	return counts, total, nil
}

func rewardBlockCountsExcludingOverlaySlots(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	poolKeys []lcommon.PoolKeyHash,
	startSlot uint64,
	endSlot uint64,
	decentralization *big.Rat,
) (map[string]uint64, uint64, error) {
	counts := make(map[string]uint64, len(poolKeys))
	for _, poolKey := range poolKeys {
		counts[string(poolKey[:])] = 0
	}
	rows, err := meta.GetPoolBlockIssuersInSlotRange(
		startSlot,
		endSlot,
		metaTxn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get reward pool block issuers: %w", err)
	}
	var total uint64
	for _, row := range rows {
		if rewardIsOverlaySlot(startSlot, decentralization, row.Slot) {
			continue
		}
		total++
		key := string(row.PoolKeyHash)
		if _, ok := counts[key]; ok {
			counts[key]++
		}
	}
	return counts, total, nil
}

func rewardIsOverlaySlot(
	firstSlot uint64,
	decentralization *big.Rat,
	slot uint64,
) bool {
	if decentralization == nil || decentralization.Sign() <= 0 || slot < firstSlot {
		return false
	}
	s := slot - firstSlot
	return rewardOverlayStep(s, decentralization).Cmp(
		rewardOverlayStep(s+1, decentralization),
	) < 0
}

func rewardOverlayStep(offset uint64, decentralization *big.Rat) *big.Int {
	value := new(big.Rat).Mul(
		new(big.Rat).SetInt(new(big.Int).SetUint64(offset)),
		decentralization,
	)
	q, r := new(big.Int).QuoRem(
		new(big.Int).Set(value.Num()),
		value.Denom(),
		new(big.Int),
	)
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q
}

func (ls *LedgerState) rewardPrefilterSlot(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	potsEpoch uint64,
) (uint64, error) {
	epoch, err := meta.GetEpoch(potsEpoch, metaTxn)
	if err != nil {
		return 0, fmt.Errorf(
			"get reward prefilter epoch %d: %w",
			potsEpoch, err,
		)
	}
	if epoch == nil {
		return 0, fmt.Errorf("reward prefilter epoch %d not found", potsEpoch)
	}
	if epoch.LengthInSlots == 0 {
		return epoch.StartSlot, nil
	}
	endSlot := epoch.StartSlot + uint64(epoch.LengthInSlots) - 1
	stabilityWindow := ls.rewardUpdateStabilityWindow()
	prefilterSlot := epoch.StartSlot
	if stabilityWindow > 0 {
		if stabilityWindow >= math.MaxUint64-epoch.StartSlot {
			prefilterSlot = endSlot
		} else {
			prefilterSlot = epoch.StartSlot + stabilityWindow + 1
		}
	}
	if prefilterSlot > endSlot {
		prefilterSlot = endSlot
	}
	return prefilterSlot, nil
}

// rewardUpdateStabilityWindow returns the randomness stabilisation window used
// by cardano-ledger's RUPD rule. This is deliberately separate from
// nonceStabilityWindow: historical eras can use a 3k/f nonce cutoff while RUPD
// starts after the 4k/f randomness window.
func (ls *LedgerState) rewardUpdateStabilityWindow() uint64 {
	if ls.config.CardanoNodeConfig == nil {
		return 0
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0
	}
	k := shelleyGenesis.SecurityParam
	if k <= 0 {
		return 0
	}
	activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil ||
		activeSlotsCoeff.Num().Sign() <= 0 {
		return 0
	}
	numerator := new(big.Int).SetInt64(int64(k))
	numerator.Mul(numerator, big.NewInt(4))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	denominator := new(big.Int).Set(activeSlotsCoeff.Num())
	result := new(big.Int).Div(numerator, denominator)
	remainder := new(big.Int).Mod(numerator, denominator)
	if remainder.Sign() != 0 {
		result.Add(result, big.NewInt(1))
	}
	if !result.IsUint64() {
		return 0
	}
	return result.Uint64()
}

func (ls *LedgerState) rewardCalculatorSnapshot(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	snapshot *models.RewardSnapshot,
	poolInputs []*models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
	blockCounts map[string]uint64,
	totalBlocks uint64,
	prefilterSlot uint64,
	requiresRewardPrefilter bool,
) (rewards.Snapshot, error) {
	if err := validateRewardCalculatorInputs(
		snapshot,
		poolInputs,
		stakeInputs,
	); err != nil {
		return rewards.Snapshot{}, err
	}
	activeAccounts, err := rewardActiveAccounts(
		meta,
		metaTxn,
		poolInputs,
		stakeInputs,
	)
	if err != nil {
		return rewards.Snapshot{}, err
	}
	prefilterAccounts, err := rewardPrefilterAccounts(
		meta,
		metaTxn,
		poolInputs,
		stakeInputs,
		prefilterSlot,
		requiresRewardPrefilter,
		activeAccounts,
	)
	if err != nil {
		return rewards.Snapshot{}, err
	}
	stakeByPool := make(map[string][]*models.RewardStakeInput)
	for _, input := range stakeInputs {
		if input == nil {
			continue
		}
		stakeByPool[string(input.PoolKeyHash)] = append(
			stakeByPool[string(input.PoolKeyHash)],
			input,
		)
	}

	ret := rewards.Snapshot{
		TotalActiveStake: uint64(snapshot.TotalActiveStake),
		Pools:            make([]rewards.Pool, 0, len(poolInputs)),
	}
	for _, input := range poolInputs {
		if input == nil {
			continue
		}
		pool, err := rewardPoolFromInput(
			input,
			stakeByPool[string(input.PoolKeyHash)],
			activeAccounts,
			prefilterAccounts,
			blockCounts[string(input.PoolKeyHash)],
			totalBlocks,
		)
		if err != nil {
			return rewards.Snapshot{}, err
		}
		ret.Pools = append(ret.Pools, pool)
	}
	return ret, nil
}

func validateRewardCalculatorInputs(
	snapshot *models.RewardSnapshot,
	poolInputs []*models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
) error {
	if snapshot == nil {
		return errors.New("missing reward snapshot")
	}
	if uint64(len(poolInputs)) != snapshot.TotalPoolCount {
		return fmt.Errorf(
			"reward pool input count %d does not match snapshot pool count %d",
			len(poolInputs),
			snapshot.TotalPoolCount,
		)
	}
	poolStakeByKey := make(map[string]uint64, len(poolInputs))
	poolOwnerStakeByKey := make(map[string]uint64, len(poolInputs))
	poolDelegatorCountByKey := make(map[string]uint64, len(poolInputs))
	poolLabelByKey := make(map[string]string, len(poolInputs))
	var totalPoolStake uint64
	var totalDelegators uint64
	for _, input := range poolInputs {
		if input == nil {
			return errors.New("nil reward pool input")
		}
		poolID, err := rewards.NewPoolID(input.PoolKeyHash)
		if err != nil {
			return fmt.Errorf("invalid reward pool input pool key: %w", err)
		}
		if input.CapturedSlot != snapshot.CapturedSlot {
			return fmt.Errorf(
				"reward pool input captured slot %d does not match snapshot captured slot %d for pool %s",
				input.CapturedSlot,
				snapshot.CapturedSlot,
				poolID.String(),
			)
		}
		if input.BoundarySlot != snapshot.BoundarySlot {
			return fmt.Errorf(
				"reward pool input boundary slot %d does not match snapshot boundary slot %d for pool %s",
				input.BoundarySlot,
				snapshot.BoundarySlot,
				poolID.String(),
			)
		}
		if input.Margin == nil || input.Margin.Rat == nil {
			return fmt.Errorf(
				"reward pool input margin is missing for pool %s",
				poolID.String(),
			)
		}
		if input.Margin.Sign() < 0 ||
			input.Margin.Cmp(big.NewRat(1, 1)) > 0 {
			return fmt.Errorf(
				"reward pool input margin outside [0,1] for pool %s",
				poolID.String(),
			)
		}
		if _, err := rewards.NewCredential(
			input.RewardAccountCredentialTag,
			input.RewardAccount,
		); err != nil {
			return fmt.Errorf(
				"invalid reward pool input reward account for pool %s: %w",
				poolID.String(),
				err,
			)
		}
		if input.OwnerStake > input.DelegatedStake {
			return fmt.Errorf(
				"reward pool input owner stake %d exceeds delegated stake %d for pool %s",
				input.OwnerStake,
				input.DelegatedStake,
				poolID.String(),
			)
		}
		key := string(input.PoolKeyHash)
		if _, ok := poolStakeByKey[key]; ok {
			return fmt.Errorf(
				"duplicate reward pool input for pool %s",
				poolID.String(),
			)
		}
		stake := uint64(input.DelegatedStake)
		poolStakeByKey[key] = stake
		poolOwnerStakeByKey[key] = uint64(input.OwnerStake)
		poolDelegatorCountByKey[key] = input.DelegatorCount
		poolLabelByKey[key] = poolID.String()
		var overflow bool
		totalPoolStake, overflow = addRewardUint64(totalPoolStake, stake)
		if overflow {
			return errors.New("reward pool input delegated stake overflow")
		}
		totalDelegators, overflow = addRewardUint64(
			totalDelegators,
			input.DelegatorCount,
		)
		if overflow {
			return errors.New("reward pool input delegator count overflow")
		}
	}
	if totalPoolStake != uint64(snapshot.TotalActiveStake) {
		return fmt.Errorf(
			"reward pool input total delegated stake %d does not match snapshot active stake %d",
			totalPoolStake,
			uint64(snapshot.TotalActiveStake),
		)
	}
	if totalDelegators != snapshot.TotalDelegators {
		return fmt.Errorf(
			"reward pool input total delegator count %d does not match snapshot delegator count %d",
			totalDelegators,
			snapshot.TotalDelegators,
		)
	}

	stakeByPool := make(map[string]uint64, len(poolStakeByKey))
	ownerStakeByPool := make(map[string]uint64, len(poolStakeByKey))
	delegatorCountByPool := make(map[string]uint64, len(poolStakeByKey))
	for _, input := range stakeInputs {
		if input == nil {
			return errors.New("nil reward stake input")
		}
		poolID, err := rewards.NewPoolID(input.PoolKeyHash)
		if err != nil {
			return fmt.Errorf("invalid reward stake input pool key: %w", err)
		}
		credential, err := rewards.NewCredential(
			input.CredentialTag,
			input.StakingKey,
		)
		if err != nil {
			return fmt.Errorf(
				"invalid reward stake input credential for pool %s: %w",
				poolID.String(),
				err,
			)
		}
		if input.CapturedSlot != snapshot.CapturedSlot {
			return fmt.Errorf(
				"reward stake input captured slot %d does not match snapshot captured slot %d for pool %s credential %x",
				input.CapturedSlot,
				snapshot.CapturedSlot,
				poolID.String(),
				credential.Hash,
			)
		}
		if input.BoundarySlot != snapshot.BoundarySlot {
			return fmt.Errorf(
				"reward stake input boundary slot %d does not match snapshot boundary slot %d for pool %s credential %x",
				input.BoundarySlot,
				snapshot.BoundarySlot,
				poolID.String(),
				credential.Hash,
			)
		}
		key := string(input.PoolKeyHash)
		if _, ok := poolStakeByKey[key]; !ok {
			return fmt.Errorf(
				"reward stake input for unknown pool %s credential %x",
				poolID.String(),
				credential.Hash,
			)
		}
		if input.Stake == 0 {
			return fmt.Errorf(
				"reward stake input for pool %s credential %x has zero stake",
				poolID.String(),
				credential.Hash,
			)
		}
		var overflow bool
		stakeByPool[key], overflow = addRewardUint64(
			stakeByPool[key],
			uint64(input.Stake),
		)
		if overflow {
			return fmt.Errorf(
				"reward stake input total overflow for pool %s",
				poolID.String(),
			)
		}
		if input.Owner {
			ownerStakeByPool[key], overflow = addRewardUint64(
				ownerStakeByPool[key],
				uint64(input.Stake),
			)
			if overflow {
				return fmt.Errorf(
					"reward owner stake input total overflow for pool %s",
					poolID.String(),
				)
			}
		}
		if delegatorCountByPool[key] == math.MaxUint64 {
			return fmt.Errorf(
				"reward stake input delegator count overflow for pool %s",
				poolID.String(),
			)
		}
		delegatorCountByPool[key]++
	}
	for key, expectedStake := range poolStakeByKey {
		actualStake := stakeByPool[key]
		if actualStake != expectedStake {
			return fmt.Errorf(
				"reward stake input total mismatch for pool %s: inputs=%d pool=%d",
				poolLabelByKey[key],
				actualStake,
				expectedStake,
			)
		}
	}
	for key, expectedOwnerStake := range poolOwnerStakeByKey {
		actualOwnerStake := ownerStakeByPool[key]
		if actualOwnerStake != expectedOwnerStake {
			return fmt.Errorf(
				"reward owner stake input total mismatch for pool %s: inputs=%d pool=%d",
				poolLabelByKey[key],
				actualOwnerStake,
				expectedOwnerStake,
			)
		}
	}
	for key, expectedDelegators := range poolDelegatorCountByKey {
		actualDelegators := delegatorCountByPool[key]
		if actualDelegators != expectedDelegators {
			return fmt.Errorf(
				"reward stake input delegator count mismatch for pool %s: inputs=%d pool=%d",
				poolLabelByKey[key],
				actualDelegators,
				expectedDelegators,
			)
		}
	}
	return nil
}

func rewardPoolFromInput(
	input *models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
	activeAccounts map[string]struct{},
	prefilterAccounts map[string]struct{},
	blocksProduced uint64,
	totalBlocks uint64,
) (rewards.Pool, error) {
	poolID, err := rewards.NewPoolID(input.PoolKeyHash)
	if err != nil {
		return rewards.Pool{}, err
	}
	rewardAccount, err := rewards.NewCredential(
		input.RewardAccountCredentialTag,
		input.RewardAccount,
	)
	if err != nil {
		return rewards.Pool{}, fmt.Errorf(
			"pool %s reward account: %w",
			poolID.String(), err,
		)
	}
	pool := rewards.Pool{
		ID:                      poolID,
		RewardAccount:           rewardAccount,
		Margin:                  ratOrZero(input.Margin),
		Pledge:                  uint64(input.Pledge),
		Cost:                    uint64(input.Cost),
		DelegatedStake:          uint64(input.DelegatedStake),
		OwnerStake:              uint64(input.OwnerStake),
		BlocksProduced:          blocksProduced,
		TotalBlocks:             totalBlocks,
		RewardAccountRegistered: rewardCredentialActive(rewardAccount, prefilterAccounts),
		RewardAccountEligible:   rewardCredentialActive(rewardAccount, activeAccounts),
		Owners:                  make(map[rewards.Credential]struct{}),
		Delegators:              make([]rewards.Delegator, 0, len(stakeInputs)),
	}
	for _, input := range stakeInputs {
		credential, err := rewards.NewCredential(
			input.CredentialTag,
			input.StakingKey,
		)
		if err != nil {
			return rewards.Pool{}, err
		}
		if input.Owner {
			pool.Owners[credential] = struct{}{}
		}
		pool.Delegators = append(pool.Delegators, rewards.Delegator{
			Credential: credential,
			Stake:      uint64(input.Stake),
			Registered: rewardCredentialActive(credential, prefilterAccounts),
			Eligible:   rewardCredentialActive(credential, activeAccounts),
		})
	}
	return pool, nil
}

func rewardActiveAccounts(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	poolInputs []*models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
) (map[string]struct{}, error) {
	refs := rewardCredentialRefs(poolInputs, stakeInputs)
	accounts, err := meta.GetAccountsByCredential(refs, false, metaTxn)
	if err != nil {
		return nil, fmt.Errorf("get active reward accounts: %w", err)
	}
	ret := make(map[string]struct{}, len(accounts))
	for key := range accounts {
		ret[key] = struct{}{}
	}
	return ret, nil
}

type rewardAccountHistoryReader interface {
	GetAccountsActiveAtSlot(
		[]models.StakeCredentialRef,
		uint64,
		types.Txn,
	) (map[string]struct{}, error)
}

func rewardPrefilterAccounts(
	meta rewardAccountHistoryReader,
	metaTxn types.Txn,
	poolInputs []*models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
	prefilterSlot uint64,
	requiresRewardPrefilter bool,
	activeAccounts map[string]struct{},
) (map[string]struct{}, error) {
	if !requiresRewardPrefilter {
		return activeAccounts, nil
	}
	refs := rewardCredentialRefs(poolInputs, stakeInputs)
	querySlot := uint64(0)
	if prefilterSlot > 0 {
		querySlot = prefilterSlot - 1
	}
	accounts, err := meta.GetAccountsActiveAtSlot(refs, querySlot, metaTxn)
	if err != nil {
		return nil, fmt.Errorf(
			"get reward prefilter accounts before slot %d: %w",
			prefilterSlot,
			err,
		)
	}
	return accounts, nil
}

func rewardCredentialRefs(
	poolInputs []*models.RewardPoolInput,
	stakeInputs []*models.RewardStakeInput,
) []models.StakeCredentialRef {
	refsByKey := make(map[string]models.StakeCredentialRef)
	for _, input := range poolInputs {
		if input == nil || len(input.RewardAccount) != rewards.CredentialHashSize {
			continue
		}
		ref := models.NewStakeCredentialRef(
			input.RewardAccountCredentialTag,
			input.RewardAccount,
		)
		refsByKey[ref.MapKey()] = ref
	}
	for _, input := range stakeInputs {
		if input == nil || len(input.StakingKey) != rewards.CredentialHashSize {
			continue
		}
		ref := models.NewStakeCredentialRef(
			input.CredentialTag,
			input.StakingKey,
		)
		refsByKey[ref.MapKey()] = ref
	}
	refs := make([]models.StakeCredentialRef, 0, len(refsByKey))
	for _, ref := range refsByKey {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].MapKey() < refs[j].MapKey()
	})
	return refs
}

func rewardPoolOutputs(
	epoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
	poolRewards []rewards.PoolReward,
) []*models.RewardPoolOutput {
	ret := make([]*models.RewardPoolOutput, 0, len(poolRewards))
	for _, reward := range poolRewards {
		poolKeyHash := make([]byte, rewards.CredentialHashSize)
		copy(poolKeyHash, reward.PoolID[:])
		ret = append(ret, &models.RewardPoolOutput{
			Epoch:               epoch,
			PoolKeyHash:         poolKeyHash,
			ApparentPerformance: rewardRat(reward.ApparentPerformance),
			OptimalReward:       types.Uint64(reward.OptimalReward),
			TotalReward:         types.Uint64(reward.PoolReward),
			LeaderReward:        types.Uint64(reward.LeaderReward),
			MemberRewardTotal:   types.Uint64(reward.MemberRewardTotal),
			OwnerStake:          types.Uint64(reward.OwnerStake),
			Undistributed:       types.Uint64(reward.Undistributed),
			Unspendable:         types.Uint64(reward.Unspendable),
			CapturedSlot:        capturedSlot,
			BoundarySlot:        boundarySlot,
		})
	}
	return ret
}

func rewardAccountOutputs(
	epoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
	accountRewards []rewards.AccountReward,
) []*models.RewardAccountOutput {
	ret := make([]*models.RewardAccountOutput, 0, len(accountRewards))
	for _, reward := range accountRewards {
		stakingKey := make([]byte, rewards.CredentialHashSize)
		copy(stakingKey, reward.Credential.Hash[:])
		poolKeyHash := make([]byte, rewards.CredentialHashSize)
		copy(poolKeyHash, reward.PoolID[:])
		ret = append(ret, &models.RewardAccountOutput{
			Epoch:         epoch,
			CredentialTag: reward.Credential.Tag,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolKeyHash,
			RewardType:    string(reward.Type),
			Amount:        types.Uint64(reward.Amount),
			Spendable:     reward.Spendable,
			CapturedSlot:  capturedSlot,
			BoundarySlot:  boundarySlot,
		})
	}
	return ret
}

func validRewardPoolID(hash []byte) bool {
	_, err := rewards.NewPoolID(hash)
	return err == nil
}

func validRewardCredential(tag uint8, hash []byte) bool {
	_, err := rewards.NewCredential(tag, hash)
	return err == nil
}

func validRewardAccountOutput(output *models.RewardAccountOutput) bool {
	_, err := rewardFromAccountOutput(output)
	return err == nil
}

func rewardFromAccountOutput(
	output *models.RewardAccountOutput,
) (rewards.AccountReward, error) {
	if output == nil {
		return rewards.AccountReward{}, errors.New("nil reward account output")
	}
	credential, err := rewards.NewCredential(
		output.CredentialTag,
		output.StakingKey,
	)
	if err != nil {
		return rewards.AccountReward{}, fmt.Errorf(
			"reward output credential: %w",
			err,
		)
	}
	poolID, err := rewards.NewPoolID(output.PoolKeyHash)
	if err != nil {
		return rewards.AccountReward{}, fmt.Errorf(
			"reward output pool: %w",
			err,
		)
	}
	switch rewards.RewardType(output.RewardType) {
	case rewards.RewardTypeLeader, rewards.RewardTypeMember:
	default:
		return rewards.AccountReward{}, fmt.Errorf(
			"unknown reward output type %q",
			output.RewardType,
		)
	}
	return rewards.AccountReward{
		Credential: credential,
		PoolID:     poolID,
		Amount:     uint64(output.Amount),
		Type:       rewards.RewardType(output.RewardType),
		Spendable:  output.Spendable,
	}, nil
}

func rewardParametersFromPParams(
	pparams lcommon.ProtocolParameters,
	nodeConfig interface {
		ShelleyGenesis() *shelley.ShelleyGenesis
	},
	epochLength uint64,
) (rewards.Parameters, error) {
	if nodeConfig == nil || nodeConfig.ShelleyGenesis() == nil {
		return rewards.Parameters{}, errors.New("missing Shelley genesis")
	}
	genesis := nodeConfig.ShelleyGenesis()
	activeSlotsCoeff := genesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil {
		return rewards.Parameters{}, errors.New("missing active slots coefficient")
	}
	params := rewards.Parameters{
		Decentralization:  new(big.Rat),
		ActiveSlotsCoeff:  new(big.Rat).Set(activeSlotsCoeff),
		EpochLength:       epochLength,
		MaxLovelaceSupply: genesis.MaxLovelaceSupply,
	}
	if params.EpochLength == 0 {
		if genesis.EpochLength < 0 {
			return rewards.Parameters{}, fmt.Errorf(
				"invalid Shelley genesis epoch length %d",
				genesis.EpochLength,
			)
		}
		// #nosec G115 -- Shelley genesis epoch length is validated non-negative above.
		params.EpochLength = uint64(genesis.EpochLength)
	}
	pv, err := GetProtocolVersion(pparams)
	if err != nil {
		return rewards.Parameters{}, err
	}
	params.ProtocolMajorVersion = uint64(pv.Major)

	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		params.OptimalPoolCount = uint64(pp.NOpt)
		params.PledgeInfluence = cloneCBORRat(pp.A0)
		params.MonetaryExpansion = cloneCBORRat(pp.Rho)
		params.TreasuryExpansion = cloneCBORRat(pp.Tau)
		params.Decentralization = cloneCBORRat(pp.Decentralization)
	case *mary.MaryProtocolParameters:
		params.OptimalPoolCount = uint64(pp.NOpt)
		params.PledgeInfluence = cloneCBORRat(pp.A0)
		params.MonetaryExpansion = cloneCBORRat(pp.Rho)
		params.TreasuryExpansion = cloneCBORRat(pp.Tau)
		params.Decentralization = cloneCBORRat(pp.Decentralization)
	case *alonzo.AlonzoProtocolParameters:
		params.OptimalPoolCount = uint64(pp.NOpt)
		params.PledgeInfluence = cloneCBORRat(pp.A0)
		params.MonetaryExpansion = cloneCBORRat(pp.Rho)
		params.TreasuryExpansion = cloneCBORRat(pp.Tau)
		params.Decentralization = cloneCBORRat(pp.Decentralization)
	case *babbage.BabbageProtocolParameters:
		params.OptimalPoolCount = uint64(pp.NOpt)
		params.PledgeInfluence = cloneCBORRat(pp.A0)
		params.MonetaryExpansion = cloneCBORRat(pp.Rho)
		params.TreasuryExpansion = cloneCBORRat(pp.Tau)
	case *conway.ConwayProtocolParameters:
		params.OptimalPoolCount = uint64(pp.NOpt)
		params.PledgeInfluence = cloneCBORRat(pp.A0)
		params.MonetaryExpansion = cloneCBORRat(pp.Rho)
		params.TreasuryExpansion = cloneCBORRat(pp.Tau)
	case *dijkstra.DijkstraProtocolParameters:
		params.OptimalPoolCount = uint64(pp.NOpt)
		params.PledgeInfluence = cloneCBORRat(pp.A0)
		params.MonetaryExpansion = cloneCBORRat(pp.Rho)
		params.TreasuryExpansion = cloneCBORRat(pp.Tau)
	default:
		return rewards.Parameters{}, fmt.Errorf(
			"unsupported reward pparams type %T",
			pparams,
		)
	}
	if err := params.Validate(); err != nil {
		return rewards.Parameters{}, err
	}
	return params, nil
}

func rewardDecentralizationFromPParams(
	pparams lcommon.ProtocolParameters,
) (*big.Rat, error) {
	var decentralization *big.Rat
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		decentralization = cloneCBORRat(pp.Decentralization)
	case *mary.MaryProtocolParameters:
		decentralization = cloneCBORRat(pp.Decentralization)
	case *alonzo.AlonzoProtocolParameters:
		decentralization = cloneCBORRat(pp.Decentralization)
	case *babbage.BabbageProtocolParameters,
		*conway.ConwayProtocolParameters,
		*dijkstra.DijkstraProtocolParameters:
		decentralization = new(big.Rat)
	default:
		return nil, fmt.Errorf("unsupported reward pparams type %T", pparams)
	}
	if decentralization == nil {
		return nil, fmt.Errorf(
			"%w: missing decentralization",
			rewards.ErrInvalidParameters,
		)
	}
	if decentralization.Sign() < 0 {
		return nil, fmt.Errorf(
			"%w: negative decentralization",
			rewards.ErrInvalidParameters,
		)
	}
	if decentralization.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, fmt.Errorf(
			"%w: decentralization greater than one",
			rewards.ErrInvalidParameters,
		)
	}
	return decentralization, nil
}

func rewardEpochFees(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	epoch models.Epoch,
) (uint64, error) {
	if epoch.LengthInSlots == 0 {
		return 0, nil
	}
	startSlot := epoch.StartSlot
	endSlot := startSlot + uint64(epoch.LengthInSlots) - 1
	return meta.SumTransactionFeesInSlotRange(startSlot, endSlot, metaTxn)
}

func rewardCredentialActive(
	credential rewards.Credential,
	activeAccounts map[string]struct{},
) bool {
	_, ok := activeAccounts[models.NewStakeCredentialRef(
		credential.Tag,
		credential.Hash[:],
	).MapKey()]
	return ok
}

func stakeRewardSourceHash(
	epoch uint64,
	reward rewards.AccountReward,
) []byte {
	h := sha256.New()
	h.Write([]byte(stakeRewardSourcePrefix)) //nolint:errcheck
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], epoch)
	h.Write(buf[:])                        //nolint:errcheck
	h.Write(reward.PoolID[:])              //nolint:errcheck
	h.Write([]byte{reward.Credential.Tag}) //nolint:errcheck
	h.Write(reward.Credential.Hash[:])     //nolint:errcheck
	h.Write([]byte(reward.Type))           //nolint:errcheck
	return h.Sum(nil)
}

func ratOrZero(r *types.Rat) *big.Rat {
	if r == nil || r.Rat == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(r.Rat)
}

func cloneCBORRat(r *cbor.Rat) *big.Rat {
	if r == nil || r.Rat == nil {
		return nil
	}
	return new(big.Rat).Set(r.Rat)
}

func rewardRat(r *big.Rat) *types.Rat {
	if r == nil {
		return nil
	}
	return &types.Rat{Rat: new(big.Rat).Set(r)}
}
