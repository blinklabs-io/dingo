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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/rewards"
)

// rewardPrecomputeChunkPools is the default number of pools processed, with
// their delegators, per chunk transaction. A mainnet pool set (roughly 2,700
// pools) splits into on the order of dozens of chunks at this size; each
// chunk's own delegator count then bounds that chunk's cost, rather than the
// whole snapshot's. See rewardLiveStakeRebuildBatch for the same reasoning
// applied to the live-stake aggregate rebuild.
const rewardPrecomputeChunkPools = 100

// rewardPrecomputeChunkSize returns the configured chunk size, or the
// default. Tests set rewardPrecomputeChunkPoolsOverride to force many small
// chunks over a small fixture pool set.
func (ls *LedgerState) rewardPrecomputeChunkSize() int {
	if ls.rewardPrecomputeChunkPoolsOverride > 0 {
		return ls.rewardPrecomputeChunkPoolsOverride
	}
	return rewardPrecomputeChunkPools
}

// rewardPrecomputeCursorFormatVersion is bumped whenever the cursor's shape
// changes incompatibly; loadRewardPrecomputeCursor treats an unknown version
// as absent, which costs one restart of Pass 2 for the epoch.
const rewardPrecomputeCursorFormatVersion = 2

// rewardPrecomputeCursor is the resumable checkpoint for the per-pool member
// reward split (Pass 2, rewards.ApplyPoolMemberRewards), persisted to
// sync_state with every chunk.
//
// InputFingerprint identifies the pool-level inputs the progress was computed
// from (rewardPrecomputeInputFingerprint). A later run resumes, or accepts a
// finished round, only while the fingerprint still matches and the pool
// outputs the cursor claims are all present: a rollback that reaches the
// round's inputs deletes those outputs with them, so progress survives exactly
// the rollbacks that cannot have changed it. OutputFingerprint, set when Done,
// pins the finished pool outputs, so a boundary accepts the round from
// pool-level reads alone.
type rewardPrecomputeCursor struct {
	FormatVersion    int    `json:"format_version"`
	SnapshotEpoch    uint64 `json:"snapshot_epoch"`
	Generation       uint64 `json:"generation"`
	InputFingerprint string `json:"input_fingerprint"`
	// LastPoolKeyHash is the hex-encoded pool_key_hash of the last pool whose
	// output rows have been committed, in the ascending order
	// GetRewardPoolInputs already returns; empty means no pool committed yet.
	LastPoolKeyHash    string `json:"last_pool_key_hash"`
	CompletedPools     int    `json:"completed_pools"`
	EffectiveRewards   uint64 `json:"effective_rewards"`
	UnspendableRewards uint64 `json:"unspendable_rewards"`
	Done               bool   `json:"done"`
	OutputFingerprint  string `json:"output_fingerprint,omitempty"`
}

func rewardPrecomputeCursorKey(snapshotEpoch uint64) string {
	return fmt.Sprintf(
		"%sprecompute-cursor:%d",
		stakeRewardSourcePrefix,
		snapshotEpoch,
	)
}

func loadRewardPrecomputeCursor(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	snapshotEpoch uint64,
) (*rewardPrecomputeCursor, error) {
	raw, err := meta.GetSyncState(
		rewardPrecomputeCursorKey(snapshotEpoch),
		metaTxn,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"load reward precompute cursor for epoch %d: %w",
			snapshotEpoch,
			err,
		)
	}
	if raw == "" {
		return nil, nil
	}
	var cursor rewardPrecomputeCursor
	if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
		// A corrupt or foreign-format record is treated the same as absent:
		// Pass 2 restarts from the beginning for this generation, which is
		// always safe (it only costs redoing already-committed work).
		//nolint:nilerr // Deliberate: corrupt cursor JSON falls back to absent.
		return nil, nil
	}
	if cursor.FormatVersion != rewardPrecomputeCursorFormatVersion {
		return nil, nil
	}
	return &cursor, nil
}

func saveRewardPrecomputeCursor(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	cursor *rewardPrecomputeCursor,
) error {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("encode reward precompute cursor: %w", err)
	}
	if err := meta.SetSyncState(
		rewardPrecomputeCursorKey(cursor.SnapshotEpoch),
		string(raw),
		metaTxn,
	); err != nil {
		return fmt.Errorf(
			"save reward precompute cursor for epoch %d: %w",
			cursor.SnapshotEpoch,
			err,
		)
	}
	return nil
}

// stakeRewardPrecomputeRound holds a reward round's inputs and Pass-1 result,
// gathered once in a read-only transaction and then reused across every
// chunk step: none of it changes for the life of one precompute attempt
// (a change is exactly what rewardInputGeneration bumping means, and every
// chunk step re-checks that before writing).
type stakeRewardPrecomputeRound struct {
	newEpoch     uint64
	epochs       stakeRewardEpochs
	capturedSlot uint64
	boundarySlot uint64
	generation   uint64

	// fence is rewardPrecomputeFence at resolve time; a boundary bumps it
	// before applying rewards, which stops background chunks from writing
	// outputs the boundary is completing in its own transaction.
	fence            uint64
	inputFingerprint string

	pots           *models.RewardAdaPots
	rewardSnapshot *models.RewardSnapshot
	// poolInputs is in the ascending pool_key_hash order
	// GetRewardPoolInputs already returns; chunk boundaries are drawn
	// directly from this order.
	poolInputs    []*models.RewardPoolInput
	params        rewards.Parameters
	prefilterSlot uint64
	blockCounts   map[string]uint64
	totalBlocks   uint64

	totals rewards.RoundTotals
	byPool map[rewards.PoolID]rewards.PoolReward

	// reconstructedStakeInputs is non-empty only when the snapshot's
	// reward_stake_input rows had already aged out of the retention window
	// (rebuildPrunedRewardStakeInputs). It is persisted once, by the first
	// chunk of a fresh run, so every chunk after that -- including one from a
	// later, resumed invocation -- reads reward_stake_input normally instead
	// of special-casing reconstruction per chunk.
	reconstructedStakeInputs []*models.RewardStakeInput
}

// resolveStakeRewardPrecomputeRound gathers a reward round's fixed inputs and
// runs Pass 1 (rewards.CalculateBaseRewards), entirely from pool-level rows
// (reward_pool_input, at most a few thousand rows on mainnet) -- it never
// reads a reward_stake_input row for a fresh (non-pruned) snapshot. ok is
// false for exactly the reasons calculateStakeRewardApplication itself
// declines to run: inputs not written yet, or the reward prefilter slot not
// reached.
func (ls *LedgerState) resolveStakeRewardPrecomputeRound(
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) (*stakeRewardPrecomputeRound, bool, error) {
	var round *stakeRewardPrecomputeRound
	var ok bool
	readTxn := ls.db.Transaction(false)
	err := readTxn.Do(func(txn *database.Txn) error {
		var err error
		round, ok, err = ls.resolveStakeRewardPrecomputeRoundInTxn(
			txn, newEpoch, capturedSlot, boundarySlot, true,
		)
		return err
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return round, true, nil
}

// resolveStakeRewardPrecomputeRoundInTxn is resolveStakeRewardPrecomputeRound
// against a caller's transaction. deferRetry controls whether a round whose
// prefilter slot has not been reached queues a retry for it.
func (ls *LedgerState) resolveStakeRewardPrecomputeRoundInTxn(
	txn *database.Txn,
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
	deferRetry bool,
) (*stakeRewardPrecomputeRound, bool, error) {
	epochs, ok := stakeRewardEpochsForApplication(newEpoch)
	if !ok || epochs.bootstrap {
		return nil, false, nil
	}
	credited, err := ls.rewardRoundCredited(txn, newEpoch)
	if err != nil || credited {
		return nil, false, err
	}
	generation := ls.rewardInputGeneration.Load()
	fence := ls.rewardPrecomputeFence.Load()
	if err := ls.ensureRewardStakeInputsReady(txn, epochs.snapshot); err != nil {
		if errors.Is(err, errRewardStakeInputsNotReady) {
			return nil, false, nil
		}
		return nil, false, err
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
	if pots == nil {
		return nil, false, nil
	}
	rewardSnapshot, err := meta.GetRewardSnapshot(
		epochs.snapshot,
		"mark",
		metaTxn,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward snapshot for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	if rewardSnapshot == nil {
		return nil, false, nil
	}
	poolInputs, err := meta.GetRewardPoolInputs(epochs.snapshot, metaTxn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward pool inputs for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	if uint64(len(poolInputs)) != rewardSnapshot.TotalPoolCount {
		return nil, false, nil
	}
	var reconstructedStakeInputs []*models.RewardStakeInput
	if len(poolInputs) > 0 && rewardSnapshot.TotalDelegators > 0 {
		// Probe the pool with the largest recorded delegator count: if
		// any pool's reward_stake_input rows survived retention, that one
		// almost certainly did. A pruned snapshot needs
		// rebuildPrunedRewardStakeInputs, an O(delegators) historical
		// reconstruction the monolithic path also pays; running it here
		// keeps the chunked path capable of the same recovery instead of
		// silently never completing for these epochs.
		probePool := poolInputs[0]
		for _, input := range poolInputs[1:] {
			if input.DelegatorCount > probePool.DelegatorCount {
				probePool = input
			}
		}
		probe, err := meta.GetRewardStakeInputsInPoolKeyHashRange(
			epochs.snapshot,
			probePool.PoolKeyHash,
			probePool.PoolKeyHash,
			metaTxn,
		)
		if err != nil {
			return nil, false, fmt.Errorf(
				"probe reward stake inputs for epoch %d: %w",
				epochs.snapshot,
				err,
			)
		}
		if len(probe) == 0 && probePool.DelegatorCount > 0 {
			rebuilt, err := ls.rebuildPrunedRewardStakeInputs(
				meta, metaTxn, epochs.snapshot, rewardSnapshot, poolInputs,
			)
			if err != nil {
				return nil, false, fmt.Errorf(
					"rebuild pruned reward stake inputs for epoch %d: %w",
					epochs.snapshot,
					err,
				)
			}
			if len(rebuilt) == 0 {
				// Nothing to reconstruct from either: same as the
				// monolithic path, decline rather than compute against an
				// empty basis.
				return nil, false, nil
			}
			reconstructedStakeInputs = rebuilt
		}
	}
	_, params, performanceDecentralization, err := ls.rewardParameters(
		txn,
		epochs.performance,
		epochs.pots,
		pots,
	)
	if err != nil {
		return nil, false, err
	}
	// Before Allegra a credential earning from several pools is paid once,
	// which needs the whole pool set at once; those rounds use the
	// single-pass precompute.
	if !params.SupportsPerPoolMemberRewards() {
		return nil, false, nil
	}
	blockCounts, totalBlocks, blockCountsKnown, err := ls.rewardBlockCounts(
		meta,
		metaTxn,
		epochs.performance,
		poolInputs,
		performanceDecentralization,
	)
	if err != nil {
		return nil, false, err
	}
	if !blockCountsKnown {
		return nil, false, nil
	}
	prefilterSlot, err := ls.rewardPrefilterSlot(meta, metaTxn, epochs.pots)
	if err != nil {
		return nil, false, err
	}
	if params.RequiresRewardPrefilter() && capturedSlot < prefilterSlot {
		if deferRetry {
			ls.deferStakeRewardPrecompute(newEpoch, prefilterSlot, generation)
		}
		return nil, false, nil
	}

	activeAccounts, err := rewardActiveAccounts(
		meta, metaTxn, poolInputs, nil,
	)
	if err != nil {
		return nil, false, err
	}
	prefilterAccounts, err := rewardPrefilterAccounts(
		meta, metaTxn, poolInputs, nil,
		prefilterSlot, params.RequiresRewardPrefilter(), activeAccounts,
	)
	if err != nil {
		return nil, false, err
	}
	summaries := make([]rewards.Pool, 0, len(poolInputs))
	for _, input := range poolInputs {
		pool, err := rewardPoolFromInput(
			input,
			nil,
			activeAccounts,
			prefilterAccounts,
			blockCounts[string(input.PoolKeyHash)],
			totalBlocks,
		)
		if err != nil {
			return nil, false, fmt.Errorf(
				"build pool summary for reward precompute: %w",
				err,
			)
		}
		summaries = append(summaries, pool)
	}
	totals, byPool, err := rewards.CalculateBaseRewards(
		rewards.Pots{
			Reserves: uint64(pots.Reserves),
			Treasury: uint64(pots.Treasury),
			Fees:     uint64(pots.Fees),
		},
		summaries,
		uint64(rewardSnapshot.TotalActiveStake),
		(*uint64)(rewardSnapshot.ExcludedActiveStake),
		params,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"calculate base rewards for epoch %d: %w",
			epochs.snapshot,
			err,
		)
	}
	round := &stakeRewardPrecomputeRound{
		newEpoch:       newEpoch,
		epochs:         epochs,
		capturedSlot:   capturedSlot,
		boundarySlot:   boundarySlot,
		generation:     generation,
		fence:          fence,
		pots:           pots,
		rewardSnapshot: rewardSnapshot,
		poolInputs:     poolInputs,
		params:         params,
		prefilterSlot:  prefilterSlot,
		blockCounts:    blockCounts,
		totalBlocks:    totalBlocks,
		totals:         totals,
		byPool:         byPool,

		reconstructedStakeInputs: reconstructedStakeInputs,
	}
	round.inputFingerprint = ls.rewardPrecomputeInputFingerprint(round)
	return round, true, nil
}

// rewardPrecomputeSnapshotUnchanged reports whether a re-read reward_snapshot
// row still matches the one a round was resolved from. It mirrors
// stakeRewardPrecomputeSnapshotGuardOK's row comparison, checked per chunk
// here instead of once at the end of a single write phase.
func rewardPrecomputeSnapshotUnchanged(want, got *models.RewardSnapshot) bool {
	if want == nil || got == nil {
		return false
	}
	return want.CapturedSlot == got.CapturedSlot &&
		want.BoundarySlot == got.BoundarySlot &&
		want.TotalActiveStake == got.TotalActiveStake &&
		want.TotalPoolCount == got.TotalPoolCount &&
		want.TotalDelegators == got.TotalDelegators &&
		want.ProtocolVersion == got.ProtocolVersion &&
		bytes.Equal(want.EpochNonce, got.EpochNonce)
}

// addRewardTotalChecked adds two running cursor totals, refusing to wrap.
// Real lovelace sums stay far below uint64's range; this is defense against
// a corrupt or adversarial input rather than an expected case.
func addRewardTotalChecked(a, b uint64) (uint64, error) {
	sum, overflow := addRewardUint64(a, b)
	if overflow {
		return 0, errors.New("reward precompute cursor total overflow")
	}
	return sum, nil
}

// fenceRewardPrecompute stops background precompute chunks from writing
// before a boundary completes and applies a round in its own transaction. It
// waits for a chunk already in flight, and must be called before the boundary
// transaction opens: on SQLite that chunk may be waiting for the writer.
func (ls *LedgerState) fenceRewardPrecompute() {
	ls.rewardPrecomputeWriteMu.Lock()
	ls.rewardPrecomputeFence.Add(1)
	ls.rewardPrecomputeWriteMu.Unlock()
}

// runChunkedStakeRewardPrecomputeRound drives Pass 2 (the per-pool member
// split) to completion for one reward round, one bounded chunk of pools per
// short write transaction, persisting a resumable cursor with every chunk. It
// reports whether the per-pool precompute took the round at all.
func (ls *LedgerState) runChunkedStakeRewardPrecomputeRound(
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) (bool, error) {
	credited, err := ls.rewardRoundCredited(nil, newEpoch)
	if err != nil || credited {
		return credited, err
	}
	if err := ls.completePendingRewardStakeInputs(newEpoch); err != nil {
		return false, err
	}
	round, ok, err := ls.resolveStakeRewardPrecomputeRound(
		newEpoch, capturedSlot, boundarySlot,
	)
	if err != nil || !ok {
		return false, err
	}
	for {
		if ls.closed.Load() {
			return true, nil
		}
		done, err := ls.stakeRewardPrecomputeChunkStep(round)
		if err != nil {
			return true, err
		}
		if done {
			return true, nil
		}
	}
}

// stakeRewardPrecomputeChunkStep processes at most one chunk of pools inside
// its own write transaction. done is true once nothing further should be
// attempted for this round: Pass 2 is complete, or the round was invalidated
// by a rollback or claimed by a boundary and the caller must stop.
func (ls *LedgerState) stakeRewardPrecomputeChunkStep(
	round *stakeRewardPrecomputeRound,
) (bool, error) {
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()

	done := false
	writeTxn := ls.db.Transaction(true)
	err := writeTxn.Do(func(txn *database.Txn) error {
		if ls.rewardInputRollbackActive.Load() != 0 ||
			ls.rewardInputGeneration.Load() != round.generation ||
			ls.rewardPrecomputeFence.Load() != round.fence {
			done = true
			return nil
		}
		current, err := ls.db.Metadata().GetRewardSnapshot(
			round.epochs.snapshot,
			"mark",
			txn.Metadata(),
		)
		if err != nil {
			return fmt.Errorf(
				"re-check reward snapshot for epoch %d: %w",
				round.epochs.snapshot,
				err,
			)
		}
		if !rewardPrecomputeSnapshotUnchanged(round.rewardSnapshot, current) {
			done = true
			return nil
		}
		done, err = ls.stakeRewardPrecomputeChunksInTxn(txn, round, 1)
		return err
	})
	return done, err
}

// stakeRewardPrecomputeChunksInTxn runs up to maxChunks chunks of Pass 2 for
// round inside txn (maxChunks <= 0 means until done), resuming from the
// persisted cursor when its progress is still valid for round. done is true
// once the round is complete.
func (ls *LedgerState) stakeRewardPrecomputeChunksInTxn(
	txn *database.Txn,
	round *stakeRewardPrecomputeRound,
	maxChunks int,
) (bool, error) {
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()
	cursor, startIndex, err := ls.resumableRewardPrecomputeCursor(
		meta, metaTxn, round,
	)
	if err != nil {
		return false, err
	}
	if cursor != nil && cursor.Done {
		return true, nil
	}
	if cursor == nil {
		if err := meta.DeleteRewardOutputsForEpoch(
			round.epochs.snapshot, metaTxn,
		); err != nil {
			return false, fmt.Errorf(
				"clear reward outputs for epoch %d: %w",
				round.epochs.snapshot,
				err,
			)
		}
		if len(round.reconstructedStakeInputs) > 0 {
			// Persisted once, up front, so every later chunk -- including
			// one from a resumed run -- reads reward_stake_input normally.
			if err := meta.SaveRewardStakeInputs(
				round.reconstructedStakeInputs, metaTxn,
			); err != nil {
				return false, fmt.Errorf(
					"save reconstructed reward stake inputs for epoch %d: %w",
					round.epochs.snapshot,
					err,
				)
			}
		}
		cursor = &rewardPrecomputeCursor{
			FormatVersion:    rewardPrecomputeCursorFormatVersion,
			SnapshotEpoch:    round.epochs.snapshot,
			Generation:       round.generation,
			InputFingerprint: round.inputFingerprint,
		}
		startIndex = 0
	}
	// A degenerate round has no per-pool results and nothing to split: Pass 1
	// already returned the whole available pot to reserves.
	if round.totals.Degenerate {
		if err := ls.finishStakeRewardPrecomputeLocked(
			meta, metaTxn, round, cursor,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	for chunks := 0; maxChunks <= 0 || chunks < maxChunks; chunks++ {
		if startIndex >= len(round.poolInputs) {
			if err := ls.finishStakeRewardPrecomputeLocked(
				meta, metaTxn, round, cursor,
			); err != nil {
				return false, err
			}
			return true, nil
		}
		next, err := ls.stakeRewardPrecomputeChunk(
			txn, round, cursor, startIndex,
		)
		if err != nil {
			return false, err
		}
		startIndex = next
		cursor.CompletedPools = next
		if startIndex >= len(round.poolInputs) {
			if err := ls.finishStakeRewardPrecomputeLocked(
				meta, metaTxn, round, cursor,
			); err != nil {
				return false, err
			}
			return true, nil
		}
		if err := saveRewardPrecomputeCursor(meta, metaTxn, cursor); err != nil {
			return false, err
		}
		if ls.rewardPrecomputeChunkHook != nil {
			ls.rewardPrecomputeChunkHook(startIndex, len(round.poolInputs))
		}
	}
	return false, nil
}

// resumableRewardPrecomputeCursor returns the persisted cursor for round and
// the pool index to resume from, or a nil cursor when Pass 2 must start over.
// A finished cursor is returned only when its output fingerprint still
// matches the stored pool outputs.
func (ls *LedgerState) resumableRewardPrecomputeCursor(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	round *stakeRewardPrecomputeRound,
) (*rewardPrecomputeCursor, int, error) {
	cursor, err := loadRewardPrecomputeCursor(
		meta, metaTxn, round.epochs.snapshot,
	)
	if err != nil || cursor == nil {
		return nil, 0, err
	}
	if cursor.InputFingerprint != round.inputFingerprint {
		return nil, 0, nil
	}
	poolOutputs, err := meta.GetRewardPoolOutputs(
		round.epochs.snapshot, metaTxn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get reward pool outputs for epoch %d: %w",
			round.epochs.snapshot,
			err,
		)
	}
	if cursor.Done {
		if uint64(round.pots.Rewards) != round.totals.TotalRewardPot ||
			rewardPoolOutputFingerprint(
				poolOutputs,
			) != cursor.OutputFingerprint {
			return nil, 0, nil
		}
		return cursor, len(round.poolInputs), nil
	}
	index, found := findRewardPoolIndexAfter(
		round.poolInputs, cursor.LastPoolKeyHash,
	)
	// A rollback that reached the round's captured slot deleted its outputs;
	// the cursor's progress is only usable while every pool it claims still
	// has its output row.
	if !found || index != cursor.CompletedPools ||
		len(poolOutputs) != cursor.CompletedPools {
		return nil, 0, nil
	}
	return cursor, index, nil
}

// stakeRewardPrecomputeChunk computes and persists Pass 2 for the chunk of
// pools starting at startIndex, advancing cursor, and returns the next pool
// index.
func (ls *LedgerState) stakeRewardPrecomputeChunk(
	txn *database.Txn,
	round *stakeRewardPrecomputeRound,
	cursor *rewardPrecomputeCursor,
	startIndex int,
) (int, error) {
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()
	var err error
	endIndex := min(
		startIndex+ls.rewardPrecomputeChunkSize(),
		len(round.poolInputs),
	)
	chunkPools := round.poolInputs[startIndex:endIndex]

	stakeInputs, err := meta.GetRewardStakeInputsInPoolKeyHashRange(
		round.epochs.snapshot,
		chunkPools[0].PoolKeyHash,
		chunkPools[len(chunkPools)-1].PoolKeyHash,
		metaTxn,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"get reward stake inputs for pool chunk: %w",
			err,
		)
	}
	stakeByPool := make(
		map[string][]*models.RewardStakeInput,
		len(chunkPools),
	)
	for _, input := range stakeInputs {
		if input == nil {
			continue
		}
		key := string(input.PoolKeyHash)
		stakeByPool[key] = append(stakeByPool[key], input)
	}
	activeAccounts, err := rewardActiveAccounts(
		meta, metaTxn, chunkPools, stakeInputs,
	)
	if err != nil {
		return 0, err
	}
	prefilterAccounts, err := rewardPrefilterAccounts(
		meta, metaTxn, chunkPools, stakeInputs,
		round.prefilterSlot, round.params.RequiresRewardPrefilter(),
		activeAccounts,
	)
	if err != nil {
		return 0, err
	}

	var poolOutputs []*models.RewardPoolOutput
	var accountOutputs []*models.RewardAccountOutput
	for _, input := range chunkPools {
		pool, err := rewardPoolFromInput(
			input,
			stakeByPool[string(input.PoolKeyHash)],
			activeAccounts,
			prefilterAccounts,
			round.blockCounts[string(input.PoolKeyHash)],
			round.totalBlocks,
		)
		if err != nil {
			return 0, fmt.Errorf(
				"build pool for reward precompute chunk: %w",
				err,
			)
		}
		poolReward, ok := round.byPool[pool.ID]
		if !ok {
			return 0, fmt.Errorf(
				"missing pass-1 reward result for pool %s",
				pool.ID.String(),
			)
		}
		res, err := rewards.ApplyPoolMemberRewards(
			pool, poolReward, round.params,
		)
		if err != nil {
			return 0, fmt.Errorf(
				"apply member rewards for pool %s: %w",
				pool.ID.String(),
				err,
			)
		}
		poolReward.MemberRewardTotal = res.MemberRewardTotal
		poolReward.Unspendable = res.Unspendable
		if poolReward.PoolReward < res.Accounted {
			return 0, fmt.Errorf(
				"pool %s accounted %d exceeds pool reward %d",
				pool.ID.String(),
				res.Accounted,
				poolReward.PoolReward,
			)
		}
		poolReward.Undistributed = poolReward.PoolReward - res.Accounted
		poolOutputs = append(poolOutputs, rewardPoolOutputs(
			round.epochs.snapshot, round.capturedSlot, round.boundarySlot,
			[]rewards.PoolReward{poolReward},
		)...)
		accountOutputs = append(accountOutputs, rewardAccountOutputs(
			round.epochs.snapshot, round.capturedSlot, round.boundarySlot,
			res.Rewards,
		)...)
		cursor.LastPoolKeyHash = hex.EncodeToString(input.PoolKeyHash)
		cursor.EffectiveRewards, err = addRewardTotalChecked(
			cursor.EffectiveRewards, res.Effective,
		)
		if err != nil {
			return 0, err
		}
		cursor.UnspendableRewards, err = addRewardTotalChecked(
			cursor.UnspendableRewards, res.Unspendable,
		)
		if err != nil {
			return 0, err
		}
	}
	// CIP-0163: a reward account expired as of the snapshot epoch is not
	// credited, and its amount counts in neither total. Expiry is judged from
	// witness history at the snapshot's captured slot, which no later block
	// changes, so the flag set here holds whenever the round is applied.
	if ls.config.DelegatorInactivityEnabled {
		guarded, err := ls.guardedExpiredRewardCredentials(
			txn,
			&stakeRewardApplication{
				accountOutputs:       accountOutputs,
				epochs:               round.epochs,
				snapshotCapturedSlot: round.rewardSnapshot.CapturedSlot,
			},
		)
		if err != nil {
			return 0, fmt.Errorf(
				"resolve reward-credential guard for chunk: %w", err,
			)
		}
		for _, output := range accountOutputs {
			key := models.NewStakeCredentialRef(
				output.CredentialTag, output.StakingKey,
			).MapKey()
			if _, isGuarded := guarded[key]; !isGuarded {
				continue
			}
			output.Guarded = true
			amount := uint64(output.Amount)
			total := &cursor.UnspendableRewards
			if output.Spendable {
				total = &cursor.EffectiveRewards
			}
			if *total < amount {
				return 0, fmt.Errorf(
					"guarded amount %d exceeds chunk total %d", amount, *total,
				)
			}
			*total -= amount
		}
	}
	if ls.rewardPrecomputeBeforeSaveHook != nil {
		ls.rewardPrecomputeBeforeSaveHook()
	}
	if err := meta.SaveRewardPoolOutputs(poolOutputs, metaTxn); err != nil {
		return 0, fmt.Errorf("save reward pool output chunk: %w", err)
	}
	if err := meta.SaveRewardAccountOutputs(
		accountOutputs, metaTxn,
	); err != nil {
		return 0, fmt.Errorf("save reward account output chunk: %w", err)
	}

	return endIndex, nil
}

// finishStakeRewardPrecomputeLocked runs once, when the last chunk of a
// round completes: it validates the round's running totals reconcile exactly
// against the available pot (the same identity Calculate itself enforces),
// persists reward_ada_pots.Rewards and marks the cursor done with the
// fingerprint of the finished pool outputs.
func (ls *LedgerState) finishStakeRewardPrecomputeLocked(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	round *stakeRewardPrecomputeRound,
	cursor *rewardPrecomputeCursor,
) error {
	if _, _, err := rewards.ReconcileRoundTotals(
		round.totals, cursor.EffectiveRewards, cursor.UnspendableRewards,
	); err != nil {
		return fmt.Errorf(
			"reconcile reward precompute totals for epoch %d: %w",
			round.epochs.snapshot,
			err,
		)
	}
	round.pots.Rewards = types.Uint64(round.totals.TotalRewardPot)
	if err := meta.SaveRewardAdaPots(round.pots, metaTxn); err != nil {
		return fmt.Errorf("save precomputed reward ADA pots: %w", err)
	}
	poolOutputs, err := meta.GetRewardPoolOutputs(
		round.epochs.snapshot, metaTxn,
	)
	if err != nil {
		return fmt.Errorf(
			"get reward pool outputs for epoch %d: %w",
			round.epochs.snapshot,
			err,
		)
	}
	cursor.Done = true
	cursor.CompletedPools = len(round.poolInputs)
	cursor.OutputFingerprint = rewardPoolOutputFingerprint(poolOutputs)
	if err := saveRewardPrecomputeCursor(meta, metaTxn, cursor); err != nil {
		return err
	}
	ls.config.Logger.Info(
		"precomputed stake rewards",
		"component", "ledger",
		"reward_snapshot_epoch", round.epochs.snapshot,
		"performance_epoch", round.epochs.performance,
		"pots_epoch", round.epochs.pots,
		"application_epoch", round.newEpoch,
		"captured_slot", round.capturedSlot,
		"boundary_slot", round.boundarySlot,
		"total_reward_pot", round.totals.TotalRewardPot,
		"available_rewards", round.totals.AvailableRewards,
		"pool_count", len(round.poolInputs),
	)
	return nil
}

// findRewardPoolIndexAfter returns the index of the first pool strictly
// after lastPoolKeyHash (hex-encoded) in poolInputs' ascending order, and
// whether lastPoolKeyHash was found at all. Not found means the cursor names
// a pool absent from the current round's pool set -- only possible when the
// caller failed to also check the generation, since a genuinely new
// generation is rejected by its caller before this is reached -- and the
// caller restarts Pass 2 from the beginning rather than guess a position.
func findRewardPoolIndexAfter(
	poolInputs []*models.RewardPoolInput,
	lastPoolKeyHash string,
) (int, bool) {
	if lastPoolKeyHash == "" {
		return 0, true
	}
	want, err := hex.DecodeString(lastPoolKeyHash)
	if err != nil {
		return 0, false
	}
	for i, input := range poolInputs {
		if bytes.Equal(input.PoolKeyHash, want) {
			return i + 1, true
		}
	}
	return 0, false
}

func writeRatFingerprint(h io.Writer, r *big.Rat) {
	if r == nil {
		_, _ = io.WriteString(h, "nil;")
		return
	}
	_, _ = io.WriteString(h, r.RatString()+";")
}

func writeTypesRatFingerprint(h io.Writer, r *types.Rat) {
	if r == nil || r.Rat == nil {
		_, _ = io.WriteString(h, "nil;")
		return
	}
	writeRatFingerprint(h, r.Rat)
}

func writeOptionalUint64Fingerprint(h io.Writer, v *types.Uint64) {
	if v == nil {
		_, _ = io.WriteString(h, "nil;")
		return
	}
	fmt.Fprintf(h, "%d;", uint64(*v))
}

// rewardPrecomputeInputFingerprint identifies every input Pass 1 and Pass 2
// read for round: the ADA pots, reward snapshot, pool inputs, reward
// parameters and operator reward settings, block counts, and prefilter slot.
// Per-credential stake inputs are frozen with the snapshot, whose nonce and
// totals are part of it. reward_ada_pots.rewards is excluded because the
// precompute itself writes it.
func (ls *LedgerState) rewardPrecomputeInputFingerprint(
	round *stakeRewardPrecomputeRound,
) string {
	h := sha256.New()
	fmt.Fprintf(
		h, "v2;%d;%d;%d;%d;%d;%t;",
		round.newEpoch, round.boundarySlot, round.epochs.snapshot,
		round.epochs.performance, round.epochs.pots, round.epochs.bootstrap,
	)
	pots := round.pots
	fmt.Fprintf(
		h, "pots;%d;%d;%d;%d;%d;",
		pots.Epoch, uint64(pots.Treasury), uint64(pots.Reserves),
		uint64(pots.Fees), pots.CapturedSlot,
	)
	writeOptionalUint64Fingerprint(h, pots.ImportedEpochFees)
	snap := round.rewardSnapshot
	fmt.Fprintf(
		h, "snap;%d;%s;%d;%d;%d;%d;%d;%x;%d;%t;%d;",
		snap.Epoch, snap.SnapshotType, uint64(snap.TotalActiveStake),
		snap.TotalPoolCount, snap.TotalDelegators, snap.CapturedSlot,
		snap.BoundarySlot, snap.EpochNonce, snap.ProtocolVersion,
		snap.Authoritative, snap.CalculationVersion,
	)
	writeOptionalUint64Fingerprint(h, snap.ExcludedActiveStake)
	for _, input := range round.poolInputs {
		fmt.Fprintf(
			h, "pool;%x;%x;%d;%d;%d;%d;%d;%d;%d;%d;",
			input.PoolKeyHash, input.RewardAccount,
			input.RewardAccountCredentialTag, uint64(input.Pledge),
			uint64(input.Cost), uint64(input.DelegatedStake),
			uint64(input.OwnerStake), input.DelegatorCount,
			input.CapturedSlot, input.BoundarySlot,
		)
		writeTypesRatFingerprint(h, input.Margin)
	}
	params := round.params
	for _, r := range []*big.Rat{
		params.MonetaryExpansion, params.TreasuryExpansion,
		params.Decentralization, params.PledgeInfluence,
		params.ActiveSlotsCoeff, params.MinPoolMargin, params.PledgeLeverage,
	} {
		writeRatFingerprint(h, r)
	}
	fmt.Fprintf(
		h, "params;%d;%d;%d;%d;%t;%t;%t;%d;",
		params.OptimalPoolCount, params.EpochLength, params.MaxLovelaceSupply,
		params.ProtocolMajorVersion, params.PledgeLeverageEnabled,
		params.FullPotRewardsEnabled, ls.config.DelegatorInactivityEnabled,
		ls.config.DelegatorInactivity,
	)
	pools := make([]string, 0, len(round.blockCounts))
	for pool := range round.blockCounts {
		pools = append(pools, pool)
	}
	slices.Sort(pools)
	fmt.Fprintf(h, "blocks;%d;", round.totalBlocks)
	for _, pool := range pools {
		fmt.Fprintf(h, "%x=%d;", pool, round.blockCounts[pool])
	}
	fmt.Fprintf(
		h, "prefilter;%d;%t;",
		round.prefilterSlot, params.RequiresRewardPrefilter(),
	)
	return hex.EncodeToString(h.Sum(nil))
}

// rewardPoolOutputFingerprint identifies a finished round's pool outputs.
func rewardPoolOutputFingerprint(outputs []*models.RewardPoolOutput) string {
	sorted := slices.Clone(outputs)
	slices.SortFunc(sorted, func(a, b *models.RewardPoolOutput) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	h := sha256.New()
	for _, output := range sorted {
		if output == nil {
			continue
		}
		fmt.Fprintf(
			h, "%x;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;",
			output.PoolKeyHash, output.Epoch, uint64(output.OptimalReward),
			uint64(output.TotalReward), uint64(output.LeaderReward),
			uint64(output.MemberRewardTotal), uint64(output.OwnerStake),
			uint64(output.Undistributed), uint64(output.Unspendable),
			output.CapturedSlot, output.BoundarySlot,
		)
		writeTypesRatFingerprint(h, output.ApparentPerformance)
	}
	return hex.EncodeToString(h.Sum(nil))
}
