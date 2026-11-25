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

package epochengine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
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

// rewardPrecomputeChunkDelegators bounds the delegators processed per chunk
// transaction. The transaction holds the only write connection while it
// writes one reward_account_output row per delegator into a table with
// several random-keyed indexes, so a 100-pool chunk of large mainnet pools
// held block application off for 30-45 seconds at a time.
const rewardPrecomputeChunkDelegators = 5000

// RewardPrecomputeChunkEnd returns the end of the chunk of pools starting at
// start: at most maxPools pools, ending at the first pool that brings the
// chunk's delegator count to maxDelegators. A chunk always takes at least one
// pool, however many delegators it has.
func RewardPrecomputeChunkEnd(
	pools []*models.RewardPoolInput,
	start int,
	maxPools int,
	maxDelegators uint64,
) int {
	end := min(start+max(1, maxPools), len(pools))
	var delegators uint64
	for i := start; i < end; i++ {
		delegators += pools[i].DelegatorCount
		if delegators >= maxDelegators {
			return i + 1
		}
	}
	return end
}

// RewardPrecomputeChunkSize returns the configured chunk size, or the
// default. Tests set rewardPrecomputeChunkPoolsOverride to force many small
// chunks over a small fixture pool set.
func (e *Engine) RewardPrecomputeChunkSize() int {
	if override := e.host.PrecomputeChunkPools(); override > 0 {
		return override
	}
	return rewardPrecomputeChunkPools
}

// rewardPrecomputeCursorFormatVersion is bumped whenever the cursor's shape
// changes incompatibly; LoadRewardPrecomputeCursor treats an unknown version
// as absent, which costs one restart of Pass 2 for the epoch.
const rewardPrecomputeCursorFormatVersion = 2

// RewardPrecomputeCursor is the resumable checkpoint for the per-pool member
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
type RewardPrecomputeCursor struct {
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
	// CapturedSlot is the slot the round's snapshot was captured at, which
	// the live set rolls the epoch's outputs back by.
	CapturedSlot uint64 `json:"captured_slot,omitempty"`
	// RollbackSeq is the live set's durable rollback counter when the round
	// began. A boundary that finds it changed re-reads the registration of
	// every credential with an output, as a rollback may have restored any
	// of them after its chunk read it.
	RollbackSeq uint64 `json:"rollback_seq,omitempty"`
}

const RewardPrecomputeCursorPrefix = models.RewardPrecomputeCursorPrefix

// IsRewardCursorKey reports whether key is a reward precompute cursor key.
func IsRewardCursorKey(key string) bool {
	return strings.HasPrefix(key, RewardPrecomputeCursorPrefix)
}

// RewardPrecomputeCursorKey is the sync_state key of the precompute cursor of
// a snapshot epoch.
func RewardPrecomputeCursorKey(snapshotEpoch uint64) string {
	return fmt.Sprintf("%s%d", RewardPrecomputeCursorPrefix, snapshotEpoch)
}

// CursorEpochOfKey returns the snapshot epoch a cursor key names.
func CursorEpochOfKey(key string) (uint64, bool) {
	n, err := strconv.ParseUint(
		strings.TrimPrefix(key, RewardPrecomputeCursorPrefix), 10, 64,
	)
	return n, err == nil && IsRewardCursorKey(key)
}

func LoadRewardPrecomputeCursor(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	snapshotEpoch uint64,
) (*RewardPrecomputeCursor, error) {
	raw, err := meta.GetSyncState(
		RewardPrecomputeCursorKey(snapshotEpoch),
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
	var cursor RewardPrecomputeCursor
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
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	cursor *RewardPrecomputeCursor,
) error {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("encode reward precompute cursor: %w", err)
	}
	if err := meta.SetSyncState(
		RewardPrecomputeCursorKey(cursor.SnapshotEpoch),
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

// StakeRewardPrecomputeRound holds a reward round's inputs and Pass-1 result,
// gathered once in a read-only transaction and then reused across every
// chunk step: none of it changes for the life of one precompute attempt
// (a change is exactly what rewardInputGeneration bumping means, and every
// chunk step re-checks that before writing).
type StakeRewardPrecomputeRound struct {
	NewEpoch     uint64
	Epochs       StakeRewardEpochs
	CapturedSlot uint64
	BoundarySlot uint64
	Generation   uint64

	// fence is rewardPrecomputeFence at resolve time; a boundary bumps it
	// before applying rewards, which stops background chunks from writing
	// outputs the boundary is completing in its own transaction.
	Fence            uint64
	InputFingerprint string

	Pots           *models.RewardAdaPots
	RewardSnapshot *models.RewardSnapshot
	// poolInputs is in the ascending pool_key_hash order
	// GetRewardPoolInputs already returns; chunk boundaries are drawn
	// directly from this order.
	PoolInputs    []*models.RewardPoolInput
	Params        rewards.Parameters
	PrefilterSlot uint64
	BlockCounts   map[string]uint64
	TotalBlocks   uint64

	Totals rewards.RoundTotals
	ByPool map[rewards.PoolID]rewards.PoolReward

	// reconstructedStakeInputs is non-empty only when the snapshot's
	// reward_stake_input rows had already aged out of the retention window
	// (RebuildPrunedRewardStakeInputs). It is persisted once, by the first
	// chunk of a fresh run, so every chunk after that -- including one from a
	// later, resumed invocation -- reads reward_stake_input normally instead
	// of special-casing reconstruction per chunk.
	ReconstructedStakeInputs []*models.RewardStakeInput
}

// ResolveStakeRewardPrecomputeRound gathers a reward round's fixed inputs and
// runs Pass 1 (rewards.CalculateBaseRewards), entirely from pool-level rows
// (reward_pool_input, at most a few thousand rows on mainnet) -- it never
// reads a reward_stake_input row for a fresh (non-pruned) snapshot. ok is
// false for exactly the reasons CalculateStakeRewardApplication itself
// declines to run: inputs not written yet, or the reward prefilter slot not
// reached.
func (e *Engine) ResolveStakeRewardPrecomputeRound(
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) (*StakeRewardPrecomputeRound, bool, error) {
	var round *StakeRewardPrecomputeRound
	var ok bool
	readTxn := e.ledgerDB().Begin(context.Background(), false)
	err := e.ledgerDB().Run(readTxn, func(txn ledgerdb.Txn) error {
		var err error
		round, ok, err = e.ResolveStakeRewardPrecomputeRoundInTxn(
			context.Background(),
			txn,
			newEpoch,
			capturedSlot,
			boundarySlot,
			true,
		)
		return err
	})
	if err != nil || !ok {
		return nil, false, err
	}
	return round, true, nil
}

// ResolveStakeRewardPrecomputeRoundInTxn is ResolveStakeRewardPrecomputeRound
// against a caller's transaction. deferRetry controls whether a round whose
// prefilter slot has not been reached queues a retry for it.
func (e *Engine) ResolveStakeRewardPrecomputeRoundInTxn(
	ctx context.Context,
	txn ledgerdb.Txn,
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
	deferRetry bool,
) (*StakeRewardPrecomputeRound, bool, error) {
	epochs, ok := StakeRewardEpochsForApplication(newEpoch)
	if !ok || epochs.Bootstrap {
		return nil, false, nil
	}
	credited, err := e.RewardRoundCredited(txn, newEpoch)
	if err != nil || credited {
		return nil, false, err
	}
	guards := e.host.RewardInputGuards()
	generation := guards.generation()
	fence := guards.fence()
	if err := e.EnsureRewardStakeInputsReady(txn, epochs.Snapshot); err != nil {
		if errors.Is(err, ErrRewardStakeInputsNotReady) {
			return nil, false, nil
		}
		return nil, false, err
	}
	ldb := e.ledgerDB()
	pots, err := ldb.GetRewardAdaPots(epochs.Pots, txn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward ADA pots for epoch %d: %w",
			epochs.Pots,
			err,
		)
	}
	if pots == nil {
		return nil, false, nil
	}
	rewardSnapshot, err := ldb.GetRewardSnapshot(
		epochs.Snapshot,
		"mark",
		txn,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward snapshot for epoch %d: %w",
			epochs.Snapshot,
			err,
		)
	}
	if rewardSnapshot == nil {
		return nil, false, nil
	}
	poolInputs, err := ldb.GetRewardPoolInputs(epochs.Snapshot, txn)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get reward pool inputs for epoch %d: %w",
			epochs.Snapshot,
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
		// RebuildPrunedRewardStakeInputs, an O(delegators) historical
		// reconstruction the monolithic path also pays; running it here
		// keeps the chunked path capable of the same recovery instead of
		// silently never completing for these epochs.
		probePool := poolInputs[0]
		for _, input := range poolInputs[1:] {
			if input.DelegatorCount > probePool.DelegatorCount {
				probePool = input
			}
		}
		probe, err := ldb.GetRewardStakeInputsInPoolKeyHashRange(
			epochs.Snapshot,
			probePool.PoolKeyHash,
			probePool.PoolKeyHash,
			txn,
		)
		if err != nil {
			return nil, false, fmt.Errorf(
				"probe reward stake inputs for epoch %d: %w",
				epochs.Snapshot,
				err,
			)
		}
		if len(probe) == 0 && probePool.DelegatorCount > 0 {
			rebuilt, err := e.RebuildPrunedRewardStakeInputs(
				ldb, txn, epochs.Snapshot, rewardSnapshot, poolInputs,
			)
			if err != nil {
				return nil, false, fmt.Errorf(
					"rebuild pruned reward stake inputs for epoch %d: %w",
					epochs.Snapshot,
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
	_, params, err := e.RewardParameters(
		txn,
		epochs.Performance,
		epochs.Pots,
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
	blockCounts, totalBlocks, blockCountsKnown, err := e.RewardBlockCounts(
		ldb,
		txn,
		epochs.Performance,
		poolInputs,
	)
	if err != nil {
		return nil, false, err
	}
	if !blockCountsKnown {
		return nil, false, nil
	}
	prefilterSlot, err := e.RewardPrefilterSlot(ldb, txn, epochs.Pots)
	if err != nil {
		return nil, false, err
	}
	if params.RequiresRewardPrefilter() && capturedSlot < prefilterSlot {
		if deferRetry {
			e.host.DeferStakeRewardPrecompute(
				newEpoch,
				prefilterSlot,
				generation,
			)
		}
		return nil, false, nil
	}

	activeAccounts, err := rewardActiveAccounts(ctx,
		ldb, txn, poolInputs, nil,
	)
	if err != nil {
		return nil, false, err
	}
	prefilterAccounts, err := RewardPrefilterAccounts(
		ldb, txn, poolInputs, nil,
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
			epochs.Snapshot,
			err,
		)
	}
	// A negative leader reward has no output row to carry it and may halt the
	// boundary, which only the single-pass calculation decides. Decline the
	// round so the boundary calculates it fresh.
	for _, poolReward := range byPool {
		if poolReward.LeaderRewardDeficit > 0 {
			return nil, false, nil
		}
	}
	round := &StakeRewardPrecomputeRound{
		NewEpoch:       newEpoch,
		Epochs:         epochs,
		CapturedSlot:   capturedSlot,
		BoundarySlot:   boundarySlot,
		Generation:     generation,
		Fence:          fence,
		Pots:           pots,
		RewardSnapshot: rewardSnapshot,
		PoolInputs:     poolInputs,
		Params:         params,
		PrefilterSlot:  prefilterSlot,
		BlockCounts:    blockCounts,
		TotalBlocks:    totalBlocks,
		Totals:         totals,
		ByPool:         byPool,

		ReconstructedStakeInputs: reconstructedStakeInputs,
	}
	round.InputFingerprint = e.rewardPrecomputeInputFingerprint(round)
	return round, true, nil
}

// RewardPrecomputeSnapshotUnchanged reports whether a re-read reward_snapshot
// row still matches the one a round was resolved from. It mirrors
// StakeRewardPrecomputeSnapshotGuardOK's row comparison, checked per chunk
// here instead of once at the end of a single write phase.
func RewardPrecomputeSnapshotUnchanged(want, got *models.RewardSnapshot) bool {
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
	sum, overflow := AddRewardUint64(a, b)
	if overflow {
		return 0, errors.New("reward precompute cursor total overflow")
	}
	return sum, nil
}

// StakeRewardPrecomputeChunksInTxn runs up to maxChunks chunks of Pass 2 for
// round inside txn (maxChunks <= 0 means until done), resuming from the
// persisted cursor when its progress is still valid for round. done is true
// once the round is complete.
func (e *Engine) StakeRewardPrecomputeChunksInTxn(ctx context.Context,
	txn ledgerdb.Txn,
	round *StakeRewardPrecomputeRound,
	maxChunks int,
) (bool, error) {
	ldb := e.ledgerDB()
	cursor, startIndex, err := e.ResumableRewardPrecomputeCursor(
		ldb, txn, round,
	)
	if err != nil {
		return false, err
	}
	if cursor != nil && cursor.Done {
		return true, nil
	}
	if cursor == nil {
		if err := DeleteUncreditedRewardOutputs(
			ldb, txn, round.Epochs.Snapshot,
		); err != nil {
			return false, fmt.Errorf(
				"clear reward outputs for epoch %d: %w",
				round.Epochs.Snapshot,
				err,
			)
		}
		if len(round.ReconstructedStakeInputs) > 0 {
			// Persisted once, up front, so every later chunk -- including
			// one from a resumed run -- reads reward_stake_input normally.
			if err := ldb.SaveRewardStakeInputs(
				round.ReconstructedStakeInputs, txn,
			); err != nil {
				return false, fmt.Errorf(
					"save reconstructed reward stake inputs for epoch %d: %w",
					round.Epochs.Snapshot,
					err,
				)
			}
		}
		seq, err := e.rewardRollbackSeq(txn)
		if err != nil {
			return false, err
		}
		cursor = &RewardPrecomputeCursor{
			FormatVersion:    rewardPrecomputeCursorFormatVersion,
			SnapshotEpoch:    round.Epochs.Snapshot,
			Generation:       round.Generation,
			InputFingerprint: round.InputFingerprint,
			CapturedSlot:     round.CapturedSlot,
			RollbackSeq:      seq,
		}
		startIndex = 0
	}
	// A degenerate round has no per-pool results and nothing to split: Pass 1
	// already returned the whole available pot to reserves.
	if round.Totals.Degenerate {
		if err := e.finishStakeRewardPrecomputeLocked(
			ldb, txn, round, cursor,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	for chunks := 0; maxChunks <= 0 || chunks < maxChunks; chunks++ {
		if startIndex >= len(round.PoolInputs) {
			if err := e.finishStakeRewardPrecomputeLocked(
				ldb, txn, round, cursor,
			); err != nil {
				return false, err
			}
			return true, nil
		}
		next, err := e.stakeRewardPrecomputeChunk(ctx,
			txn, round, cursor, startIndex,
		)
		if err != nil {
			return false, err
		}
		startIndex = next
		cursor.CompletedPools = next
		if startIndex >= len(round.PoolInputs) {
			if err := e.finishStakeRewardPrecomputeLocked(
				ldb, txn, round, cursor,
			); err != nil {
				return false, err
			}
			return true, nil
		}
		if err := saveRewardPrecomputeCursor(ldb, txn, cursor); err != nil {
			return false, err
		}
		e.host.RewardPrecomputeChunkDone(startIndex, len(round.PoolInputs))
	}
	return false, nil
}

// ResumableRewardPrecomputeCursor returns the persisted cursor for round and
// the pool index to resume from, or a nil cursor when Pass 2 must start over.
// A resumed unfinished cursor's captured slot replaces round.CapturedSlot.
// A finished cursor is returned only when its output fingerprint still
// matches the stored pool outputs.
func (e *Engine) ResumableRewardPrecomputeCursor(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	round *StakeRewardPrecomputeRound,
) (*RewardPrecomputeCursor, int, error) {
	cursor, err := LoadRewardPrecomputeCursor(
		meta, metaTxn, round.Epochs.Snapshot,
	)
	if err != nil || cursor == nil {
		return nil, 0, err
	}
	if cursor.InputFingerprint != round.InputFingerprint {
		return nil, 0, nil
	}
	poolOutputs, err := meta.GetRewardPoolOutputs(
		round.Epochs.Snapshot, metaTxn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get reward pool outputs for epoch %d: %w",
			round.Epochs.Snapshot,
			err,
		)
	}
	if cursor.Done {
		if uint64(round.Pots.Rewards) != round.Totals.TotalRewardPot ||
			RewardPoolOutputFingerprint(
				poolOutputs,
			) != cursor.OutputFingerprint {
			return nil, 0, nil
		}
		return cursor, len(round.PoolInputs), nil
	}
	index, found := findRewardPoolIndexAfter(
		round.PoolInputs, cursor.LastPoolKeyHash,
	)
	// A rollback that reached the round's captured slot deleted its outputs;
	// the cursor's progress is only usable while every pool it claims still
	// has its output row.
	if !found || index != cursor.CompletedPools ||
		len(poolOutputs) != cursor.CompletedPools {
		return nil, 0, nil
	}
	// Every output row of a round carries one captured slot. The captured
	// slot is not part of the input fingerprint, and the boundary and startup
	// resume at a different one than the background run, so a resumed round
	// adopts the slot its committed rows already carry. A finished round
	// writes no more rows, and a discarded cursor has no rows to match.
	if cursor.CapturedSlot != 0 {
		round.CapturedSlot = cursor.CapturedSlot
	}
	return cursor, index, nil
}

// stakeRewardPrecomputeChunk computes and persists Pass 2 for the chunk of
// pools starting at startIndex, advancing cursor, and returns the next pool
// index.
func (e *Engine) stakeRewardPrecomputeChunk(ctx context.Context,
	txn ledgerdb.Txn,
	round *StakeRewardPrecomputeRound,
	cursor *RewardPrecomputeCursor,
	startIndex int,
) (int, error) {
	ldb := e.ledgerDB()
	var err error
	endIndex := RewardPrecomputeChunkEnd(
		round.PoolInputs,
		startIndex,
		e.RewardPrecomputeChunkSize(),
		rewardPrecomputeChunkDelegators,
	)
	chunkPools := round.PoolInputs[startIndex:endIndex]

	stakeInputs, err := ldb.GetRewardStakeInputsInPoolKeyHashRange(
		round.Epochs.Snapshot,
		chunkPools[0].PoolKeyHash,
		chunkPools[len(chunkPools)-1].PoolKeyHash,
		txn,
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
	activeAccounts, err := rewardActiveAccounts(ctx,
		ldb, txn, chunkPools, stakeInputs,
	)
	if err != nil {
		return 0, err
	}
	prefilterAccounts, err := RewardPrefilterAccounts(
		ldb, txn, chunkPools, stakeInputs,
		round.PrefilterSlot, round.Params.RequiresRewardPrefilter(),
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
			round.BlockCounts[string(input.PoolKeyHash)],
			round.TotalBlocks,
		)
		if err != nil {
			return 0, fmt.Errorf(
				"build pool for reward precompute chunk: %w",
				err,
			)
		}
		poolReward, ok := round.ByPool[pool.ID]
		if !ok {
			return 0, fmt.Errorf(
				"missing pass-1 reward result for pool %s",
				pool.ID.String(),
			)
		}
		res, err := rewards.ApplyPoolMemberRewards(
			pool, poolReward, round.Params,
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
			round.Epochs.Snapshot, round.CapturedSlot, round.BoundarySlot,
			[]rewards.PoolReward{poolReward},
		)...)
		accountOutputs = append(accountOutputs, rewardAccountOutputs(
			round.Epochs.Snapshot, round.CapturedSlot, round.BoundarySlot,
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
	if e.config.DelegatorInactivityEnabled {
		guarded, err := e.GuardedExpiredRewardCredentials(ctx,
			txn,
			&StakeRewardApplication{
				AccountOutputs:       accountOutputs,
				Epochs:               round.Epochs,
				SnapshotCapturedSlot: round.RewardSnapshot.CapturedSlot,
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
	e.host.RewardPrecomputeBeforeSave()
	if err := ldb.SaveRewardPoolOutputs(poolOutputs, txn); err != nil {
		return 0, fmt.Errorf("save reward pool output chunk: %w", err)
	}
	if err := ldb.SaveRewardAccountOutputs(
		accountOutputs, txn,
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
func (e *Engine) finishStakeRewardPrecomputeLocked(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	round *StakeRewardPrecomputeRound,
	cursor *RewardPrecomputeCursor,
) error {
	if _, _, err := rewards.ReconcileRoundTotals(
		round.Totals, cursor.EffectiveRewards, cursor.UnspendableRewards,
	); err != nil {
		return fmt.Errorf(
			"reconcile reward precompute totals for epoch %d: %w",
			round.Epochs.Snapshot,
			err,
		)
	}
	round.Pots.Rewards = types.Uint64(round.Totals.TotalRewardPot)
	if err := meta.SaveRewardAdaPots(round.Pots, metaTxn); err != nil {
		return fmt.Errorf("save precomputed reward ADA pots: %w", err)
	}
	poolOutputs, err := meta.GetRewardPoolOutputs(
		round.Epochs.Snapshot, metaTxn,
	)
	if err != nil {
		return fmt.Errorf(
			"get reward pool outputs for epoch %d: %w",
			round.Epochs.Snapshot,
			err,
		)
	}
	cursor.Done = true
	cursor.CompletedPools = len(round.PoolInputs)
	cursor.OutputFingerprint = RewardPoolOutputFingerprint(poolOutputs)
	if err := saveRewardPrecomputeCursor(meta, metaTxn, cursor); err != nil {
		return err
	}
	e.rewardFactsAfterCommit(
		metaTxn,
		round.Epochs.Snapshot,
		round.CapturedSlot,
	)
	e.config.Logger.Info(
		"precomputed stake rewards",
		"component", "ledger",
		"reward_snapshot_epoch", round.Epochs.Snapshot,
		"performance_epoch", round.Epochs.Performance,
		"pots_epoch", round.Epochs.Pots,
		"application_epoch", round.NewEpoch,
		"captured_slot", round.CapturedSlot,
		"boundary_slot", round.BoundarySlot,
		"total_reward_pot", round.Totals.TotalRewardPot,
		"available_rewards", round.Totals.AvailableRewards,
		"pool_count", len(round.PoolInputs),
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
func (e *Engine) rewardPrecomputeInputFingerprint(
	round *StakeRewardPrecomputeRound,
) string {
	h := sha256.New()
	fmt.Fprintf(
		h, "v2;%d;%d;%d;%d;%d;%t;",
		round.NewEpoch, round.BoundarySlot, round.Epochs.Snapshot,
		round.Epochs.Performance, round.Epochs.Pots, round.Epochs.Bootstrap,
	)
	pots := round.Pots
	fmt.Fprintf(
		h, "pots;%d;%d;%d;%d;%d;",
		pots.Epoch, uint64(pots.Treasury), uint64(pots.Reserves),
		uint64(pots.Fees), pots.CapturedSlot,
	)
	writeOptionalUint64Fingerprint(h, pots.ImportedEpochFees)
	snap := round.RewardSnapshot
	fmt.Fprintf(
		h, "snap;%d;%s;%d;%d;%d;%d;%d;%x;%d;%t;%d;",
		snap.Epoch, snap.SnapshotType, uint64(snap.TotalActiveStake),
		snap.TotalPoolCount, snap.TotalDelegators, snap.CapturedSlot,
		snap.BoundarySlot, snap.EpochNonce, snap.ProtocolVersion,
		snap.Authoritative, snap.CalculationVersion,
	)
	writeOptionalUint64Fingerprint(h, snap.ExcludedActiveStake)
	for _, input := range round.PoolInputs {
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
	params := round.Params
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
		params.FullPotRewardsEnabled, e.config.DelegatorInactivityEnabled,
		e.config.DelegatorInactivity,
	)
	pools := make([]string, 0, len(round.BlockCounts))
	for pool := range round.BlockCounts {
		pools = append(pools, pool)
	}
	slices.Sort(pools)
	fmt.Fprintf(h, "blocks;%d;", round.TotalBlocks)
	for _, pool := range pools {
		fmt.Fprintf(h, "%x=%d;", pool, round.BlockCounts[pool])
	}
	fmt.Fprintf(
		h, "prefilter;%d;%t;",
		round.PrefilterSlot, params.RequiresRewardPrefilter(),
	)
	return hex.EncodeToString(h.Sum(nil))
}

// RewardPoolOutputFingerprint identifies a finished round's pool outputs.
func RewardPoolOutputFingerprint(outputs []*models.RewardPoolOutput) string {
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
