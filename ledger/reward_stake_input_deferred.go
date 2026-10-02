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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

// A mark snapshot's per-credential reward basis (reward_stake_input) is first
// read by the reward round two epochs later, so the boundary commits every
// other row of the snapshot and writes these after it commits. The snapshot's
// entry in rewardStakeInputsPendingKey marks its rows as incomplete until the
// last chunk commits; every reader of reward_stake_input goes through
// ensureRewardStakeInputsReady first.
const rewardStakeInputsPendingKey = stakeRewardSourcePrefix +
	"stake-inputs-pending"

// deferredStakeInputChunk bounds each write transaction, so the writer never
// holds the metadata writer for the whole snapshot.
const deferredStakeInputChunk = 50_000

type rewardStakeInputsPendingEntry struct {
	Epoch        uint64 `json:"epoch"`
	BoundarySlot uint64 `json:"boundary_slot"`
}

type rewardStakeInputsPending struct {
	Entries []rewardStakeInputsPendingEntry `json:"entries"`
}

// errRewardStakeInputsNotReady reports that a snapshot's reward basis is still
// being written by the deferred writer in this process.
var errRewardStakeInputsNotReady = errors.New(
	"reward stake inputs are still being written",
)

func loadRewardStakeInputsPending(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
) (*rewardStakeInputsPending, error) {
	raw, err := meta.GetSyncState(rewardStakeInputsPendingKey, metaTxn)
	if err != nil {
		return nil, fmt.Errorf("load pending reward stake inputs: %w", err)
	}
	pending := &rewardStakeInputsPending{}
	if raw == "" {
		return pending, nil
	}
	if err := json.Unmarshal([]byte(raw), pending); err != nil {
		return nil, fmt.Errorf("decode pending reward stake inputs: %w", err)
	}
	return pending, nil
}

func saveRewardStakeInputsPending(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	pending *rewardStakeInputsPending,
) error {
	raw, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("encode pending reward stake inputs: %w", err)
	}
	if err := meta.SetSyncState(
		rewardStakeInputsPendingKey, string(raw), metaTxn,
	); err != nil {
		return fmt.Errorf("save pending reward stake inputs: %w", err)
	}
	return nil
}

func (p *rewardStakeInputsPending) find(epoch uint64) int {
	return slices.IndexFunc(
		p.Entries,
		func(e rewardStakeInputsPendingEntry) bool {
			return e.Epoch == epoch
		},
	)
}

// markRewardStakeInputsPending records, in the boundary transaction, that the
// snapshot for epoch has not had its reward_stake_input rows written yet.
func markRewardStakeInputsPending(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	epoch uint64,
	boundarySlot uint64,
) error {
	pending, err := loadRewardStakeInputsPending(meta, metaTxn)
	if err != nil {
		return err
	}
	entry := rewardStakeInputsPendingEntry{
		Epoch: epoch, BoundarySlot: boundarySlot,
	}
	if i := pending.find(epoch); i >= 0 {
		pending.Entries[i] = entry
	} else {
		pending.Entries = append(pending.Entries, entry)
	}
	return saveRewardStakeInputsPending(meta, metaTxn, pending)
}

func clearRewardStakeInputsPending(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	epoch uint64,
) error {
	pending, err := loadRewardStakeInputsPending(meta, metaTxn)
	if err != nil {
		return err
	}
	i := pending.find(epoch)
	if i < 0 {
		return nil
	}
	pending.Entries = slices.Delete(pending.Entries, i, i+1)
	return saveRewardStakeInputsPending(meta, metaTxn, pending)
}

// takeDeferredRewardStakeInputs hands the reward_stake_input rows the
// boundary capture staged in txn to a background writer that starts once txn
// commits.
func (ls *LedgerState) takeDeferredRewardStakeInputs(
	ctx context.Context,
	txn *database.Txn,
) error {
	hook := ls.epochBoundaryDeferredStakeInputsHook()
	if hook == nil {
		return nil
	}
	epoch, boundarySlot, inputs, ok := hook(txn)
	if !ok {
		return nil
	}
	if err := markRewardStakeInputsPending(
		ls.db.Metadata(), txn.Metadata(), epoch, boundarySlot,
	); err != nil {
		return err
	}
	generation := ls.rewardInputGeneration.Load()
	txn.AfterCommit(func() {
		ls.queueDeferredRewardStakeInputs(
			context.WithoutCancel(ctx),
			epoch, boundarySlot, inputs, generation,
		)
	})
	return nil
}

func (ls *LedgerState) queueDeferredRewardStakeInputs(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
	inputs []*models.RewardStakeInput,
	generation uint64,
) {
	ls.rewardPrecomputeMu.Lock()
	if ls.closed.Load() {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	ls.deferredStakeInputsWG.Add(1)
	if ls.deferredStakeInputsWriting == nil {
		ls.deferredStakeInputsWriting = make(map[uint64]struct{})
	}
	ls.deferredStakeInputsWriting[epoch] = struct{}{}
	ls.rewardPrecomputeMu.Unlock()
	go func() {
		defer ls.deferredStakeInputsWG.Done()
		defer func() {
			ls.rewardPrecomputeMu.Lock()
			delete(ls.deferredStakeInputsWriting, epoch)
			ls.rewardPrecomputeMu.Unlock()
		}()
		// The write is idempotent and stops by itself once a rollback or a
		// rebuild has taken the entry, so a failed attempt is retried: with
		// no writer left, only a read-write reader could rebuild the rows.
		backoff := deferredStakeInputRetryMin
		for {
			err := ls.writeDeferredRewardStakeInputs(
				ctx,
				epoch, boundarySlot, inputs, generation,
			)
			if err == nil {
				return
			}
			ls.config.Logger.Warn(
				"failed to write deferred reward stake inputs; retrying",
				"component", "ledger",
				"epoch", epoch,
				"retry_in", backoff,
				"error", err,
			)
			if !ls.waitUnlessClosed(backoff) {
				return
			}
			backoff = min(backoff*2, deferredStakeInputRetryMax)
		}
	}()
}

const (
	deferredStakeInputRetryMin = 100 * time.Millisecond
	deferredStakeInputRetryMax = 30 * time.Second
)

// waitUnlessClosed waits d, returning false as soon as the ledger closes.
func (ls *LedgerState) waitUnlessClosed(d time.Duration) bool {
	const poll = 100 * time.Millisecond
	for waited := time.Duration(0); waited < d; waited += poll {
		if ls.closed.Load() {
			return false
		}
		time.Sleep(min(poll, d-waited))
	}
	return !ls.closed.Load()
}

// writeDeferredRewardStakeInputs writes one snapshot's staged rows in chunks.
// Each chunk runs under rewardPrecomputeWriteMu and stops once a rollback has
// started since the boundary committed, so no chunk commits after a rollback
// that deleted the snapshot: its entry stays pending and a later reader
// rebuilds the rows.
func (ls *LedgerState) writeDeferredRewardStakeInputs(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
	inputs []*models.RewardStakeInput,
	generation uint64,
) error {
	for start := 0; ; start += deferredStakeInputChunk {
		if ls.closed.Load() {
			return nil
		}
		end := min(start+deferredStakeInputChunk, len(inputs))
		final := end == len(inputs)
		stop := false
		ls.rewardPrecomputeWriteMu.Lock()
		txn := ls.db.Transaction(ctx, true)
		err := txn.Do(func(txn *database.Txn) error {
			if ls.rewardInputRollbackActive.Load() != 0 ||
				ls.rewardInputGeneration.Load() != generation {
				stop = true
				return nil
			}
			if ls.deferredStakeInputsFailHook != nil {
				if err := ls.deferredStakeInputsFailHook(); err != nil {
					return err
				}
			}
			meta := ls.db.Metadata()
			metaTxn := txn.Metadata()
			pending, err := loadRewardStakeInputsPending(meta, metaTxn)
			if err != nil {
				return err
			}
			i := pending.find(epoch)
			if i < 0 || pending.Entries[i].BoundarySlot != boundarySlot {
				stop = true
				return nil
			}
			if err := meta.SaveRewardStakeInputs(
				inputs[start:end], metaTxn,
			); err != nil {
				return fmt.Errorf(
					"save deferred reward stake inputs for epoch %d: %w",
					epoch, err,
				)
			}
			if final {
				return clearRewardStakeInputsPending(meta, metaTxn, epoch)
			}
			return nil
		})
		ls.rewardPrecomputeWriteMu.Unlock()
		if err != nil || stop {
			return err
		}
		if final {
			// A precompute that found these rows not ready yet was dropped;
			// requeue the round in progress.
			ls.queueStartupRewardPrecompute()
			return nil
		}
	}
}

// ensureRewardStakeInputsReady makes a snapshot's reward_stake_input rows
// complete before a reader uses them. While this process is still writing
// them it returns errRewardStakeInputsNotReady. A pending entry with no writer
// -- the node stopped, or a rollback stopped the writer -- is completed here
// from the historical reconstruction the retention path already uses,
// checked against the snapshot's persisted pool inputs, in txn.
func (ls *LedgerState) ensureRewardStakeInputsReady(
	txn *database.Txn,
	epoch uint64,
) error {
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()
	pending, err := loadRewardStakeInputsPending(meta, metaTxn)
	if err != nil {
		return err
	}
	i := pending.find(epoch)
	if i < 0 {
		return nil
	}
	ls.rewardPrecomputeMu.Lock()
	_, writing := ls.deferredStakeInputsWriting[epoch]
	ls.rewardPrecomputeMu.Unlock()
	if writing {
		return errRewardStakeInputsNotReady
	}
	if !txn.IsReadWrite() {
		return errRewardStakeInputsNotReady
	}
	snapshot, err := meta.GetRewardSnapshot(epoch, "mark", metaTxn)
	if err != nil {
		return fmt.Errorf("get reward snapshot for epoch %d: %w", epoch, err)
	}
	if snapshot == nil ||
		snapshot.BoundarySlot != pending.Entries[i].BoundarySlot {
		// The snapshot the entry describes was rolled back.
		return clearRewardStakeInputsPending(meta, metaTxn, epoch)
	}
	poolInputs, err := meta.GetRewardPoolInputs(epoch, metaTxn)
	if err != nil {
		return fmt.Errorf(
			"get reward pool inputs for epoch %d: %w", epoch, err,
		)
	}
	rebuilt, err := ls.rebuildPrunedRewardStakeInputs(
		meta, metaTxn, epoch, snapshot, poolInputs,
	)
	if err != nil {
		return fmt.Errorf(
			"rebuild pending reward stake inputs for epoch %d: %w",
			epoch, err,
		)
	}
	if len(rebuilt) == 0 && snapshot.TotalDelegators > 0 {
		return fmt.Errorf(
			"rebuild pending reward stake inputs for epoch %d returned no rows for %d snapshot delegators",
			epoch,
			snapshot.TotalDelegators,
		)
	}
	if err := meta.DeleteRewardInputsForEpoch(epoch, metaTxn); err != nil {
		return fmt.Errorf(
			"clear partial reward inputs for epoch %d: %w", epoch, err,
		)
	}
	if err := meta.SaveRewardPoolInputs(poolInputs, metaTxn); err != nil {
		return fmt.Errorf(
			"restore reward pool inputs for epoch %d: %w", epoch, err,
		)
	}
	if err := meta.SaveRewardStakeInputs(rebuilt, metaTxn); err != nil {
		return fmt.Errorf(
			"save rebuilt reward stake inputs for epoch %d: %w", epoch, err,
		)
	}
	ls.config.Logger.Info(
		"rebuilt reward stake inputs a deferred write did not finish",
		"component", "ledger",
		"epoch", epoch,
		"rows", len(rebuilt),
	)
	return clearRewardStakeInputsPending(meta, metaTxn, epoch)
}

// completePendingRewardStakeInputs runs ensureRewardStakeInputsReady for the
// snapshot a round reads in its own write transaction, so a background
// precompute can rebuild rows its read-only resolution cannot.
func (ls *LedgerState) completePendingRewardStakeInputs(newEpoch uint64) error {
	epochs, ok := stakeRewardEpochsForApplication(newEpoch)
	if !ok || epochs.bootstrap {
		return nil
	}
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()
	if ls.rewardInputRollbackActive.Load() != 0 {
		return nil
	}
	txn := ls.db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		return ls.ensureRewardStakeInputsReady(txn, epochs.snapshot)
	})
	if errors.Is(err, errRewardStakeInputsNotReady) {
		return nil
	}
	return err
}
