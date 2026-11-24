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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// rewardStakeInputsPendingKey names the record of the mark snapshots whose
// per-credential reward basis (reward_stake_input) is held only in part: a
// store migrated from one that wrote those rows after the boundary committed
// and stopped before the last of them. The set keeps one row per such
// snapshot; every reader of reward_stake_input goes through
// ensureRewardStakeInputsReady first, which rebuilds the rows.
const rewardStakeInputsPendingKey = models.RewardStakeInputsPendingKey

// RewardStakeInputsPendingEntry names a snapshot whose stake rows are not
// all written.
type RewardStakeInputsPendingEntry struct {
	Epoch        uint64 `json:"epoch"`
	BoundarySlot uint64 `json:"boundary_slot"`
}

type rewardStakeInputsPending struct {
	Entries []RewardStakeInputsPendingEntry `json:"entries"`
}

// errRewardStakeInputsNotReady reports that a snapshot's reward basis is
// incomplete and the reader's transaction cannot rebuild it.
var errRewardStakeInputsNotReady = errors.New(
	"reward stake inputs are incomplete",
)

var errRewardStakeInputsUnrecoverable = errors.New(
	"reward stake inputs cannot be reconstructed",
)

func loadRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
) (*rewardStakeInputsPending, error) {
	raw, err := meta.GetSyncState(rewardStakeInputsPendingKey, metaTxn)
	if err != nil {
		return nil, fmt.Errorf("load pending reward stake inputs: %w", err)
	}
	return decodeRewardStakeInputsPending(raw)
}

func decodeRewardStakeInputsPending(
	raw string,
) (*rewardStakeInputsPending, error) {
	pending := &rewardStakeInputsPending{}
	if raw == "" {
		return pending, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return nil, fmt.Errorf("decode pending reward stake inputs: %w", err)
	}
	entries, ok := envelope["entries"]
	if !ok || len(envelope) != 1 {
		return nil, errors.New("decode pending reward stake inputs: unknown state format")
	}
	decoder := json.NewDecoder(bytes.NewReader(entries))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&pending.Entries); err != nil {
		return nil, fmt.Errorf("decode pending reward stake inputs: %w", err)
	}
	return pending, nil
}

// ParseRewardStakeInputsPending decodes the value of the pending sync_state
// key, rejecting a format it does not know.
func ParseRewardStakeInputsPending(
	raw string,
) ([]RewardStakeInputsPendingEntry, error) {
	pending, err := decodeRewardStakeInputsPending(raw)
	if err != nil {
		return nil, err
	}
	return pending.Entries, nil
}

// FormatRewardStakeInputsPending is the pending sync_state value naming
// entries, empty when there are none.
func FormatRewardStakeInputsPending(
	entries []RewardStakeInputsPendingEntry,
) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(&rewardStakeInputsPending{Entries: entries})
	if err != nil {
		return "", fmt.Errorf("encode pending reward stake inputs: %w", err)
	}
	return string(raw), nil
}

func saveRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
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
		func(e RewardStakeInputsPendingEntry) bool {
			return e.Epoch == epoch
		},
	)
}

func clearRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
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

// takeDeferredRewardStakeInputs stages the reward_stake_input rows the
// boundary capture held back from txn, so they commit with the boundary.
func (ls *LedgerState) takeDeferredRewardStakeInputs(
	txn ledgerdb.Txn,
) error {
	hook := ls.epochBoundaryDeferredStakeInputsHook()
	if hook == nil {
		return nil
	}
	_, _, inputs, ok := hook(txn)
	if !ok {
		return nil
	}
	return ls.ledgerDB().SaveRewardStakeInputs(inputs, txn)
}

// RewardStakeInputsIncomplete reports whether any snapshot labelled in
// [from, through) still has incomplete reward_stake_input rows: no reward
// round has rebuilt it yet.
// A read-only reader cannot complete them, so it must not present the
// snapshot's rows as the whole distribution.
func (ls *LedgerState) RewardStakeInputsIncomplete(
	txn ledgerdb.Txn,
	from uint64,
	through uint64,
) (bool, error) {
	return ls.rewardStakeInputsIncomplete(txn, from, through)
}

// RewardStakeInputsPendingState returns the pending record in the encoding
// ParseRewardStakeInputsPending reads, or the empty string when no snapshot
// is pending.
func (ls *LedgerState) RewardStakeInputsPendingState() (string, error) {
	return ls.ledgerDB().GetSyncState(rewardStakeInputsPendingKey, nil)
}

// RewardStakeInputsIncompleteCommitted is RewardStakeInputsIncomplete read
// from the ledger store's committed state, for a reader whose own
// transaction belongs to another store: the ledger store records which
// snapshots are still being written, and a split IndexerDB copies their rows
// from it.
func (ls *LedgerState) RewardStakeInputsIncompleteCommitted(
	from uint64,
	through uint64,
) (bool, error) {
	return ls.rewardStakeInputsIncomplete(nil, from, through)
}

func (ls *LedgerState) rewardStakeInputsIncomplete(
	txn ledgerdb.Txn,
	from uint64,
	through uint64,
) (bool, error) {
	pending, err := loadRewardStakeInputsPending(ls.ledgerDB(), txn)
	if err != nil {
		return false, err
	}
	for _, entry := range pending.Entries {
		if entry.Epoch >= from && entry.Epoch < through {
			return true, nil
		}
	}
	return false, nil
}

// ensureRewardStakeInputsReady makes a snapshot's reward_stake_input rows
// complete before a reader uses them. A pending snapshot is completed here
// from the historical reconstruction the retention path already uses,
// checked against the snapshot's persisted pool inputs, in txn; a read-only
// txn gets errRewardStakeInputsNotReady.
func (ls *LedgerState) ensureRewardStakeInputsReady(
	txn ledgerdb.Txn,
	epoch uint64,
) error {
	return ls.ensureRewardStakeInputsReadyIn(ls.ledgerDB(), txn, epoch)
}

// ensureRewardStakeInputsReadyIn is ensureRewardStakeInputsReady against an
// explicit store, so the metadata store can be read directly while the ledger
// store would serve the set.
func (ls *LedgerState) ensureRewardStakeInputsReadyIn(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	epoch uint64,
) error {
	metaTxn := txn
	pending, err := loadRewardStakeInputsPending(ldb, metaTxn)
	if err != nil {
		return err
	}
	i := pending.find(epoch)
	if i < 0 {
		return nil
	}
	if !txn.IsReadWrite() {
		return errRewardStakeInputsNotReady
	}
	snapshot, err := ldb.GetRewardSnapshot(epoch, "mark", metaTxn)
	if err != nil {
		return fmt.Errorf("get reward snapshot for epoch %d: %w", epoch, err)
	}
	if snapshot == nil ||
		snapshot.BoundarySlot != pending.Entries[i].BoundarySlot {
		// The snapshot the entry describes was rolled back.
		return clearRewardStakeInputsPending(ldb, metaTxn, epoch)
	}
	poolInputs, err := ldb.GetRewardPoolInputs(epoch, metaTxn)
	if err != nil {
		return fmt.Errorf(
			"get reward pool inputs for epoch %d: %w", epoch, err,
		)
	}
	rebuilt, rejected, err := ls.rebuildPrunedRewardStakeInputsDetailed(
		ldb, metaTxn, epoch, snapshot, poolInputs,
	)
	if err != nil {
		return fmt.Errorf(
			"rebuild pending reward stake inputs for epoch %d: %w",
			epoch, err,
		)
	}
	if rejected && snapshot.TotalDelegators > 0 {
		return fmt.Errorf(
			"%w: pending epoch %d reconstruction disagrees with the "+
				"retained reward_pool_input totals",
			errRewardStakeInputsUnrecoverable,
			epoch,
		)
	}
	if len(rebuilt) == 0 && snapshot.TotalDelegators > 0 {
		return fmt.Errorf(
			"%w: pending epoch %d returned no rows for %d snapshot delegators",
			errRewardStakeInputsUnrecoverable,
			epoch,
			snapshot.TotalDelegators,
		)
	}
	if err := ldb.DeleteRewardInputsForEpoch(epoch, metaTxn); err != nil {
		return fmt.Errorf(
			"clear partial reward inputs for epoch %d: %w", epoch, err,
		)
	}
	if err := ldb.SaveRewardPoolInputs(poolInputs, metaTxn); err != nil {
		return fmt.Errorf(
			"restore reward pool inputs for epoch %d: %w", epoch, err,
		)
	}
	if err := ldb.SaveRewardStakeInputs(rebuilt, metaTxn); err != nil {
		return fmt.Errorf(
			"save rebuilt reward stake inputs for epoch %d: %w", epoch, err,
		)
	}
	ls.config.Logger.Info(
		"rebuilt incomplete reward stake inputs",
		"component", "ledger",
		"epoch", epoch,
		"rows", len(rebuilt),
	)
	return clearRewardStakeInputsPending(ldb, metaTxn, epoch)
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
	txn := ls.ledgerDB().Begin(context.Background(), true)
	err := ls.ledgerDB().Run(txn, func(txn ledgerdb.Txn) error {
		return ls.ensureRewardStakeInputsReady(txn, epochs.snapshot)
	})
	if errors.Is(err, errRewardStakeInputsNotReady) {
		return nil
	}
	return err
}

// ResolveReleasedRewardStakeInputs completes every pending reward
// stake-input epoch of a v0.94.2 database from ldb, its metadata store, each
// in one write transaction. It runs in the migration out of that store,
// before the UTxO and delegation history the rebuild reads stop being the
// ledger's, and before any ledger is built over the database. An epoch whose
// history cannot reproduce the retained pool totals keeps its marker.
func ResolveReleasedRewardStakeInputs(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
	logger *slog.Logger,
	delegatorInactivityEnabled bool,
	delegatorInactivity uint64,
) error {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ls := &LedgerState{config: LedgerStateConfig{
		Logger:                     logger,
		DelegatorInactivityEnabled: delegatorInactivityEnabled,
		DelegatorInactivity:        delegatorInactivity,
	}}
	pending, err := loadRewardStakeInputsPending(ldb, nil)
	if err != nil {
		return err
	}
	for _, entry := range pending.Entries {
		txn := ldb.Begin(ctx, true)
		err := ldb.Run(txn, func(txn ledgerdb.Txn) error {
			return ls.ensureRewardStakeInputsReadyIn(ldb, txn, entry.Epoch)
		})
		switch {
		case err == nil:
		case errors.Is(err, errRewardStakeInputsUnrecoverable):
			ls.config.Logger.Warn(
				"pending reward stake inputs stay incomplete",
				"component", "ledger",
				"epoch", entry.Epoch,
				"error", err,
			)
		default:
			return err
		}
	}
	return nil
}
