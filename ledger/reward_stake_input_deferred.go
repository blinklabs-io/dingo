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
	"log/slog"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
)

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
	return ls.ledgerDB().GetSyncState(epochengine.RewardStakeInputsPendingKey, nil)
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
	pending, err := epochengine.LoadRewardStakeInputsPending(ls.ledgerDB(), txn)
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

// completePendingRewardStakeInputs runs EnsureRewardStakeInputsReady for the
// snapshot a round reads in its own write transaction, so a background
// precompute can rebuild rows its read-only resolution cannot.
func (ls *LedgerState) completePendingRewardStakeInputs(newEpoch uint64) error {
	epochs, ok := epochengine.StakeRewardEpochsForApplication(newEpoch)
	if !ok || epochs.Bootstrap {
		return nil
	}
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()
	if ls.rewardInputRollbackActive.Load() != 0 {
		return nil
	}
	txn := ls.ledgerDB().Begin(context.Background(), true)
	err := ls.ledgerDB().Run(txn, func(txn ledgerdb.Txn) error {
		return ls.engine().EnsureRewardStakeInputsReady(txn, epochs.Snapshot)
	})
	if errors.Is(err, epochengine.ErrRewardStakeInputsNotReady) {
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
	pending, err := epochengine.LoadRewardStakeInputsPending(ldb, nil)
	if err != nil {
		return err
	}
	for _, entry := range pending.Entries {
		txn := ldb.Begin(ctx, true)
		err := ldb.Run(txn, func(txn ledgerdb.Txn) error {
			return ls.engine().EnsureRewardStakeInputsReadyIn(ldb, txn, entry.Epoch)
		})
		switch {
		case err == nil:
		case errors.Is(err, epochengine.ErrRewardStakeInputsUnrecoverable):
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

// RewardStakeInputsPendingEntry is one snapshot whose per-credential reward
// basis is held only in part.
type RewardStakeInputsPendingEntry = epochengine.RewardStakeInputsPendingEntry

// ParseRewardStakeInputsPending decodes the pending record encoded by
// FormatRewardStakeInputsPending.
func ParseRewardStakeInputsPending(
	raw string,
) ([]RewardStakeInputsPendingEntry, error) {
	return epochengine.ParseRewardStakeInputsPending(raw)
}

// FormatRewardStakeInputsPending encodes entries as the pending record.
func FormatRewardStakeInputsPending(
	entries []RewardStakeInputsPendingEntry,
) (string, error) {
	return epochengine.FormatRewardStakeInputsPending(entries)
}
