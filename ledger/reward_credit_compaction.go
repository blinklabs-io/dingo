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
	"fmt"

	"github.com/blinklabs-io/dingo/database"
)

// rewardCreditRoundsKeptUnfolded is how many of the newest credited rounds
// compaction leaves unfolded. Folding never changes a balance, so no reader
// needs a round unfolded; the newest two are left alone because a rollback
// that stays within the stability window can reach the boundary that applied
// them, and undoing folded credits there means reverting their journal rows as
// well as their flags.
const rewardCreditRoundsKeptUnfolded = 2

// rewardCreditCompactionChunk bounds the credits each compaction transaction
// writes, and with it how long the job holds the metadata write lock.
const rewardCreditCompactionChunk = 1_000

// queueRewardCreditCompaction folds credited rounds older than the newest
// rewardCreditRoundsKeptUnfolded into account rows in the background, which
// keeps the derived-balance sums to the last few rounds. A request while the
// job runs makes it look again once it finishes.
func (ls *LedgerState) queueRewardCreditCompaction() {
	ls.rewardPrecomputeMu.Lock()
	defer ls.rewardPrecomputeMu.Unlock()
	if ls.closed.Load() {
		return
	}
	if ls.rewardCreditCompacting {
		ls.rewardCreditCompactAgain = true
		return
	}
	ls.rewardCreditCompacting = true
	ls.rewardCreditCompactionWG.Add(1)
	go func() {
		defer ls.rewardCreditCompactionWG.Done()
		for {
			if err := ls.compactRewardCreditRounds(); err != nil {
				ls.config.Logger.Warn(
					"failed to compact credited reward rounds",
					"component", "ledger",
					"error", err,
				)
			}
			ls.rewardPrecomputeMu.Lock()
			again := ls.rewardCreditCompactAgain && !ls.closed.Load()
			ls.rewardCreditCompactAgain = false
			if !again {
				ls.rewardCreditCompacting = false
			}
			ls.rewardPrecomputeMu.Unlock()
			if !again {
				return
			}
		}
	}()
}

// compactRewardCreditRounds folds, one bounded transaction at a time, every
// credited round older than the newest rewardCreditRoundsKeptUnfolded. The
// folded flag is the progress record: each transaction claims and writes the
// next unfolded credits, so a stopped job resumes where it left off.
func (ls *LedgerState) compactRewardCreditRounds() error {
	for {
		if ls.closed.Load() {
			return nil
		}
		done, err := ls.compactRewardCreditChunk(rewardCreditCompactionChunk)
		if err != nil || done {
			return err
		}
	}
}

// compactRewardCreditChunk folds up to limit credits of the oldest round that
// is due and reports whether no round is due.
func (ls *LedgerState) compactRewardCreditChunk(limit int) (bool, error) {
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()
	if ls.rewardInputRollbackActive.Load() != 0 {
		return true, nil
	}
	done := true
	txn := ls.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		meta := ls.db.Metadata()
		metaTxn := txn.Metadata()
		rounds, err := meta.GetPendingRewardCreditRounds(metaTxn)
		if err != nil {
			return err
		}
		if len(rounds) <= rewardCreditRoundsKeptUnfolded {
			return nil
		}
		for _, round := range rounds[:len(rounds)-rewardCreditRoundsKeptUnfolded] {
			outputs, err := meta.ClaimUnfoldedRewardCredits(
				round.SnapshotEpoch, limit, metaTxn,
			)
			if err != nil {
				return err
			}
			if len(outputs) == 0 {
				continue
			}
			done = false
			if err := ls.writeClaimedRewardCredits(txn, outputs); err != nil {
				return fmt.Errorf(
					"compact reward round %d: %w", round.SnapshotEpoch, err,
				)
			}
			return nil
		}
		return nil
	})
	return done, err
}
