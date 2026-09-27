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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// A reward round a boundary applies from the per-pool precompute is recorded
// as pending (models.RewardCreditRound) instead of being credited account by
// account in the boundary transaction. From that boundary on, a credential's
// balance is its account reward plus its spendable, unguarded outputs in a
// pending round that have no journal row yet; the store adds those outputs
// wherever it reads balances in aggregate (the SNAP stake read and DRep voting
// power), and pendingRewardCredit adds them for a single credential. The
// credits are written in the background by foldPendingRewardCredits, at the
// boundary slot, with the same journal rows the boundary would have written,
// so a rollback below the boundary reverts them the same way.

// rewardCreditFoldChunkPools bounds each background fold transaction.
const rewardCreditFoldChunkPools = 100

type rewardCreditFoldCursor struct {
	BoundarySlot uint64 `json:"boundary_slot"`
	NextPool     int    `json:"next_pool"`
}

func rewardCreditFoldCursorKey(snapshotEpoch uint64) string {
	return stakeRewardSourcePrefix + "fold-cursor:" +
		strconv.FormatUint(snapshotEpoch, 10)
}

func loadRewardCreditFoldCursor(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	round models.RewardCreditRound,
) (int, error) {
	raw, err := meta.GetSyncState(
		rewardCreditFoldCursorKey(round.SnapshotEpoch), metaTxn,
	)
	if err != nil || raw == "" {
		return 0, err
	}
	var cursor rewardCreditFoldCursor
	if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
		return 0, fmt.Errorf("decode reward credit fold cursor: %w", err)
	}
	if cursor.BoundarySlot != round.BoundarySlot {
		return 0, nil
	}
	return cursor.NextPool, nil
}

func registerPendingRewardCreditRound(
	meta metadata.MetadataStore,
	metaTxn types.Txn,
	round models.RewardCreditRound,
) error {
	rounds, err := meta.GetPendingRewardCreditRounds(metaTxn)
	if err != nil {
		return err
	}
	rounds = slices.DeleteFunc(rounds, func(r models.RewardCreditRound) bool {
		return r.SnapshotEpoch == round.SnapshotEpoch
	})
	rounds = append(rounds, round)
	if err := meta.DeleteSyncState(
		rewardCreditFoldCursorKey(round.SnapshotEpoch), metaTxn,
	); err != nil {
		return err
	}
	return meta.SetPendingRewardCreditRounds(rounds, metaTxn)
}

func pendingRoundIndex(
	rounds []models.RewardCreditRound,
	round models.RewardCreditRound,
) int {
	return slices.IndexFunc(rounds, func(r models.RewardCreditRound) bool {
		return r == round
	})
}

// foldRewardCreditChunk credits up to maxPools pools of a pending round in
// txn (maxPools <= 0 means all of them) and reports whether the round is now
// fully credited, in which case it is no longer pending.
func (ls *LedgerState) foldRewardCreditChunk(
	txn *database.Txn,
	round models.RewardCreditRound,
	maxPools int,
) (bool, error) {
	meta := ls.db.Metadata()
	metaTxn := txn.Metadata()
	rounds, err := meta.GetPendingRewardCreditRounds(metaTxn)
	if err != nil {
		return false, err
	}
	index := pendingRoundIndex(rounds, round)
	if index < 0 {
		return true, nil
	}
	pools, err := meta.GetRewardPoolOutputs(round.SnapshotEpoch, metaTxn)
	if err != nil {
		return false, fmt.Errorf(
			"get reward pool outputs for epoch %d: %w",
			round.SnapshotEpoch, err,
		)
	}
	slices.SortFunc(pools, func(a, b *models.RewardPoolOutput) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	next, err := loadRewardCreditFoldCursor(meta, metaTxn, round)
	if err != nil {
		return false, err
	}
	end := len(pools)
	if maxPools > 0 {
		end = min(next+maxPools, len(pools))
	}
	if next < end {
		outputs, err := meta.GetRewardAccountOutputsInPoolKeyHashRange(
			round.SnapshotEpoch,
			pools[next].PoolKeyHash,
			pools[end-1].PoolKeyHash,
			metaTxn,
		)
		if err != nil {
			return false, err
		}
		credits, err := rewardCreditsForOutputs(round, outputs)
		if err != nil {
			return false, err
		}
		if err := ls.db.AddAccountRewardsByCredential(credits, txn); err != nil {
			return false, fmt.Errorf(
				"credit stake rewards epoch %d: %w", round.SnapshotEpoch, err,
			)
		}
	}
	if end < len(pools) {
		raw, err := json.Marshal(rewardCreditFoldCursor{
			BoundarySlot: round.BoundarySlot, NextPool: end,
		})
		if err != nil {
			return false, err
		}
		return false, meta.SetSyncState(
			rewardCreditFoldCursorKey(
				round.SnapshotEpoch,
			),
			string(raw),
			metaTxn,
		)
	}
	if err := meta.DeleteSyncState(
		rewardCreditFoldCursorKey(round.SnapshotEpoch), metaTxn,
	); err != nil {
		return false, err
	}
	rounds = slices.Delete(rounds, index, index+1)
	if err := meta.SetPendingRewardCreditRounds(rounds, metaTxn); err != nil {
		return false, err
	}
	ls.config.Logger.Info(
		"credited stake rewards",
		"component", "ledger",
		"reward_snapshot_epoch", round.SnapshotEpoch,
		"boundary_slot", round.BoundarySlot,
	)
	return true, nil
}

// rewardCreditsForOutputs returns the credits a boundary writes for outputs:
// every spendable, unguarded output, journaled at the round's boundary slot.
func rewardCreditsForOutputs(
	round models.RewardCreditRound,
	outputs []*models.RewardAccountOutput,
) ([]models.AccountRewardCredit, error) {
	credits := make([]models.AccountRewardCredit, 0, len(outputs))
	for _, output := range outputs {
		if output == nil || !output.Spendable || output.Guarded {
			continue
		}
		reward, err := rewardFromAccountOutput(output)
		if err != nil {
			return nil, err
		}
		credits = append(credits, models.AccountRewardCredit{
			CredentialTag: reward.Credential.Tag,
			StakingKey:    reward.Credential.Hash[:],
			Amount:        reward.Amount,
			Slot:          round.BoundarySlot,
			SourceHash:    stakeRewardSourceHash(round.SnapshotEpoch, reward),
		})
	}
	return credits, nil
}

// queueRewardCreditFold credits a pending round in the background, one
// bounded chunk per write transaction.
func (ls *LedgerState) queueRewardCreditFold(round models.RewardCreditRound) {
	ls.rewardPrecomputeMu.Lock()
	if ls.closed.Load() {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	ls.rewardCreditFoldWG.Add(1)
	ls.rewardPrecomputeMu.Unlock()
	go func() {
		defer ls.rewardCreditFoldWG.Done()
		if err := ls.foldPendingRewardCredits(round); err != nil {
			ls.config.Logger.Warn(
				"failed to credit stake rewards",
				"component", "ledger",
				"reward_snapshot_epoch", round.SnapshotEpoch,
				"error", err,
			)
		}
	}()
}

// foldPendingRewardCredits runs each chunk under rewardPrecomputeWriteMu and
// stops once a rollback has started, so no chunk commits after a rollback
// that removed the round.
func (ls *LedgerState) foldPendingRewardCredits(
	round models.RewardCreditRound,
) error {
	generation := ls.rewardInputGeneration.Load()
	for {
		if ls.closed.Load() {
			return nil
		}
		if ls.rewardCreditFoldHook != nil {
			ls.rewardCreditFoldHook()
		}
		done := false
		stop := false
		ls.rewardPrecomputeWriteMu.Lock()
		txn := ls.db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			if ls.rewardInputRollbackActive.Load() != 0 ||
				ls.rewardInputGeneration.Load() != generation {
				stop = true
				return nil
			}
			var err error
			done, err = ls.foldRewardCreditChunk(
				txn, round, rewardCreditFoldChunkPools,
			)
			return err
		})
		ls.rewardPrecomputeWriteMu.Unlock()
		if err != nil || done {
			return err
		}
		if stop {
			// A rollback that keeps the round leaves it pending; fold it
			// again under the new generation.
			generation = ls.rewardInputGeneration.Load()
			if ls.rewardInputRollbackActive.Load() != 0 {
				return nil
			}
		}
	}
}

// resumePendingRewardCreditFolds restarts the background fold of every round
// still pending, as after a restart.
func (ls *LedgerState) resumePendingRewardCreditFolds() {
	rounds, err := ls.db.Metadata().GetPendingRewardCreditRounds(nil)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to load pending reward credit rounds",
			"component", "ledger",
			"error", err,
		)
		return
	}
	for _, round := range rounds {
		ls.queueRewardCreditFold(round)
	}
}

// finishPendingRewardCreditsInTxn credits, in txn, every pending round applied
// at a boundary before boundarySlot. A boundary runs it first: the store adds
// only a pending round's unjournaled outputs to single-credential balances,
// but adds all of them to the aggregate reads at the next SNAP point, which
// is exact only for a round nothing has partially credited out of order.
func (ls *LedgerState) finishPendingRewardCreditsInTxn(
	txn *database.Txn,
	boundarySlot uint64,
) error {
	rounds, err := ls.db.Metadata().GetPendingRewardCreditRounds(
		txn.Metadata(),
	)
	if err != nil {
		return err
	}
	for _, round := range rounds {
		if round.BoundarySlot >= boundarySlot {
			continue
		}
		if _, err := ls.foldRewardCreditChunk(txn, round, 0); err != nil {
			return err
		}
	}
	return nil
}

// pendingRewardCreditOutputs returns the credits of the pending rounds for
// one credential that have not been journaled yet.
func (ls *LedgerState) pendingRewardCreditOutputs(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) ([]models.AccountRewardCredit, error) {
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	meta := ls.db.Metadata()
	rounds, err := meta.GetPendingRewardCreditRounds(metaTxn)
	if err != nil || len(rounds) == 0 {
		return nil, err
	}
	epochs := make([]uint64, 0, len(rounds))
	boundaryByEpoch := make(map[uint64]models.RewardCreditRound, len(rounds))
	for _, round := range rounds {
		epochs = append(epochs, round.SnapshotEpoch)
		boundaryByEpoch[round.SnapshotEpoch] = round
	}
	outputs, err := meta.GetRewardAccountOutputsForCredential(
		epochs, credentialTag, stakingKey, metaTxn,
	)
	if err != nil {
		return nil, err
	}
	var credits []models.AccountRewardCredit
	for _, output := range outputs {
		roundCredits, err := rewardCreditsForOutputs(
			boundaryByEpoch[output.Epoch],
			[]*models.RewardAccountOutput{output},
		)
		if err != nil {
			return nil, err
		}
		credits = append(credits, roundCredits...)
	}
	if len(credits) == 0 {
		return nil, nil
	}
	applied, err := meta.RewardCreditsAlreadyApplied(credits, metaTxn)
	if err != nil {
		return nil, err
	}
	pending := credits[:0]
	for i, credit := range credits {
		if !applied[i] {
			pending = append(pending, credit)
		}
	}
	return pending, nil
}

// pendingRewardCredit is the part of a credential's reward balance a pending
// round has not written to its account yet.
func (ls *LedgerState) pendingRewardCredit(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	credits, err := ls.pendingRewardCreditOutputs(
		txn,
		credentialTag,
		stakingKey,
	)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, credit := range credits {
		sum, overflow := addRewardUint64(total, credit.Amount)
		if overflow {
			return 0, fmt.Errorf(
				"pending reward credit overflow for %x", stakingKey,
			)
		}
		total = sum
	}
	return total, nil
}

// PendingRewardCredit returns the part of a credential's reward balance that
// an applied reward round has not written to its account row yet. A reader of
// the account row's reward adds it, read in the same txn, to report the
// balance.
func (ls *LedgerState) PendingRewardCredit(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) (uint64, error) {
	return ls.pendingRewardCredit(txn, credentialTag, stakingKey)
}

// foldRewardCreditFor writes one credential's pending credits in txn, before
// a write that reads its stored balance.
func (ls *LedgerState) foldRewardCreditFor(
	txn *database.Txn,
	credentialTag uint8,
	stakingKey []byte,
) error {
	credits, err := ls.pendingRewardCreditOutputs(
		txn,
		credentialTag,
		stakingKey,
	)
	if err != nil || len(credits) == 0 {
		return err
	}
	return ls.db.AddAccountRewardsByCredential(credits, txn)
}

// foldRewardCreditsForWithdrawals writes the pending reward credits of every
// credential tx withdraws from, before the withdrawal reads its balance.
func (ls *LedgerState) foldRewardCreditsForWithdrawals(
	tx lcommon.Transaction,
	txn *database.Txn,
) error {
	withdrawals := tx.Withdrawals()
	if len(withdrawals) == 0 {
		return nil
	}
	var metaTxn types.Txn
	if txn != nil {
		metaTxn = txn.Metadata()
	}
	rounds, err := ls.db.Metadata().GetPendingRewardCreditRounds(metaTxn)
	if err != nil || len(rounds) == 0 {
		return err
	}
	for address := range withdrawals {
		if address == nil {
			continue
		}
		tag, ok := models.StakeCredentialTagFromAddress(*address)
		if !ok {
			continue
		}
		stakeKey := address.StakeKeyHash()
		if err := ls.foldRewardCreditFor(txn, tag, stakeKey.Bytes()); err != nil {
			return fmt.Errorf(
				"credit pending rewards before withdrawal: %w",
				err,
			)
		}
	}
	return nil
}
