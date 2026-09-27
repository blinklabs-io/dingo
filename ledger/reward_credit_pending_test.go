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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// heldRewardCreditFold runs the dump fixture's boundary with its background
// credit held, returning the fixture and a release function.
func heldRewardCreditFold(t *testing.T) (*epochBoundaryBenchFixture, func()) {
	t.Helper()
	f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	hold := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	t.Cleanup(release)
	f.ls.rewardCreditFoldHook = func() { <-hold }
	f.rollover(t)
	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Len(t, rounds, 1, "the boundary must leave its round pending")
	return f, release
}

// rewardedCredentials returns every credential the round credits, with the
// total it credits.
func rewardedCredentials(
	t *testing.T,
	f *epochBoundaryBenchFixture,
) map[string]uint64 {
	t.Helper()
	outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
	require.NoError(t, err)
	credits := make(map[string]uint64)
	for _, output := range outputs {
		if output.Spendable && !output.Guarded {
			credits[string(output.StakingKey)] += uint64(output.Amount)
		}
	}
	require.NotEmpty(t, credits)
	return credits
}

func accountRewards(
	t *testing.T,
	f *epochBoundaryBenchFixture,
	keys map[string]uint64,
) map[string]uint64 {
	t.Helper()
	ret := make(map[string]uint64, len(keys))
	for key := range keys {
		account, err := f.db.GetAccountByCredential(0, []byte(key), true, nil)
		require.NoError(t, err)
		ret[key] = uint64(account.Reward)
	}
	return ret
}

// TestPendingRewardRoundReadsMatchCreditedBalances pins that a round the
// boundary leaves pending is never observed early or late: while its credits
// are still being written, every balance read -- ledger validation, the local
// state query and DRep voting power -- already includes them, and the values
// do not move when the background write lands.
func TestPendingRewardRoundReadsMatchCreditedBalances(t *testing.T) {
	t.Parallel()
	f, release := heldRewardCreditFold(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)

	var creds []olocalstatequery.StakeCredential
	txn := f.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		view := &LedgerView{ls: f.ls, txn: txn}
		for key, credit := range credits {
			var hash lcommon.Blake2b224
			copy(hash[:], key)
			balance, err := view.RewardAccountBalance(lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(hash),
			})
			require.NoError(t, err)
			require.NotNil(t, balance)
			require.Equal(t, stored[key]+credit, *balance,
				"validation must see the pending credit of %x", key)
			creds = append(creds, olocalstatequery.StakeCredential{
				Tag: 0, Bytes: hash,
			})
		}
		return nil
	}))
	result, err := f.ls.queryShelleyFilteredDelegationAndRewardAccounts(creds)
	require.NoError(t, err)
	_, rewardsDuring := unwrapFilteredDelegationResult(t, result)
	powerDuring := dumpDRepVotingPower(t, f)

	release()
	f.ls.waitEpochBoundaryBenchBackground()

	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Empty(t, rounds)
	credited := accountRewards(t, f, credits)
	for key, credit := range credits {
		require.Equal(t, stored[key]+credit, credited[key])
	}
	result, err = f.ls.queryShelleyFilteredDelegationAndRewardAccounts(creds)
	require.NoError(t, err)
	_, rewardsAfter := unwrapFilteredDelegationResult(t, result)
	require.Equal(t, rewardsAfter, rewardsDuring)
	require.Equal(t, dumpDRepVotingPower(t, f), powerDuring)
}

// TestPendingRewardRoundWithdrawalCreditsOnce pins the withdrawal rule: a
// transaction that withdraws a credential's whole balance while its round is
// pending first writes that credential's credit, so the withdrawal sees it,
// and the background write does not credit it a second time.
func TestPendingRewardRoundWithdrawalCreditsOnce(t *testing.T) {
	t.Parallel()
	f, release := heldRewardCreditFold(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	var key string
	for candidate := range credits {
		key = candidate
		break
	}
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey, lcommon.AddressNetworkMainnet,
		nil, []byte(key),
	)
	require.NoError(t, err)
	balance := stored[key] + credits[key]
	tx := &conway.ConwayTransaction{Body: conway.ConwayTransactionBody{
		TxWithdrawals: map[*lcommon.Address]uint64{&address: balance},
	}}
	txn := f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := f.ls.foldRewardCreditsForWithdrawals(tx, txn); err != nil {
			return err
		}
		return f.db.Metadata().ApplyAccountRewardWithdrawal(
			0, []byte(key), balance,
			epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1)+5,
			make([]byte, 32), txn.Metadata(),
		)
	}))
	release()
	f.ls.waitEpochBoundaryBenchBackground()
	account, err := f.db.GetAccountByCredential(0, []byte(key), true, nil)
	require.NoError(t, err)
	require.Zero(t, uint64(account.Reward),
		"the background write must not credit a withdrawn round again")
}

// TestPendingRewardRoundRollbackBelowBoundary pins rollback: undoing the
// boundary drops the pending round and reverts every credit already written,
// and applying the boundary again reproduces the same balances.
func TestPendingRewardRoundRollbackBelowBoundary(t *testing.T) {
	t.Parallel()
	f, release := heldRewardCreditFold(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	// One chunk of the fold lands before the rollback.
	txn := f.db.Transaction(true)
	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, err := f.ls.foldRewardCreditChunk(txn, rounds[0], 5)
		return err
	}))
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	txn = f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := f.db.DeleteAccountRewardsAfterSlot(boundary-1, txn); err != nil {
			return err
		}
		return f.db.DeleteRewardStateAfterSlot(boundary-1, txn)
	}))
	rounds, err = f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Empty(t, rounds, "a rollback below the boundary drops the round")
	require.Equal(t, stored, accountRewards(t, f, credits),
		"a rollback below the boundary reverts written credits")
	release()
	f.ls.waitEpochBoundaryBenchBackground()
	require.Equal(t, stored, accountRewards(t, f, credits),
		"the dropped round must not be written after the rollback")
}

// TestPendingRewardRoundResumesAfterRestart pins resumption: a fold stopped
// part way, as by a restart, finishes from its cursor with every credential
// credited exactly once.
func TestPendingRewardRoundResumesAfterRestart(t *testing.T) {
	t.Parallel()
	f, release := heldRewardCreditFold(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	txn := f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		done, err := f.ls.foldRewardCreditChunk(txn, rounds[0], 7)
		require.False(t, done)
		return err
	}))
	f.ls.rewardCreditFoldHook = nil
	release()
	f.ls.waitEpochBoundaryBenchBackground()
	f.ls.resumePendingRewardCreditFolds()
	f.ls.waitEpochBoundaryBenchBackground()
	credited := accountRewards(t, f, credits)
	for key, credit := range credits {
		require.Equal(t, stored[key]+credit, credited[key],
			"credential %x credited exactly once", key)
	}
	var deltas int
	raw := rewardCalcSQLDB(t, f.db)
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE added_slot = ?",
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1),
	).Scan(&deltas))
	var outputs int
	require.NoError(t, raw.QueryRow(`SELECT COUNT(*) FROM reward_account_output
WHERE epoch = 8 AND spendable AND NOT guarded AND amount != '0'`).Scan(&outputs))
	require.Equal(t, outputs, deltas, "one journal row per credit")
}

// TestNextBoundaryFinishesEarlierPendingRound pins the next boundary's first
// step: a round still pending from an earlier boundary is written in full
// before the new round is applied.
func TestNextBoundaryFinishesEarlierPendingRound(t *testing.T) {
	t.Parallel()
	f, _ := heldRewardCreditFold(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	next := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 2)
	txn := f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return f.ls.finishPendingRewardCreditsInTxn(txn, next)
	}))
	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Empty(t, rounds)
	credited := accountRewards(t, f, credits)
	for key, credit := range credits {
		require.Equal(t, stored[key]+credit, credited[key])
	}
}

// TestFencedPrecomputeChunkDoesNotWrite pins the boundary fence: a background
// chunk whose round was resolved before a boundary claimed the round stops
// without writing.
func TestFencedPrecomputeChunkDoesNotWrite(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	round, ok, err := ls.resolveStakeRewardPrecomputeRound(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	)
	require.NoError(t, err)
	require.True(t, ok)
	ls.fenceRewardPrecompute()
	done, err := ls.stakeRewardPrecomputeChunkStep(round)
	require.NoError(t, err)
	require.True(t, done, "a fenced chunk must stop")
	outputs, err := db.Metadata().
		GetRewardPoolOutputs(survivalSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Empty(t, outputs, "a fenced chunk must not write")
}

// TestPendingRewardStakeInputsHoldBackTheRound pins the deferred-write guard:
// while a snapshot's reward_stake_input rows are still being written, a
// precompute of a round that reads them does not start.
func TestPendingRewardStakeInputsHoldBackTheRound(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return markRewardStakeInputsPending(
			db.Metadata(), txn.Metadata(), survivalSnapshotEpoch, 100,
		)
	}))
	ls.rewardPrecomputeMu.Lock()
	ls.deferredStakeInputsWriting = map[uint64]struct{}{
		survivalSnapshotEpoch: {},
	}
	ls.rewardPrecomputeMu.Unlock()
	_, ok, err := ls.resolveStakeRewardPrecomputeRound(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok, "a round must wait for its snapshot's inputs")
}
