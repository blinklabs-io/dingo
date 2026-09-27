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
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// creditedRewardRound runs the dump fixture's boundary with the reward
// precompute complete, leaving the round credited and every credit unfolded.
func creditedRewardRound(t *testing.T) *epochBoundaryBenchFixture {
	t.Helper()
	f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()
	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Len(t, rounds, 1, "the boundary must record its round credited")
	return f
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
	f := creditedRewardRound(t)
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

	settleRewardCredits(t, f.ls)
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
	f := creditedRewardRound(t)
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
	var hash lcommon.Blake2b224
	copy(hash[:], key)
	readTxn := f.db.Transaction(false)
	require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
		view := &LedgerView{ls: f.ls, txn: txn}
		after, err := view.RewardAccountBalance(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(hash),
		})
		require.NoError(t, err)
		require.NotNil(t, after)
		require.Zero(t, *after, "a folded credit must not count again")
		return nil
	}))
	settleRewardCredits(t, f.ls)
	account, err := f.db.GetAccountByCredential(0, []byte(key), true, nil)
	require.NoError(t, err)
	require.Zero(t, uint64(account.Reward),
		"folding the round must not credit a withdrawn credit again")
}

// TestPendingRewardRoundRollbackBelowBoundary pins rollback: undoing the
// boundary drops the credited round, reverts every credit already folded, and
// leaves the surviving outputs unfolded for the boundary to credit again.
func TestPendingRewardRoundRollbackBelowBoundary(t *testing.T) {
	t.Parallel()
	f := creditedRewardRound(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	// Some credits are folded, as by withdrawals, before the rollback.
	txn := f.db.Transaction(true)
	folded := 0
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for key := range credits {
			if folded == 5 {
				break
			}
			folded++
			if err := f.ls.foldRewardCreditFor(txn, 0, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	txn = f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := f.db.DeleteAccountRewardsAfterSlot(boundary-1, txn); err != nil {
			return err
		}
		return f.db.DeleteRewardStateAfterSlot(boundary-1, txn)
	}))
	rounds, err := f.db.Metadata().GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Empty(t, rounds, "a rollback below the boundary drops the round")
	require.Equal(t, stored, accountRewards(t, f, credits),
		"a rollback below the boundary reverts folded credits")
	var stillFolded int
	require.NoError(t, rewardCalcSQLDB(t, f.db).QueryRow(
		`SELECT COUNT(*) FROM reward_account_output WHERE folded`,
	).Scan(&stillFolded))
	require.Zero(t, stillFolded,
		"a reapplied round must count its outputs as unfolded again")
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

// TestDeferredStakeInputWriteRetriesAfterError pins the deferred stake-input
// writer: a failed write is retried until the snapshot's rows are written,
// without a reader having to rebuild them.
func TestDeferredStakeInputWriteRetriesAfterError(t *testing.T) {
	t.Parallel()
	stakeInputs := func(t *testing.T, fail bool) string {
		f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
		require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
			epochBoundaryBenchPrecomputeEvent(),
		))
		var failures atomic.Int32
		if fail {
			f.ls.deferredStakeInputsFailHook = func() error {
				if failures.Add(1) == 1 {
					return errors.New("transient write failure")
				}
				return nil
			}
		}
		f.rollover(t)
		f.ls.waitEpochBoundaryBenchBackground()
		if fail {
			require.GreaterOrEqual(t, failures.Load(), int32(2),
				"the failed write must be attempted again")
		}
		meta := f.db.Metadata()
		pending, err := loadRewardStakeInputsPending(meta, nil)
		require.NoError(t, err)
		require.Empty(t, pending.Entries,
			"the retried write must complete the snapshot's rows")
		raw, err := dbtest.RawSQLiteMetadata(t, f.db)
		require.NoError(t, err)
		defer raw.Close()
		dump := dumpEpochBoundaryState(t, raw)
		i := strings.Index(dump, "== reward_stake_input")
		require.GreaterOrEqual(t, i, 0)
		return dump[i:]
	}
	require.Equal(t, stakeInputs(t, false), stakeInputs(t, true))
}

// TestPrecomputeLeavesCreditedRoundAlone pins that a precompute run for a
// round the boundary already credited, as a queued startup precompute can be,
// neither recomputes nor rewrites its outputs: they are balances, and a
// rewrite would count its folded credits again.
func TestPrecomputeLeavesCreditedRoundAlone(t *testing.T) {
	t.Parallel()
	f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	// Deregistrations after the precompute make the boundary correct the
	// outputs, so a later precompute no longer matches them.
	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	defer raw.Close()
	deregisterEpochBoundaryDumpDelegators(t, raw)
	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()
	credits := rewardedCredentials(t, f)
	keys := make([]string, 0, len(credits))
	for key := range credits {
		keys = append(keys, key)
	}
	txn := f.db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, key := range keys[:len(keys)/4] {
			if err := f.ls.foldRewardCreditFor(txn, 0, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	before := observePendingRoundReaders(t, f, credits)
	f.ls.queueStartupRewardPrecompute()
	f.ls.rewardPrecomputeWG.Wait()
	after := observePendingRoundReaders(t, f, credits)
	require.Equal(t, before, after,
		"a credited round's balances must not move")
}
