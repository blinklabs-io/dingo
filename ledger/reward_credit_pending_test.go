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
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// creditedRewardRound runs the dump fixture's boundary with the reward
// precompute complete, leaving the round credited and every credit unfolded.
func creditedRewardRound(t *testing.T) *epochBoundaryBenchFixture {
	t.Helper()
	shape := epochBoundaryDumpShape()
	shape.delegators = 60
	f := newEpochBoundaryBenchFixture(t, shape, "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()
	rounds, err := seedDB(t, f.db).GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Len(t, rounds, 1, "the boundary must record its round credited")
	return f
}

// journaledRewardRound is creditedRewardRound with the boundary committed as
// the first block of the next epoch, after a block ending the epoch, so a
// rollback to the returned point undoes the boundary along the set's journal.
func journaledRewardRound(
	t *testing.T,
) (*epochBoundaryBenchFixture, ocommon.Point) {
	t.Helper()
	shape := epochBoundaryDumpShape()
	shape.delegators = 60
	f := newEpochBoundaryBenchFixture(t, shape, "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	lastOfEpoch := fxPoint(boundary - 1)
	fxTip(t, f.ls, lastOfEpoch)
	f.ls.fenceRewardPrecompute()
	fxBlock(t, f.ls, fxPoint(boundary), func(
		_ ledgerdb.LedgerDB, txn ledgerdb.Txn,
	) error {
		_, err := f.ls.processEpochRollover(
			context.Background(),
			dbadapter.Unwrap(txn),
			f.epochs[epochBoundaryBenchEndedEpoch],
			eras.ConwayEraDesc,
			f.pparams,
		)
		return err
	})
	f.ls.waitEpochBoundaryBenchBackground()
	rounds, err := seedDB(t, f.db).GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Len(t, rounds, 1, "the boundary must record its round credited")
	return f, lastOfEpoch
}

// rewardedCredentials returns every credential the round credits, with the
// total it credits.
func rewardedCredentials(
	t *testing.T,
	f *epochBoundaryBenchFixture,
) map[string]uint64 {
	t.Helper()
	outputs, err := seedDB(t, f.db).GetRewardAccountOutputs(8, nil)
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
		account, err := seedDB(t, f.db).GetAccountByCredential(
			context.Background(),
			0,
			[]byte(key),
			true,
			nil,
		)
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
	txn := f.db.Transaction(context.Background(), false)
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
	result, err := f.ls.queryShelleyFilteredDelegationAndRewardAccounts(context.Background(), creds, QueryPoint{}, nil)
	require.NoError(t, err)
	_, rewardsDuring := unwrapFilteredDelegationResult(t, result)
	powerDuring := dumpDRepVotingPower(t, f)

	settleRewardCredits(t, f.ls)
	credited := accountRewards(t, f, credits)
	for key, credit := range credits {
		require.Equal(t, stored[key]+credit, credited[key])
	}
	result, err = f.ls.queryShelleyFilteredDelegationAndRewardAccounts(context.Background(), creds, QueryPoint{}, nil)
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
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxWithdrawals: map[*lcommon.Address]uint64{&address: balance},
		},
		TxIsValid: true,
	}
	txn := f.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := f.ls.foldRewardCreditsForWithdrawals(context.Background(), tx, txn); err != nil {
			return err
		}
		return f.ls.stageAccountTransaction(
			dbadapter.Txn(txn), tx,
			epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1)+5,
			0, make([]byte, 32), database.BatchedTxIngestOpts{}, nil,
		)
	}))
	var hash lcommon.Blake2b224
	copy(hash[:], key)
	readTxn := f.db.Transaction(context.Background(), false)
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
	account, err := seedDB(t, f.db).GetAccountByCredential(
		context.Background(),
		0,
		[]byte(key),
		true,
		nil,
	)
	require.NoError(t, err)
	require.Zero(t, uint64(account.Reward),
		"folding the round must not credit a withdrawn credit again")
}

// TestPendingRewardRoundRollbackBelowBoundary pins rollback: undoing the
// boundary drops the credited round, reverts every credit already folded, and
// leaves the surviving outputs unfolded for the boundary to credit again.
func TestPendingRewardRoundRollbackBelowBoundary(t *testing.T) {
	t.Parallel()
	f, lastOfEpoch := journaledRewardRound(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	outputsBefore := unfoldedCreditCount(t, f)
	require.Positive(t, outputsBefore)
	// Some credits are folded, as by withdrawals, before the rollback.
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	folded := 0
	fxBlock(t, f.ls, fxPoint(boundary+1), func(
		_ ledgerdb.LedgerDB, txn ledgerdb.Txn,
	) error {
		for key := range credits {
			if folded == 5 {
				break
			}
			folded++
			if err := f.ls.foldRewardCreditFor(
				context.Background(), txn, 0, []byte(key),
			); err != nil {
				return err
			}
		}
		return nil
	})
	require.Equal(t, outputsBefore-folded, unfoldedCreditCount(t, f),
		"control: the folded credits left the round")
	fxRollback(t, f.ls, lastOfEpoch)
	rounds, err := seedDB(t, f.db).GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Empty(t, rounds, "a rollback below the boundary drops the round")
	require.Equal(t, stored, accountRewards(t, f, credits),
		"a rollback below the boundary reverts folded credits")
	require.Equal(t, outputsBefore, unfoldedCreditCount(t, f),
		"a reapplied round must count its outputs as unfolded again")
}

// TestFencedPrecomputeChunkDoesNotWrite pins the boundary fence: a background
// chunk whose round was resolved before a boundary claimed the round stops
// without writing.
func TestFencedPrecomputeChunkDoesNotWrite(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	round, ok, err := ls.engine().ResolveStakeRewardPrecomputeRound(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	)
	require.NoError(t, err)
	require.True(t, ok)
	ls.fenceRewardPrecompute()
	done, err := ls.stakeRewardPrecomputeChunkStep(round)
	require.NoError(t, err)
	require.True(t, done, "a fenced chunk must stop")
	outputs, err := seedDB(t, db).
		GetRewardPoolOutputs(survivalSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Empty(t, outputs, "a fenced chunk must not write")
}

// TestPendingRewardStakeInputsHoldBackTheRound pins the incomplete-basis
// guard: while a snapshot's reward_stake_input rows are marked incomplete, a
// read-only resolution of a round that reads them does not start it.
func TestPendingRewardStakeInputsHoldBackTheRound(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return markRewardStakeInputsPending(
			ls.ledgerDB(), txn, survivalSnapshotEpoch, 100,
		)
	}))
	_, ok, err := ls.engine().ResolveStakeRewardPrecomputeRound(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok, "a round must wait for its snapshot's inputs")
}

// TestPrecomputeLeavesCreditedRoundAlone pins that a precompute run for a
// round the boundary already credited, as a queued startup precompute can be,
// neither recomputes nor rewrites its outputs: they are balances, and a
// rewrite would count its folded credits again.
func TestPrecomputeLeavesCreditedRoundAlone(t *testing.T) {
	t.Parallel()
	shape := epochBoundaryDumpShape()
	shape.delegators = 248
	f := newEpochBoundaryBenchFixture(t, shape, "")
	require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
		epochBoundaryBenchPrecomputeEvent(),
	))
	// Deregistrations after the precompute make the boundary correct the
	// outputs, so a later precompute no longer matches them.
	for d := 0; d < shape.delegators; d += 11 {
		fxDeregisterStake(
			t, f.db, epochBoundaryBenchHash(0x30, uint64(d)+1),
			epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch)+1_000+uint64(d),
		)
	}
	f.rollover(t)
	f.ls.waitEpochBoundaryBenchBackground()
	credits := rewardedCredentials(t, f)
	keys := make([]string, 0, len(credits))
	for key := range credits {
		keys = append(keys, key)
	}
	txn := f.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, key := range keys[:len(keys)/4] {
			if err := f.ls.foldRewardCreditFor(context.Background(), txn, 0, []byte(key)); err != nil {
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

// pendingRoundReads is every reader outside the boundary that can see a
// pending round, rendered so two observations compare with require.Equal.
type pendingRoundReads struct {
	liveInputs     string
	boundaryStake  string
	localStateDist string
	drepPower      string
	drepSingle     string
	// Readers with no reward term, which a pending round cannot move.
	utxoStake string
}

func renderStakeMaps(stakes, delegators map[string]uint64) string {
	lines := make([]string, 0, len(stakes))
	for key, stake := range stakes {
		lines = append(lines, fmt.Sprintf("%x=%d/%d", key, stake, delegators[key]))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func observePendingRoundReaders(
	t *testing.T,
	f *epochBoundaryBenchFixture,
	credits map[string]uint64,
) pendingRoundReads {
	t.Helper()
	pools, err := seedDB(t, f.db).GetDelegatedPoolKeyHashes(nil)
	require.NoError(t, err)
	require.NotEmpty(t, pools)
	boundary := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch + 1)
	var ret pendingRoundReads

	inputs, err := seedDB(t, f.db).GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	lines := make([]string, 0, len(inputs))
	for _, input := range inputs {
		lines = append(lines, fmt.Sprintf("%x/%d/%x=%d",
			input.PoolKeyHash, input.CredentialTag, input.StakingKey,
			uint64(input.Stake)))
	}
	sort.Strings(lines)
	ret.liveInputs = strings.Join(lines, "\n")

	stakes, delegators, err := seedDB(t, f.db).GetEpochBoundaryStakeByPools(
		pools, boundary-1, boundary, nil, 0, 0, nil,
	)
	require.NoError(t, err)
	ret.boundaryStake = renderStakeMaps(stakes, delegators)

	dist, err := f.ls.queryShelleyStakeDistribution(t.Context(), QueryPoint{}, nil)
	require.NoError(t, err)
	ret.localStateDist = fmt.Sprintf("%+v", dist)

	ret.drepPower = dumpDRepVotingPower(t, f)
	dreps, err := seedDB(t, f.db).GetActiveDreps(context.Background(), nil)
	require.NoError(t, err)
	singles := make([]string, 0, len(dreps))
	for _, drep := range dreps {
		power, err := seedDB(t, f.db).GetDRepVotingPower(
			t.Context(), drep.CredentialTag, drep.Credential, 0, nil,
		)
		require.NoError(t, err)
		singles = append(singles, fmt.Sprintf("%x=%d", drep.Credential, power))
	}
	sort.Strings(singles)
	ret.drepSingle = strings.Join(singles, "\n")

	stakes, delegators, err = seedDB(t, f.db).GetStakeByPools(t.Context(), pools, nil)
	require.NoError(t, err)
	ret.utxoStake = renderStakeMaps(stakes, delegators)
	return ret
}

// TestPendingRewardRoundAggregateReadsCountEachCreditOnce pins every aggregate
// and historical reader against a credited round both before any of its
// credits is folded and after some are, as by withdrawals: a credit must be
// counted exactly once, from the account row or from the round, so no reading
// moves when the rest are folded.
func TestPendingRewardRoundAggregateReadsCountEachCreditOnce(t *testing.T) {
	t.Parallel()
	f := creditedRewardRound(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	unfolded := observePendingRoundReaders(t, f, credits)

	keys := make([]string, 0, len(credits))
	for key := range credits {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	require.Greater(t, len(keys), 12)
	txn := f.db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, key := range keys[:len(keys)/3] {
			if err := f.ls.foldRewardCreditFor(context.Background(), txn, 0, []byte(key)); err != nil {
				return err
			}
		}
		return nil
	}))
	first, last := keys[0], keys[len(keys)-1]
	require.Equal(t, stored[first]+credits[first],
		accountRewards(t, f, credits)[first], "control: a folded credit")
	require.Equal(t, stored[last], accountRewards(t, f, credits)[last],
		"control: an unfolded credit")
	partial := observePendingRoundReaders(t, f, credits)

	settleRewardCredits(t, f.ls)
	require.Equal(t, stored[last]+credits[last],
		accountRewards(t, f, credits)[last])
	after := observePendingRoundReaders(t, f, credits)

	for name, got := range map[string]pendingRoundReads{
		"unfolded": unfolded, "partly folded": partial,
	} {
		require.Equal(t, after.liveInputs, got.liveInputs,
			"%s: live stake inputs", name)
		require.Equal(t, after.boundaryStake, got.boundaryStake,
			"%s: boundary stake reconstruction", name)
		require.Equal(t, after.localStateDist, got.localStateDist,
			"%s: local state query stake distribution", name)
		require.Equal(t, after.drepPower, got.drepPower,
			"%s: DRep voting power", name)
		require.Equal(t, after.drepSingle, got.drepSingle,
			"%s: single DRep voting power", name)
		require.Equal(t, after.utxoStake, got.utxoStake,
			"%s: UTxO-only pool stake", name)
	}
}

// TestPendingRewardRoundPinnedQuery pins the pending round at an acquired
// point: GetFilteredDelegationsAndRewardAccounts counts its credits from the
// boundary that applied it, exactly once, and not before.
func TestPendingRewardRoundPinnedQuery(t *testing.T) {
	t.Parallel()
	f := creditedRewardRound(t)
	credits := rewardedCredentials(t, f)
	stored := accountRewards(t, f, credits)
	rounds, err := seedDB(t, f.db).GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Len(t, rounds, 1)
	boundary := rounds[0].BoundarySlot
	require.Positive(t, boundary)

	creds := make([]olocalstatequery.StakeCredential, 0, len(credits))
	for key := range credits {
		var hash lcommon.Blake2b224
		copy(hash[:], key)
		creds = append(creds, olocalstatequery.StakeCredential{
			Tag: 0, Bytes: hash,
		})
	}
	rewardsAt := func(at QueryPoint) map[olocalstatequery.StakeCredential]uint64 {
		t.Helper()
		result, err := f.ls.queryShelleyFilteredDelegationAndRewardAccounts(
			context.Background(), creds, at, nil,
		)
		require.NoError(t, err)
		_, rewards := unwrapFilteredDelegationResult(t, result)
		return rewards
	}
	live := rewardsAt(QueryPoint{})
	require.Equal(t, live, rewardsAt(QueryPoint{Slot: boundary}))
	before := rewardsAt(QueryPoint{Slot: boundary - 1})
	for _, cred := range creds {
		key := string(cred.Bytes[:])
		require.Equal(t, stored[key]+credits[key], live[cred])
		require.Equal(t, stored[key], before[cred])
	}
}
