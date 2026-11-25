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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// unloadedRewardSet is a set that has not loaded its reward rows.
type unloadedRewardSet struct{ *utxoset.Set }

func (unloadedRewardSet) RewardOutputsLoaded() (bool, error) { return false, nil }

type rewardOutputHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	// store is the metadata store read directly, without the set's compare.
	store ledgerdb.LedgerDB
	pool  []byte
	owner models.StakeCredentialRef
	deleg models.StakeCredentialRef
	// base is the slot the rollback restores; start is the epoch's first.
	base, start uint64
}

func newRewardOutputHarness(
	t *testing.T,
	wrap func(*utxoset.Set) ledger.UtxoSetWriter,
) *rewardOutputHarness {
	t.Helper()
	return newRewardOutputHarnessReads(t, wrap, "metadata-visible")
}

// newRewardOutputHarnessReads is newRewardOutputHarness with the given read
// mode.
func newRewardOutputHarnessReads(
	t *testing.T,
	wrap func(*utxoset.Set) ledger.UtxoSetWriter,
	reads string,
) *rewardOutputHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	if wrap != nil {
		scenario.WrapSet(func(ledger.UtxoSetWriter) ledger.UtxoSetWriter {
			return wrap(set)
		})
	}
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	h := &rewardOutputHarness{
		set: set, scenario: scenario, store: store,
		pool:  bytes.Repeat([]byte{0xd1}, 28),
		owner: models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x71}, 28)),
		deleg: models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x72}, 28)),
		base:  scenario.RollbackSlot(),
		start: scenario.EpochStartSlot(),
	}
	// The epoch's first slot is at or before the rollback target, so a
	// credit there survives a rollback; a credit just after it does not.
	require.LessOrEqual(t, h.start, h.base)
	require.Less(t, h.base, h.start+3)
	scenario.AddSetStake(h.owner.Key, 20_000_000)
	scenario.AddSetStake(h.deleg.Key, 20_000_000)
	return h
}

// write runs fn in a write transaction through the ledger's LedgerDB and
// commits it.
func (h *rewardOutputHarness) write(t *testing.T, fn func(ledgerdb.LedgerDB, ledgerdb.Txn)) {
	t.Helper()
	ldb := h.scenario.LedgerDB()
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	fn(ldb, txn)
	require.NoError(t, txn.Commit())
}

func (h *rewardOutputHarness) output(
	ref models.StakeCredentialRef,
	epoch, amount, captured, boundary uint64,
) *models.RewardAccountOutput {
	return &models.RewardAccountOutput{
		Epoch: epoch, CredentialTag: ref.Tag, StakingKey: ref.Key,
		PoolKeyHash: h.pool, RewardType: "member",
		Amount: types.Uint64(amount), Spendable: true,
		CapturedSlot: captured, BoundarySlot: boundary,
	}
}

// Rounds 11, 12 and 13 credit the delegator 7, 70 and 700 and the owner 3,
// 30 and 300. Round 11 is captured at the rollback target, so a rollback
// keeps its outputs; the others are captured after it. Round 11 also has a
// guarded and a non-spendable output, which are not credits.
func (h *rewardOutputHarness) round(epoch uint64) models.RewardCreditRound {
	return models.RewardCreditRound{
		SnapshotEpoch: epoch, BoundarySlot: h.start + 9 + epoch,
	}
}

func (h *rewardOutputHarness) saveRounds(t *testing.T) {
	t.Helper()
	for i, epoch := range []uint64{11, 12, 13} {
		captured := h.start + epoch
		if epoch == 11 {
			captured = h.base
		}
		boundary := h.round(epoch).BoundarySlot
		scale := uint64(1)
		for range i {
			scale *= 10
		}
		outputs := []*models.RewardAccountOutput{
			h.output(h.deleg, epoch, 7*scale, captured, boundary),
			h.output(h.owner, epoch, 3*scale, captured, boundary),
		}
		if epoch == 11 {
			guarded := h.output(h.deleg, epoch, 1000, captured, boundary)
			guarded.RewardType = "leader"
			guarded.Guarded = true
			unspendable := h.output(h.owner, epoch, 2000, captured, boundary)
			unspendable.RewardType = "leader"
			unspendable.Spendable = false
			outputs = append(outputs, guarded, unspendable)
		}
		h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
			require.NoError(t, ldb.SaveRewardAccountOutputs(outputs, txn))
		})
	}
}

func (h *rewardOutputHarness) applyRound(t *testing.T, epoch uint64) {
	t.Helper()
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.AddAppliedRewardCreditRound(h.round(epoch), txn))
	})
}

func (h *rewardOutputHarness) creditPostSnapshot(t *testing.T) {
	t.Helper()
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		for _, c := range []struct {
			ref    models.StakeCredentialRef
			amount uint64
			source byte
		}{{h.owner, 11, 0xa1}, {h.owner, 12, 0xa2}, {h.deleg, 5, 0xa3}} {
			require.NoError(t, ldb.AddPostSnapshotAccountRewardByCredential(
				t.Context(), c.ref.Tag, c.ref.Key, c.amount, h.start,
				[]byte{c.source}, txn,
			))
		}
	})
}

// creditLater credits the delegator after the rollback target.
func (h *rewardOutputHarness) creditLater(t *testing.T) {
	t.Helper()
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.AddPostSnapshotAccountRewardByCredential(
			t.Context(), h.deleg.Tag, h.deleg.Key, 4, h.start+3, []byte{0xa4}, txn,
		))
	})
}

// rewardReads are the answers of every reward state read.
type rewardReads struct {
	rounds     []models.RewardCreditRound
	hasPending bool
	pending    map[string][]*models.RewardAccountOutput
	applied    map[uint64]bool
	unfolded   map[uint64]bool
	postSnap   []*models.AccountRewardDelta
	postLater  []*models.AccountRewardDelta
}

func (h *rewardOutputHarness) readAll(t *testing.T) rewardReads {
	t.Helper()
	ldb := h.scenario.LedgerDB()
	r := rewardReads{
		pending:  map[string][]*models.RewardAccountOutput{},
		applied:  map[uint64]bool{},
		unfolded: map[uint64]bool{},
	}
	var err error
	r.rounds, err = ldb.GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	r.hasPending, err = ldb.HasPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	for _, epoch := range []uint64{11, 12, 13, 14} {
		r.applied[epoch], err = ldb.HasAppliedRewardCreditRound(epoch, nil)
		require.NoError(t, err)
		r.unfolded[epoch], err = ldb.HasUnfoldedRewardCreditsThroughEpoch(epoch, nil)
		require.NoError(t, err)
	}
	for name, ref := range map[string]models.StakeCredentialRef{
		"owner": h.owner, "deleg": h.deleg,
	} {
		r.pending[name], err = ldb.GetPendingRewardAccountOutputsForCredential(
			ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err)
	}
	r.postSnap, err = ldb.GetPostSnapshotRewardCredits(h.start, nil)
	require.NoError(t, err)
	r.postLater, err = ldb.GetPostSnapshotRewardCredits(h.start+3, nil)
	require.NoError(t, err)
	// Live stake and DRep power add the pending credits.
	_, err = ldb.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, nil)
	require.NoError(t, err)
	_, err = ldb.GetDRepVotingPowerByType(t.Context(), []uint64{2, 3}, 0, nil)
	require.NoError(t, err)
	return r
}

// requireAgree reads everything and requires the set to have agreed each
// time; it returns the reads.
func (h *rewardOutputHarness) requireAgree(t *testing.T, label string) rewardReads {
	t.Helper()
	_ = label
	return h.readAll(t)
}

func (h *rewardOutputHarness) setOutputs(t *testing.T, ref models.StakeCredentialRef) map[uint64]uint64 {
	t.Helper()
	rows, err := h.set.CredentialRewardOutputs(ref.Tag, ref.Key, nil)
	require.NoError(t, err)
	out := map[uint64]uint64{}
	for _, r := range rows {
		out[r.Epoch] = uint64(r.Amount)
	}
	return out
}

// TestRewardOutputsEpochWipeAndEligibilityFlip requires a delete of an
// epoch's outputs and a save that makes an output ineligible to remove the
// set's rows, and a rollback not to bring a wiped row back, as the store's
// deletes are not undone.
func TestRewardOutputsEpochWipeAndEligibilityFlip(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.saveRounds(t)
	require.Equal(t, map[uint64]uint64{11: 7, 12: 70, 13: 700}, h.setOutputs(t, h.deleg))

	// A recheck at the boundary finds the owner's round 12 credit
	// unregistered: saved again, it is no longer spendable.
	flipped := h.output(h.owner, 12, 30, h.start+12, h.round(12).BoundarySlot)
	flipped.Spendable = false
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{flipped}, txn,
		))
	})
	require.Equal(t, map[uint64]uint64{11: 3, 13: 300}, h.setOutputs(t, h.owner))

	// The precompute of a round replaces the outputs of its epoch.
	h.write(t, func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
		require.NoError(t, ldb.DeleteRewardOutputsForEpoch(13, txn))
	})
	require.Equal(t, map[uint64]uint64{11: 7, 12: 70}, h.setOutputs(t, h.deleg))
	require.Equal(t, map[uint64]uint64{11: 3}, h.setOutputs(t, h.owner))
	epochs, err := h.set.RewardOutputEpochs(nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{11, 12}, epochs, "the epoch index goes with the rows")

	h.scenario.RollbackBeforeBoundary()
	require.Equal(t, map[uint64]uint64{11: 7}, h.setOutputs(t, h.deleg),
		"the rollback drops rows captured after it and does not restore the wiped epoch")
	epochs, err = h.set.RewardOutputEpochs(nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{11}, epochs)
	h.requireAgree(t, "after the rollback")
}

// TestRewardOutputReadsSeeTheirTransactionsStagedChanges requires the reads
// in a write transaction to see the saves, wipe, round and claim that
// transaction has staged and not yet committed, as the metadata store's do,
// for every reward read the set compares.
func TestRewardOutputReadsSeeTheirTransactionsStagedChanges(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.saveRounds(t)
	h.applyRound(t, 11)
	ldb := h.scenario.LedgerDB()
	txn := ldb.Begin(t.Context(), true)
	defer func() { require.NoError(t, txn.Rollback()) }()
	pending := func() int {
		out, err := ldb.GetPendingRewardAccountOutputsForCredential(
			h.deleg.Tag, h.deleg.Key, txn,
		)
		require.NoError(t, err)
		return len(out)
	}
	hasPending := func() bool {
		has, err := ldb.HasPendingRewardCreditRounds(txn)
		require.NoError(t, err)
		return has
	}
	require.Equal(t, 1, pending())

	// A round 12 whose outputs the set holds already is credited here.
	require.NoError(t, ldb.AddAppliedRewardCreditRound(h.round(12), txn))
	require.Equal(t, 2, pending(), "the staged round credits the saved output")
	// The recompute of round 13 replaces its outputs: the old row goes and
	// a new one is saved, in this transaction.
	require.NoError(t, ldb.DeleteRewardOutputsForEpoch(13, txn))
	require.NoError(t, ldb.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{h.output(h.deleg, 13, 5, h.start+30, h.start+40)}, txn,
	))
	require.NoError(t, ldb.AddAppliedRewardCreditRound(models.RewardCreditRound{
		SnapshotEpoch: 13, BoundarySlot: h.start + 40,
	}, txn))
	require.Equal(t, 3, pending())
	has, err := ldb.HasUnfoldedRewardCreditsThroughEpoch(13, txn)
	require.NoError(t, err)
	require.True(t, has)

	// Claiming every credit leaves the owner's.
	claimed, err := ldb.ClaimPendingRewardCreditsForCredential(h.deleg.Tag, h.deleg.Key, txn)
	require.NoError(t, err)
	require.Len(t, claimed, 3)
	require.Zero(t, pending())
	require.True(t, hasPending(), "the owner's credits are pending")
	_, err = ldb.ClaimUnfoldedRewardCredits(11, 10, txn)
	require.NoError(t, err)
	_, err = ldb.ClaimUnfoldedRewardCredits(12, 10, txn)
	require.NoError(t, err)
	_, err = ldb.ClaimUnfoldedRewardCredits(13, 10, txn)
	require.NoError(t, err)
	require.False(t, hasPending(), "every credit is folded")
	has, err = ldb.HasUnfoldedRewardCreditsThroughEpoch(13, txn)
	require.NoError(t, err)
	require.False(t, has)

}

// TestRewardOutputViewsInsideAStagedRollback requires the set's views, in a
// transaction that stages a rollback, to show the rows as the rollback leaves
// them with the transaction's later writes on top: the outputs captured after
// the target gone, a folded output the target keeps back, the rounds and
// credits after it gone.
func TestRewardOutputViewsInsideAStagedRollback(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.creditPostSnapshot(t)
	h.creditLater(t)
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	h.scenario.FoldForWithdrawal(h.deleg)
	require.Equal(t, map[uint64]uint64{13: 700}, h.setOutputs(t, h.deleg))

	v := h.scenario.RewardViewsInsideRollback(h.deleg, h.start+3, nil)
	require.Empty(t, v.Rounds, "the rounds are after the target")
	require.Empty(t, v.Pending)
	require.False(t, v.HasPending)
	require.Empty(t, v.Credits, "the credit after the target is gone")
	v = h.scenario.RewardViewsInsideRollback(h.deleg, h.start, nil)
	require.Len(t, v.Credits, 2, "the credits at the target's slot stay")

	v = h.scenario.RewardViewsInsideRollback(h.deleg, h.start+3,
		func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn) {
			// Round 11's output was folded after the target, so the
			// rollback unfolds it; crediting the round again makes it
			// pending.
			require.NoError(t, ldb.AddAppliedRewardCreditRound(h.round(11), txn))
		})
	require.Len(t, v.Rounds, 1)
	require.True(t, v.HasPending)
	require.Len(t, v.Pending, 1)
	require.Equal(t, uint64(11), v.Pending[0].Epoch)
	require.Equal(t, types.Uint64(7), v.Pending[0].Amount)
}
