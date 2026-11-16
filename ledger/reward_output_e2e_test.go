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
	"errors"
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

// unsnapshottableSet is a set whose snapshot always fails.
type unsnapshottableSet struct{ *utxoset.Set }

func (unsnapshottableSet) SnapshotUtxoSet() (
	ledger.UtxoSetWriter, func(), error,
) {
	return nil, nil, errors.New("snapshot unavailable")
}

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
	return newRewardOutputHarnessReads(t, wrap, ledger.UtxoSetReadsVerify)
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
	var w ledger.UtxoSetWriter = set
	if wrap != nil {
		w = wrap(set)
	}
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, w, reads)
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
	_, before := h.scenario.RewardOutputReads()
	_, stakeBefore := h.scenario.LiveStakeReads()
	verifiedBefore, _ := h.scenario.RewardOutputReads()
	r := h.readAll(t)
	verified, mismatches := h.scenario.RewardOutputReads()
	require.Greater(t, verified, verifiedBefore, label)
	require.Equal(t, before, mismatches, "%s: reward state reads", label)
	_, stakeMismatches := h.scenario.LiveStakeReads()
	require.Equal(t, stakeBefore, stakeMismatches, "%s: live stake reads", label)
	return r
}

func (h *rewardOutputHarness) liveStake(t *testing.T, ref models.StakeCredentialRef) uint64 {
	t.Helper()
	in, err := h.scenario.LedgerDB().GetLiveStakeInputsForPools([][]byte{h.pool}, 0, nil)
	require.NoError(t, err)
	got, ok := stakeOf(in, ref)
	require.True(t, ok)
	return got
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

// TestRewardOutputsFollowSavesFoldsCompactionRoundsAndARollback requires the
// set's reward rows to agree with the metadata store's through the writers
// that run outside the block transaction (the precompute's saves, the
// withdrawal fold, the compaction), a credit round and a rollback that
// restores a folded output, with live stake exact throughout.
func TestRewardOutputsFollowSavesFoldsCompactionRoundsAndARollback(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.requireAgree(t, "before any reward row")

	h.creditPostSnapshot(t)
	h.creditLater(t)
	r := h.requireAgree(t, "after post-snapshot credits")
	require.Len(t, r.postSnap, 2)
	require.Equal(t, uint64(23), uint64(r.postSnap[0].Amount), "the owner's two credits total")
	require.Equal(t, uint64(5), uint64(r.postSnap[1].Amount))
	require.Len(t, r.postLater, 1)

	h.saveRounds(t)
	r = h.requireAgree(t, "after saving outputs")
	require.False(t, r.hasPending, "no round is credited yet")
	require.Equal(t, map[uint64]uint64{11: 7, 12: 70, 13: 700}, h.setOutputs(t, h.deleg),
		"the guarded output is not held")
	require.Equal(t, map[uint64]uint64{11: 3, 12: 30, 13: 300}, h.setOutputs(t, h.owner),
		"the non-spendable output is not held")
	require.Equal(t, uint64(20_000_009), h.liveStake(t, h.deleg),
		"the post-snapshot credits are in the account's reward")

	h.applyRound(t, 11)
	h.applyRound(t, 12)
	r = h.requireAgree(t, "after crediting rounds 11 and 12")
	require.True(t, r.hasPending)
	require.True(t, r.applied[11])
	require.False(t, r.applied[13])
	require.Len(t, r.rounds, 2)
	require.Len(t, r.pending["deleg"], 2, "round 13 is not credited")
	require.Equal(t, uint64(20_000_086), h.liveStake(t, h.deleg), "live stake adds the credits")
	require.Equal(t, uint64(20_000_056), h.liveStake(t, h.owner))

	h.scenario.FoldForWithdrawal(h.deleg)
	r = h.requireAgree(t, "after the withdrawal fold")
	require.Empty(t, r.pending["deleg"])
	require.Len(t, r.pending["owner"], 2)
	require.True(t, r.hasPending, "the owner's credits are still pending")
	require.Equal(t, map[uint64]uint64{13: 700}, h.setOutputs(t, h.deleg),
		"the fold removes the credited rounds' rows")
	require.Equal(t, uint64(20_000_086), h.liveStake(t, h.deleg), "folding does not change the stake")

	h.applyRound(t, 13)
	require.False(t, h.scenario.CompactRewardCredits(1000), "round 11 is due")
	require.True(t, h.scenario.CompactRewardCredits(1000), "then nothing is")
	r = h.requireAgree(t, "after the compaction")
	require.False(t, r.unfolded[11], "the compaction folded round 11")
	require.True(t, r.unfolded[12])
	require.Equal(t, map[uint64]uint64{12: 30, 13: 300}, h.setOutputs(t, h.owner))
	require.Equal(t, uint64(20_000_356), h.liveStake(t, h.owner))

	h.scenario.RollbackBeforeBoundary()
	r = h.requireAgree(t, "after the rollback")
	require.False(t, r.hasPending)
	require.Empty(t, r.rounds, "the rollback removes the rounds")
	require.Len(t, r.postSnap, 2, "a credit at the target's slot survives")
	require.Empty(t, r.postLater, "a credit after it does not")
	require.Equal(t, map[uint64]uint64{11: 7}, h.setOutputs(t, h.deleg),
		"the rollback unfolds round 11, whose output the target keeps, and drops the rest")
	require.Equal(t, map[uint64]uint64{11: 3}, h.setOutputs(t, h.owner))
	require.Equal(t, uint64(20_000_005), h.liveStake(t, h.deleg))
	rounds, err := h.set.RewardRounds(nil)
	require.NoError(t, err)
	require.Empty(t, rounds)
}

// TestRewardOutputsLoadFromTheMetadataStore requires a set from before it kept
// reward rows to load the eligible unfolded outputs, the credited rounds and
// the post-snapshot credits from the metadata store, as the set that followed
// the writers holds them.
func TestRewardOutputsLoadFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.creditPostSnapshot(t)
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	h.scenario.FoldForWithdrawal(h.deleg)

	fresh, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })
	loaded, err := fresh.RewardOutputsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(t, fresh.LoadLedgerState(t.Context(), h.store))
	loaded, err = fresh.RewardOutputsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	for _, ref := range []models.StakeCredentialRef{h.owner, h.deleg} {
		want, err := h.set.CredentialRewardOutputs(ref.Tag, ref.Key, nil)
		require.NoError(t, err)
		got, err := fresh.CredentialRewardOutputs(ref.Tag, ref.Key, nil)
		require.NoError(t, err)
		require.NotEmpty(t, want)
		require.Equal(t, want, got)
	}
	wantRounds, err := h.set.RewardRounds(nil)
	require.NoError(t, err)
	gotRounds, err := fresh.RewardRounds(nil)
	require.NoError(t, err)
	require.Len(t, wantRounds, 2)
	require.Equal(t, wantRounds, gotRounds)
	wantEpochs, err := h.set.RewardOutputEpochs(nil)
	require.NoError(t, err)
	gotEpochs, err := fresh.RewardOutputEpochs(nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{11, 12, 13}, wantEpochs)
	require.Equal(t, wantEpochs, gotEpochs)

	// The loaded total stands for the sources the followed set keeps apart.
	total := func(set *utxoset.Set) map[string]uint64 {
		cs, err := set.PostSnapshotCredits(h.start, nil)
		require.NoError(t, err)
		out := map[string]uint64{}
		for _, c := range cs {
			out[c.Ref.MapKey()] += c.Amount
		}
		return out
	}
	require.Equal(t, total(h.set), total(fresh))
	require.Equal(t, uint64(23), total(fresh)[h.owner.MapKey()])
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

// TestRewardOutputsVerificationCountsADivergence requires each verified read
// to count a mismatch when the metadata store holds a round and a credit the
// set never saw, and the live stake read to count the credit it lacks.
func TestRewardOutputsVerificationCountsADivergence(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	meta := h.scenario.DB.Metadata()
	// Written around the ledger's LedgerDB, so the set never sees them.
	require.NoError(t, meta.AddAppliedRewardCreditRound(h.round(11), nil))
	require.NoError(t, meta.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			h.output(h.deleg, 11, 9, h.start+11, h.round(11).BoundarySlot),
		}, nil,
	))
	_, before := h.scenario.RewardOutputReads()
	_, stakeBefore := h.scenario.LiveStakeReads()
	ldb := h.scenario.LedgerDB()

	applied, err := ldb.HasAppliedRewardCreditRound(11, nil)
	require.NoError(t, err)
	require.True(t, applied, "the read answers from the metadata store")
	_, mismatches := h.scenario.RewardOutputReads()
	require.Equal(t, before+1, mismatches)
	has, err := ldb.HasPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.True(t, has)
	pending, err := ldb.GetPendingRewardAccountOutputsForCredential(h.deleg.Tag, h.deleg.Key, nil)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	_, mismatches = h.scenario.RewardOutputReads()
	require.Equal(t, before+3, mismatches)

	in, err := ldb.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, nil)
	require.NoError(t, err)
	got, _ := stakeOf(in, h.deleg)
	require.Equal(t, uint64(20_000_009), got)
	_, stakeMismatches := h.scenario.LiveStakeReads()
	require.Equal(t, stakeBefore+1, stakeMismatches,
		"a credit the set never saw is a disagreement, not a tolerance")
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
	before, beforeMismatches := h.scenario.RewardOutputReads()
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

	verified, mismatches := h.scenario.RewardOutputReads()
	require.Equal(t, before+8, verified, "every read here is compared")
	require.Equal(t, beforeMismatches, mismatches)
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

// TestVerifyReadsOfAPinnedTransactionCompareAsOfThePin requires a verify read
// through a read transaction pinned before the compaction folds a round, as
// the ratification job's is after its boundary commits, to compare with the
// set as of the pin and count no mismatch. The metadata store answers from the
// pin's snapshot, which lacks the fold, and so does the set's snapshot.
func TestVerifyReadsOfAPinnedTransactionCompareAsOfThePin(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	h.applyRound(t, 13)
	pinned := h.scenario.PinnedReadTxn()
	require.False(t, h.scenario.CompactRewardCredits(1000), "round 11 is due")

	_, stakeBefore := h.scenario.LiveStakeReads()
	accountBefore := h.scenario.AccountReadMismatches()
	ldb := h.scenario.LedgerDB()
	acct, err := ldb.GetAccountByCredential(
		t.Context(), h.deleg.Tag, h.deleg.Key, false, pinned,
	)
	require.NoError(t, err)
	require.Zero(t, uint64(acct.Reward), "the pin's snapshot lacks the fold")
	_, err = ldb.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, pinned)
	require.NoError(t, err)
	_, err = ldb.GetDRepVotingPowerByType(t.Context(), []uint64{2, 3}, 0, pinned)
	require.NoError(t, err)

	require.Equal(t, accountBefore, h.scenario.AccountReadMismatches(),
		"the fold is a write after the pin, not a disagreement")
	_, stakeAfter := h.scenario.LiveStakeReads()
	require.Equal(t, stakeBefore, stakeAfter)

	// A transaction pinned after the fold compares.
	fresh := h.scenario.PinnedReadTxn()
	acct, err = ldb.GetAccountByCredential(
		t.Context(), h.deleg.Tag, h.deleg.Key, false, fresh,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(7), uint64(acct.Reward), "the fold is in its snapshot")
	require.Equal(t, accountBefore, h.scenario.AccountReadMismatches())
}

// TestSetReadsOfAPinnedTransactionAnswerAsOfThePin requires set-mode reads of
// a transaction pinned before the compaction folds a round to answer what the
// metadata store's snapshot does: the fold is a commit after the pin. A
// transaction pinned after the fold sees it.
func TestSetReadsOfAPinnedTransactionAnswerAsOfThePin(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, ledger.UtxoSetReadsSet)
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	h.applyRound(t, 13)
	pinned := h.scenario.PinnedReadTxn()
	require.False(t, h.scenario.CompactRewardCredits(1000), "round 11 is due")

	read := func(
		db ledgerdb.LedgerDB,
		txn ledgerdb.Txn,
	) (reward, stake uint64) {
		acct, err := db.GetAccountByCredential(
			t.Context(), h.deleg.Tag, h.deleg.Key, false, txn,
		)
		require.NoError(t, err)
		in, err := db.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, txn)
		require.NoError(t, err)
		stake, _ = stakeOf(in, h.deleg)
		return uint64(acct.Reward), stake
	}
	ldb := h.scenario.LedgerDB()
	wantReward, wantStake := read(h.store, pinned)
	require.Zero(t, wantReward, "the pin's snapshot lacks the fold")
	reward, stake := read(ldb, pinned)
	require.Equal(t, wantReward, reward, "the pinned account read")
	require.Equal(t, wantStake, stake, "the pinned live stake read")

	fresh := h.scenario.PinnedReadTxn()
	wantReward, wantStake = read(h.store, fresh)
	require.Equal(t, uint64(7), wantReward, "a pin after the fold sees it")
	reward, stake = read(ldb, fresh)
	require.Equal(t, wantReward, reward, "the account read after the fold")
	require.Equal(t, wantStake, stake, "the live stake read after the fold")
}

// TestSetReadsOfAPinnedTransactionStayOnTheStoreWithoutASnapshot requires a
// pinned transaction whose set snapshot failed to answer from the metadata
// store's snapshot, not from the set's later state.
func TestSetReadsOfAPinnedTransactionWithoutASnapshotUseTheStore(
	t *testing.T,
) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t,
		func(s *utxoset.Set) ledger.UtxoSetWriter {
			return unsnapshottableSet{s}
		},
		ledger.UtxoSetReadsSet,
	)
	h.saveRounds(t)
	h.applyRound(t, 11)
	h.applyRound(t, 12)
	h.applyRound(t, 13)
	pinned := h.scenario.PinnedReadTxn()
	require.False(t, h.scenario.CompactRewardCredits(1000), "round 11 is due")

	acct, err := h.scenario.LedgerDB().GetAccountByCredential(
		t.Context(), h.deleg.Tag, h.deleg.Key, false, pinned,
	)
	require.NoError(t, err)
	require.Zero(t, uint64(acct.Reward), "the store's snapshot lacks the fold")
}
