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
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// governanceReadNames are the LedgerDB governance reads the set serves in
// set mode; GetGovernanceVotesAtSlot reads vote history and stays on the
// store.
var governanceReadNames = []string{
	"GetGovernanceProposal", "GetActiveGovernanceProposals",
	"GetGovernanceProposalSet", "GetGovernanceProposalSetAtSlot",
	"GetExpiringGovernanceProposals", "GetExpiredGovernanceProposalsAt",
	"GetExpiredAwaitingDropGovernanceProposals",
	"GetDroppedGovernanceProposalsAt", "GetEnactedGovernanceProposalsAt",
	"GetRatifiedGovernanceProposals", "GetLastEnactedGovernanceProposal",
	"GetGovernanceVotes",
}

func optU64(p *uint64) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatUint(*p, 10)
}

func optU32(p *uint32) string {
	if p == nil {
		return "-"
	}
	return strconv.FormatUint(uint64(*p), 10)
}

// proposalRow renders every field of a proposal, row ID included. The
// action CBOR is left out when withCbor is false.
func proposalRow(p *models.GovernanceProposal, withCbor bool) string {
	if p == nil {
		return "<nil>"
	}
	cbor := fmt.Sprintf("%x", p.GovActionCbor)
	if !withCbor {
		cbor = ""
	}
	return fmt.Sprintf(
		"id=%d %x#%d type=%d proposed=%d expires=%d parent=%x/%s "+
			"enacted=%s/%s ratified=%s/%s expired=%s/%s dropped=%s/%s "+
			"policy=%x anchor=%s/%x deposit=%d return=%x added=%d deleted=%s "+
			"txindex=%s cbor=%s",
		p.ID, p.TxHash, p.ActionIndex, p.ActionType, p.ProposedEpoch,
		p.ExpiresEpoch, p.ParentTxHash, optU32(p.ParentActionIdx),
		optU64(p.EnactedEpoch), optU64(p.EnactedSlot),
		optU64(p.RatifiedEpoch), optU64(p.RatifiedSlot),
		optU64(p.ExpiredEpoch), optU64(p.ExpiredSlot),
		optU64(p.DroppedEpoch), optU64(p.DroppedSlot),
		p.PolicyHash, p.AnchorURL, p.AnchorHash, p.Deposit, p.ReturnAddress,
		p.AddedSlot, optU64(p.DeletedSlot), optU32(p.TxIndex), cbor,
	)
}

func proposalRows(ps []*models.GovernanceProposal, withCbor bool) []string {
	rows := make([]string, 0, len(ps))
	for _, p := range ps {
		rows = append(rows, proposalRow(p, withCbor))
	}
	return rows
}

func voteRow(v *models.GovernanceVote) string {
	return fmt.Sprintf(
		"id=%d proposal=%d voter=%d/%d/%x vote=%d anchor=%s/%x added=%d "+
			"updated=%s deleted=%s cast=%x/%d",
		v.ID, v.ProposalID, v.VoterType, v.VoterCredentialTag,
		v.VoterCredential, v.Vote, v.AnchorURL, v.AnchorHash, v.AddedSlot,
		optU64(v.VoteUpdatedSlot), optU64(v.DeletedSlot), v.CastTxHash,
		v.CastCertIndex,
	)
}

// requireProposalVoteReads reads every proposal and vote read through the
// ledger's LedgerDB and requires each answer, order and row IDs included,
// to be the metadata store's. Votes are compared in row ID order, which is
// what the store's unordered read yields. The probes cover the epochs and
// slots the stored proposals name and the ones next to them.
func requireProposalVoteReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	stage string,
	withCbor bool,
) {
	t.Helper()
	ctx := t.Context()
	all, err := store.GetAllGovernanceProposals(ctx, nil)
	require.NoError(t, err, stage)
	epochs := map[uint64]struct{}{0: {}}
	slots := map[uint64]struct{}{0: {}, 1 << 40: {}}
	addEpoch := func(p *uint64) {
		if p != nil {
			for _, e := range []uint64{*p, *p + 1} {
				epochs[e] = struct{}{}
			}
			if *p > 0 {
				epochs[*p-1] = struct{}{}
			}
		}
	}
	addSlot := func(p *uint64) {
		if p != nil {
			slots[*p], slots[*p+1] = struct{}{}, struct{}{}
			if *p > 0 {
				slots[*p-1] = struct{}{}
			}
		}
	}
	for _, p := range all {
		addEpoch(&p.ProposedEpoch)
		addEpoch(&p.ExpiresEpoch)
		for _, e := range []*uint64{p.EnactedEpoch, p.RatifiedEpoch, p.ExpiredEpoch, p.DroppedEpoch} {
			addEpoch(e)
		}
		addSlot(&p.AddedSlot)
		for _, s := range []*uint64{p.EnactedSlot, p.RatifiedSlot, p.ExpiredSlot, p.DroppedSlot, p.DeletedSlot} {
			addSlot(s)
		}
	}
	same := func(name string, want, got []*models.GovernanceProposal, wantErr, gotErr error) {
		t.Helper()
		label := stage + ": " + name
		require.Equal(t, wantErr == nil, gotErr == nil, label, wantErr, gotErr)
		require.Equal(
			t,
			proposalRows(want, withCbor),
			proposalRows(got, withCbor),
			label,
		)
	}
	for _, p := range all {
		want, wantErr := store.GetGovernanceProposal(
			ctx,
			p.TxHash,
			p.ActionIndex,
			nil,
		)
		got, gotErr := ldb.GetGovernanceProposal(
			ctx,
			p.TxHash,
			p.ActionIndex,
			nil,
		)
		require.Equal(t, wantErr == nil, gotErr == nil, stage, wantErr, gotErr)
		if wantErr != nil {
			require.ErrorIs(
				t,
				gotErr,
				models.ErrGovernanceProposalNotFound,
				stage,
			)
		}
		require.Equal(
			t,
			proposalRow(want, withCbor),
			proposalRow(got, withCbor),
			stage,
		)
	}
	_, missing := ldb.GetGovernanceProposal(
		ctx,
		bytes.Repeat([]byte{0xfe}, 32),
		9,
		nil,
	)
	require.True(
		t,
		errors.Is(missing, models.ErrGovernanceProposalNotFound),
		stage,
	)
	want, wantErr := store.GetGovernanceProposalSet(ctx, nil)
	got, gotErr := ldb.GetGovernanceProposalSet(ctx, nil)
	same("proposal set", want, got, wantErr, gotErr)
	want, wantErr = store.GetRatifiedGovernanceProposals(ctx, nil)
	got, gotErr = ldb.GetRatifiedGovernanceProposals(ctx, nil)
	same("ratified", want, got, wantErr, gotErr)
	for e := range epochs {
		want, wantErr = store.GetActiveGovernanceProposals(ctx, e, nil)
		got, gotErr = ldb.GetActiveGovernanceProposals(ctx, e, nil)
		same(fmt.Sprintf("active at %d", e), want, got, wantErr, gotErr)
		want, wantErr = store.GetExpiringGovernanceProposals(ctx, e, nil)
		got, gotErr = ldb.GetExpiringGovernanceProposals(ctx, e, nil)
		same(fmt.Sprintf("expiring at %d", e), want, got, wantErr, gotErr)
		want, wantErr = store.GetExpiredAwaitingDropGovernanceProposals(
			ctx,
			e,
			nil,
		)
		got, gotErr = ldb.GetExpiredAwaitingDropGovernanceProposals(ctx, e, nil)
		same(fmt.Sprintf("awaiting drop at %d", e), want, got, wantErr, gotErr)
		for s := range slots {
			label := fmt.Sprintf("at epoch %d slot %d", e, s)
			want, wantErr = store.GetExpiredGovernanceProposalsAt(
				ctx,
				e,
				s,
				nil,
			)
			got, gotErr = ldb.GetExpiredGovernanceProposalsAt(ctx, e, s, nil)
			same("expired "+label, want, got, wantErr, gotErr)
			want, wantErr = store.GetDroppedGovernanceProposalsAt(
				ctx,
				e,
				s,
				nil,
			)
			got, gotErr = ldb.GetDroppedGovernanceProposalsAt(ctx, e, s, nil)
			same("dropped "+label, want, got, wantErr, gotErr)
			want, wantErr = store.GetEnactedGovernanceProposalsAt(
				ctx,
				e,
				s,
				nil,
			)
			got, gotErr = ldb.GetEnactedGovernanceProposalsAt(ctx, e, s, nil)
			same("enacted "+label, want, got, wantErr, gotErr)
		}
	}
	for s := range slots {
		want, wantErr = store.GetGovernanceProposalSetAtSlot(ctx, s, nil)
		got, gotErr = ldb.GetGovernanceProposalSetAtSlot(ctx, s, nil)
		same(fmt.Sprintf("set at slot %d", s), want, got, wantErr, gotErr)
	}
	for _, purpose := range [][]uint8{{0}, {1}, {3, 4}, {1, 2}} {
		w, err := store.GetLastEnactedGovernanceProposal(ctx, purpose, nil)
		require.NoError(t, err, stage)
		g, err := ldb.GetLastEnactedGovernanceProposal(ctx, purpose, nil)
		require.NoError(t, err, stage)
		require.Equal(
			t,
			proposalRow(w, withCbor),
			proposalRow(g, withCbor),
			stage,
		)
	}
	ids := []uint{999_999}
	for _, p := range all {
		ids = append(ids, p.ID)
	}
	for _, id := range ids {
		w, err := store.GetGovernanceVotes(ctx, id, nil)
		require.NoError(t, err, stage)
		g, err := ldb.GetGovernanceVotes(ctx, id, nil)
		require.NoError(t, err, stage)
		require.NotNil(t, g, stage)
		var wantRows, gotRows []string
		slices.SortFunc(
			w,
			func(a, b *models.GovernanceVote) int { return int(a.ID) - int(b.ID) },
		)
		for _, v := range w {
			wantRows = append(wantRows, voteRow(v))
		}
		for _, v := range g {
			gotRows = append(gotRows, voteRow(v))
		}
		require.Equal(t, wantRows, gotRows, "%s: votes on %d", stage, id)
	}
}

// TestGovernanceReadsFromTheLiveUtxoSet reads every proposal and vote read
// through the ledger's LedgerDB in set mode with the metadata store's reads
// failing, after each write of the proposal scenario (submissions, votes,
// re-votes, ratification and its clearing, a DRep's votes deleted, an
// expiry, an era translation of the action CBOR, a drop and an enactment),
// inside an abandoned write transaction and after a rollback, and requires
// every answer to be the store's, row IDs and order included.
func TestGovernanceReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	reached, _ := scenario.HideStoreReads(governanceReadNames...)
	ldb := scenario.LedgerDB()
	steps := 0
	scenario.OnProposalWrite(func(step int) {
		steps = step
		requireProposalVoteReads(
			t,
			ldb,
			store,
			fmt.Sprintf("write %d", step),
			true,
		)
	})
	slots := scenario.Proposals()
	require.Equal(t, 12, steps)
	stored, err := store.GetAllGovernanceProposals(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, stored, 2)
	for _, p := range stored {
		require.NotZero(t, p.ID)
	}
	votes, err := ldb.GetGovernanceVotes(t.Context(), stored[0].ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 2, "both of the first proposal's DRep votes are live")
	for _, v := range votes {
		require.NotZero(t, v.ID)
		require.Equal(t, stored[0].ID, v.ProposalID)
	}
	scenario.ProposalsInsideTxn(slots.Enact + 1)
	requireProposalVoteReads(t, ldb, store, "after an abandoned write", true)
	scenario.RollbackBeforeBoundary()
	// A rollback across an era translation leaves the set the action CBOR
	// from before it, where the store keeps the translated bytes.
	requireProposalVoteReads(t, ldb, store, "after the rollback", false)
	require.Zero(t, reached(), "a read reached the metadata store")
	_, mismatches := scenario.GovernanceReads()
	require.Zero(t, mismatches)
}

func updateCommitteeCbor(
	t *testing.T,
	cold lcommon.Credential,
	expiry uint64,
) []byte {
	t.Helper()
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil, nil,
		map[*lcommon.Credential]uint64{&cold: expiry},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	return encoded
}

// TestGovernanceTallyAndCommitteeLineageFromTheLiveUtxoSet tallies a
// proposal and resolves pending committee members through the ledger's
// LedgerDB in set mode with the metadata store's governance reads failing,
// and requires the results to be the ones the store gives. The tally reads
// the proposal's votes by its row ID. The lineage walk tracks visited
// proposals by row ID, and two proposals that add a credential in one slot
// are told apart by it, against the order the list comes in.
func TestGovernanceTallyAndCommitteeLineageFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	reached, _ := scenario.HideStoreReads(governanceReadNames...)
	ldb := scenario.LedgerDB()
	ctx := t.Context()
	d1 := bytes.Repeat([]byte{0xd1}, 28)
	d2 := bytes.Repeat([]byte{0xd2}, 28)
	drepState := &governance.DRepVotingState{
		Dreps: []*models.Drep{
			{CredentialTag: 0, Credential: d1},
			{CredentialTag: 0, Credential: d2},
		},
		Powers: map[string]uint64{
			models.NewStakeCredentialRef(0, d1).MapKey(): 100,
			models.NewStakeCredentialRef(0, d2).MapKey(): 50,
		},
	}
	tallied := 0
	scenario.OnProposalWrite(func(step int) {
		// The first proposal is voted on by step 3 and revoted by step 4.
		if step < 3 || step > 8 {
			return
		}
		stored, err := store.GetAllGovernanceProposals(ctx, nil)
		require.NoError(t, err)
		proposal := stored[0]
		tally := func(db ledgerdb.LedgerDB) *governance.ProposalTally {
			tc := &governance.TallyContext{
				DB: db, DRepState: drepState,
				SPOState:       &governance.SPOVotingState{},
				CommitteeState: &governance.CommitteeVotingState{},
			}
			got, err := governance.TallyProposal(ctx, tc, proposal)
			require.NoError(t, err)
			return got
		}
		want, got := tally(store), tally(ldb)
		require.Equal(t, want, got, "tally after write %d", step)
		require.Equal(t, proposal.ID, got.ProposalID)
		require.NotZero(
			t,
			got.DRepYesStake+got.DRepNoStake+got.DRepAbstainStake,
			"the tally counted no vote after write %d",
			step,
		)
		tallied++
	})
	slots := scenario.Proposals()
	require.NotZero(t, tallied)

	cold := func(b byte) lcommon.Credential {
		c := lcommon.Credential{CredType: lcommon.CredentialTypeAddrKeyHash}
		copy(c.Credential[:], bytes.Repeat([]byte{b}, 28))
		return c
	}
	tied, chained := cold(0xc1), cold(0xc2)
	hash := func(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }
	zero := uint32(0)
	enacted := slots.Enact + 2
	slot := slots.Enact + 10
	write := func(p *models.GovernanceProposal) {
		txn := ldb.Begin(ctx, true)
		defer txn.Release()
		if p.ActionType == 0 {
			p.ActionType = uint8(lcommon.GovActionTypeUpdateCommittee)
		}
		p.ProposedEpoch, p.ExpiresEpoch = slots.Epoch, slots.Epoch+50
		p.AnchorHash = bytes.Repeat([]byte{1}, 32)
		p.ReturnAddress = bytes.Repeat([]byte{0xe0}, 29)
		require.NoError(t, ldb.SetGovernanceProposal(ctx, p, txn))
		require.NoError(t, txn.Commit())
	}
	root := &models.GovernanceProposal{
		TxHash: hash(0xb0), AddedSlot: slots.Enact + 1,
		EnactedEpoch: &slots.Epoch, EnactedSlot: &enacted,
		GovActionCbor: updateCommitteeCbor(t, cold(0xc9), 5),
	}
	write(root)
	// Submitted in this order: the lower hash, so the one the list puts
	// first, has the lower row ID.
	first := &models.GovernanceProposal{
		TxHash: hash(0xb1), AddedSlot: slot,
		ParentTxHash: hash(0xb0), ParentActionIdx: &zero,
		GovActionCbor: updateCommitteeCbor(t, tied, 50),
	}
	write(first)
	second := &models.GovernanceProposal{
		TxHash: hash(0xb2), AddedSlot: slot,
		ParentTxHash: hash(0xb0), ParentActionIdx: &zero,
		GovActionCbor: updateCommitteeCbor(t, tied, 60),
	}
	write(second)
	grandchild := &models.GovernanceProposal{
		TxHash: hash(0xb3), AddedSlot: slot + 1,
		ParentTxHash: hash(0xb1), ParentActionIdx: &zero,
		GovActionCbor: updateCommitteeCbor(t, chained, 70),
	}
	write(grandchild)
	require.Less(t, first.ID, second.ID)
	// Two enacted proposals of one purpose in one transition, neither the
	// other's child: the store picks the later row ID, which is the one the
	// submission order, by hash, puts first.
	const purpose = 6
	for _, h := range []byte{0xa2, 0xa1} {
		write(&models.GovernanceProposal{
			TxHash: hash(h), ActionType: purpose, AddedSlot: slot + 2,
			EnactedEpoch: &slots.Epoch, EnactedSlot: &enacted,
		})
	}
	write(&models.GovernanceProposal{
		TxHash: hash(0xd0), AddedSlot: slot + 3, DeletedSlot: &enacted,
	})
	lastEnacted, err := store.GetLastEnactedGovernanceProposal(
		ctx,
		[]uint8{purpose},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, hash(0xa1), lastEnacted.TxHash)
	got, err := ldb.GetLastEnactedGovernanceProposal(ctx, []uint8{purpose}, nil)
	require.NoError(t, err)
	require.Equal(t, proposalRow(lastEnacted, true), proposalRow(got, true))
	requireProposalVoteReads(
		t,
		ldb,
		store,
		"with the committee proposals",
		true,
	)

	resolve := func(db ledgerdb.LedgerDB, c lcommon.Credential) (*lcommon.CommitteeMember, uint64) {
		proposals, err := db.GetGovernanceProposalSet(ctx, nil)
		require.NoError(t, err)
		rootProposal, err := db.GetLastEnactedGovernanceProposal(
			ctx, []uint8{
				uint8(lcommon.GovActionTypeNoConfidence),
				uint8(lcommon.GovActionTypeUpdateCommittee),
			}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, rootProposal)
		member, termStart, err := governance.ResolveCommitteeProposal(
			proposals, rootProposal, c, nil,
		)
		require.NoError(t, err)
		return member, termStart
	}
	for name, c := range map[string]lcommon.Credential{"tied": tied, "chained": chained} {
		wantMember, wantStart := resolve(store, c)
		gotMember, gotStart := resolve(ldb, c)
		require.NotNil(t, wantMember, name)
		require.Equal(t, wantMember, gotMember, name)
		require.Equal(t, wantStart, gotStart, name)
	}
	member, _ := resolve(ldb, tied)
	require.Equal(
		t,
		uint64(60),
		member.ExpiryEpoch,
		"the later submission wins the tie",
	)
	member, start := resolve(ldb, chained)
	require.Equal(t, uint64(70), member.ExpiryEpoch)
	require.Equal(t, slot+1, start)
	require.Zero(t, reached(), "a read reached the metadata store")
}
