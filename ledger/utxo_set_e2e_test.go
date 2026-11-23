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
	"math"
	"math/big"
	"slices"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// recordingSet applies each commit's batch to a set and keeps a copy.
type recordingSet struct {
	*utxoset.Set
	mu      sync.Mutex
	batches []recordedBatch
}

type recordedBatch struct {
	tip     *ocommon.Point
	changes ledger.LedgerChanges
}

func (r *recordingSet) ApplyUtxoChanges(
	tip *ocommon.Point,
	changes []ledger.UtxoEvent,
) error {
	return r.ApplyLedgerChanges(tip, ledger.LedgerChanges{Utxo: changes})
}

func (r *recordingSet) ApplyLedgerChanges(
	tip *ocommon.Point,
	changes ledger.LedgerChanges,
) error {
	r.mu.Lock()
	r.batches = append(r.batches, recordedBatch{tip: tip, changes: changes})
	r.mu.Unlock()
	return r.Set.ApplyLedgerChanges(tip, changes)
}

func optString(p *uint64) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprint(*p)
}

// requireGovernanceMatch requires the set's proposals and votes to be the
// metadata store's rows, soft-deleted ones included, without row IDs and the
// action CBOR, which a rollback across a translation restores in the set
// only. It returns how many proposals and votes there are.
func requireGovernanceMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) (int, int) {
	t.Helper()
	ctx := t.Context()
	proposal := func(p models.GovernanceProposal) [2]string {
		index := "-"
		if p.TxIndex != nil {
			index = fmt.Sprint(*p.TxIndex)
		}
		parent := "-"
		if p.ParentActionIdx != nil {
			parent = fmt.Sprint(*p.ParentActionIdx)
		}
		return [2]string{
			fmt.Sprintf("%x#%d", p.TxHash, p.ActionIndex),
			fmt.Sprintf(
				"%d %d %d %d %s %x/%s %s/%s %s/%s %s/%s %s/%s %s %x %s %x %x",
				p.ActionType, p.ProposedEpoch, p.ExpiresEpoch, p.Deposit, index,
				p.ParentTxHash, parent,
				optString(p.EnactedEpoch), optString(p.EnactedSlot),
				optString(p.RatifiedEpoch), optString(p.RatifiedSlot),
				optString(p.ExpiredEpoch), optString(p.ExpiredSlot),
				optString(p.DroppedEpoch), optString(p.DroppedSlot),
				optString(p.DeletedSlot), p.PolicyHash, p.AnchorURL,
				p.AnchorHash, p.ReturnAddress,
			),
		}
	}
	vote := func(txHash []byte, idx uint32, v models.GovernanceVote) [2]string {
		return [2]string{
			fmt.Sprintf("%x#%d/%d/%d/%x", txHash, idx, v.VoterType, v.VoterCredentialTag, v.VoterCredential),
			fmt.Sprintf("%d %d %s %s %s %x", v.Vote, v.AddedSlot,
				optString(v.VoteUpdatedSlot), optString(v.DeletedSlot), v.AnchorURL, v.AnchorHash),
		}
	}
	rows, err := ldb.GetAllGovernanceProposals(ctx, txn)
	require.NoError(t, err)
	want, got := map[string]string{}, map[string]string{}
	for _, p := range rows {
		kv := proposal(*p)
		want[kv[0]] = kv[1]
	}
	all, err := set.ProposalsAt(nil)
	require.NoError(t, err)
	for _, p := range all {
		kv := proposal(p)
		got[kv[0]] = kv[1]
	}
	require.Equal(t, want, got, "governance proposals")
	voteRows, err := ldb.GetAllGovernanceVotes(ctx, txn)
	require.NoError(t, err)
	wantVotes, gotVotes := map[string]string{}, map[string]string{}
	for _, r := range voteRows {
		kv := vote(r.TxHash, r.ActionIndex, r.Vote)
		wantVotes[kv[0]] = kv[1]
	}
	for _, p := range all {
		votes, err := set.VotesAt(p.TxHash, p.ActionIndex, nil)
		require.NoError(t, err)
		for _, v := range votes {
			kv := vote(p.TxHash, p.ActionIndex, v)
			gotVotes[kv[0]] = kv[1]
		}
	}
	require.Equal(t, wantVotes, gotVotes, "governance votes")
	return len(want), len(wantVotes)
}

// requireGovernanceReads reads each proposal and vote query through the
// ledger's LedgerDB. With metadata reads available each read is compared with the set's, so
// the caller reads the mismatch counter for the set's own answer.
func requireGovernanceReads(
	t *testing.T,
	ledgerDB ledgerdb.LedgerDB,
	slots ledger.GovernanceSlots,
) {
	t.Helper()
	ctx := t.Context()
	for _, hash := range [][]byte{slots.P1, slots.P2} {
		p, err := ledgerDB.GetGovernanceProposal(ctx, hash, 0, nil)
		if !errors.Is(err, models.ErrGovernanceProposalNotFound) {
			require.NoError(t, err)
		}
		if p != nil {
			_, err = ledgerDB.GetGovernanceVotes(ctx, p.ID, nil)
			require.NoError(t, err)
		}
	}
	_, err := ledgerDB.GetGovernanceProposal(ctx, slots.P1, 9, nil)
	require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
	_, err = ledgerDB.GetActiveGovernanceProposals(ctx, slots.Epoch, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetActiveGovernanceProposals(ctx, slots.Epoch+9, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetGovernanceProposalSet(ctx, nil)
	require.NoError(t, err)
	for _, slot := range []uint64{
		slots.Base, slots.Submit, slots.Ratify, slots.ClearRatify, slots.Expire,
		slots.Drop, slots.Enact,
	} {
		_, err = ledgerDB.GetGovernanceProposalSetAtSlot(ctx, slot, nil)
		require.NoError(t, err)
	}
	_, err = ledgerDB.GetExpiringGovernanceProposals(ctx, slots.Epoch+9, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetExpiredGovernanceProposalsAt(ctx, slots.Epoch, slots.Expire, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetExpiredAwaitingDropGovernanceProposals(ctx, slots.Epoch+1, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetDroppedGovernanceProposalsAt(ctx, slots.Epoch, slots.Drop, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetEnactedGovernanceProposalsAt(ctx, slots.Epoch, slots.Enact, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetRatifiedGovernanceProposals(ctx, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetLastEnactedGovernanceProposal(ctx, []uint8{1}, nil)
	require.NoError(t, err)
}

// requireCommitteeMatch requires the set's committee and constitution rows
// to be what the metadata store's reads give. It returns how many committee
// member rows, deleted ones included, there are.
func requireCommitteeMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	ctx := t.Context()
	st, err := set.CommitteeStateAt(nil)
	require.NoError(t, err)
	rows, err := ldb.GetCommitteeMembersIncludeDeleted(ctx, txn)
	require.NoError(t, err)
	member := func(tag uint8, hash []byte, added, expires, term uint64, set bool, deleted *uint64) [2]string {
		d := "-"
		if deleted != nil {
			d = fmt.Sprint(*deleted)
		}
		return [2]string{
			fmt.Sprintf("%d/%x/%d", tag, hash, added),
			fmt.Sprintf("%d %d %v %s", expires, term, set, d),
		}
	}
	want := map[string]string{}
	for _, m := range rows {
		kv := member(m.ColdCredentialTag, m.ColdCredHash, m.AddedSlot,
			m.ExpiresEpoch, m.TermStartSlot, m.TermStartSlotSet, m.DeletedSlot)
		want[kv[0]] = kv[1]
	}
	got := map[string]string{}
	for _, m := range st.Members {
		kv := member(m.ColdCredentialTag, m.ColdCredHash, m.AddedSlot,
			m.ExpiresEpoch, m.TermStartSlot, m.TermStartSlotSet, m.DeletedSlot)
		got[kv[0]] = kv[1]
	}
	require.Equal(t, want, got, "committee member rows")

	quorum, err := ldb.GetCommitteeQuorum(ctx, txn)
	require.NoError(t, err)
	require.Equal(t, quorum, st.LatestQuorum(), "committee quorum")

	constitution, err := ldb.GetConstitution(txn)
	require.NoError(t, err)
	current := st.CurrentConstitution()
	require.Equal(t, constitution == nil, current == nil, "constitution presence")
	if constitution != nil {
		require.Equal(t, constitution.AnchorURL, current.AnchorURL)
		require.Equal(t, constitution.AddedSlot, current.AddedSlot)
		require.Equal(t, len(constitution.PolicyHash), len(current.PolicyHash))
	}

	auths, err := ldb.GetCommitteeHotAuthorizationsSince(ctx, 0, txn)
	require.NoError(t, err)
	wantAuth := map[string]string{}
	for _, a := range auths {
		wantAuth[fmt.Sprintf("%d/%x", a.ColdCredentialTag, a.ColdCredential)] =
			fmt.Sprintf("%d/%x/%d", a.HotCredentialTag, a.HotCredential, a.AddedSlot)
	}
	gotAuth := map[string]string{}
	for _, a := range st.Auths {
		gotAuth[fmt.Sprintf("%d/%x", a.ColdCredentialTag, a.ColdCredential)] =
			fmt.Sprintf("%d/%x/%d", a.HotCredentialTag, a.HotCredential, a.AddedSlot)
	}
	require.Equal(t, wantAuth, gotAuth, "latest hot-key authorizations")

	active, err := ldb.GetActiveCommitteeMembers(ctx, txn)
	require.NoError(t, err)
	require.Len(t, st.ActiveMembers(), len(active), "active committee members")
	for _, m := range rows {
		resigned, err := ldb.IsCommitteeMemberResigned(
			ctx, m.ColdCredentialTag, m.ColdCredHash, m.TermStartSlot, txn,
		)
		require.NoError(t, err)
		require.Equal(t, resigned,
			st.ResignedSince(m.ColdCredentialTag, m.ColdCredHash, m.TermStartSlot),
			"resignation of %x", m.ColdCredHash)
	}
	return len(rows)
}

// requireCommitteeReads reads each committee and constitution query through
// the ledger's LedgerDB. With metadata reads available each read is compared with the set's,
// so the caller reads the mismatch counter for the set's own answer.
func requireCommitteeReads(
	t *testing.T,
	ledgerDB ledgerdb.LedgerDB,
	slots ledger.CommitteeSlots,
) {
	t.Helper()
	ctx := t.Context()
	_, err := ledgerDB.GetConstitution(nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetCommitteeQuorum(ctx, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetCommitteeMembersIncludeDeleted(ctx, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetActiveCommitteeMembers(ctx, nil)
	require.NoError(t, err)
	_, err = ledgerDB.GetCommitteeAuthorizedCount(ctx, nil)
	require.NoError(t, err)
	for _, from := range []uint64{0, slots.Reauth, slots.Reauth + 1} {
		_, err = ledgerDB.GetCommitteeHotAuthorizationsSince(ctx, from, nil)
		require.NoError(t, err)
	}
	for _, cold := range [][]byte{slots.A, slots.B, slots.C} {
		for _, term := range []uint64{0, slots.Update} {
			_, err = ledgerDB.GetCommitteeMember(ctx, 0, cold, term, nil)
			if !errors.Is(err, models.ErrCommitteeMemberNotFound) {
				require.NoError(t, err)
			}
			_, err = ledgerDB.IsCommitteeMemberResigned(ctx, 0, cold, term, nil)
			require.NoError(t, err)
		}
	}
	_, err = ledgerDB.GetResignedCommitteeMembers(ctx, []models.CommitteeCredential{
		{Credential: slots.A}, {Credential: slots.B, TermStartSlot: slots.Update},
		{Credential: slots.B}, {Credential: slots.C},
	}, nil)
	require.NoError(t, err)
}

// requirePParamUpdatesMatch requires the set's protocol parameter update
// proposals to be the metadata store's: for each genesis key, the same
// proposals (slot, target epoch and CBOR) in the same order. The order
// between keys is not compared; the metadata store gives it by row ID,
// which within one transaction is the order of a map. It returns how many
// proposals there are.
func requirePParamUpdatesMatch(
	t *testing.T,
	set *utxoset.Set,
	db *database.Database,
) int {
	t.Helper()
	ldb := dbadapter.New(db)
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	epochs, err := ldb.GetEpochs(txn)
	require.NoError(t, err)
	var last uint64
	for _, e := range epochs {
		last = max(last, e.EpochId)
	}
	// GetPParamUpdates answers an epoch and the one before it, so reading
	// every epoch up to one past the last reads every proposal once the
	// duplicates are dropped; the second epoch after the last is where a
	// proposal made for the next-but-one epoch would sit.
	byID := map[uint]models.PParamUpdate{}
	for epoch := uint64(0); epoch <= last+2; epoch++ {
		rows, err := db.Metadata().GetPParamUpdates(epoch, nil)
		require.NoError(t, err)
		for _, r := range rows {
			byID[r.ID] = r
		}
	}
	ids := make([]uint, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	type proposal struct {
		Slot, Epoch uint64
		Cbor        string
	}
	want := map[string][]proposal{}
	for _, id := range ids {
		r := byID[id]
		want[string(r.GenesisHash)] = append(want[string(r.GenesisHash)],
			proposal{r.AddedSlot, r.Epoch, string(r.Cbor)})
	}
	got := map[string][]proposal{}
	n := 0
	require.NoError(t, set.PParamUpdates(func(u models.PParamUpdate) error {
		got[string(u.GenesisHash)] = append(got[string(u.GenesisHash)],
			proposal{u.AddedSlot, u.Epoch, string(u.Cbor)})
		n++
		return nil
	}))
	require.Equal(t, want, got, "protocol parameter update proposals per genesis key")
	require.Len(t, ids, n)
	return n
}

// requireOpCertsMatch requires the set's op-cert counters to be the metadata
// store's: each pool's highest sequence and the slot of its latest row. It
// returns how many pools have a counter.
func requireOpCertsMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	sequences, err := ldb.LatestPoolOpCertSequences(t.Context(), txn)
	require.NoError(t, err)
	rows, err := ldb.GetPoolBlockIssuersInSlotRange(0, math.MaxInt64, txn)
	require.NoError(t, err)
	lastSlot := map[string]uint64{}
	for _, r := range rows {
		lastSlot[string(r.PoolKeyHash)] = max(lastSlot[string(r.PoolKeyHash)], r.Slot)
	}
	want := map[string][2]uint64{}
	for pool, sequence := range sequences {
		want[pool] = [2]uint64{sequence, lastSlot[pool]}
	}
	got := map[string][2]uint64{}
	require.NoError(t, set.OpCertStates(func(st *ledger.OpCertState) error {
		got[string(st.Pool)] = [2]uint64{st.Sequence, st.Slot}
		return nil
	}))
	require.Equal(t, want, got, "op-cert counters (sequence, latest slot)")
	return len(want)
}

// requirePoolsMatch requires the set's pool states to be what the metadata
// store's rules give for the current epoch: the registration in effect, a
// later one waiting, the pending retirement and the deposit held. It
// returns how many registered pools there are.
func requirePoolsMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	epochs, err := ldb.GetEpochs(txn)
	require.NoError(t, err)
	var cur models.Epoch
	for _, e := range epochs {
		if e.EpochId > cur.EpochId {
			cur = e
		}
	}
	active, err := ldb.ListPoolKeyHashes(txn)
	require.NoError(t, err)
	pkhs := make([]lcommon.PoolKeyHash, 0, len(active))
	for _, a := range active {
		pkhs = append(pkhs, lcommon.PoolKeyHash(a))
	}
	effective, err := ldb.GetPoolRegistrationsEffectiveForEpoch(
		pkhs, cur.StartSlot, cur.EpochId, math.MaxInt64, txn,
	)
	require.NoError(t, err)
	pools, err := ldb.GetPools(t.Context(), pkhs, txn)
	require.NoError(t, err)
	deposits, err := ldb.GetPoolDepositsHeld(pkhs, txn)
	require.NoError(t, err)
	byPool := map[string]*models.Pool{}
	for i := range pools {
		byPool[string(pools[i].PoolKeyHash)] = &pools[i]
	}
	desc := func(r *models.PoolRegistration) string {
		if r == nil {
			return "none"
		}
		var owners []string
		for _, o := range r.Owners {
			owners = append(owners, fmt.Sprintf("%x", o.KeyHash))
		}
		slices.Sort(owners)
		return fmt.Sprintf("slot=%d vrf=%x pledge=%d cost=%d margin=%v reward=%d/%x meta=%s/%x owners=%v",
			r.AddedSlot, r.VrfKeyHash, r.Pledge, r.Cost, r.Margin, r.RewardAccountCredentialTag,
			r.RewardAccount, r.MetadataUrl, r.MetadataHash, owners)
	}
	optional := func(v *uint64) string {
		if v == nil {
			return "-"
		}
		return fmt.Sprint(*v)
	}
	var want, got []string
	for i := range effective {
		r := &effective[i]
		p := byPool[string(r.PoolKeyHash)]
		require.NotNil(t, p)
		latest, retiring := ledger.PoolLatestEvents(p)
		var future *models.PoolRegistration
		if latest != nil && latest.AddedSlot > r.AddedSlot {
			future = latest
		}
		var deposit *uint64
		if d, ok := deposits[string(r.PoolKeyHash)]; ok {
			deposit = &d
		}
		want = append(want, fmt.Sprintf("%x params[%s] future[%s] retiring=%s deposit=%s",
			r.PoolKeyHash, desc(r), desc(future), optional(retiring), optional(deposit)))
	}
	require.NoError(t, set.PoolStates(func(st *ledger.PoolState) error {
		got = append(got, fmt.Sprintf("%x params[%s] future[%s] retiring=%s deposit=%s",
			st.Pool, desc(st.Params), desc(st.Future), optional(st.RetiringEpoch),
			optional(st.DepositHeld)))
		return nil
	}))
	require.ElementsMatch(t, want, got, "pools")
	return len(want)
}

// requireDrepStateMatches requires the set's DReps, live delegator links
// and dormancy counter to be the metadata store's, and returns how many
// DReps and links there are.
func requireDrepStateMatches(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) (int, int) {
	t.Helper()
	drepKey := func(d *models.Drep) string {
		c := *d
		c.ID, c.Delegators = 0, nil
		if len(c.AnchorHash) == 0 {
			c.AnchorHash = nil
		}
		return fmt.Sprintf("%+v", c)
	}
	var want, got []string
	require.NoError(t, ldb.IterateDreps(txn, func(d *models.Drep) error {
		want = append(want, drepKey(d))
		return nil
	}))
	require.NoError(t, set.Dreps(func(d *models.Drep) error {
		got = append(got, drepKey(d))
		return nil
	}))
	require.ElementsMatch(t, want, got, "dreps")
	var wantLinks, gotLinks []string
	require.NoError(t, ldb.IterateLiveDrepDelegators(txn,
		func(d, s models.StakeCredentialRef) error {
			wantLinks = append(wantLinks, d.MapKey()+"|"+s.MapKey())
			return nil
		}))
	require.NoError(t, set.DRepLinks(nil, func(d, s models.StakeCredentialRef) error {
		gotLinks = append(gotLinks, d.MapKey()+"|"+s.MapKey())
		return nil
	}))
	require.ElementsMatch(t, wantLinks, gotLinks, "drep delegator links")
	wantDormant, err := ldb.GetDormantDRepEpochs(t.Context(), txn)
	require.NoError(t, err)
	gotDormant, err := set.DormantEpochs()
	require.NoError(t, err)
	require.Equal(t, wantDormant, gotDormant, "dormant epochs")
	return len(want), len(wantLinks)
}

// requireAccountsMatch requires the set's accounts to be the metadata
// store's, row and certificate IDs aside, and returns how many there are.
func requireAccountsMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	key := func(a *models.Account) string {
		c := *a
		c.ID, c.CertificateID, c.ImportDeposit = 0, 0, nil
		c.InactivityActivation, c.InactivityActivationEpoch = false, 0
		if len(c.Pool) == 0 {
			c.Pool = nil
		}
		if len(c.Drep) == 0 {
			c.Drep = nil
		}
		return fmt.Sprintf("%+v", c)
	}
	var want, got []string
	require.NoError(t, ldb.IterateAccounts(txn, func(a *models.Account) error {
		want = append(want, key(a))
		return nil
	}))
	require.NoError(t, set.Accounts(func(a *models.Account) error {
		got = append(got, key(a))
		return nil
	}))
	require.ElementsMatch(t, want, got, "accounts")
	return len(want)
}

// requirePParamsMatch requires the set's protocol parameter rows to be the
// metadata store's, row IDs aside, in write order, and returns how many
// there are.
func requirePParamsMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	n := 0
	for _, era := range eras.ErasWithDijkstra {
		want, err := ldb.ListPParamsForEra(era.Id, txn)
		require.NoError(t, err)
		for i := range want {
			want[i].ID = 0
		}
		got, err := set.PParamsForEra(era.Id)
		require.NoError(t, err)
		if len(want) == 0 {
			require.Empty(t, got, "pparams of era %d", era.Id)
			continue
		}
		require.Equal(t, want, got, "pparams of era %d", era.Id)
		n += len(want)
	}
	return n
}

// requireNetworkStateMatches requires the set's network state and donation
// rows to be the metadata store's, row IDs aside, and returns how many
// network state rows there are.
func requireNetworkStateMatches(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	var want []models.NetworkState
	for slot := uint64(math.MaxInt64); ; {
		row, err := ldb.GetNetworkStateAsOfSlot(slot, txn)
		require.NoError(t, err)
		if row == nil {
			break
		}
		c := *row
		c.ID = 0
		want = append([]models.NetworkState{c}, want...)
		if row.Slot == 0 {
			break
		}
		slot = row.Slot - 1
	}
	got, err := set.NetworkStates()
	require.NoError(t, err)
	require.ElementsMatch(t, want, got, "network states")
	wantDonations, err := ldb.ListNetworkDonations(txn)
	require.NoError(t, err)
	gotDonations, err := set.NetworkDonations()
	require.NoError(t, err)
	require.ElementsMatch(t, wantDonations, gotDonations, "donations")
	return len(want)
}

// requireEpochStateMatches requires the set's epoch and pool stake snapshot
// rows to be the metadata store's, row IDs aside.
func requireEpochStateMatches(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	want, err := ldb.GetEpochs(txn)
	require.NoError(t, err)
	got, err := set.Epochs()
	require.NoError(t, err)
	strip := func(es []models.Epoch) []models.Epoch {
		out := make([]models.Epoch, 0, len(es))
		for _, e := range es {
			e.ID = 0
			out = append(out, e)
		}
		return out
	}
	require.ElementsMatch(t, strip(want), strip(got), "epochs")
	stripSnaps := func(ss []*models.PoolStakeSnapshot) []models.PoolStakeSnapshot {
		out := make([]models.PoolStakeSnapshot, 0, len(ss))
		for _, s := range ss {
			c := *s
			c.ID = 0
			out = append(out, c)
		}
		return out
	}
	snapshots := 0
	for _, e := range want {
		// Snapshots are captured for the epochs after the boundary too.
		for epoch := e.EpochId; epoch <= e.EpochId+3; epoch++ {
			for _, typ := range []string{"mark", "set", "go", "actv"} {
				ws, err := ldb.GetPoolStakeSnapshotsByEpoch(epoch, typ, txn)
				require.NoError(t, err)
				gs, err := set.PoolStakeSnapshots(epoch, typ)
				require.NoError(t, err)
				require.ElementsMatch(t, stripSnaps(ws), stripSnaps(gs),
					"snapshots %d/%s", epoch, typ)
				snapshots += len(ws)
			}
		}
	}
	return snapshots
}

// TestValidationReadsFromTheLiveUtxoSet resolves every output the scenario
// left live, and every one it spent, the way validation does. With metadata reads available
// every answer must agree with the metadata store, and from the live set the set's
// answers must be the store's.
func TestValidationReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{"set"} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			rec := &recordingSet{Set: set}
			var db *database.Database
			scenario := ledger.RunUtxoSetScenarioForTest(
				t,
				types.StorageModeCore,
				nil,
				rec,
				func(d *database.Database) { db = d },
			)
			ldb := dbadapter.New(db)
			var refs []models.UtxoId
			want := map[string][]byte{}
			require.NoError(t, ldb.IterateLiveUtxos(
				t.Context(), nil,
				func(u *models.Utxo) error {
					refs = append(
						refs,
						models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx},
					)
					want[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
					return nil
				},
			))
			spent := 0
			for _, b := range rec.batches {
				for _, c := range b.changes.Utxo {
					for _, r := range c.Removed {
						if _, live := want[fmt.Sprintf("%x#%d", r.Hash, r.Idx)]; !live {
							refs = append(refs, r)
							spent++
						}
					}
				}
			}
			require.Positive(t, spent, "the scenario spent nothing")

			got, err := scenario.ReadsForValidation(refs)
			require.NoError(t, err)
			gotByRef := map[string][]byte{}
			for _, u := range got {
				gotByRef[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
			}
			require.Equal(t, want, gotByRef)
		})
	}
}

// TestNonceReadsFromTheLiveUtxoSet reads every block nonce the scenario
// wrote through the ledger. With metadata reads available every read must agree with the
// metadata store; from the live set the set must give the store's answers, after
// a rollback too.
func TestNonceReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{"metadata-visible", "set"} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			var db *database.Database
			scenario := ledger.RunUtxoSetScenarioForTest(
				t,
				types.StorageModeCore,
				nil,
				set,
				func(d *database.Database) {
					db = d
					// An empty set at origin holds every nonce there is.
					require.NoError(t, set.Reconcile(
						t.Context(), dbadapter.New(d), nil,
					))
				},
			)
			check := func(at ocommon.Point) {
				t.Helper()
				ldb := dbadapter.New(db)
				want, err := ldb.GetBlockNoncesInSlotRange(0, at.Slot+1, nil)
				require.NoError(t, err)
				require.NotEmpty(t, want)
				got, err := scenario.BlockNoncesInSlotRange(0, at.Slot+1)
				require.NoError(t, err)
				require.Equal(t, len(want), len(got))
				for i, w := range want {
					require.Equal(t, w.Slot, got[i].Slot)
					require.Equal(t, w.Hash, got[i].Hash)
					require.Equal(t, w.Nonce, got[i].Nonce)
					n, err := scenario.BlockNonce(
						ocommon.Point{Slot: w.Slot, Hash: w.Hash},
					)
					require.NoError(t, err)
					require.Equal(t, w.Nonce, n)
				}
				latest, ok, err := scenario.LatestBlockNonce()
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, want[len(want)-1].Slot, latest.Slot)
			}
			check(scenario.Points[4])
			require.NoError(t, scenario.Rollback(scenario.Points[3]))
			check(scenario.Points[3])
		})
	}
}

// TestLiveUtxoSetEpochStateFollowsABoundary crosses an epoch boundary with
// the snapshot manager writing through the ledger, and rolls it back. The
// set's epoch and pool stake snapshot rows must equal the metadata store's
// after the boundary and after the rollback.
func TestLiveUtxoSetEpochStateFollowsABoundary(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	ldb := dbadapter.New(scenario.DB)
	// The harness's outputs have no CBOR to rebuild a set from; the test
	// follows the epoch state only.
	require.NoError(t, set.LoadLedgerState(t.Context(), ldb))
	check := func(stage string) int {
		t.Helper()
		txn := ldb.Begin(t.Context(), false)
		defer txn.Release()
		epochs, err := ldb.GetEpochs(txn)
		require.NoError(t, err)
		require.NotEmpty(t, epochs, stage)
		requireNetworkStateMatches(t, set, ldb, txn)
		requirePParamsMatch(t, set, ldb, txn)
		requireAccountsMatch(t, set, ldb, txn)
		requireDrepStateMatches(t, set, ldb, txn)
		requirePoolsMatch(t, set, ldb, txn)
		requireOpCertsMatch(t, set, ldb, txn)
		rewarded := 0
		require.NoError(t, set.Accounts(func(a *models.Account) error {
			if a.Reward > 0 {
				rewarded++
			}
			return nil
		}))
		t.Logf("%s: %d accounts with rewards", stage, rewarded)
		return requireEpochStateMatches(t, set, ldb, txn)
	}
	check("before the boundary")
	scenario.ApplyBoundary()
	require.Positive(t, check("after the boundary"),
		"the boundary captured no snapshots")
	scenario.Donate(40)
	check("after a donation")
	var ref models.StakeCredentialRef
	var before uint64
	require.NoError(t, ldb.IterateAccounts(nil, func(a *models.Account) error {
		if a.Active && ref.Key == nil {
			ref = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
			before = uint64(a.Reward)
		}
		return nil
	}))
	require.NotNil(t, ref.Key, "the scenario has no active account")
	scenario.Credit(ref, 7)
	check("after credits")
	credited, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.Equal(t, before+14, uint64(credited.Reward),
		"each credit source applies once")
	require.NoError(t, scenario.Withdraw(ref, 5))
	check("after a withdrawal")
	withdrawn, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.Equal(t, before+9, uint64(withdrawn.Reward))
	var pool []byte
	var delegator models.StakeCredentialRef
	require.NoError(t, ldb.IterateAccounts(nil, func(a *models.Account) error {
		if a.Active && len(a.Pool) > 0 && pool == nil {
			pool = a.Pool
			delegator = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
		}
		return nil
	}))
	require.NotNil(t, pool, "the scenario has no delegated account")
	scenario.Reap(pool)
	check("after a pool reap")
	reaped, err := set.Account(delegator.Tag, delegator.Key)
	require.NoError(t, err)
	require.Empty(t, reaped.Pool)
	scenario.Expire(ref)
	check("after expirations")
	scenario.Governance(ref, 9)
	check("after governance")
	poolSlots := scenario.Pools(func() {
		check("after pool certificates")
	})
	newPool := poolSlots.Pool
	check("after pool certificates and a boundary")
	slots := scenario.OpCerts()
	check("after op-cert counters")
	requireCounter := func(pool []byte, sequence, slot uint64) {
		t.Helper()
		st, err := set.OpCertState(pool)
		require.NoError(t, err)
		require.NotNil(t, st, "pool %x has no counter", pool)
		require.Equal(t, sequence, st.Sequence, "pool %x sequence", pool)
		require.Equal(t, slot, st.Slot, "pool %x latest slot", pool)
	}
	requireCounter(slots.Pool, 6, slots.Batched)
	requireCounter(slots.Other, 2, slots.Repeated)
	cslots := scenario.Committee()
	check("after committee writes")
	func() {
		st, err := set.CommitteeStateAt(nil)
		require.NoError(t, err)
		require.Len(t, st.Members, 4, "A and B at base, C and A at the update")
		require.Nil(t, st.LatestQuorum(), "the no-confidence action clears the quorum")
		require.Empty(t, st.SeatedMembers())
		require.Equal(t, "https://example.test/c2", st.CurrentConstitution().AnchorURL)
		require.True(t, st.ResignedSince(0, cslots.B, cslots.Base))
		require.False(t, st.ResignedSince(0, cslots.B, cslots.Update))
		require.Len(t, st.Auths, 2)
		require.Equal(t, cslots.Reauth, st.AuthSince(0, cslots.A, 0).AddedSlot)
	}()
	gslots := scenario.Proposals()
	check("after proposals and votes")
	func() {
		txn := ldb.Begin(t.Context(), false)
		defer txn.Release()
		proposals, votes := requireGovernanceMatch(t, set, ldb, txn)
		require.Equal(t, 2, proposals)
		require.Equal(t, 2, votes)
		p1, err := set.ProposalAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		require.Equal(t, gslots.Enact, *p1.EnactedSlot)
		require.Equal(t, gslots.RatifyAgain, *p1.RatifiedSlot)
		require.Equal(t, []byte{0x02, 0x03}, p1.GovActionCbor)
		p2, err := set.ProposalAt(gslots.P2, 0, nil)
		require.NoError(t, err)
		require.Equal(t, gslots.Drop, *p2.DroppedSlot)
		votesOnP1, err := set.VotesAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		for _, v := range votesOnP1 {
			require.Nil(t, v.DeletedSlot, "the DRep's later vote undeletes it")
		}
	}()
	ptxn := ldb.Begin(t.Context(), false)
	st, err := set.PoolState(newPool)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Nil(t, st.Future, "the boundary promotes the re-registration")
	require.Equal(t, uint64(2_000_000), uint64(st.Params.Pledge))
	ptxn.Release()
	expiring, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.NotZero(t, expiring.ExpirationEpoch)
	txn := ldb.Begin(t.Context(), false)
	require.Equal(t, 2, requireNetworkStateMatches(t, set, ldb, txn),
		"the boundary wrote no network state")
	donations, err := set.NetworkDonations()
	require.NoError(t, err)
	require.Len(t, donations, 2)
	txn.Release()
	scenario.RollbackBeforeBoundary()
	check("after the rollback")
	restored, err := set.Account(delegator.Tag, delegator.Key)
	require.NoError(t, err)
	require.Equal(t, pool, restored.Pool, "the rollback restores the delegation")
	func() {
		st, err := set.CommitteeStateAt(nil)
		require.NoError(t, err)
		require.Len(t, st.Members, 2, "the rollback removes the later member rows")
		require.Len(t, st.SeatedMembers(), 2, "the rollback restores the soft-deleted rows")
		require.Equal(t, big.NewRat(1, 2), st.LatestQuorum())
		require.Equal(t, "https://example.test/c1", st.CurrentConstitution().AnchorURL)
		require.Empty(t, st.Auths)
		require.Empty(t, st.Resigns)
	}()
	func() {
		p1, err := set.ProposalAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, p1, "a proposal submitted before the rollback stays")
		require.Nil(t, p1.RatifiedSlot)
		require.Nil(t, p1.EnactedSlot)
		require.Equal(t, []byte{0x01}, p1.GovActionCbor,
			"the rollback restores the action CBOR from before its translation")
		p2, err := set.ProposalAt(gslots.P2, 0, nil)
		require.NoError(t, err)
		require.Nil(t, p2, "the rollback removes a proposal submitted after it")
		votes, err := set.VotesAt(gslots.P1, 0, nil)
		require.NoError(t, err)
		require.Empty(t, votes)
	}()
	requireCounter(slots.Pool, 3, slots.Base)
	gone, err := set.OpCertState(slots.Other)
	require.NoError(t, err)
	require.Nil(t, gone, "the rollback removes a counter first observed after it")
	donations, err = set.NetworkDonations()
	require.NoError(t, err)
	require.Len(t, donations, 1, "the rollback left the later donation")
}

// TestLiveUtxoSetEpochStateUndoesAMidEpochEraSwitch requires the set's epoch
// rows to follow a mid-epoch era switch and a rollback to before it, which
// gives the epoch row its source era back without removing it. Reads inside
// the rollback must give the source era before the set applies it.
func TestLiveUtxoSetEpochStateUndoesAMidEpochEraSwitch(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{
		"set",
	} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewMidEpochEraSwitchScenarioForTest(t, set)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			eraOf := func(epoch uint64) uint {
				t.Helper()
				txn := store.Begin(t.Context(), false)
				defer txn.Release()
				requireEpochStateMatches(t, set, store, txn)
				require.Positive(t, requirePParamsMatch(t, set, store, txn))
				e, err := scenario.LedgerDB().GetEpoch(epoch, nil)
				require.NoError(t, err)
				require.NotNil(t, e)
				return e.EraId
			}
			pparamsRows := func() int {
				t.Helper()
				txn := store.Begin(t.Context(), false)
				defer txn.Release()
				return requirePParamsMatch(t, set, store, txn)
			}
			before := pparamsRows()
			scenario.ApplySwitch()
			require.Equal(t, eras.ConwayEraDesc.Id, eraOf(3))
			require.Greater(t, pparamsRows(), before,
				"the switch wrote no translated parameters")
			if reads == "set" {
				require.Equal(t, eras.BabbageEraDesc.Id,
					scenario.EraInsideRollback(),
					"a read inside the rollback sees the source era")
			}
			scenario.RollbackBeforeSwitch()
			require.Equal(t, eras.BabbageEraDesc.Id, eraOf(3))
			require.Equal(t, before, pparamsRows())
		})
	}
}

// TestLiveUtxoSetPParamsFollowAClassicUpdate requires the set's protocol
// parameter rows to follow a boundary that enacts a classic update and
// translates into Conway, and its rollback, with reads agreeing with the
// metadata store.
func TestLiveUtxoSetPParamsFollowAClassicUpdate(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{
		"set",
	} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(t, set)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			rows := func() int {
				t.Helper()
				txn := store.Begin(t.Context(), false)
				defer txn.Release()
				for _, era := range []eras.EraDesc{eras.BabbageEraDesc, eras.ConwayEraDesc} {
					want, err := store.ListPParamsForEra(era.Id, txn)
					require.NoError(t, err)
					got, err := scenario.LedgerDB().ListPParamsForEra(era.Id, nil)
					require.NoError(t, err)
					require.Len(t, got, len(want), "era %d", era.Id)
					for epoch := uint64(0); epoch <= 2; epoch++ {
						w, err := store.GetPParams(epoch, era.Id, era.DecodePParamsFunc, txn)
						require.NoError(t, err)
						g, err := scenario.LedgerDB().GetPParams(
							epoch, era.Id, era.DecodePParamsFunc, nil,
						)
						require.NoError(t, err)
						require.Equal(t, w == nil, g == nil, "era %d epoch %d", era.Id, epoch)
					}
				}
				return requirePParamsMatch(t, set, store, txn)
			}
			before := rows()
			scenario.ApplyBoundary()
			// The enacted Babbage row and the translated Conway row.
			require.Equal(t, before+2, rows())
			if reads == "set" {
				afterWrite, afterRollback, afterBoth := scenario.ConwayRowsInsideTxn()
				require.Equal(t, 2, afterWrite,
					"a read in the transaction sees its own write")
				require.Zero(t, afterRollback,
					"a read in the transaction sees its staged rollback")
				require.Equal(t, 1, afterBoth,
					"a write after the rollback survives it")
			}
			scenario.RollbackBeforeBoundary()
			require.Equal(t, before, rows())
		})
	}
}

// TestLiveUtxoSetPParamUpdatesFollowTheLedger requires the set's protocol
// parameter update proposals to equal the metadata store's: the ones it
// loads, the ones valid transactions store through the single and the
// batched write, not those of an invalid transaction, across the boundary
// that enacts an update and after a rollback, which removes only the
// proposals stored after its target.
func TestLiveUtxoSetPParamUpdatesFollowTheLedger(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	seeded := requirePParamUpdatesMatch(t, set, scenario.DB)
	require.Positive(t, seeded, "the scenario seeded no update proposal")

	at := scenario.RollbackTarget.Slot
	// Proposals for the next epoch, so none changes what the boundary enacts.
	for _, p := range []ledger.UpdateProposal{
		{Slot: at, Epoch: 1, Index: 0, Keys: []byte{1, 2, 3}, MaxTxSize: 20_000},
		{Slot: at + 1, Epoch: 1, Index: 0, Keys: []byte{1, 2}, MaxTxSize: 21_000},
		{Slot: at + 1, Epoch: 1, Index: 1, Keys: []byte{1}, MaxTxSize: 22_000, Batched: true},
		{Slot: at + 2, Epoch: 1, Index: 0, Keys: []byte{4}, MaxTxSize: 23_000, Invalid: true},
		{Slot: at + 3, Epoch: 1, Index: 0, Keys: []byte{5}, MaxTxSize: 24_000, Batched: true},
	} {
		scenario.Propose(p)
	}
	const proposed, kept = 7, 3
	require.Equal(t, seeded+proposed,
		requirePParamUpdatesMatch(t, set, scenario.DB),
		"an invalid transaction's proposals are not stored")
	scenario.ApplyBoundary()
	require.Equal(t, seeded+proposed,
		requirePParamUpdatesMatch(t, set, scenario.DB),
		"the boundary keeps every proposal")
	scenario.RollbackBeforeBoundary()
	require.Equal(t, seeded+kept,
		requirePParamUpdatesMatch(t, set, scenario.DB),
		"the rollback removes the proposals stored after its target")
}

// TestLiveUtxoSetAccountsFollowGenesisStaking requires the accounts genesis
// staking writes, after the set has loaded the empty account table at
// origin, to reach the set.
func TestLiveUtxoSetAccountsFollowGenesisStaking(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewGenesisScenarioForTest(t, set)
	ldb := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), ldb))
	scenario.CreateGenesis()
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	require.Positive(t, requireAccountsMatch(t, set, ldb, txn),
		"genesis staking made no accounts")
}

// TestEpochStateReadsFromTheLiveUtxoSet reads epochs and pool stake
// snapshots through the ledger's LedgerDB across an epoch boundary and its
// rollback. With metadata reads available every read must agree with the metadata store; in
// set mode the set's answers must be the store's.
func TestEpochStateReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{"set"} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			check := func(stage string) {
				t.Helper()
				ldb := scenario.LedgerDB()
				want, err := store.GetEpochs(nil)
				require.NoError(t, err)
				got, err := ldb.GetEpochs(nil)
				require.NoError(t, err)
				require.Len(t, got, len(want), stage)
				snaps := 0
				for i, w := range want {
					require.Equal(t, w.EpochId, got[i].EpochId, stage)
					e, err := ldb.GetEpoch(w.EpochId, nil)
					require.NoError(t, err)
					require.NotNil(t, e, stage)
					require.Equal(t, w.StartSlot, e.StartSlot, stage)
					bySlot, err := ldb.GetEpochBySlot(w.StartSlot, nil)
					require.NoError(t, err)
					require.NotNil(t, bySlot, stage)
					require.Equal(t, w.EpochId, bySlot.EpochId, stage)
					for epoch := w.EpochId; epoch <= w.EpochId+3; epoch++ {
						for _, typ := range []string{"mark", "set", "go"} {
							ws, err := store.GetPoolStakeSnapshotsByEpoch(epoch, typ, nil)
							require.NoError(t, err)
							gs, err := ldb.GetPoolStakeSnapshotsByEpoch(epoch, typ, nil)
							require.NoError(t, err)
							require.Len(t, gs, len(ws), "%s %d/%s", stage, epoch, typ)
							snaps += len(ws)
							for _, sn := range ws {
								one, err := ldb.GetPoolStakeSnapshot(
									epoch, typ, sn.PoolKeyHash, nil,
								)
								require.NoError(t, err)
								require.NotNil(t, one, stage)
								require.Equal(t, sn.TotalStake, one.TotalStake, stage)
							}
						}
					}
				}
				if stage == "after the boundary" {
					require.Positive(t, snaps, "no snapshots to read")
				}
			}
			checkNetwork := func(stage string) {
				t.Helper()
				ldb := scenario.LedgerDB()
				want, err := store.GetNetworkState(nil)
				require.NoError(t, err)
				require.NotNil(t, want, stage)
				got, err := ldb.GetNetworkState(nil)
				require.NoError(t, err)
				require.NotNil(t, got, stage)
				require.Equal(t, want.Slot, got.Slot, stage)
				require.Equal(t, want.Treasury, got.Treasury, stage)
				for _, slot := range []uint64{0, want.Slot, want.Slot + 1} {
					w, err := store.GetNetworkStateAsOfSlot(slot, nil)
					require.NoError(t, err)
					g, err := ldb.GetNetworkStateAsOfSlot(slot, nil)
					require.NoError(t, err)
					require.Equal(t, w.Slot, g.Slot, "%s at %d", stage, slot)
				}
				var refs []models.StakeCredentialRef
				require.NoError(t, store.IterateAccounts(nil, func(a *models.Account) error {
					refs = append(refs, models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey))
					return nil
				}))
				require.NotEmpty(t, refs, stage)
				for _, ref := range refs {
					for _, inactive := range []bool{false, true} {
						_, err := ldb.GetAccountByCredential(
							t.Context(), ref.Tag, ref.Key, inactive, nil,
						)
						require.NoError(t, err)
					}
				}
				_, err = ldb.GetAccountsByCredential(t.Context(), refs, false, nil)
				require.NoError(t, err)
				epochs, err := store.GetEpochs(nil)
				require.NoError(t, err)
				for _, e := range epochs {
					w, err := store.SumNetworkDonationsForEpoch(e.EpochId, nil)
					require.NoError(t, err)
					g, err := ldb.SumNetworkDonationsForEpoch(e.EpochId, nil)
					require.NoError(t, err)
					require.Equal(t, w, g, "%s donations %d", stage, e.EpochId)
				}
			}
			check("before the boundary")
			checkNetwork("before the boundary")
			scenario.ApplyBoundary()
			check("after the boundary")
			checkNetwork("after the boundary")
			scenario.Donate(40)
			checkNetwork("after a donation")
			var govRef models.StakeCredentialRef
			require.NoError(t, store.IterateAccounts(nil, func(a *models.Account) error {
				if a.Active && govRef.Key == nil {
					govRef = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
				}
				return nil
			}))
			scenario.Governance(govRef, 10)
			for _, inactive := range []bool{false, true} {
				for _, key := range [][]byte{
					bytes.Repeat([]byte{0x5d}, 28), bytes.Repeat([]byte{0x5e}, 28),
				} {
					_, err := scenario.LedgerDB().GetDrepByCredential(
						t.Context(), 0, key, inactive, nil,
					)
					if !errors.Is(err, models.ErrDrepNotFound) {
						require.NoError(t, err)
					}
				}
			}
			_, err = scenario.LedgerDB().GetActiveDreps(t.Context(), nil)
			require.NoError(t, err)
			_, err = scenario.LedgerDB().GetDormantDRepEpochs(t.Context(), nil)
			require.NoError(t, err)
			poolHashes, err := store.ListPoolKeyHashes(nil)
			require.NoError(t, err)
			require.NotEmpty(t, poolHashes)
			pkhs := make([]lcommon.PoolKeyHash, 0, len(poolHashes))
			for _, p := range poolHashes {
				pkhs = append(pkhs, lcommon.PoolKeyHash(p))
			}
			epochs, err := store.GetEpochs(nil)
			require.NoError(t, err)
			var cur models.Epoch
			for _, e := range epochs {
				if e.EpochId > cur.EpochId {
					cur = e
				}
			}
			_, err = scenario.LedgerDB().GetPoolRegistrationsEffectiveForEpoch(
				pkhs, cur.StartSlot, cur.EpochId, math.MaxInt64, nil,
			)
			require.NoError(t, err)
			slots := scenario.OpCerts()
			requireOpCertReads(t, scenario.LedgerDB(), store, slots)
			scenario.OpCertsInsideTxn(slots.Pool, 9, slots.Batched+1)
			scenario.OpCertsInsideTxn(slots.Other, 7, slots.Batched+2)
			gslots := scenario.Proposals()
			requireGovernanceReads(t, scenario.LedgerDB(), gslots)
			scenario.ProposalsInsideTxn(gslots.Enact + 1)
			cslots := scenario.Committee()
			requireCommitteeReads(t, scenario.LedgerDB(), cslots)
			scenario.CommitteeInsideTxn(cslots.NoConfidence + 1)
			func() {
				txn := store.Begin(t.Context(), false)
				defer txn.Release()
				requireAccountsMatch(t, set, store, txn)
				requireDrepStateMatches(t, set, store, txn)
				require.Equal(t, 2, requireOpCertsMatch(t, set, store, txn))
				require.Equal(t, 4, requireCommitteeMatch(t, set, store, txn))
				proposals, votes := requireGovernanceMatch(t, set, store, txn)
				require.Equal(t, [2]int{2, 2}, [2]int{proposals, votes})
			}()
			if reads == "set" {
				slot, donations := scenario.NetworkInsideRollback()
				require.Zero(t, slot, "the rollback hides the boundary's state")
				require.Zero(t, donations, "the rollback hides the donation")
			}
			scenario.RollbackBeforeBoundary()
			check("after the rollback")
			checkNetwork("after the rollback")
			func() {
				txn := store.Begin(t.Context(), false)
				defer txn.Release()
				require.Equal(t, 1, requireOpCertsMatch(t, set, store, txn),
					"the rollback leaves the counter from before it")
			}()
			requireOpCertReads(t, scenario.LedgerDB(), store, slots)
			requireCommitteeReads(t, scenario.LedgerDB(), cslots)
			requireGovernanceReads(t, scenario.LedgerDB(), gslots)
			func() {
				txn := store.Begin(t.Context(), false)
				defer txn.Release()
				proposals, votes := requireGovernanceMatch(t, set, store, txn)
				require.Equal(t, [2]int{1, 0}, [2]int{proposals, votes},
					"the rollback leaves the proposal from before it")
				require.Equal(t, 2, requireCommitteeMatch(t, set, store, txn),
					"the rollback leaves the members from before it")
			}()
		})
	}
}

// requireOpCertReads reads each op-cert counter query through the ledger's
// LedgerDB and requires the answers to be the metadata store's, whatever the
// set holds. With metadata reads available each read the set can answer is compared with the
// store's, so the caller reads the mismatch counter for the set's own answer.
func requireOpCertReads(
	t *testing.T,
	ledgerDB, store ledgerdb.LedgerDB,
	slots ledger.OpCertSlots,
) {
	t.Helper()
	ctx := t.Context()
	// Not every slot is a bound the set can answer for a non-decreasing
	// history: a range or "after" bound is never placed at the lowered
	// counter's slot, which a valid chain does not produce.
	atSlots := []uint64{
		slots.Base - 1, slots.Base, slots.Raised, slots.Repeated,
		slots.Lowered, slots.Batched, slots.Batched + 1,
	}
	afterSlots := []uint64{slots.Base, slots.Raised, slots.Lowered, slots.Batched}
	ranges := [][2]uint64{
		{slots.Base, slots.Batched}, {slots.Raised, slots.Lowered},
		{slots.Repeated, slots.Lowered}, {slots.Lowered + 1, slots.Batched},
		{slots.Batched, slots.Base},
	}
	for _, pool := range [][]byte{slots.Pool, slots.Other} {
		pkh := lcommon.PoolKeyHash(pool)
		type answer struct {
			sequence uint64
			found    bool
		}
		both := func(what string, read func(ledgerdb.LedgerDB) (uint64, bool, error)) {
			t.Helper()
			ws, wf, err := read(store)
			require.NoError(t, err, what)
			gs, gf, err := read(ledgerDB)
			if err != nil {
				require.ErrorIs(t, err, ledger.ErrUtxoSetUnserved, "%x %s", pool, what)
				return
			}
			require.Equal(t, answer{ws, wf}, answer{gs, gf}, "%x %s", pool, what)
		}
		both("latest", func(l ledgerdb.LedgerDB) (uint64, bool, error) {
			return l.LatestPoolOpCertSequence(ctx, pkh, nil)
		})
		for _, slot := range afterSlots {
			both(fmt.Sprintf("after %d", slot), func(l ledgerdb.LedgerDB) (uint64, bool, error) {
				return l.LatestPoolOpCertSequenceAfter(ctx, pkh, slot, nil)
			})
		}
		for _, slot := range atSlots {
			both(fmt.Sprintf("at or before %d", slot), func(l ledgerdb.LedgerDB) (uint64, bool, error) {
				return l.LatestPoolOpCertSequenceAtOrBefore(ctx, pkh, slot, nil)
			})
		}
		for _, r := range ranges {
			both(fmt.Sprintf("in %d..%d", r[0], r[1]), func(l ledgerdb.LedgerDB) (uint64, bool, error) {
				return l.LatestPoolOpCertSequenceInRange(ctx, pkh, r[0], r[1], nil)
			})
		}
	}
	want, err := store.LatestPoolOpCertSequences(ctx, nil)
	require.NoError(t, err)
	got, err := ledgerDB.LatestPoolOpCertSequences(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestLiveUtxoSetOpCertsLoadFromTheMetadataStore requires a set from before it
// kept op-cert counters to load each pool's highest sequence and latest slot
// from the metadata store, and a read of a slot before the set's record to
// be answered from the loaded counter.
func TestLiveUtxoSetOpCertsLoadFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	poolA := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x41}, 28))
	poolB := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x42}, 28))
	for _, w := range []struct {
		pool           lcommon.PoolKeyHash
		sequence, slot uint64
	}{{poolA, 4, 100}, {poolB, 3, 150}, {poolA, 6, 200}} {
		require.NoError(t, store.UpdatePoolOpCertSequence(
			t.Context(), w.pool, w.sequence, w.slot, nil,
		))
	}
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	txn := store.Begin(t.Context(), false)
	require.Equal(t, 2, requireOpCertsMatch(t, set, store, txn))
	txn.Release()
	st, err := set.OpCertState(poolA[:])
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Equal(t, uint64(6), st.Sequence)
	require.Equal(t, uint64(200), st.Slot)

	// The set has no record of poolA at slot 150, so the counter it was
	// loaded with answers.
	ldb := scenario.LedgerDB()
	at150, _, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		t.Context(), poolA, 150, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), at150)
	at250, _, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
		t.Context(), poolA, 250, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), at250)
}

// TestOpCertReadUsesSetStateAfterAnUntrackedObservation pins the set result
// when a counter exists only in metadata.
func TestOpCertReadUsesSetStateAfterAnUntrackedObservation(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x43}, 28))
	require.NoError(t, store.UpdatePoolOpCertSequence(t.Context(), pool, 9, 10, nil))
	reached, _ := scenario.HideStoreReads("LatestPoolOpCertSequence")
	_, found, err := scenario.LedgerDB().LatestPoolOpCertSequence(t.Context(), pool, nil)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, reached(), "op-cert read reached the metadata store")
}

// TestLiveUtxoSetCommitteeLoadsFromTheMetadataStore requires a set from
// before it kept the committee rows to load them, resignations
// included, from the metadata store.
func TestLiveUtxoSetCommitteeLoadsFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	cslots := scenario.Committee()
	// Loading replaces whatever rows the set staged while it lacked the
	// marker.
	loaded, err := set.CommitteeLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	// A quorum enacted after the no-confidence action is the latest row.
	require.NoError(t, store.SetCommitteeQuorum(
		t.Context(), big.NewRat(1, 3), cslots.NoConfidence+1, nil,
	))
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	txn := store.Begin(t.Context(), false)
	defer txn.Release()
	requireCommitteeMatch(t, set, store, txn)
	st, err := set.CommitteeStateAt(nil)
	require.NoError(t, err)
	require.Equal(t, big.NewRat(1, 3), st.LatestQuorum())
	require.True(t, st.ResignedSince(0, cslots.B, cslots.Base))
	require.Equal(t, "https://example.test/c2", st.CurrentConstitution().AnchorURL)
}

// TestCommitteeReadUsesTheSetAfterAnUntrackedMetadataWrite requires a
// constitution written outside the ledger to stay out of the set's answer.
func TestCommitteeReadUsesTheSetAfterAnUntrackedMetadataWrite(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	// Written around the ledger's LedgerDB, so the set never sees it.
	require.NoError(t, store.SetConstitution(&models.Constitution{
		AnchorURL: "https://example.test/around", AnchorHash: bytes.Repeat([]byte{4}, 32),
		AddedSlot: 10,
	}, nil))
	got, err := scenario.LedgerDB().GetConstitution(nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestLiveUtxoSetGovernanceLoadsFromTheMetadataStore requires a set from
// before it kept proposals and votes to load every row, with the lifecycle
// slots and soft deletes, from the metadata store.
func TestLiveUtxoSetGovernanceLoadsFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	// Seed the empty set before the fixture applies governance writes.
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	slots := scenario.Proposals()
	// The soft delete of a DRep's vote is a row the load must carry.
	_, err = store.DeleteGovernanceVotesForDrep(
		t.Context(), 0, slots.D1, slots.Epoch, slots.Enact+1, nil,
	)
	require.NoError(t, err)
	require.NoError(t, set.InvalidateGovernance())
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	loaded, err := set.GovernanceLoaded()
	require.NoError(t, err)
	require.True(t, loaded, "metadata reload marks the governance family loaded")
	txn := store.Begin(t.Context(), false)
	defer txn.Release()
	proposals, votes := requireGovernanceMatch(t, set, store, txn)
	require.Equal(t, [2]int{2, 2}, [2]int{proposals, votes})
	p1, err := set.ProposalAt(slots.P1, 0, nil)
	require.NoError(t, err)
	require.Equal(t, slots.Enact, *p1.EnactedSlot)
	require.NotNil(t, p1.TxIndex)
}

// TestGovernanceReadUsesSetStateAfterAnUntrackedProposal pins the set result
// when a proposal exists only in metadata.
func TestGovernanceReadUsesSetStateAfterAnUntrackedProposal(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	hash := bytes.Repeat([]byte{0x94}, 32)
	require.NoError(t, store.SetGovernanceProposal(t.Context(), &models.GovernanceProposal{
		TxHash: hash, ActionType: 1, ExpiresEpoch: 9, AnchorURL: "https://example.test/x",
		AnchorHash: bytes.Repeat([]byte{9}, 32), ReturnAddress: bytes.Repeat([]byte{0xe0}, 29),
		AddedSlot: 10,
	}, nil))
	reached, _ := scenario.HideStoreReads("GetGovernanceProposal")
	got, err := scenario.LedgerDB().GetGovernanceProposal(t.Context(), hash, 0, nil)
	require.Error(t, err)
	require.Nil(t, got)
	require.Zero(t, reached(), "governance read reached the metadata store")
}

// liveStakeHarness is the boundary harness with the live UTxO set holding the
// stake of the harness's two delegated accounts, which the metadata store
// holds as rows.
type liveStakeHarness struct {
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	pool     []byte
	owner    models.StakeCredentialRef
	deleg    models.StakeCredentialRef
}

func newLiveStakeHarness(t *testing.T, stake bool) *liveStakeHarness {
	t.Helper()
	return newLiveStakeHarnessWithReads(t, stake, "metadata-visible")
}

func newLiveStakeHarnessWithReads(
	t *testing.T,
	stake bool,
	reads string,
) *liveStakeHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	h := &liveStakeHarness{
		scenario: scenario,
		store:    store,
		pool:     bytes.Repeat([]byte{0xd1}, 28),
		owner:    models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x71}, 28)),
		deleg:    models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x72}, 28)),
	}
	if stake {
		scenario.AddSetStake(h.owner.Key, 20_000_000)
		scenario.AddSetStake(h.deleg.Key, 20_000_000)
	}
	return h
}

func stakeOf(inputs []*models.RewardStakeInput, ref models.StakeCredentialRef) (uint64, bool) {
	for _, in := range inputs {
		if in.CredentialTag == ref.Tag && bytes.Equal(in.StakingKey, ref.Key) {
			return uint64(in.Stake), true
		}
	}
	return 0, false
}

// TestLiveStakeReadsSeeTheirTransactionsStagedAccounts requires a live stake
// read in a write transaction to see the account change that transaction has
// staged, as the metadata store does.
func TestLiveStakeReadsSeeTheirTransactionsStagedAccounts(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, true)
	stake := h.scenario.CreditInsideTxn(h.deleg, h.pool, 11)
	require.Equal(t, uint64(20_000_011), stake)
}

// TestLiveStakeReadsSeeAnAccountRegisteredInTheirTransaction requires the
// pool's live stake read in a write transaction to list a credential that
// transaction registered and delegated, which the set's committed index does
// not hold.
func TestLiveStakeReadsSeeAnAccountRegisteredInTheirTransaction(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, true)
	key := bytes.Repeat([]byte{0x73}, 28)
	keys := h.scenario.RegisterInsideTxn(key, h.pool)
	require.Len(t, keys, 3, "the new credential joins the two delegators")
	require.Contains(t, keys, key)
}

// TestLiveStakeReadsUseSetStateWithoutLoadedRewardRows requires live stake
// to use the set's account and UTxO state without mixing in metadata-only
// reward credits.
func TestLiveStakeReadsUseSetStateWithoutLoadedRewardRows(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, func(s *utxoset.Set) ledger.UtxoSetWriter {
		return unloadedRewardSet{s}
	})
	meta := h.scenario.DB.Metadata()
	require.NoError(t, meta.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{{SnapshotEpoch: 1, BoundarySlot: 100}}, nil,
	))
	require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{{
		Epoch: 1, CredentialTag: h.deleg.Tag, StakingKey: h.deleg.Key,
		PoolKeyHash: h.pool, RewardType: "member", Amount: 9,
		Spendable: true, CapturedSlot: 100, BoundarySlot: 100,
	}}, nil))
	ldb := h.scenario.LedgerDB()
	in, err := ldb.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, nil)
	require.NoError(t, err)
	got, _ := stakeOf(in, h.deleg)
	require.Equal(t, uint64(20_000_000), got, "metadata-only pending credits are not in the set")

	// Additional UTxO stake is reflected in the set answer.
	h.scenario.AddSetStake(h.deleg.Key, 50)
	in, err = ldb.GetLiveStakeInputsForPools([][]byte{h.pool}, 0, nil)
	require.NoError(t, err)
	got, _ = stakeOf(in, h.deleg)
	require.Equal(t, uint64(20_000_050), got)
}

// TestLiveUtxoSetAccountExpirationsAgreeAfterRollback requires the set's
// accounts to equal the metadata store's after a rollback to before the epoch
// boundary that ran the CIP-0163 activation stamp, and both to have the
// stamp undone. Production stamps at the first slot of the epoch it enters,
// so a rollback target earlier than that slot is always in an earlier epoch
// than the marker's, and the store resets the stamp as the set's journal
// does. The harness's Expire stamps at a mid-epoch slot of the current epoch,
// which a rollback target in that same epoch leaves in the store but not in
// the set; production cannot produce that pairing, so the test runs the
// stamp after a boundary.
func TestLiveUtxoSetAccountExpirationsAgreeAfterRollback(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	ldb := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), ldb))
	scenario.ApplyBoundary()
	var ref models.StakeCredentialRef
	require.NoError(t, ldb.IterateAccounts(nil, func(a *models.Account) error {
		if a.Active && ref.Key == nil {
			ref = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
		}
		return nil
	}))
	require.NotNil(t, ref.Key)
	scenario.Expire(ref)
	stamped, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.NotZero(t, stamped.ExpirationEpoch)
	scenario.RollbackBeforeBoundary()
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	requireAccountsMatch(t, set, ldb, txn)
	n := 0
	require.NoError(t, set.Accounts(func(a *models.Account) error {
		n++
		require.Zero(t, a.ExpirationEpoch, "the rollback undoes the stamp")
		return nil
	}))
	require.NotZero(t, n)
}

// TestLiveUtxoSetRewardGuardKeepsActivationFloor requires a set-backed
// ledger to preserve the activation membership used by the production reward
// guard after the SQL account family is drained. A witness in the preceding
// epoch expires before the reward snapshot, while the activation floor keeps
// the account active through that snapshot.
func TestLiveUtxoSetRewardGuardKeepsActivationFloor(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	store := dbadapter.New(scenario.DB)

	var ref models.StakeCredentialRef
	require.NoError(t, store.IterateAccounts(nil, func(a *models.Account) error {
		if a.Active && a.CredentialTag == 0 && ref.Key == nil {
			ref = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
		}
		return nil
	}))
	require.NotNil(t, ref.Key)
	scenario.WithdrawAtSlot(ref, scenario.RollbackSlot(), 0)
	witnesses, err := store.AccountLastWitnessSlots(
		t.Context(), []models.StakeCredentialRef{ref}, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Contains(t, witnesses, ref.MapKey())
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
	)
	activationEpoch, boundary := scenario.ActivateInactivityAtBoundary(1)
	witness, found, err := set.LastWitnessSlot(ref, boundary)
	require.NoError(t, err)
	require.True(t, found)
	require.Less(t, witness, boundary)
	require.False(t, scenario.RewardAccountGuarded(
		ref,
		activationEpoch+1,
		boundary,
	), "the activation floor keeps the stamped account reward-eligible")
	activated, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.True(t, activated.InactivityActivation)
	require.Equal(t, activationEpoch, activated.InactivityActivationEpoch)
	scenario.RollbackBeforeBoundary()
	rolledBack, err := set.Account(ref.Tag, ref.Key)
	require.NoError(t, err)
	require.False(t, rolledBack.InactivityActivation)
	require.Zero(t, rolledBack.InactivityActivationEpoch)
}

// TestSetReadsOfAPinnedSnapshotAreTheSetAsOfThePin requires live-set reads of
// a transaction pinned before later commits to answer the pin's DReps and
// votes, as the ratification job's do: the scenario asserts the earlier
// expiry and the single vote the pin saw.
func TestSetReadsOfAPinnedSnapshotAreTheSetAsOfThePin(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.SnapshotReadsAfterLaterCommit()
}

// TestSetReadsOfAPinnedSnapshotSeeABatchCommittedBeforeItsApply requires a
// pin taken after a commit, before its batch reaches the set, to read that
// commit and nothing after it: the snapshot of the set lacks the batch, so the
// pin carries it, and a later commit is not in either.
func TestSetReadsOfAPinnedSnapshotSeeABatchCommittedBeforeItsApply(
	t *testing.T,
) {
	t.Parallel()
	modes := []string{"set"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewBoundaryEpochStateScenarioForTest(
				t, set,
			)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			got := scenario.PinInsideCommit()
			require.Equal(t, got.Epoch+21, got.Pinned,
				"the pinned read sees the commit it was pinned after")
			require.Equal(t, got.Epoch+22, got.Latest,
				"an unpinned read sees the latest")
		})
	}
}

// TestResumeFromASetBehindTheLedgerRefreshesTheLiveStake leaves the live UTxO
// set behind the ledger, as a crash after the metadata commit and before the
// set's does, and resumes the ledger from the set's tip. The metadata store
// holds no utxo rows, so reward_live_stake can only come back to the set's
// stake by being recomputed from the set: the rolled-back blocks' outputs are
// not in the set's journal.
func TestResumeFromASetBehindTheLedgerRefreshesTheLiveStake(t *testing.T) {
	t.Parallel()
	only, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = only.Close() })
	var db *database.Database
	run := ledger.RunUtxoSetOnlyScenarioForTest(
		t, only, func(d *database.Database) { db = d },
	)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	liveStake := func() map[string]string {
		rows, err := raw.Query(
			`SELECT credential_tag, hex(staking_key), utxo_stake
			FROM reward_live_stake WHERE utxo_stake <> '0'`,
		)
		require.NoError(t, err)
		defer rows.Close()
		got := map[string]string{}
		for rows.Next() {
			var tag int
			var key, stake string
			require.NoError(t, rows.Scan(&tag, &key, &stake))
			got[fmt.Sprintf("%d/%s", tag, key)] = stake
		}
		require.NoError(t, rows.Err())
		return got
	}
	setStake := func() map[string]string {
		got := map[string]string{}
		require.NoError(t, only.EachCredentialStake(
			func(tag uint8, hash []byte, n uint64) error {
				got[fmt.Sprintf("%d/%X", tag, hash)] = fmt.Sprint(n)
				return nil
			},
		))
		return got
	}
	require.Equal(t, setStake(), liveStake(), "stake differs before the resume")

	behindAt := run.Points[1]
	_, err = only.Rollback(behindAt)
	require.NoError(t, err)
	resumed, err := run.Resume(t.Context(), behindAt)
	require.NoError(t, err)
	require.True(t, resumed)
	require.NotEmpty(t, setStake())
	require.Equal(t, setStake(), liveStake(),
		"reward_live_stake keeps the stake of blocks the resume rolled back")
}
