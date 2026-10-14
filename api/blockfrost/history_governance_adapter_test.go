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

package blockfrost

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var asc = PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc}

func rewardAccount(t *testing.T, key byte) []byte {
	t.Helper()
	return append([]byte{0xe0}, bytes32of(28, key)...)
}

func mustCbor(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := cbor.Encode(value)
	require.NoError(t, err)
	return encoded
}

// treasuryActionCbor encodes a treasury withdrawal of amount to each of the
// reward accounts. A Go map cannot hold byte-string keys, so the map is
// assembled by hand.
func treasuryActionCbor(t *testing.T, amounts map[byte]uint64, keys ...byte) []byte {
	t.Helper()
	out := []byte{0x83, 0x02, 0xa0 + byte(len(keys))}
	for _, key := range keys {
		out = append(out, 0x58, 29)
		out = append(out, rewardAccount(t, key)...)
		out = append(out, mustCbor(t, amounts[key])...)
	}
	return append(out, 0xf6)
}

func (fx *epochFixture) addProposal(
	t *testing.T,
	tx byte,
	index uint32,
	actionType lcommon.GovActionType,
	actionCbor []byte,
	slot uint64,
) *models.GovernanceProposal {
	t.Helper()
	proposal := &models.GovernanceProposal{
		TxHash:        fill32(tx),
		ActionIndex:   index,
		ActionType:    uint8(actionType),
		ProposedEpoch: 1,
		ExpiresEpoch:  7,
		AnchorURL:     "https://example.com/proposal.json",
		AnchorHash:    fill32(0x5a),
		Deposit:       100_000_000,
		ReturnAddress: rewardAccount(t, 0x77),
		GovActionCbor: actionCbor,
		AddedSlot:     slot,
	}
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().SetGovernanceProposal(proposal, nil))
	return proposal
}

func TestNodeAdapterGovernanceProposalsAndDetail(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	info := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 100)
	fx.addProposal(t, 0x02, 1, lcommon.GovActionTypeTreasuryWithdrawal,
		treasuryActionCbor(t, map[byte]uint64{0x31: 5000, 0x32: 7000}, 0x31, 0x32), 200)
	ratified, enacted, dropped := uint64(2), uint64(3), uint64(4)
	slot := uint64(900)
	info.RatifiedEpoch, info.RatifiedSlot = &ratified, &slot
	info.EnactedEpoch, info.EnactedSlot = &enacted, &slot
	info.DroppedEpoch, info.DroppedSlot = &dropped, &slot
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().SetGovernanceProposal(info, nil))

	rows, total, err := fx.adapter.GovernanceProposals(ctx, asc)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, rows, 2)
	assert.Equal(t, GovernanceProposalInfo{
		ID: testGovActionID(t, 0x01, 0), TxHash: hexOf(fill32(0x01)), CertIndex: 0, GovernanceType: "info_action",
	}, rows[0])
	assert.Equal(t, "treasury_withdrawals", rows[1].GovernanceType)
	assert.Equal(t, 1, rows[1].CertIndex)

	rows, total, err = fx.adapter.GovernanceProposals(ctx, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, rows, 1)
	assert.Equal(t, hexOf(fill32(0x02)), rows[0].TxHash)

	rows, _, err = fx.adapter.GovernanceProposals(ctx, PaginationParams{Count: 1, Page: 3, Order: PaginationOrderAsc})
	require.NoError(t, err)
	assert.Empty(t, rows)

	detail, err := fx.adapter.GovernanceProposal(ctx, GovActionRef{TxHash: fill32(0x01), Index: 0})
	require.NoError(t, err)
	assert.JSONEq(t, `{"tag":"InfoAction"}`, string(detail.Description))
	assert.Equal(t, "100000000", detail.Deposit)
	returnAddress, err := lcommon.NewAddressFromBytes(rewardAccount(t, 0x77))
	require.NoError(t, err)
	assert.Equal(t, returnAddress.String(), detail.ReturnAddress)
	assert.Equal(t, uint64(7), detail.Expiration)
	require.NotNil(t, detail.RatifiedEpoch)
	assert.Equal(t, uint64(2), *detail.RatifiedEpoch)
	assert.Equal(t, uint64(3), *detail.EnactedEpoch)
	assert.Equal(t, uint64(4), *detail.DroppedEpoch)
	assert.Nil(t, detail.ExpiredEpoch)

	detail, err = fx.adapter.GovernanceProposal(ctx, GovActionRef{TxHash: fill32(0x02), Index: 1})
	require.NoError(t, err)
	address, err := lcommon.NewAddressFromBytes(rewardAccount(t, 0x31))
	require.NoError(t, err)
	assert.Contains(t, string(detail.Description), `"tag":"TreasuryWithdrawals"`)
	assert.Contains(t, string(detail.Description), `"`+address.String()+`":5000`)
	assert.Nil(t, detail.RatifiedEpoch)

	_, err = fx.adapter.GovernanceProposal(ctx, GovActionRef{TxHash: fill32(0x99), Index: 0})
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

func TestNodeAdapterGovernanceProposalWithdrawals(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	fx.addProposal(t, 0x02, 1, lcommon.GovActionTypeTreasuryWithdrawal,
		treasuryActionCbor(t, map[byte]uint64{0x31: 5000, 0x32: 7000}, 0x31, 0x32), 200)
	fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 100)
	ref := GovActionRef{TxHash: fill32(0x02), Index: 1}

	first, err := lcommon.NewAddressFromBytes(rewardAccount(t, 0x31))
	require.NoError(t, err)
	second, err := lcommon.NewAddressFromBytes(rewardAccount(t, 0x32))
	require.NoError(t, err)
	want := []GovernanceWithdrawalInfo{
		{StakeAddress: first.String(), Amount: "5000"},
		{StakeAddress: second.String(), Amount: "7000"},
	}
	rows, total, err := fx.adapter.GovernanceProposalWithdrawals(ctx, ref, asc)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, want, rows)

	rows, total, err = fx.adapter.GovernanceProposalWithdrawals(ctx, ref, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, want[1:], rows)

	rows, total, err = fx.adapter.GovernanceProposalWithdrawals(ctx, GovActionRef{TxHash: fill32(0x01), Index: 0}, asc)
	require.NoError(t, err)
	assert.Equal(t, 0, total, "an info action withdraws nothing")
	assert.Empty(t, rows)

	_, _, err = fx.adapter.GovernanceProposalWithdrawals(ctx, GovActionRef{TxHash: fill32(0x99), Index: 0}, asc)
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

func TestNodeAdapterGovernanceProposalParameters(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	half := cbor.Rat{Rat: big.NewRat(1, 2)}
	pool := []cbor.Rat{half, half, half, half, {Rat: big.NewRat(3, 5)}}
	update := map[uint]any{0: uint(44), 5: uint(2_000_000), 25: pool, 27: uint(7)}
	fx.addProposal(t, 0x03, 0, lcommon.GovActionTypeParameterChange,
		mustCbor(t, []any{0, nil, update, nil}), 100)
	fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 50)

	info, err := fx.adapter.GovernanceProposalParameters(ctx, GovActionRef{TxHash: fill32(0x03), Index: 0})
	require.NoError(t, err)
	assert.Equal(t, testGovActionID(t, 0x03, 0), info.ID)
	params := info.Parameters
	require.NotNil(t, params.MinFeeA)
	assert.Equal(t, uint64(44), *params.MinFeeA)
	require.NotNil(t, params.KeyDeposit)
	assert.Equal(t, "2000000", *params.KeyDeposit)
	require.NotNil(t, params.CommitteeMinSize)
	assert.Equal(t, "7", *params.CommitteeMinSize)
	require.NotNil(t, params.PvtMotionNoConfidence)
	assert.InDelta(t, 0.5, *params.PvtMotionNoConfidence, 1e-9)
	require.NotNil(t, params.PvtPPSecurityGroup)
	assert.InDelta(t, 0.6, *params.PvtPPSecurityGroup, 1e-9)
	assert.Nil(t, params.MinFeeB, "a parameter the update leaves alone is null")
	assert.Nil(t, params.DvtTreasuryWithdrawal)
	require.NotNil(t, params.Epoch)
	assert.Equal(t, uint64(1), *params.Epoch, "the proposal's epoch")

	detail, err := fx.adapter.GovernanceProposal(ctx, GovActionRef{TxHash: fill32(0x03), Index: 0})
	require.NoError(t, err)
	assert.Contains(t, string(detail.Description), `"tag":"ParameterChange"`)
	assert.Contains(t, string(detail.Description), `"min_fee_a":44`)

	_, err = fx.adapter.GovernanceProposalParameters(ctx, GovActionRef{TxHash: fill32(0x01), Index: 0})
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound, "an info action has no parameters")
	_, err = fx.adapter.GovernanceProposalParameters(ctx, GovActionRef{TxHash: fill32(0x99), Index: 0})
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

func TestNodeAdapterGovernanceProposalMetadata(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	proposal := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 100)
	ref := GovActionRef{TxHash: fill32(0x01), Index: 0}

	_, err := fx.adapter.GovernanceProposalMetadata(ctx, ref)
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound, "no document has been fetched")

	meta := fx.adapter.ledgerState.Database().Metadata()
	_, err = meta.EnsureOffchainMetadataPointers(ctx, time.Now(), nil)
	require.NoError(t, err)
	_, err = fx.adapter.GovernanceProposalMetadata(ctx, ref)
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound, "a pending document is not available")

	content := []byte(`{"title":"x"}`)
	setOffchain(t, fx.raw, models.OffchainMetadataSourceGovernanceProposal,
		proposal.AnchorURL, models.OffchainMetadataStatusFetched, "", content)
	info, err := fx.adapter.GovernanceProposalMetadata(ctx, ref)
	require.NoError(t, err)
	assert.Equal(t, proposal.AnchorURL, info.URL)
	assert.Equal(t, hexOf(proposal.AnchorHash), info.Hash)
	assert.JSONEq(t, string(content), string(info.JSONMetadata))
	assert.Equal(t, "\\x"+hexOf(content), info.Bytes)
	assert.Equal(t, testGovActionID(t, 0x01, 0), info.ID)

	_, err = fx.adapter.GovernanceProposalMetadata(ctx, GovActionRef{TxHash: fill32(0x99), Index: 0})
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

// setOffchain records the outcome of fetching the off-chain document behind
// an on-chain pointer, directly in the cache table. The store's own update
// is an optimistic claim-and-release that only the fetcher drives.
func setOffchain(
	t *testing.T,
	raw *sql.DB,
	source string,
	url string,
	status string,
	lastError string,
	content []byte,
) {
	t.Helper()
	result, err := raw.Exec(
		`UPDATE offchain_metadata SET status = ?, last_error = ?, content = ?, body_hash = ? WHERE source_type = ? AND url = ?`,
		status, lastError, content, fill32(0x01), source, url,
	)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "the pointer row exists")
}

// votingTx is a transaction that carries only the voting data the adapter
// reads. The embedded interface is nil, so any other method panics.
type votingTx struct {
	lcommon.Transaction
	hash       lcommon.Blake2b256
	valid      bool
	procedures lcommon.VotingProcedures
}

func (v votingTx) Hash() lcommon.Blake2b256                   { return v.hash }
func (v votingTx) IsValid() bool                              { return v.valid }
func (v votingTx) VotingProcedures() lcommon.VotingProcedures { return v.procedures }

func voter(voterType uint8, key byte) *lcommon.Voter {
	return &lcommon.Voter{Type: voterType, Hash: [28]byte(bytes32of(28, key))}
}

func govID(t *testing.T, tx byte, index uint32) *lcommon.GovActionId {
	t.Helper()
	id, err := lcommon.NewGovActionId(fill32(tx), index)
	require.NoError(t, err)
	return &id
}

func procedures(votes ...any) lcommon.VotingProcedures {
	ret := lcommon.VotingProcedures{}
	for i := 0; i < len(votes); i += 3 {
		v := votes[i].(*lcommon.Voter)
		if ret[v] == nil {
			ret[v] = map[*lcommon.GovActionId]lcommon.VotingProcedure{}
		}
		ret[v][votes[i+1].(*lcommon.GovActionId)] = lcommon.VotingProcedure{Vote: votes[i+2].(uint8)}
	}
	return ret
}

func newTx(hash byte, valid bool, p lcommon.VotingProcedures) votingTx {
	return votingTx{hash: lcommon.NewBlake2b256(fill32(hash)), valid: valid, procedures: p}
}

// serveBlocks makes the adapter read slot -> block from the given map; a
// slot the map lacks has no block.
func serveBlocks(fx *epochFixture, blocks map[uint64]*slotBlock) {
	fx.adapter.slotBlocks = func(_ context.Context, slot uint64) (*slotBlock, error) {
		block, ok := blocks[slot]
		if !ok {
			return nil, ErrHistoryNotRetained
		}
		return block, nil
	}
}

func (fx *epochFixture) addVote(
	t *testing.T,
	proposal *models.GovernanceProposal,
	voterType uint8,
	tag uint8,
	key byte,
	vote uint8,
	slot uint64,
	deleted bool,
) {
	t.Helper()
	updated := slot
	row := &models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: voterType, VoterCredentialTag: tag,
		VoterCredential: bytes32of(28, key), Vote: vote,
		AddedSlot: slot, VoteUpdatedSlot: &updated,
	}
	if voterType == models.VoterTypeCC {
		row.AnchorURL, row.AnchorHash = "https://example.com/vote.json", fill32(0x6b)
	}
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().SetGovernanceVote(row, nil))
	if deleted {
		_, err := fx.raw.Exec(`UPDATE governance_vote SET deleted_slot = ? WHERE id = ?`, slot+1, row.ID)
		require.NoError(t, err)
	}
}

func TestNodeAdapterGovernanceProposalVotes(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	proposal := fx.addProposal(t, 0x01, 2, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	other := fx.addProposal(t, 0x02, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 11)
	target := govID(t, 0x01, 2)
	elsewhere := govID(t, 0x02, 0)

	drep := voter(lcommon.VoterTypeDRepKeyHash, 0xd1)
	spo := voter(lcommon.VoterTypeStakingPoolKeyHash, 0xa1)
	cc := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0xcc)

	// Slot 100: the DRep votes in tx 0xa1 and again, changing its vote, in
	// tx 0xa2; an invalid tx 0xa3 names the same vote and must not count.
	// Slot 200: one tx carries three voters' procedures; each voter's index
	// counts only its own actions.
	serveBlocks(fx, map[uint64]*slotBlock{
		100: {number: 40, txs: []lcommon.Transaction{
			newTx(0xa1, true, procedures(drep, target, uint8(0))),
			newTx(0xa2, true, procedures(drep, target, uint8(1))),
			newTx(0xa3, false, procedures(drep, target, uint8(2))),
		}},
		200: {number: 41, txs: []lcommon.Transaction{
			newTx(0xb1, true, procedures(
				spo, target, uint8(2),
				drep, elsewhere, uint8(1),
				cc, target, uint8(1),
			)),
		}},
	})
	fx.addVote(t, proposal, models.VoterTypeDRep, 0, 0xd1, models.VoteYes, 100, false)
	fx.addVote(t, other, models.VoterTypeDRep, 0, 0xd1, models.VoteYes, 200, false)
	fx.addVote(t, proposal, models.VoterTypeSPO, 0, 0xa1, models.VoteAbstain, 200, false)
	fx.addVote(t, proposal, models.VoterTypeCC, 0, 0xcc, models.VoteYes, 200, true)

	ref := GovActionRef{TxHash: fill32(0x01), Index: 2}
	rows, total, err := fx.adapter.GovernanceProposalVotes(ctx, ref, asc)
	require.NoError(t, err)
	assert.Equal(t, 3, total, "the other proposal's vote is not listed")
	drepID, spoID := drep.String(), spo.String()
	require.Len(t, rows, 3)
	assert.Equal(t, GovernanceProposalVoteInfo{
		TxHash: hexOf(fill32(0xa2)), CertIndex: 0, VoterRole: "drep",
		Voter: drepID, Vote: "yes", Counted: true,
	}, rows[0], "the later transaction of the block set the stored value")
	assert.Equal(t, GovernanceProposalVoteInfo{
		TxHash: hexOf(fill32(0xb1)), CertIndex: 0, VoterRole: "spo",
		Voter: spoID, Vote: "abstain", Counted: true,
	}, rows[1], "the pool's only action in the transaction")
	assert.Equal(t, "constitutional_committee", rows[2].VoterRole)
	assert.Equal(t, 0, rows[2].CertIndex)
	assert.False(t, rows[2].Counted, "a deleted vote is no longer counted")

	rows, _, err = fx.adapter.GovernanceProposalVotes(ctx, ref, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, hexOf(fill32(0xb1)), rows[0].TxHash)

	_, _, err = fx.adapter.GovernanceProposalVotes(ctx, GovActionRef{TxHash: fill32(0x99), Index: 0}, asc)
	assert.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

// A vote whose casting transaction cannot be located is reported, never
// answered with a made-up transaction or silently dropped.
func TestNodeAdapterVotesReportUnrecoverableTransaction(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	proposal := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	fx.addVote(t, proposal, models.VoterTypeDRep, 0, 0xd1, models.VoteYes, 100, false)
	ref := GovActionRef{TxHash: fill32(0x01), Index: 0}

	// No block is served for the slot.
	serveBlocks(fx, nil)
	_, _, err := fx.adapter.GovernanceProposalVotes(ctx, ref, asc)
	assert.ErrorIs(t, err, ErrHistoryNotRetained)

	// The block is held but no valid transaction carries the vote.
	serveBlocks(fx, map[uint64]*slotBlock{100: {number: 1, txs: []lcommon.Transaction{
		newTx(0xa1, true, procedures(voter(lcommon.VoterTypeDRepKeyHash, 0xee), govID(t, 0x01, 0), uint8(1))),
	}}})
	_, _, err = fx.adapter.GovernanceProposalVotes(ctx, ref, asc)
	assert.ErrorIs(t, err, ErrHistoryNotRetained)

	// With no override the block comes from the chain store, which here holds
	// none.
	fx.adapter.slotBlocks = nil
	_, _, err = fx.adapter.GovernanceProposalVotes(ctx, ref, asc)
	assert.ErrorIs(t, err, ErrHistoryNotRetained)
}

func TestNodeAdapterDRepVotesAndPoolVotes(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	first := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	second := fx.addProposal(t, 0x02, 3, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 11)
	poolID := fx.addPool(t, bytes32of(28, 0xa1), nil)
	drep := voter(lcommon.VoterTypeDRepKeyHash, 0xd1)
	script := voter(lcommon.VoterTypeDRepScriptHash, 0xd1)
	spo := voter(lcommon.VoterTypeStakingPoolKeyHash, 0xa1)
	serveBlocks(fx, map[uint64]*slotBlock{
		100: {number: 1, txs: []lcommon.Transaction{newTx(0xa1, true, procedures(drep, govID(t, 0x01, 0), uint8(1), spo, govID(t, 0x01, 0), uint8(0)))}},
		110: {number: 2, txs: []lcommon.Transaction{newTx(0xa2, true, procedures(drep, govID(t, 0x02, 3), uint8(0), script, govID(t, 0x02, 3), uint8(1)))}},
	})
	fx.addVote(t, first, models.VoterTypeDRep, 0, 0xd1, models.VoteYes, 100, false)
	fx.addVote(t, first, models.VoterTypeSPO, 0, 0xa1, models.VoteNo, 100, false)
	fx.addVote(t, second, models.VoterTypeDRep, 0, 0xd1, models.VoteNo, 110, false)
	fx.addVote(t, second, models.VoterTypeDRep, 1, 0xd1, models.VoteYes, 110, false)
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().CreateDrep(nil, &models.Drep{
		Credential: bytes32of(28, 0xd1), CredentialTag: 0, Active: true, AddedSlot: 5,
	}))

	credential := DRepCredential{ID: hexOf(bytes32of(28, 0xd1)), Hash: bytes32of(28, 0xd1)}
	rows, total, err := fx.adapter.DRepVotes(ctx, credential, asc)
	require.NoError(t, err)
	assert.Equal(t, 2, total, "the script DRep with the same hash is a different voter")
	assert.Equal(t, []DRepVoteInfo{
		{TxHash: hexOf(fill32(0xa1)), CertIndex: 0, ProposalID: testGovActionID(t, 0x01, 0), ProposalTxHash: hexOf(fill32(0x01)), ProposalCertIndex: 0, Vote: "yes"},
		{TxHash: hexOf(fill32(0xa2)), CertIndex: 0, ProposalID: testGovActionID(t, 0x02, 3), ProposalTxHash: hexOf(fill32(0x02)), ProposalCertIndex: 3, Vote: "no"},
	}, rows)

	_, _, err = fx.adapter.DRepVotes(ctx, DRepCredential{ID: "x", Hash: bytes32of(28, 0x99)}, asc)
	assert.ErrorIs(t, err, ErrDRepNotFound)
	predefined := uint64(models.DrepTypeAlwaysAbstain)
	rows, total, err = fx.adapter.DRepVotes(ctx, DRepCredential{ID: "drep_always_abstain", Predefined: &predefined}, asc)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)

	votes, total, err := fx.adapter.PoolVotes(ctx, poolID, asc)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []PoolVoteInfo{{TxHash: hexOf(fill32(0xa1)), CertIndex: 0, Vote: "no"}}, votes)
	_, _, err = fx.adapter.PoolVotes(ctx, "not-a-pool", asc)
	assert.ErrorIs(t, err, ErrInvalidPoolID)
}

func (fx *epochFixture) addDRepCert(
	t *testing.T,
	table string,
	credentialColumn string,
	tx byte,
	slot uint64,
	deposit any,
) {
	t.Helper()
	cert := fx.addCert(t, fill32(tx), slot, 0, int(tx))
	query := `INSERT INTO ` + table + ` (` + credentialColumn + `, credential_tag, certificate_id, added_slot`
	args := []any{bytes32of(28, 0xd1), 0, cert, slot}
	if deposit != nil {
		query += `, deposit_amount) VALUES (?, ?, ?, ?, ?)`
		args = append(args, deposit)
	} else {
		query += `) VALUES (?, ?, ?, ?)`
	}
	_, err := fx.raw.Exec(query, args...)
	require.NoError(t, err)
}

func TestNodeAdapterDRepUpdatesDelegatorsAndMetadata(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	meta := fx.adapter.ledgerState.Database().Metadata()
	require.NoError(t, meta.CreateDrep(nil, &models.Drep{
		Credential: bytes32of(28, 0xd1), CredentialTag: 0, Active: true, AddedSlot: 5,
		AnchorURL: "https://example.com/drep.json", AnchorHash: fill32(0x7d),
	}))
	credential := DRepCredential{ID: "drep1test", Hash: bytes32of(28, 0xd1)}

	fx.addDRepCert(t, "registration_drep", "drep_credential", 0xe1, 100, "500000000")
	fx.addDRepCert(t, "update_drep", "credential", 0xe2, 200, nil)
	fx.addDRepCert(t, "deregistration_drep", "drep_credential", 0xe3, 300, "500000000")
	deposit := "500000000"
	want := []DRepUpdateInfo{
		{TxHash: hexOf(fill32(0xe1)), CertIndex: 0xe1, Action: "registered", Deposit: &deposit},
		{TxHash: hexOf(fill32(0xe2)), CertIndex: 0xe2, Action: "updated"},
		{TxHash: hexOf(fill32(0xe3)), CertIndex: 0xe3, Action: "deregistered", Deposit: &deposit},
	}
	rows, total, err := fx.adapter.DRepUpdates(ctx, credential, asc)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, want, rows)
	rows, _, err = fx.adapter.DRepUpdates(ctx, credential, PaginationParams{Count: 2, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, []DRepUpdateInfo{want[2], want[1]}, rows)
	_, _, err = fx.adapter.DRepUpdates(ctx, DRepCredential{ID: "x", Hash: bytes32of(28, 0x99)}, asc)
	assert.ErrorIs(t, err, ErrDRepNotFound)

	// Delegators: two accounts delegate to the DRep, one has since left, and a
	// third delegates to nobody.
	for _, delegator := range []struct {
		key     byte
		removed any
		stake   string
	}{{0x01, nil, "700"}, {0x02, nil, "200"}, {0x03, 150, "999"}} {
		_, err := fx.raw.Exec(
			`INSERT INTO drep_delegator (drep_credential_tag, drep_credential, stake_credential_tag, stake_credential, added_slot, removed_slot) VALUES (0, ?, 0, ?, ?, ?)`,
			bytes32of(28, 0xd1), bytes32of(28, delegator.key), 100+int(delegator.key), delegator.removed,
		)
		require.NoError(t, err)
		fx.addAccount(t, delegator.key)
		_, err = fx.raw.Exec(
			`UPDATE reward_live_stake SET total_stake = ?, registered = TRUE WHERE staking_key = ?`,
			delegator.stake, bytes32of(28, delegator.key),
		)
		require.NoError(t, err)
	}
	first, err := fx.adapter.stakeAddressFromTag(0, bytes32of(28, 0x01))
	require.NoError(t, err)
	second, err := fx.adapter.stakeAddressFromTag(0, bytes32of(28, 0x02))
	require.NoError(t, err)
	delegators, total, err := fx.adapter.DRepDelegators(ctx, credential, asc)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	assert.Equal(t, []DRepDelegatorInfo{{Address: first, Amount: "700"}, {Address: second, Amount: "200"}}, delegators)
	delegators, _, err = fx.adapter.DRepDelegators(ctx, credential, PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc})
	require.NoError(t, err)
	assert.Equal(t, []DRepDelegatorInfo{{Address: second, Amount: "200"}}, delegators)

	// An always-abstain delegation has no DRep row; its delegators come from
	// the accounts.
	_, err = fx.raw.Exec(`UPDATE account SET drep_type = 2, drep = NULL WHERE staking_key = ?`, bytes32of(28, 0x02))
	require.NoError(t, err)
	predefined := uint64(models.DrepTypeAlwaysAbstain)
	delegators, total, err = fx.adapter.DRepDelegators(ctx, DRepCredential{ID: "drep_always_abstain", Predefined: &predefined}, asc)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []DRepDelegatorInfo{{Address: second, Amount: "200"}}, delegators)

	// Metadata: no document yet, then a failed fetch, then a fetched one.
	info, err := fx.adapter.DRepMetadata(ctx, credential)
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/drep.json", info.URL)
	assert.Equal(t, hexOf(fill32(0x7d)), info.Hash)
	assert.Nil(t, info.Bytes)
	assert.Nil(t, info.Error)
	_, err = meta.EnsureOffchainMetadataPointers(ctx, time.Now(), nil)
	require.NoError(t, err)
	setOffchain(t, fx.raw, models.OffchainMetadataSourceDrep, "https://example.com/drep.json",
		models.OffchainMetadataStatusFailed, models.OffchainFetchErrHashMismatch, nil)
	info, err = fx.adapter.DRepMetadata(ctx, credential)
	require.NoError(t, err)
	require.NotNil(t, info.Error)
	assert.Equal(t, "HASH_MISMATCH", info.Error.Code)
	assert.Nil(t, info.Bytes)
	setOffchain(t, fx.raw, models.OffchainMetadataSourceDrep, "https://example.com/drep.json",
		models.OffchainMetadataStatusFetched, "", []byte(`{"a":1}`))
	info, err = fx.adapter.DRepMetadata(ctx, credential)
	require.NoError(t, err)
	assert.Nil(t, info.Error)
	require.NotNil(t, info.Bytes)
	assert.Equal(t, "\\x"+hexOf([]byte(`{"a":1}`)), *info.Bytes)
	assert.JSONEq(t, `{"a":1}`, string(info.JSONMetadata))

	// A DRep that never carried an anchor has no metadata.
	require.NoError(t, meta.CreateDrep(nil, &models.Drep{Credential: bytes32of(28, 0xd2), Active: true, AddedSlot: 6}))
	_, err = fx.adapter.DRepMetadata(ctx, DRepCredential{ID: "x", Hash: bytes32of(28, 0xd2)})
	assert.ErrorIs(t, err, ErrDRepNotFound)
}

func (fx *epochFixture) authorizeHot(t *testing.T, coldTag uint8, cold byte, hotTag uint8, hot byte, slot uint64) {
	t.Helper()
	_, err := fx.raw.Exec(
		`INSERT INTO auth_committee_hot (cold_credential_tag, cold_credential, hot_credential_tag, host_credential, certificate_id, added_slot) VALUES (?, ?, ?, ?, 0, ?)`,
		coldTag, bytes32of(28, cold), hotTag, bytes32of(28, hot), slot,
	)
	require.NoError(t, err)
}

func TestNodeAdapterGovernanceCommittee(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	meta := fx.adapter.ledgerState.Database().Metadata()
	require.NoError(t, meta.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredentialTag: 0, ColdCredHash: bytes32of(28, 0x0a), ExpiresEpoch: 500, AddedSlot: 10},
		{ColdCredentialTag: 1, ColdCredHash: bytes32of(28, 0x0b), ExpiresEpoch: 600, AddedSlot: 10},
		{ColdCredentialTag: 0, ColdCredHash: bytes32of(28, 0x0c), ExpiresEpoch: 700, AddedSlot: 10},
	}, nil))
	fx.authorizeHot(t, 0, 0x0a, 0, 0x1a, 20)
	fx.authorizeHot(t, 0, 0x0c, 1, 0x1c, 20)
	_, err := fx.raw.Exec(
		`INSERT INTO resign_committee_cold (cold_credential_tag, cold_credential, certificate_id, added_slot) VALUES (0, ?, 0, 30)`,
		bytes32of(28, 0x0c),
	)
	require.NoError(t, err)
	quorum := &types.Rat{Rat: big.NewRat(3, 5)}
	require.NoError(t, meta.SetCommitteeQuorum(quorum, 10, nil))
	proposal := fx.addProposal(t, 0x04, 1, lcommon.GovActionTypeUpdateCommittee, mustCbor(t, []any{4, nil, []any{}, map[int]int{}, cbor.Rat{Rat: big.NewRat(3, 5)}}), 10)
	epoch, slot := uint64(2), uint64(50)
	proposal.EnactedEpoch, proposal.EnactedSlot = &epoch, &slot
	proposal.RatifiedEpoch, proposal.RatifiedSlot = &epoch, &slot
	require.NoError(t, meta.SetGovernanceProposal(proposal, nil))

	info, err := fx.adapter.GovernanceCommittee(ctx)
	require.NoError(t, err)
	assert.False(t, info.IsDissolved)
	assert.Equal(t, uint64(3), info.QuorumNum)
	assert.Equal(t, uint64(5), info.QuorumDenom)
	require.NotNil(t, info.GovActionID)
	assert.Equal(t, testGovActionID(t, 0x04, 1), *info.GovActionID)
	assert.Equal(t, hexOf(fill32(0x04)), *info.ProposalTxHash)
	assert.Equal(t, 1, *info.ProposalIndex)
	require.Len(t, info.Members, 3)

	byCold := map[string]CommitteeMemberInfo{}
	for _, member := range info.Members {
		byCold[member.ColdHex] = member
	}
	authorized := byCold["12"+hexOf(bytes32of(28, 0x0a))]
	assert.Equal(t, "authorized", authorized.Status)
	assert.Equal(t, uint64(500), authorized.ExpirationEpoch)
	assert.False(t, authorized.ColdHasScript)
	require.NotNil(t, authorized.HotID)
	assert.Equal(t, committeeCredentialID("cc_hot", false, bytes32of(28, 0x1a), false), *authorized.HotID)
	assert.Equal(t, "02"+hexOf(bytes32of(28, 0x1a)), *authorized.HotHex)
	assert.False(t, *authorized.HotHasScript)
	assert.Equal(t, committeeCredentialID("cc_cold", true, bytes32of(28, 0x0a), false), authorized.ColdID)

	unauthorized := byCold["13"+hexOf(bytes32of(28, 0x0b))]
	assert.Equal(t, "not_authorized", unauthorized.Status)
	assert.True(t, unauthorized.ColdHasScript)
	assert.Nil(t, unauthorized.HotID)
	assert.Nil(t, unauthorized.HotHasScript)

	resigned := byCold["12"+hexOf(bytes32of(28, 0x0c))]
	assert.Equal(t, "resigned", resigned.Status)
	assert.Nil(t, resigned.HotID, "a resigned member reports no hot key")

	// NoConfidence soft-deletes every member: the committee is dissolved.
	_, err = fx.raw.Exec(`UPDATE committee_member SET deleted_slot = 60`)
	require.NoError(t, err)
	info, err = fx.adapter.GovernanceCommittee(ctx)
	require.NoError(t, err)
	assert.True(t, info.IsDissolved)
	assert.Empty(t, info.Members)
}

func TestNodeAdapterGovernanceCommitteeBeforeAnyChange(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	info, err := fx.adapter.GovernanceCommittee(context.Background())
	require.NoError(t, err)
	assert.False(t, info.IsDissolved, "a committee that was never seated is not dissolved")
	assert.Nil(t, info.GovActionID, "no action has changed the genesis committee")
	assert.Nil(t, info.ProposalTxHash)
	assert.Nil(t, info.ProposalIndex)
	assert.Empty(t, info.Members)
	assert.Positive(t, info.QuorumDenom, "the quorum falls back to a valid threshold")
}

func TestNodeAdapterCommitteeVotes(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	proposal := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	second := fx.addProposal(t, 0x02, 1, lcommon.GovActionTypeNoConfidence, mustCbor(t, []any{3, nil}), 11)
	// Cold member 0x0a rotated its hot key from 0x1a to 0x1b; 0x1c belongs to
	// another member.
	fx.authorizeHot(t, 0, 0x0a, 0, 0x1a, 20)
	fx.authorizeHot(t, 0, 0x0a, 0, 0x1b, 40)
	fx.authorizeHot(t, 0, 0x0c, 0, 0x1c, 20)
	hotA := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0x1a)
	hotB := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0x1b)
	hotC := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0x1c)
	serveBlocks(fx, map[uint64]*slotBlock{
		100: {number: 7, txs: []lcommon.Transaction{newTx(0xa1, true, procedures(hotA, govID(t, 0x01, 0), uint8(1)))}},
		200: {number: 8, txs: []lcommon.Transaction{newTx(0xa2, true, procedures(hotB, govID(t, 0x02, 1), uint8(0)))}},
		300: {number: 9, txs: []lcommon.Transaction{newTx(0xa3, true, procedures(hotC, govID(t, 0x01, 0), uint8(2)))}},
	})
	fx.addVote(t, proposal, models.VoterTypeCC, 0, 0x1a, models.VoteYes, 100, false)
	fx.addVote(t, second, models.VoterTypeCC, 0, 0x1b, models.VoteNo, 200, false)
	fx.addVote(t, proposal, models.VoterTypeCC, 0, 0x1c, models.VoteAbstain, 300, false)

	all, total, err := fx.adapter.CommitteeVotes(ctx, nil, false, asc)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, all, 3)
	url := "https://example.com/vote.json"
	anchorHash := hexOf(fill32(0x6b))
	slotTime, err := fx.adapter.ledgerState.SlotToTime(100)
	require.NoError(t, err)
	assert.Equal(t, CommitteeVoteInfo{
		TxHash:         hexOf(fill32(0xa1)),
		VoterHotID:     committeeCredentialID("cc_hot", false, bytes32of(28, 0x1a), false),
		ProposalID:     testGovActionID(t, 0x01, 0),
		ProposalTxHash: hexOf(fill32(0x01)),
		ProposalIndex:  0,
		GovernanceType: "info_action",
		Vote:           "yes",
		MetadataURL:    &url,
		MetadataHash:   &anchorHash,
		BlockHeight:    7,
		BlockTime:      slotTime.Unix(),
	}, all[0])
	assert.Equal(t, "no_confidence", all[1].GovernanceType)
	assert.Equal(t, "abstain", all[2].Vote)

	// A hot credential selects only its own votes.
	hot := &CommitteeCredentialRef{Hash: bytes32of(28, 0x1a)}
	rows, total, err := fx.adapter.CommitteeVotes(ctx, hot, false, asc)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, rows, 1)
	assert.Equal(t, hexOf(fill32(0xa1)), rows[0].TxHash)

	// A cold credential aggregates its hot keys across the rotation.
	cold := &CommitteeCredentialRef{Hash: bytes32of(28, 0x0a)}
	rows, total, err = fx.adapter.CommitteeVotes(ctx, cold, true, asc)
	require.NoError(t, err)
	assert.Equal(t, 2, total, "votes of both hot keys, not the other member's")
	require.Len(t, rows, 2)
	assert.Equal(t, hexOf(fill32(0xa1)), rows[0].TxHash)
	assert.Equal(t, hexOf(fill32(0xa2)), rows[1].TxHash)

	// A cold credential that never authorized a hot key has cast nothing.
	rows, total, err = fx.adapter.CommitteeVotes(ctx, &CommitteeCredentialRef{Hash: bytes32of(28, 0x99)}, true, asc)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)
}

// Blockfrost's vote cert_index is db-sync's voting_procedure.index: the
// position of the governance action among one voter's procedures in the
// transaction, counted from 0 for each voter (db-sync numbers each voter's
// Map.toList separately), not a position across every voter of the
// transaction.
func TestNodeAdapterVoteCertIndexCountsPerVoter(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	first := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	second := fx.addProposal(t, 0x02, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 11)
	poolID := fx.addPool(t, bytes32of(28, 0xa1), nil)
	cc := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0xcc)
	drep := voter(lcommon.VoterTypeDRepKeyHash, 0xd1)
	spo := voter(lcommon.VoterTypeStakingPoolKeyHash, 0xa1)
	serveBlocks(fx, map[uint64]*slotBlock{
		100: {number: 1, txs: []lcommon.Transaction{newTx(0xb1, true, procedures(
			spo, govID(t, 0x01, 0), uint8(0),
			drep, govID(t, 0x02, 0), uint8(1),
			cc, govID(t, 0x02, 0), uint8(1),
			drep, govID(t, 0x01, 0), uint8(0),
			cc, govID(t, 0x01, 0), uint8(0),
		))}},
	})
	fx.addVote(t, second, models.VoterTypeDRep, 0, 0xd1, models.VoteNo, 100, false)
	fx.addVote(t, first, models.VoterTypeSPO, 0, 0xa1, models.VoteNo, 100, false)

	rows, _, err := fx.adapter.GovernanceProposalVotes(ctx, GovActionRef{TxHash: fill32(0x02), Index: 0}, asc)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 1, rows[0].CertIndex,
		"the DRep's second action; the committee member's votes do not count")

	votes, _, err := fx.adapter.PoolVotes(ctx, poolID, asc)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	assert.Equal(t, 0, votes[0].CertIndex, "the pool's only action")
}

// A committee vote reports metadata only when it carries an anchor, whatever
// the hot credential's kind.
func TestNodeAdapterCommitteeVotesReportOnlyRecordedAnchors(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	proposal := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	script := voter(lcommon.VoterTypeConstitutionalCommitteeHotScriptHash, 0x1d)
	key := voter(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, 0x1e)
	serveBlocks(fx, map[uint64]*slotBlock{
		100: {number: 1, txs: []lcommon.Transaction{newTx(0xa1, true, procedures(script, govID(t, 0x01, 0), uint8(1)))}},
		200: {number: 2, txs: []lcommon.Transaction{newTx(0xa2, true, procedures(key, govID(t, 0x01, 0), uint8(0)))}},
	})
	fx.addVote(t, proposal, models.VoterTypeCC, 1, 0x1d, models.VoteYes, 100, false)
	slot := uint64(200)
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeCC, VoterCredentialTag: 0,
		VoterCredential: bytes32of(28, 0x1e), Vote: models.VoteNo,
		AddedSlot: slot, VoteUpdatedSlot: &slot,
	}, nil))

	rows, total, err := fx.adapter.CommitteeVotes(ctx, nil, false, asc)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, rows, 2)
	require.NotNil(t, rows[0].MetadataURL, "a script hot credential's anchored vote")
	assert.Equal(t, "https://example.com/vote.json", *rows[0].MetadataURL)
	assert.Nil(t, rows[1].MetadataURL, "a vote without an anchor")
	assert.Nil(t, rows[1].MetadataHash)
}

// An update that sets cost models to an empty map still sets them; only an
// update that leaves them alone reports null.
func TestProposalParametersKeepEmptyCostModels(t *testing.T) {
	t.Parallel()
	ret, err := proposalParameters(mustCbor(t, map[uint]any{18: map[uint][]int64{}}), 1)
	require.NoError(t, err)
	encoded, err := json.Marshal(ret)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"cost_models":{}`)

	ret, err = proposalParameters(mustCbor(t, map[uint]any{0: uint(44)}), 1)
	require.NoError(t, err)
	encoded, err = json.Marshal(ret)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"cost_models":null`)
}

// A member that re-authorizes on a schedule accumulates hot credentials
// without bound in API storage mode, so the cold-credential vote filter must
// not grow with them.
func TestNodeAdapterCommitteeVotesColdWithManyHotCredentials(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	proposal := fx.addProposal(t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 10)
	const rotations = 1500
	stmt, err := fx.raw.Prepare(
		`INSERT INTO auth_committee_hot (cold_credential_tag, cold_credential, hot_credential_tag, host_credential, certificate_id, added_slot) VALUES (0, ?, 0, ?, 0, ?)`,
	)
	require.NoError(t, err)
	defer stmt.Close()
	cold := bytes32of(28, 0x0a)
	for i := range rotations {
		hot := bytes32of(28, 0x1a)
		hot[0], hot[1] = byte(i), byte(i>>8)
		_, err := stmt.Exec(cold, hot, 20+i)
		require.NoError(t, err)
	}
	last := bytes32of(28, 0x1a)
	lastIndex := rotations - 1
	last[0], last[1] = byte(lastIndex), byte(lastIndex>>8)
	hotVoter := &lcommon.Voter{Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, Hash: [28]byte(last)}
	serveBlocks(fx, map[uint64]*slotBlock{
		5000: {number: 1, txs: []lcommon.Transaction{newTx(0xa1, true, procedures(hotVoter, govID(t, 0x01, 0), uint8(1)))}},
	})
	slot := uint64(5000)
	require.NoError(t, fx.adapter.ledgerState.Database().Metadata().SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeCC,
		VoterCredential: last, Vote: models.VoteYes,
		AddedSlot: slot, VoteUpdatedSlot: &slot,
	}, nil))

	rows, total, err := fx.adapter.CommitteeVotes(
		context.Background(), &CommitteeCredentialRef{Hash: cold}, true, asc,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, rows, 1)
	assert.Equal(t, hexOf(fill32(0xa1)), rows[0].TxHash)
}
