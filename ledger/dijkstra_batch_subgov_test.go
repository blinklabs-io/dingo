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
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dijkstrabatchfixture"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newSubGovLedger(t *testing.T) (*LedgerState, *database.Database) {
	t.Helper()
	db := newTestDB(t)
	ls := &LedgerState{
		db: db,
		currentPParams: &dijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: conway.ConwayProtocolParameters{
				GovActionValidityPeriod: 20,
				DRepInactivityPeriod:    20,
			},
		},
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Now(), time.Second, 100),
			DefaultSlotClockConfig(),
		),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	publishGovernanceTestEpoch(ls, 12, eras.DijkstraEraDesc)
	return ls, db
}

func subGovProposal(
	t *testing.T,
	marker byte,
	action common.GovAction,
	actionType common.GovActionType,
) dijkstra.DijkstraProposalProcedure {
	t.Helper()
	rewardAddress, err := common.NewAddressFromBytes(
		append([]byte{0xe0}, bytes.Repeat([]byte{0x42}, 28)...),
	)
	require.NoError(t, err)
	return dijkstra.DijkstraProposalProcedure{
		PPDeposit:       42,
		PPRewardAccount: rewardAddress,
		PPGovAction: dijkstra.DijkstraGovAction{
			Type:   uint(actionType),
			Action: action,
		},
		PPAnchor: common.GovAnchor{
			Url:      "https://example.com/subgov",
			DataHash: [32]byte(bytes.Repeat([]byte{marker}, 32)),
		},
	}
}

func subGovInfoProposal(
	t *testing.T,
	marker byte,
) dijkstra.DijkstraProposalProcedure {
	t.Helper()
	return subGovProposal(
		t,
		marker,
		&common.InfoGovAction{Type: uint(common.GovActionTypeInfo)},
		common.GovActionTypeInfo,
	)
}

// subGovBodyID returns the id a sub-transaction body has once decoded, which
// is the transaction id its governance actions are keyed by.
func subGovBodyID(
	t *testing.T,
	body dijkstra.DijkstraSubTransactionBody,
) common.Blake2b256 {
	t.Helper()
	tx := subGovTx(t, true, nil, body)
	return tx.Body.TxSubTransactions.Items()[0].Body.Id()
}

func subGovVote(
	voter common.Voter,
	action common.Blake2b256,
	idx uint32,
) common.VotingProcedures {
	return common.VotingProcedures{
		&voter: {
			&common.GovActionId{
				TransactionId: action,
				GovActionIdx:  idx,
			}: common.VotingProcedure{Vote: common.GovVoteYes},
		},
	}
}

// subGovTx builds and decodes a Dijkstra batch. A nil top-level body yields an
// empty enclosing body.
func subGovTx(
	t *testing.T,
	valid bool,
	top *dijkstra.DijkstraTransactionBody,
	children ...dijkstra.DijkstraSubTransactionBody,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	subs := make([]dijkstra.DijkstraSubTransaction, len(children))
	for i := range children {
		subs[i] = dijkstra.DijkstraSubTransaction{Body: children[i]}
	}
	tx := &dijkstra.DijkstraTransaction{TxIsValid: valid}
	if top != nil {
		tx.Body = *top
	}
	tx.Body.TxSubTransactions = cbor.NewSetType(subs, true)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		txCbor,
	)
	require.NoError(t, err)
	got := decoded.(*dijkstra.DijkstraTransaction)
	if valid {
		return got
	}
	// A standalone Dijkstra transaction cannot encode is_valid=false; only a
	// block_transaction carries the flag, so decode the invalid batch from a
	// block body.
	got.TxIsValid = false
	got.SetCbor(nil)
	blockBodyCbor, err := dijkstra.DijkstraBlockBody{
		Transactions: []dijkstra.DijkstraTransaction{*got},
	}.MarshalCBOR()
	require.NoError(t, err)
	var blockBody dijkstra.DijkstraBlockBody
	require.NoError(t, blockBody.UnmarshalCBOR(blockBodyCbor))
	require.Len(t, blockBody.Transactions, 1)
	require.False(t, blockBody.Transactions[0].IsValid())
	return &blockBody.Transactions[0]
}

func applySubGovBlock(
	t *testing.T,
	ls *LedgerState,
	slot uint64,
	tx *dijkstra.DijkstraTransaction,
) error {
	t.Helper()
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  slot,
					Slot:         slot,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	blockHash := bytes.Repeat([]byte{byte(slot)}, 32)
	offsets, err := database.NewBlockIndexer(slot, blockHash).ComputeOffsets(
		blockCbor,
		block,
	)
	require.NoError(t, err)
	delta := NewLedgerDelta(
		ocommon.Point{Slot: slot, Hash: blockHash},
		uint(dijkstra.EraIdDijkstra),
		1,
	)
	delta.Offsets = offsets
	delta.addTransaction(tx, 0)
	t.Cleanup(delta.Release)
	return ls.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		return delta.apply(context.Background(), ls, txn)
	})
}

func requireSubGovProposal(
	t *testing.T,
	db *database.Database,
	id common.Blake2b256,
) *models.GovernanceProposal {
	t.Helper()
	got, err := db.GetGovernanceProposal(context.Background(), id.Bytes(), 0, nil)
	require.NoError(t, err)
	require.Equal(t, id.Bytes(), got.TxHash)
	return got
}

func requireNoSubGovProposal(
	t *testing.T,
	db *database.Database,
	id common.Blake2b256,
) {
	t.Helper()
	_, err := db.GetGovernanceProposal(context.Background(), id.Bytes(), 0, nil)
	require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
}

func TestDijkstraBatchChildVoteOnExistingProposal(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	// A plain transaction's proposal, keyed by that transaction's id.
	existing := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
				subGovInfoProposal(t, 0x01),
			},
		},
		TxIsValid: true,
	}
	existingCbor, err := existing.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		existingCbor,
	)
	require.NoError(t, err)
	existing = decoded.(*dijkstra.DijkstraTransaction)
	require.NoError(t, applySubGovBlock(t, ls, 1, existing))
	proposalID := existing.Hash()
	proposal := requireSubGovProposal(t, db, proposalID)

	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x51}, 28)),
	}
	tx := subGovTx(t, true, nil, dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: subGovVote(drep, proposalID, 0),
	})
	require.NoError(t, applySubGovBlock(t, ls, 2, tx))

	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, drep.Hash[:], votes[0].VoterCredential)
	require.Equal(t, uint8(common.GovVoteYes), votes[0].Vote)
}

func TestDijkstraBatchLaterChildVotesOnEarlierChildProposal(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	child1 := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovInfoProposal(t, 0x02),
		},
	}
	child1ID := subGovBodyID(t, child1)
	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x52}, 28)),
	}
	child2 := dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: subGovVote(drep, child1ID, 0),
	}
	tx := subGovTx(t, true, nil, child1, child2)
	require.NotEqual(t, child1ID, tx.Hash())
	require.NoError(t, applySubGovBlock(t, ls, 1, tx))

	proposal := requireSubGovProposal(t, db, child1ID)
	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, drep.Hash[:], votes[0].VoterCredential)
}

// A child that votes on a proposal made by a later child must fail: children
// are applied in encoded order, so the action does not exist yet.
func TestDijkstraBatchChildVoteBeforeProposalIsRejected(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	proposing := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovInfoProposal(t, 0x03),
		},
	}
	proposalID := subGovBodyID(t, proposing)
	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x53}, 28)),
	}
	voting := dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: subGovVote(drep, proposalID, 0),
	}
	tx := subGovTx(t, true, nil, voting, proposing)
	require.Error(t, applySubGovBlock(t, ls, 1, tx))
	requireNoSubGovProposal(t, db, proposalID)
}

func TestDijkstraBatchTopLevelVoteOnChildProposal(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	child := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovInfoProposal(t, 0x04),
		},
	}
	childID := subGovBodyID(t, child)
	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x54}, 28)),
	}
	top := dijkstra.DijkstraTransactionBody{
		TxVotingProcedures: subGovVote(drep, childID, 0),
	}
	tx := subGovTx(t, true, &top, child)
	require.NoError(t, applySubGovBlock(t, ls, 1, tx))

	proposal := requireSubGovProposal(t, db, childID)
	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
}

func TestDijkstraBatchChildProposalParentChain(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	child1 := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovProposal(
				t, 0x05,
				&common.NoConfidenceGovAction{
					Type: uint(common.GovActionTypeNoConfidence),
				},
				common.GovActionTypeNoConfidence,
			),
		},
	}
	child1ID := subGovBodyID(t, child1)
	child2 := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovProposal(
				t, 0x06,
				&common.NoConfidenceGovAction{
					Type: uint(common.GovActionTypeNoConfidence),
					ActionId: &common.GovActionId{
						TransactionId: child1ID,
					},
				},
				common.GovActionTypeNoConfidence,
			),
		},
	}
	child2ID := subGovBodyID(t, child2)
	tx := subGovTx(t, true, nil, child1, child2)
	require.NoError(t, applySubGovBlock(t, ls, 1, tx))

	requireSubGovProposal(t, db, child1ID)
	got := requireSubGovProposal(t, db, child2ID)
	require.Equal(t, child1ID.Bytes(), got.ParentTxHash)
	require.NotNil(t, got.ParentActionIdx)
	require.Equal(t, uint32(0), *got.ParentActionIdx)
}

func TestDijkstraBatchSharedGovernanceFixture(t *testing.T) {
	t.Parallel()
	const slot = uint64(1000)
	fixture, err := dijkstrabatchfixture.New()
	require.NoError(t, err)
	liveLedger, liveDB := newSubGovLedger(t)
	require.NoError(t, applySubGovBlock(t, liveLedger, slot, fixture.Transaction))
	liveState := requireSharedSubGovState(t, liveDB, fixture)

	replayLedger, replayDB := newSubGovLedger(t)
	require.NoError(t, replaySubGovBlock(t, replayLedger, slot, fixture.Transaction))
	replayState := requireSharedSubGovState(t, replayDB, fixture)
	require.Equal(t, liveState, replayState)
}

type sharedSubGovState struct {
	Root      models.GovernanceProposal
	Child     models.GovernanceProposal
	RootVotes []*models.GovernanceVote
}

func requireSharedSubGovState(
	t *testing.T,
	db *database.Database,
	fixture dijkstrabatchfixture.Fixture,
) sharedSubGovState {
	t.Helper()
	root := requireSubGovProposal(t, db, fixture.ProposalIDs[0])
	child := requireSubGovProposal(t, db, fixture.ProposalIDs[1])
	subTransactions := fixture.Transaction.Body.TxSubTransactions.Items()
	require.Len(t, subTransactions, 2)
	for index, proposal := range []*models.GovernanceProposal{root, child} {
		procedure := subTransactions[index].Body.TxProposalProcedures[0]
		actionCbor, err := cbor.Encode(procedure.PPGovAction.Action)
		require.NoError(t, err)
		returnAddress, err := procedure.PPRewardAccount.Bytes()
		require.NoError(t, err)
		require.Zero(t, proposal.ActionIndex)
		require.Equal(t, uint8(procedure.PPGovAction.Type), proposal.ActionType)
		require.Equal(t, uint64(12), proposal.ProposedEpoch)
		require.Equal(t, uint64(32), proposal.ExpiresEpoch)
		require.Equal(t, procedure.PPAnchor.Url, proposal.AnchorURL)
		require.Equal(t, procedure.PPAnchor.DataHash[:], proposal.AnchorHash)
		require.Equal(t, procedure.PPDeposit, proposal.Deposit)
		require.Equal(t, returnAddress, proposal.ReturnAddress)
		require.Equal(t, actionCbor, proposal.GovActionCbor)
		require.Empty(t, proposal.PolicyHash)
		require.Equal(t, uint64(1000), proposal.AddedSlot)
		require.Nil(t, proposal.EnactedEpoch)
		require.Nil(t, proposal.EnactedSlot)
		require.Nil(t, proposal.RatifiedEpoch)
		require.Nil(t, proposal.RatifiedSlot)
		require.Nil(t, proposal.ExpiredEpoch)
		require.Nil(t, proposal.ExpiredSlot)
		require.Nil(t, proposal.DroppedEpoch)
		require.Nil(t, proposal.DroppedSlot)
		require.Nil(t, proposal.DeletedSlot)
	}
	require.Empty(t, root.ParentTxHash)
	require.Equal(t, fixture.RootID.Bytes(), child.ParentTxHash)
	require.NotNil(t, child.ParentActionIdx)
	require.Zero(t, *child.ParentActionIdx)
	votes, err := db.GetGovernanceVotes(context.Background(), root.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 2)
	voters := make(map[byte]bool, len(votes))
	var credentials [][]byte
	for _, vote := range votes {
		require.Equal(t, root.ID, vote.ProposalID)
		require.Equal(t, uint8(models.VoterTypeDRep), vote.VoterType)
		require.Zero(t, vote.VoterCredentialTag)
		require.Equal(t, uint8(models.VoteYes), vote.Vote)
		require.Nil(t, vote.DeletedSlot)
		require.Equal(t, uint64(1000), vote.AddedSlot)
		require.NotNil(t, vote.VoteUpdatedSlot)
		require.Equal(t, uint64(1000), *vote.VoteUpdatedSlot)
		require.Empty(t, vote.AnchorURL)
		require.Empty(t, vote.AnchorHash)
		voters[vote.VoterCredential[0]] = true
		require.Equal(t, bytes.Repeat([]byte{vote.VoterCredential[0]}, 28), vote.VoterCredential)
		credentials = append(credentials, vote.VoterCredential)
	}
	require.Equal(t, map[byte]bool{0x41: true, 0x42: true}, voters)
	require.ElementsMatch(t, [][]byte{
		bytes.Repeat([]byte{0x41}, 28),
		bytes.Repeat([]byte{0x42}, 28),
	}, credentials)
	return sharedSubGovState{Root: *root, Child: *child, RootVotes: votes}
}

func replaySubGovBlock(
	t *testing.T,
	ls *LedgerState,
	slot uint64,
	tx *dijkstra.DijkstraTransaction,
) error {
	t.Helper()
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  slot,
					Slot:         slot,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	blockHash := bytes.Repeat([]byte{byte(slot)}, 32)
	offsets, err := database.NewBlockIndexer(slot, blockHash).ComputeOffsets(
		blockCbor,
		block,
	)
	require.NoError(t, err)
	return ls.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		delta, err := ls.ledgerProcessBlock(context.Background(),
			txn,
			ocommon.Point{Slot: slot, Hash: blockHash},
			block,
			false,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.DijkstraEraDesc,
			ls.currentPParams,
			nil,
			0,
			0,
			false,
		)
		if err != nil {
			return err
		}
		if delta == nil {
			return nil
		}
		defer delta.Release()
		return delta.apply(context.Background(), ls, txn)
	})
}

// A child DRep registration and a sibling child's vote both apply. The stored
// row does not show their relative order: the registration overwrites the row
// the vote-repair path inserts, so either order yields the same DRep.
func TestDijkstraBatchChildDRepRegistrationAndChildVote(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x57}, 28)),
	}
	proposing := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovInfoProposal(t, 0x07),
		},
	}
	proposalID := subGovBodyID(t, proposing)
	anchor := &common.GovAnchor{
		Url:      "https://example.com/drep",
		DataHash: [32]byte(bytes.Repeat([]byte{0x58}, 32)),
	}
	registering := dijkstra.DijkstraSubTransactionBody{
		TxCertificates: []common.CertificateWrapper{{
			Type: uint(common.CertificateTypeRegistrationDrep),
			Certificate: &common.RegistrationDrepCertificate{
				CertType: uint(common.CertificateTypeRegistrationDrep),
				DrepCredential: common.Credential{
					CredType:   common.CredentialTypeAddrKeyHash,
					Credential: common.CredentialHash(drep.Hash),
				},
				Amount: 500_000_000,
				Anchor: anchor,
			},
		}},
	}
	voting := dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: subGovVote(drep, proposalID, 0),
	}
	tx := subGovTx(t, true, nil, proposing, registering, voting)
	require.NoError(t, applySubGovBlock(t, ls, 1, tx))

	got, err := db.GetDrepByCredential(context.Background(), 0, drep.Hash[:], true, nil)
	require.NoError(t, err)
	require.Equal(t, anchor.Url, got.AnchorURL)
	proposal := requireSubGovProposal(t, db, proposalID)
	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
}

func TestDijkstraBatchChildGovernanceRollbackAndReapply(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	proposing := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovInfoProposal(t, 0x08),
		},
	}
	proposalID := subGovBodyID(t, proposing)
	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x59}, 28)),
	}
	voting := dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: subGovVote(drep, proposalID, 0),
	}
	tx := subGovTx(t, true, nil, proposing, voting)
	require.NoError(t, applySubGovBlock(t, ls, 5, tx))
	before := requireSubGovProposal(t, db, proposalID)
	votesBefore, err := db.GetGovernanceVotes(context.Background(), before.ID, nil)
	require.NoError(t, err)
	require.Len(t, votesBefore, 1)

	require.NoError(t, db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		if err := db.DeleteGovernanceVotesAfterSlot(context.Background(), 4, txn); err != nil {
			return err
		}
		return db.DeleteGovernanceProposalsAfterSlot(context.Background(), 4, txn)
	}))
	requireNoSubGovProposal(t, db, proposalID)

	require.NoError(t, applySubGovBlock(t, ls, 5, tx))
	after := requireSubGovProposal(t, db, proposalID)
	votesAfter, err := db.GetGovernanceVotes(context.Background(), after.ID, nil)
	require.NoError(t, err)
	require.Len(t, votesAfter, 1)
	require.Equal(t, before.ActionType, after.ActionType)
	require.Equal(t, before.ProposedEpoch, after.ProposedEpoch)
	require.Equal(
		t,
		votesBefore[0].VoterCredential,
		votesAfter[0].VoterCredential,
	)
	require.Equal(t, votesBefore[0].Vote, votesAfter[0].Vote)
}

func TestDijkstraBatchPhase2InvalidPersistsNoChildGovernance(t *testing.T) {
	t.Parallel()
	ls, db := newSubGovLedger(t)
	proposing := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			subGovInfoProposal(t, 0x09),
		},
	}
	proposalID := subGovBodyID(t, proposing)
	drep := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{0x5a}, 28)),
	}
	voting := dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: subGovVote(drep, proposalID, 0),
	}
	tx := subGovTx(t, false, nil, proposing, voting)
	require.NoError(t, applySubGovBlock(t, ls, 1, tx))
	requireNoSubGovProposal(t, db, proposalID)
	_, err := db.GetDrepByCredential(context.Background(), 0, drep.Hash[:], true, nil)
	require.ErrorIs(t, err, models.ErrDrepNotFound)
}
