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

package node

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dijkstrabatchfixture"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestBackfillProcessBlockTxsBatchedUsesSharedGovernanceFixture ensures the
// same child proposal IDs, parent root, and voting inputs used by ledger tests
// are reconstructed by historical metadata backfill.
func TestBackfillProcessBlockTxsBatchedUsesSharedGovernanceFixture(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	backfill := NewBackfill(
		db,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	fixture, err := dijkstrabatchfixture.New()
	require.NoError(t, err)
	tx := fixture.Transaction

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         1000,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	blockBodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(blockBodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	point := ocommon.Point{
		Slot: 1000,
		Hash: bytes.Repeat([]byte{0x34}, 32),
	}
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:   point.Slot,
		Hash:   point.Hash,
		Number: 1,
		Cbor:   blockCbor,
		Type:   uint(block.Type()),
	}, nil))
	offsets, err := database.NewBlockIndexer(point.Slot, point.Hash).
		ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			GovActionValidityPeriod: 20,
			DRepInactivityPeriod:    20,
		},
	}
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := backfill.processBlockTxsBatched(
			[]lcommon.Transaction{tx},
			point,
			12,
			dijkstra.EraIdDijkstra,
			pparams,
			offsets,
			acc,
			txn,
			nil,
			true,
		); err != nil {
			return err
		}
		return db.FlushBatch(acc, txn)
	}))

	root, err := db.GetGovernanceProposal(fixture.ProposalIDs[0].Bytes(), 0, nil)
	require.NoError(t, err)
	child, err := db.GetGovernanceProposal(fixture.ProposalIDs[1].Bytes(), 0, nil)
	require.NoError(t, err)
	subTransactions := fixture.Transaction.Body.TxSubTransactions.Items()
	require.Len(t, subTransactions, 2)
	for index, proposal := range []*models.GovernanceProposal{root, child} {
		procedure := subTransactions[index].Body.TxProposalProcedures[0]
		actionCbor, err := cbor.Encode(procedure.PPGovAction.Action)
		require.NoError(t, err)
		returnAddress, err := procedure.PPRewardAccount.Bytes()
		require.NoError(t, err)
		require.Equal(t, fixture.ProposalIDs[index].Bytes(), proposal.TxHash)
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
		require.Equal(t, uint64(point.Slot), proposal.AddedSlot)
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
	require.Nil(t, root.ParentActionIdx)
	require.Equal(t, fixture.RootID.Bytes(), child.ParentTxHash)
	require.NotNil(t, child.ParentActionIdx)
	require.Zero(t, *child.ParentActionIdx)
	votes, err := db.GetGovernanceVotes(root.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 2)
	voters := make(map[byte]bool, len(votes))
	var credentials [][]byte
	for _, vote := range votes {
		require.Equal(t, root.ID, vote.ProposalID)
		require.Equal(t, uint8(models.VoterTypeDRep), vote.VoterType)
		require.Zero(t, vote.VoterCredentialTag)
		require.Equal(t, uint8(models.VoteYes), vote.Vote)
		require.Equal(t, uint64(point.Slot), vote.AddedSlot)
		require.NotNil(t, vote.VoteUpdatedSlot)
		require.Equal(t, uint64(point.Slot), *vote.VoteUpdatedSlot)
		require.Empty(t, vote.AnchorURL)
		require.Empty(t, vote.AnchorHash)
		require.Nil(t, vote.DeletedSlot)
		voters[vote.VoterCredential[0]] = true
		require.Equal(t, bytes.Repeat([]byte{vote.VoterCredential[0]}, 28), vote.VoterCredential)
		credentials = append(credentials, vote.VoterCredential)
	}
	require.Equal(t, map[byte]bool{0x41: true, 0x42: true}, voters)
	require.ElementsMatch(t, [][]byte{
		bytes.Repeat([]byte{0x41}, 28),
		bytes.Repeat([]byte{0x42}, 28),
	}, credentials)
}
