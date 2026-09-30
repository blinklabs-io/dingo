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
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func backfillSubGovTx(
	t *testing.T,
	top *dijkstra.DijkstraTransactionBody,
	children ...dijkstra.DijkstraSubTransactionBody,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	subs := make([]dijkstra.DijkstraSubTransaction, len(children))
	for i := range children {
		subs[i] = dijkstra.DijkstraSubTransaction{Body: children[i]}
	}
	tx := &dijkstra.DijkstraTransaction{TxIsValid: true}
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
	return decoded.(*dijkstra.DijkstraTransaction)
}

// TestBackfillProcessBlockTxsBatchedAppliesChildGovernanceInOrder backfills a
// batch whose second child votes on the first child's proposal and whose
// enclosing body votes on it again, which only resolves when every child's
// governance is applied in encoded order before the enclosing body's.
func TestBackfillProcessBlockTxsBatchedAppliesChildGovernanceInOrder(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	backfill := NewBackfill(
		db,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	rewardAddress, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe0}, bytes.Repeat([]byte{0x42}, 28)...),
	)
	require.NoError(t, err)
	child1 := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{{
			PPDeposit:       42,
			PPRewardAccount: rewardAddress,
			PPGovAction: dijkstra.DijkstraGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
				Action: &lcommon.InfoGovAction{
					Type: uint(lcommon.GovActionTypeInfo),
				},
			},
			PPAnchor: lcommon.GovAnchor{
				Url:      "https://example.invalid/backfill-child-1",
				DataHash: [32]byte(bytes.Repeat([]byte{0x31}, 32)),
			},
		}},
	}
	child1ID := backfillSubGovTx(t, nil, child1).
		Body.TxSubTransactions.Items()[0].Body.Id()
	vote := func(marker byte) lcommon.VotingProcedures {
		voter := lcommon.Voter{
			Type: lcommon.VoterTypeDRepKeyHash,
			Hash: [28]byte(bytes.Repeat([]byte{marker}, 28)),
		}
		return lcommon.VotingProcedures{
			&voter: {
				&lcommon.GovActionId{TransactionId: child1ID}: {
					Vote: lcommon.GovVoteYes,
				},
			},
		}
	}
	child2 := dijkstra.DijkstraSubTransactionBody{
		TxVotingProcedures: vote(0x32),
	}
	top := dijkstra.DijkstraTransactionBody{
		TxVotingProcedures: vote(0x33),
	}
	tx := backfillSubGovTx(t, &top, child1, child2)

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

	proposal, err := db.GetGovernanceProposal(child1ID.Bytes(), 0, nil)
	require.NoError(t, err)
	require.Equal(t, child1ID.Bytes(), proposal.TxHash)
	votes, err := db.GetGovernanceVotes(proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 2)
	voters := map[byte]bool{}
	for _, v := range votes {
		voters[v.VoterCredential[0]] = true
	}
	require.Equal(t, map[byte]bool{0x32: true, 0x33: true}, voters)
}
