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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestDijkstraBatchIndexesAndStoresSubtransactionOutput(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x42}, 28)...)
	subBody, err := cbor.Encode(map[uint]any{
		0: []any{},
		1: []any{map[uint]any{0: address, 1: uint64(1_000_000)}},
	})
	require.NoError(t, err)
	subTransaction, err := cbor.Encode([]any{
		cbor.RawMessage(subBody), map[uint]any{}, nil,
	})
	require.NoError(t, err)
	body, err := cbor.Encode(map[uint]any{
		0:  []any{},
		1:  []any{},
		2:  uint64(0),
		23: cbor.NewSetType([]cbor.RawMessage{subTransaction}, true),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{cbor.RawMessage(body), map[uint]any{}, true, nil})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)
	childHash := dijkstraTx.Body.TxSubTransactions.Items()[0].Body.Id()
	parentHash := tx.Hash()
	var childHashArray, parentHashArray [32]byte
	copy(childHashArray[:], childHash.Bytes())
	copy(parentHashArray[:], parentHash.Bytes())

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         1,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*dijkstraTx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	blockHash := bytes.Repeat([]byte{0x01}, 32)
	offsets, err := database.NewBlockIndexer(1, blockHash).ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	require.Contains(t, offsets.TxOffsets, childHashArray)
	require.Contains(t, offsets.TxOffsets, parentHashArray)
	require.Contains(t, offsets.UtxoOffsets, database.UtxoRef{TxId: childHashArray, OutputIdx: 0})

	delta := NewLedgerDelta(
		ocommon.Point{Slot: 1, Hash: blockHash},
		uint(dijkstra.EraIdDijkstra),
		1,
	)
	delta.Offsets = offsets
	delta.addTransaction(tx, 0)
	t.Cleanup(delta.Release)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
	}
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	child, err := db.Metadata().GetTransactionByHash(childHash.Bytes(), nil)
	require.NoError(t, err)
	require.Len(t, child.Outputs, 1)
	require.Equal(t, childHash.Bytes(), child.Outputs[0].TxId)
	parent, err := db.Metadata().GetTransactionByHash(parentHash.Bytes(), nil)
	require.NoError(t, err)
	require.Empty(t, parent.Outputs)
}

func TestDijkstraBatchAppliesSubtransactionGovernance(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	rewardAddress, err := common.NewAddressFromBytes(
		append([]byte{0xe0}, bytes.Repeat([]byte{0x42}, 28)...),
	)
	require.NoError(t, err)
	proposal := dijkstra.DijkstraProposalProcedure{
		PPDeposit:       42,
		PPRewardAccount: rewardAddress,
		PPGovAction: dijkstra.DijkstraGovAction{
			Type: uint(common.GovActionTypeInfo),
			Action: &common.InfoGovAction{
				Type: uint(common.GovActionTypeInfo),
			},
		},
		PPAnchor: common.GovAnchor{
			Url:      "https://example.com/dijkstra-child-proposal",
			DataHash: [32]byte(bytes.Repeat([]byte{0x24}, 32)),
		},
	}
	tx := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{{
					Body: dijkstra.DijkstraSubTransactionBody{
						TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{proposal},
					},
				}},
				true,
			),
		},
		TxIsValid: true,
	}
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	decodedTx, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		txCbor,
	)
	require.NoError(t, err)
	tx = decodedTx.(*dijkstra.DijkstraTransaction)
	childHash := tx.Body.TxSubTransactions.Items()[0].Body.Id()
	rootHash := tx.Hash()
	require.NotEqual(t, childHash, rootHash)
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         1,
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
	blockHash := bytes.Repeat([]byte{0x25}, 32)
	offsets, err := database.NewBlockIndexer(1, blockHash).ComputeOffsets(
		blockCbor,
		block,
	)
	require.NoError(t, err)

	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			GovActionValidityPeriod: 20,
			DRepInactivityPeriod:    20,
		},
	}
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 12,
		},
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
	}
	delta := NewLedgerDelta(
		ocommon.Point{Slot: 1, Hash: blockHash},
		uint(dijkstra.EraIdDijkstra),
		1,
	)
	delta.Offsets = offsets
	delta.addTransaction(tx, 0)
	t.Cleanup(delta.Release)
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	got, err := db.GetGovernanceProposal(childHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.Equal(t, childHash.Bytes(), got.TxHash)
	require.Equal(t, uint64(12), got.ProposedEpoch)
	rootProposal, err := db.GetGovernanceProposal(rootHash.Bytes(), 0, nil)
	require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)
	require.Nil(t, rootProposal)
}

func TestDijkstraDirectDepositUpdatesRewardAccountBalance(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	stakeKey := bytes.Repeat([]byte{0x42}, 28)
	rewardAddress := append([]byte{0xe0}, stakeKey...)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		AddedSlot:     1,
		Reward:        dbtypes.Uint64(5),
		Active:        true,
	}))
	body, err := cbor.Encode(map[uint]any{
		0:  []any{},
		1:  []any{},
		2:  uint64(0),
		25: map[cbor.ByteString]uint64{cbor.NewByteString(rewardAddress): 20},
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{cbor.RawMessage(body), map[uint]any{}, true, nil})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         2,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*dijkstraTx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	blockHash := bytes.Repeat([]byte{0x02}, 32)
	offsets, err := database.NewBlockIndexer(2, blockHash).ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	delta := NewLedgerDelta(
		ocommon.Point{Slot: 2, Hash: blockHash},
		uint(dijkstra.EraIdDijkstra),
		1,
	)
	delta.Offsets = offsets
	delta.addTransaction(tx, 0)
	t.Cleanup(delta.Release)
	ls := &LedgerState{
		db:             db,
		currentPParams: &dijkstra.DijkstraProtocolParameters{},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
	}
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))
	account, err := db.GetAccountByCredential(0, stakeKey, false, nil)
	require.NoError(t, err)
	require.Equal(t, dbtypes.Uint64(25), account.Reward)

	// Direct-deposit credits use the transaction-body hash as their journal
	// source, so rollback removes the credit and replay can apply it again.
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(1, nil))
	account, err = db.GetAccountByCredential(0, stakeKey, false, nil)
	require.NoError(t, err)
	require.Equal(t, dbtypes.Uint64(5), account.Reward)
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		return ApplyDijkstraDirectDeposits(db, tx, 2, txn)
	}))
	account, err = db.GetAccountByCredential(0, stakeKey, false, nil)
	require.NoError(t, err)
	require.Equal(t, dbtypes.Uint64(25), account.Reward)
}
