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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestLedgerProcessBlockExpandsIndexesAcrossValidatedTransactions(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	batch := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{{
					Body: dijkstra.DijkstraSubTransactionBody{},
				}},
				true,
			),
		},
		TxIsValid: true,
	}
	batchCbor, err := batch.MarshalCBOR()
	require.NoError(t, err)
	batch, err = dijkstra.NewDijkstraTransactionFromCbor(batchCbor)
	require.NoError(t, err)
	plain := &dijkstra.DijkstraTransaction{TxIsValid: true}
	plainCbor, err := plain.MarshalCBOR()
	require.NoError(t, err)
	plain, err = dijkstra.NewDijkstraTransactionFromCbor(plainCbor)
	require.NoError(t, err)

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        10,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*batch, *plain},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)

	point := ocommon.Point{Slot: 10, Hash: block.Hash().Bytes()}
	var blockHash [32]byte
	copy(blockHash[:], point.Hash)
	offsets := &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	transactions := []common.Transaction{batch, plain}
	for _, tx := range transactions {
		for _, level := range TransactionLevels(tx) {
			var txHash [32]byte
			copy(txHash[:], level.Hash().Bytes())
			offsets.TxOffsets[txHash] = database.CborOffset{
				BlockSlot:  point.Slot,
				BlockHash:  blockHash,
				ByteLength: 1,
			}
		}
	}

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig:        nodeConfig,
			Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
			SkipDijkstraTxValidation: true,
		},
	}
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			point,
			block,
			true,
			false,
			false,
			nil,
			envelopeParent{},
			offsets,
			eras.DijkstraEraDesc,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	}))

	batchSubTx := batch.Body.TxSubTransactions.Items()[0].Body.Id()
	for _, want := range []struct {
		hash  []byte
		index uint32
	}{
		{hash: batchSubTx.Bytes(), index: 0},
		{hash: batch.Hash().Bytes(), index: 1},
		{hash: plain.Hash().Bytes(), index: 2},
	} {
		stored, err := db.Metadata().GetTransactionByHash(want.hash, nil)
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.Equal(t, want.index, stored.BlockIndex)
	}
}

// dijkstraBatchWithSubOutputTx builds a valid Dijkstra batch whose single
// sub-transaction produces one output and whose enclosing body produces none.
func dijkstraBatchWithSubOutputTx(t *testing.T) *dijkstra.DijkstraTransaction {
	t.Helper()
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
	return dijkstraTx
}

// TestTxValidationSessionAppliesDijkstraBatchLevels stages a batch through the
// validation session's applyTx, whose synthetic offsets must be keyed by each
// level's body hash for the sub-transaction row and output to be recorded.
func TestTxValidationSessionAppliesDijkstraBatchLevels(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	tx := dijkstraBatchWithSubOutputTx(t)
	childHash := tx.Body.TxSubTransactions.Items()[0].Body.Id()
	ls := &LedgerState{
		db: db,
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Now(), time.Second, 100),
			DefaultSlotClockConfig(),
		),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.withTxValidationSession(nil, nil, true, func(
		_ func(common.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]common.Utxo) error,
		_ func() bool,
		applyTx txValidationApplyFunc,
	) error {
		return applyTx(
			tx,
			0,
			ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x63}, 32)},
			uint(dijkstra.EraIdDijkstra),
			1,
		)
	})
	require.NoError(t, err)
	// The session stages its writes and always rolls them back.
	stored, err := db.Metadata().GetTransactionByHash(childHash.Bytes(), nil)
	require.NoError(t, err)
	require.Nil(t, stored)
}

func TestDijkstraBatchIndexesAndStoresSubtransactionOutput(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	dijkstraTx := dijkstraBatchWithSubOutputTx(t)
	var tx common.Transaction = dijkstraTx
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
		db:             db,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
	}
	publishGovernanceTestEpoch(ls, 12, eras.DijkstraEraDesc)
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
