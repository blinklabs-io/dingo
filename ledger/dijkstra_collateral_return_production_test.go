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
	"crypto/ed25519"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	dijkstraCollateralRegularInputByte    = byte(0x71)
	dijkstraCollateralCollateralInputByte = byte(0x72)
	dijkstraCollateralReturnTestSlot      = uint64(10)
)

type dijkstraCollateralReturnFixture struct {
	db       *database.Database
	ls       *LedgerState
	tx       *gdijkstra.DijkstraTransaction
	txCbor   []byte
	block    *gdijkstra.DijkstraBlock
	offsets  *database.BlockIngestionResult
	inputIds [][]byte
	startTip ochainsync.Tip
}

func (fx *dijkstraCollateralReturnFixture) rawBlock() chain.RawBlock {
	return chain.RawBlock{
		Slot:        fx.block.SlotNumber(),
		Hash:        fx.block.Hash().Bytes(),
		BlockNumber: fx.block.BlockNumber(),
		Type:        uint(gledger.BlockTypeDijkstra),
		PrevHash:    fx.block.PrevHash().Bytes(),
		Cbor:        fx.block.Cbor(),
	}
}

func newDijkstraCollateralReturnFixture(
	t *testing.T,
	returnAddressType uint8,
) *dijkstraCollateralReturnFixture {
	t.Helper()
	db := newTestDB(t)

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x91}, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	paymentHash := lcommon.Blake2b224Hash(publicKey).Bytes()
	paymentAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash,
		nil,
	)
	require.NoError(t, err)

	returnAddress := paymentAddress
	if returnAddressType == lcommon.AddressTypeKeyPointer ||
		returnAddressType == lcommon.AddressTypeScriptPointer {
		rawAddress := append(
			[]byte{returnAddressType<<4 | lcommon.AddressNetworkTestnet},
			paymentHash...,
		)
		// A zero-valued slot, transaction index, and certificate index form
		// an in-range Shelley pointer.
		rawAddress = append(rawAddress, 0, 0, 0)
		returnAddress, err = lcommon.NewAddressFromBytes(rawAddress)
		require.NoError(t, err)
	}

	regularInputId := bytes.Repeat(
		[]byte{dijkstraCollateralRegularInputByte},
		lcommon.Blake2b256Size,
	)
	collateralInputId := bytes.Repeat(
		[]byte{dijkstraCollateralCollateralInputByte},
		lcommon.Blake2b256Size,
	)
	regularInput := shelley.NewShelleyTransactionInput(
		hex.EncodeToString(regularInputId),
		0,
	)
	collateralInput := shelley.NewShelleyTransactionInput(
		hex.EncodeToString(collateralInputId),
		0,
	)
	regularOutput := &shelley.ShelleyTransactionOutput{
		OutputAddress: paymentAddress,
		OutputAmount:  9_000_000,
	}
	collateralReturn := &shelley.ShelleyTransactionOutput{
		OutputAddress: returnAddress,
		OutputAmount:  3_000_000,
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs:  conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{regularInput}),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{{Output: regularOutput}},
			TxFee:     1_000_000,
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{collateralInput},
				false,
			),
			TxCollateralReturn: &gdijkstra.DijkstraTransactionOutput{Output: collateralReturn},
			TxTotalCollateral:  2_000_000,
		},
		TxIsValid: true,
	}
	initialCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	tx, err = gdijkstra.NewDijkstraTransactionFromCbor(initialCbor)
	require.NoError(t, err)
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, tx.Hash().Bytes()),
		}},
		false,
	)
	tx.WitnessSet.SetCbor(nil)
	tx.SetCbor(nil)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	tx, err = gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)

	for _, seed := range []struct {
		id     []byte
		amount uint64
	}{
		{id: regularInputId, amount: 10_000_000},
		{id: collateralInputId, amount: 5_000_000},
	} {
		outputCbor, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: paymentAddress,
			OutputAmount:  seed.amount,
		})
		require.NoError(t, err)
		require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
			if err := db.CreateUtxo(txn, &models.Utxo{
				TxId:      seed.id,
				OutputIdx: 0,
				AddedSlot: 0,
			}); err != nil {
				return err
			}
			return db.Blob().SetUtxo(txn.Blob(), seed.id, 0, outputCbor)
		}))
	}

	params := dijkstraTestProtocolParameters()
	params.MaxBlockBodySize = 2_000_000
	params.MaxBlockHeaderSize = 100_000
	params.MaxValueSize = 5_000
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:             db,
		activeEras:     []eras.EraDesc{eras.ConwayEraDesc, eras.DijkstraEraDesc},
		currentEra:     eras.DijkstraEraDesc,
		currentPParams: params,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         gdijkstra.EraIdDijkstra,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.publishSnapshotsLocked()

	block := newDijkstraCollateralReturnBlock(t, tx)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	txCborLength := uint32(len(tx.Cbor())) // #nosec G115 -- test fixture is bounded by MaxTxSize
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  dijkstraCollateralReturnTestSlot,
				ByteLength: txCborLength,
			},
		},
	}
	startTip := ochainsync.Tip{Point: ocommon.Point{
		Slot: 1,
		Hash: []byte("pre-existing-tip"),
	}}
	return &dijkstraCollateralReturnFixture{
		db:       db,
		ls:       ls,
		tx:       tx,
		txCbor:   txCbor,
		block:    block,
		offsets:  offsets,
		inputIds: [][]byte{regularInputId, collateralInputId},
		startTip: startTip,
	}
}

func newDijkstraCollateralReturnBlock(
	t *testing.T,
	tx *gdijkstra.DijkstraTransaction,
) *gdijkstra.DijkstraBlock {
	t.Helper()
	block := &gdijkstra.DijkstraBlock{
		BlockHeader: &gdijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 0,
					Slot:        dijkstraCollateralReturnTestSlot,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		BlockBody: gdijkstra.DijkstraBlockBody{
			Transactions: []gdijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	decoded, err := gdijkstra.NewDijkstraBlockFromCbor(blockCbor)
	require.NoError(t, err)
	return decoded
}

func newDijkstraCollateralReturnReplayFixture(
	t *testing.T,
	returnAddressType uint8,
) *dijkstraCollateralReturnFixture {
	t.Helper()
	fx := newDijkstraCollateralReturnFixture(t, returnAddressType)
	ls := newReplayTestLedger(
		t, fx.db, fx.block, uint(gledger.BlockTypeDijkstra),
		eras.DijkstraEraDesc, fx.ls.currentPParams,
	)
	fx.ls = ls
	return fx
}

func assertDijkstraPointerReturnFailure(t *testing.T, err error) {
	t.Helper()
	var pointerErr *lcommon.PtrPresentInCollateralReturn
	require.ErrorAs(t, err, &pointerErr)
	require.EqualValues(t, 22, pointerErr.Type)
}

func TestDijkstraCollateralReturnPointerThroughLedgerAndMempool(t *testing.T) {
	t.Parallel()

	t.Run("pointer is rejected", func(t *testing.T) {
		t.Parallel()
		fx := newDijkstraCollateralReturnFixture(t, lcommon.AddressTypeKeyPointer)
		assertDijkstraPointerReturnFailure(t, fx.ls.ValidateTx(fx.tx))

		pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
			Validator:       fx.ls,
			Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
			PromRegistry:    prometheus.NewRegistry(),
			MempoolCapacity: 1024 * 1024,
		})
		require.NoError(t, err)
		require.NoError(t, pool.Start(context.Background()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, pool.Stop(ctx))
		})
		assertDijkstraPointerReturnFailure(
			t,
			pool.AddTransaction(uint(gdijkstra.TxTypeDijkstra), fx.txCbor),
		)
		require.Empty(t, pool.Transactions())
	})

	t.Run("non-pointer control is accepted", func(t *testing.T) {
		t.Parallel()
		fx := newDijkstraCollateralReturnFixture(t, lcommon.AddressTypeKeyNone)
		require.NoError(t, fx.ls.ValidateTx(fx.tx))

		pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
			Validator:       fx.ls,
			Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
			PromRegistry:    prometheus.NewRegistry(),
			MempoolCapacity: 1024 * 1024,
		})
		require.NoError(t, err)
		require.NoError(t, pool.Start(context.Background()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, pool.Stop(ctx))
		})
		require.NoError(t, pool.AddTransaction(uint(gdijkstra.TxTypeDijkstra), fx.txCbor))
		require.Len(t, pool.Transactions(), 1)
	})
}

func TestDijkstraCollateralReturnPointerRejectedByBlockValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*dijkstraCollateralReturnFixture) error
	}{
		{
			name: "imported block transaction validation",
			run: func(fx *dijkstraCollateralReturnFixture) error {
				return fx.db.Transaction(true).Do(func(txn *database.Txn) error {
					_, err := fx.ls.ledgerProcessBlock(
						txn,
						ocommon.NewPoint(dijkstraCollateralReturnTestSlot, fx.block.Hash().Bytes()),
						fx.block,
						true,
						false,
						false,
						nil,
						envelopeParent{origin: true},
						fx.offsets,
						eras.DijkstraEraDesc,
						fx.ls.currentPParams,
						nil,
						0,
						0,
						false,
					)
					return err
				})
			},
		},
		{
			name: "forged block transaction revalidation",
			run: func(fx *dijkstraCollateralReturnFixture) error {
				return fx.ls.validateForgedTxs(fx.block)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := newDijkstraCollateralReturnFixture(t, lcommon.AddressTypeScriptPointer)
			require.NoError(t, fx.db.SetTip(fx.startTip, nil))
			initialUtxos := make([]*models.Utxo, 0, len(fx.inputIds))
			for _, inputId := range fx.inputIds {
				utxo, err := fx.db.UtxoByRef(inputId, 0, nil)
				require.NoError(t, err)
				initialUtxos = append(initialUtxos, utxo)
			}

			// Repeating after the failed DB transaction models the block retry
			// after rollback: the same source block must fail without consuming
			// either collateral or regular inputs.
			for range 2 {
				err := tc.run(fx)
				assertDijkstraPointerReturnFailure(t, err)
			}
			require.Equal(t, fx.startTip, func() ochainsync.Tip {
				tip, err := fx.db.GetTip(nil)
				require.NoError(t, err)
				return tip
			}())
			for index, inputId := range fx.inputIds {
				utxo, err := fx.db.UtxoByRef(inputId, 0, nil)
				require.NoError(t, err)
				require.Equal(t, initialUtxos[index], utxo)
			}
		})
	}
}

func TestDijkstraCollateralReturnPointerRejectedDuringBlockReplay(t *testing.T) {
	t.Parallel()
	fx := newDijkstraCollateralReturnReplayFixture(
		t,
		lcommon.AddressTypeKeyPointer,
	)
	initialUtxos := make([]*models.Utxo, 0, len(fx.inputIds))
	for _, inputId := range fx.inputIds {
		utxo, err := fx.db.UtxoByRef(inputId, 0, nil)
		require.NoError(t, err)
		initialUtxos = append(initialUtxos, utxo)
	}
	replay := func() {
		t.Helper()
		results := make(chan readChainResult, 1)
		results <- readChainResult{blocks: []gledger.Block{fx.block}}
		close(results)
		assertDijkstraPointerReturnFailure(
			t,
			fx.ls.ledgerProcessBlocksFromSource(context.Background(), results),
		)
	}
	replay()
	require.NoError(t, fx.ls.chain.Rollback(ocommon.Point{}))
	require.Empty(t, fx.ls.chain.Tip().Point.Hash)
	require.NoError(t, fx.ls.chain.AddRawBlocks([]chain.RawBlock{fx.rawBlock()}))
	replay()
	require.Equal(t, ochainsync.Tip{}, func() ochainsync.Tip {
		tip, err := fx.db.GetTip(nil)
		require.NoError(t, err)
		return tip
	}())
	for index, inputId := range fx.inputIds {
		utxo, err := fx.db.UtxoByRef(inputId, 0, nil)
		require.NoError(t, err)
		require.Equal(t, initialUtxos[index], utxo)
	}
}
