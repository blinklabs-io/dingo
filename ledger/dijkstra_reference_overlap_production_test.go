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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type dijkstraReferenceOverlapFixture struct {
	db         *database.Database
	ls         *LedgerState
	tx         *dijkstra.DijkstraTransaction
	block      *dijkstra.DijkstraBlock
	blockCbor  []byte
	offsets    *database.BlockIngestionResult
	originHash []byte
	pparams    *dijkstra.DijkstraProtocolParameters
}

func newDijkstraReferenceOverlapFixture(
	t *testing.T,
) *dijkstraReferenceOverlapFixture {
	t.Helper()
	db := newTestDB(t)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x91
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	paymentHash := lcommon.Blake2b224Hash(publicKey)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash[:],
		nil,
	)
	require.NoError(t, err)
	inputTxID := bytes.Repeat([]byte{0x92}, lcommon.Blake2b256Size)
	input := shelley.ShelleyTransactionInput{
		TxId:        lcommon.NewBlake2b256(inputTxID),
		OutputIndex: 0,
	}
	const value uint64 = 1_000_000
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  value,
	}
	require.NoError(
		t,
		db.Transaction(context.Background(), true).
			Do(func(txn *database.Txn) error {
				if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{
					TxId:       inputTxID,
					OutputIdx:  0,
					PaymentKey: paymentHash.Bytes(),
					AddedSlot:  1,
					Amount:     dbtypes.Uint64(value),
				}); err != nil {
					return err
				}
				encoded, err := cbor.Encode(output)
				if err != nil {
					return err
				}
				return db.Blob().SetUtxo(txn.Blob(), inputTxID, 0, encoded)
			}),
	)

	tx := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []dijkstra.DijkstraTransactionOutput{{
				Output: &mary.MaryTransactionOutput{
					OutputAddress: address,
					OutputAmount:  mary.MaryTransactionOutputValue{Amount: value},
				},
			}},
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				true,
			),
		},
		TxIsValid: true,
	}
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	txHash := tx.Body.Id()
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, txHash[:]),
		}},
		false,
	)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	decodedTx, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		txCbor,
	)
	require.NoError(t, err)
	tx, ok := decodedTx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)

	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	originHash := bytes.Repeat([]byte{0xf1}, lcommon.Blake2b256Size)
	originTip := ochainsync.Tip{Point: ocommon.Point{Slot: 1, Hash: originHash}}
	require.NoError(t, db.SetTip(originTip, nil))
	config := newTestShelleyGenesisCfg(t)
	config.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{eras.DijkstraEraDesc},
		currentEra: eras.DijkstraEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         eras.DijkstraEraDesc.Id,
		},
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         eras.DijkstraEraDesc.Id,
		}},
		currentPParams: pparams,
		currentTip:     originTip,
		currentTipBlockNonce: bytes.Repeat(
			[]byte{0xf2},
			lcommon.Blake2b256Size,
		),
		validationEnabled: true,
		config: LedgerStateConfig{
			CardanoNodeConfig: config,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()

	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        10,
					PrevHash:    lcommon.NewBlake2b256(originHash),
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
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
	point := ocommon.Point{Slot: 10, Hash: block.Hash().Bytes()}
	offsets, err := database.NewBlockIndexer(point.Slot, point.Hash).
		ComputeOffsets(blockCbor, block)
	require.NoError(t, err)

	return &dijkstraReferenceOverlapFixture{
		db:         db,
		ls:         ls,
		tx:         tx,
		block:      block,
		blockCbor:  blockCbor,
		offsets:    offsets,
		originHash: originHash,
		pparams:    pparams,
	}
}

func applyDijkstraReferenceOverlapBlock(
	t *testing.T,
	fixture *dijkstraReferenceOverlapFixture,
) error {
	t.Helper()
	point := ocommon.Point{
		Slot: fixture.block.SlotNumber(),
		Hash: fixture.block.Hash().Bytes(),
	}
	return fixture.db.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			_, err := fixture.ls.ledgerProcessBlock(
				context.Background(),
				txn,
				point,
				fixture.block,
				true,
				false,
				false,
				fixture.originHash,
				envelopeParent{origin: true},
				fixture.offsets,
				eras.DijkstraEraDesc,
				fixture.pparams,
				nil,
				0,
				0,
				false,
			)
			return err
		})
}

func TestDijkstraSpendReferenceOverlapLedgerAdmissionAndForging(t *testing.T) {
	t.Parallel()
	fixture := newDijkstraReferenceOverlapFixture(t)
	require.NoError(t, fixture.ls.ValidateTx(fixture.tx))
	require.NoError(t, fixture.ls.ValidateTxWithOverlay(fixture.tx, nil, nil))
	require.NoError(t, fixture.ls.validateForgedTxs(fixture.block))
}

func TestDijkstraSpendReferenceOverlapLiveApplyAndReplay(t *testing.T) {
	t.Parallel()
	t.Run("live block application", func(t *testing.T) {
		fixture := newDijkstraReferenceOverlapFixture(t)
		require.NoError(t, applyDijkstraReferenceOverlapBlock(t, fixture))
		_, err := fixture.db.Metadata().GetUtxoIncludingSpent(
			fixture.tx.Hash().Bytes(),
			0,
			nil,
		)
		require.NoError(t, err)
	})

	t.Run("historical replay", func(t *testing.T) {
		fixture := newDijkstraReferenceOverlapFixture(t)
		point := ocommon.Point{
			Slot: fixture.block.SlotNumber(),
			Hash: fixture.block.Hash().Bytes(),
		}
		require.NoError(t, fixture.db.BlockCreate(models.Block{
			Slot:     point.Slot,
			Hash:     point.Hash,
			PrevHash: fixture.originHash,
			Number:   fixture.block.BlockNumber(),
			Type:     gledger.BlockTypeDijkstra,
			Cbor:     fixture.blockCbor,
		}, nil))
		results := make(chan readChainResult, 1)
		done := make(chan struct{})
		results <- readChainResult{
			blocks: []gledger.Block{fixture.block},
			done:   done,
		}
		close(results)
		require.NoError(t, fixture.ls.ledgerProcessBlocksFromSource(
			t.Context(),
			results,
		))
		select {
		case <-done:
		default:
			t.Fatal("reader result was not released after replay")
		}
		_, err := fixture.db.Metadata().GetUtxoIncludingSpent(
			fixture.tx.Hash().Bytes(),
			0,
			nil,
		)
		require.NoError(t, err)
	})
}
