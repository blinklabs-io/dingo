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
	"crypto/ed25519"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	dijkstraRefFeeStride   = 100
	dijkstraRefFeeBase     = 1_000
	dijkstraRefFeeBalance  = uint64(50_000_000)
	dijkstraRefFeeScript   = 250
	dijkstraRefFeeMultiple = 2
)

// dijkstraTieredRefScriptFee prices each full stride of reference-script bytes
// at one coin per byte, doubling the price per stride, and the remainder at
// the next tier. It is written out so the expectation does not come from the
// code under test.
func dijkstraTieredRefScriptFee(size uint64) uint64 {
	price := big.NewRat(1, 1)
	total := new(big.Rat)
	for size >= dijkstraRefFeeStride {
		total.Add(total, new(big.Rat).Mul(price, big.NewRat(dijkstraRefFeeStride, 1)))
		price.Mul(price, big.NewRat(dijkstraRefFeeMultiple, 1))
		size -= dijkstraRefFeeStride
	}
	total.Add(total, new(big.Rat).Mul(price, new(big.Rat).SetUint64(size)))
	return new(big.Int).Quo(total.Num(), total.Denom()).Uint64()
}

func dijkstraRefFeeParams() *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeB:                    dijkstraRefFeeBase,
			MaxTxSize:                  16_384,
			MaxValueSize:               5_000,
			MaxBlockBodySize:           100_000,
			MaxBlockHeaderSize:         100_000,
			MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(1, 1)},
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
		MaxRefScriptSizePerTx:    100_000,
		MaxRefScriptSizePerBlock: 1_000_000,
		RefScriptCostStride:      dijkstraRefFeeStride,
		RefScriptCostMultiplier: &cbor.Rat{
			Rat: big.NewRat(dijkstraRefFeeMultiple, 1),
		},
	}
}

type dijkstraRefFeeFixture struct {
	inputs   []lcommon.Utxo
	tx       func(t *testing.T, fee uint64) *gdijkstra.DijkstraTransaction
	minFee   uint64
	refSize  uint64
	spendIn  shelley.ShelleyTransactionInput
	refInput shelley.ShelleyTransactionInput
}

func newDijkstraRefFeeFixture(t *testing.T) *dijkstraRefFeeFixture {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x55
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
	newOutput := func(amount uint64) babbage.BabbageTransactionOutput {
		return babbage.BabbageTransactionOutput{
			OutputAddress: address,
			OutputAmount:  mary.MaryTransactionOutputValue{Amount: amount},
		}
	}
	spendIn := shelley.NewShelleyTransactionInput(
		"a228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee11", 0,
	)
	refInput := shelley.NewShelleyTransactionInput(
		"b228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22", 0,
	)
	refOutput := newOutput(2_000_000)
	refOutput.TxOutScriptRef = &lcommon.ScriptRef{
		Type: lcommon.ScriptRefTypePlutusV4,
		Script: lcommon.PlutusV4Script(
			make([]byte, dijkstraRefFeeScript),
		),
	}
	return &dijkstraRefFeeFixture{
		inputs: []lcommon.Utxo{
			{
				Id: spendIn,
				Output: gdijkstra.DijkstraTransactionOutput{
					Output: newOutput(dijkstraRefFeeBalance),
				},
			},
			{
				Id:     refInput,
				Output: gdijkstra.DijkstraTransactionOutput{Output: refOutput},
			},
		},
		tx: func(t *testing.T, fee uint64) *gdijkstra.DijkstraTransaction {
			t.Helper()
			tx := &gdijkstra.DijkstraTransaction{
				Body: gdijkstra.DijkstraTransactionBody{
					TxInputs: conway.NewConwayTransactionInputSet(
						[]shelley.ShelleyTransactionInput{spendIn},
					),
					TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
						Output: newOutput(dijkstraRefFeeBalance - fee),
					}},
					TxFee: fee,
					TxReferenceInputs: cbor.NewSetType(
						[]shelley.ShelleyTransactionInput{refInput},
						false,
					),
				},
				TxIsValid: true,
			}
			bodyCbor, err := cbor.Encode(tx.Body)
			require.NoError(t, err)
			tx.Body.SetCbor(bodyCbor)
			txHash := tx.Hash()
			tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
				[]lcommon.VkeyWitness{{
					Vkey:      publicKey,
					Signature: ed25519.Sign(privateKey, txHash[:]),
				}},
				false,
			)
			return tx
		},
		minFee: dijkstraRefFeeBase +
			dijkstraTieredRefScriptFee(dijkstraRefFeeScript),
		refSize:  dijkstraRefFeeScript,
		spendIn:  spendIn,
		refInput: refInput,
	}
}

func newDijkstraRefFeeLedgerState(
	t *testing.T,
	db *database.Database,
) *LedgerState {
	t.Helper()
	era := eras.DijkstraEraDesc
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		EraId:         era.Id,
		SlotLength:    1000,
		LengthInSlots: 200_000_000,
	}
	ls := &LedgerState{
		db:             db,
		activeEras:     []eras.EraDesc{era},
		currentEra:     era,
		currentPParams: dijkstraRefFeeParams(),
		currentEpoch:   epoch,
		epochCache:     []models.Epoch{epoch},
		config: LedgerStateConfig{
			Logger:            testLogger(),
			CardanoNodeConfig: nodeConfig,
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	return ls
}

func requireDijkstraFeeTooSmall(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var feeErr shelley.FeeTooSmallUtxoError
	require.ErrorAs(t, err, &feeErr)
}

// TestLedgerStateValidateTxDijkstraTieredRefScriptFee drives the mempool
// validation entry point: the tiered reference-script fee, priced from the
// Dijkstra stride and multiplier, is enforced at the exact minimum.
func TestLedgerStateValidateTxDijkstraTieredRefScriptFee(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	fx := newDijkstraRefFeeFixture(t)
	ls := newDijkstraRefFeeLedgerState(t, db)
	created := make(map[utxoref.Key]lcommon.Utxo, len(fx.inputs))
	for _, utxo := range fx.inputs {
		created[utxoref.ForInput(utxo.Id)] = utxo
	}
	// A flat per-byte price would charge the base fee plus 250.
	require.Greater(t, fx.minFee, uint64(dijkstraRefFeeBase)+fx.refSize)

	require.NoError(
		t,
		ls.ValidateTxWithOverlay(fx.tx(t, fx.minFee), nil, created),
		"exactly paid fee must be accepted",
	)
	requireDijkstraFeeTooSmall(
		t,
		ls.ValidateTxWithOverlay(fx.tx(t, fx.minFee-1), nil, created),
	)
	requireDijkstraFeeTooSmall(
		t,
		ls.ValidateTxWithOverlay(
			fx.tx(t, uint64(dijkstraRefFeeBase)+fx.refSize), nil, created,
		),
	)
}

// TestLedgerProcessBlockDijkstraTieredRefScriptFee drives block application:
// a transaction paying the exact tiered minimum is applied, and one a coin
// short is rejected before any of its state changes are stored.
func TestLedgerProcessBlockDijkstraTieredRefScriptFee(t *testing.T) {
	t.Parallel()
	fx := newDijkstraRefFeeFixture(t)
	for _, tc := range []struct {
		name    string
		fee     uint64
		wantErr bool
	}{
		{name: "exact minimum fee applies", fee: fx.minFee},
		{name: "one below minimum fee is rejected", fee: fx.minFee - 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
			require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
				for _, utxo := range fx.inputs {
					id := utxo.Id.Id().Bytes()
					idx := utxo.Id.Index()
					if err := db.CreateUtxo(txn, &models.Utxo{
						TxId: id, OutputIdx: idx, AddedSlot: 1,
					}); err != nil {
						return err
					}
					encoded, err := cbor.Encode(utxo.Output)
					if err != nil {
						return err
					}
					if err := db.Blob().SetUtxo(
						txn.Blob(), id, idx, encoded,
					); err != nil {
						return err
					}
				}
				return nil
			}))
			tx := fx.tx(t, tc.fee)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			offsets := &database.BlockIngestionResult{
				TxOffsets: map[[32]byte]database.CborOffset{
					txHash: {BlockSlot: 10, ByteLength: 1},
				},
				UtxoOffsets: map[database.UtxoRef]database.CborOffset{
					{TxId: txHash, OutputIdx: 0}: {BlockSlot: 10, ByteLength: 1},
				},
			}
			ls := newDijkstraRefFeeLedgerState(t, db)
			block := &validityOutcomeTestBlock{
				header: &gdijkstra.DijkstraBlockHeader{
					BabbageBlockHeader: babbage.BabbageBlockHeader{
						Body: babbage.BabbageBlockHeaderBody{
							BlockNumber: 1,
							Slot:        10,
							ProtoVersion: babbage.BabbageProtoVersion{
								Major: gdijkstra.MinProtocolVersionDijkstra,
							},
						},
					},
				},
				txs: []lcommon.Transaction{tx},
				era: gdijkstra.EraDijkstra,
			}
			processErr := db.Transaction(true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(
					txn,
					ocommon.NewPoint(10, block.Hash().Bytes()),
					block,
					true,
					false,
					false,
					nil,
					envelopeParent{origin: true},
					offsets,
					eras.DijkstraEraDesc,
					dijkstraRefFeeParams(),
					nil,
					0,
					0,
					false,
				)
				return err
			})

			spent, err := db.Metadata().GetUtxoIncludingSpent(
				fx.spendIn.Id().Bytes(), fx.spendIn.Index(), nil,
			)
			require.NoError(t, err)
			output, err := db.Metadata().GetUtxoIncludingSpent(
				tx.Hash().Bytes(), 0, nil,
			)
			require.NoError(t, err)
			if tc.wantErr {
				requireDijkstraFeeTooSmall(t, processErr)
				require.Zero(t, spent.DeletedSlot)
				require.Nil(t, output)
				return
			}
			require.NoError(t, processErr)
			require.Equal(t, uint64(10), spent.DeletedSlot)
			require.NotNil(t, output)
		})
	}
}
