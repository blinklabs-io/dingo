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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type validityOutcomeTestBlock struct {
	header lcommon.BlockHeader
	txs    []lcommon.Transaction
	era    lcommon.Era
}

func (b *validityOutcomeTestBlock) Type() int {
	if b.era.Id == gdijkstra.EraIdDijkstra {
		return gdijkstra.BlockTypeDijkstra
	}
	return gledger.BlockTypeByronMain
}
func (b *validityOutcomeTestBlock) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256Hash([]byte("validity-outcome-block"))
}

func (b *validityOutcomeTestBlock) Header() lcommon.BlockHeader { return b.header }
func (b *validityOutcomeTestBlock) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}
func (b *validityOutcomeTestBlock) BlockNumber() uint64 { return 1 }
func (b *validityOutcomeTestBlock) SlotNumber() uint64  { return 1 }
func (b *validityOutcomeTestBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *validityOutcomeTestBlock) BlockBodySize() uint64 { return 1 }
func (b *validityOutcomeTestBlock) Era() lcommon.Era {
	if b.era.Id != 0 {
		return b.era
	}
	return byron.EraByron
}
func (b *validityOutcomeTestBlock) Transactions() []lcommon.Transaction {
	return b.txs
}

// validityOutcomeStateTx keeps the mock fixture's simple body helpers while
// preserving the Dijkstra invalid-transaction state transition: invalid
// transactions consume collateral and produce only collateral return.
type validityOutcomeStateTx struct {
	*omockledger.MockTransaction
}

func (t *validityOutcomeStateTx) Consumed() []lcommon.TransactionInput {
	if t.IsValid() {
		return t.Inputs()
	}
	return t.Collateral()
}

func (t *validityOutcomeStateTx) Produced() []lcommon.Utxo {
	if t.IsValid() {
		return t.MockTransaction.Produced()
	}
	if t.CollateralReturn() == nil {
		return nil
	}
	return []lcommon.Utxo{{
		Id: shelley.NewShelleyTransactionInput(
			t.Hash().String(),
			len(t.Outputs()),
		),
		Output: t.CollateralReturn(),
	}}
}

func TestLedgerProcessBlockDijkstraValidityOutcomeStateTransitions(
	t *testing.T,
) {
	tests := []struct {
		name          string
		declaredValid bool
		phase2Fails   bool
	}{
		{name: "valid script passes", declaredValid: true},
		{name: "invalid script fails", declaredValid: false, phase2Fails: true},
		{name: "valid script fails", declaredValid: true, phase2Fails: true},
		{name: "invalid script passes", declaredValid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t)
			regularInputID := bytes.Repeat([]byte{0x41}, 32)
			collateralInputID := bytes.Repeat([]byte{0x42}, 32)
			regularInput, err := omockledger.NewSimpleTransactionInput(
				regularInputID,
				0,
			)
			require.NoError(t, err)
			collateralInput, err := omockledger.NewSimpleTransactionInput(
				collateralInputID,
				0,
			)
			require.NoError(t, err)
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				lcommon.AddressNetworkTestnet,
				bytes.Repeat([]byte{0x44}, lcommon.AddressHashSize),
				nil,
			)
			require.NoError(t, err)
			output, err := omockledger.NewSimpleTransactionOutput(
				address.String(),
				10,
			)
			require.NoError(t, err)
			collateralReturn, err := omockledger.NewSimpleTransactionOutput(
				address.String(),
				5,
			)
			require.NoError(t, err)
			mockTx := omockledger.NewTransactionBuilder()
			mockTx.WithId(bytes.Repeat([]byte{0x43}, 32))
			mockTx.WithType(gdijkstra.TxTypeDijkstra)
			mockTx.WithInputs(regularInput)
			mockTx.WithOutputs(output)
			mockTx.WithCollateral(collateralInput)
			mockTx.WithCollateralReturn(collateralReturn)
			mockTx.WithValid(tt.declaredValid)
			tx := &validityOutcomeStateTx{MockTransaction: mockTx}
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			offsets := &database.BlockIngestionResult{
				TxOffsets: map[[32]byte]database.CborOffset{
					txHash: {BlockSlot: 10, ByteLength: 1},
				},
				UtxoOffsets: map[database.UtxoRef]database.CborOffset{
					{TxId: txHash, OutputIdx: 0}: {
						BlockSlot:  10,
						ByteLength: 1,
					},
					{TxId: txHash, OutputIdx: 1}: {
						BlockSlot:  10,
						ByteLength: 1,
					},
				},
			}

			require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
				TxId: regularInputID, OutputIdx: 0, AddedSlot: 1,
			}))
			require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
				TxId: collateralInputID, OutputIdx: 0, AddedSlot: 1,
			}))
			initialTip := ochainsync.Tip{Point: ocommon.Point{
				Slot: 1, Hash: []byte("unchanged-tip"),
			}}
			require.NoError(t, db.SetTip(initialTip, nil))

			phase1Reached := false
			phase2Reached := false
			testEra := eras.DijkstraEraDesc
			testEra.ValidateTxFunc = func(
				gotTx lcommon.Transaction,
				_ uint64,
				_ lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				phase1Reached = true
				phase2Reached = true
				if tt.declaredValid == tt.phase2Fails {
					return errors.New(
						"Dijkstra declared validity does not match phase-2 result",
					)
				}
				return nil
			}
			pparams := &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
					MaxBlockBodySize:   100_000,
					MaxBlockHeaderSize: 100_000,
				},
			}
			nodeConfig := newTestShelleyGenesisCfg(t)
			nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
			ls := &LedgerState{
				db:         db,
				activeEras: []eras.EraDesc{testEra},
				config: LedgerStateConfig{
					CardanoNodeConfig: nodeConfig,
					Logger:            testLogger(),
				},
				currentEra: testEra,
			}
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
					testEra,
					pparams,
					nil,
					true,
				)
				return err
			})
			require.True(t, phase1Reached)
			require.True(t, phase2Reached)

			regular, err := db.Metadata().
				GetUtxoIncludingSpent(regularInputID, 0, nil)
			require.NoError(t, err)
			collateral, err := db.Metadata().
				GetUtxoIncludingSpent(collateralInputID, 0, nil)
			require.NoError(t, err)
			outputRow, err := db.Metadata().
				GetUtxoIncludingSpent(tx.Hash().Bytes(), 0, nil)
			require.NoError(t, err)
			returnRow, err := db.Metadata().
				GetUtxoIncludingSpent(tx.Hash().Bytes(), 1, nil)
			require.NoError(t, err)
			tip, err := db.GetTip(nil)
			require.NoError(t, err)

			mismatch := tt.declaredValid == tt.phase2Fails
			if mismatch {
				require.Error(t, processErr)
				require.Zero(t, regular.DeletedSlot)
				require.Zero(t, collateral.DeletedSlot)
				require.Nil(t, outputRow)
				require.Nil(t, returnRow)
				require.Equal(t, initialTip, tip)
				return
			}
			require.NoError(t, processErr)
			require.Equal(t, initialTip, tip)
			if tt.declaredValid {
				require.Equal(t, uint64(10), regular.DeletedSlot)
				require.Zero(t, collateral.DeletedSlot)
				require.NotNil(t, outputRow)
				require.Nil(t, returnRow)
				return
			}
			require.Zero(t, regular.DeletedSlot)
			require.Equal(t, uint64(10), collateral.DeletedSlot)
			require.Nil(t, outputRow)
			require.NotNil(t, returnRow)
		})
	}
}

// TestLedgerProcessBlockHistoricalValidationRunsPhase2 verifies the
// configuration decision at the real block-application boundary.  A
// historical-validation replay must pass a LedgerView with phase-two
// validation enabled; the trusted-replay control retains the skip shortcut.
func TestLedgerProcessBlockHistoricalValidationRunsPhase2(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name              string
		validationEnabled bool
		wantValidationErr bool
	}{
		{
			name:              "historical validation evaluates phase two",
			validationEnabled: true,
			wantValidationErr: true,
		},
		{
			name:              "trusted replay keeps phase two skipped",
			validationEnabled: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			tx := omockledger.NewTransactionBuilder()
			tx.WithId(bytes.Repeat([]byte{0x52}, 32))
			tx.WithType(gdijkstra.TxTypeDijkstra)
			tx.WithValid(true)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			offsets := &database.BlockIngestionResult{
				TxOffsets: map[[32]byte]database.CborOffset{
					txHash: {BlockSlot: 10, ByteLength: 1},
				},
			}

			phase2Called := false
			testEra := eras.DijkstraEraDesc
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				_ uint64,
				state lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				phase2Called = true
				skipper, ok := state.(interface {
					SkipPhase2Validation() bool
				})
				if ok && skipper.SkipPhase2Validation() {
					return nil
				}
				return conway.PlutusScriptFailedError{
					Err: errors.New("phase-two validation mismatch"),
				}
			}
			pparams := &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
					MaxBlockBodySize:   100_000,
					MaxBlockHeaderSize: 100_000,
				},
			}
			nodeConfig := newTestShelleyGenesisCfg(t)
			nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
			ls := &LedgerState{
				db:         db,
				activeEras: []eras.EraDesc{testEra},
				config: LedgerStateConfig{
					CardanoNodeConfig: nodeConfig,
					Logger:            testLogger(),
				},
				currentEra: testEra,
			}
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
			skipPhase2 := shouldSkipConfiguredPhase2Validation(
				tt.validationEnabled,
				true,
				true,
			)
			processErr := db.Transaction(true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(
					txn,
					ocommon.NewPoint(10, block.Hash().Bytes()),
					block,
					true,
					false,
					skipPhase2,
					nil,
					envelopeParent{origin: true},
					offsets,
					testEra,
					pparams,
					nil,
					true,
				)
				return err
			})
			require.True(t, phase2Called)
			if tt.wantValidationErr {
				var plutusErr conway.PlutusScriptFailedError
				require.ErrorAs(t, processErr, &plutusErr)
			} else {
				require.NoError(t, processErr)
			}
		})
	}
}

func (b *validityOutcomeTestBlock) Cbor() []byte { return []byte{0x82, 0x80, 0x80} }
func (b *validityOutcomeTestBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}
func (b *validityOutcomeTestBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func TestLedgerProcessBlockEnforcesTransactionValidationOutcomes(
	t *testing.T,
) {
	tests := []struct {
		name          string
		declaredValid bool
		validationErr func(error) error
	}{
		{
			name:          "declared invalid still runs validation",
			declaredValid: false,
			validationErr: func(sentinel error) error { return sentinel },
		},
		{
			name:          "declared valid script failure rejects block",
			declaredValid: true,
			validationErr: func(sentinel error) error {
				return conway.PlutusScriptFailedError{Err: sentinel}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

			tx := omockledger.NewTransactionBuilder()
			tx.WithId(bytes.Repeat([]byte{0x35}, 32))
			tx.WithType(byron.TxTypeByron)
			tx.WithValid(tt.declaredValid)
			sentinel := errors.New("transaction validation sentinel")
			called := false
			testEra := eras.ByronEraDesc
			testEra.ValidateTxFunc = func(
				lcommon.Transaction,
				uint64,
				lcommon.LedgerState,
				lcommon.ProtocolParameters,
			) error {
				called = true
				return tt.validationErr(sentinel)
			}
			ls := &LedgerState{
				db:         db,
				activeEras: []eras.EraDesc{testEra},
				config:     LedgerStateConfig{Logger: testLogger()},
				currentEra: testEra,
			}
			block := &validityOutcomeTestBlock{
				header: &byron.ByronMainBlockHeader{},
				txs:    []lcommon.Transaction{tx},
			}

			err = db.Transaction(true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(
					txn,
					ocommon.NewPoint(1, block.Hash().Bytes()),
					block,
					true,
					false,
					false,
					nil,
					envelopeParent{origin: true},
					nil,
					testEra,
					nil,
					nil,
					true,
				)
				return err
			})
			require.True(
				t,
				called,
				"validated block must run transaction validation",
			)
			require.ErrorIs(t, err, sentinel)
		})
	}
}
