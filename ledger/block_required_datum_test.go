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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// requiredDatumFixture is a script-locked UTxO carrying a datum hash, plus a
// collateral UTxO locked by key, both seeded in the database.
type requiredDatumFixture struct {
	db             *database.Database
	key            ed25519.PrivateKey
	script         lcommon.Script
	datum          lcommon.Datum
	spendTxId      []byte
	collateralTxId []byte
}

// decodeRequiredDatum returns a datum decoded from fixed bytes so its hash is
// taken over the original encoding, which survives the block's re-encoding.
func decodeRequiredDatum(t *testing.T) lcommon.Datum {
	t.Helper()
	var datum lcommon.Datum
	_, err := cbor.Decode([]byte{0xd8, 0x79, 0x80}, &datum)
	require.NoError(t, err)
	return datum
}

func newRequiredDatumFixture(
	t *testing.T,
	script lcommon.Script,
	withDatumHash bool,
) *requiredDatumFixture {
	t.Helper()
	f := &requiredDatumFixture{
		db: newTestDB(t),
		key: ed25519.NewKeyFromSeed(
			bytes.Repeat([]byte{0xd3}, ed25519.SeedSize),
		),
		script:         script,
		datum:          decodeRequiredDatum(t),
		spendTxId:      bytes.Repeat([]byte{0xd1}, 32),
		collateralTxId: bytes.Repeat([]byte{0xd2}, 32),
	}
	scriptAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		script.Hash().Bytes(),
		nil,
	)
	require.NoError(t, err)
	keyAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		lcommon.Blake2b224Hash(f.key.Public().(ed25519.PublicKey)).Bytes(),
		nil,
	)
	require.NoError(t, err)
	scriptAddrBytes, err := scriptAddr.Bytes()
	require.NoError(t, err)
	keyAddrBytes, err := keyAddr.Bytes()
	require.NoError(t, err)
	spendOutput := map[uint]any{0: scriptAddrBytes, 1: uint64(10_000_000)}
	if withDatumHash {
		spendOutput[2] = []any{0, f.datum.Hash().Bytes()}
	}
	collateralOutput := map[uint]any{
		0: keyAddrBytes,
		1: uint64(10_000_000),
	}
	for txId, output := range map[*[]byte]map[uint]any{
		&f.spendTxId:      spendOutput,
		&f.collateralTxId: collateralOutput,
	} {
		encoded, err := cbor.Encode(output)
		require.NoError(t, err)
		require.NoError(
			t,
			f.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
				if err := f.db.CreateUtxo(context.Background(), txn, &models.Utxo{
					TxId: *txId, OutputIdx: 0, AddedSlot: 1,
				}); err != nil {
					return err
				}
				return f.db.Blob().SetUtxo(txn.Blob(), *txId, 0, encoded)
			}),
		)
	}
	return f
}

func (f *requiredDatumFixture) witnessSetMap(
	withWitnessDatum bool,
) map[uint]any {
	ws := map[uint]any{}
	switch s := f.script.(type) {
	case lcommon.PlutusV1Script:
		ws[3] = []any{[]byte(s)}
	case lcommon.PlutusV2Script:
		ws[6] = []any{[]byte(s)}
	case lcommon.PlutusV3Script:
		ws[7] = []any{[]byte(s)}
	}
	if withWitnessDatum {
		ws[4] = []any{f.datum}
	}
	return ws
}

func (f *requiredDatumFixture) inputSet() cbor.Tag {
	return cbor.Tag{
		Number:  258,
		Content: []any{[]any{f.spendTxId, uint(0)}},
	}
}

func (f *requiredDatumFixture) collateralSet() cbor.Tag {
	return cbor.Tag{
		Number:  258,
		Content: []any{[]any{f.collateralTxId, uint(0)}},
	}
}

func conwayRequiredDatumBlock(
	t *testing.T,
	f *requiredDatumFixture,
	withWitnessDatum bool,
	valid bool,
	slot uint64,
) (*conway.ConwayBlock, *database.BlockIngestionResult) {
	t.Helper()
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0:  f.inputSet(),
			1:  []any{},
			2:  uint64(0),
			13: f.collateralSet(),
		},
		f.witnessSetMap(withWitnessDatum),
		valid,
		nil,
	})
	require.NoError(t, err)
	return conwayTestBlock(t, txCbor, uint(lcommon.ProtocolVersionPlomin), slot)
}

// conwayTestBlock wraps one encoded Conway transaction in a block at slot
// whose header announces major, and returns it with the transaction's offset.
func conwayTestBlock(
	t *testing.T,
	txCbor []byte,
	major uint,
	slot uint64,
) (*conway.ConwayBlock, *database.BlockIngestionResult) {
	t.Helper()
	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{},
		TransactionBodies: []conway.ConwayTransactionBody{
			tx.Body,
		},
		TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{
			tx.WitnessSet,
		},
	}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.ProtoVersion.Major = uint64(major)
	if !tx.IsValid() {
		block.InvalidTransactions = []uint{0}
	}
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	var txHash [32]byte
	copy(txHash[:], block.Transactions()[0].Hash().Bytes())
	return block, &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {
				BlockSlot:  slot,
				ByteLength: uint32(len(txCbor)),
			}, // #nosec G115
		},
	}
}

func applyRequiredDatumBlock(
	ls *LedgerState,
	block gledger.Block,
	slot uint64,
	reachesTip bool,
	offsets *database.BlockIngestionResult,
	era eras.EraDesc,
	pparams lcommon.ProtocolParameters,
) error {
	return ls.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(context.Background(),
			txn,
			ocommon.NewPoint(slot, block.Hash().Bytes()),
			block,
			true,
			reachesTip,
			false,
			nil,
			envelopeParent{origin: true},
			offsets,
			era,
			pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

func newRequiredDatumLedger(
	t *testing.T,
	db *database.Database,
	era eras.EraDesc,
) *LedgerState {
	t.Helper()
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	return &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{era},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
		currentEra: era,
	}
}

func TestLedgerProcessBlockConwayRejectsMissingRequiredSpendingDatum(
	t *testing.T,
) {
	t.Parallel()

	const slot = uint64(10)
	for _, tc := range []struct {
		name         string
		script       lcommon.Script
		datumHash    bool
		witnessDatum bool
		wantMissing  bool
	}{
		{name: "V1 datum hash without witness datum", script: lcommon.PlutusV1Script{0x41, 0x01}, datumHash: true, wantMissing: true},
		{name: "V2 datum hash without witness datum", script: lcommon.PlutusV2Script{0x41, 0x02}, datumHash: true, wantMissing: true},
		{name: "V1 script input without datum hash", script: lcommon.PlutusV1Script{0x41, 0x01}, wantMissing: true},
		{name: "V1 datum hash with witness datum", script: lcommon.PlutusV1Script{0x41, 0x01}, datumHash: true, witnessDatum: true},
		{name: "V3 script input without datum hash", script: lcommon.PlutusV3Script{0x41, 0x03}},
	} {
		for _, valid := range []bool{true, false} {
			for _, reachesTip := range []bool{false, true} {
				name := tc.name
				if valid {
					name += "/isValid=true"
				} else {
					name += "/isValid=false"
				}
				if reachesTip {
					name += "/live"
				} else {
					name += "/replay"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					f := newRequiredDatumFixture(t, tc.script, tc.datumHash)
					block, offsets := conwayRequiredDatumBlock(
						t, f, tc.witnessDatum, valid, slot,
					)
					pparams := &conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: lcommon.ProtocolVersionPlomin,
						},
						MaxBlockBodySize:   100_000,
						MaxBlockHeaderSize: 100_000,
						MaxTxExUnits: lcommon.ExUnits{
							Memory: 10_000_000, Steps: 100_000_000,
						},
					}
					ls := newRequiredDatumLedger(t, f.db, eras.ConwayEraDesc)
					err := applyRequiredDatumBlock(
						ls, block, slot, reachesTip, offsets,
						eras.ConwayEraDesc, pparams,
					)
					var missing lcommon.MissingDatumForSpendingScriptError
					if !tc.wantMissing {
						// The block must still fail inside transaction
						// validation, so an earlier block-level guard cannot
						// make the absence assertion pass vacuously.
						require.ErrorContains(t, err, "validation failure")
						require.False(
							t,
							errors.As(err, &missing),
							"datum requirement must not fire: %v", err,
						)
						return
					}
					require.ErrorAs(t, err, &missing)
					require.Equal(t, tc.script.Hash(), missing.ScriptHash)
				})
			}
		}
	}
}

func dijkstraRequiredDatumBlock(
	t *testing.T,
	f *requiredDatumFixture,
	inChild bool,
	withWitnessDatum bool,
	valid bool,
	slot uint64,
) (*gdijkstra.DijkstraBlock, *database.BlockIngestionResult) {
	t.Helper()
	input := shelley.NewShelleyTransactionInput(
		hex.EncodeToString(f.spendTxId), 0,
	)
	collateral := shelley.NewShelleyTransactionInput(
		hex.EncodeToString(f.collateralTxId), 0,
	)
	witnessSet := gdijkstra.DijkstraTransactionWitnessSet{}
	switch s := f.script.(type) {
	case lcommon.PlutusV1Script:
		witnessSet.WsPlutusV1Scripts = cbor.NewSetType(
			[]lcommon.PlutusV1Script{s}, false,
		)
	case lcommon.PlutusV2Script:
		witnessSet.WsPlutusV2Scripts = cbor.NewSetType(
			[]lcommon.PlutusV2Script{s}, false,
		)
	}
	if withWitnessDatum {
		witnessSet.WsPlutusData = cbor.NewSetType(
			[]lcommon.Datum{f.datum}, false,
		)
	}
	inputs := conway.NewConwayTransactionInputSet(
		[]shelley.ShelleyTransactionInput{input},
	)
	tx := gdijkstra.DijkstraTransaction{TxIsValid: valid}
	tx.Body.TxCollateral = cbor.NewSetType(
		[]shelley.ShelleyTransactionInput{collateral}, false,
	)
	if inChild {
		tx.Body.TxSubTransactions = cbor.NewSetType(
			[]gdijkstra.DijkstraSubTransaction{{
				Body: gdijkstra.DijkstraSubTransactionBody{
					TxInputs: inputs,
				},
				WitnessSet: witnessSet,
			}},
			false,
		)
	} else {
		tx.Body.TxInputs = inputs
		tx.WitnessSet = witnessSet
	}
	block := &gdijkstra.DijkstraBlock{
		BlockHeader: &gdijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        slot,
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			},
		},
		BlockBody: gdijkstra.DijkstraBlockBody{
			Transactions: []gdijkstra.DijkstraTransaction{tx},
		},
	}
	if !valid {
		block.BlockBody.InvalidTransactions = []uint{0}
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	var txHash [32]byte
	copy(txHash[:], block.Transactions()[0].Hash().Bytes())
	return block, &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {BlockSlot: slot, ByteLength: 1},
		},
	}
}

func TestLedgerProcessBlockDijkstraRejectsMissingRequiredSpendingDatum(
	t *testing.T,
) {
	t.Parallel()

	const slot = uint64(10)
	for _, tc := range []struct {
		name         string
		script       lcommon.Script
		inChild      bool
		witnessDatum bool
		wantMissing  bool
	}{
		{name: "V1 top-level missing datum", script: lcommon.PlutusV1Script{0x41, 0x01}, wantMissing: true},
		{name: "V2 top-level missing datum", script: lcommon.PlutusV2Script{0x41, 0x02}, wantMissing: true},
		{name: "V1 child subtransaction missing datum", script: lcommon.PlutusV1Script{0x41, 0x01}, inChild: true, wantMissing: true},
		{name: "V1 top-level datum present", script: lcommon.PlutusV1Script{0x41, 0x01}, witnessDatum: true},
		{name: "V1 child subtransaction datum present", script: lcommon.PlutusV1Script{0x41, 0x01}, inChild: true, witnessDatum: true},
	} {
		for _, valid := range []bool{true, false} {
			for _, reachesTip := range []bool{false, true} {
				name := tc.name
				if valid {
					name += "/isValid=true"
				} else {
					name += "/isValid=false"
				}
				if reachesTip {
					name += "/live"
				} else {
					name += "/replay"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					f := newRequiredDatumFixture(t, tc.script, true)
					block, offsets := dijkstraRequiredDatumBlock(
						t, f, tc.inChild, tc.witnessDatum, valid, slot,
					)
					pparams := dijkstraTestProtocolParameters()
					pparams.MaxBlockBodySize = 100_000
					pparams.MaxBlockHeaderSize = 100_000
					ls := newRequiredDatumLedger(t, f.db, eras.DijkstraEraDesc)
					err := applyRequiredDatumBlock(
						ls, block, slot, reachesTip, offsets,
						eras.DijkstraEraDesc,
						pparams,
					)
					var missing lcommon.MissingDatumForSpendingScriptError
					if !tc.wantMissing {
						// The block must still fail inside transaction
						// validation, so an earlier block-level guard cannot
						// make the absence assertion pass vacuously.
						require.ErrorContains(t, err, "validation failure")
						require.False(
							t,
							errors.As(err, &missing),
							"datum requirement must not fire: %v", err,
						)
						return
					}
					require.ErrorAs(t, err, &missing)
					require.Equal(t, tc.script.Hash(), missing.ScriptHash)
				})
			}
		}
	}
}
