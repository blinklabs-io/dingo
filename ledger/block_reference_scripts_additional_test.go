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
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func additionalReferenceOutput(
	t *testing.T,
	size int,
) *babbage.BabbageTransactionOutput {
	t.Helper()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		0,
		bytes.Repeat([]byte{1}, 28),
		nil,
	)
	require.NoError(t, err)
	return &babbage.BabbageTransactionOutput{
		OutputAddress: address,
		TxOutScriptRef: &lcommon.ScriptRef{
			Type: lcommon.ScriptRefTypePlutusV3, Script: make(lcommon.PlutusV3Script, size),
		},
	}
}

func additionalReferenceAdmission(
	t *testing.T,
	db *database.Database,
	block gledger.Block,
	era eras.EraDesc,
	pp lcommon.ProtocolParameters,
	wantSize uint64,
	wantLookupError bool,
) {
	t.Helper()
	// Encode real era bodies so envelope admission reaches the aggregate rule.
	encode := func() {
		encoded, err := cbor.EncodeGeneric(block)
		require.NoError(t, err)
		switch b := block.(type) {
		case *conway.ConwayBlock:
			b.SetCbor(encoded)
		case *dijkstra.DijkstraBlock:
			b.SetCbor(encoded)
		}
	}
	encode()
	size, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	switch b := block.(type) {
	case *conway.ConwayBlock:
		b.BlockHeader.Body.BlockBodySize = size
		b.SetCbor(nil)
	case *dijkstra.DijkstraBlock:
		b.BlockHeader.Body.BlockBodySize = size
		b.SetCbor(nil)
	}
	encode()
	sentinel := errors.New("later transaction validator reached")
	era.ValidateTxFunc = func(lcommon.Transaction, uint64, lcommon.LedgerState, lcommon.ProtocolParameters) error {
		return sentinel
	}
	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		db:             db,
		activeEras:     []eras.EraDesc{era},
		currentEra:     era,
		currentPParams: pp,
		config: LedgerStateConfig{
			Logger:            testLogger(),
			CardanoNodeConfig: cfg,
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	for path, run := range map[string]func() error{
		"imported": func() error {
			return db.Transaction(true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, era, pp, nil, 0)
				return err
			})
		},
		"forged": func() error { return ls.validateForgedTxs(block) },
	} {
		t.Run(path, func(t *testing.T) {
			err := run()
			switch {
			case wantLookupError:
				require.ErrorContains(
					t,
					err,
					"resolve consumed reference-script input",
					"reference lookup failure must reject before transaction validation",
				)
			case wantSize != 0:
				var limit lcommon.RefScriptSizePerBlockTooLargeError
				require.ErrorAs(
					t,
					err,
					&limit,
					"aggregate rule must reject before transaction validation",
				)
				require.Equal(t, wantSize, limit.BlockSize)
			default:
				require.ErrorIs(
					t,
					err,
					sentinel,
					"valid aggregate must reach later transaction validation",
				)
			}
		})
	}
}

func TestDijkstraBlockReferenceScriptCustomLimitAdmission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lastSize int
		invalid  bool
	}{
		{"below", 59, false}, {"exact", 60, false}, {"above", 61, false}, {"phase_invalid_exact", 60, true}, {"phase_invalid_above", 61, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			block := &dijkstra.DijkstraBlock{
				BlockHeader: &dijkstra.DijkstraBlockHeader{},
			}
			block.BlockHeader.Body.BlockNumber = 1
			block.BlockHeader.Body.Slot = 1
			block.BlockHeader.Body.ProtoVersion.Major = 12
			for i, size := range []int{60, tc.lastSize} {
				id := bytes.Repeat([]byte{byte(i + 1)}, 32)
				encoded, err := cbor.Encode(additionalReferenceOutput(t, size))
				require.NoError(t, err)
				require.NoError(
					t,
					db.Transaction(true).Do(func(txn *database.Txn) error {
						if err := db.CreateUtxo(txn, &models.Utxo{TxId: id}); err != nil {
							return err
						}
						return db.Blob().SetUtxo(txn.Blob(), id, 0, encoded)
					}),
				)
				block.BlockBody.Transactions = append(
					block.BlockBody.Transactions,
					dijkstra.DijkstraTransaction{
						TxIsValid: !(tc.invalid && i == 1),
						Body: dijkstra.DijkstraTransactionBody{
							TxReferenceInputs: cbor.NewSetType(
								[]shelley.ShelleyTransactionInput{
									{TxId: lcommon.NewBlake2b256(id)},
								},
								false,
							),
						},
					},
				)
			}
			if tc.invalid {
				block.BlockBody.InvalidTransactions = []uint{1}
			}
			pp := &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					MaxBlockBodySize: 2000000, MaxBlockHeaderSize: 100000,
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: 12,
					},
				},
				MaxRefScriptSizePerBlock: 120,
				MaxRefScriptSizePerTx:    100,
			}
			var want uint64
			if tc.lastSize > 60 {
				want = uint64(60 + tc.lastSize)
			}
			additionalReferenceAdmission(
				t,
				db,
				block,
				eras.DijkstraEraDesc,
				pp,
				want,
				false,
			)
		})
	}
}

func TestBlockReferenceScriptProducedOutputAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		proto                          uint
		over, invalidProducer, missing bool
	}{
		{name: "pv11_below", proto: 11}, {name: "pv11_above", proto: 11, over: true},
		{name: "pv10_no_overlay", proto: 10}, {name: "invalid_producer", proto: 11, invalidProducer: true},
		{name: "missing_reference", proto: 11, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			size := int(conway.MaxRefScriptSizePerBlock / 6)
			if tc.over {
				size++
			}
			require.Less(t, uint64(size), conway.MaxRefScriptSizePerTx)
			block := &conway.ConwayBlock{
				BlockHeader: &conway.ConwayBlockHeader{},
			}
			block.BlockHeader.Body.BlockNumber = 1
			block.BlockHeader.Body.Slot = 1
			block.BlockHeader.Body.ProtoVersion.Major = uint64(tc.proto)
			block.TransactionBodies = []conway.ConwayTransactionBody{
				{
					TxOutputs: []babbage.BabbageTransactionOutput{
						*additionalReferenceOutput(t, size),
					},
				},
			}
			block.TransactionWitnessSets = []conway.ConwayTransactionWitnessSet{
				{},
			}
			id := block.Transactions()[0].Hash()
			if tc.missing {
				id = lcommon.NewBlake2b256(bytes.Repeat([]byte{9}, 32))
			}
			for i := range 6 {
				block.TransactionBodies = append(
					block.TransactionBodies,
					conway.ConwayTransactionBody{
						TxFee: uint64(i),
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{{TxId: id}},
							false,
						),
					},
				)
				block.TransactionWitnessSets = append(
					block.TransactionWitnessSets,
					conway.ConwayTransactionWitnessSet{},
				)
			}
			if tc.invalidProducer {
				block.InvalidTransactions = []uint{0}
			}
			pp := &conway.ConwayProtocolParameters{
				MaxBlockBodySize:   2000000,
				MaxBlockHeaderSize: 100000,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: tc.proto,
				},
			}
			var want uint64
			if tc.over {
				want = uint64(size * 6)
			}
			additionalReferenceAdmission(
				t,
				db,
				block,
				eras.ConwayEraDesc,
				pp,
				want,
				tc.proto < 11 || tc.invalidProducer || tc.missing,
			)
		})
	}
}

func TestValidateBlockReferenceScriptsEntryControls(t *testing.T) {
	t.Run("nil_and_empty", func(t *testing.T) {
		ls := &LedgerState{}
		require.ErrorContains(
			t,
			ls.ValidateBlockReferenceScripts(nil),
			"nil block",
		)
		require.NoError(
			t,
			ls.ValidateBlockReferenceScripts(&conway.ConwayBlock{}),
		)
		require.NoError(
			t,
			ls.ValidateBlockReferenceScripts(&dijkstra.DijkstraBlock{}),
		)
	})
	for _, tc := range []struct {
		name                string
		previous, prototype bool
	}{
		{name: "conway"}, {name: "conway_prototype_flag", prototype: true},
		{name: "previous_era_parameters", previous: true},
		{name: "active_dijkstra_prototype_compatibility", previous: true, prototype: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			block := &conway.ConwayBlock{
				BlockHeader: &conway.ConwayBlockHeader{},
				TransactionBodies: []conway.ConwayTransactionBody{
					{
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{
								{
									TxId: lcommon.NewBlake2b256(
										bytes.Repeat([]byte{7}, 32),
									),
								},
							},
							false,
						),
					},
				},
				TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{
					{},
				},
			}
			pp := &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 11,
				},
			}
			ls := &LedgerState{
				db:             db,
				currentEra:     eras.ConwayEraDesc,
				currentPParams: pp,
				config: LedgerStateConfig{
					SkipDijkstraTxValidation: tc.prototype,
				},
			}
			if tc.previous {
				ls.currentEra = eras.DijkstraEraDesc
				ls.currentPParams = &dijkstra.DijkstraProtocolParameters{
					ConwayProtocolParameters: *pp,
					MaxRefScriptSizePerBlock: 120,
				}
				ls.prevEraPParams = pp
			}
			ls.publishSnapshotsLocked()
			if tc.previous && tc.prototype {
				// The active Dijkstra prototype may decode blocks using
				// the concrete Conway type; its explicit bypass policy
				// follows the active era rather than that block type.
				require.NoError(t, ls.ValidateBlockReferenceScripts(block))
				return
			}
			require.ErrorContains(
				t,
				ls.ValidateBlockReferenceScripts(block),
				"resolve consumed reference-script input",
				"Conway input lookup must not bypass aggregate validation or use Dijkstra parameters",
			)
		})
	}
}
