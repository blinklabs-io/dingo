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
	"encoding/hex"
	"errors"
	"fmt"
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

// additionalReferenceLedger returns a ledger whose only transaction validator
// fails with the returned sentinel, so a block that clears the aggregate
// reference-script rule is observable as reaching it. It encodes real era
// bodies so envelope admission reaches the aggregate rule.
func additionalReferenceLedger(
	t *testing.T,
	db *database.Database,
	block gledger.Block,
	era eras.EraDesc,
	pp lcommon.ProtocolParameters,
) (*LedgerState, error) {
	t.Helper()
	encode := func() {
		encoded, err := cbor.EncodeGeneric(block)
		require.NoError(t, err)
		switch b := block.(type) {
		case *babbage.BabbageBlock:
			b.SetCbor(encoded)
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
	case *babbage.BabbageBlock:
		b.BlockHeader.Body.BlockBodySize = size
		b.SetCbor(nil)
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
	return ls, sentinel
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
	ls, sentinel := additionalReferenceLedger(t, db, block, era, pp)
	for path, run := range map[string]func() error{
		"imported": func() error {
			return db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(t.Context(), txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, ls.currentEra, pp, nil, 0, 0, false)
				return err
			})
		},
		"forged": func() error { return ls.validateForgedTxs(context.Background(), block) },
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
					db.Transaction(context.Background(), true).
						Do(func(txn *database.Txn) error {
							if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{TxId: id}); err != nil {
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
				tc.invalidProducer || tc.missing,
			)
		})
	}
}

type referenceScriptStateFunc func(
	lcommon.TransactionInput,
) (lcommon.Utxo, error)

func (f referenceScriptStateFunc) UtxoById(
	input lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	return f(input)
}

func TestPV10ReferenceScriptStateOnlySuppressesMissing(t *testing.T) {
	input := &shelley.ShelleyTransactionInput{}
	wrappedMissing := fmt.Errorf("lookup: %w", database.ErrUtxoNotFound)
	state := pv10ReferenceScriptState{
		state: referenceScriptStateFunc(
			func(lcommon.TransactionInput) (lcommon.Utxo, error) {
				return lcommon.Utxo{}, wrappedMissing
			},
		),
	}
	utxo, err := state.UtxoById(input)
	require.NoError(t, err)
	require.Equal(t, lcommon.Utxo{}, utxo)

	storageErr := errors.New("database unavailable")
	state.state = referenceScriptStateFunc(
		func(lcommon.TransactionInput) (lcommon.Utxo, error) {
			return lcommon.Utxo{}, storageErr
		},
	)
	_, err = state.UtxoById(input)
	require.ErrorIs(t, err, storageErr)
}

func TestPV10AggregateReferenceScriptLookupErrors(t *testing.T) {
	newBlock := func() *conway.ConwayBlock {
		return &conway.ConwayBlock{
			BlockHeader: &conway.ConwayBlockHeader{},
			TransactionBodies: []conway.ConwayTransactionBody{{
				TxReferenceInputs: cbor.NewSetType(
					[]shelley.ShelleyTransactionInput{{
						TxId: lcommon.NewBlake2b256(
							bytes.Repeat([]byte{7}, 32),
						),
					}},
					false,
				),
			}},
			TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{{}},
		}
	}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 10},
	}
	missing := referenceScriptStateFunc(
		func(lcommon.TransactionInput) (lcommon.Utxo, error) {
			return lcommon.Utxo{}, fmt.Errorf(
				"lookup wrapper: %w",
				database.ErrUtxoNotFound,
			)
		},
	)
	require.NoError(t, validateBlockReferenceScripts(newBlock(), pp, missing))

	decodeErr := errors.New("reference output decode failed")
	failing := referenceScriptStateFunc(
		func(lcommon.TransactionInput) (lcommon.Utxo, error) {
			return lcommon.Utxo{}, decodeErr
		},
	)
	require.ErrorIs(
		t,
		validateBlockReferenceScripts(newBlock(), pp, failing),
		decodeErr,
	)

	pp.ProtocolVersion.Major = 11
	require.ErrorContains(
		t,
		validateBlockReferenceScripts(newBlock(), pp, missing),
		"resolve consumed reference-script input",
	)
}

func TestValidateBlockReferenceScriptsEntryControls(t *testing.T) {
	t.Run("nil_and_empty", func(t *testing.T) {
		ls := &LedgerState{}
		require.ErrorContains(
			t,
			ls.ValidateBlockReferenceScripts(context.Background(), nil),
			"nil block",
		)
		require.NoError(
			t,
			ls.ValidateBlockReferenceScripts(
				context.Background(),
				&conway.ConwayBlock{},
			),
		)
		require.NoError(
			t,
			ls.ValidateBlockReferenceScripts(
				context.Background(),
				&dijkstra.DijkstraBlock{},
			),
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
				ls.activeEras = eras.ErasWithDijkstra
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
				require.NoError(
					t,
					ls.ValidateBlockReferenceScripts(
						context.Background(),
						block,
					),
				)
				return
			}
			require.ErrorContains(
				t,
				ls.ValidateBlockReferenceScripts(context.Background(), block),
				"resolve consumed reference-script input",
				"Conway input lookup must not bypass aggregate validation or use Dijkstra parameters",
			)
		})
	}
}

// TestBlockReferenceScriptAggregateReusesPrefetchedUtxos checks that the
// aggregate reference-script rule and the per-transaction validators resolve a
// block's reference inputs once between them: both read from the block's UTxO
// prefetch instead of querying each input themselves.
func TestBlockReferenceScriptAggregateReusesPrefetchedUtxos(t *testing.T) {
	t.Parallel()

	seed := func(t *testing.T, db *database.Database) []lcommon.TransactionInput {
		t.Helper()
		var inputs []lcommon.TransactionInput
		for i := range 2 {
			id := bytes.Repeat([]byte{byte(i + 1)}, 32)
			encoded, err := cbor.Encode(additionalReferenceOutput(t, 10))
			require.NoError(t, err)
			require.NoError(
				t,
				db.Transaction(t.Context(), true).
					Do(func(txn *database.Txn) error {
						if err := db.CreateUtxo(t.Context(), txn, &models.Utxo{TxId: id}); err != nil {
							return err
						}
						return db.Blob().SetUtxo(txn.Blob(), id, 0, encoded)
					}),
			)
			inputs = append(
				inputs,
				shelley.NewShelleyTransactionInput(hex.EncodeToString(id), 0),
			)
		}
		return inputs
	}
	// One transaction references every input: a transaction is recorded as
	// soon as it validates, so only the first validator can run here.
	refInputs := func(inputs []lcommon.TransactionInput) cbor.SetType[shelley.ShelleyTransactionInput] {
		var refs []shelley.ShelleyTransactionInput
		for _, input := range inputs {
			refs = append(refs, shelley.ShelleyTransactionInput{
				TxId: input.Id(), OutputIndex: input.Index(),
			})
		}
		return cbor.NewSetType(refs, false)
	}
	for _, tc := range []struct {
		name  string
		desc  eras.EraDesc
		pp    lcommon.ProtocolParameters
		block func(inputs []lcommon.TransactionInput) gledger.Block
	}{
		{
			name: "conway",
			desc: eras.ConwayEraDesc,
			pp: &conway.ConwayProtocolParameters{
				MaxBlockBodySize: 2_000_000, MaxBlockHeaderSize: 100_000,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 11},
			},
			block: func(inputs []lcommon.TransactionInput) gledger.Block {
				block := &conway.ConwayBlock{BlockHeader: &conway.ConwayBlockHeader{}}
				block.BlockHeader.Body.BlockNumber = 1
				block.BlockHeader.Body.Slot = 1
				block.BlockHeader.Body.ProtoVersion.Major = 11
				block.TransactionBodies = []conway.ConwayTransactionBody{
					{TxReferenceInputs: refInputs(inputs)},
				}
				block.TransactionWitnessSets = []conway.ConwayTransactionWitnessSet{{}}
				return block
			},
		},
		{
			name: "dijkstra",
			desc: eras.DijkstraEraDesc,
			pp: &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					MaxBlockBodySize: 2_000_000, MaxBlockHeaderSize: 100_000,
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 12},
				},
				MaxRefScriptSizePerBlock: 120,
				MaxRefScriptSizePerTx:    100,
			},
			block: func(inputs []lcommon.TransactionInput) gledger.Block {
				block := &dijkstra.DijkstraBlock{BlockHeader: &dijkstra.DijkstraBlockHeader{}}
				block.BlockHeader.Body.BlockNumber = 1
				block.BlockHeader.Body.Slot = 1
				block.BlockHeader.Body.ProtoVersion.Major = 12
				block.BlockBody.Transactions = []dijkstra.DijkstraTransaction{{
					TxIsValid: true,
					Body:      dijkstra.DijkstraTransactionBody{TxReferenceInputs: refInputs(inputs)},
				}}
				return block
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			inputs := seed(t, db)
			block := tc.block(inputs)
			ls, sentinel := additionalReferenceLedger(
				t,
				db,
				block,
				tc.desc,
				tc.pp,
			)
			// The transaction validator resolves its reference inputs through
			// the state it is handed, so the read count covers both checks.
			resolved := 0
			ls.currentEra.ValidateTxFunc = func(
				tx lcommon.Transaction,
				_ uint64,
				state lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				for _, input := range tx.ReferenceInputs() {
					if _, err := state.UtxoById(input); err != nil {
						return err
					}
					resolved++
				}
				return sentinel
			}
			ls.activeEras = []eras.EraDesc{ls.currentEra}

			err := db.Transaction(t.Context(), true).
				Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(
						t.Context(),
						txn,
						ocommon.NewPoint(1, block.Hash().Bytes()),
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{origin: true},
						nil,
						ls.currentEra,
						tc.pp,
						nil,
						0,
						0,
						false,
					)
					return err
				})
			require.ErrorIs(t, err, sentinel)
			require.Equal(t, len(inputs), resolved)
			require.Equal(t, uint64(len(inputs)), ls.utxoByRefReads.Load(),
				"each reference input must be read from the database once")
			require.Equal(t, uint64(1), ls.utxoBatchLookups.Load())
		})
	}
}

// TestBlockReferenceScriptBudgetDoesNotApplyBeforeConway checks that a
// pre-Conway block whose reference inputs cannot be resolved is not rejected by
// the aggregate reference-script budget, on the imported and forged paths. The
// budget is a Conway rule, so Babbage transactions reach their own validator.
func TestBlockReferenceScriptBudgetDoesNotApplyBeforeConway(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	block := &babbage.BabbageBlock{BlockHeader: &babbage.BabbageBlockHeader{}}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = 1
	block.BlockHeader.Body.ProtoVersion.Major = 8
	block.TransactionBodies = []babbage.BabbageTransactionBody{{
		TxReferenceInputs: cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{
				{TxId: lcommon.NewBlake2b256(bytes.Repeat([]byte{7}, 32))},
			},
			false,
		),
	}}
	block.TransactionWitnessSets = []babbage.BabbageTransactionWitnessSet{{}}
	additionalReferenceAdmission(
		t,
		db,
		block,
		eras.BabbageEraDesc,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor:    8,
			MaxBlockBodySize: 2_000_000, MaxBlockHeaderSize: 100_000,
		},
		0,
		false,
	)
}

// TestBlockReferenceScriptsUsePreviousEraParametersOnImport checks that
// block application judges a block of the previous era under the previous
// era's parameters. The Conway rule rejects any other parameter shape, so
// selecting the current Dijkstra parameters would reject the block.
func TestBlockReferenceScriptsUsePreviousEraParametersOnImport(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	block := &conway.ConwayBlock{BlockHeader: &conway.ConwayBlockHeader{}}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = 1
	block.BlockHeader.Body.ProtoVersion.Major = 11
	conwayPParams := &conway.ConwayProtocolParameters{
		MaxBlockBodySize: 2_000_000, MaxBlockHeaderSize: 100_000,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 11},
	}
	dijkstraPParams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: *conwayPParams,
		MaxRefScriptSizePerBlock: 120,
	}
	ls, _ := additionalReferenceLedger(
		t,
		db,
		block,
		eras.DijkstraEraDesc,
		dijkstraPParams,
	)
	ls.activeEras = []eras.EraDesc{eras.ConwayEraDesc, ls.currentEra}

	require.NoError(
		t,
		db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(t.Context(),
				txn, ocommon.NewPoint(1, block.Hash().Bytes()), block,
				true, false, false, nil, envelopeParent{origin: true}, nil,
				ls.currentEra, dijkstraPParams, conwayPParams, 0, 0, false,
			)
			return err
		}),
	)
}

// TestDijkstraBlockReferenceScriptsWithConwayParameters pins the stricter
// lookup a Dijkstra block gets when handed Conway-shaped parameters at an
// earlier protocol major. Conway tolerates a missing reference input there;
// Dijkstra does not. Dijkstra blocks do not exist before major 12, so the
// asymmetry is unreachable on a chain and rejecting is the safe behaviour.
func TestDijkstraBlockReferenceScriptsWithConwayParameters(t *testing.T) {
	t.Parallel()

	missing := shelley.ShelleyTransactionInput{
		TxId: lcommon.NewBlake2b256(bytes.Repeat([]byte{9}, 32)),
	}
	refs := cbor.NewSetType([]shelley.ShelleyTransactionInput{missing}, false)
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 10},
	}
	state := referenceScriptStateFunc(
		func(lcommon.TransactionInput) (lcommon.Utxo, error) {
			return lcommon.Utxo{}, database.ErrUtxoNotFound
		},
	)
	conwayBlock := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{},
		TransactionBodies: []conway.ConwayTransactionBody{
			{TxReferenceInputs: refs},
		},
		TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{{}},
	}
	require.NoError(t, validateBlockReferenceScripts(conwayBlock, pp, state))

	dijkstraBlock := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{},
	}
	dijkstraBlock.BlockBody.Transactions = []dijkstra.DijkstraTransaction{{
		TxIsValid: true,
		Body:      dijkstra.DijkstraTransactionBody{TxReferenceInputs: refs},
	}}
	require.ErrorContains(
		t,
		validateBlockReferenceScripts(dijkstraBlock, pp, state),
		"resolve consumed reference-script input",
	)
}

// TestReferenceScriptParamsFollowEraListPredecessor checks that the aggregate
// check picks the previous era's parameters by the same rule
// resolveValidationEra applies to the block's transactions: the block's era is
// the one listed immediately before the ledger's era, whatever its numeric ID.
func TestReferenceScriptParamsFollowEraListPredecessor(t *testing.T) {
	t.Parallel()

	block := &conway.ConwayBlock{BlockHeader: &conway.ConwayBlockHeader{}}
	current := &dijkstra.DijkstraProtocolParameters{}
	previous := &conway.ConwayProtocolParameters{}
	ledgerEra := eras.EraDesc{Id: uint(conway.EraIdConway) + 2}
	for _, tc := range []struct {
		name    string
		eraList []eras.EraDesc
		want    lcommon.ProtocolParameters
	}{
		{
			name:    "listed predecessor with a non-adjacent ID",
			eraList: []eras.EraDesc{eras.ConwayEraDesc, ledgerEra},
			want:    previous,
		},
		{
			name: "two eras behind",
			eraList: []eras.EraDesc{
				eras.ConwayEraDesc, {Id: uint(conway.EraIdConway) + 1}, ledgerEra,
			},
			want: current,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := referenceScriptParams(
				block, ledgerEra, tc.eraList, current, previous,
			)
			require.Same(t, tc.want, got)
		})
	}
}
