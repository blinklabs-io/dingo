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
	"maps"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	feeProdMinFeeA = 44
	feeProdMinFeeB = 155_381
	feeProdUtxo    = uint64(50_000_000)
)

// feeProdBudget costs a whole number of lovelace plus one half at
// feeProdPrices, so one ceiling over two such budgets is a lovelace less than
// rounding each separately. It covers a script that only takes its arguments.
var feeProdBudget = lcommon.ExUnits{Memory: 10_001, Steps: 1_000_000}

func feeProdPrices() lcommon.ExUnitPrice {
	half := &cbor.Rat{Rat: big.NewRat(1, 2)}
	return lcommon.ExUnitPrice{MemPrice: half, StepPrice: half}
}

// feeProdExecFee is an independent oracle for the execution component: one
// ceiling over the sum of every declared budget at feeProdPrices.
func feeProdExecFee(budgets ...lcommon.ExUnits) uint64 {
	var total int64
	for _, budget := range budgets {
		total += budget.Memory + budget.Steps
	}
	return uint64((total + 1) / 2) // #nosec G115 -- small test budgets
}

func feeProdSizeFee(t *testing.T, tx lcommon.Transaction) uint64 {
	t.Helper()
	size, err := lcommon.TxSizeForFee(tx)
	require.NoError(t, err)
	return uint64(size)*feeProdMinFeeA + feeProdMinFeeB // #nosec G115
}

// feeProdScriptBytes is a validator that takes its script context and
// returns unit, or fails when fails is set.
func feeProdScriptBytes(t *testing.T, fails bool) []byte {
	t.Helper()
	var body syn.Term[syn.DeBruijn] = &syn.Constant{Con: &syn.Unit{}}
	if fails {
		body = &syn.Error{}
	}
	flat, err := syn.Encode(&syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: body},
	})
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flat)
	require.NoError(t, err)
	return scriptBytes
}

type feeProdFixture struct {
	db      *database.Database
	key     ed25519.PrivateKey
	keyHash lcommon.Blake2b224
	keyAddr []byte
}

func newFeeProdFixture(t *testing.T) *feeProdFixture {
	t.Helper()
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xf5}, ed25519.SeedSize))
	keyHash := lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey))
	return &feeProdFixture{
		db:      newTestDB(t),
		key:     key,
		keyHash: keyHash,
		keyAddr: feeProdAddress(t, lcommon.AddressTypeKeyNone, keyHash.Bytes()),
	}
}

func feeProdAddress(t *testing.T, addrType uint8, hash []byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		addrType, lcommon.AddressNetworkTestnet, hash, nil,
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	return addrBytes
}

// seed stores a feeProdUtxo output at addr under a transaction id of repeated
// b, with datumHash when it is not nil, and returns that id.
func (f *feeProdFixture) seed(
	t *testing.T,
	b byte,
	addr []byte,
	datumHash []byte,
) []byte {
	t.Helper()
	output := map[uint]any{0: addr, 1: feeProdUtxo}
	if datumHash != nil {
		output[2] = []any{0, datumHash}
	}
	return f.seedOutput(t, b, output)
}

// seedReference stores a key-locked output carrying a Plutus V4 reference
// script and returns its transaction id.
func (f *feeProdFixture) seedReference(
	t *testing.T,
	b byte,
	script lcommon.PlutusV4Script,
) []byte {
	t.Helper()
	scriptRef, err := cbor.Encode([]any{uint(4), []byte(script)})
	require.NoError(t, err)
	return f.seedOutput(t, b, map[uint]any{
		0: f.keyAddr,
		1: feeProdUtxo,
		3: cbor.Tag{Number: 24, Content: scriptRef},
	})
}

func (f *feeProdFixture) seedOutput(
	t *testing.T,
	b byte,
	output map[uint]any,
) []byte {
	t.Helper()
	id := bytes.Repeat([]byte{b}, lcommon.Blake2b256Size)
	encoded, err := cbor.Encode(output)
	require.NoError(t, err)
	require.NoError(t, f.db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := f.db.CreateUtxo(txn, &models.Utxo{
			TxId: id, OutputIdx: 0, AddedSlot: 0,
		}); err != nil {
			return err
		}
		return f.db.Blob().SetUtxo(txn.Blob(), id, 0, encoded)
	}))
	return id
}

// feeProdSpend is a signed Babbage or Conway transaction spending one input
// into one key output worth the input less the fee, plus balance.
type feeProdSpend struct {
	input      []byte
	collateral []byte
	// script is the spent input's validator; nil makes a key spend with no
	// redeemer.
	script lcommon.Script
	datum  *lcommon.Datum
	budget lcommon.ExUnits
	fee    uint64
	// balance is negative for deposits paid and positive for refunds claimed.
	balance    int64
	extra      map[uint]any
	isValid    bool
	costModels map[uint][]int64
	conway     bool
}

func (f *feeProdFixture) spendCbor(t *testing.T, s feeProdSpend) []byte {
	t.Helper()
	set := func(items ...any) any {
		if s.conway {
			return cbor.Tag{Number: 258, Content: items}
		}
		return items
	}
	// #nosec G115 -- small test amounts
	output := int64(feeProdUtxo) - int64(s.fee) + s.balance
	require.Positive(t, output)
	body := map[uint]any{
		0: set([]any{s.input, uint(0)}),
		1: []any{map[uint]any{0: f.keyAddr, 1: uint64(output)}},
		2: s.fee,
	}
	maps.Copy(body, s.extra)
	witnessSet := map[uint]any{}
	if s.script != nil {
		body[13] = set([]any{s.collateral, uint(0)})
		redeemerData := decodeRequiredDatum(t)
		// #nosec G115 -- small test budgets
		units := []uint64{uint64(s.budget.Memory), uint64(s.budget.Steps)}
		spend := uint64(lcommon.RedeemerTagSpend)
		var redeemers any = []any{[]any{spend, uint64(0), redeemerData, units}}
		if s.conway {
			redeemers = map[any]any{
				[2]uint64{spend, 0}: []any{redeemerData, units},
			}
		}
		redeemersCbor, err := cbor.Encode(redeemers)
		require.NoError(t, err)
		witnessSet[5] = cbor.RawMessage(redeemersCbor)
		var datumsCbor []byte
		if s.datum != nil {
			datumsCbor, err = cbor.Encode([]any{*s.datum})
			require.NoError(t, err)
			witnessSet[4] = cbor.RawMessage(datumsCbor)
		}
		var version uint
		switch script := s.script.(type) {
		case lcommon.PlutusV1Script:
			version = 0
			witnessSet[3] = []any{[]byte(script)}
		case lcommon.PlutusV3Script:
			version = 2
			witnessSet[7] = []any{[]byte(script)}
		default:
			t.Fatalf("unsupported script %T", s.script)
		}
		langViews, err := lcommon.EncodeLangViews(
			map[uint]struct{}{version: {}},
			s.costModels,
		)
		require.NoError(t, err)
		integrity := lcommon.Blake2b256Hash(
			bytes.Join([][]byte{redeemersCbor, datumsCbor, langViews}, nil),
		)
		body[11] = integrity.Bytes()
	}
	bodyCbor, err := cbor.Encode(body)
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(bodyCbor)
	witnessSet[0] = []any{[]any{
		[]byte(f.key.Public().(ed25519.PublicKey)),
		ed25519.Sign(f.key, bodyHash.Bytes()),
	}}
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(bodyCbor), witnessSet, s.isValid, nil},
	)
	require.NoError(t, err)
	return txCbor
}

func babbageFeeProdBlock(
	t *testing.T,
	tx *babbage.BabbageTransaction,
) (*babbage.BabbageBlock, *database.BlockIngestionResult) {
	t.Helper()
	const slot = uint64(10)
	block := &babbage.BabbageBlock{
		BlockHeader:       &babbage.BabbageBlockHeader{},
		TransactionBodies: []babbage.BabbageTransactionBody{tx.Body},
		TransactionWitnessSets: []babbage.BabbageTransactionWitnessSet{
			tx.WitnessSet,
		},
	}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.ProtoVersion.Major = 8
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	return block, feeProdOffsets(t, block)
}

func feeProdOffsets(
	t *testing.T,
	block gledger.Block,
) *database.BlockIngestionResult {
	t.Helper()
	offsets, err := database.NewBlockIndexer(
		block.SlotNumber(), block.Hash().Bytes(),
	).ComputeOffsets(block.Cbor(), block)
	require.NoError(t, err)
	return offsets
}

// feeProdCase is one transaction and the block carrying it, built over a
// fresh database seeded with its inputs.
type feeProdCase struct {
	fx        *feeProdFixture
	era       eras.EraDesc
	blockType uint
	txType    uint
	pparams   lcommon.ProtocolParameters
	tx        lcommon.Transaction
	txCbor    []byte
	block     gledger.Block
	offsets   *database.BlockIngestionResult
}

func (c feeProdCase) ledger(t *testing.T) *LedgerState {
	t.Helper()
	return newReplayTestLedger(
		t, c.fx.db, c.block, c.blockType, c.era, c.pparams,
	)
}

func (c feeProdCase) applyBlock(ls *LedgerState) error {
	return ls.db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.NewPoint(c.block.SlotNumber(), c.block.Hash().Bytes()),
			c.block,
			true,
			true,
			false,
			nil,
			envelopeParent{origin: true},
			c.offsets,
			c.era,
			c.pparams,
			nil,
			0,
			0,
			false,
		)
		return err
	})
}

type feeProdPath struct {
	name string
	run  func(t *testing.T, c feeProdCase) error
}

var (
	feeProdLedgerAdmission = feeProdPath{
		name: "ledger admission",
		run: func(t *testing.T, c feeProdCase) error {
			return c.ledger(t).ValidateTx(c.tx)
		},
	}
	feeProdMempoolAdmission = feeProdPath{
		name: "mempool admission",
		run: func(t *testing.T, c feeProdCase) error {
			pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
				Validator:       c.ledger(t),
				Logger:          testLogger(),
				PromRegistry:    prometheus.NewRegistry(),
				MempoolCapacity: 1024 * 1024,
			})
			require.NoError(t, err)
			require.NoError(t, pool.Start(context.Background()))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(
					context.Background(), 5*time.Second,
				)
				defer cancel()
				require.NoError(t, pool.Stop(ctx))
			})
			err = pool.AddTransaction(c.txType, c.txCbor)
			if err != nil {
				require.Empty(t, pool.Transactions())
				return err
			}
			// A batch is one mempool entry carrying the one batch fee.
			pending := pool.Transactions()
			require.Len(t, pending, 1)
			require.Equal(t, c.tx.Hash().String(), pending[0].Hash)
			return nil
		},
	}
	feeProdBlockValidation = feeProdPath{
		name: "block validation",
		run: func(t *testing.T, c feeProdCase) error {
			err := c.applyBlock(c.ledger(t))
			if err == nil {
				requireOneBatchFeePersisted(t, c)
			}
			return err
		},
	}
	feeProdForgedRevalidation = feeProdPath{
		name: "forged block revalidation",
		run: func(t *testing.T, c feeProdCase) error {
			return c.ledger(t).validateForgedTxs(c.block)
		},
	}
	feeProdReplay = feeProdPath{
		name: "replay",
		run: func(t *testing.T, c feeProdCase) error {
			err := replayTestBlock(c.ledger(t), c.block)
			if err == nil {
				requireOneBatchFeePersisted(t, c)
			}
			return err
		},
	}
	// feeProdRollbackReapply replays the block, rolls the ledger back to
	// origin when it applied, and replays it again; both applications must
	// decide alike.
	feeProdRollbackReapply = feeProdPath{
		name: "rollback and reapply",
		run: func(t *testing.T, c feeProdCase) error {
			ls := c.ledger(t)
			first := replayTestBlock(ls, c.block)
			if first == nil {
				require.NoError(t, ls.rollback(ocommon.Point{}))
				for _, input := range c.tx.Inputs() {
					_, err := c.fx.db.UtxoByRef(
						input.Id().Bytes(), input.Index(), nil,
					)
					require.NoError(t, err, "rollback left %s spent", input)
				}
				// Rolling back to origin restores the genesis era, which this
				// fixture has no history for; replay resumes in the test era.
				setReplayTestLedgerOrigin(ls, c.era, c.pparams)
			}
			second := replayTestBlock(ls, c.block)
			require.Equal(t, first == nil, second == nil,
				"first %v, second %v", first, second)
			if second == nil {
				requireOneBatchFeePersisted(t, c)
			}
			return second
		},
	}
	feeProdAllPaths = []feeProdPath{
		feeProdLedgerAdmission,
		feeProdMempoolAdmission,
		feeProdBlockValidation,
		feeProdForgedRevalidation,
		feeProdReplay,
		feeProdRollbackReapply,
	}
	feeProdBlockPaths = []feeProdPath{
		feeProdBlockValidation,
		feeProdForgedRevalidation,
		feeProdReplay,
	}
)

// requireOneBatchFeePersisted checks the transaction rows block application
// stored: the enclosing transaction keeps the declared fee and each
// sub-transaction body is stored with none.
func requireOneBatchFeePersisted(t *testing.T, c feeProdCase) {
	t.Helper()
	for _, level := range TransactionLevels(c.tx) {
		stored, err := c.fx.db.GetTransactionByHash(level.Hash().Bytes(), nil)
		require.NoError(t, err)
		require.NotNil(t, stored)
		want := uint64(0)
		if level.Hash() == c.tx.Hash() {
			want = c.tx.Fee().Uint64()
		}
		require.Equal(
			t,
			types.Uint64(want),
			stored.Fee,
			"level %s",
			level.Hash(),
		)
	}
}

func requireFeeProdOutcome(t *testing.T, err error, accept bool) {
	t.Helper()
	if accept {
		require.NoError(t, err)
		return
	}
	var feeErr shelley.FeeTooSmallUtxoError
	require.ErrorAs(t, err, &feeErr)
}

// A Babbage fee covering only the size component, or the minimum less one
// lovelace, is rejected on every production path; the exact minimum, and a
// redeemer-free spend at the size fee alone, are accepted.
func TestBabbageFeeIncludesExecutionUnitsOnProductionPaths(t *testing.T) {
	t.Parallel()
	costModels := map[uint][]int64{
		0: blockV3MachineCostModel(t, lang.LanguageVersionV1),
	}
	pparams := &babbage.BabbageProtocolParameters{
		MinFeeA:            feeProdMinFeeA,
		MinFeeB:            feeProdMinFeeB,
		MaxBlockBodySize:   100_000,
		MaxTxSize:          16_384,
		MaxBlockHeaderSize: 100_000,
		ProtocolMajor:      8,
		AdaPerUtxoByte:     4_310,
		CostModels:         costModels,
		ExecutionCosts:     feeProdPrices(),
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10_000_000,
			Steps:  10_000_000_000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50_000_000,
			Steps:  50_000_000_000,
		},
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
	build := func(
		t *testing.T,
		scripted bool,
		fee func(sizeFee uint64) uint64,
	) feeProdCase {
		fx := newFeeProdFixture(t)
		spend := feeProdSpend{
			fee:        1 << 20,
			isValid:    true,
			costModels: costModels,
		}
		if scripted {
			script := alwaysSucceedsV1(t)
			datum := decodeRequiredDatum(t)
			spend.input = fx.seed(
				t,
				0xb1,
				feeProdAddress(
					t,
					lcommon.AddressTypeScriptNone,
					script.Hash().Bytes(),
				),
				datum.Hash().Bytes(),
			)
			spend.collateral = fx.seed(t, 0xb2, fx.keyAddr, nil)
			spend.script, spend.datum, spend.budget = script, &datum, feeProdBudget
		} else {
			spend.input = fx.seed(t, 0xb1, fx.keyAddr, nil)
		}
		probe, err := babbage.NewBabbageTransactionFromCbor(
			fx.spendCbor(t, spend),
		)
		require.NoError(t, err)
		spend.fee = fee(feeProdSizeFee(t, probe))
		txCbor := fx.spendCbor(t, spend)
		tx, err := babbage.NewBabbageTransactionFromCbor(txCbor)
		require.NoError(t, err)
		require.Equal(t, len(probe.Cbor()), len(tx.Cbor()))
		block, offsets := babbageFeeProdBlock(t, tx)
		return feeProdCase{
			fx:        fx,
			era:       eras.BabbageEraDesc,
			blockType: uint(gledger.BlockTypeBabbage),
			txType:    uint(gledger.TxTypeBabbage),
			pparams:   pparams,
			tx:        block.Transactions()[0],
			txCbor:    txCbor,
			block:     block,
			offsets:   offsets,
		}
	}
	exec := feeProdExecFee(feeProdBudget)
	for _, tc := range []struct {
		name     string
		scripted bool
		fee      func(sizeFee uint64) uint64
		accept   bool
	}{
		{"size fee only", true, func(s uint64) uint64 { return s }, false},
		{"minimum less one", true, func(s uint64) uint64 { return s + exec - 1 }, false},
		{"exact minimum", true, func(s uint64) uint64 { return s + exec }, true},
		{"redeemer-free at size fee", false, func(s uint64) uint64 { return s }, true},
	} {
		for _, path := range feeProdAllPaths {
			t.Run(tc.name+"/"+path.name, func(t *testing.T) {
				t.Parallel()
				c := build(t, tc.scripted, tc.fee)
				requireFeeProdOutcome(t, path.run(t, c), tc.accept)
			})
		}
	}
}

// dijkstraFeeProdBatch spends one key input at the top level, plus one script
// input when topScript is set, and one script input in each of children
// sub-transactions. Every script redeemer declares feeProdBudget, and only
// the top level pays a fee.
func dijkstraFeeProdBatch(
	t *testing.T,
	fx *feeProdFixture,
	pparams *gdijkstra.DijkstraProtocolParameters,
	fee uint64,
	topScript bool,
	children int,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	script := lcommon.PlutusV4Script(feeProdScriptBytes(t, false))
	scriptAddr := feeProdAddress(
		t, lcommon.AddressTypeScriptNone, script.Hash().Bytes(),
	)
	keyAddr, err := lcommon.NewAddressFromBytes(fx.keyAddr)
	require.NoError(t, err)
	input := func(id []byte) shelley.ShelleyTransactionInput {
		return shelley.NewShelleyTransactionInput(hex.EncodeToString(id), 0)
	}
	output := func(amount uint64) gdijkstra.DijkstraTransactionOutput {
		return gdijkstra.DijkstraTransactionOutput{
			Output: &shelley.ShelleyTransactionOutput{
				OutputAddress: keyAddr,
				OutputAmount:  amount,
			},
		}
	}
	// scriptLevel adds a spend redeemer at index 0 and returns the level's
	// integrity hash. A sub-transaction runs Plutus V4 or later, and Plutus V4
	// is supplied only by reference script; each child references its own
	// copy, and the top level resolves it from them.
	scriptLevel := func(
		ws *gdijkstra.DijkstraTransactionWitnessSet,
	) *lcommon.Blake2b256 {
		ws.WsRedeemers.Redeemers = map[lcommon.RedeemerKey]lcommon.RedeemerValue{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Data:    decodeRequiredDatum(t),
				ExUnits: feeProdBudget,
			},
		}
		redeemersCbor, err := cbor.Encode(ws.WsRedeemers.Redeemers)
		require.NoError(t, err)
		ws.WsRedeemers.SetCbor(redeemersCbor)
		langViews, err := lcommon.EncodeLangViews(
			map[uint]struct{}{3: {}}, pparams.CostModels,
		)
		require.NoError(t, err)
		hash := lcommon.Blake2b256Hash(append(redeemersCbor, langViews...))
		return &hash
	}

	// Script inputs sort before the key input, so a top-level script spend
	// is redeemer index 0.
	topInputs := []shelley.ShelleyTransactionInput{
		input(fx.seed(t, 0xf0, fx.keyAddr, nil)),
	}
	topValue := feeProdUtxo
	tx := &gdijkstra.DijkstraTransaction{TxIsValid: true}
	if topScript {
		topInputs = append(topInputs, input(fx.seed(
			t, 0x10, scriptAddr, nil,
		)))
		topValue += feeProdUtxo
		tx.Body.TxScriptDataHash = scriptLevel(&tx.WitnessSet)
	}
	require.Greater(t, topValue, fee)
	tx.Body.TxInputs = conway.NewConwayTransactionInputSet(topInputs)
	tx.Body.TxOutputs = []gdijkstra.DijkstraTransactionOutput{
		output(topValue - fee),
	}
	tx.Body.TxFee = fee
	tx.Body.TxCollateral = cbor.NewSetType(
		[]shelley.ShelleyTransactionInput{
			input(fx.seed(t, 0xf1, fx.keyAddr, nil)),
		},
		false,
	)
	if children > 0 {
		subs := make([]gdijkstra.DijkstraSubTransaction, children)
		for i := range subs {
			// #nosec G115 -- small child count
			subs[i].Body.TxInputs = conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{
					input(fx.seed(t, byte(0x20+i), scriptAddr, nil)),
				},
			)
			subs[i].Body.TxOutputs = []gdijkstra.DijkstraTransactionOutput{
				output(feeProdUtxo),
			}
			// #nosec G115 -- small child count
			subs[i].Body.TxReferenceInputs = cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{
					input(fx.seedReference(t, byte(0x30+i), script)),
				},
				false,
			)
			subs[i].Body.TxScriptDataHash = scriptLevel(&subs[i].WitnessSet)
		}
		tx.Body.TxSubTransactions = cbor.NewSetType(subs, false)
	}
	unsigned, err := tx.MarshalCBOR()
	require.NoError(t, err)
	tx, err = gdijkstra.NewDijkstraTransactionFromCbor(unsigned)
	require.NoError(t, err)
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      fx.key.Public().(ed25519.PublicKey),
			Signature: ed25519.Sign(fx.key, tx.Hash().Bytes()),
		}},
		false,
	)
	tx.WitnessSet.SetCbor(nil)
	tx.SetCbor(nil)
	signed, err := tx.MarshalCBOR()
	require.NoError(t, err)
	tx, err = gdijkstra.NewDijkstraTransactionFromCbor(signed)
	require.NoError(t, err)
	return tx
}

// A Dijkstra batch pays one top-level fee whose execution component is a
// single ceiling over every top-level and sub-transaction budget. Each
// production path rejects the minimum less one lovelace and accepts the exact
// minimum, which per-child rounding or a per-child base fee would exceed; an
// accepted batch is one mempool entry and records its fee on the enclosing
// transaction only, in the ledger delta and in the stored rows, after replay
// and after rollback and reapplication.
func TestDijkstraBatchFeeOnProductionPaths(t *testing.T) {
	t.Parallel()
	pparams := dijkstraTestProtocolParameters()
	pparams.MinFeeA = feeProdMinFeeA
	pparams.MinFeeB = feeProdMinFeeB
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	pparams.AdaPerUtxoByte = 4_310
	pparams.ExecutionCosts = feeProdPrices()
	pparams.CostModels = map[uint][]int64{
		3: blockV3MachineCostModel(t, lang.LanguageVersionV4),
	}
	pparams.MaxTxExUnits = lcommon.ExUnits{
		Memory: 10_000_000,
		Steps:  10_000_000_000,
	}
	pparams.MaxBlockExUnits = lcommon.ExUnits{
		Memory: 50_000_000,
		Steps:  50_000_000_000,
	}
	pparams.MinFeeRefScriptCostPerByte = &cbor.Rat{Rat: big.NewRat(15, 1)}
	pparams.MaxRefScriptSizePerTx = 200 * 1024
	pparams.MaxRefScriptSizePerBlock = 1024 * 1024

	for _, shape := range []struct {
		name      string
		topScript bool
		children  int
	}{
		{"redeemer-free", false, 0},
		{"child only", false, 1},
		{"multiple children", false, 2},
		{"top level and children", true, 2},
	} {
		budgets := make([]lcommon.ExUnits, shape.children)
		for i := range budgets {
			budgets[i] = feeProdBudget
		}
		if shape.topScript {
			budgets = append(budgets, feeProdBudget)
		}
		build := func(t *testing.T, offset int64) feeProdCase {
			probe := dijkstraFeeProdBatch(
				t, newFeeProdFixture(t), pparams, 1<<20,
				shape.topScript, shape.children,
			)
			minFee := feeProdSizeFee(t, probe) + feeProdExecFee(budgets...)
			fx := newFeeProdFixture(t)
			// #nosec G115 -- offset is -1 or 0
			tx := dijkstraFeeProdBatch(
				t, fx, pparams, uint64(int64(minFee)+offset),
				shape.topScript, shape.children,
			)
			require.Equal(t, len(probe.Cbor()), len(tx.Cbor()))
			txCbor, err := tx.MarshalCBOR()
			require.NoError(t, err)
			block := newDijkstraCollateralReturnBlock(t, tx)
			return feeProdCase{
				fx:        fx,
				era:       eras.DijkstraEraDesc,
				blockType: uint(gledger.BlockTypeDijkstra),
				txType:    uint(gdijkstra.TxTypeDijkstra),
				pparams:   pparams,
				tx:        block.Transactions()[0],
				txCbor:    txCbor,
				block:     block,
				offsets:   feeProdOffsets(t, block),
			}
		}
		for _, fee := range []struct {
			name   string
			offset int64
			accept bool
		}{
			{"minimum less one", -1, false},
			{"exact minimum", 0, true},
		} {
			for _, path := range feeProdAllPaths {
				t.Run(
					shape.name+"/"+fee.name+"/"+path.name,
					func(t *testing.T) {
						t.Parallel()
						c := build(t, fee.offset)
						requireFeeProdOutcome(t, path.run(t, c), fee.accept)
					},
				)
			}
		}
	}
}

// conwayDepositProdVariant is a Conway body that pays or claims an amount the
// transaction itself states, against the amount the protocol parameters or
// ledger state require.
type conwayDepositProdVariant struct {
	name string
	// required is the amount the ledger demands; wrong is a lower deposit or
	// a higher refund claim.
	required, wrong uint64
	refund          bool
	extra           func(fx *feeProdFixture, amount uint64) map[uint]any
	setup           func(t *testing.T, fx *feeProdFixture)
	// incorrect is the error target an isValid=true body with the wrong
	// amount is rejected with.
	incorrect func() any
}

// An isValid=false Conway transaction whose script genuinely fails, so the
// evaluated phase-2 result agrees with the flag, is applied in a block only
// when its body balances on the deposits and refunds the protocol parameters
// and ledger state require. Balanced on a deposit or refund it states itself,
// block validation, forged-block revalidation and replay reject it for value
// conservation, by exactly the difference. The isValid=true counterparts,
// with a passing script, are accepted with the required amount and rejected
// with the stated one.
func TestConwayPhase2InvalidDepositAccountingOnBlockPaths(t *testing.T) {
	t.Parallel()
	const (
		keyDeposit    = 2_000_000
		drepDeposit   = 5_000_000
		govDeposit    = 10_000_000
		fee           = 1_000_000
		understated   = 1_000_000
		drepRefundBig = drepDeposit + 1_000_000
	)
	costModels := map[uint][]int64{
		2: blockV3MachineCostModel(t, lang.LanguageVersionV3),
	}
	pparams := &conway.ConwayProtocolParameters{
		MinFeeA:            feeProdMinFeeA,
		MinFeeB:            feeProdMinFeeB,
		MaxBlockBodySize:   100_000,
		MaxTxSize:          16_384,
		MaxBlockHeaderSize: 100_000,
		KeyDeposit:         keyDeposit,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
		AdaPerUtxoByte: 4_310,
		CostModels:     costModels,
		ExecutionCosts: feeProdPrices(),
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10_000_000,
			Steps:  10_000_000_000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50_000_000,
			Steps:  50_000_000_000,
		},
		MaxValueSize:               5_000,
		CollateralPercentage:       150,
		MaxCollateralInputs:        3,
		GovActionValidityPeriod:    6,
		GovActionDeposit:           govDeposit,
		DRepDeposit:                drepDeposit,
		DRepInactivityPeriod:       20,
		MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(15, 1)},
	}
	keyCred := func(fx *feeProdFixture) []any {
		return []any{
			uint(lcommon.CredentialTypeAddrKeyHash),
			fx.keyHash.Bytes(),
		}
	}
	variants := []conwayDepositProdVariant{
		{
			name:      "stake registration deposit",
			incorrect: func() any { return &conway.CertificateDepositIncorrectError{} },
			required:  keyDeposit,
			wrong:     understated,
			extra: func(fx *feeProdFixture, amount uint64) map[uint]any {
				return map[uint]any{4: []any{[]any{
					uint(
						lcommon.CertificateTypeRegistration,
					), keyCred(fx), amount,
				}}}
			},
		},
		{
			name:      "drep registration deposit",
			incorrect: func() any { return &conway.CertificateDepositIncorrectError{} },
			required:  drepDeposit,
			wrong:     understated,
			extra: func(fx *feeProdFixture, amount uint64) map[uint]any {
				return map[uint]any{4: []any{[]any{
					uint(lcommon.CertificateTypeRegistrationDrep),
					keyCred(fx), amount, nil,
				}}}
			},
		},
		{
			name:      "drep deregistration refund",
			incorrect: func() any { return &conway.CertificateRefundIncorrectError{} },
			required:  drepDeposit,
			wrong:     drepRefundBig,
			refund:    true,
			extra: func(fx *feeProdFixture, amount uint64) map[uint]any {
				return map[uint]any{4: []any{[]any{
					uint(lcommon.CertificateTypeDeregistrationDrep),
					keyCred(fx), amount,
				}}}
			},
			setup: func(t *testing.T, fx *feeProdFixture) {
				tag, err := models.CredentialTagFromUint(
					uint(lcommon.CredentialTypeAddrKeyHash),
				)
				require.NoError(t, err)
				require.NoError(t, fx.db.Metadata().ImportDrep(
					&models.Drep{
						CredentialTag: tag,
						Credential:    fx.keyHash.Bytes(),
						AddedSlot:     1,
						Active:        true,
					},
					&models.RegistrationDrep{
						CredentialTag:  tag,
						DrepCredential: fx.keyHash.Bytes(),
						AddedSlot:      1,
						DepositAmount:  types.Uint64(drepDeposit),
					},
					nil,
				))
			},
		},
		{
			name:      "proposal deposit",
			incorrect: func() any { return &conway.ProposalDepositIncorrectError{} },
			required:  govDeposit,
			wrong:     understated,
			extra: func(fx *feeProdFixture, amount uint64) map[uint]any {
				rewardAccount := append(
					[]byte{0xe0 | lcommon.AddressNetworkTestnet},
					fx.keyHash.Bytes()...,
				)
				return map[uint]any{20: []any{[]any{
					amount,
					rewardAccount,
					[]any{uint(lcommon.GovActionTypeInfo)},
					[]any{
						"https://example.com",
						bytes.Repeat([]byte{0xab}, 32),
					},
				}}}
			},
			setup: func(t *testing.T, fx *feeProdFixture) {
				require.NoError(t, fx.db.CreateAccount(nil, &models.Account{
					StakingKey:    fx.keyHash.Bytes(),
					CredentialTag: 0,
					Active:        true,
				}))
			},
		},
	}
	build := func(
		t *testing.T,
		v conwayDepositProdVariant,
		amount uint64,
		isValid bool,
		fails bool,
	) feeProdCase {
		fx := newFeeProdFixture(t)
		if v.setup != nil {
			v.setup(t, fx)
		}
		script := lcommon.PlutusV3Script(feeProdScriptBytes(t, fails))
		// #nosec G115 -- small test amounts
		balance := -int64(amount)
		if v.refund {
			balance = int64(amount)
		}
		txCbor := fx.spendCbor(t, feeProdSpend{
			input: fx.seed(
				t,
				0xc1,
				feeProdAddress(
					t,
					lcommon.AddressTypeScriptNone,
					script.Hash().Bytes(),
				),
				nil,
			),
			collateral: fx.seed(t, 0xc2, fx.keyAddr, nil),
			script:     script,
			budget:     feeProdBudget,
			fee:        fee,
			balance:    balance,
			extra:      v.extra(fx, amount),
			isValid:    isValid,
			costModels: costModels,
			conway:     true,
		})
		block, _ := conwayTestBlock(
			t, txCbor, uint(lcommon.ProtocolVersionPlomin), 10,
		)
		return feeProdCase{
			fx:        fx,
			era:       eras.ConwayEraDesc,
			blockType: uint(gledger.BlockTypeConway),
			txType:    uint(gledger.TxTypeConway),
			pparams:   pparams,
			tx:        block.Transactions()[0],
			txCbor:    txCbor,
			block:     block,
			offsets:   feeProdOffsets(t, block),
		}
	}
	for _, v := range variants {
		for _, path := range feeProdBlockPaths {
			t.Run(
				v.name+"/isValid=false/stated amount/"+path.name,
				func(t *testing.T) {
					t.Parallel()
					err := path.run(t, build(t, v, v.wrong, false, true))
					var notConserved shelley.ValueNotConservedUtxoError
					require.ErrorAs(t, err, &notConserved)
					imbalance := new(big.Int).Sub(
						notConserved.Produced, notConserved.Consumed,
					)
					imbalance.Abs(imbalance)
					// #nosec G115 -- small test amounts
					want := big.NewInt(int64(v.wrong) - int64(v.required))
					want.Abs(want)
					require.Zero(t, want.Cmp(imbalance),
						"imbalance %s, want %s", imbalance, want)
				},
			)
			t.Run(
				v.name+"/isValid=false/required amount/"+path.name,
				func(t *testing.T) {
					t.Parallel()
					c := build(t, v, v.required, false, true)
					require.NoError(t, path.run(t, c))
					if path.name == feeProdForgedRevalidation.name {
						return
					}
					// Only the collateral is consumed.
					_, err := c.fx.db.UtxoByRef(
						c.tx.Collateral()[0].Id().Bytes(), 0, nil,
					)
					require.ErrorIs(t, err, types.ErrUtxoNotFound)
					_, err = c.fx.db.UtxoByRef(
						c.tx.Inputs()[0].Id().Bytes(), 0, nil,
					)
					require.NoError(t, err)
				},
			)
			t.Run(
				v.name+"/isValid=true/required amount/"+path.name,
				func(t *testing.T) {
					t.Parallel()
					require.NoError(
						t,
						path.run(t, build(t, v, v.required, true, false)),
					)
				},
			)
			t.Run(
				v.name+"/isValid=true/stated amount/"+path.name,
				func(t *testing.T) {
					t.Parallel()
					err := path.run(t, build(t, v, v.wrong, true, false))
					require.ErrorAs(t, err, v.incorrect())
				},
			)
			// The fixture script fails when evaluated, so the isValid=false
			// cases above agree with phase 2 rather than skip it. Replay
			// reports the mismatch as a local state recovery instead.
			t.Run(
				v.name+"/isValid=true/failing script/"+path.name,
				func(t *testing.T) {
					t.Parallel()
					err := path.run(t, build(t, v, v.required, true, true))
					require.Error(t, err)
					if path.name != feeProdReplay.name {
						var failed conway.PlutusScriptFailedError
						require.ErrorAs(t, err, &failed)
					}
				},
			)
		}
	}
}
