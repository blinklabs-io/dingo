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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// utxoRefKey mirrors the "<hex txid>:<index>" overlay key UtxoById builds
// for consumedUtxos/intraBlockUtxos (ledger/view.go), so tests can populate
// those maps exactly like production does.
func utxoRefKey(in lcommon.TransactionInput) string {
	return fmt.Sprintf("%s:%d", in.Id().String(), in.Index())
}

// distinctUtxoRefs returns the number of distinct (txId, index) refs a
// transaction's spend, collateral, and reference inputs name together --
// the number of database reads a single validation of tx should need with
// the per-view memo, however many times a rule resolves the same ref.
func distinctUtxoRefs(tx lcommon.Transaction) int {
	seen := map[string]struct{}{}
	for _, in := range tx.Inputs() {
		seen[utxoRefKey(in)] = struct{}{}
	}
	for _, in := range tx.Collateral() {
		seen[utxoRefKey(in)] = struct{}{}
	}
	for _, in := range tx.ReferenceInputs() {
		seen[utxoRefKey(in)] = struct{}{}
	}
	return len(seen)
}

// TestUtxoByIdMemoReducesDbReads_ValidateTx checks issue #4226: validating
// the Preprod fixture transaction through (*LedgerState).ValidateTx does one
// database.UtxoByRef read per distinct input ref, not one per UtxoById call.
// Without the memo in LedgerView.UtxoById it reads 60 times for 4 refs.
func TestUtxoByIdMemoReducesDbReads_ValidateTx(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)

	wantReads := distinctUtxoRefs(fx.tx)
	require.Equal(
		t,
		4,
		wantReads,
		"fixture drifted: expected the documented 4 distinct refs (3 spend incl. 1 shared with collateral, 1 reference)",
	)

	before := fx.dingoLS.utxoByRefReads.Load()
	require.NoError(t, fx.dingoLS.ValidateTx(fx.tx))
	reads := fx.dingoLS.utxoByRefReads.Load() - before

	require.Equal(
		t,
		uint64(
			wantReads,
		), //nolint:gosec // wantReads is a small, non-negative distinct-ref count
		reads,
		"ValidateTx must read each distinct UTxO ref from the database at most once",
	)
}

// TestUtxoByIdMemoReducesDbReads_LedgerProcessBlock repeats the same proof
// through the block-apply path ((*LedgerState).ledgerProcessBlock), which
// builds its own per-tx LedgerView with an intraBlockUtxos overlay --
// a different LedgerView construction site than ValidateTx, exercising the
// same UtxoById memo.
func TestUtxoByIdMemoReducesDbReads_LedgerProcessBlock(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	wantReads := distinctUtxoRefs(fx.tx)

	block := &validityOutcomeTestBlock{
		header: fx.block.Header(),
		txs:    []lcommon.Transaction{fx.tx},
		era:    conway.EraConway,
	}

	offsets := &database.BlockIngestionResult{
		TxOffsets:   map[[32]byte]database.CborOffset{},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{},
	}
	blockSlot := fx.blockSlot
	var txHash [32]byte
	copy(txHash[:], fx.tx.Hash().Bytes())
	offsets.TxOffsets[txHash] = database.CborOffset{
		BlockSlot: blockSlot, ByteLength: 1,
	}
	for _, utxo := range fx.tx.Produced() {
		var producedHash [32]byte
		copy(producedHash[:], utxo.Id.Id().Bytes())
		offsets.UtxoOffsets[database.UtxoRef{
			TxId:      producedHash,
			OutputIdx: uint32(utxo.Id.Index()), //nolint:gosec // small fixture index
		}] = database.CborOffset{BlockSlot: blockSlot, ByteLength: 1}
	}

	point := ocommon.NewPoint(blockSlot, block.Hash().Bytes())

	before := fx.dingoLS.utxoByRefReads.Load()
	err := fx.db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := fx.dingoLS.ledgerProcessBlock(
			txn,
			point,
			block,
			true,  // shouldValidate
			false, // reachesTip
			false, // skipPhase2Validation
			nil,   // expectedPrevHash
			envelopeParent{origin: true},
			offsets,
			eras.ConwayEraDesc,
			fx.dingoLS.currentPParams,
			nil, // prevEraPParams
			0,   // committeeEpoch
			false,
		)
		return err
	})
	require.NoError(t, err)
	reads := fx.dingoLS.utxoByRefReads.Load() - before

	require.Equal(
		t,
		uint64(
			wantReads,
		), //nolint:gosec // wantReads is a small, non-negative distinct-ref count
		reads,
		"ledgerProcessBlock must read each distinct UTxO ref from the database at most once",
	)
}

// TestLedgerViewUtxoByIdMemoNotSharedAcrossViews checks that the memo is
// scoped to one LedgerView: two views built from the same LedgerState and
// txn each pay their own database read for the same ref.
func TestLedgerViewUtxoByIdMemoNotSharedAcrossViews(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	fxInputs := fx.tx.Inputs()
	require.NotEmpty(t, fxInputs)
	input := fxInputs[0]

	txn := fx.db.Transaction(false)
	defer txn.Release()

	before := fx.dingoLS.utxoByRefReads.Load()

	view1 := fx.dingoLS.NewView(txn)
	_, err := view1.UtxoById(input)
	require.NoError(t, err)
	afterView1 := fx.dingoLS.utxoByRefReads.Load()
	require.Equal(
		t,
		before+1,
		afterView1,
		"view1's first lookup must read the database",
	)

	// A repeat lookup on view1 hits its own memo: no further DB read.
	_, err = view1.UtxoById(input)
	require.NoError(t, err)
	require.Equal(t, afterView1, fx.dingoLS.utxoByRefReads.Load())

	// A fresh view for the *same* ref must not see view1's memo.
	view2 := fx.dingoLS.NewView(txn)
	_, err = view2.UtxoById(input)
	require.NoError(t, err)
	require.Equal(
		t, afterView1+1, fx.dingoLS.utxoByRefReads.Load(),
		"a new LedgerView must not inherit another view's memo",
	)
}

// TestLedgerViewUtxoByIdChecksOverlaysBeforeMemo checks that consumedUtxos
// still wins over a memo entry populated by an earlier successful call on
// the same view. The overlay maps are shared by reference and
// validateForgedTxs extends them between transactions, so a view reused
// across that boundary must not answer from the memo. Consulting the memo
// before the overlays returns the cached success instead of
// ErrUtxoAlreadyConsumed.
func TestLedgerViewUtxoByIdChecksOverlaysBeforeMemo(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	fxInputs := fx.tx.Inputs()
	require.NotEmpty(t, fxInputs)
	input := fxInputs[0]

	txn := fx.db.Transaction(false)
	defer txn.Release()
	lv := &LedgerView{
		txn:           txn,
		ls:            fx.dingoLS,
		consumedUtxos: map[string]struct{}{},
	}

	// First call: nothing consumed yet, resolves from the database and
	// populates the memo.
	_, err := lv.UtxoById(input)
	require.NoError(t, err)

	// Simulate a caller marking the same ref consumed on this view between
	// calls.
	lv.consumedUtxos[utxoRefKey(input)] = struct{}{}

	_, err = lv.UtxoById(input)
	require.ErrorIs(
		t, err, ErrUtxoAlreadyConsumed,
		"consumedUtxos must be checked before the memo on every call",
	)
}

// TestLedgerViewUtxoByIdDoesNotMemoizeNotFound checks that a miss is not
// cached: a ref that becomes resolvable after a failed lookup resolves on a
// later call within the same view.
func TestLedgerViewUtxoByIdDoesNotMemoizeNotFound(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ls := &LedgerState{db: db, config: LedgerStateConfig{Logger: testLogger()}}

	txId := bytes.Repeat([]byte{0x77}, 32)
	input, err := omockledger.NewSimpleTransactionInput(txId, 0)
	require.NoError(t, err)

	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x78}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		lv := &LedgerView{txn: txn, ls: ls}

		_, err := lv.UtxoById(input)
		require.Error(t, err, "ref must not exist yet")

		require.NoError(t, db.CreateUtxo(txn, &models.Utxo{
			TxId:      txId,
			OutputIdx: 0,
			AddedSlot: 1,
		}))
		encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: address,
			OutputAmount:  5_000_000,
		})
		require.NoError(t, err)
		require.NoError(t, db.Blob().SetUtxo(txn.Blob(), txId, 0, encoded))

		utxo, err := lv.UtxoById(input)
		require.NoError(
			t,
			err,
			"a not-found result must not be memoized: the ref is now resolvable",
		)
		require.NotNil(t, utxo.Output)
		require.Equal(t, uint64(5_000_000), utxo.Output.Amount().Uint64())
		return nil
	}))
}

// intraBlockDoubleSpendFixture is a block whose two transactions spend the
// same funded input. Its era's ValidateTxFunc resolves every declared input
// through the LedgerView it is given, as script.ResolveTxInputs does, and
// records each transaction that resolved all of its inputs.
type intraBlockDoubleSpendFixture struct {
	db        *database.Database
	ls        *LedgerState
	era       eras.EraDesc
	pparams   lcommon.ProtocolParameters
	block     *validityOutcomeTestBlock
	offsets   *database.BlockIngestionResult
	tx1       lcommon.Transaction
	validated *[]string
}

func newIntraBlockDoubleSpendFixture(
	t *testing.T,
) *intraBlockDoubleSpendFixture {
	t.Helper()

	db := newTestDB(t)
	fundingTxId := bytes.Repeat([]byte{0x91}, 32)
	fundingInput, err := omockledger.NewSimpleTransactionInput(fundingTxId, 0)
	require.NoError(t, err)

	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x92}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId: fundingTxId, OutputIdx: 0, AddedSlot: 1,
	}))
	fundingOutputCbor, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  10_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), fundingTxId, 0, fundingOutputCbor)
	}))

	output1, err := omockledger.NewSimpleTransactionOutput(
		address.String(),
		4_000_000,
	)
	require.NoError(t, err)
	output2, err := omockledger.NewSimpleTransactionOutput(
		address.String(),
		4_000_000,
	)
	require.NoError(t, err)

	tx1 := omockledger.NewTransactionBuilder()
	tx1.WithId(bytes.Repeat([]byte{0x93}, 32))
	tx1.WithType(gdijkstra.TxTypeDijkstra)
	tx1.WithInputs(fundingInput)
	tx1.WithOutputs(output1)
	tx1.WithValid(true)

	tx2 := omockledger.NewTransactionBuilder()
	tx2.WithId(bytes.Repeat([]byte{0x94}, 32))
	tx2.WithType(gdijkstra.TxTypeDijkstra)
	tx2.WithInputs(
		fundingInput,
	) // same input as tx1: the intra-block double spend
	tx2.WithOutputs(output2)
	tx2.WithValid(true)

	var tx1Hash, tx2Hash [32]byte
	copy(tx1Hash[:], tx1.Hash().Bytes())
	copy(tx2Hash[:], tx2.Hash().Bytes())
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			tx1Hash: {BlockSlot: 10, ByteLength: 1},
			tx2Hash: {BlockSlot: 10, ByteLength: 1},
		},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{
			{TxId: tx1Hash, OutputIdx: 0}: {BlockSlot: 10, ByteLength: 1},
			{TxId: tx2Hash, OutputIdx: 0}: {BlockSlot: 10, ByteLength: 1},
		},
	}

	initialTip := ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: []byte("unchanged-tip")},
	}
	require.NoError(t, db.SetTip(initialTip, nil))

	validated := []string{}
	testEra := eras.DijkstraEraDesc
	testEra.ValidateTxFunc = func(
		gotTx lcommon.Transaction,
		_ uint64,
		lv lcommon.LedgerState,
		_ lcommon.ProtocolParameters,
	) error {
		for _, in := range gotTx.Inputs() {
			if _, err := lv.UtxoById(in); err != nil {
				return fmt.Errorf("resolve input %s: %w", in.String(), err)
			}
		}
		validated = append(validated, gotTx.Hash().String())
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
		db:             db,
		activeEras:     []eras.EraDesc{testEra},
		currentEra:     testEra,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
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
		txs: []lcommon.Transaction{tx1, tx2},
		era: gdijkstra.EraDijkstra,
	}
	return &intraBlockDoubleSpendFixture{
		db:        db,
		ls:        ls,
		era:       testEra,
		pparams:   pparams,
		block:     block,
		offsets:   offsets,
		tx1:       tx1,
		validated: &validated,
	}
}

// TestLedgerProcessBlockRejectsIntraBlockDoubleSpend checks the block-apply
// path: tx1's delta removes the shared input from the database transaction
// before tx2's view is built, so tx2 must fail to resolve it. A memo shared
// across the per-transaction views would answer tx2 from tx1's resolution.
func TestLedgerProcessBlockRejectsIntraBlockDoubleSpend(t *testing.T) {
	t.Parallel()
	fx := newIntraBlockDoubleSpendFixture(t)

	processErr := fx.db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := fx.ls.ledgerProcessBlock(
			txn,
			ocommon.NewPoint(10, fx.block.Hash().Bytes()),
			fx.block,
			true,
			false,
			false,
			nil,
			envelopeParent{origin: true},
			fx.offsets,
			fx.era,
			fx.pparams,
			nil,
			0,
			false,
		)
		return err
	})

	require.ErrorIs(
		t,
		processErr,
		database.ErrUtxoNotFound,
		"tx2 must be rejected: it spends an input tx1 already consumed in this block",
	)
	require.Equal(
		t, []string{fx.tx1.Hash().String()}, *fx.validated,
		"tx1 must validate successfully before tx2 is attempted and rejected",
	)
}

// TestValidateForgedTxsRejectsIntraBlockDoubleSpend checks the forged-block
// path, which writes nothing to the database between transactions and
// relies on the consumedUtxos overlay instead: tx2 must fail with
// ErrUtxoAlreadyConsumed. A memo shared across views and consulted before
// the overlays would answer tx2 from tx1's resolution.
func TestValidateForgedTxsRejectsIntraBlockDoubleSpend(t *testing.T) {
	t.Parallel()
	fx := newIntraBlockDoubleSpendFixture(t)

	err := fx.ls.validateForgedTxs(fx.block)

	require.ErrorIs(
		t,
		err,
		ErrUtxoAlreadyConsumed,
		"tx2 must be rejected: it spends an input tx1 already consumed in this block",
	)
	require.Equal(
		t, []string{fx.tx1.Hash().String()}, *fx.validated,
		"tx1 must validate successfully before tx2 is attempted and rejected",
	)
}

// TestLedgerViewMemoizedUtxosUnchangedByValidation checks the memo's sharing
// contract: every UtxoById caller on a view receives the same decoded
// Output, where each call used to decode its own. After full Conway
// validation of the Preprod fixture, including Plutus evaluation, each
// memoized output must still equal a fresh decode of its stored bytes.
func TestLedgerViewMemoizedUtxosUnchangedByValidation(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)

	var lv *LedgerView
	require.NoError(t, fx.dingoLS.validateTxCore(
		fx.tx,
		func(txn *database.Txn) *LedgerView {
			lv = &LedgerView{txn: txn, ls: fx.dingoLS}
			return lv
		},
	))
	require.NotNil(t, lv)
	require.Len(t, lv.utxoMemo, distinctUtxoRefs(fx.tx))

	for key, cached := range lv.utxoMemo {
		stored, err := fx.db.UtxoByRef(key.txId.Bytes(), key.index, nil)
		require.NoError(t, err)
		fresh, err := stored.Decode()
		require.NoError(t, err)
		require.Equal(
			t, fresh, cached.Output,
			"memoized output %s#%d was mutated during validation",
			key.txId.String(), key.index,
		)
	}
}
