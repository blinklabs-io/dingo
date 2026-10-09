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
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func aheadFixtureBlock(
	fx *utxoMemoPreprodFixture,
	txs ...lcommon.Transaction,
) gledger.Block {
	return &validityOutcomeTestBlock{
		header: fx.block.Header(),
		txs:    txs,
		era:    conway.EraConway,
	}
}

// stopWithin fails the test instead of hanging when stop does not return.
func stopWithin(t *testing.T, p *utxoPrefetchAhead) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		p.stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("utxoPrefetchAhead.stop did not return")
	}
}

func TestUtxoPrefetchAheadServesUnconsumedInputs(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	fx.dingoLS.utxoPrefetchAheadAwaitReady = true
	blocks := []gledger.Block{
		aheadFixtureBlock(fx),
		aheadFixtureBlock(fx, fx.tx),
	}
	p := fx.dingoLS.startUtxoPrefetchAhead(
		t.Context(), blocks, []bool{true, true},
	)
	defer stopWithin(t, p)

	require.Nil(t, p.take(0), "the first block of a chunk is never prefetched")
	got := p.take(1)
	require.NotEmpty(t, fx.tx.Inputs())
	for _, in := range fx.tx.Inputs() {
		require.Contains(t, got, utxoref.ForInput(in))
	}
}

func TestUtxoPrefetchAheadExcludesInputsSpentByEarlierBlocks(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	fx.dingoLS.utxoPrefetchAheadAwaitReady = true
	// Block 0 spends every input block 2 names, so a snapshot taken before the
	// chunk still holds all of them as live.
	blocks := []gledger.Block{
		aheadFixtureBlock(fx, fx.tx),
		aheadFixtureBlock(fx),
		aheadFixtureBlock(fx, fx.tx),
	}
	p := fx.dingoLS.startUtxoPrefetchAhead(
		t.Context(), blocks, []bool{true, true, true},
	)
	defer stopWithin(t, p)

	require.Nil(t, p.take(0))
	require.Empty(t, p.take(1))
	got := p.take(2)
	for _, in := range fx.tx.Inputs() {
		require.NotContains(
			t, got, utxoref.ForInput(in),
			"an input consumed by an earlier block must not be served",
		)
	}
	for _, in := range fx.tx.Collateral() {
		require.NotContains(t, got, utxoref.ForInput(in))
	}
}

func TestCollectConsumedInputsWalksSubTransactions(t *testing.T) {
	t.Parallel()
	spentHash := bytes.Repeat([]byte{0x11}, 32)
	subBody, err := cbor.Encode(map[uint]any{
		0: []any{[]any{spentHash, uint64(3)}},
		1: []any{},
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
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(body), map[uint]any{}, true, nil},
	)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	require.Empty(t, tx.Inputs())

	consumed := make(map[utxoref.Key]struct{})
	collectConsumedInputs(consumed, tx)
	require.Contains(t, consumed, utxoref.Key{
		TxId:  lcommon.NewBlake2b256(spentHash),
		Index: 3,
	})
}

func TestUtxoPrefetchAheadStopJoinsAndReleasesReadTxn(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	fx.dingoLS.utxoPrefetchAheadAwaitReady = true
	blocks := []gledger.Block{
		aheadFixtureBlock(fx),
		aheadFixtureBlock(fx, fx.tx),
		aheadFixtureBlock(fx),
	}
	for range 50 {
		p := fx.dingoLS.startUtxoPrefetchAhead(
			t.Context(), blocks, []bool{true, true, true},
		)
		// Block 1 is resolved, so the goroutine holds its read transaction
		// and then waits for permission to run block 2.
		require.NotEmpty(t, p.take(1))
		stopWithin(t, p)
		select {
		case <-p.done:
		default:
			t.Fatal("stop returned before the goroutine exited")
		}
		require.NotNil(t, p.readTxn)
		released := make(chan struct{})
		p.readTxn.OnFinish(func() { close(released) })
		select {
		case <-released:
		default:
			t.Fatal("read transaction not released when stop returned")
		}
	}
}

func TestUtxoPrefetchAheadTakeDoesNotWaitForReadPool(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	limit := fx.db.Metadata().(interface{ ReadSnapshotLimit() int }).
		ReadSnapshotLimit()
	// Hold every read-pool connection, as snapshot opens parked on the
	// chunk's commit barrier do.
	held := make([]*database.Txn, 0, limit+1)
	releaseHeld := func() {
		for _, h := range held {
			h.Release()
		}
		held = nil
	}
	defer releaseHeld()
	for range limit + 1 {
		held = append(held, fx.db.Transaction(t.Context(), false))
	}
	blocks := []gledger.Block{
		aheadFixtureBlock(fx),
		aheadFixtureBlock(fx, fx.tx),
	}
	p := fx.dingoLS.startUtxoPrefetchAhead(
		t.Context(), blocks, []bool{true, true},
	)
	defer stopWithin(t, p)

	took := make(chan map[utxoref.Key]lcommon.Utxo, 1)
	go func() {
		p.take(0)
		took <- p.take(1)
	}()
	select {
	case got := <-took:
		require.Nil(t, got)
	case <-time.After(10 * time.Second):
		t.Fatal("take waited for a read-pool connection")
	}
	select {
	case <-p.ready:
		t.Fatal("the read pool was not saturated")
	default:
	}

	// Once a connection is free the goroutine serves blocks again.
	releaseHeld()
	select {
	case <-p.ready:
	case <-time.After(30 * time.Second):
		t.Fatal("goroutine did not acquire a freed read connection")
	}
	p.ls.utxoPrefetchAheadAwaitReady = true
	require.NotEmpty(t, p.take(1))
}

func TestUtxoPrefetchAheadResolvesFirstBlockDuringTakeZero(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	blocks := []gledger.Block{
		aheadFixtureBlock(fx),
		aheadFixtureBlock(fx, fx.tx),
	}
	p := fx.dingoLS.startUtxoPrefetchAhead(
		t.Context(), blocks, []bool{true, true},
	)
	defer stopWithin(t, p)

	require.Nil(t, p.take(0))
	// Block 1 resolves while block 0 applies; take(1) has not been called.
	select {
	case got := <-p.slots[1]:
		require.NotEmpty(t, got)
	case <-time.After(30 * time.Second):
		t.Fatal("block 1 was not resolved after take(0)")
	}
}

func TestUtxoPrefetchAheadStopWithoutTake(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)
	blocks := []gledger.Block{
		aheadFixtureBlock(fx),
		aheadFixtureBlock(fx, fx.tx),
	}
	for _, tc := range []struct {
		name         string
		cancelParent bool
	}{
		{name: "stop cancels an idle goroutine"},
		{name: "parent context cancelled first", cancelParent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Nothing takes a result, so the goroutine waits for permission
			// to resolve block 1 and only cancellation can end it.
			p := fx.dingoLS.startUtxoPrefetchAhead(
				ctx, blocks, []bool{true, true},
			)
			if tc.cancelParent {
				cancel()
			}
			stopWithin(t, p)
			select {
			case <-p.done:
			default:
				t.Fatal("stop returned before the goroutine exited")
			}
			// A stopped prefetcher serves nothing rather than blocking.
			require.Nil(t, p.take(1))
		})
	}
	var none *utxoPrefetchAhead
	require.Nil(t, none.take(1))
	none.stop()
}

// aheadReplayEnv is a ledger replaying a fixed chain through
// ledgerProcessBlocksFromSource with historical validation on.
type aheadReplayEnv struct {
	ls                        *LedgerState
	db                        *database.Database
	spendTxId, collateralTxId []byte
	freeTxId                  []byte
	blocks                    []gledger.Block
	txs                       []lcommon.Transaction
}

func aheadSignedKeyTx(
	t *testing.T,
	f *requiredDatumFixture,
	inputTxId []byte,
) []byte {
	t.Helper()
	publicKey := f.key.Public().(ed25519.PublicKey)
	keyAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		lcommon.Blake2b224Hash(publicKey).Bytes(),
		nil,
	)
	require.NoError(t, err)
	keyAddrBytes, err := keyAddr.Bytes()
	require.NoError(t, err)
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{[]any{inputTxId, uint(0)}}},
		1: []any{map[uint]any{0: keyAddrBytes, 1: uint64(9_999_998)}},
		2: uint64(2),
	})
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(bodyCbor)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{0: []any{[]any{
			[]byte(publicKey), ed25519.Sign(f.key, bodyHash.Bytes()),
		}}},
		true,
		nil,
	})
	require.NoError(t, err)
	return txCbor
}

func aheadChainBlock(
	t *testing.T,
	txCbor []byte,
	number, slot uint64,
	prev lcommon.Blake2b256,
) *conway.ConwayBlock {
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
	block.BlockHeader.Body.BlockNumber = number
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.PrevHash = prev
	block.BlockHeader.Body.ProtoVersion.Major = uint64(
		lcommon.ProtocolVersionPlomin,
	)
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	return block
}

// newAheadReplayEnv builds a chain of one block per transaction in txCbors and
// a ledger, with LedgerPrefetchAheadEnabled set to ahead, positioned before the
// first block.
func newAheadReplayEnv(
	t *testing.T,
	ahead bool,
	build func(f *requiredDatumFixture, freeTxId []byte) [][]byte,
) *aheadReplayEnv {
	t.Helper()
	f := newRequiredDatumFixture(t, alwaysSucceedsV1(t), true)
	// A third live UTxO that no transaction of the first block touches.
	freeTxId := bytes.Repeat([]byte{0xd4}, 32)
	publicKey := f.key.Public().(ed25519.PublicKey)
	keyAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		lcommon.Blake2b224Hash(publicKey).Bytes(),
		nil,
	)
	require.NoError(t, err)
	keyAddrBytes, err := keyAddr.Bytes()
	require.NoError(t, err)
	freeOutput, err := cbor.Encode(
		map[uint]any{0: keyAddrBytes, 1: uint64(10_000_000)},
	)
	require.NoError(t, err)
	require.NoError(t, f.db.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			if err := f.db.CreateUtxo(t.Context(), txn, &models.Utxo{
				TxId: freeTxId, OutputIdx: 0, AddedSlot: 1,
			}); err != nil {
				return err
			}
			return f.db.Blob().SetUtxo(txn.Blob(), freeTxId, 0, freeOutput)
		},
	))

	costModels := map[uint][]int64{
		0: blockV3MachineCostModel(t, lang.LanguageVersionV1),
	}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
		MaxBlockBodySize:     100_000,
		MaxBlockHeaderSize:   100_000,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		MaxCollateralInputs:  3,
		CollateralPercentage: 150,
		CostModels:           costModels,
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1_000_000_000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1_000_000_000)},
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10_000_000, Steps: 10_000_000_000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 50_000_000, Steps: 50_000_000_000,
		},
	}

	env := &aheadReplayEnv{
		db:             f.db,
		spendTxId:      f.spendTxId,
		collateralTxId: f.collateralTxId,
		freeTxId:       freeTxId,
	}
	var prev lcommon.Blake2b256
	for i, txCbor := range build(f, freeTxId) {
		block := aheadChainBlock(
			t, txCbor, uint64(i+1), uint64(10+i), prev, //nolint:gosec // small index
		)
		prev = block.Hash()
		env.blocks = append(env.blocks, block)
		env.txs = append(env.txs, block.Transactions()[0])
	}

	cm, err := chain.NewManager(t.Context(), f.db, nil)
	require.NoError(t, err)
	raw := make([]chain.RawBlock, 0, len(env.blocks))
	for _, block := range env.blocks {
		raw = append(raw, chain.RawBlock{
			Slot:        block.SlotNumber(),
			Hash:        block.Hash().Bytes(),
			BlockNumber: block.BlockNumber(),
			Type:        uint(gledger.BlockTypeConway),
			PrevHash:    block.PrevHash().Bytes(),
			Cbor:        block.Cbor(),
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(t.Context(), raw))
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:                   f.db,
		ChainManager:               cm,
		CardanoNodeConfig:          nodeConfig,
		Logger:                     testLogger(),
		PromRegistry:               prometheus.NewRegistry(),
		ValidateHistorical:         true,
		ManualBlockProcessing:      true,
		LedgerPrefetchAheadEnabled: ahead,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	setReplayTestLedgerOrigin(ls, eras.ConwayEraDesc, pp)
	require.NoError(t, cm.SetLedger(ls))
	ls.utxoPrefetchAheadAwaitReady = ahead
	env.ls = ls
	return env
}

func (e *aheadReplayEnv) replayOneBatch() error {
	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: e.blocks}
	close(results)
	return e.ls.ledgerProcessBlocksFromSource(context.Background(), results)
}

// aheadReplayState is the ledger state observable after a replay.
type aheadReplayState struct {
	Tip       ochainsync.Tip
	LiveUtxos map[string]bool
}

func (e *aheadReplayEnv) state(t *testing.T) aheadReplayState {
	t.Helper()
	tip, err := e.db.GetTip(nil)
	require.NoError(t, err)
	state := aheadReplayState{Tip: tip, LiveUtxos: map[string]bool{}}
	refs := [][]byte{e.spendTxId, e.collateralTxId, e.freeTxId}
	for _, tx := range e.txs {
		refs = append(refs, tx.Hash().Bytes())
	}
	for _, id := range refs {
		_, err := e.db.UtxoByRef(t.Context(), id, 0, nil)
		if err != nil {
			require.ErrorIs(t, err, types.ErrUtxoNotFound)
		}
		state.LiveUtxos[string(id)] = err == nil
	}
	return state
}

// aheadSpendChain builds three blocks: a script spend, a spend of a UTxO the
// first block does not touch, and a spend of the UTxO the first block lists as
// collateral without consuming it.
func aheadSpendChain(
	t *testing.T,
) func(f *requiredDatumFixture, freeTxId []byte) [][]byte {
	return func(f *requiredDatumFixture, freeTxId []byte) [][]byte {
		costModels := map[uint][]int64{
			0: blockV3MachineCostModel(t, lang.LanguageVersionV1),
		}
		body, witnessSet := signedDatumSpend(t, f, costModels, true)
		scriptTx, err := cbor.Encode([]any{body, witnessSet, true, nil})
		require.NoError(t, err)
		return [][]byte{
			scriptTx,
			aheadSignedKeyTx(t, f, freeTxId),
			aheadSignedKeyTx(t, f, f.collateralTxId),
		}
	}
}

func TestLedgerPrefetchAheadMatchesSerialState(t *testing.T) {
	t.Parallel()
	off := newAheadReplayEnv(t, false, aheadSpendChain(t))
	on := newAheadReplayEnv(t, true, aheadSpendChain(t))

	require.NoError(t, off.replayOneBatch())
	require.NoError(t, on.replayOneBatch())

	offState, onState := off.state(t), on.state(t)
	require.Equal(t, off.blocks[2].Hash().Bytes(), offState.Tip.Point.Hash)
	// Every transaction applied: all three inputs are spent, all outputs live.
	for _, id := range [][]byte{off.spendTxId, off.collateralTxId, off.freeTxId} {
		require.False(t, offState.LiveUtxos[string(id)])
	}
	for _, tx := range off.txs {
		require.True(t, offState.LiveUtxos[string(tx.Hash().Bytes())])
	}
	require.Equal(t, offState, onState)
	require.Zero(t, off.ls.utxoPrefetchAheadServed.Load())
	require.Equal(
		t, uint64(1), on.ls.utxoPrefetchAheadServed.Load(),
		"the flag-on run must serve the untouched UTxO from the ahead prefetch",
	)
}

func TestLedgerPrefetchAheadRejectsInputSpentByPreviousBlock(t *testing.T) {
	t.Parallel()
	doubleSpend := func(
		f *requiredDatumFixture, _ []byte,
	) [][]byte {
		costModels := map[uint][]int64{
			0: blockV3MachineCostModel(t, lang.LanguageVersionV1),
		}
		body, witnessSet := signedDatumSpend(t, f, costModels, true)
		scriptTx, err := cbor.Encode([]any{body, witnessSet, true, nil})
		require.NoError(t, err)
		return [][]byte{scriptTx, scriptTx}
	}
	off := newAheadReplayEnv(t, false, doubleSpend)
	on := newAheadReplayEnv(t, true, doubleSpend)

	offErr := off.replayOneBatch()
	onErr := on.replayOneBatch()

	require.Error(t, offErr)
	require.Error(t, onErr)
	require.Equal(
		t, offErr.Error(), onErr.Error(),
		"a UTxO spent by block i must not be served to block i+1",
	)
	require.Equal(t, off.state(t), on.state(t))
	require.Equal(t, ochainsync.Tip{}, on.state(t).Tip)
	require.Zero(t, on.ls.utxoPrefetchAheadServed.Load())
}
