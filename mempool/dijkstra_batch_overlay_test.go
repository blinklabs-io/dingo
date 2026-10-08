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

package mempool

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

type batchOutRef struct {
	txID  []byte
	index uint64
}

// batchSpec describes one Dijkstra batch. Each entry of childInputs and
// childOutputs is one sub-transaction.
type batchSpec struct {
	childInputs      [][]batchOutRef
	childOutputs     [][]uint64
	inputs           []batchOutRef
	outputs          []uint64
	collateral       []batchOutRef
	collateralReturn uint64
	fee              uint64
	invalid          bool
}

func batchInput(seed byte, index uint64) batchOutRef {
	return batchOutRef{txID: bytes.Repeat([]byte{seed}, 32), index: index}
}

func (r batchOutRef) cbor() []any {
	return []any{r.txID, r.index}
}

func batchInputsCbor(refs []batchOutRef) []any {
	ret := make([]any, 0, len(refs))
	for _, ref := range refs {
		ret = append(ret, ref.cbor())
	}
	return ret
}

func batchOutputsCbor(amounts []uint64) []any {
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x42}, 28)...)
	ret := make([]any, 0, len(amounts))
	for _, amount := range amounts {
		ret = append(ret, map[uint]any{0: address, 1: amount})
	}
	return ret
}

func buildDijkstraBatch(
	t *testing.T,
	spec batchSpec,
) (*dijkstra.DijkstraTransaction, []byte) {
	t.Helper()
	children := make([]cbor.RawMessage, 0, len(spec.childInputs))
	for idx := range spec.childInputs {
		subBody, err := cbor.Encode(map[uint]any{
			0: batchInputsCbor(spec.childInputs[idx]),
			1: batchOutputsCbor(spec.childOutputs[idx]),
		})
		require.NoError(t, err)
		child, err := cbor.Encode([]any{
			cbor.RawMessage(subBody), map[uint]any{}, nil,
		})
		require.NoError(t, err)
		children = append(children, child)
	}
	bodyMap := map[uint]any{
		0: batchInputsCbor(spec.inputs),
		1: batchOutputsCbor(spec.outputs),
		2: spec.fee,
	}
	if len(children) > 0 {
		bodyMap[23] = cbor.NewSetType(children, true)
	}
	if len(spec.collateral) > 0 {
		bodyMap[13] = batchInputsCbor(spec.collateral)
	}
	if spec.collateralReturn > 0 {
		bodyMap[16] = batchOutputsCbor([]uint64{spec.collateralReturn})[0]
	}
	body, err := cbor.Encode(bodyMap)
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(body), map[uint]any{}, true, nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)
	// The wire format cannot carry is_valid=false; a block marks the
	// transaction invalid after decoding, so do the same here.
	dijkstraTx.TxIsValid = !spec.invalid
	return dijkstraTx, txCbor
}

func refKey(ref batchOutRef) utxoref.Key {
	return utxoref.ForInput(
		shelley.NewShelleyTransactionInput(
			fmt.Sprintf("%x", ref.txID),
			int(ref.index), //nolint:gosec
		),
	)
}

func childOutputKey(
	tx *dijkstra.DijkstraTransaction,
	child int,
	index int,
) utxoref.Key {
	childID := tx.Body.TxSubTransactions.Items()[child].Body.Id()
	return utxoref.ForInput(
		shelley.NewShelleyTransactionInput(childID.String(), index),
	)
}

func TestUtxoOverlayApplyTxCoversEveryDijkstraBatchLevel(t *testing.T) {
	t.Parallel()
	childIn := batchInput(0x11, 0)
	topIn := batchInput(0x12, 0)
	tx, txCbor := buildDijkstraBatch(t, batchSpec{
		childInputs:  [][]batchOutRef{{childIn}},
		childOutputs: [][]uint64{{1_000_000}},
		inputs:       []batchOutRef{topIn},
		outputs:      []uint64{2_000_000},
	})
	overlay := newUtxoOverlay()
	overlay.applyTx(tx.Hash().String(), uint(dijkstra.TxTypeDijkstra), txCbor, tx)

	require.Contains(t, overlay.consumed, refKey(childIn))
	require.Contains(t, overlay.consumed, refKey(topIn))
	require.Len(t, overlay.consumed, 2)
	childOut := childOutputKey(tx, 0, 0)
	topOut := utxoref.ForInput(
		shelley.NewShelleyTransactionInput(tx.Hash().String(), 0),
	)
	require.NotEqual(t, childOut, topOut)
	require.Contains(t, overlay.created, childOut)
	require.Contains(t, overlay.created, topOut)
	require.Len(t, overlay.created, 2)
}

func TestUtxoOverlayApplyTxReservesOnlyCollateralForInvalidBatch(
	t *testing.T,
) {
	t.Parallel()
	childIn := batchInput(0x21, 0)
	topIn := batchInput(0x22, 0)
	collateral := batchInput(0x23, 0)
	tx, txCbor := buildDijkstraBatch(t, batchSpec{
		childInputs:      [][]batchOutRef{{childIn}},
		childOutputs:     [][]uint64{{1_000_000}},
		inputs:           []batchOutRef{topIn},
		outputs:          []uint64{2_000_000},
		collateral:       []batchOutRef{collateral},
		collateralReturn: 3_000_000,
		invalid:          true,
	})
	overlay := newUtxoOverlay()
	overlay.applyTx(tx.Hash().String(), uint(dijkstra.TxTypeDijkstra), txCbor, tx)

	require.Equal(
		t,
		map[utxoref.Key]struct{}{refKey(collateral): {}},
		overlay.consumed,
	)
	require.Len(t, overlay.created, 1)
	collateralReturnKey := utxoref.ForInput(
		shelley.NewShelleyTransactionInput(tx.Hash().String(), 1),
	)
	require.Contains(t, overlay.created, collateralReturnKey)
}

func TestUtxoOverlayPrunesDescendantOfBatchChildOutput(t *testing.T) {
	t.Parallel()
	parent, parentCbor := buildDijkstraBatch(t, batchSpec{
		childInputs:  [][]batchOutRef{{batchInput(0x31, 0)}},
		childOutputs: [][]uint64{{1_000_000}},
		inputs:       []batchOutRef{batchInput(0x32, 0)},
		outputs:      []uint64{2_000_000},
	})
	childOutput := childOutputKey(parent, 0, 0)
	childID := parent.Body.TxSubTransactions.Items()[0].Body.Id()
	descendant, descendantCbor := buildDijkstraBatch(t, batchSpec{
		inputs: []batchOutRef{{txID: childID.Bytes(), index: 0}},
		outputs: []uint64{
			900_000,
		},
	})
	unrelated, unrelatedCbor := buildDijkstraBatch(t, batchSpec{
		inputs:  []batchOutRef{batchInput(0x33, 0)},
		outputs: []uint64{800_000},
	})
	overlay := newUtxoOverlay()
	for _, entry := range []struct {
		tx   *dijkstra.DijkstraTransaction
		cbor []byte
	}{{parent, parentCbor}, {descendant, descendantCbor}, {unrelated, unrelatedCbor}} {
		overlay.applyTx(
			entry.tx.Hash().String(),
			uint(dijkstra.TxTypeDijkstra),
			entry.cbor,
			entry.tx,
		)
	}
	require.Contains(t, overlay.consumed, childOutput)

	pruned := overlay.removeBatchWithDescendants(
		map[string]struct{}{parent.Hash().String(): {}},
	)
	require.Equal(t, []string{descendant.Hash().String()}, pruned)
	require.Len(t, overlay.applied, 1)
	require.Equal(t, unrelated.Hash().String(), overlay.applied[0].hash)
}

// chainedOverlayValidator applies the ledger's overlay rules: an input must
// be unspent on chain or created by a pending transaction, and must not be
// consumed by a pending transaction.
type chainedOverlayValidator struct {
	mu        sync.Mutex
	onChain   map[utxoref.Key]struct{}
	chainUsed map[utxoref.Key]struct{}
}

func (v *chainedOverlayValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (v *chainedOverlayValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	consumed map[utxoref.Key]struct{},
	created map[utxoref.Key]lcommon.Utxo,
) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, input := range tx.Consumed() {
		key := utxoref.ForInput(input)
		if _, spent := consumed[key]; spent {
			return fmt.Errorf("input %s already spent by a pending transaction", key)
		}
		if _, spent := v.chainUsed[key]; spent {
			return fmt.Errorf("input %s already spent on chain", key)
		}
		_, onChain := v.onChain[key]
		_, pending := created[key]
		if !onChain && !pending {
			return fmt.Errorf("input %s is unknown", key)
		}
	}
	return nil
}

func newChainedOverlayValidator(
	refs ...batchOutRef,
) *chainedOverlayValidator {
	v := &chainedOverlayValidator{
		onChain:   make(map[utxoref.Key]struct{}),
		chainUsed: make(map[utxoref.Key]struct{}),
	}
	for _, ref := range refs {
		v.onChain[refKey(ref)] = struct{}{}
	}
	return v
}

func addBatch(t *testing.T, pool *Mempool, cborBytes []byte) error {
	t.Helper()
	return pool.AddTransaction(uint(dijkstra.TxTypeDijkstra), cborBytes)
}

func TestAddTransactionDijkstraBatchChildInputDoubleSpend(t *testing.T) {
	t.Parallel()
	shared := batchInput(0x41, 0)
	other := batchInput(0x42, 0)
	validator := newChainedOverlayValidator(shared, other)
	pool := newTestMempoolWithValidator(t, validator)
	defer pool.Stop(context.Background())

	_, batchCbor := buildDijkstraBatch(t, batchSpec{
		childInputs:  [][]batchOutRef{{shared}},
		childOutputs: [][]uint64{{1_000_000}},
		outputs:      []uint64{1},
	})
	require.NoError(t, addBatch(t, pool, batchCbor))

	t.Run("new top-level spend of a pending child input", func(t *testing.T) {
		_, cborBytes := buildDijkstraBatch(t, batchSpec{
			inputs:  []batchOutRef{shared},
			outputs: []uint64{2},
		})
		require.ErrorContains(t, addBatch(t, pool, cborBytes), "already spent")
	})
	t.Run("new child spend of a pending child input", func(t *testing.T) {
		_, cborBytes := buildDijkstraBatch(t, batchSpec{
			childInputs:  [][]batchOutRef{{shared}},
			childOutputs: [][]uint64{{3}},
			outputs:      []uint64{3},
		})
		require.ErrorContains(t, addBatch(t, pool, cborBytes), "already spent")
	})
	require.Len(t, pool.Transactions(), 1)

	t.Run("pending top-level spend then child spend", func(t *testing.T) {
		_, topCbor := buildDijkstraBatch(t, batchSpec{
			inputs:  []batchOutRef{other},
			outputs: []uint64{4},
		})
		require.NoError(t, addBatch(t, pool, topCbor))
		_, childCbor := buildDijkstraBatch(t, batchSpec{
			childInputs:  [][]batchOutRef{{other}},
			childOutputs: [][]uint64{{5}},
			outputs:      []uint64{5},
		})
		require.ErrorContains(t, addBatch(t, pool, childCbor), "already spent")
	})
}

func TestAddTransactionDijkstraBatchChildOutputChaining(t *testing.T) {
	t.Parallel()
	source := batchInput(0x51, 0)
	validator := newChainedOverlayValidator(source)
	pool := newTestMempoolWithValidator(t, validator)
	defer pool.Stop(context.Background())

	parent, parentCbor := buildDijkstraBatch(t, batchSpec{
		childInputs:  [][]batchOutRef{{source}},
		childOutputs: [][]uint64{{1_000_000}},
		outputs:      []uint64{1},
	})
	require.NoError(t, addBatch(t, pool, parentCbor))
	childID := parent.Body.TxSubTransactions.Items()[0].Body.Id()

	descendant, descendantCbor := buildDijkstraBatch(t, batchSpec{
		inputs:  []batchOutRef{{txID: childID.Bytes(), index: 0}},
		outputs: []uint64{900_000},
	})
	require.NoError(t, addBatch(t, pool, descendantCbor))
	require.Len(t, pool.Transactions(), 2)

	// Removing the parent drops the transaction chained onto its child output.
	pool.RemoveTransaction(parent.Hash().String())
	require.Empty(t, pool.Transactions())
	_, found := pool.GetTransaction(descendant.Hash().String())
	require.False(t, found)
}

func TestMempoolRevalidationDropsDescendantsOfBatchSpentByBlock(t *testing.T) {
	t.Parallel()
	source := batchInput(0x61, 0)
	validator := newChainedOverlayValidator(source)
	pool := newTestMempoolWithValidator(t, validator)
	defer pool.Stop(context.Background())

	parent, parentCbor := buildDijkstraBatch(t, batchSpec{
		childInputs:  [][]batchOutRef{{source}},
		childOutputs: [][]uint64{{1_000_000}},
		outputs:      []uint64{1},
	})
	require.NoError(t, addBatch(t, pool, parentCbor))
	childID := parent.Body.TxSubTransactions.Items()[0].Body.Id()
	_, descendantCbor := buildDijkstraBatch(t, batchSpec{
		inputs:  []batchOutRef{{txID: childID.Bytes(), index: 0}},
		outputs: []uint64{900_000},
	})
	require.NoError(t, addBatch(t, pool, descendantCbor))
	survivor, survivorCbor := buildDijkstraBatch(t, batchSpec{
		inputs:  []batchOutRef{batchInput(0x62, 0)},
		outputs: []uint64{700_000},
	})
	validator.mu.Lock()
	validator.onChain[refKey(batchInput(0x62, 0))] = struct{}{}
	validator.mu.Unlock()
	require.NoError(t, addBatch(t, pool, survivorCbor))
	require.Len(t, pool.Transactions(), 3)

	// A block consumes the parent's child-level input.
	validator.mu.Lock()
	validator.chainUsed[refKey(source)] = struct{}{}
	validator.mu.Unlock()
	require.NoError(t, pool.rebuildOverlay(context.Background()))

	remaining := pool.Transactions()
	require.Len(t, remaining, 1)
	require.Equal(t, survivor.Hash().String(), remaining[0].Hash)
}
