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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// processFixtureBlock applies a block of the given transactions through
// ledgerProcessBlock with validation on.
func processFixtureBlock(
	t *testing.T,
	fx *utxoMemoPreprodFixture,
	txs []lcommon.Transaction,
) error {
	t.Helper()
	block := &validityOutcomeTestBlock{
		header: fx.block.Header(),
		txs:    txs,
		era:    conway.EraConway,
	}
	offsets := &database.BlockIngestionResult{
		TxOffsets:   map[[32]byte]database.CborOffset{},
		UtxoOffsets: map[database.UtxoRef]database.CborOffset{},
	}
	for _, tx := range txs {
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		offsets.TxOffsets[txHash] = database.CborOffset{
			BlockSlot: fx.blockSlot, ByteLength: 1,
		}
		for _, utxo := range tx.Produced() {
			var producedHash [32]byte
			copy(producedHash[:], utxo.Id.Id().Bytes())
			offsets.UtxoOffsets[database.UtxoRef{
				TxId:      producedHash,
				OutputIdx: uint32(utxo.Id.Index()), //nolint:gosec // small fixture index
			}] = database.CborOffset{BlockSlot: fx.blockSlot, ByteLength: 1}
		}
	}
	point := ocommon.NewPoint(fx.blockSlot, block.Hash().Bytes())
	return fx.db.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			_, err := fx.dingoLS.ledgerProcessBlock(
				context.Background(),
				txn,
				point,
				block,
				true,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				offsets,
				eras.ConwayEraDesc,
				fx.dingoLS.currentPParams,
				nil,
				0,
				0,
				false,
			)
			return err
		})
}

// TestLedgerProcessBlockPrefetchesUtxosInOneBatch checks that block
// application resolves the block's input UTxOs with one UtxosByRefs query
// instead of one point lookup per ref, while still reading each distinct ref
// from the database once.
func TestLedgerProcessBlockPrefetchesUtxosInOneBatch(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)

	beforeReads := fx.dingoLS.utxoByRefReads.Load()
	beforeBatches := fx.dingoLS.utxoBatchLookups.Load()
	require.NoError(t, processFixtureBlock(t, fx, []lcommon.Transaction{fx.tx}))

	require.Equal(
		t,
		uint64(1),
		fx.dingoLS.utxoBatchLookups.Load()-beforeBatches,
		"block application must prefetch input UTxOs with one batch query",
	)
	require.Equal(
		t,
		uint64(distinctUtxoRefs(fx.tx)), //nolint:gosec // small count
		fx.dingoLS.utxoByRefReads.Load()-beforeReads,
		"each distinct ref must still be read from the database once",
	)
}

// TestLedgerProcessBlockPrefetchDoesNotResurrectSpentUtxos checks that an
// input spent by an earlier transaction in the block is not answered from
// the prefetched snapshot: a second transaction spending it must be rejected.
func TestLedgerProcessBlockPrefetchDoesNotResurrectSpentUtxos(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)

	err := processFixtureBlock(
		t, fx, []lcommon.Transaction{fx.tx, fx.tx},
	)
	require.Error(t, err, "double spend within one block must be rejected")
}

// TestForgetSpentPrefetchedUtxosDropsSubTransactionInputs checks that an
// input consumed by a Dijkstra sub-transaction is dropped from the prefetched
// set once the enclosing transaction is applied. The enclosing transaction's
// Inputs() lists only its own body's inputs, so a later transaction in the
// block spending the same ref would otherwise be answered from the snapshot.
func TestForgetSpentPrefetchedUtxosDropsSubTransactionInputs(t *testing.T) {
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
		[]any{cbor.RawMessage(body), map[uint]any{}, nil},
	)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	levels := TransactionLevels(tx)
	require.Len(t, levels, 2)
	require.Len(t, levels[0].Inputs(), 1)
	require.Empty(t, tx.Inputs())

	key := utxoref.ForInput(levels[0].Inputs()[0])
	prefetched := map[utxoref.Key]lcommon.Utxo{
		key: {Id: levels[0].Inputs()[0]},
	}
	forgetSpentPrefetchedUtxos(prefetched, tx)
	require.NotContains(
		t,
		prefetched,
		key,
		"an input spent by a sub-transaction must leave the prefetched set",
	)
}
