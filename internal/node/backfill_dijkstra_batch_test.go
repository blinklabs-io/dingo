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

package node

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func backfillTestRef(seed byte) []byte { return bytes.Repeat([]byte{seed}, 32) }

func backfillTestOutput(amount uint64) map[uint]any {
	return map[uint]any{
		0: append([]byte{0x60}, bytes.Repeat([]byte{0x42}, 28)...),
		1: amount,
	}
}

func backfillTestBatch(
	t *testing.T,
	childInput, topInput, collateral []byte,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	childBody, err := cbor.Encode(map[uint]any{
		0: []any{[]any{childInput, uint64(0)}},
		1: []any{backfillTestOutput(1_000_000)},
	})
	require.NoError(t, err)
	child, err := cbor.Encode([]any{cbor.RawMessage(childBody), map[uint]any{}, nil})
	require.NoError(t, err)
	body, err := cbor.Encode(map[uint]any{
		0:  []any{[]any{topInput, uint64(0)}},
		1:  []any{backfillTestOutput(2_000_000)},
		2:  uint64(5),
		13: []any{[]any{collateral, uint64(0)}},
		16: backfillTestOutput(3_000_000),
		23: cbor.NewSetType([]cbor.RawMessage{child}, true),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{cbor.RawMessage(body), map[uint]any{}, true, nil})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)
	return dijkstraTx
}

// TestBackfillDijkstraBatchUtxoEffects checks the UTxO set backfill leaves
// behind: a valid batch spends every level's inputs and creates every level's
// outputs, while a phase-2-invalid batch spends only its collateral.
func TestBackfillDijkstraBatchUtxoEffects(t *testing.T) {
	t.Parallel()
	for _, valid := range []bool{true, false} {
		name := "valid"
		if !valid {
			name = "phase-2-invalid"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			backfill := NewBackfill(db, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			childIn, topIn, collateral := backfillTestRef(0xe1), backfillTestRef(0xe2), backfillTestRef(0xe3)
			require.NoError(t, db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
				for _, id := range [][]byte{childIn, topIn, collateral} {
					if err := db.CreateUtxo(context.Background(), txn, &models.Utxo{
						TxId:       id,
						OutputIdx:  0,
						PaymentKey: bytes.Repeat([]byte{0x42}, 28),
						AddedSlot:  1,
						Amount:     dbtypes.Uint64(5_000_000),
					}); err != nil {
						return err
					}
				}
				return nil
			}))
			tx := backfillTestBatch(t, childIn, topIn, collateral)
			childID := tx.Body.TxSubTransactions.Items()[0].Body.Id()

			block := &dijkstra.DijkstraBlock{
				BlockHeader: &dijkstra.DijkstraBlockHeader{
					BabbageBlockHeader: babbage.BabbageBlockHeader{
						Body: babbage.BabbageBlockHeaderBody{
							BlockNumber:  1,
							Slot:         1000,
							ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
						},
					},
				},
				BlockBody: dijkstra.DijkstraBlockBody{
					Transactions: []dijkstra.DijkstraTransaction{*tx},
				},
			}
			bodyCbor, err := block.BlockBody.MarshalCBOR()
			require.NoError(t, err)
			block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
			blockCbor, err := block.MarshalCBOR()
			require.NoError(t, err)
			block.SetCbor(blockCbor)
			point := ocommon.Point{Slot: 1000, Hash: bytes.Repeat([]byte{0xe4}, 32)}
			offsets, err := database.NewBlockIndexer(point.Slot, point.Hash).
				ComputeOffsets(blockCbor, block)
			require.NoError(t, err)
			if !valid {
				// Only a decoded-and-flagged batch can be invalid; the indexer
				// saw a valid one and so computed no collateral-return offset.
				tx.TxIsValid = false
				var sample database.CborOffset
				for _, offset := range offsets.UtxoOffsets {
					sample = offset
					break
				}
				var id [32]byte
				copy(id[:], tx.Hash().Bytes())
				offsets.UtxoOffsets[database.UtxoRef{TxId: id, OutputIdx: 1}] = sample
			}

			acc := db.NewBatchAccumulator()
			require.NoError(t, db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
				if err := backfill.processBlockTxsBatched(context.Background(),
					[]lcommon.Transaction{tx},
					point,
					12,
					dijkstra.EraIdDijkstra,
					&dijkstra.DijkstraProtocolParameters{},
					offsets,
					acc,
					txn,
					nil,
					false,
				); err != nil {
					return err
				}
				return db.FlushBatch(acc, txn)
			}))

			live := func(id []byte, idx uint32) bool {
				utxo, err := db.Metadata().GetUtxo(id, idx, nil)
				require.NoError(t, err)
				return utxo != nil
			}
			require.Equal(t, !valid, live(childIn, 0), "child input")
			require.Equal(t, !valid, live(topIn, 0), "top-level input")
			require.Equal(t, valid, live(collateral, 0), "collateral")
			require.Equal(t, valid, live(childID.Bytes(), 0), "child output")
			require.Equal(t, valid, live(tx.Hash().Bytes(), 0), "top-level output")
			require.Equal(t, !valid, live(tx.Hash().Bytes(), 1), "collateral return")
		})
	}
}
