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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An input the live UTxO set shows a block at or below the applied ledger tip
// consuming is a bad input the failing block carries: no rollback makes it
// spendable, so recovery does not rewind. An input its producer created that
// the set never held, or one consumed only above the durably applied state, is
// local state a rollback to the producer repairs.
func TestReplayRecoverySpentInputReadsTheLiveUtxoSet(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		// produced adds the output in the producer block, spent consumes it
		// in the spender block.
		produced, spent bool
		// invalid marks the producer as failing phase-2 validation.
		invalid bool
		// floorAtProducer puts the durably applied state at the producer,
		// below the ledger tip.
		floorAtProducer bool
		// pruneBelow drops the journal at and below that slot.
		pruneBelow uint64
		// sameBlock has a later transaction of the producer's block consume
		// the output, so the set never shows it.
		sameBlock bool
		// genesis makes the producer a genesis transaction, at origin.
		genesis       bool
		wantRecovered bool
	}{
		{name: "consumed at the applied ledger tip", produced: true, spent: true},
		{
			name:     "consumed above the durably applied state",
			produced: true, spent: true, floorAtProducer: true,
			wantRecovered: true,
		},
		{name: "never held though its producer applied", wantRecovered: true},
		{name: "never created by a failed producer", invalid: true},
		{name: "consumed in its producer's block", sameBlock: true},
		{name: "genesis output consumed", genesis: true, spent: true},
		{
			name:     "consumed with the producer below the journal",
			produced: true, spent: true, pruneBelow: 120,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
			})
			require.NoError(t, err)
			cm, err := chain.NewManager(context.Background(), db, nil)
			require.NoError(t, err)

			parentBlock := testRawBlock("set-spent-parent", 100, 1, nil)
			producerBlock := testRawBlock(
				"set-spent-producer", 120, 2, parentBlock.Hash,
			)
			producerTxHash := testHashBytes("set-spent-producer-tx")
			if tc.sameBlock {
				pay := rowBatchingBaseAddress(t, 0x44, 0x55)
				produce := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{testHashBytes("set-spent-funds"), uint64(0)}},
					1: []any{map[uint]any{0: pay, 1: uint64(2_000_000)}},
					2: uint64(100_000),
				})
				consume := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{produce.hash, uint64(0)}},
					1: []any{map[uint]any{0: pay, 1: uint64(1_900_000)}},
					2: uint64(100_000),
				})
				block := rowBatchingBlock(
					t, 120, 2, lcommon.NewBlake2b256(parentBlock.Hash),
					produce, consume,
				)
				producerBlock = chain.RawBlock{
					Slot:        block.SlotNumber(),
					Hash:        block.Hash().Bytes(),
					BlockNumber: block.BlockNumber(),
					Type:        gledger.BlockTypeConway,
					PrevHash:    bytes.Clone(parentBlock.Hash),
					Cbor:        block.Cbor(),
				}
				producerTxHash = produce.hash
			}
			spenderBlock := testRawBlock(
				"set-spent-spender", 140, 3, producerBlock.Hash,
			)
			failingBlock := testRawBlock(
				"set-spent-failing", 160, 4, spenderBlock.Hash,
			)
			require.NoError(t, cm.PrimaryChain().AddRawBlocks(
				context.Background(),
				[]chain.RawBlock{
					parentBlock, producerBlock, spenderBlock, failingBlock,
				},
			))
			chainTip := rawBlockTip(failingBlock)

			ls, err := newReplayRecoveryLedgerState(t, LedgerStateConfig{
				Database:          db,
				ChainManager:      cm,
				CardanoNodeConfig: newTestShelleyGenesisCfg(t),
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			})
			require.NoError(t, err)
			ls.metrics.init(prometheus.NewRegistry())

			ref := models.UtxoId{Hash: producerTxHash, Idx: 0}
			parentPoint := rawBlockTip(parentBlock).Point
			producerPoint := rawBlockTip(producerBlock).Point
			spenderPoint := rawBlockTip(spenderBlock).Point
			indexPoint := producerPoint
			if tc.genesis {
				fxAddOutput(t, ls, producerTxHash)
				indexPoint = ocommon.Point{Hash: testHashBytes("set-spent-genesis")}
			}
			fxCommitAt(t, db, parentPoint, LedgerChanges{})
			produce := UtxoEvent{Point: producerPoint}
			if tc.produced {
				produce.Added = []UtxoOutput{{
					Ref: ref, Cbor: boundaryOutputCbor(t, 1_000_000),
				}}
			}
			fxCommitAt(t, db, producerPoint, LedgerChanges{
				Utxo: []UtxoEvent{produce},
			})
			spend := UtxoEvent{Point: spenderPoint}
			if tc.spent {
				spend.Removed = []models.UtxoId{ref}
			}
			fxCommitAt(t, db, spenderPoint, LedgerChanges{
				Utxo: []UtxoEvent{spend},
			})
			ls.stageTxIndex(
				nil, producerTxHash, indexPoint, 0, 1, !tc.invalid,
			)
			if tc.pruneBelow != 0 {
				fxPruneJournal(t, ls, tc.pruneBelow)
			}

			ledgerTip := rawBlockTip(spenderBlock)
			floor := ledgerTip
			if tc.floorAtProducer {
				floor = rawBlockTip(producerBlock)
			}
			require.NoError(t, seedBlockNonce(t, db,
				floor.Point.Hash, floor.Point.Slot,
				[]byte("nonce-floor"), false, nil,
			))
			require.NoError(t, db.SetTip(ledgerTip, nil))
			ls.currentTip = ledgerTip
			ls.currentTipBlockNonce = []byte("nonce-floor")
			ls.publishSnapshotsLocked()
			require.False(t, fxOutputLive(t, ls, producerTxHash))

			recovered, err := ls.tryRecoverFromTxValidationError(
				&txValidationError{
					BlockPoint: chainTip.Point,
					TxHash:     testHashBytes("set-spent-failing-tx"),
					Inputs: []lcommon.TransactionInput{
						&replayRecoveryInput{txId: producerTxHash, index: 0},
					},
					Cause: fmt.Errorf(
						"shelley utxo validation rule 8: %w",
						shelley.BadInputsUtxoError{},
					),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantRecovered, recovered)
			if !tc.wantRecovered {
				assert.Equal(t, ledgerTip, ls.currentTip,
					"a spent input must not rewind the ledger")
				assert.Equal(t, chainTip, ls.chain.Tip())
			}
		})
	}
}
