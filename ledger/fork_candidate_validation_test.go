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
	"errors"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForkCandidateTransactionDeltasReadCandidateConsensus processes a block
// with validation on, which applies each transaction's own delta inside
// ledgerProcessBlock. A governance transaction in an epoch the live cache
// lacks resolves its epoch only from the candidate's consensus view.
func TestForkCandidateTransactionDeltasReadCandidateConsensus(t *testing.T) {
	t.Parallel()

	process := func(t *testing.T, candidate bool) error {
		t.Helper()
		ls, _ := newSubGovLedger(t)
		ls.config.SkipDijkstraTxValidation = true
		tx := subGovTx(t, true, nil, dijkstra.DijkstraSubTransactionBody{
			TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
				subGovInfoProposal(t, 0x61),
			},
		})
		const slot = 15_000
		block := &dijkstra.DijkstraBlock{
			BlockHeader: &dijkstra.DijkstraBlockHeader{
				BabbageBlockHeader: babbage.BabbageBlockHeader{
					Body: babbage.BabbageBlockHeaderBody{
						BlockNumber:  1,
						Slot:         slot,
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
		blockHash := bytes.Repeat([]byte{0x7a}, 32)
		cbor, err := block.MarshalCBOR()
		require.NoError(t, err)
		block.SetCbor(cbor)
		offsets, err := database.NewBlockIndexer(slot, blockHash).
			ComputeOffsets(cbor, block)
		require.NoError(t, err)
		live := ls.loadConsensusSnapshot()
		pparams, ok := ls.currentPParams.(*dijkstra.DijkstraProtocolParameters)
		require.True(t, ok)
		pparams.MaxBlockBodySize = 100_000
		pparams.MaxBlockHeaderSize = 100_000
		return ls.db.Transaction(context.Background(), true).Do(
			func(txn *database.Txn) error {
				if candidate {
					snapshot := *live
					snapshot.epochCache = append(
						slices.Clone(live.epochCache),
						models.Epoch{
							EpochId:       13,
							StartSlot:     10_000,
							LengthInSlots: 10_000,
							EraId:         eras.DijkstraEraDesc.Id,
						},
					)
					ls.utxoSetBatchForTxn(dbadapter.Txn(txn), func(b *utxoSetBatch) {
						b.validationOnly = true
					})
					ls.setForkValidationConsensus(dbadapter.Txn(txn), &snapshot)
				}
				delta, err := ls.ledgerProcessBlock(
					context.Background(), txn,
					ocommon.Point{Slot: slot, Hash: blockHash}, block,
					true, false, false, nil, envelopeParent{}, offsets,
					eras.DijkstraEraDesc, pparams, nil, 12, 0, false,
				)
				if delta != nil {
					delta.Release()
				}
				return err
			},
		)
	}

	t.Run("live view lacks the epoch", func(t *testing.T) {
		t.Parallel()
		require.ErrorContains(
			t, process(t, false), "resolve governance block epoch",
		)
	})
	t.Run("candidate view holds it", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, process(t, true))
	})
}

// TestForkCandidateStagingFailureIsNotFatal stages an account failure in a
// validation-only transaction, which rejects the candidate, and in an
// ordinary one, which stops the live set.
func TestForkCandidateStagingFailureIsNotFatal(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, validation bool) (fatal int, validationErr error, failed bool) {
		t.Helper()
		ls, db := newSubGovLedger(t)
		ls.config.FatalErrorFunc = func(error) { fatal++ }
		txn := db.Transaction(context.Background(), true)
		defer txn.Release()
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			wrapped := dbadapter.Txn(txn)
			if validation {
				ls.utxoSetBatchForTxn(wrapped, func(b *utxoSetBatch) {
					b.validationOnly = true
				})
			}
			ls.failAccountStaging(wrapped, errors.New("read account"))
			validationErr = ls.utxoSetValidationError(wrapped)
			return nil
		}))
		return fatal, validationErr, ls.utxoSetFailed.Load()
	}

	fatal, validationErr, failed := run(t, true)
	assert.Zero(t, fatal)
	assert.ErrorContains(t, validationErr, "read account")
	assert.False(t, failed)

	fatal, validationErr, failed = run(t, false)
	assert.Equal(t, 1, fatal)
	assert.NoError(t, validationErr)
	assert.True(t, failed)
}

// TestForkCandidateStagingUsesCandidateEpoch records a donation in a
// validation-only transaction whose candidate is in a later epoch than the
// live one.
func TestForkCandidateStagingUsesCandidateEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newSubGovLedger(t)
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		wrapped := dbadapter.Txn(txn)
		assert.Equal(t, uint64(12), ls.stagingEpoch(wrapped))
		live := ls.loadConsensusSnapshot()
		snapshot := *live
		snapshot.currentEpoch.EpochId = 13
		ls.utxoSetBatchForTxn(wrapped, func(b *utxoSetBatch) {
			b.validationOnly = true
		})
		ls.setForkValidationConsensus(wrapped, &snapshot)
		assert.Equal(t, uint64(13), ls.stagingEpoch(wrapped))
		return nil
	}))
}

// TestForkCandidateKeepsInFlightCandidateWhenExtended sends a longer path
// from the same peer while the candidate's bodies are in flight. The
// candidate keeps running and remembers the extra headers.
func TestForkCandidateKeepsInFlightCandidateWhenExtended(t *testing.T) {
	t.Parallel()

	fixture := newForkCandidateFixture(t, 432000)
	blocks, err := fixtures.GenerateAlonzoChain(
		fixture.ancestorTip.BlockNumber+1,
		common.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		fixture.currentTip.Point.Slot+9,
		1,
		3,
	)
	require.NoError(t, err)
	events := make([]ChainsyncEvent, len(blocks))
	for i, block := range blocks {
		events[i] = ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				block.SlotNumber(), block.Hash().Bytes(),
			),
			BlockHeader: block.Header(),
			Type:        uint(block.Type()),
		}
	}
	requests := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId, ocommon.Point, ocommon.Point,
	) (uint64, error) {
		requests++
		return 17, nil
	}
	t.Cleanup(func() {
		fixture.ls.chainsyncBlockfetchMutex.Lock()
		fixture.ls.discardForkCandidateLocked()
		fixture.ls.chainsyncBlockfetchMutex.Unlock()
	})

	require.NoError(t, fixture.ls.startForkCandidateValidation(
		fixture.connId, fixture.ancestorTip.Point, fixture.currentTip,
		events[:2],
	))
	first := fixture.ls.forkCandidate
	require.NotNil(t, first)
	require.NoError(t, fixture.ls.startForkCandidateValidation(
		fixture.connId, fixture.ancestorTip.Point, fixture.currentTip,
		events,
	))
	require.Same(t, first, fixture.ls.forkCandidate)
	assert.Equal(t, 1, requests)
	require.Len(t, first.tail, 1)
	assert.Equal(t, events[2].Point, first.tail[0].Point)

	other := []ChainsyncEvent{events[0], {
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(events[1].Point.Slot+1, bytes.Repeat([]byte{9}, 32)),
		BlockHeader:  events[1].BlockHeader,
	}}
	require.NoError(t, fixture.ls.startForkCandidateValidation(
		fixture.connId, fixture.ancestorTip.Point, fixture.currentTip, other,
	))
	assert.NotSame(t, first, fixture.ls.forkCandidate)
	assert.Equal(t, 2, requests)
}

// TestForkCandidateInstallFailureRequestsResync fails the install after the
// rollback to the ancestor. The peer does not resend the dropped suffix, so
// the ledger must clear the queue and re-intersect with it.
func TestForkCandidateInstallFailureRequestsResync(t *testing.T) {
	t.Parallel()

	fixture := newForkCandidateFixture(t, 432000)
	blocks, err := fixtures.GenerateAlonzoChain(
		fixture.ancestorTip.BlockNumber+1,
		common.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		fixture.currentTip.Point.Slot+9,
		1,
		2,
	)
	require.NoError(t, err)
	unrelated, err := fixtures.GenerateAlonzoChain(
		fixture.ancestorTip.BlockNumber+7,
		common.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32)),
		fixture.currentTip.Point.Slot+20,
		1,
		2,
	)
	require.NoError(t, err)

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	fixture.ls.config.EventBus = bus
	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subID := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if e, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				resyncCh <- e
			}
		},
	)
	t.Cleanup(func() { bus.Unsubscribe(event.ChainsyncResyncEventType, subID) })

	block := blocks[0]
	point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	candidate := &forkCandidate{
		connection: fixture.connId,
		ancestor:   fixture.ancestorTip.Point,
		localTip:   fixture.currentTip,
		generation: fixture.ls.CurrentHeaderCryptoVerificationGeneration(),
		path: []ChainsyncEvent{{
			ConnectionId: fixture.connId,
			Point:        point,
			BlockHeader:  unrelated[0].Header(),
			Type:         uint(block.Type()),
		}},
		blocks: []BlockfetchEvent{{
			ConnectionId: fixture.connId, Point: point, Block: block,
		}},
	}
	fixture.ls.finishForkCandidate(candidate)

	testutil.WaitForCondition(t, func() bool {
		for {
			select {
			case e := <-resyncCh:
				if e.Reason == event.ChainsyncResyncReasonPersistentFork {
					assert.Equal(t, fixture.connId, e.ConnectionId)
					return true
				}
			default:
				return false
			}
		}
	}, testutil.AsyncWait, "install failure must request a chainsync re-sync")
	assert.Zero(t, fixture.ls.chain.HeaderCount())
	assert.Equal(t, fixture.ancestorTip.Point.Slot, fixture.ls.chain.Tip().Point.Slot)
}

// TestForkCandidateInstallDiscardsSupersededBatchBlocks leaves a block of a
// batch the rollback superseded pending when the candidate installs. It is
// dropped, and the batch stays superseded afterwards.
func TestForkCandidateInstallDiscardsSupersededBatchBlocks(t *testing.T) {
	t.Parallel()

	fixture := newForkCandidateFixture(t, 432000)
	ls := fixture.ls
	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchBatchRollbackGeneration = ls.blockfetchRollbackGeneration.Load()
	ls.blockfetchBatchChainGeneration = ls.chainRollbackGeneration.Load()
	ls.pendingBlockfetchEvents = []BlockfetchEvent{{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(99, bytes.Repeat([]byte{0x31}, 32)),
	}}
	ls.blockfetchRollbackGeneration.Add(1)
	ls.chainsyncBlockfetchMutex.Unlock()

	_, err := ls.installForkCandidate(&forkCandidate{}, nil)
	require.NoError(t, err)

	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	assert.Empty(t, ls.pendingBlockfetchEvents)
	assert.False(t, ls.blockfetchBatchStillCurrent())
}
