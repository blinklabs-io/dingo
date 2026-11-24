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
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deliverForkCandidate drives a preferred fork whose headers are blocks'
// headers through tryResolveFork and answers the candidate's blockfetch
// request with the blocks, then waits for the validation worker. The
// fork's first block must extend the fixture's ancestor.
func deliverForkCandidate(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
	blocks []gledger.Block,
) {
	t.Helper()
	request := captureForkCandidateRequest(t, fixture)
	for _, block := range blocks[:len(blocks)-1] {
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: fixture.connId,
			HeaderCryptoVerificationGeneration: fixture.ls.
				loadConsensusSnapshot().generation,
			Point: ocommon.NewPoint(
				block.SlotNumber(), block.Hash().Bytes(),
			),
			BlockHeader: block.Header(),
			Type:        uint(block.Type()),
		})
	}
	tipBlock := blocks[len(blocks)-1]
	err := fixture.ls.chain.AddBlockHeader(
		context.Background(), tipBlock.Header(),
	)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)
	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				tipBlock.SlotNumber(), tipBlock.Hash().Bytes(),
			),
			BlockHeader: tipBlock.Header(),
			Type:        uint(tipBlock.Type()),
		},
		notFitErr,
		nil,
		0,
	)
	require.NoError(t, err)
	require.True(t, resolved)
	require.Equal(t, blocks[0].SlotNumber(), request.start.Slot)
	require.Equal(t, tipBlock.SlotNumber(), request.end.Slot)

	for _, block := range blocks {
		fixture.ls.handleEventBlockfetch(event.NewEvent(
			BlockfetchEventType,
			BlockfetchEvent{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					block.SlotNumber(), block.Hash().Bytes(),
				),
				Block:     block,
				Type:      uint(block.Type()),
				RequestId: request.id,
			},
		))
	}
	fixture.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: fixture.connId,
			RequestId:    request.id,
			BatchDone:    true,
		},
	))
	fixture.ls.blockfetchContinuationMu.Lock()
	fixture.ls.blockfetchContinuationWG.Wait()
	fixture.ls.blockfetchContinuationMu.Unlock()
}

// newForkCandidateFixture is the rollback fixture over a ledger that holds the
// epoch state a running node has at the ancestor: epoch zero of the era the
// fork's blocks are in, with the ledger's live epoch cache the same. Its
// ancestor and current blocks are real Alonzo blocks, so the candidate
// validation can decode its parent.
func newForkCandidateFixture(
	t *testing.T,
	epochLength uint,
) *chainsyncRollbackFixture {
	t.Helper()
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: epochLength,
		SlotLength:    1000,
		EraId:         eras.AlonzoEraDesc.Id,
		Nonce:         bytes.Repeat([]byte{0x2a}, 32),
	}
	local, err := fixtures.GenerateAlonzoChain(
		1, lcommon.Blake2b256{}, 10, 10, 2,
	)
	require.NoError(t, err)
	raw := make([]chain.RawBlock, len(local))
	for i, block := range local {
		raw[i] = chain.RawBlock{
			Slot:        block.SlotNumber(),
			Hash:        block.Hash().Bytes(),
			BlockNumber: block.BlockNumber(),
			Type:        uint(block.Type()),
			Cbor:        block.Cbor(),
		}
		if i > 0 {
			raw[i].PrevHash = raw[i-1].Hash
		}
	}
	db := newTestDB(t)
	OpenLiveSetForTest(t, db)
	fixture := newChainsyncRollbackFixtureWithBlocks(
		t, db, raw[0], raw[1], func() {
			fxCommitAt(t, db, ocommon.NewPoint(
				raw[0].Slot, raw[0].Hash,
			), LedgerChanges{
				Epochs: []models.Epoch{epoch},
				RewardBlocks: []RewardBlockChange{{
					Slot: raw[0].Slot, Epoch: 0, Open: true,
				}},
			})
		},
	)
	fixture.ls.config.CardanoNodeConfig.ShelleyGenesisHash = hex.EncodeToString(
		testHashBytes("shelley-genesis"),
	)
	fixture.ls.epochCache = []models.Epoch{epoch}
	fixture.ls.currentEpoch = epoch
	fixture.ls.currentEra = eras.AlonzoEraDesc
	fixture.ls.publishSnapshotsLocked()
	return fixture
}

// runLedgerPipeline applies the blocks the primary chain gains, as the
// running node's block pipeline does, until the test ends.
func runLedgerPipeline(t *testing.T, fixture *chainsyncRollbackFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	attempt := func(attemptCtx context.Context) error {
		return fixture.ls.runLedgerReadChainAttempt(
			attemptCtx,
			fixture.ls.ledgerReadChain,
			fixture.ls.ledgerProcessBlocksFromSource,
		)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.ls.ledgerProcessBlocksWithAttempt(ctx, attempt)
	}()
	t.Cleanup(func() {
		cancel()
		testutil.RequireReceive(
			t, done, testutil.AsyncWait,
			"the ledger pipeline must stop with its context",
		)
	})
}

// requireLedgerTipAt waits for the ledger to apply blocks up to tip.
func requireLedgerTipAt(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
	tip gledger.Block,
) {
	t.Helper()
	testutil.WaitForCondition(t, func() bool {
		fixture.ls.RLock()
		defer fixture.ls.RUnlock()
		return fixture.ls.currentTip.Point.Slot == tip.SlotNumber()
	}, testutil.AsyncWait, "the ledger must apply the validated fork")
	fixture.ls.RLock()
	ledgerTip := fixture.ls.currentTip
	fixture.ls.RUnlock()
	assert.Equal(t, tip.Hash().Bytes(), ledgerTip.Point.Hash)
	assert.Equal(t, tip.BlockNumber(), ledgerTip.BlockNumber)
}

func TestForkCandidateValidBodiesSwitchSelectedChain(t *testing.T) {
	t.Parallel()

	fixture := newForkCandidateFixture(t, 432000)
	blocks, err := fixtures.GenerateAlonzoChain(
		fixture.ancestorTip.BlockNumber+1,
		lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		fixture.currentTip.Point.Slot+9,
		1,
		2,
	)
	require.NoError(t, err)

	runLedgerPipeline(t, fixture)
	deliverForkCandidate(t, fixture, blocks)

	tip := blocks[len(blocks)-1]
	assert.Equal(t, tip.SlotNumber(), fixture.ls.chain.Tip().Point.Slot)
	assert.Equal(t, tip.Hash().Bytes(), fixture.ls.chain.Tip().Point.Hash)
	requireLedgerTipAt(t, fixture, tip)
}

func TestForkCandidateCrossingEpochBoundarySwitchesSelectedChain(t *testing.T) {
	t.Parallel()

	const epochLength = 25
	fixture := newForkCandidateFixture(t, epochLength)
	blocks, err := fixtures.GenerateAlonzoChain(
		fixture.ancestorTip.BlockNumber+1,
		lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		fixture.currentTip.Point.Slot+4,
		2,
		2,
	)
	require.NoError(t, err)
	require.Less(t, blocks[0].SlotNumber(), uint64(epochLength))
	require.GreaterOrEqual(t, blocks[1].SlotNumber(), uint64(epochLength))
	require.Len(t, fixture.ls.loadConsensusSnapshot().epochCache, 1)

	runLedgerPipeline(t, fixture)
	deliverForkCandidate(t, fixture, blocks)

	tip := blocks[len(blocks)-1]
	assert.Equal(t, tip.SlotNumber(), fixture.ls.chain.Tip().Point.Slot)
	assert.Equal(t, tip.Hash().Bytes(), fixture.ls.chain.Tip().Point.Hash)
	requireLedgerTipAt(t, fixture, tip)
	// Validating an epoch the live cache lacks must leave the live UTxO set
	// following the ledger.
	assert.False(t, fixture.ls.utxoSetFailed.Load())
}

// TestLedgerDeltaReadsCandidateConsensusForUnpublishedEpoch applies a
// governance transaction in an epoch the live epoch cache does not hold yet,
// as a fork candidate that crosses a boundary does. The governance rules
// resolve the block's epoch and parameters from the delta's consensus
// snapshot, so the candidate's own must replace the live one.
func TestLedgerDeltaReadsCandidateConsensusForUnpublishedEpoch(t *testing.T) {
	t.Parallel()

	apply := func(t *testing.T, candidate bool) (*LedgerState, error) {
		t.Helper()
		db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

		pparams := mockledger.NewMockConwayProtocolParams()
		pparams.GovActionValidityPeriod = 20
		pparams.DRepInactivityPeriod = 20
		ls := &LedgerState{
			db:             db,
			currentPParams: &pparams,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		}
		attachTestLiveSet(t, ls)
		publishGovernanceTestEpoch(ls, 12, eras.ConwayEraDesc)

		rewardAddress, err := lcommon.NewAddressFromBytes(
			append([]byte{0xE1}, bytes.Repeat([]byte{0xAB}, 28)...),
		)
		require.NoError(t, err)
		procedure, err := conway.NewConwayProposalProcedure(
			42,
			rewardAddress,
			&lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)},
			lcommon.GovAnchor{Url: "https://example.com/candidate"},
		)
		require.NoError(t, err)
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(bytes.Repeat([]byte{0x53}, lcommon.Blake2b256Size))
		tx.WithType(gledger.TxTypeConway)
		tx.WithProposalProcedures(procedure)
		tx.WithValid(true)

		const slot = 15_000
		delta := NewLedgerDelta(
			ocommon.NewPoint(slot, bytes.Repeat([]byte{0x22}, 32)),
			uint(conway.EraIdConway),
			1,
		)
		defer delta.Release()
		delta.addTransaction(tx, 0)
		var hash [32]byte
		copy(hash[:], tx.Hash().Bytes())
		delta.Offsets = &database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{
				hash: {},
			},
			UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
		}
		if candidate {
			live := ls.loadConsensusSnapshot()
			require.NotNil(t, live)
			snapshot := *live
			snapshot.epochCache = append(
				slices.Clone(live.epochCache),
				models.Epoch{
					EpochId:       13,
					StartSlot:     10_000,
					LengthInSlots: 10_000,
					EraId:         eras.ConwayEraDesc.Id,
				},
			)
			delta.consensus = &snapshot
		}
		txn := db.Transaction(t.Context(), true)
		return ls, txn.Do(func(txn *database.Txn) error {
			return delta.apply(t.Context(), ls, txn)
		})
	}

	t.Run("live snapshot lacks the epoch", func(t *testing.T) {
		t.Parallel()
		_, err := apply(t, false)
		require.ErrorContains(t, err, "resolve governance block epoch")
	})
	t.Run("candidate snapshot holds it", func(t *testing.T) {
		t.Parallel()
		_, err := apply(t, true)
		require.NoError(t, err)
	})
}

// TestForkCandidateInstallRestampsStaleHeaderGeneration checks that a header
// carrying the generation published before the rollback leaves the install
// authorized by the current one, and that an unstamped header stays so.
func TestForkCandidateInstallRestampsStaleHeaderGeneration(t *testing.T) {
	t.Parallel()

	fixture := newForkCandidateFixture(t, 432000)
	ls := fixture.ls
	stale := ls.loadConsensusSnapshot().generation
	ls.Lock()
	ls.publishSnapshotsLocked()
	ls.Unlock()
	current := ls.CurrentHeaderCryptoVerificationGeneration()
	require.NotEqual(t, stale, current)

	got := ls.restampForkHeaderAfterRollback(ChainsyncEvent{
		HeaderCryptoVerificationGeneration: stale,
	})
	assert.Equal(t, current, got.HeaderCryptoVerificationGeneration)
	got = ls.restampForkHeaderAfterRollback(ChainsyncEvent{})
	assert.Zero(t, got.HeaderCryptoVerificationGeneration)
}
