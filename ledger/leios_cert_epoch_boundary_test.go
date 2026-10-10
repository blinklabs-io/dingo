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
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gconway "github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// leiosBoundaryTestBlock builds a decoded Dijkstra block with a Leios header
// extension. announce is the endorser block the block announces (nil for
// none); a certifying block carries a certificate and no transactions.
func leiosBoundaryTestBlock(
	t *testing.T,
	blockNumber uint64,
	slot uint64,
	prevHash lcommon.Blake2b256,
	certifies bool,
	announce []byte,
	transactions ...cbor.RawMessage,
) *gdijkstra.DijkstraBlock {
	t.Helper()
	var certField any
	if certifies {
		certField = []any{[]byte{0x80}, make([]byte, 48)}
	}
	blockTransactions := make([]any, 0, len(transactions))
	for _, raw := range transactions {
		var fields []cbor.RawMessage
		_, err := cbor.Decode(raw, &fields)
		require.NoError(t, err)
		require.Len(t, fields, 3)
		blockTransactions = append(blockTransactions, []any{
			fields[0], fields[1], fields[2], true,
		})
	}
	bodyCbor, err := cbor.Encode([]any{blockTransactions, certField, nil})
	require.NoError(t, err)
	var body gdijkstra.DijkstraBlockBody
	_, err = cbor.Decode(bodyCbor, &body)
	require.NoError(t, err)

	headerBodyCbor, err := cbor.Encode(&babbage.BabbageBlockHeaderBody{
		BlockNumber:   blockNumber,
		Slot:          slot,
		PrevHash:      prevHash,
		BlockBodySize: uint64(len(bodyCbor)),
		BlockBodyHash: body.Hash(),
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: gdijkstra.MinProtocolVersionDijkstra,
		},
	})
	require.NoError(t, err)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(headerBodyCbor, &headerBody)
	require.NoError(t, err)
	var announcement any
	if announce != nil {
		announcement = []any{announce, uint64(4096)}
	}
	headerBody = append(
		headerBody,
		leiosTestRaw(t, certifies),
		leiosTestRaw(t, announcement),
	)
	headerCbor, err := cbor.Encode([]any{headerBody, []byte{}})
	require.NoError(t, err)
	blockCbor, err := cbor.Encode([]cbor.RawMessage{
		cbor.RawMessage(headerCbor),
		cbor.RawMessage(bodyCbor),
	})
	require.NoError(t, err)
	block, err := gdijkstra.NewDijkstraBlockFromCbor(blockCbor)
	require.NoError(t, err)
	return block
}

// TestLedgerProcessBlocksDefersLeiosCertificateCheckPastEpochBoundary
// covers one read batch that reaches from epoch 0 into epoch 1, where one
// block announces an endorser block and the next certifies it. Epoch 1 is not
// in the epoch cache until the rollover at the boundary publishes it, so the
// Leios pre-check must not resolve that certificate before the rollover. The
// certificate must still be checked afterwards, against epoch 1, both by the
// pre-check and at apply. The hard-fork case crosses from Conway into
// Dijkstra, where the epoch cache is never forecast across the boundary.
func TestLedgerProcessBlocksDefersLeiosCertificateCheckPastEpochBoundary(
	t *testing.T,
) {
	t.Parallel()

	t.Run("same era", func(t *testing.T) {
		t.Parallel()
		runLeiosCertEpochBoundaryCase(t, false, nil, false)
	})
	t.Run("hard fork", func(t *testing.T) {
		t.Parallel()
		runLeiosCertEpochBoundaryCase(t, true, nil, false)
	})
}

// TestLedgerProcessBlocksRejectsInvalidLeiosCertificatePastEpochBoundary
// shows that deferring the certificate check past the boundary does not
// accept an invalid certificate: the rollover still happens, the certifying
// block is checked against its own epoch, and its rejection stops the batch
// before the block is applied and drops it from the primary chain.
func TestLedgerProcessBlocksRejectsInvalidLeiosCertificatePastEpochBoundary(
	t *testing.T,
) {
	t.Parallel()

	errInvalidCertificate := fmt.Errorf(
		"%w: quorum not met",
		ErrLeiosInvalidCertificate,
	)
	t.Run("same era", func(t *testing.T) {
		t.Parallel()
		runLeiosCertEpochBoundaryCase(t, false, errInvalidCertificate, false)
	})
	t.Run("hard fork", func(t *testing.T) {
		t.Parallel()
		runLeiosCertEpochBoundaryCase(t, true, errInvalidCertificate, false)
	})
}

// TestLedgerProcessBlocksRetriesLocalLeiosCertificateFailurePastEpochBoundary
// shows that a certificate the node could not check, because the validator
// failed locally, leaves the certifying block on the primary chain to be
// retried rather than rewinding past it.
func TestLedgerProcessBlocksRetriesLocalLeiosCertificateFailurePastEpochBoundary(
	t *testing.T,
) {
	t.Parallel()

	errLocal := errors.New("leios vote manager is unavailable")
	t.Run("same era", func(t *testing.T) {
		t.Parallel()
		runLeiosCertEpochBoundaryCase(t, false, errLocal, false)
	})
	t.Run("hard fork", func(t *testing.T) {
		t.Parallel()
		runLeiosCertEpochBoundaryCase(t, true, errLocal, false)
	})
}

func TestLedgerProcessBlocksAppliesCertifiedClosureBeforeEpochSnapshot(t *testing.T) {
	t.Parallel()
	runLeiosCertEpochBoundaryCase(t, false, nil, true)
}

// runLeiosCertEpochBoundaryCase drives the batch through the ledger. A nil
// certificateErr makes the certificate validator accept. One wrapping
// ErrLeiosInvalidCertificate is a verdict, and the certifying block must be
// rejected before it applies; any other is a local failure, and the block must
// stay on the primary chain unapplied.
func runLeiosCertEpochBoundaryCase(
	t *testing.T,
	hardFork bool,
	certificateErr error,
	crossingClosure bool,
) {
	t.Helper()
	const epochLength = 1_000
	ebHash := leiosTestHash(0xE4)
	// In the same-era case the batch opens with the last block of epoch 0.
	// A Conway ledger cannot apply a Dijkstra block, so in the hard-fork case
	// the batch opens at the boundary instead.
	var blocks []gledger.Block
	var prevHash lcommon.Blake2b256
	if !hardFork && !crossingClosure {
		last := leiosBoundaryTestBlock(
			t, 0, epochLength-10, lcommon.Blake2b256{}, false, nil,
		)
		blocks = append(blocks, last)
		prevHash = last.Hash()
	}
	announceSlot := uint64(epochLength + 10)
	if crossingClosure {
		announceSlot = epochLength - 10
	}
	announcer := leiosBoundaryTestBlock(
		t, uint64(len(blocks)), announceSlot, prevHash, false, ebHash,
	)
	certifier := leiosBoundaryTestBlock(
		t, uint64(len(blocks))+1, epochLength+20, announcer.Hash(), true, nil,
	)
	blocks = append(blocks, announcer, certifier)

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for _, blk := range blocks {
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        blk.SlotNumber(),
			Hash:        blk.Hash().Bytes(),
			BlockNumber: blk.BlockNumber(),
			Type:        uint(gledger.BlockTypeDijkstra),
			PrevHash:    blk.PrevHash().Bytes(),
			Cbor:        blk.Cbor(),
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), rawBlocks))

	startEra := eras.DijkstraEraDesc
	params := dijkstraTestProtocolParameters()
	params.MaxBlockBodySize = 2_000_000
	params.MaxBlockHeaderSize = 100_000
	var startPParams lcommon.ProtocolParameters = params
	if hardFork {
		startEra = eras.ConwayEraDesc
		// The Dijkstra hard fork encodes the Conway pparams it converts, so
		// they need their rational fields set.
		conwayParams := epochBoundaryBenchPParams()
		conwayParams.ProtocolVersion.Major = gconway.MaxProtocolVersionConway
		conwayParams.MaxBlockBodySize = params.MaxBlockBodySize
		conwayParams.MaxBlockHeaderSize = params.MaxBlockHeaderSize
		startPParams = conwayParams
	}
	nonce := bytes.Repeat([]byte{0x42}, 32)
	epoch0 := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: epochLength,
		EraId:         startEra.Id,
		Nonce:         nonce,
		EvolvingNonce: nonce,
	}
	require.NoError(t, db.SetEpoch(
		epoch0.StartSlot, epoch0.EpochId,
		nonce, nonce, nil, nil,
		epoch0.EraId, epoch0.SlotLength, epoch0.LengthInSlots,
		nil,
	))

	var (
		validatedMu     sync.Mutex
		validatedEpochs []uint64
	)
	var closureRaw cbor.RawMessage
	var closureTx lcommon.Transaction
	if crossingClosure {
		closureRaw, closureTx = leiosApplyTestProducerTx(t, 0xA1)
	}
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	nodeConfig.ShelleyGenesisHash = strings.Repeat("42", 32)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     nodeConfig,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:          prometheus.NewRegistry(),
		EnableDijkstra:        true,
		ManualBlockProcessing: true,
		EndorserBlockProvider: func(
			hash []byte,
			_ uint64,
		) ([]cbor.RawMessage, bool) {
			if crossingClosure {
				return []cbor.RawMessage{closureRaw}, bytes.Equal(hash, ebHash)
			}
			return []cbor.RawMessage{}, bytes.Equal(hash, ebHash)
		},
		ValidateLeiosCertificate: func(
			epoch uint64,
			_ []byte,
			_ []byte,
			_ []byte,
		) error {
			validatedMu.Lock()
			defer validatedMu.Unlock()
			validatedEpochs = append(validatedEpochs, epoch)
			return certificateErr
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.currentEra = startEra
	ls.currentPParams = startPParams
	ls.currentEpoch = epoch0
	ls.epochCache = []models.Epoch{epoch0}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))
	rewardPParams := lcommon.ProtocolParameters(dijkstraRetentionPParams())
	if hardFork {
		rewardPParams = epochBoundaryBenchPParams()
	}
	seedEmptyRewardBasisForRollover(t, db, epoch0, rewardPParams)
	var closureVisibleAtSnap bool
	if crossingClosure {
		ls.SetEpochBoundarySnapshotStakeHook(func(txn *database.Txn, _ event.EpochTransitionEvent) error {
			tx, err := db.GetTransactionByHash(t.Context(), closureTx.Hash().Bytes(), txn)
			if err != nil {
				return err
			}
			closureVisibleAtSnap = tx != nil
			return nil
		})
	}

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: blocks}
	close(results)
	processErr := ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	)
	if certificateErr != nil &&
		!errors.Is(certificateErr, ErrLeiosInvalidCertificate) {
		require.Equal(
			t,
			certifier.Hash().Bytes(),
			cm.PrimaryChain().Tip().Point.Hash,
			"a block whose certificate was not checked must stay on the "+
				"primary chain",
		)
		require.ErrorIs(t, processErr, certificateErr)
		require.NotErrorIs(t, processErr, errRestartLedgerPipeline)
		require.Less(t, ls.currentTip.Point.Slot, certifier.SlotNumber())
		return
	}
	if certificateErr != nil {
		// The rejected block is dropped from the primary chain and the
		// pipeline restarts, as for any other block that fails validation.
		require.ErrorIs(t, processErr, errRestartLedgerPipeline)
		require.NotEqual(
			t,
			certifier.Hash().Bytes(),
			cm.PrimaryChain().Tip().Point.Hash,
			"the rejected certifying block must leave the primary chain",
		)
		require.Less(
			t,
			ls.currentTip.Point.Slot,
			certifier.SlotNumber(),
			"the rejected certifying block must not be applied",
		)
		validatedMu.Lock()
		defer validatedMu.Unlock()
		require.NotEmpty(t, validatedEpochs)
		for _, epoch := range validatedEpochs {
			require.Equal(
				t,
				uint64(1),
				epoch,
				"the certificate must be checked against its own epoch, "+
					"never a forecast or previous-era one",
			)
		}
		return
	}
	require.NoError(t, processErr)

	require.Equal(t, uint64(1), ls.currentEpoch.EpochId)
	require.Equal(t, eras.DijkstraEraDesc.Id, ls.currentEra.Id)
	require.Equal(t, certifier.SlotNumber(), ls.currentTip.Point.Slot)
	validatedMu.Lock()
	defer validatedMu.Unlock()
	expectedEpochs := []uint64{1, 1}
	if crossingClosure {
		require.True(t, closureVisibleAtSnap, "the certified closure must be applied to the unticked ledger before SNAP")
		fees, err := db.Metadata().SumTransactionFeesInSlotRange(0, epochLength-1, nil)
		require.NoError(t, err)
		require.Equal(t, closureTx.Fee().Uint64(), fees)
		fees, err = db.Metadata().SumTransactionFeesInSlotRange(epochLength, 2*epochLength-1, nil)
		require.NoError(t, err)
		require.Zero(t, fees)
		storedTx, err := db.GetTransactionByHash(t.Context(), closureTx.Hash().Bytes(), nil)
		require.NoError(t, err)
		require.Equal(t, certifier.SlotNumber(), storedTx.Slot, "rollback ownership remains with the certifying ranking block")
		_, _, err = db.TruncateAfterSlot(t.Context(), ocommon.Point{Slot: announcer.SlotNumber(), Hash: announcer.Hash().Bytes()}, 0, nil)
		require.NoError(t, err)
		storedTx, err = db.GetTransactionByHash(t.Context(), closureTx.Hash().Bytes(), nil)
		require.NoError(t, err)
		require.Nil(t, storedTx, "rolling back the certifier removes its pre-tick closure")
		fees, err = db.Metadata().SumTransactionFeesInSlotRange(0, epochLength-1, nil)
		require.NoError(t, err)
		require.Zero(t, fees, "rollback removes the closure's fee context")
		expectedEpochs = []uint64{0, 0, 0, 0}
	}
	require.Equal(
		t,
		expectedEpochs,
		validatedEpochs,
		"the pre-check and the apply must each validate the certificate "+
			"against the epoch of its endorser block",
	)
}

// TestBlocksBeforeEpochEnd pins the prefix to the apply loop's own stop
// condition: the first block at or past the epoch end, and every block while
// the epoch is uninitialized.
func TestBlocksBeforeEpochEnd(t *testing.T) {
	t.Parallel()

	epoch := models.Epoch{StartSlot: 100, SlotLength: 1_000, LengthInSlots: 100}
	var prev lcommon.Blake2b256
	var blocks []gledger.Block
	for idx, slot := range []uint64{150, 199, 200, 210} {
		blk := leiosBoundaryTestBlock(t, uint64(idx), slot, prev, false, nil)
		blocks = append(blocks, blk)
		prev = blk.Hash()
	}

	require.Equal(t, blocks[:2], blocksBeforeEpochEnd(blocks, epoch))
	require.Equal(t, blocks[:2], blocksBeforeEpochEnd(blocks[:2], epoch))
	require.Empty(t, blocksBeforeEpochEnd(blocks[2:], epoch))
	uninitialized := epoch
	uninitialized.SlotLength = 0
	require.Empty(t, blocksBeforeEpochEnd(blocks, uninitialized))
}

func TestLedgerProcessBlocksAppliesRankingTransactionsBeforeNextClosure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		batchSize int
	}{{"separate batches", 1}, {"shared batch", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRankingTransactionsBeforeNextClosure(t, tc.batchSize)
		})
	}
}

func runRankingTransactionsBeforeNextClosure(t *testing.T, batchSize int) {
	t.Helper()
	producerRaw, producer := leiosApplyTestProducerTx(t, 0xB1)
	spenderRaw, spender := leiosApplyTestSpendingTx(t, 0xB2, producer.Hash().Bytes(), 0)
	ebHash := leiosTestHash(0xE5)
	announcer := leiosBoundaryTestBlock(t, 0, 10, lcommon.Blake2b256{}, false, ebHash, producerRaw)
	certifier := leiosBoundaryTestBlock(t, 1, 20, announcer.Hash(), true, nil)
	db := newTestDB(t)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	for _, block := range []gledger.Block{announcer, certifier} {
		require.NoError(t, cm.PrimaryChain().AddBlock(t.Context(), block, nil))
	}
	nonce := bytes.Repeat([]byte{0x42}, 32)
	epoch := models.Epoch{EpochId: 0, StartSlot: 0, SlotLength: 1000, LengthInSlots: 1000, EraId: eras.DijkstraEraDesc.Id, Nonce: nonce, EvolvingNonce: nonce}
	require.NoError(t, db.SetEpoch(0, 0, nonce, nonce, nil, nil, epoch.EraId, epoch.SlotLength, epoch.LengthInSlots, nil))
	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().NetworkId = "Testnet"
	cfg.ShelleyGenesisHash = strings.Repeat("42", 32)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database: db, ChainManager: cm, CardanoNodeConfig: cfg,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), PromRegistry: prometheus.NewRegistry(),
		EnableDijkstra: true, ManualBlockProcessing: true,
		EndorserBlockProvider: func(hash []byte, _ uint64) ([]cbor.RawMessage, bool) {
			return []cbor.RawMessage{spenderRaw}, bytes.Equal(hash, ebHash)
		},
		ValidateLeiosCertificate: func(uint64, []byte, []byte, []byte) error { return nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = dijkstraTestProtocolParameters()
	ls.currentEpoch = epoch
	ls.epochCache = []models.Epoch{epoch}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))
	blocks := []gledger.Block{announcer, certifier}
	batches := make(chan []gledger.Block, len(blocks))
	for i := 0; i < len(blocks); i += batchSize {
		batches <- blocks[i : i+batchSize]
	}
	close(batches)
	require.NoError(t, ls.ProcessTrustedBlockBatches(t.Context(), batches))
	stored, err := db.GetTransactionByHash(t.Context(), spender.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Len(t, stored.Inputs, 1, "the closure must consume the earlier ranking-block output even in a shared batch")
	require.Equal(t, certifier.SlotNumber(), stored.Inputs[0].DeletedSlot)
	require.Equal(t, spender.Hash().Bytes(), []byte(stored.Inputs[0].SpentAtTxId))
}

// A certifying ranking block whose parent announced no endorser block has
// nothing to certify, so the reference Forker.applyBlock rejects it as invalid
// (LeiosCertificateWithoutAnnouncement) rather than waiting for a closure that
// cannot exist. The pipeline must drop it from the primary chain and restart,
// not retry it as an unavailable closure until the stuck-pipeline halt.
func TestLedgerProcessBlocksRejectsCertificateWithoutParentAnnouncement(
	t *testing.T,
) {
	t.Parallel()
	const epochLength = 1_000
	parent := leiosBoundaryTestBlock(
		t, 0, 10, lcommon.Blake2b256{}, false, nil,
	)
	certifier := leiosBoundaryTestBlock(
		t, 1, 20, parent.Hash(), true, nil,
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	rawBlocks := make([]chain.RawBlock, 0, 2)
	for _, blk := range []*gdijkstra.DijkstraBlock{parent, certifier} {
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        blk.SlotNumber(),
			Hash:        blk.Hash().Bytes(),
			BlockNumber: blk.BlockNumber(),
			Type:        uint(gledger.BlockTypeDijkstra),
			PrevHash:    blk.PrevHash().Bytes(),
			Cbor:        blk.Cbor(),
		})
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), rawBlocks),
	)

	nonce := bytes.Repeat([]byte{0x42}, 32)
	epoch0 := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: epochLength,
		EraId:         eras.DijkstraEraDesc.Id,
		Nonce:         nonce,
		EvolvingNonce: nonce,
	}
	require.NoError(t, db.SetEpoch(
		epoch0.StartSlot, epoch0.EpochId,
		nonce, nonce, nil, nil,
		epoch0.EraId, epoch0.SlotLength, epoch0.LengthInSlots,
		nil,
	))
	params := dijkstraTestProtocolParameters()
	params.MaxBlockBodySize = 2_000_000
	params.MaxBlockHeaderSize = 100_000
	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	nodeConfig.ShelleyGenesisHash = strings.Repeat("42", 32)
	certChecks := 0
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     nodeConfig,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:          prometheus.NewRegistry(),
		EnableDijkstra:        true,
		ManualBlockProcessing: true,
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		ValidateLeiosCertificate: func(uint64, []byte, []byte, []byte) error {
			certChecks++
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = params
	ls.currentEpoch = epoch0
	ls.epochCache = []models.Epoch{epoch0}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))

	results := make(chan readChainResult, 2)
	results <- readChainResult{blocks: []gledger.Block{parent}}
	results <- readChainResult{blocks: []gledger.Block{certifier}}
	close(results)
	processErr := ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	)

	require.ErrorIs(t, processErr, errRestartLedgerPipeline)
	require.NotErrorIs(t, processErr, errCertifiedEndorserBlockUnavailable)
	require.Zero(t, certChecks, "there is no announcement to verify against")
	require.Equal(t, parent.SlotNumber(), ls.currentTip.Point.Slot)
	require.Equal(
		t,
		parent.Hash().Bytes(),
		cm.PrimaryChain().Tip().Point.Hash,
		"the certifying block must be dropped from the primary chain",
	)
}
