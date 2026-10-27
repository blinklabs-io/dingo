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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boundaryFixture is the last Byron block and the first Shelley block of a
// network's fork epoch, taken from the chain itself over NtN blockfetch.
type boundaryFixture struct {
	name             string
	byronFile        string
	byronType        uint
	byronSlot        uint64
	shelleyFile      string
	shelleyType      uint
	shelleySlot      uint64
	maxBlockBodySize uint
	maxHeaderSize    uint
}

// The two shipped networks that have a Byron prefix. Both fixtures were
// fetched with blockfetch over the range spanning the boundary, which delivers
// Byron epoch boundary blocks as well -- verified against mainnet's Byron
// epoch 0/1 boundary at slot 21600, where the range does contain an EBB
// (block number 21586, shared with its parent, followed by the regular block
// 21587 at the same slot). Neither fork boundary below contains one.
var boundaryFixtures = []boundaryFixture{
	{
		name:        "preprod",
		byronFile:   "preprod-byron-last-84242.cbor",
		byronType:   1,
		byronSlot:   84_242,
		shelleyFile: "preprod-shelley-first-86400.cbor",
		shelleyType: 2,
		shelleySlot: 86_400,
		// preprod Shelley genesis protocolParams.
		maxBlockBodySize: 65_536,
		maxHeaderSize:    1_100,
	},
	{
		name:        "mainnet",
		byronFile:   "mainnet-byron-last-4492799.cbor",
		byronType:   1,
		byronSlot:   4_492_799,
		shelleyFile: "mainnet-shelley-first-4492800.cbor",
		shelleyType: 2,
		shelleySlot: 4_492_800,
		// mainnet Shelley genesis protocolParams.
		maxBlockBodySize: 65_536,
		maxHeaderSize:    1_100,
	},
}

func loadBoundaryBlock(
	t *testing.T,
	file string,
	blockType uint,
) gledger.Block {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", file))
	require.NoError(t, err)
	block, err := gledger.NewBlockFromCbor(blockType, raw)
	require.NoError(t, err)
	return block
}

// TestByronShelleyBoundaryHasNoEpochBoundaryBlock pins the chain shape the
// era-transition path depends on: the first block of the fork epoch is a
// Shelley block, not a Byron EBB.
//
// ledgerProcessBlocksFromSource ends a batch at the first block whose slot
// reaches the epoch end and takes nextEpochEraId from that block's era. A
// Byron EBB carries the Byron era and its parent's block number, so an EBB
// leading the fork epoch would defer the era transition by one block and leave
// the first Shelley block to be validated under a Byron era with nil protocol
// parameters. Both shipped networks with a Byron prefix rule that out: the
// Shelley block links directly to the last Byron block.
func TestByronShelleyBoundaryHasNoEpochBoundaryBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			last := loadBoundaryBlock(t, tc.byronFile, tc.byronType)
			first := loadBoundaryBlock(t, tc.shelleyFile, tc.shelleyType)

			require.Equal(t, tc.byronSlot, last.SlotNumber())
			require.Equal(t, tc.shelleySlot, first.SlotNumber())
			assert.EqualValues(t, byron.EraIdByron, last.Era().Id)
			assert.EqualValues(t, shelley.EraIdShelley, first.Era().Id)

			// Neither block is an EBB, and no EBB can sit between them: the
			// Shelley block names the Byron block as its parent and takes the
			// next block number. An EBB would share 'last' block number and
			// break that link.
			_, lastIsEbb := last.(*byron.ByronEpochBoundaryBlock)
			_, firstIsEbb := first.(*byron.ByronEpochBoundaryBlock)
			assert.False(t, lastIsEbb)
			assert.False(t, firstIsEbb)
			assert.Equal(
				t,
				last.Hash().String(),
				first.PrevHash().String(),
				"first Shelley block must link directly to the last Byron block",
			)
			assert.Equal(t, last.BlockNumber()+1, first.BlockNumber())
		})
	}
}

// TestByronShelleyBoundaryEnvelopeRequiresProtocolParameters pins that the
// first Shelley block is validated with protocol parameters, not exempted from
// validation for lacking them.
//
// validateInboundBlockEnvelope runs before validateBlockHeaderProtocolVersion
// in ledgerProcessBlock, and it reaches validateBlockSizes for any non-Byron
// block. So nil parameters at this point are a rejection, not a bypass: there
// is no ordering in which a "Byron era, nil pparams" allowance for the first
// Shelley block can take effect, and adding one to the envelope check would
// drop maxBlockHeaderSize and maxBlockBodySize for a real Shelley block.
//
// The era transition supplies those parameters. It runs at the epoch break
// ahead of this block precisely because the block above is Shelley, which
// TestByronShelleyBoundaryHasNoEpochBoundaryBlock pins.
func TestByronShelleyBoundaryEnvelopeRequiresProtocolParameters(t *testing.T) {
	t.Parallel()

	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			last := loadBoundaryBlock(t, tc.byronFile, tc.byronType)
			first := loadBoundaryBlock(t, tc.shelleyFile, tc.shelleyType)
			parent := envelopeParentFromBlock(last)

			byronConfig := newByronEnvelopeNodeConfig(
				t,
				len(last.Cbor()),
				len(last.Header().Cbor()),
			)
			// The Byron parent itself needs no protocol parameters: its
			// limits come from Byron genesis instead.
			require.NoError(
				t,
				validateInboundBlockEnvelope(
					last,
					nil,
					byronConfig,
					envelopeParent{
						origin: true,
					},
				),
			)

			err := validateInboundBlockEnvelope(first, nil, nil, parent)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"block size validation unsupported for protocol parameters",
			)

			pp := &shelley.ShelleyProtocolParameters{
				MaxBlockBodySize:   tc.maxBlockBodySize,
				MaxBlockHeaderSize: tc.maxHeaderSize,
			}
			assert.NoError(
				t,
				validateInboundBlockEnvelope(first, pp, nil, parent),
			)

			// The block's declared body size is what the size check measures,
			// so a limit one byte below it must reject. This keeps the
			// positive case above from passing on an unenforced limit.
			tooSmall := &shelley.ShelleyProtocolParameters{
				//nolint:gosec // fixture body size is well under uint range
				MaxBlockBodySize:   uint(first.BlockBodySize()) - 1,
				MaxBlockHeaderSize: tc.maxHeaderSize,
			}
			assert.ErrorContains(
				t,
				validateInboundBlockEnvelope(first, tooSmall, nil, parent),
				"exceeds maxBlockBodySize",
			)
		})
	}
}

// TestByronBlockHeaderProtocolVersionSkippedWithoutPParams pins that a
// validated Byron block does not require protocol parameters.
//
// Byron headers carry no ProtVer field, so HeaderProtocolMajor reports no
// version for them and ValidateHeaderProtocolVersion already skips them. The
// LedgerState wrapper must reach that skip rather than failing earlier on
// GetProtocolVersion(nil): with ValidateHistorical enabled on a network that
// has a Byron prefix, every block of the prefix is validated while
// currentPParams is nil, so demanding parameters here rejects the entire
// Byron era.
//
// The Shelley half of the test keeps the wrapper fail-closed for headers that
// do carry a version, which is the case validateInboundBlockEnvelope also
// rejects.
func TestByronBlockHeaderProtocolVersionSkippedWithoutPParams(t *testing.T) {
	t.Parallel()

	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			ls := newLedgerStateForNetwork(t, "Testnet", 42)

			byronHeader := loadBoundaryBlock(
				t, tc.byronFile, tc.byronType,
			).Header()
			_, hasVersion := HeaderProtocolMajor(byronHeader)
			require.False(
				t,
				hasVersion,
				"a Byron header carries no protocol major version",
			)
			assert.NoError(
				t,
				ls.validateBlockHeaderProtocolVersion(byronHeader, nil),
			)

			shelleyHeader := loadBoundaryBlock(
				t, tc.shelleyFile, tc.shelleyType,
			).Header()
			_, hasVersion = HeaderProtocolMajor(shelleyHeader)
			require.True(
				t,
				hasVersion,
				"a Shelley header carries a protocol major version",
			)
			assert.ErrorContains(
				t,
				ls.validateBlockHeaderProtocolVersion(shelleyHeader, nil),
				"protocol parameters are nil",
			)
		})
	}
}

func newByronShelleyBoundaryLedger(
	t *testing.T,
) (*LedgerState, gledger.Block, gledger.Block) {
	t.Helper()
	ls, lastByron, firstShelley := newUnseededByronShelleyBoundaryLedger(t)
	seedBoundaryFixtureLiveUtxo(t, ls)
	return ls, lastByron, firstShelley
}

// newUnseededByronShelleyBoundaryLedger is newByronShelleyBoundaryLedger
// without the live UTxO, for tests that control the translated balance.
func newUnseededByronShelleyBoundaryLedger(
	t *testing.T,
) (*LedgerState, gledger.Block, gledger.Block) {
	t.Helper()

	const byronGenesisJSON = `{
		"protocolConsts": {"k": 2160, "protocolMagic": 764824073},
		"blockVersionData": {"slotDuration": "20000"}
	}`
	const shelleyGenesisJSON = `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 2160,
		"epochLength": 432000,
		"slotLength": 1,
		"networkId": "Mainnet",
		"networkMagic": 764824073,
		"maxLovelaceSupply": 45000000000000000,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("42", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg,
		strings.NewReader(byronGenesisJSON),
	))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))

	return newUnseededByronShelleyBoundaryLedgerFrom(
		t,
		cfg,
		loadBoundaryBlock(t, "mainnet-byron-last-4492799.cbor", 1),
		loadBoundaryBlock(t, "mainnet-shelley-first-4492800.cbor", 2),
		207,
		4_471_200,
		21_600,
	)
}

// newByronShelleyBoundaryLedgerFrom builds a ledger whose tip is lastByron, in
// the Byron epoch byronEpoch, with firstShelley stored after it on the primary
// chain and not yet applied.
func newByronShelleyBoundaryLedgerFrom(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
	lastByron, firstShelley gledger.Block,
	byronEpoch, byronEpochStart uint64,
	byronEpochLength uint,
) (*LedgerState, gledger.Block, gledger.Block) {
	t.Helper()
	ls, lastByron, firstShelley := newUnseededByronShelleyBoundaryLedgerFrom(
		t, cfg, lastByron, firstShelley,
		byronEpoch, byronEpochStart, byronEpochLength,
	)
	seedBoundaryFixtureLiveUtxo(t, ls)
	return ls, lastByron, firstShelley
}

// seedBoundaryFixtureLiveUtxo writes the one live UTxO a boundary fixture
// carries. A ledger at the end of Byron holds the translated genesis funds,
// and the boundary rejects an empty live UTxO set as an unpopulated table.
func seedBoundaryFixtureLiveUtxo(t *testing.T, ls *LedgerState) {
	t.Helper()
	txn := ls.db.Transaction(context.Background(), true)
	require.NoError(t, ls.db.CreateUtxo(
		context.Background(), txn, &models.Utxo{
			TxId:      bytes.Repeat([]byte{0x03}, 32),
			AddedSlot: 0,
			Amount:    types.Uint64(1_000_000_000),
		},
	))
	require.NoError(t, txn.Commit())
}

// newUnseededByronShelleyBoundaryLedgerFrom is newByronShelleyBoundaryLedgerFrom
// without the live UTxO, for tests that control the translated balance.
func newUnseededByronShelleyBoundaryLedgerFrom(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
	lastByron, firstShelley gledger.Block,
	byronEpoch, byronEpochStart uint64,
	byronEpochLength uint,
) (*LedgerState, gledger.Block, gledger.Block) {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{
		securityParam: 2,
	}))
	rawBlocks := []chain.RawBlock{{
		Slot:        lastByron.SlotNumber(),
		Hash:        lastByron.Hash().Bytes(),
		BlockNumber: lastByron.BlockNumber(),
		Type:        1,
		Cbor:        lastByron.Cbor(),
	}}
	rawBlocks = append(rawBlocks, chain.RawBlock{
		Slot:        firstShelley.SlotNumber(),
		Hash:        firstShelley.Hash().Bytes(),
		BlockNumber: firstShelley.BlockNumber(),
		Type:        2,
		PrevHash:    firstShelley.PrevHash().Bytes(),
		Cbor:        firstShelley.Cbor(),
	})
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), rawBlocks),
	)

	require.NoError(t, db.SetEpoch(
		byronEpochStart,
		byronEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ByronEraDesc.Id,
		20_000,
		byronEpochLength,
		nil,
	))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: cfg,
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		ValidateHistorical: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })

	byronTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			lastByron.SlotNumber(),
			lastByron.Hash().Bytes(),
		),
		BlockNumber: lastByron.BlockNumber(),
	}
	require.NoError(t, db.SetTip(byronTip, nil))
	byronNonce := bytes.Repeat([]byte{0x42}, 32)
	require.NoError(t, db.SetBlockNonce(
		byronTip.Point.Hash,
		byronTip.Point.Slot,
		byronNonce,
		true,
		nil,
	))
	ls.currentTip = byronTip
	ls.currentTipBlockNonce = byronNonce
	ls.currentEpoch = models.Epoch{
		EpochId:       byronEpoch,
		StartSlot:     byronEpochStart,
		SlotLength:    20_000,
		LengthInSlots: byronEpochLength,
		EraId:         eras.ByronEraDesc.Id,
	}
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentEra = eras.ByronEraDesc
	ls.currentPParams = nil
	ls.transitionInfo = hardfork.NewTransitionUnknown()
	ls.publishSnapshotsLocked()

	return ls, lastByron, firstShelley
}

func TestByronShelleyBoundaryProcessesFirstShelleyBlockWithPParams(
	t *testing.T,
) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
	require.True(t, ls.validationEnabled)

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)

	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	// Historical validation is enabled above, so the normal processing path
	// calls validateInboundBlockEnvelope before applying this block. That call
	// rejects a Shelley block with nil pparams; reaching the new tip therefore
	// proves the boundary rollover installed them before validation.
	require.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEra.Id)
	pparams, ok := ls.currentPParams.(*shelley.ShelleyProtocolParameters)
	require.True(t, ok, "the first Shelley block must install Shelley pparams")
	assert.Equal(t, uint(2), pparams.ProtocolMajor)
	assert.Equal(t, firstShelley.SlotNumber(), ls.currentTip.Point.Slot)
}

// TestByronShelleyBoundaryDefersReadResultDoneUntilCachedBatchApplied is a
// regression test: ledgerProcessBlocksFromSource must not signal a
// readChainResult's done channel until the whole result has been applied,
// including any post-boundary remainder deferred through cachedNextBatch.
//
// firstShelley alone crosses the Byron/Shelley epoch boundary (see
// newByronShelleyBoundaryLedger), so processing this one-block batch takes
// two outer-loop passes: the first discovers the boundary and defers the
// entire block to cachedNextBatch without applying it (blocksProcessed stays
// 0); the second runs the epoch/era rollover and then actually applies
// firstShelley. Signalling done at the end of the first pass -- before
// firstShelley is ever applied -- previously told the reader goroutine this
// result was fully consumed while the tip was still at the pre-boundary
// block, letting it start gathering and decoding the next raw batch early.
//
// firstShelley carries no transactions, so the existing
// beforeTransactionApplyPublish hook (gated on having transaction events to
// publish) never fires for it and can't be used to pin this timing. Instead
// this test uses the dedicated beforeReadResultDoneSignal hook, which fires
// unconditionally once per outer-loop pass, to deterministically pause the
// pipeline goroutine at each pass boundary and assert on done's state there
// -- rather than racing a separate observer goroutine against the
// pipeline's own progress after done closes.
func TestByronShelleyBoundaryDefersReadResultDoneUntilCachedBatchApplied(
	t *testing.T,
) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
	require.True(t, ls.validationEnabled)

	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	results <- readChainResult{
		blocks: []gledger.Block{firstShelley},
		done:   done,
	}
	close(results)

	requireDoneNotYetClosed := func(msg string) {
		t.Helper()
		select {
		case <-done:
			t.Fatal(msg)
		default:
		}
	}

	// passReached/releasePass rendezvous with the pipeline goroutine inside
	// beforeReadResultDoneSignal: once a receive on passReached completes,
	// the pipeline goroutine's only next step is to block on releasePass
	// (see beforeReadResultDoneSignal's body below), so it is guaranteed to
	// not yet have run completeReadResult() for this pass.
	passReached := make(chan struct{})
	releasePass := make(chan struct{})
	ls.beforeReadResultDoneSignal = func() {
		passReached <- struct{}{}
		<-releasePass
	}

	processDone := make(chan error, 1)
	go func() {
		processDone <- ls.ledgerProcessBlocksFromSource(
			context.Background(),
			results,
		)
	}()

	// Pass 1: discovers the epoch boundary and defers firstShelley into
	// cachedNextBatch without applying it.
	testutil.RequireReceive(
		t, passReached, testutil.AsyncWait,
		"pass 1 (boundary discovery) never reached the done-signal hook",
	)
	requireDoneNotYetClosed(
		"done must not fire after only the boundary-discovery pass",
	)
	releasePass <- struct{}{}

	// Pass 2: runs the epoch/era rollover and then actually applies
	// firstShelley.
	testutil.RequireReceive(
		t, passReached, testutil.AsyncWait,
		"pass 2 (cached-batch apply) never reached the done-signal hook",
	)
	ls.RLock()
	appliedSlot := ls.currentTip.Point.Slot
	ls.RUnlock()
	require.Equal(
		t,
		firstShelley.SlotNumber(),
		appliedSlot,
		"firstShelley must already be applied by the time the second "+
			"pass reaches the done-signal hook",
	)
	requireDoneNotYetClosed(
		"done must not fire until the pass that actually applied the " +
			"deferred block finishes",
	)
	releasePass <- struct{}{}

	require.NoError(t, <-processDone)

	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("readChainResult.done was never closed")
	}
}

// TestByronShelleyBoundaryClosesReadResultDoneOnEpochRolloverFailure is a
// regression test for a deadlock the cachedNextBatch fix above could
// otherwise introduce: once a boundary-crossing batch defers its remainder
// to cachedNextBatch, currentReadResultDone is deliberately left open (not
// closed) across the pass boundary -- see the "cachedNextBatch != nil"
// branch and the completeReadResult() guard in
// ledgerProcessBlocksFromSource. If the epoch/era rollover that runs at the
// top of the next pass then fails, the function returns before that guard
// ever runs, and would leave the read-chain reader goroutine blocked on
// <-result.done forever.
//
// This forces that exact rollover failure (by clearing CardanoNodeConfig,
// which processEpochRollover checks first) after firstShelley's boundary
// crossing has already deferred it to cachedNextBatch, and asserts that
// ledgerProcessBlocksFromSource still signals done before returning its
// error.
func TestByronShelleyBoundaryClosesReadResultDoneOnEpochRolloverFailure(
	t *testing.T,
) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
	require.True(t, ls.validationEnabled)

	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	results <- readChainResult{
		blocks: []gledger.Block{firstShelley},
		done:   done,
	}
	close(results)

	passReached := make(chan struct{})
	releasePass := make(chan struct{})
	ls.beforeReadResultDoneSignal = func() {
		passReached <- struct{}{}
		<-releasePass
	}

	processDone := make(chan error, 1)
	go func() {
		processDone <- ls.ledgerProcessBlocksFromSource(
			context.Background(),
			results,
		)
	}()

	// Pass 1: discovers the epoch boundary and defers firstShelley into
	// cachedNextBatch. Break processEpochRollover for the pass that
	// follows, right before releasing it -- ensureReferencedEndorserBlocks
	// and the rest of pass 1 don't touch CardanoNodeConfig for this
	// single-block batch, so clearing it here only affects pass 2.
	testutil.RequireReceive(
		t, passReached, testutil.AsyncWait,
		"pass 1 (boundary discovery) never reached the done-signal hook",
	)
	ls.config.CardanoNodeConfig = nil
	releasePass <- struct{}{}

	err := testutil.RequireReceive(
		t, processDone, testutil.AsyncWait,
		"ledgerProcessBlocksFromSource never returned after the forced "+
			"epoch-rollover failure",
	)
	require.ErrorContains(t, err, "process epoch rollover")

	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal(
			"readChainResult.done was left open after an epoch-rollover " +
				"failure, which would block the read-chain reader goroutine " +
				"forever",
		)
	}
}

// TestByronShelleyBoundarySeedsEpochNonceOnProductionPath pins the multi-era
// epoch-nonce seeding fix through the same production path as
// TestByronShelleyBoundaryProcessesFirstShelleyBlockWithPParams: without the
// pre-rollover Byron-to-Shelley translation, calculateEpochNonce sees Byron
// and returns a nil nonce instead of seeding the Shelley genesis hash, and the
// transitioned epoch is persisted with no nonce at all. That existing test
// only asserts on era, pparams, and tip, so it still passes with the
// nonce-seeding block deleted; this test asserts on the nonce itself, in all
// three places a caller can observe it — the in-memory current epoch, the epoch
// cache, and the persisted database row — and fails without the fix.
func TestByronShelleyBoundarySeedsEpochNonceOnProductionPath(t *testing.T) {
	t.Parallel()

	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
	require.True(t, ls.validationEnabled)

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)

	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEra.Id)

	// The fix seeds the nonce/evolving/candidate nonce from the Shelley
	// genesis hash, which newByronShelleyBoundaryLedger sets to 32 bytes of
	// 0x42 (ShelleyGenesisHash: strings.Repeat("42", 32)).
	expectedNonce := bytes.Repeat([]byte{0x42}, 32)
	transitionedEpoch := ls.currentEpoch.EpochId

	// In-memory current epoch.
	assert.Equal(
		t,
		expectedNonce,
		[]byte(ls.currentEpoch.Nonce),
		"in-memory epoch must carry the seeded nonce",
	)
	assert.Equal(
		t,
		expectedNonce,
		[]byte(ls.currentEpoch.EvolvingNonce),
	)
	assert.Equal(
		t,
		expectedNonce,
		[]byte(ls.currentEpoch.CandidateNonce),
	)

	// Epoch cache.
	var cached *models.Epoch
	for i := range ls.epochCache {
		if ls.epochCache[i].EpochId == transitionedEpoch {
			cached = &ls.epochCache[i]
			break
		}
	}
	require.NotNil(
		t,
		cached,
		"epoch cache must contain the transitioned epoch",
	)
	assert.Equal(
		t,
		expectedNonce,
		[]byte(cached.Nonce),
		"epoch cache entry must carry the seeded nonce",
	)

	// Persisted epoch row.
	epochs, err := ls.db.GetEpochs(nil)
	require.NoError(t, err)
	var persisted *models.Epoch
	for i := range epochs {
		if epochs[i].EpochId == transitionedEpoch {
			persisted = &epochs[i]
			break
		}
	}
	require.NotNil(
		t,
		persisted,
		"the transitioned epoch must be persisted",
	)
	assert.Equal(
		t,
		expectedNonce,
		[]byte(persisted.Nonce),
		"persisted epoch row must carry the seeded nonce",
	)
}

func TestRollbackChainAndStateClearsShelleyPParamsInsideByronPrefix(
	t *testing.T,
) {
	t.Parallel()

	ls, lastByron, firstShelley := newByronShelleyBoundaryLedger(t)

	const shelleyEpoch = uint64(208)
	shelleyPParams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor:      2,
		ProtocolMinor:      0,
		MaxBlockBodySize:   65_536,
		MaxBlockHeaderSize: 1_100,
	}
	pparamsCbor, err := cbor.Encode(shelleyPParams)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetEpoch(
		firstShelley.SlotNumber(),
		shelleyEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1_000,
		432_000,
		nil,
	))
	require.NoError(t, ls.db.SetPParams(
		pparamsCbor,
		firstShelley.SlotNumber(),
		shelleyEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	shelleyTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			firstShelley.SlotNumber(),
			firstShelley.Hash().Bytes(),
		),
		BlockNumber: firstShelley.BlockNumber(),
	}
	require.NoError(t, ls.db.SetTip(shelleyTip, nil))
	ls.currentTip = shelleyTip
	ls.currentEpoch = models.Epoch{
		EpochId:       shelleyEpoch,
		StartSlot:     firstShelley.SlotNumber(),
		SlotLength:    1_000,
		LengthInSlots: 432_000,
		EraId:         eras.ShelleyEraDesc.Id,
	}
	ls.epochCache = append(ls.epochCache, ls.currentEpoch)
	ls.currentEra = eras.ShelleyEraDesc
	ls.currentPParams = shelleyPParams
	ls.transitionInfo = hardfork.NewTransitionKnown(shelleyEpoch)
	ls.publishSnapshotsLocked()

	byronPoint := ocommon.NewPoint(
		lastByron.SlotNumber(),
		lastByron.Hash().Bytes(),
	)
	require.NoError(t, ls.rollbackChainAndStateDeferred(context.Background(), byronPoint, nil))

	assert.Equal(t, byronPoint, ls.currentTip.Point)
	assert.Equal(t, eras.ByronEraDesc.Id, ls.currentEra.Id)
	assert.Nil(t, ls.currentPParams)
	assert.Nil(t, ls.prevEraPParams)
	assert.Equal(t, hardfork.NewTransitionUnknown(), ls.transitionInfo)
}

func loadPreprodNodeConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"preprod/config.json",
	)
	require.NoError(t, err)
	return cfg
}

// TestByronShelleyBoundaryPreprodHeaderMajorDoesNotSkipShelley pins the era
// preprod's fork boundary opens. The first Shelley block there advertises
// header major 3, Allegra's, but Shelley ends by a version trigger that only
// Shelley's own enacted parameters can meet: preprod runs epoch 4 at major 2
// and enacts major 3 at epoch 5. Reading the header as the era moved epoch 4
// into Allegra.
func TestByronShelleyBoundaryPreprodHeaderMajorDoesNotSkipShelley(
	t *testing.T,
) {
	t.Parallel()

	firstShelley := loadBoundaryBlock(t, "preprod-shelley-first-86400.cbor", 2)
	headerMajor, known := HeaderProtocolMajor(firstShelley.Header())
	require.True(t, known)
	require.Equal(t, eras.AllegraEraDesc.MinMajorVersion, headerMajor,
		"the premise: preprod's first Shelley header names Allegra's major")

	t.Run("version trigger", func(t *testing.T) {
		t.Parallel()
		ls := &LedgerState{
			config: LedgerStateConfig{
				CardanoNodeConfig: loadPreprodNodeConfig(t),
			},
		}
		target, allowTwo := ls.boundaryEraForBlock(
			eras.ByronEraDesc.Id,
			eras.ShelleyEraDesc.Id,
			headerMajor,
			known,
			4,
		)
		assert.Equal(t, eras.ShelleyEraDesc.Id, target)
		assert.False(t, allowTwo)
	})

	t.Run("scheduled epoch", func(t *testing.T) {
		t.Parallel()
		cfg := loadPreprodNodeConfig(t)
		shelleyEpoch, allegraEpoch := uint64(4), uint64(4)
		cfg.TestShelleyHardForkAtEpoch = &shelleyEpoch
		cfg.TestAllegraHardForkAtEpoch = &allegraEpoch
		ls := &LedgerState{config: LedgerStateConfig{CardanoNodeConfig: cfg}}
		target, allowTwo := ls.boundaryEraForBlock(
			eras.ByronEraDesc.Id,
			eras.ShelleyEraDesc.Id,
			headerMajor,
			known,
			4,
		)
		assert.Equal(t, eras.AllegraEraDesc.Id, target,
			"an Allegra scheduled for the fork epoch still opens it")
		assert.True(t, allowTwo)
	})

	t.Run("rollover", func(t *testing.T) {
		t.Parallel()
		ls, _, block := newByronShelleyBoundaryLedgerFrom(
			t,
			loadPreprodNodeConfig(t),
			loadBoundaryBlock(t, "preprod-byron-last-84242.cbor", 1),
			firstShelley,
			3,
			64_800,
			21_600,
		)
		results := make(chan readChainResult, 1)
		results <- readChainResult{blocks: []gledger.Block{block}}
		close(results)
		require.NoError(t, ls.ledgerProcessBlocksFromSource(
			context.Background(),
			results,
		))
		require.Equal(t, block.SlotNumber(), ls.currentTip.Point.Slot)
		assert.Equal(t, uint64(4), ls.currentEpoch.EpochId)
		assert.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEpoch.EraId)
		assert.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEra.Id)
		pparams, ok := ls.currentPParams.(*shelley.ShelleyProtocolParameters)
		require.True(t, ok, "epoch 4 runs on Shelley pparams, got %T",
			ls.currentPParams)
		assert.Equal(t, uint(2), pparams.ProtocolMajor)
	})
}

// Value that leaves circulation during Byron (fees) is gone from the UTxO set
// the Shelley ledger starts from, and cardano-ledger sets the initial reserves
// to the maximum supply minus that set (Shelley/API/ByronTranslation.hs).
// The genesis reserves written at slot 0 therefore overstate nothing and
// understate the reserves by exactly the Byron-era loss.
//
// Mainnet and preprod both open Shelley this way: preprod's prefix is four
// Byron epochs and its first Shelley header names Allegra's major, so the
// fixture also pins that neither shape bypasses the translation.
func TestByronShelleyBoundaryReservesExcludeLiveUtxoBalance(t *testing.T) {
	t.Parallel()

	const (
		genesisUtxo = uint64(1_000_000_000)
		liveUtxo    = uint64(900_000_000)
		spentUtxo   = uint64(50_000_000)
	)

	for _, tc := range []struct {
		name   string
		ledger func(t *testing.T) (*LedgerState, gledger.Block, gledger.Block)
	}{
		{name: "mainnet", ledger: newUnseededByronShelleyBoundaryLedger},
		{name: "preprod", ledger: newUnseededPreprodByronShelleyBoundaryLedger},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, _, firstShelley := tc.ledger(t)
			maxSupply := ls.config.CardanoNodeConfig.ShelleyGenesis().
				MaxLovelaceSupply
			require.NotZero(t, maxSupply)
			expectedReserve := maxSupply - liveUtxo
			seedByronBoundaryUtxos(
				t, ls, maxSupply-genesisUtxo, liveUtxo, spentUtxo,
			)

			crossByronShelleyBoundary(t, ls, firstShelley)

			requireBoundaryReserves(t, ls, expectedReserve)
		})
	}
}

// An empty live UTxO set is not a translated Byron ledger: genesis funds can
// move but not vanish, so zero means the UTxO table was never populated. The
// boundary must fail rather than commit the first Shelley epoch, and the ADA
// pots its first reward round reads, from reserves the translation never set.
func TestByronShelleyBoundaryRejectsEmptyLiveUtxo(t *testing.T) {
	t.Parallel()

	const (
		maxSupply   = uint64(45_000_000_000_000_000)
		genesisUtxo = uint64(1_000_000_000)
	)

	ls, _, firstShelley := newUnseededByronShelleyBoundaryLedger(t)
	seedByronBoundaryUtxos(t, ls, maxSupply-genesisUtxo, 0, 0)

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)
	err := ls.ledgerProcessBlocksFromSource(context.Background(), results)
	require.ErrorContains(t, err, "no live UTxO at the Byron-to-Shelley boundary")
	require.Equal(t, eras.ByronEraDesc.Id, ls.currentEra.Id)

	pots, err := ls.db.Metadata().GetRewardAdaPots(208, nil)
	require.NoError(t, err)
	require.Nil(t, pots, "the failed boundary must not commit Shelley pots")
}

// seedByronBoundaryUtxos writes the slot-0 network state and, for each
// nonzero amount, one live and one spent UTxO.
func seedByronBoundaryUtxos(
	t *testing.T,
	ls *LedgerState,
	genesisReserves, liveUtxo, spentUtxo uint64,
) {
	t.Helper()
	require.NoError(t, ls.db.Metadata().SetNetworkState(
		0, genesisReserves, 0, nil,
	))
	txn := ls.db.Transaction(context.Background(), true)
	if liveUtxo > 0 {
		require.NoError(t, ls.db.CreateUtxo(
			context.Background(), txn, &models.Utxo{
				TxId:      bytes.Repeat([]byte{0x01}, 32),
				AddedSlot: 0,
				Amount:    types.Uint64(liveUtxo),
			},
		))
	}
	if spentUtxo > 0 {
		require.NoError(t, ls.db.CreateUtxo(
			context.Background(), txn, &models.Utxo{
				TxId:        bytes.Repeat([]byte{0x02}, 32),
				AddedSlot:   0,
				DeletedSlot: 100,
				Amount:      types.Uint64(spentUtxo),
			},
		))
	}
	require.NoError(t, txn.Commit())
}

func crossByronShelleyBoundary(
	t *testing.T,
	ls *LedgerState,
	firstShelley gledger.Block,
) {
	t.Helper()
	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)
	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(t, eras.ShelleyEraDesc.Id, ls.currentEra.Id)
}

// requireBoundaryReserves checks the reserves the boundary left in the
// network state and in the first Shelley epoch's ADA pots, which the first
// Shelley reward round reads.
func requireBoundaryReserves(
	t *testing.T,
	ls *LedgerState,
	expectedReserve uint64,
) {
	t.Helper()
	meta := ls.db.Metadata()
	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, expectedReserve, uint64(state.Reserves))
	require.Equal(t, uint64(0), uint64(state.Treasury))

	pots, err := meta.GetRewardAdaPots(ls.currentEpoch.EpochId, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(
		t, expectedReserve, uint64(pots.Reserves),
		"the first Shelley epoch's pots must carry the boundary reserves",
	)
}

func newUnseededPreprodByronShelleyBoundaryLedger(
	t *testing.T,
) (*LedgerState, gledger.Block, gledger.Block) {
	t.Helper()
	return newUnseededByronShelleyBoundaryLedgerFrom(
		t,
		loadPreprodNodeConfig(t),
		loadBoundaryBlock(t, "preprod-byron-last-84242.cbor", 1),
		loadBoundaryBlock(t, "preprod-shelley-first-86400.cbor", 2),
		3,
		64_800,
		21_600,
	)
}
