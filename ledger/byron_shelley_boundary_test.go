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
	for _, tc := range boundaryFixtures {
		t.Run(tc.name, func(t *testing.T) {
			last := loadBoundaryBlock(t, tc.byronFile, tc.byronType)
			first := loadBoundaryBlock(t, tc.shelleyFile, tc.shelleyType)
			parent := envelopeParentFromBlock(last)

			// The Byron parent itself needs no parameters: Byron returns
			// before the size checks.
			require.NoError(
				t,
				validateInboundBlockEnvelope(last, nil, envelopeParent{
					origin: true,
				}),
			)

			err := validateInboundBlockEnvelope(first, nil, parent)
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
			assert.NoError(t, validateInboundBlockEnvelope(first, pp, parent))

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
				validateInboundBlockEnvelope(first, tooSmall, parent),
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
	require.NoError(t, cfg.LoadByronGenesisFromReader(
		strings.NewReader(byronGenesisJSON),
	))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{
		securityParam: 2,
	}))

	lastByron := loadBoundaryBlock(
		t,
		"mainnet-byron-last-4492799.cbor",
		1,
	)
	firstShelley := loadBoundaryBlock(
		t,
		"mainnet-shelley-first-4492800.cbor",
		2,
	)
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
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))

	const (
		byronEpoch       = uint64(207)
		byronEpochStart  = uint64(4_471_200)
		byronEpochLength = uint(21_600)
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
// regression test for issue #3533: ledgerProcessBlocksFromSource must not
// signal a readChainResult's done channel until the whole result has been
// applied, including any post-boundary remainder deferred through
// cachedNextBatch.
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
		t, passReached, 2*time.Second,
		"pass 1 (boundary discovery) never reached the done-signal hook",
	)
	requireDoneNotYetClosed(
		"done must not fire after only the boundary-discovery pass",
	)
	releasePass <- struct{}{}

	// Pass 2: runs the epoch/era rollover and then actually applies
	// firstShelley.
	testutil.RequireReceive(
		t, passReached, 2*time.Second,
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
	case <-time.After(2 * time.Second):
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
		t, passReached, 2*time.Second,
		"pass 1 (boundary discovery) never reached the done-signal hook",
	)
	ls.config.CardanoNodeConfig = nil
	releasePass <- struct{}{}

	err := testutil.RequireReceive(
		t, processDone, 2*time.Second,
		"ledgerProcessBlocksFromSource never returned after the forced "+
			"epoch-rollover failure",
	)
	require.ErrorContains(t, err, "process epoch rollover")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(
			"readChainResult.done was left open after an epoch-rollover " +
				"failure, which would block the read-chain reader goroutine " +
				"forever",
		)
	}
}

// TestByronShelleyBoundarySeedsEpochNonceOnProductionPath pins the fix for
// #3559 through the same production path as
// TestByronShelleyBoundaryProcessesFirstShelleyBlockWithPParams: without the
// post-Byron nonce seeding in applyBoundaryEraTransitions (ledger/state.go),
// calculateEpochNonce returns a nil nonce for any rollover whose source era is
// Byron, regardless of the destination era, and the transitioned epoch is
// persisted with no nonce at all. That existing test only asserts on era,
// pparams, and tip, so it still passes with the nonce-seeding block deleted;
// this test asserts on the nonce itself, in all three places a caller can
// observe it — the in-memory current epoch, the epoch cache, and the
// persisted database row — and fails without the fix.
func TestByronShelleyBoundarySeedsEpochNonceOnProductionPath(t *testing.T) {
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
	require.NoError(t, ls.rollbackChainAndState(byronPoint))

	assert.Equal(t, byronPoint, ls.currentTip.Point)
	assert.Equal(t, eras.ByronEraDesc.Id, ls.currentEra.Id)
	assert.Nil(t, ls.currentPParams)
	assert.Nil(t, ls.prevEraPParams)
	assert.Equal(t, hardfork.NewTransitionUnknown(), ls.transitionInfo)
}
