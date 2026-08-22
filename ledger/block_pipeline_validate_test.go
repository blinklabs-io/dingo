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
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// productionValidateVerifyConfig mirrors the generic VRF/KES portion of
// NewLedgerState's pipeline wiring. Dingo adds OpCert verification after the
// generic stage succeeds.
var productionValidateVerifyConfig = lcommon.VerifyConfig{
	SkipBodyHashValidation:    true,
	SkipTransactionValidation: true,
	SkipStakePoolValidation:   true,
}

func TestNewLedgerStateRejectsPipelineValidationWithoutKesConfig(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	_, err = NewLedgerState(LedgerStateConfig{
		ChainManager:                 cm,
		Database:                     db,
		Logger:                       testLogger(),
		BlockPipelineEnabled:         true,
		BlockPipelineValidateEnabled: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slotsPerKESPeriod")
}

// buildValidatedTestModelsBlock wraps testutil.BuildValidatedConwayBlockBytes
// as a models.Block plus its ValidatedConwayBlock parameters, for tests that
// need a genuinely VRF/KES-valid block flowing through decodeReadChainBatch.
func buildValidatedTestModelsBlock(
	t *testing.T,
	seed [32]byte,
	nonceSeed byte,
	slotRangeStart uint64,
	blockNumber uint64,
) (models.Block, testutil.ValidatedConwayBlock) {
	t.Helper()
	vb := testutil.BuildValidatedConwayBlockBytes(
		t,
		seed,
		nonceSeed,
		slotRangeStart,
		blockNumber,
	)
	return models.Block{
		Slot:   vb.Slot,
		Hash:   vb.Hash,
		Number: vb.BlockNumber,
		Type:   gledger.BlockTypeConway,
		Cbor:   vb.Cbor,
	}, vb
}

func wrongNonceHexFor(t *testing.T, nonceHex string) string {
	t.Helper()
	require.GreaterOrEqual(t, len(nonceHex), 2)

	wrongNonceHex := nonceHex[:len(nonceHex)-2] + "00"
	if wrongNonceHex == nonceHex {
		wrongNonceHex = nonceHex[:len(nonceHex)-2] + "11"
	}
	return wrongNonceHex
}

func newValidatedPipelineTestLedger(
	t *testing.T,
	vb testutil.ValidatedConwayBlock,
) *LedgerState {
	t.Helper()
	nonce, err := hex.DecodeString(vb.EpochNonceHex)
	require.NoError(t, err)
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 1_000_000,
				Nonce:         nonce,
			},
		},
		validationEnabled: true,
		config: LedgerStateConfig{
			CardanoNodeConfig:            newTestShelleyGenesisCfg(t),
			Logger:                       testLogger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	ls.slotsPerKESPeriod.Store(ls.loadSlotsPerKESPeriod())
	ls.publishSnapshotsLocked()
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(1),
		pipeline.WithValidateWorkers(1),
		pipeline.WithEta0(vb.EpochNonceHex),
		pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	t.Cleanup(func() {
		if ls.blockPipeline != nil {
			require.NoError(t, ls.blockPipeline.Stop())
		}
	})
	return ls
}

// loadRealByronMainBlock returns a genuine Byron main block from the shared
// ouroboros-consensus golden fixtures. The pipeline regression needs actual
// Byron CBOR rather than a mock whose Era method merely reports Byron.
func loadRealByronMainBlock(t *testing.T) models.Block {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Block_Byron_regular",
	)
	require.NoError(t, err)
	raw, err := fixture.ConsensusLedgerBlockBytes()
	require.NoError(t, err)
	blockType, err := fixture.LedgerBlockType()
	require.NoError(t, err)
	require.Equal(t, uint(gledger.BlockTypeByronMain), blockType)
	decoded, err := gledger.NewBlockFromCbor(blockType, raw)
	require.NoError(t, err)
	return models.Block{
		Slot:   decoded.SlotNumber(),
		Hash:   decoded.Hash().Bytes(),
		Number: decoded.BlockNumber(),
		Type:   blockType,
		Cbor:   raw,
	}
}

func TestDecodeReadChainBatchAcceptsRealByronWithValidation(t *testing.T) {
	block := loadRealByronMainBlock(t)
	ls := &LedgerState{
		validationEnabled: true,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         nil,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig:            newTestShelleyGenesisCfg(t),
			Logger:                       testLogger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	ls.slotsPerKESPeriod.Store(ls.loadSlotsPerKESPeriod())
	ls.publishSnapshotsLocked()
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(1),
		pipeline.WithValidateWorkers(1),
		pipeline.WithEta0Provider(ls.blockPipelineEta0Provider),
		pipeline.WithSlotsPerKesPeriod(ls.SlotsPerKESPeriod()),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, ls.blockPipeline.Stop()) })

	decoded, ok := ls.decodeReadChainBatch(
		t.Context(),
		[]models.Block{block},
	)
	require.True(t, ok)
	require.Len(t, decoded, 1)
	require.Equal(t, gledger.BlockTypeByronMain, decoded[0].Type())
}

func TestVerifyBlockHeaderStatelessCryptoRejectsTamperedByronSignature(
	t *testing.T,
) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	header, ok := block.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	require.Len(t, header.ConsensusData.BlockSig, 2)
	proxySignature, ok := header.ConsensusData.BlockSig[1].([]any)
	require.True(t, ok)
	require.Len(t, proxySignature, 2)
	signature, ok := proxySignature[1].([]byte)
	require.True(t, ok)
	require.NotEmpty(t, signature)
	signature[0] ^= 0xff

	nodeConfig := newByronPBFTTestNodeConfig(t, block, 10)
	ls := &LedgerState{
		config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
	}
	_, err = ls.verifyBlockHeaderStatelessCrypto(block, false)
	require.ErrorContains(t, err, "signature")
}

func TestDecodeReadChainBatchRejectsInvalidOpCertWhenEnabled(t *testing.T) {
	var seed [32]byte
	seed[0] = 91
	vb := testutil.BuildValidatedConwayBlockBytesWithInvalidOpCert(
		t,
		seed,
		17,
		100,
		1,
	)
	block := models.Block{
		Slot:   vb.Slot,
		Hash:   vb.Hash,
		Number: vb.BlockNumber,
		Type:   gledger.BlockTypeConway,
		Cbor:   vb.Cbor,
	}
	ls := newValidatedPipelineTestLedger(t, vb)

	_, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	assert.False(
		t,
		ok,
		"VRF/KES-valid block with an unrelated OpCert signer must be rejected",
	)
}

func TestDecodeReadChainBatchReturnsRecoverableValidationError(t *testing.T) {
	var seed [32]byte
	seed[0] = 94
	vb := testutil.BuildValidatedConwayBlockBytesWithInvalidOpCert(
		t,
		seed,
		20,
		100,
		1,
	)
	block := models.Block{
		Slot:   vb.Slot,
		Hash:   vb.Hash,
		Number: vb.BlockNumber,
		Type:   gledger.BlockTypeConway,
		Cbor:   vb.Cbor,
	}
	ls := newValidatedPipelineTestLedger(t, vb)

	_, err := ls.decodeReadChainBatchWithError(
		t.Context(),
		[]models.Block{block},
	)
	require.Error(t, err)
	var validationErr *headerValidationError
	require.ErrorAs(t, err, &validationErr)
	require.NotNil(t, validationErr)
	assert.Equal(t, block.Slot, validationErr.BlockPoint.Slot)
	assert.Equal(t, block.Hash, validationErr.BlockPoint.Hash)
	assert.Contains(t, validationErr.Cause.Error(), "operational certificate")
}

func TestDecodeReadChainBatchRejectsExpiredOpCertWhenEnabled(t *testing.T) {
	var seed [32]byte
	seed[0] = 92
	const slotsPerKesPeriod = uint64(129600)
	block, vb := buildValidatedTestModelsBlock(
		t,
		seed,
		18,
		2*slotsPerKesPeriod,
		1,
	)
	ls := newValidatedPipelineTestLedger(t, vb)
	ls.config.CardanoNodeConfig.ShelleyGenesis().MaxKESEvolutions = 1

	_, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	assert.False(
		t,
		ok,
		"block beyond the OpCert's maximum KES evolutions must be rejected",
	)
}

func TestDecodeReadChainBatchSkipsValidationWithoutCachedNonce(
	t *testing.T,
) {
	var seed [32]byte
	seed[0] = 93
	block, vb := buildValidatedTestModelsBlock(t, seed, 19, 100, 1)
	ls := newValidatedPipelineTestLedger(t, vb)
	ls.epochCache = []models.Epoch{
		{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 1_000_000,
			Nonce:         nil,
		},
	}
	ls.publishSnapshotsLocked()
	require.NoError(t, ls.blockPipeline.Stop())
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(1),
		pipeline.WithValidateWorkers(1),
		pipeline.WithEta0Provider(ls.blockPipelineEta0Provider),
		pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))

	decoded, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	require.True(
		t,
		ok,
		"the read path must mirror admission's nonce-availability gate",
	)
	require.Len(t, decoded, 1)
}

func TestDecodeReadChainBatchMirrorsSerialValidationGates(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*LedgerState, models.Block)
	}{
		{
			name: "historical validation disabled",
			mutate: func(ls *LedgerState, _ models.Block) {
				ls.validationEnabled = false
			},
		},
		{
			name: "inside Mithril trust boundary",
			mutate: func(ls *LedgerState, block models.Block) {
				ls.mithrilLedgerSlot = block.Slot
			},
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seed [32]byte
			seed[0] = byte(95 + index) //nolint:gosec
			block, vb := buildValidatedTestModelsBlock(
				t,
				seed,
				21,
				100,
				1,
			)
			ls := newValidatedPipelineTestLedger(t, vb)
			tt.mutate(ls, block)
			ls.publishSnapshotsLocked()

			require.NoError(t, ls.blockPipeline.Stop())
			ls.blockPipeline = pipeline.NewBlockPipeline(
				pipeline.WithDecodeWorkers(1),
				pipeline.WithValidateWorkers(1),
				pipeline.WithEta0(wrongNonceHexFor(t, vb.EpochNonceHex)),
				pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
				pipeline.WithVerifyConfig(productionValidateVerifyConfig),
			)
			require.NoError(t, ls.blockPipeline.Start(t.Context()))

			decoded, ok := ls.decodeReadChainBatch(
				t.Context(),
				[]models.Block{block},
			)
			require.True(t, ok)
			require.Len(t, decoded, 1)
		})
	}
}

func TestDecodeReadChainBatchValidatesBlocksWhenEnabled(t *testing.T) {
	var seed1, seed2 [32]byte
	seed1[0], seed2[0] = 1, 2
	const nonceSeed = 7

	block1, vb1 := buildValidatedTestModelsBlock(t, seed1, nonceSeed, 100, 1)
	block2, _ := buildValidatedTestModelsBlock(t, seed2, nonceSeed, 500, 2)
	rawBatch := []models.Block{block1, block2}

	ls := newValidatedPipelineTestLedger(t, vb1)

	decoded, ok := ls.decodeReadChainBatch(t.Context(), rawBatch)
	require.True(t, ok)
	require.Len(t, decoded, 2)
	assert.Equal(t, block1.Slot, decoded[0].SlotNumber())
	assert.Equal(t, block2.Slot, decoded[1].SlotNumber())
}

// TestDecodeReadChainBatchValidatesBlockAcrossKesPeriodBoundary is a
// regression test for BuildValidatedConwayBlockBytes signing every block at
// hardcoded KES period 0 regardless of which slot in its search window
// actually won leadership. A slotRangeStart at or after slotsPerKesPeriod
// (129600) forces every candidate slot in the 200-slot search window into
// KES period 1 or later, so whichever slot is chosen requires the signing
// key to have been evolved past period 0 -- did not happen before the fix,
// and made every such block fail KES verification.
//
// slotRangeStart is set to exactly one full KES period rather than a range
// straddling the boundary: with the 99% active slot coefficient used
// throughout this helper, the very first slot tried is overwhelmingly
// likely to already be eligible, so a window starting *before* the
// boundary would almost never actually reach a post-boundary slot and
// would not reliably exercise the bug at all.
func TestDecodeReadChainBatchValidatesBlockAcrossKesPeriodBoundary(
	t *testing.T,
) {
	var seed [32]byte
	seed[0] = 9
	const nonceSeed = 11
	const slotsPerKesPeriod = 129600

	block, vb := buildValidatedTestModelsBlock(
		t, seed, nonceSeed, slotsPerKesPeriod, 1,
	)
	require.GreaterOrEqual(t, vb.Slot, uint64(slotsPerKesPeriod))

	ls := newValidatedPipelineTestLedger(t, vb)

	decoded, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	require.True(
		t,
		ok,
		"block at slot %d (KES period %d) failed VRF/KES validation",
		vb.Slot,
		vb.Slot/slotsPerKesPeriod,
	)
	require.Len(t, decoded, 1)
	assert.Equal(t, block.Slot, decoded[0].SlotNumber())
}

func TestDecodeReadChainBatchRejectsFailedValidationWhenEnabled(t *testing.T) {
	var seed [32]byte
	seed[0] = 3
	block, vb := buildValidatedTestModelsBlock(t, seed, 7, 100, 1)

	ls := newValidatedPipelineTestLedger(t, vb)
	require.NoError(t, ls.blockPipeline.Stop())
	// Configure the pipeline with a *different* epoch nonce than the one the
	// block was actually proven against, so VRF verification genuinely
	// fails (rather than tampering CBOR bytes, which risks failing decode
	// instead of validation and testing the wrong thing).
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(wrongNonceHexFor(t, vb.EpochNonceHex)),
		pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))

	_, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	assert.False(t, ok, "batch with a VRF-invalid block must be rejected")
}

// TestDecodeReadChainBatchDrainsRemainingResultsAfterEarlyFailure is a
// regression test for a batch failure leaking unread pipeline results into
// a later, unrelated call. decodeReadChainBatch submits an entire batch up
// front and only afterwards reads that many entries back off the shared
// blockPipeline.Results() channel; if it stops reading as soon as it sees
// the first failure, the remaining entries it already Submitted for this
// batch are left sitting on that channel. blockPipeline has no notion of
// "whose submission is whose" beyond result order, so the very next call --
// here, a second batch with its own, unrelated block -- would then read
// those stale leftovers instead of its own submission's result.
//
// The first batch here has a VRF-invalid block ahead of two otherwise-valid
// ones, so a regression that returns as soon as it sees the invalid block
// leaves the two valid ones' results unread. The second call must still
// see its own block's result, not one of those leftovers.
func TestDecodeReadChainBatchDrainsRemainingResultsAfterEarlyFailure(
	t *testing.T,
) {
	const correctNonceSeed = 20
	const wrongNonceSeed = 21

	var badSeed, seed2, seed3, seed4 [32]byte
	badSeed[0], seed2[0], seed3[0], seed4[0] = 1, 2, 3, 4

	// invalidBlock is proven against a different epoch nonce than the one
	// the pipeline below is configured with, so it genuinely fails VRF
	// verification (not merely a decode error).
	invalidBlock, _ := buildValidatedTestModelsBlock(
		t, badSeed, wrongNonceSeed, 100, 1,
	)
	validBlock2, vb2 := buildValidatedTestModelsBlock(
		t, seed2, correctNonceSeed, 300, 2,
	)
	validBlock3, _ := buildValidatedTestModelsBlock(
		t, seed3, correctNonceSeed, 500, 3,
	)
	secondBatchBlock, _ := buildValidatedTestModelsBlock(
		t, seed4, correctNonceSeed, 700, 4,
	)

	ls := newValidatedPipelineTestLedger(t, vb2)
	require.NoError(t, ls.blockPipeline.Stop())
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(vb2.EpochNonceHex),
		pipeline.WithSlotsPerKesPeriod(vb2.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))

	_, ok := ls.decodeReadChainBatch(
		t.Context(),
		[]models.Block{invalidBlock, validBlock2, validBlock3},
	)
	require.False(t, ok, "batch with a VRF-invalid block must be rejected")

	decoded, ok := ls.decodeReadChainBatch(
		t.Context(),
		[]models.Block{secondBatchBlock},
	)
	require.True(
		t,
		ok,
		"second batch's own valid block must not be rejected by a "+
			"leftover result from the first batch",
	)
	require.Len(t, decoded, 1)
	assert.Equal(
		t,
		secondBatchBlock.Slot,
		decoded[0].SlotNumber(),
		"second batch received a leftover result from the first batch "+
			"instead of its own submission",
	)
}

// TestDecodeReadChainBatchIgnoresValidationOutcomeWhenDisabled verifies that
// decodeReadChainBatch does not consult the pipeline's validation outcome at
// all when LedgerStateConfig.BlockPipelineValidateEnabled is false, even if
// the shared *pipeline.BlockPipeline instance happens to have validation
// wired (e.g. a stale/misconfigured instance) -- the config flag is the
// single source of truth for whether validation gates the batch.
func TestDecodeReadChainBatchIgnoresValidationOutcomeWhenDisabled(
	t *testing.T,
) {
	var seed [32]byte
	seed[0] = 4
	block, vb := buildValidatedTestModelsBlock(t, seed, 7, 100, 1)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: testLogger(),
			// BlockPipelineValidateEnabled left false.
		},
	}
	// Wire a nonce that will fail validation -- if decodeReadChainBatch
	// incorrectly consulted the validation outcome anyway, this would flip
	// the assertion below from ok=true to ok=false.
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(wrongNonceHexFor(t, vb.EpochNonceHex)),
		pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	defer func() {
		require.NoError(t, ls.blockPipeline.Stop())
	}()

	decoded, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	require.True(t, ok)
	require.Len(t, decoded, 1)
	assert.Equal(t, block.Slot, decoded[0].SlotNumber())
}

// TestLedgerReadChainIteratorPipelineValidateMatchesSerial extends
// TestLedgerReadChainIteratorPipelineMatchesSerial (decode-only) to phase 3:
// running the full ledgerReadChainIterator loop with the pipeline's VRF/KES
// validate stage enabled against genuinely valid blocks must produce the
// identical decoded batch, in the identical order, as the pipeline-disabled
// serial path -- the acceptance criterion that the resulting chain tip must
// match between modes for a legitimately valid chain.
func TestLedgerReadChainIteratorPipelineValidateMatchesSerial(t *testing.T) {
	var seed1, seed2, seed3 [32]byte
	seed1[0], seed2[0], seed3[0] = 10, 11, 12
	const nonceSeed = 42

	block1, vb1 := buildValidatedTestModelsBlock(t, seed1, nonceSeed, 100, 1)
	block2, _ := buildValidatedTestModelsBlock(t, seed2, nonceSeed, 500, 2)
	block3, _ := buildValidatedTestModelsBlock(t, seed3, nonceSeed, 900, 3)

	point := func(b models.Block) ocommon.Point {
		return ocommon.Point{Slot: b.Slot, Hash: b.Hash}
	}

	run := func(t *testing.T, ls *LedgerState) []gledger.Block {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resultCh := make(chan readChainResult)
		iter := &scriptedLedgerReadIterator{
			ctx: ctx,
			results: []*chain.ChainIteratorResult{
				{Point: point(block1), Block: block1},
				{Point: point(block2), Block: block2},
				{Point: point(block3), Block: block3},
			},
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			ls.ledgerReadChainIterator(ctx, iter, resultCh)
		}()
		result := <-resultCh
		require.False(t, result.rollback)
		require.Len(t, result.blocks, 3)
		close(result.done)
		cancel()
		<-done
		return result.blocks
	}

	serialLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	serialBlocks := run(t, serialLS)

	pipelineLS := &LedgerState{
		config: LedgerStateConfig{
			Logger:                       testLogger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	pipelineLS.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(vb1.EpochNonceHex),
		pipeline.WithSlotsPerKesPeriod(vb1.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, pipelineLS.blockPipeline.Start(t.Context()))
	defer func() {
		require.NoError(t, pipelineLS.blockPipeline.Stop())
	}()
	pipelineBlocks := run(t, pipelineLS)

	require.Len(t, serialBlocks, len(pipelineBlocks))
	for i := range serialBlocks {
		assert.Equal(
			t,
			serialBlocks[i].SlotNumber(),
			pipelineBlocks[i].SlotNumber(),
			"block %d slot mismatch",
			i,
		)
		assert.Equal(
			t,
			serialBlocks[i].Hash(),
			pipelineBlocks[i].Hash(),
			"block %d hash mismatch",
			i,
		)
	}
}

func TestBlockPipelineEta0Provider_ReturnsNonceHex(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         []byte{0x01, 0x02, 0x03},
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()

	nonceHex, err := ls.blockPipelineEta0Provider(1000)
	require.NoError(t, err)
	assert.Equal(t, "010203", nonceHex)
}

// TestBlockPipelineEta0Provider_EmptyCache is a regression test for
// blockPipelineEta0Provider previously wrapping *every* lookup error in
// errBlockPipelineEta0Unavailable. An empty epoch cache means the published
// state does not cover the slot at all, so it must be classified as deferred
// rather than as the distinct "covered epoch has no nonce" case.
func TestBlockPipelineEta0Provider_EmptyCache(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()

	_, err := ls.blockPipelineEta0Provider(1000)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errHeaderVerificationDeferred))
	assert.False(
		t,
		errors.Is(err, errBlockPipelineEta0Unavailable),
		"a slot outside the cache must not be classified as a covered epoch without a nonce",
	)
}

// TestBlockPipelineEta0Provider_NoNonceForEpoch covers an epoch present in the
// cache without a published Praos nonce. This is permanent for Byron and can
// be transient later; the provider must fail rather than inventing a nonce,
// while preserving the sentinel that lets enforcement defer to admission.
func TestBlockPipelineEta0Provider_NoNonceForEpoch(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         nil,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()

	_, err := ls.blockPipelineEta0Provider(1000)
	require.Error(t, err)
	assert.True(
		t,
		errors.Is(err, errBlockPipelineEta0Unavailable),
		"a covered epoch without a nonce must retain the eta0-unavailable sentinel",
	)
}
