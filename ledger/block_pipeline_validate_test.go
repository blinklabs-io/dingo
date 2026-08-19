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
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// productionValidateVerifyConfig mirrors the VerifyConfig NewLedgerState
// wires onto the block pipeline's validate stage: VRF/KES only, matching
// the scope of the existing serial header-only check (verifyBlockHeaderHex).
var productionValidateVerifyConfig = lcommon.VerifyConfig{
	SkipBodyHashValidation:    true,
	SkipTransactionValidation: true,
	SkipStakePoolValidation:   true,
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

func TestDecodeReadChainBatchValidatesBlocksWhenEnabled(t *testing.T) {
	var seed1, seed2 [32]byte
	seed1[0], seed2[0] = 1, 2
	const nonceSeed = 7

	block1, vb1 := buildValidatedTestModelsBlock(t, seed1, nonceSeed, 100, 1)
	block2, _ := buildValidatedTestModelsBlock(t, seed2, nonceSeed, 500, 2)
	rawBatch := []models.Block{block1, block2}

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:                       testLogger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(vb1.EpochNonceHex),
		pipeline.WithSlotsPerKesPeriod(vb1.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	defer func() {
		require.NoError(t, ls.blockPipeline.Stop())
	}()

	decoded, ok := ls.decodeReadChainBatch(t.Context(), rawBatch)
	require.True(t, ok)
	require.Len(t, decoded, 2)
	assert.Equal(t, block1.Slot, decoded[0].SlotNumber())
	assert.Equal(t, block2.Slot, decoded[1].SlotNumber())
}

func TestDecodeReadChainBatchRejectsFailedValidationWhenEnabled(t *testing.T) {
	var seed [32]byte
	seed[0] = 3
	block, vb := buildValidatedTestModelsBlock(t, seed, 7, 100, 1)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:                       testLogger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	// Configure the pipeline with a *different* epoch nonce than the one the
	// block was actually proven against, so VRF verification genuinely
	// fails (rather than tampering CBOR bytes, which risks failing decode
	// instead of validation and testing the wrong thing).
	wrongNonceHex := vb.EpochNonceHex[:len(vb.EpochNonceHex)-2] + "00"
	if wrongNonceHex == vb.EpochNonceHex {
		wrongNonceHex = vb.EpochNonceHex[:len(vb.EpochNonceHex)-2] + "11"
	}
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(wrongNonceHex),
		pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	defer func() {
		require.NoError(t, ls.blockPipeline.Stop())
	}()

	_, ok := ls.decodeReadChainBatch(t.Context(), []models.Block{block})
	assert.False(t, ok, "batch with a VRF-invalid block must be rejected")
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
	wrongNonceHex := vb.EpochNonceHex[:len(vb.EpochNonceHex)-2] + "00"
	if wrongNonceHex == vb.EpochNonceHex {
		wrongNonceHex = vb.EpochNonceHex[:len(vb.EpochNonceHex)-2] + "11"
	}
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0(wrongNonceHex),
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

func TestBlockPipelineEta0Provider_EmptyCache(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()

	_, err := ls.blockPipelineEta0Provider(1000)
	assert.Error(t, err)
}

// TestBlockPipelineEta0Provider_NoNonceForEpoch simulates a Byron-era epoch
// (present in the cache but with no Praos nonce yet) and confirms the
// provider fails rather than returning a bogus nonce -- decodeReadChainBatch
// relies on this to ignore validation failures specifically (and only) for
// Byron-era blocks.
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
	assert.Error(t, err)
}
