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
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// replayHeaderApplyBlock wraps the header built by createTestBlock in a full
// Babbage block with an empty body and real CBOR, so it can be applied through
// ledgerProcessBlocksFromSource (whose tip-advance path stores the block and
// resolves its hash from the header rather than the zero hash realBabbageBlock
// reports).
func replayHeaderApplyBlock(t *testing.T, tb *testBlockResult) *babbage.BabbageBlock {
	t.Helper()
	header := tb.block.header
	headerCbor, err := cbor.Encode(header)
	require.NoError(t, err)
	header.SetCbor(headerCbor)
	block := &babbage.BabbageBlock{BlockHeader: header}
	blockCbor, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	return block
}

// newReplayHeaderApplyLedger builds a LedgerState at the origin of a Babbage
// epoch whose nonce matches the VRF inputs createTestBlock generates, with the
// given blocks already on the primary chain (so the apply path's candidate-tip
// lookup finds them). Historical validation is off, matching the bulk-load
// replay path VerifyReplayHeaders targets.
func newReplayHeaderApplyLedger(
	t *testing.T,
	epochNonce []byte,
	blocks ...gledger.Block,
) *LedgerState {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	rawBlocks := make([]chain.RawBlock, 0, len(blocks))
	for _, block := range blocks {
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        block.SlotNumber(),
			Hash:        block.Hash().Bytes(),
			BlockNumber: block.BlockNumber(),
			Type:        uint(block.Type()),
			PrevHash:    block.PrevHash().Bytes(),
			Cbor:        block.Cbor(),
		})
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), rawBlocks))

	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     nodeConfig,
		Logger:                testLogger(),
		PromRegistry:          prometheus.NewRegistry(),
		ManualBlockProcessing: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })

	ls.currentEra = eras.BabbageEraDesc
	ls.currentPParams = &babbage.BabbageProtocolParameters{
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
		ProtocolMajor:      7,
	}
	ls.currentEpoch = models.Epoch{
		EpochId:       5,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: 1_000_000,
		EraId:         eras.BabbageEraDesc.Id,
		Nonce:         epochNonce,
	}
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = make([]byte, 32)
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))
	return ls
}

// TestReplayHeaderVerificationAppliesOnReplayPath drives the full
// ledgerProcessBlocksFromSource batch-apply loop with VerifyReplayHeaders on,
// proving the state.go wiring (prepareReplayHeaders, startReplayHeaderJobs and
// withReplayHeaderVerification) actually feeds the pre-verified crypto verdict
// into ordered apply: a block whose KES signature was tampered is rejected by
// apply, not by a directly invoked verifier.
func TestReplayHeaderVerificationAppliesOnReplayPath(t *testing.T) {
	t.Parallel()

	valid := createTestBlock(t, [32]byte{60}, 0, tamperNone)
	tampered := createTestBlock(t, [32]byte{61}, 0, tamperKESSig)

	validBlock := replayHeaderApplyBlock(t, valid)
	tamperedBlock := replayHeaderApplyBlock(t, tampered)

	ls := newReplayHeaderApplyLedger(
		t, valid.epochNonce, validBlock,
	)
	ls.config.VerifyReplayHeaders = true

	tamperedPoint := ocommon.NewPoint(
		tamperedBlock.SlotNumber(),
		tamperedBlock.Hash().Bytes(),
	)

	results := make(chan readChainResult, 1)
	results <- readChainResult{
		blocks: []gledger.Block{tamperedBlock, validBlock},
	}
	close(results)

	err := ls.ledgerProcessBlocksFromSource(t.Context(), results)
	require.Error(t, err)

	// The rejected header is deterministic (the block is already persisted),
	// so manual block processing surfaces the typed rejection wrapped in the
	// pipeline-restart sentinel. Assert the block point and the crypto cause to
	// prove the pre-verified KES verdict, not an ordered stake/eligibility
	// check, was what apply consumed.
	var hve *headerValidationError
	require.ErrorAs(t, err, &hve)
	require.Equal(t, tamperedPoint, hve.BlockPoint)
	require.Contains(t, hve.Cause.Error(), "KES")
	require.Equal(t, ochainsync.Tip{}, ls.currentTip)
}
