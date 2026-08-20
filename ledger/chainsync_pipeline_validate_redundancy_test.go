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
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockPipelineRevalidatesCrypto exercises blockPipelineRevalidatesCrypto
// directly: it must be true only when a block-decode pipeline is actually
// constructed (ls.blockPipeline != nil -- nil whenever BlockPipelineEnabled
// is off or ManualBlockProcessing bypasses it, per NewLedgerState) AND its
// VRF/KES validate stage is enabled.
func TestBlockPipelineRevalidatesCrypto(t *testing.T) {
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	assert.False(
		t,
		ls.blockPipelineRevalidatesCrypto(),
		"no pipeline constructed",
	)

	ls.blockPipeline = pipeline.NewBlockPipeline()
	assert.False(
		t,
		ls.blockPipelineRevalidatesCrypto(),
		"pipeline constructed but its validate stage is not enabled",
	)

	ls.config.BlockPipelineValidateEnabled = true
	assert.True(
		t,
		ls.blockPipelineRevalidatesCrypto(),
		"pipeline constructed with its validate stage enabled",
	)
}

// TestShouldVerifyChainsyncHeaderCryptoSkipsWhenPipelineRevalidates verifies
// that the chainsync-header-time stateless VRF/KES pre-check
// (shouldVerifyChainsyncHeaderCrypto, consumed by
// handleEventChainsyncBlockHeaderWithPending) is skipped once the
// block-decode pipeline's validate stage will independently re-verify the
// same block before ledger apply -- even though every other condition that
// would otherwise require the check (validationEnabled and a cached epoch
// nonce for the slot) still holds.
func TestShouldVerifyChainsyncHeaderCryptoSkipsWhenPipelineRevalidates(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{60}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.publishSnapshotsLocked()

	slot := tb.block.SlotNumber()
	require.True(
		t,
		ls.shouldVerifyChainsyncHeaderCrypto(slot),
		"sanity: the crypto pre-check runs without a validating pipeline",
	)

	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true
	assert.False(
		t,
		ls.shouldVerifyChainsyncHeaderCrypto(slot),
		"redundant admission-time crypto pre-check must be skipped once "+
			"the pipeline will re-verify VRF/KES before apply",
	)
}

// TestHandleEventBlockfetchBlockSkipsRedundantCryptoWhenPipelineValidates
// proves the blockfetch-time half of the same de-duplication end to end.
// The test block's VRF proof is deliberately tampered (a genuine crypto
// failure, not a deferred one) while the ledger tip is left one slot behind
// the block so that the non-crypto state checks
// (verifyRegisteredVrfKey/verifyBlockLeaderEligibility) defer rather than
// hard-fail -- matching
// TestVerifyBlockHeaderCryptoBeforeApplyDefersMissingPoolState's fixture.
//
// Without a validating pipeline, handleEventBlockfetchBlock must still catch
// the tampered VRF proof directly (a real, non-deferred error). Once the
// pipeline's validate stage is wired in, the same block must be accepted at
// admission (its state checks legitimately defer) because the pipeline will
// independently reject the bad VRF proof before ledger apply
// (decodeReadChainBatch) -- proving the serial crypto re-check is no longer
// performed for it here.
func TestHandleEventBlockfetchBlockSkipsRedundantCryptoWhenPipelineValidates(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{61}, 0, tamperVRFProof)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.chain = &chain.Chain{}
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.publishSnapshotsLocked()

	evt := BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point: ocommon.Point{
			Slot: tb.block.SlotNumber(),
			Hash: tb.block.Hash().Bytes(),
		},
	}

	// Without a validating pipeline, the tampered VRF proof is caught
	// directly here as a genuine (non-deferred) crypto failure.
	err := ls.handleEventBlockfetchBlock(evt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "crypto verification failed")
	assert.Empty(t, ls.pendingBlockfetchEvents)

	// Reset the per-block dedup/bookkeeping state the failed call above
	// touched, then enable the pipeline's validate stage: the identical
	// tampered block must now be admitted (deferred to state-only checks)
	// because the pipeline, not this admission path, is now responsible
	// for catching the bad VRF proof before ledger apply.
	ls.pendingBlockfetchEvents = nil
	ls.shadowBlockReceivedHashes = nil
	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true

	err = ls.handleEventBlockfetchBlock(evt)
	require.NoError(t, err)
	assert.Len(t, ls.pendingBlockfetchEvents, 1)
}
