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

// TestShouldVerifyChainsyncHeaderCryptoKeepsAdmissionGate verifies that a
// later pipeline stage does not weaken the chainsync admission gate. Once a
// header is queued, the corresponding block can be persisted and served
// before the ledger reader reaches the pipeline validation stage.
func TestShouldVerifyChainsyncHeaderCryptoKeepsAdmissionGate(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{60}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.publishSnapshotsLocked()

	slot := tb.block.SlotNumber()
	verifyNow, _ := ls.chainsyncHeaderCryptoPolicy(slot)
	require.True(
		t,
		verifyNow,
		"sanity: the crypto pre-check runs without a validating pipeline",
	)

	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true
	verifyNow, _ = ls.chainsyncHeaderCryptoPolicy(slot)
	assert.True(
		t,
		verifyNow,
		"pipeline validation must not replace the admission-time crypto gate",
	)
}

// TestHandleEventBlockfetchBlockKeepsAdmissionCryptoWhenPipelineValidates
// proves the blockfetch-time admission gate remains fail-closed when the
// later pipeline validate stage is enabled.
// The test block's VRF proof is deliberately tampered (a genuine crypto
// failure, not a deferred one) while the ledger tip is left one slot behind
// the block so that the non-crypto state checks
// (verifyRegisteredVrfKey/verifyBlockLeaderEligibility) defer rather than
// hard-fail -- matching
// TestVerifyBlockHeaderCryptoBeforeApplyDefersMissingPoolState's fixture.
//
// Without a validating pipeline, handleEventBlockfetchBlock must still catch
// the tampered VRF proof directly (a real, non-deferred error). The pipeline
// cannot replace this gate because the block becomes visible to downstream
// readers before its later validate stage runs.
func TestHandleEventBlockfetchBlockKeepsAdmissionCryptoWhenPipelineValidates(
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
	// touched, then enable the pipeline's validate stage. The identical
	// tampered block must still be rejected before persistence.
	ls.pendingBlockfetchEvents = nil
	ls.shadowBlockReceivedHashes = nil
	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true

	err = ls.handleEventBlockfetchBlock(evt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "crypto verification failed")
	assert.Empty(t, ls.pendingBlockfetchEvents)
}

func TestHandleEventBlockfetchBlockRejectsInvalidOpCertWhenPipelineValidates(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{62}, 0, tamperOpCertSig)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.chain = &chain.Chain{}
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true
	ls.publishSnapshotsLocked()

	err := ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point: ocommon.Point{
			Slot: tb.block.SlotNumber(),
			Hash: tb.block.Hash().Bytes(),
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "opcert cold-key signature invalid")
	assert.Empty(t, ls.pendingBlockfetchEvents)
}
