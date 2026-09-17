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
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestartQueuedBlockfetchAfterForkPreservesInFlightBatchFromOtherConnection
// pins the second half of the live chain-switch-storm fix: even after
// SwitchBackCooldown bounded the RATE of chain-selection switches, a running
// Preview instance built from that fix still applied zero blocks, because
// tryResolveFork's "fork extends from current tip" branch calls
// restartQueuedBlockfetchAfterForkLocked on essentially every
// active-connection switch (the newly active connection's next header
// almost never fits a header queue built by the connection it replaced),
// and that function previously tore down ANY in-flight batch unconditionally
// -- bypassing handoffPipelineOnSwitchLocked's #1922 "preserve in-flight
// blockfetch batch across chain switch" protection through this side
// channel. handleEventBlockfetchBlockDeferred only accepts blocks whose
// connection matches the CURRENT activeBlockfetchConnId, so every block
// already in flight from the torn-down batch was silently discarded on
// arrival, and if the active connection changed faster than one batch's
// round-trip -- true even at a bounded switch rate, confirmed live via
// dingo_ledger_block_stage_duration_seconds{stage="apply"} staying at a
// zero count while blockfetch protocol messages kept arriving -- no block
// ever survived to be applied.
func TestRestartQueuedBlockfetchAfterForkPreservesInFlightBatchFromOtherConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, testChain := newForkExtensionRestartFixture(t)
	otherConn := testChainsyncConnId(6000, 3001)
	newConn := testChainsyncConnId(6000, 3002)

	inFlight := make(chan struct{})
	ls.chainsyncBlockfetchReadyChan = inFlight
	ls.activeBlockfetchConnId = otherConn
	ls.selectedBlockfetchConnId = otherConn
	// Slot 1 is Mithril-covered so handleEventBlockfetchBlockDeferred skips
	// header crypto verification below: that machinery is orthogonal to what
	// this test proves (acceptance is gated on activeBlockfetchConnId, not on
	// whether a restart was attempted), matching the same setup
	// TestBlockfetchHeaderVerificationSkippedForMithrilCoveredSlot uses.
	ls.mithrilLedgerSlot = 1
	ls.publishSnapshotsLocked()

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) error {
		requestCount++
		return nil
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, newConn, nil),
	)

	assert.Equal(
		t,
		0,
		requestCount,
		"a healthy in-flight batch from a different connection must not be interrupted",
	)
	assert.Equal(
		t,
		inFlight,
		ls.chainsyncBlockfetchReadyChan,
		"the in-flight batch's ready channel must survive untouched",
	)
	assert.Equal(
		t,
		otherConn,
		ls.activeBlockfetchConnId,
		"the connection actually fetching must not change",
	)
	assert.Equal(
		t,
		newConn,
		ls.selectedBlockfetchConnId,
		"the NEXT batch must still be retargeted to the new connection",
	)
	assert.Equal(t, 1, testChain.HeaderCount(),
		"queued fork-extension headers survive alongside the preserved batch")

	// The in-flight batch's own connection is still the one blockfetch will
	// accept blocks from: this is the property that actually matters, since
	// handleEventBlockfetchBlockDeferred keys acceptance on
	// activeBlockfetchConnId, not on whether a restart was ever attempted.
	require.NoError(t, ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: otherConn,
		Block:        &mockBabbageBlock{slot: 1},
		Point:        ocommon.Point{Slot: 1, Hash: []byte("fork-ext-hdr-1")},
	}, nil))
	assert.Len(
		t,
		ls.pendingBlockfetchEvents,
		1,
		"a block from the preserved in-flight connection must still be accepted",
	)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestRestartQueuedBlockfetchAfterForkStillRestartsSameConnection asserts the
// narrower surviving case: a restart requested for the SAME connection that
// is already fetching still tears down and restarts unconditionally. There
// is no "different peer being preempted" to protect against here, and
// TestStartQueuedBlockfetchAfterForkRestartClearsShadowState
// (chainsync_shadow_test.go) depends on this path resetting per-batch shadow
// state even when connId is already the active connection.
func TestRestartQueuedBlockfetchAfterForkStillRestartsSameConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newForkExtensionRestartFixture(t)
	sameConn := testChainsyncConnId(6000, 3001)

	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = sameConn
	ls.selectedBlockfetchConnId = sameConn

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) error {
		requestCount++
		return nil
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, sameConn, nil),
	)

	assert.Equal(
		t,
		1,
		requestCount,
		"a restart on the same connection must still start a fresh batch",
	)
	assert.Equal(t, sameConn, ls.activeBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}
