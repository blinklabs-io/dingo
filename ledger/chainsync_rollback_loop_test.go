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
	"time"

	"github.com/blinklabs-io/dingo/chain"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A crossable rollback the node actually applies must not leave its point in
// the per-connection loop detector. Otherwise a later, legitimate rollback to
// the same fork point counts the crossing we already made and is suppressed as
// a false loop, which is the reconnect-churn wedge in issue #2790.
func TestHandleEventChainsyncRollbackClearsLoopHistoryForCrossedPoint(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	})
	require.NoError(t, err)
	require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	for _, r := range fixture.ls.rollbackHistory {
		assert.Falsef(
			t,
			pointMatches(r.point, fixture.ancestorTip.Point),
			"a successfully applied rollback must clear its loop-detector record",
		)
	}
}

// The #2790 loop shape: after crossing a fork point the node advances forward
// on the peer's chain, then the peer rolls it back to the same fork point
// again. Because the successful first cross reset the loop counter, the second
// crossable rollback must be applied, not suppressed as a false loop.
func TestHandleEventChainsyncRollbackAppliesRepeatedCrossableRollback(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	}))
	require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	// Re-extend the chain past the fork point so a second rollback to it is
	// once more a real sub-K rollback.
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot,
			Hash:        fixture.currentTip.Point.Hash,
			BlockNumber: fixture.currentTip.BlockNumber,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Equal(
		t,
		fixture.currentTip.Point.Slot,
		fixture.ls.chain.Tip().Point.Slot,
	)

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	})
	require.NoError(t, err)
	require.NotErrorIs(t, err, ErrRollbackLoopDetected)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
}

// When the loop detector breaks a loop for a genuinely un-crossable rollback,
// the skip path must surface the stuck condition through the point-keyed #2728
// unrecoverable-rollback tracker so the escalation and metric can fire,
// instead of silently skipping and hiding a persistently un-recoverable
// divergence (issue #2790 bullet 6).
func TestHandleEventChainsyncRollbackSkipReportsUnrecoverableRollback(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	// A rollback point the node cannot cross to (a fork block below our tip
	// that is not present in our chain), so the loop detector genuinely skips
	// it rather than applying it.
	uncrossablePoint := ocommon.Point{
		Slot: fixture.currentTip.Point.Slot - 3,
		Hash: testHashBytes("uncrossable-report-fork-block"),
	}

	// Pre-seed one prior rollback so this call reaches the loop threshold
	// and takes the skip path.
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: uncrossablePoint.Slot,
				Hash: append([]byte(nil), uncrossablePoint.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        uncrossablePoint,
	})
	require.ErrorIs(t, err, ErrRollbackLoopDetected)

	_, ok := fixture.ls.unrecoverableRollbacks[unrecoverableRollbackKey(
		uncrossablePoint,
	)]
	assert.True(
		t,
		ok,
		"skip path must record the point in the unrecoverable-rollback tracker",
	)
}

// A crossable rollback that reaches the per-connection loop threshold must
// still be APPLIED, not suppressed as a false loop: the loop detector only
// breaks loops for rollbacks the node cannot cross (issue #2790, requirement
// 1). This exercises the appliability guard directly by pre-seeding history to
// the threshold, unlike the reset-on-success path which never reaches it.
func TestHandleEventChainsyncRollbackAppliesCrossableRollbackAtLoopThreshold(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	// Seed a prior rollback to the (crossable) shared ancestor so this call
	// hits the loop threshold.
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.ancestorTip.Point.Slot,
				Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	})
	require.NoError(t, err)
	require.NotErrorIs(t, err, ErrRollbackLoopDetected)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
}
