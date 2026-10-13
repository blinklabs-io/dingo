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
	"encoding/hex"
	"fmt"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// queueClearHeaders builds count contiguous mock headers extending from, one
// block and 10 slots apart, named so two seeds never collide.
func queueClearHeaders(
	seed string,
	prevHash []byte,
	prevBlock uint64,
	prevSlot uint64,
	count int,
) []mockHeader {
	out := make([]mockHeader, 0, count)
	for i := range count {
		hash := testHashBytes(fmt.Sprintf("%s-%d", seed, i))
		out = append(out, mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: prevBlock + uint64(i) + 1,
			slot:        prevSlot + uint64(i+1)*10,
		})
		prevHash = hash
	}
	return out
}

func queueClearEvent(
	connId ouroboros.ConnectionId,
	h lcommon.BlockHeader,
	advertised ochainsync.Tip,
) ChainsyncEvent {
	return ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  h,
		Point:        ocommon.NewPoint(h.SlotNumber(), h.Hash().Bytes()),
		Tip:          advertised,
	}
}

// resyncRecorder collects ChainsyncResyncEvents. drain blocks on a barrier
// published after the handler calls, so every earlier request has arrived.
type resyncRecorder struct {
	bus *event.EventBus
	ch  <-chan event.Event
}

func newResyncRecorder(t *testing.T, bus *event.EventBus) *resyncRecorder {
	t.Helper()
	_, ch := bus.Subscribe(event.ChainsyncResyncEventType)
	return &resyncRecorder{bus: bus, ch: ch}
}

func (r *resyncRecorder) drain(t *testing.T) []event.ChainsyncResyncEvent {
	t.Helper()
	const barrier = "test barrier"
	r.bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{Reason: barrier},
		),
	)
	var out []event.ChainsyncResyncEvent
	for {
		evt := testutil.RequireReceive(
			t, r.ch, testutil.AsyncWait, "resync barrier",
		)
		e, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		if e.Reason == barrier {
			return out
		}
		out = append(out, e)
	}
}

// One genuine mismatch clears the header queue. The honest peer's remaining
// in-flight headers are still the canonical continuation of what it already
// sent, so they must not each count as a fork mismatch and sum to a
// persistent-fork resync of that peer.
func TestQueueClearDoesNotCascadeIntoPersistentFork(t *testing.T) {
	t.Parallel()

	for _, queued := range []int{5, 250, 257, 1000} {
		t.Run(fmt.Sprintf("queued=%d", queued), func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(func() { bus.Stop() })
			fixture.ls.config.EventBus = bus
			rec := newResyncRecorder(t, bus)

			tipHdr := fixture.currentTip
			advertised := ochainsync.Tip{
				Point:       ocommon.NewPoint(100000, testHashBytes("far-tip")),
				BlockNumber: 5000,
			}
			canon := queueClearHeaders(
				"canon", tipHdr.Point.Hash, tipHdr.BlockNumber,
				tipHdr.Point.Slot, queued+40,
			)
			for _, h := range canon[:queued] {
				require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
					queueClearEvent(fixture.connId, h, advertised)))
			}
			require.Equal(t, queued, fixture.ls.chain.HeaderCount(),
				"fixture must hold a pipeline of queued headers")

			// One genuine mismatch that production declines without a
			// resync: a shorter competing header that does not extend the
			// tip, so Praos refuses it and only the queue clear remains.
			stray := mockHeader{
				hash:        lcommon.NewBlake2b256(testHashBytes("shorter-competitor")),
				prevHash:    lcommon.NewBlake2b256(testHashBytes("competitor-parent")),
				blockNumber: tipHdr.BlockNumber - 1,
				slot:        tipHdr.Point.Slot,
			}
			require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
				queueClearEvent(fixture.connId, stray, advertised)))

			maxCount := 0
			for _, h := range canon[queued:] {
				require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
					queueClearEvent(fixture.connId, h, advertised)))
				maxCount = max(maxCount, fixture.ls.headerMismatchCount)
			}
			for _, e := range rec.drain(t) {
				require.Falsef(t, sameConnectionId(e.ConnectionId, fixture.connId),
					"honest peer resynced (reason %q, max mismatches %d)",
					e.Reason, maxCount)
			}
		})
	}
}

// Retention past the count limit applies only to headers still ahead of the
// local tip; headers the chain already holds stay bounded by the limit.
func TestPeerHeaderHistoryRetainsOnlyUnappliedPastLimit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		applied bool
		want    int
	}{
		{"ahead of tip", false, 300},
		{"at or behind tip", true, maxPeerHeaderHistoryPerConn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			tip := fixture.currentTip
			headers := queueClearHeaders(
				"hist", tip.Point.Hash, tip.BlockNumber, tip.Point.Slot, 300,
			)
			advertised := ochainsync.Tip{Point: tip.Point}
			for _, h := range headers {
				if tc.applied {
					h.slot = tip.Point.Slot
				}
				fixture.ls.recordPeerHeaderHistory(
					queueClearEvent(fixture.connId, h, advertised))
			}
			history := fixture.ls.peerHeaderHistory[connIdKey(fixture.connId)]
			require.NotNil(t, history)
			require.Equal(t, tc.want, len(history.order))
		})
	}
}

// Retention must hold for headers of production size: the byte budgets may not
// evict unapplied headers while the header queue still has room for them.
func TestPeerHeaderHistoryRetainsRealSizedUnappliedHeaders(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	capacity := fixture.ls.chain.MaxQueuedHeaders()
	count := capacity - 1
	tip := fixture.currentTip
	headers := queueClearHeaders(
		"sized", tip.Point.Hash, tip.BlockNumber, tip.Point.Slot, count,
	)
	advertised := ochainsync.Tip{Point: tip.Point}
	cbor := make([]byte, 900)
	for _, h := range headers {
		fixture.ls.recordPeerHeaderHistory(queueClearEvent(
			fixture.connId, sizedMockHeader{mockHeader: h, cbor: cbor},
			advertised,
		))
	}
	history := fixture.ls.peerHeaderHistory[connIdKey(fixture.connId)]
	require.NotNil(t, history)
	require.Equal(t, count, len(history.order))
	require.Equal(t, hex.EncodeToString(headers[0].hash.Bytes()),
		history.order[0])
	require.LessOrEqual(t, fixture.ls.peerHeaderHistoryBytes,
		maxPeerHeaderHistoryBytesTotal)
}

// Headers too large to fit the queue in the byte budget are still bounded.
func TestPeerHeaderHistoryUnappliedRetentionStaysByteBounded(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	tip := fixture.currentTip
	headers := queueClearHeaders(
		"huge", tip.Point.Hash, tip.BlockNumber, tip.Point.Slot, 2000,
	)
	advertised := ochainsync.Tip{Point: tip.Point}
	cbor := make([]byte, 64<<10)
	for _, h := range headers {
		fixture.ls.recordPeerHeaderHistory(queueClearEvent(
			fixture.connId, sizedMockHeader{mockHeader: h, cbor: cbor},
			advertised,
		))
	}
	history := fixture.ls.peerHeaderHistory[connIdKey(fixture.connId)]
	require.NotNil(t, history)
	require.LessOrEqual(t, history.retainedBytes,
		maxPeerHeaderHistoryUnappliedBytesPerConn)
	require.Greater(t, len(history.order), 0)
}
