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

package chainsync_test

import (
	"net"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// newTestConnId creates a unique ConnectionId for testing by
// using the id as the remote port number.
func newTestConnId(id uint) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 3001,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: int(id),
		},
	}
}

func newTestEventBus(t *testing.T) *event.EventBus {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	return bus
}

func newTestState(
	t *testing.T,
	bus *event.EventBus,
	cfg chainsync.Config,
) *chainsync.State {
	t.Helper()
	return chainsync.NewStateWithConfig(bus, nil, cfg)
}

// --- Client registry tests ---

func TestAddAndRemoveClientConnId(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)

	s.AddClientConnId(connA)
	require.True(t, s.HasClientConnId(connA))
	require.Equal(t, 1, s.ClientConnCount())

	s.AddClientConnId(connB)
	require.True(t, s.HasClientConnId(connB))
	require.Equal(t, 2, s.ClientConnCount())

	// First added should be active (primary)
	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connA, *active)

	// Remove first client; second should become active
	s.RemoveClientConnId(connA)
	require.False(t, s.HasClientConnId(connA))
	require.Equal(t, 1, s.ClientConnCount())

	active = s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)
}

func TestTryAddClientConnId_LimitEnforced(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.Config{
		MaxClients:   2,
		StallTimeout: 30 * time.Second,
	})

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	connC := newTestConnId(3)

	require.True(t, s.TryAddClientConnId(connA, 2))
	require.True(t, s.TryAddClientConnId(connB, 2))
	// Third should be rejected
	require.False(t, s.TryAddClientConnId(connC, 2))
	require.Equal(t, 2, s.ClientConnCount())
}

func TestTryAddClientConnId_DuplicateRejected(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	require.True(t, s.TryAddClientConnId(conn, 10))
	require.False(t, s.TryAddClientConnId(conn, 10))
	require.Equal(t, 1, s.ClientConnCount())
}

func TestAddClientConnId_DuplicatePreservesState(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)

	// Update tip so the client has meaningful state
	point := ocommon.NewPoint(500, []byte("block_hash"))
	tip := ochainsync.Tip{
		Point:       point,
		BlockNumber: 42,
	}
	s.UpdateClientTip(conn, point, tip)

	// Re-adding should be a no-op and preserve state
	s.AddClientConnId(conn)
	require.Equal(t, 1, s.ClientConnCount())

	tc := s.GetTrackedClient(conn)
	require.NotNil(t, tc)
	require.Equal(
		t, uint64(500), tc.Cursor.Slot,
		"cursor should be preserved",
	)
	require.Equal(
		t, uint64(1), tc.HeadersRecv,
		"headers counter should be preserved",
	)
}

func TestPromoteBestClient_NoHealthyClients(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.Config{
		MaxClients:   5,
		StallTimeout: 50 * time.Millisecond,
	}
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Set A as primary
	s.SetClientConnId(connA)

	// Update tips so both have data
	s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("ha")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("ha")),
		})
	s.UpdateClientTip(connB,
		ocommon.NewPoint(200, []byte("hb")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(200, []byte("hb")),
		})

	// Wait for stall timeout to elapse for both clients.
	// Use a map to deduplicate connection IDs across
	// polling iterations.
	stalledSet := make(
		map[ouroboros.ConnectionId]struct{},
	)
	require.Eventually(t, func() bool {
		for _, id := range s.CheckStalledClients() {
			stalledSet[id] = struct{}{}
		}
		return len(stalledSet) >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"both clients should become stalled",
	)
	require.Len(t, stalledSet, 2)

	// Remove primary; only stalled clients remain so
	// active should be nil (no fallback to bad peers).
	s.RemoveClientConnId(connA)
	active := s.GetClientConnId()
	require.Nil(
		t, active,
		"should not promote stalled client",
	)
}

func TestGetClientConnIds(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	ids := s.GetClientConnIds()
	require.Len(t, ids, 2)
	require.Contains(t, ids, connA)
	require.Contains(t, ids, connB)
}

func TestSetClientConnId(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Override active client
	s.SetClientConnId(connB)
	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)
}

// --- Failover tests ---

func TestFailover_PrimaryDisconnects(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	connC := newTestConnId(3)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)
	s.AddClientConnId(connC)

	// Update tips so B is the best
	s.UpdateClientTip(connA, ocommon.NewPoint(100, []byte("aa")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("aa"))})
	s.UpdateClientTip(connB, ocommon.NewPoint(300, []byte("bb")),
		ochainsync.Tip{Point: ocommon.NewPoint(300, []byte("bb"))})
	s.UpdateClientTip(connC, ocommon.NewPoint(200, []byte("cc")),
		ochainsync.Tip{Point: ocommon.NewPoint(200, []byte("cc"))})

	// Set A as primary, then remove it
	s.SetClientConnId(connA)
	s.RemoveClientConnId(connA)

	// B should be promoted (highest tip)
	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)
}

func TestFailover_PromotesNonStalledClient(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.Config{
		MaxClients:   5,
		StallTimeout: 50 * time.Millisecond,
	}
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	connC := newTestConnId(3)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)
	s.AddClientConnId(connC)

	// Give B a higher tip
	s.UpdateClientTip(connA, ocommon.NewPoint(100, []byte("aa")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("aa"))})
	s.UpdateClientTip(connB, ocommon.NewPoint(300, []byte("bb")),
		ochainsync.Tip{Point: ocommon.NewPoint(300, []byte("bb"))})
	s.UpdateClientTip(connC, ocommon.NewPoint(200, []byte("cc")),
		ochainsync.Tip{Point: ocommon.NewPoint(200, []byte("cc"))})

	// Set A as primary
	s.SetClientConnId(connA)

	// Poll until B's stall timeout elapses, refreshing A
	// and C on each iteration to keep them active. Use a
	// map to deduplicate IDs across polling iterations.
	stalledSet := make(
		map[ouroboros.ConnectionId]struct{},
	)
	require.Eventually(t, func() bool {
		s.UpdateClientTip(connA,
			ocommon.NewPoint(101, []byte("a2")),
			ochainsync.Tip{
				Point: ocommon.NewPoint(
					101, []byte("a2"),
				),
			})
		s.UpdateClientTip(connC,
			ocommon.NewPoint(201, []byte("c2")),
			ochainsync.Tip{
				Point: ocommon.NewPoint(
					201, []byte("c2"),
				),
			})
		for _, id := range s.CheckStalledClients() {
			stalledSet[id] = struct{}{}
		}
		return len(stalledSet) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"B should become stalled",
	)
	require.Contains(t, stalledSet, connB)

	// Remove A (primary)
	s.RemoveClientConnId(connA)

	// C should be promoted because B is stalled
	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connC, *active)
}

// --- Header deduplication tests ---

func TestHeaderDeduplication_NewHeader(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	s.AddClientConnId(connA)

	isNew := s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("hash1")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("hash1"))})
	require.True(t, isNew)
}

func TestHeaderDeduplication_DuplicateHeader(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	point := ocommon.NewPoint(100, []byte("same_hash"))
	tip := ochainsync.Tip{Point: point}

	// First report is new
	isNew := s.UpdateClientTip(connA, point, tip)
	require.True(t, isNew)

	// Second report of same hash is duplicate
	isNew = s.UpdateClientTip(connB, point, tip)
	require.False(t, isNew)
}

// --- Fork detection tests ---

func TestForkDetection(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ForkDetectedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Client A reports hash1 at slot 100
	s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("hash_a")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("hash_a")),
		})

	// Client B reports hash2 at slot 100 (fork)
	isNew := s.UpdateClientTip(connB,
		ocommon.NewPoint(100, []byte("hash_b")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("hash_b")),
		})
	require.True(t, isNew, "fork header should be treated as new")

	// Verify fork detection event was emitted
	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected fork detection event",
	)
	forkEvt, ok := evt.Data.(chainsync.ForkDetectedEvent)
	require.True(t, ok)
	require.Equal(t, uint64(100), forkEvt.Slot)
	require.Equal(t, []byte("hash_a"), forkEvt.HashA)
	require.Equal(t, []byte("hash_b"), forkEvt.HashB)
	require.Equal(t, connA, forkEvt.ConnIdA)
	require.Equal(t, connB, forkEvt.ConnIdB)
}

func TestForkDetection_DuplicateForkHashDeduplicated(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ForkDetectedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	connC := newTestConnId(3)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)
	s.AddClientConnId(connC)

	// A reports hash_a at slot 100
	s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("hash_a")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("hash_a")),
		})

	// B reports hash_b at slot 100 (fork)
	isNew := s.UpdateClientTip(connB,
		ocommon.NewPoint(100, []byte("hash_b")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("hash_b")),
		})
	require.True(t, isNew)

	// Consume the fork event
	_ = testutil.RequireReceive(
		t, ch, 2*time.Second,
		"expected fork detection event",
	)

	// C reports hash_b (same as B) -- should be
	// deduplicated, NOT trigger another fork event
	isNew = s.UpdateClientTip(connC,
		ocommon.NewPoint(100, []byte("hash_b")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("hash_b")),
		})
	require.False(t, isNew,
		"duplicate fork hash should be deduplicated",
	)

	// No additional fork event should be emitted
	testutil.RequireNoReceive(
		t, ch, 50*time.Millisecond,
		"no duplicate fork event expected",
	)
}

// --- Stall detection tests ---

func TestStallDetection(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ClientStalledEventType)
	cfg := chainsync.Config{
		MaxClients:   5,
		StallTimeout: 50 * time.Millisecond,
	}
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Update both initially
	s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("ha"))})
	s.UpdateClientTip(connB,
		ocommon.NewPoint(200, []byte("hb")),
		ochainsync.Tip{Point: ocommon.NewPoint(200, []byte("hb"))})

	// Wait for stall timeout to elapse. Use a map to
	// deduplicate connection IDs across polling iterations.
	stalledSet := make(
		map[ouroboros.ConnectionId]struct{},
	)
	require.Eventually(t, func() bool {
		for _, id := range s.CheckStalledClients() {
			stalledSet[id] = struct{}{}
		}
		return len(stalledSet) >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"both clients should become stalled",
	)
	require.Len(t, stalledSet, 2)

	// Verify stall events emitted
	evt1 := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected first stall event",
	)
	stalledEvt := evt1.Data.(chainsync.ClientStalledEvent)
	require.Contains(t,
		[]ouroboros.ConnectionId{connA, connB},
		stalledEvt.ConnId,
	)
}

func TestStallDetection_RecoveryOnActivity(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.Config{
		MaxClients:   5,
		StallTimeout: 50 * time.Millisecond,
	}
	s := newTestState(t, bus, cfg)

	conn := newTestConnId(1)
	s.AddClientConnId(conn)
	s.UpdateClientTip(conn,
		ocommon.NewPoint(100, []byte("h1")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("h1"))})

	// Wait for stall timeout to elapse. Use a map to
	// deduplicate connection IDs across polling iterations.
	stalledSet := make(
		map[ouroboros.ConnectionId]struct{},
	)
	require.Eventually(t, func() bool {
		for _, id := range s.CheckStalledClients() {
			stalledSet[id] = struct{}{}
		}
		return len(stalledSet) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"client should become stalled",
	)
	require.Len(t, stalledSet, 1)

	// Verify stalled status
	tc := s.GetTrackedClient(conn)
	require.NotNil(t, tc)
	require.Equal(t, chainsync.ClientStatusStalled, tc.Status)

	// Activity should recover the client
	s.UpdateClientTip(conn,
		ocommon.NewPoint(101, []byte("h2")),
		ochainsync.Tip{Point: ocommon.NewPoint(101, []byte("h2"))})
	tc = s.GetTrackedClient(conn)
	require.NotNil(t, tc)
	require.Equal(t, chainsync.ClientStatusSyncing, tc.Status)
}

func TestStallDetection_PrimaryFailover(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.Config{
		MaxClients:   5,
		StallTimeout: 50 * time.Millisecond,
	}
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Set A as primary with a high tip
	s.SetClientConnId(connA)
	s.UpdateClientTip(connA,
		ocommon.NewPoint(500, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("ha"))})
	s.UpdateClientTip(connB,
		ocommon.NewPoint(400, []byte("hb")),
		ochainsync.Tip{Point: ocommon.NewPoint(400, []byte("hb"))})

	// Poll until A's stall timeout elapses, keeping B
	// active. Use a map to deduplicate IDs across polling
	// iterations.
	stalledSet := make(
		map[ouroboros.ConnectionId]struct{},
	)
	require.Eventually(t, func() bool {
		s.UpdateClientTip(connB,
			ocommon.NewPoint(401, []byte("hb2")),
			ochainsync.Tip{
				Point: ocommon.NewPoint(
					401, []byte("hb2"),
				),
			})
		for _, id := range s.CheckStalledClients() {
			stalledSet[id] = struct{}{}
		}
		return len(stalledSet) >= 1
	}, 2*time.Second, 10*time.Millisecond,
		"A should become stalled",
	)
	require.Contains(t, stalledSet, connA)

	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)
}

// --- Client synced tests ---

func TestMarkClientSynced(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ClientSyncedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)
	s.UpdateClientTip(conn,
		ocommon.NewPoint(100, []byte("h1")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("h1"))})

	s.MarkClientSynced(conn)

	tc := s.GetTrackedClient(conn)
	require.NotNil(t, tc)
	require.Equal(t, chainsync.ClientStatusSynced, tc.Status)

	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected synced event",
	)
	syncEvt := evt.Data.(chainsync.ClientSyncedEvent)
	require.Equal(t, conn, syncEvt.ConnId)
	require.Equal(t, uint64(100), syncEvt.Slot)
}

func TestMarkClientSynced_IdempotentNoDoubleEvent(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ClientSyncedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)
	s.UpdateClientTip(conn,
		ocommon.NewPoint(100, []byte("h1")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(100, []byte("h1")),
		})

	// First call should emit event
	s.MarkClientSynced(conn)
	_ = testutil.RequireReceive(
		t, ch, 2*time.Second, "expected synced event",
	)

	// Second call should NOT emit another event
	s.MarkClientSynced(conn)
	testutil.RequireNoReceive(
		t, ch, 50*time.Millisecond,
		"no duplicate synced event expected",
	)
}

// --- Client events tests ---

func TestClientAddedEvent(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ClientAddedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)

	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected client added event",
	)
	addedEvt := evt.Data.(chainsync.ClientAddedEvent)
	require.Equal(t, conn, addedEvt.ConnId)
	require.Equal(t, 1, addedEvt.TotalClients)
}

func TestClientRemovedEvent(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ClientRemovedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)
	s.RemoveClientConnId(conn)

	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected client removed event",
	)
	removedEvt := evt.Data.(chainsync.ClientRemovedEvent)
	require.Equal(t, conn, removedEvt.ConnId)
	require.Equal(t, 0, removedEvt.TotalClients)
	require.True(t, removedEvt.WasPrimary)
}

func TestClientRemovedEvent_NonPrimary(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ClientRemovedEventType)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA) // becomes primary
	s.AddClientConnId(connB)
	s.RemoveClientConnId(connB) // not primary

	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected client removed event",
	)
	removedEvt := evt.Data.(chainsync.ClientRemovedEvent)
	require.Equal(t, connB, removedEvt.ConnId)
	require.False(t, removedEvt.WasPrimary)
}

// --- Tracked client state tests ---

func TestGetTrackedClient(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)
	s.UpdateClientTip(conn,
		ocommon.NewPoint(42, []byte("xyz")),
		ochainsync.Tip{
			Point:       ocommon.NewPoint(42, []byte("xyz")),
			BlockNumber: 10,
		})

	tc := s.GetTrackedClient(conn)
	require.NotNil(t, tc)
	require.Equal(t, conn, tc.ConnId)
	require.Equal(t, uint64(42), tc.Cursor.Slot)
	require.Equal(t, uint64(1), tc.HeadersRecv)
	require.Equal(t, chainsync.ClientStatusSyncing, tc.Status)
}

func TestGetTrackedClient_NotFound(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	tc := s.GetTrackedClient(newTestConnId(999))
	require.Nil(t, tc)
}

func TestGetTrackedClients(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	s.AddClientConnId(newTestConnId(1))
	s.AddClientConnId(newTestConnId(2))
	s.AddClientConnId(newTestConnId(3))

	clients := s.GetTrackedClients()
	require.Len(t, clients, 3)
}

// --- Seen headers cache tests ---

func TestClearSeenHeaders(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)

	s.UpdateClientTip(conn,
		ocommon.NewPoint(100, []byte("h1")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("h1"))})

	s.ClearSeenHeaders()

	// Same header should now be treated as new
	isNew := s.UpdateClientTip(conn,
		ocommon.NewPoint(100, []byte("h1")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("h1"))})
	require.True(t, isNew)
}

func TestPruneSeenHeaders(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Add headers at slots 50 and 150
	s.UpdateClientTip(connA,
		ocommon.NewPoint(50, []byte("old")),
		ochainsync.Tip{Point: ocommon.NewPoint(50, []byte("old"))})
	s.UpdateClientTip(connA,
		ocommon.NewPoint(150, []byte("new")),
		ochainsync.Tip{Point: ocommon.NewPoint(150, []byte("new"))})

	// Prune headers before slot 100
	s.PruneSeenHeaders(100)

	// Slot 50 should be treated as new again
	isNew := s.UpdateClientTip(connB,
		ocommon.NewPoint(50, []byte("old")),
		ochainsync.Tip{Point: ocommon.NewPoint(50, []byte("old"))})
	require.True(t, isNew)

	// Slot 150 should still be a duplicate
	isNew = s.UpdateClientTip(connB,
		ocommon.NewPoint(150, []byte("new")),
		ochainsync.Tip{Point: ocommon.NewPoint(150, []byte("new"))})
	require.False(t, isNew)
}

// --- Config tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := chainsync.DefaultConfig()
	require.Equal(t, chainsync.DefaultMaxClients, cfg.MaxClients)
	require.Equal(t, chainsync.DefaultStallTimeout, cfg.StallTimeout)
}

func TestNewStateWithConfig_InvalidDefaults(t *testing.T) {
	bus := newTestEventBus(t)
	// Zero/negative values should be replaced with defaults
	s := chainsync.NewStateWithConfig(bus, nil, chainsync.Config{
		MaxClients:   -1,
		StallTimeout: 0,
	})
	require.Equal(t, chainsync.DefaultMaxClients, s.MaxClients())
}

// --- ClientStatus string tests ---

func TestClientStatus_String(t *testing.T) {
	require.Equal(t, "syncing", chainsync.ClientStatusSyncing.String())
	require.Equal(t, "synced", chainsync.ClientStatusSynced.String())
	require.Equal(t, "stalled", chainsync.ClientStatusStalled.String())
	require.Equal(t, "failed", chainsync.ClientStatusFailed.String())
	require.Equal(t, "unknown", chainsync.ClientStatus(99).String())
}

// --- NewState backward compatibility ---

func TestNewState_BackwardCompatible(t *testing.T) {
	bus := newTestEventBus(t)
	// NewState (no config) should still work
	s := chainsync.NewState(bus, nil)
	require.NotNil(t, s)
	require.Equal(t, chainsync.DefaultMaxClients, s.MaxClients())

	// Single client should work exactly as before
	conn := newTestConnId(1)
	s.AddClientConnId(conn)
	require.True(t, s.HasClientConnId(conn))

	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, conn, *active)

	s.RemoveClientConnId(conn)
	require.False(t, s.HasClientConnId(conn))
	require.Equal(t, 0, s.ClientConnCount())
}

// --- UpdateClientTip for untracked connection ---

func TestUpdateClientTip_UntrackedConnection(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	// Updating an untracked connection should return true
	// (treat as new) and not panic
	isNew := s.UpdateClientTip(newTestConnId(999),
		ocommon.NewPoint(100, []byte("h1")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("h1"))})
	require.True(t, isNew)
}

// --- Concurrent access test ---

func TestConcurrentAccess(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.Config{
		MaxClients:   50,
		StallTimeout: 30 * time.Second,
	})

	var wg sync.WaitGroup
	// Spawn goroutines that add, update, and remove clients
	for i := range 20 {
		wg.Add(1)
		go func(id uint) {
			defer wg.Done()
			conn := newTestConnId(id)
			s.AddClientConnId(conn)
			for j := range 10 {
				s.UpdateClientTip(conn,
					ocommon.NewPoint(
						uint64(j),
						[]byte{byte(id), byte(j)},
					),
					ochainsync.Tip{
						Point: ocommon.NewPoint(
							uint64(j),
							[]byte{byte(id), byte(j)},
						),
					})
			}
			_ = s.GetClientConnId()
			_ = s.GetClientConnIds()
			_ = s.ClientConnCount()
			_ = s.GetTrackedClient(conn)
			_ = s.GetTrackedClients()
			s.CheckStalledClients()
			s.RemoveClientConnId(conn)
		}(uint(i))
	}
	wg.Wait()
}

// --- Remove all clients leaves nil active ---

func TestRemoveAllClients_NilActive(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	s.RemoveClientConnId(connA)
	s.RemoveClientConnId(connB)

	active := s.GetClientConnId()
	require.Nil(t, active)
	require.Equal(t, 0, s.ClientConnCount())
}
