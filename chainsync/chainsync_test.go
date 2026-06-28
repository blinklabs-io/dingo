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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type testBlockHeader struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	blockNumber uint64
	slot        uint64
}

func (h testBlockHeader) Hash() lcommon.Blake2b256          { return h.hash }
func (h testBlockHeader) PrevHash() lcommon.Blake2b256      { return h.prevHash }
func (h testBlockHeader) BlockNumber() uint64               { return h.blockNumber }
func (h testBlockHeader) SlotNumber() uint64                { return h.slot }
func (h testBlockHeader) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (h testBlockHeader) BlockBodySize() uint64             { return 0 }
func (h testBlockHeader) Era() lcommon.Era                  { return babbage.EraBabbage }
func (h testBlockHeader) Cbor() []byte                      { return nil }
func (h testBlockHeader) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

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

func TestTryAddObservedClientConnId_DoesNotConsumeEligibleLimit(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.Config{
		MaxClients:   1,
		StallTimeout: 30 * time.Second,
	})

	connObserved := newTestConnId(1)
	connEligible := newTestConnId(2)

	require.True(t, s.TryAddObservedClientConnId(connObserved))
	require.True(t, s.TryAddClientConnId(connEligible, 1))
	require.True(t, s.HasClientConnId(connObserved))
	require.True(t, s.HasClientConnId(connEligible))
	require.Equal(t, 1, s.ClientConnCount())

	observabilityOnly, exists := s.ClientObservabilityOnly(connObserved)
	require.True(t, exists)
	require.True(t, observabilityOnly)

	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connEligible, *active)
}

func TestSetClientObservabilityOnly_DemotesPrimary(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)
	s.SetClientConnId(connA)
	s.UpdateClientTip(connA,
		ocommon.NewPoint(500, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("ha"))},
	)
	s.UpdateClientTip(connB,
		ocommon.NewPoint(400, []byte("hb")),
		ochainsync.Tip{Point: ocommon.NewPoint(400, []byte("hb"))},
	)

	require.True(t, s.SetClientObservabilityOnly(connA, true))
	require.Equal(t, 1, s.ClientConnCount())

	observabilityOnly, exists := s.ClientObservabilityOnly(connA)
	require.True(t, exists)
	require.True(t, observabilityOnly)

	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)
}

func TestSetClientObservabilityOnly_EligiblePromotionRespectsMaxClients(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.Config{
		MaxClients:   1,
		StallTimeout: 30 * time.Second,
	})

	connObserved := newTestConnId(1)
	connEligible := newTestConnId(2)
	require.True(t, s.TryAddObservedClientConnId(connObserved))
	require.True(t, s.TryAddClientConnId(connEligible, 1))

	require.False(t, s.SetClientObservabilityOnly(connObserved, false))

	observabilityOnly, exists := s.ClientObservabilityOnly(connObserved)
	require.True(t, exists)
	require.True(t, observabilityOnly)
	require.Equal(t, 1, s.ClientConnCount())
}

func TestSetClientObservabilityOnly_PreservesCurrentActiveClient(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	require.True(t, s.TryAddObservedClientConnId(connB))
	s.SetClientConnId(connA)
	s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("ha"))},
	)
	s.UpdateClientTipWithoutDedup(connB,
		ocommon.NewPoint(500, []byte("hb")),
		ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("hb"))},
	)

	require.True(t, s.SetClientObservabilityOnly(connB, false))

	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connA, *active)
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
	// the most recently active stalled client is promoted
	// as a fallback to prevent permanent nil-selection deadlock.
	s.RemoveClientConnId(connA)
	active := s.GetClientConnId()
	require.NotNil(
		t, active,
		"should promote stalled client as fallback when no healthy clients exist",
	)
	require.Equal(t, connB, *active,
		"should promote the remaining stalled client",
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

func TestObservedHeaderHistoryPersistsAcrossDedupAndClearsOnRemove(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	s.AddClientConnId(conn)

	hash := []byte("hash-1")
	prevHash := []byte("prev-hash-1")
	point := ocommon.NewPoint(100, hash)
	tip := ochainsync.Tip{Point: point, BlockNumber: 10}
	header := testBlockHeader{
		hash:        lcommon.NewBlake2b256(hash),
		prevHash:    lcommon.NewBlake2b256(prevHash),
		blockNumber: tip.BlockNumber,
		slot:        point.Slot,
	}

	s.RecordObservedHeader(chainsync.ObservedHeader{
		ConnectionId: conn,
		Point:        point,
		BlockHeader:  header,
		Tip:          tip,
	})

	recordedHeader, recordedPrevHash, ok := s.LookupObservedHeader(
		conn,
		hash,
	)
	require.True(t, ok)
	require.Equal(t, point, recordedHeader.Point)
	require.Equal(t, header.prevHash.Bytes(), recordedPrevHash)

	s.RemoveClientConnId(conn)
	_, _, ok = s.LookupObservedHeader(conn, hash)
	require.False(t, ok)
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

func TestStallDetection_SkipsObservabilityOnlyClient(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.Config{
		MaxClients:   1,
		StallTimeout: 50 * time.Millisecond,
	}
	s := newTestState(t, bus, cfg)

	conn := newTestConnId(1)
	require.True(t, s.TryAddObservedClientConnId(conn))
	s.UpdateClientTipWithoutDedup(conn,
		ocommon.NewPoint(100, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("ha"))},
	)

	require.Eventually(t, func() bool {
		return len(s.CheckStalledClients()) == 0
	}, 250*time.Millisecond, 10*time.Millisecond)

	tc := s.GetTrackedClient(conn)
	require.NotNil(t, tc)
	require.Equal(t, chainsync.ClientStatusSyncing, tc.Status)
	require.True(t, tc.ObservabilityOnly)
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

func TestClearSeenHeadersFrom(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	s.UpdateClientTip(connA,
		ocommon.NewPoint(50, []byte("old")),
		ochainsync.Tip{Point: ocommon.NewPoint(50, []byte("old"))})
	s.UpdateClientTip(connA,
		ocommon.NewPoint(100, []byte("keep")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("keep"))})
	s.UpdateClientTip(connA,
		ocommon.NewPoint(150, []byte("replay")),
		ochainsync.Tip{Point: ocommon.NewPoint(150, []byte("replay"))})

	s.ClearSeenHeadersFrom(100)

	isNew := s.UpdateClientTip(connB,
		ocommon.NewPoint(50, []byte("old")),
		ochainsync.Tip{Point: ocommon.NewPoint(50, []byte("old"))})
	require.False(t, isNew)

	isNew = s.UpdateClientTip(connB,
		ocommon.NewPoint(100, []byte("keep")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("keep"))})
	require.False(t, isNew)

	isNew = s.UpdateClientTip(connB,
		ocommon.NewPoint(150, []byte("replay")),
		ochainsync.Tip{Point: ocommon.NewPoint(150, []byte("replay"))})
	require.True(t, isNew)
}

// --- Automatic pruning on the progress path ---

// feedHeader records a header for conn at the given slot and returns
// whether it was treated as new (not a duplicate).
func feedHeader(
	s *chainsync.State,
	conn ouroboros.ConnectionId,
	slot uint64,
	hash string,
) bool {
	p := ocommon.NewPoint(slot, []byte(hash))
	return s.UpdateClientTip(conn, p, ochainsync.Tip{Point: p})
}

// TestSeenHeadersPrunedOnProgress proves that ordinary ChainSync progress
// (UpdateClientTip) prunes header observations that fall outside the
// retention window, while observations within the window remain available
// for duplicate detection.
func TestSeenHeadersPrunedOnProgress(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.SeenHeadersRetention = 500
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Observe an old header, then advance the frontier far enough that a
	// prune scan runs (>= seenHeadersPruneInterval of progress) and the old
	// slot falls outside the 500-slot retention window.
	require.True(t, feedHeader(s, connA, 100, "old"))
	require.True(t, feedHeader(s, connA, 1600, "recent"))

	// The stale observation was pruned; only the recent slot remains.
	require.Equal(t, 1, s.SeenHeadersLen())

	// The pruned slot is treated as new again when re-offered...
	require.True(t, feedHeader(s, connB, 100, "old"))
	// ...while the recent observation is still detected as a duplicate,
	// confirming pruning does not weaken near-tip dedup.
	require.False(t, feedHeader(s, connB, 1600, "recent"))
}

// TestSeenHeadersForkDetectionSurvivesPruning confirms that duplicate and
// fork detection on recent slots keep working once auto-pruning is active.
func TestSeenHeadersForkDetectionSurvivesPruning(t *testing.T) {
	bus := newTestEventBus(t)
	_, ch := bus.Subscribe(chainsync.ForkDetectedEventType)
	cfg := chainsync.DefaultConfig()
	cfg.SeenHeadersRetention = 500
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	s.AddClientConnId(connA)
	s.AddClientConnId(connB)

	// Drive the frontier past the prune interval so pruning is active.
	require.True(t, feedHeader(s, connA, 100, "stale"))
	require.True(t, feedHeader(s, connA, 2000, "tip_a"))

	// A competing hash at the recent slot is still flagged as a fork.
	require.True(t, feedHeader(s, connB, 2000, "tip_b"))
	evt := testutil.RequireReceive(
		t, ch, 2*time.Second, "expected fork detection event",
	)
	forkEvt, ok := evt.Data.(chainsync.ForkDetectedEvent)
	require.True(t, ok)
	require.Equal(t, uint64(2000), forkEvt.Slot)
}

// TestSeenHeadersBoundedOverLongRun proves the dedup cache stays bounded by
// the retention window over a long run instead of growing with every slot
// observed (the unbounded-growth leak this addresses).
func TestSeenHeadersBoundedOverLongRun(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.SeenHeadersRetention = 2000
	s := newTestState(t, bus, cfg)

	conn := newTestConnId(1)
	s.AddClientConnId(conn)

	const step = 50
	for slot := uint64(0); slot <= 100_000; slot += step {
		feedHeader(s, conn, slot, fmt.Sprintf("h-%d", slot))
	}

	// Retained slots stay near retention + one prune interval rather than
	// scaling with the 100k slots observed. The interval mirrors the
	// package-internal seenHeadersPruneInterval (1000 slots).
	const pruneInterval = 1000
	maxSlots := int((cfg.SeenHeadersRetention+pruneInterval)/step) + 2
	require.LessOrEqual(t, s.SeenHeadersLen(), maxSlots)
	require.Positive(t, s.SeenHeadersLen())
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

func TestRewindTrackedClientsToRewindsAheadClients(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connAhead := newTestConnId(1)
	connSame := newTestConnId(2)
	connBehind := newTestConnId(3)
	rollbackPoint := ocommon.NewPoint(100, []byte("rollback"))
	aheadPoint := ocommon.NewPoint(120, []byte("ahead"))
	samePoint := ocommon.NewPoint(100, []byte("same"))
	behindPoint := ocommon.NewPoint(90, []byte("behind"))

	s.AddClientConnId(connAhead)
	s.AddClientConnId(connSame)
	s.AddClientConnId(connBehind)
	s.UpdateClientTip(
		connAhead,
		aheadPoint,
		ochainsync.Tip{Point: aheadPoint},
	)
	s.UpdateClientTip(
		connSame,
		samePoint,
		ochainsync.Tip{Point: samePoint},
	)
	s.UpdateClientTip(
		connBehind,
		behindPoint,
		ochainsync.Tip{Point: behindPoint},
	)
	s.MarkClientSynced(connAhead)

	rewound := s.RewindTrackedClientsTo(rollbackPoint)

	// A chainsync cursor must match the local rollback point exactly. A
	// peer sitting at the same slot on a different hash still needs a
	// fresh intersect before it can supply headers that fit the current
	// selected chain.
	require.ElementsMatch(
		t,
		[]ouroboros.ConnectionId{connAhead, connSame},
		rewound,
	)

	aheadClient := s.GetTrackedClient(connAhead)
	require.NotNil(t, aheadClient)
	require.Equal(t, rollbackPoint, aheadClient.Cursor)
	require.Equal(t, chainsync.ClientStatusSyncing, aheadClient.Status)

	sameClient := s.GetTrackedClient(connSame)
	require.NotNil(t, sameClient)
	require.Equal(t, rollbackPoint, sameClient.Cursor)
	require.Equal(t, chainsync.ClientStatusSyncing, sameClient.Status)

	behindClient := s.GetTrackedClient(connBehind)
	require.NotNil(t, behindClient)
	require.Equal(t, behindPoint, behindClient.Cursor)
}

// --- StartedAsOutbound direction flag tests ---

func TestTryAddClientConnIdWithDirection_RecordsOutboundFlag(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	connOutbound := newTestConnId(1)
	connInbound := newTestConnId(2)

	// Add an outbound client
	require.True(t, s.TryAddClientConnIdWithDirection(connOutbound, 10, true))
	// Add an inbound (non-outbound) client
	require.True(t, s.TryAddClientConnIdWithDirection(connInbound, 10, false))

	// Verify outbound flag is recorded
	outbound, exists := s.ClientStartedAsOutbound(connOutbound)
	require.True(t, exists)
	require.True(t, outbound, "outbound client should have StartedAsOutbound=true")

	inbound, exists := s.ClientStartedAsOutbound(connInbound)
	require.True(t, exists)
	require.False(t, inbound, "inbound client should have StartedAsOutbound=false")

	// Verify non-existent client returns false, false
	_, exists = s.ClientStartedAsOutbound(newTestConnId(99))
	require.False(t, exists)
}

func TestTryAddClientConnId_DefaultsOutboundFalse(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	require.True(t, s.TryAddClientConnId(conn, 10))

	outbound, exists := s.ClientStartedAsOutbound(conn)
	require.True(t, exists)
	require.False(t, outbound, "TryAddClientConnId should default to StartedAsOutbound=false")
}

func TestTryAddObservedClientConnId_DefaultsOutboundFalse(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	require.True(t, s.TryAddObservedClientConnId(conn))

	outbound, exists := s.ClientStartedAsOutbound(conn)
	require.True(t, exists)
	require.False(t, outbound, "TryAddObservedClientConnId should default to StartedAsOutbound=false")
}

func TestTryAddObservedClientConnIdWithDirection_RecordsOutboundFlag(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	require.True(t, s.TryAddObservedClientConnIdWithDirection(conn, true))

	outbound, exists := s.ClientStartedAsOutbound(conn)
	require.True(t, exists)
	require.True(t, outbound)

	require.True(t, s.SetClientStartedAsOutbound(conn, false))
	outbound, exists = s.ClientStartedAsOutbound(conn)
	require.True(t, exists)
	require.False(t, outbound)
}

func TestTryAddClientConnIdWithDirection_DuplicateRejected(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(1)
	require.True(t, s.TryAddClientConnIdWithDirection(conn, 10, true))
	require.False(t, s.TryAddClientConnIdWithDirection(conn, 10, true))
	require.Equal(t, 1, s.ClientConnCount())
}

func TestTryAddClientConnIdWithDirection_LimitEnforced(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.Config{
		MaxClients:   1,
		StallTimeout: 30 * time.Second,
	})

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.TryAddClientConnIdWithDirection(connA, 1, true))
	require.False(t, s.TryAddClientConnIdWithDirection(connB, 1, true))
	require.Equal(t, 1, s.ClientConnCount())
}

// --- ChainProvider interface and ObservedHeader record tests ---

// mockChainProvider is a test double for the ChainProvider interface.
type mockChainProvider struct {
	iter         *chain.ChainIterator
	iterErr      error
	stabilityWin uint64
}

func (m *mockChainProvider) GetChainFromPoint(
	_ ocommon.Point,
	_ bool,
) (*chain.ChainIterator, error) {
	return m.iter, m.iterErr
}

func (m *mockChainProvider) StabilityWindow() uint64 {
	return m.stabilityWin
}

// TestAddClient_NilChainProvider_ReturnsError verifies that AddClient returns
// an error rather than panicking when no ChainProvider has been wired.
func TestAddClient_NilChainProvider_ReturnsError(t *testing.T) {
	s := chainsync.NewStateWithConfig(nil, nil, chainsync.DefaultConfig())
	conn := newTestConnId(1)
	_, err := s.AddClient(conn, ocommon.Point{})
	require.Error(t, err)
}

// TestAddClient_ChainProviderErrorPropagated verifies that an error from
// ChainProvider.GetChainFromPoint is returned by AddClient unchanged.
func TestAddClient_ChainProviderErrorPropagated(t *testing.T) {
	provider := &mockChainProvider{iterErr: fmt.Errorf("chain lookup failed")}
	s := chainsync.NewStateWithConfig(nil, provider, chainsync.DefaultConfig())
	conn := newTestConnId(1)
	_, err := s.AddClient(conn, ocommon.Point{})
	require.ErrorContains(t, err, "chain lookup failed")
}

// TestSeenHeadersRetention_DerivedFromChainProvider verifies that the
// stability window reported by ChainProvider drives the header dedup cache
// pruning boundary when no explicit SeenHeadersRetention override is set.
func TestSeenHeadersRetention_DerivedFromChainProvider(t *testing.T) {
	const providerWindow = 500
	provider := &mockChainProvider{stabilityWin: providerWindow}
	cfg := chainsync.DefaultConfig() // SeenHeadersRetention == 0, falls back to provider
	s := chainsync.NewStateWithConfig(nil, provider, cfg)

	conn := newTestConnId(1)
	s.AddClientConnId(conn)

	// Seed an old header then advance the frontier past the prune interval
	// (seenHeadersPruneInterval = 1000) so a prune scan fires.
	// slot 100 is older than frontier(1600) − window(500) = 1100, so it
	// should be evicted.
	require.True(t, feedHeader(s, conn, 100, "old"))
	require.True(t, feedHeader(s, conn, 1600, "new"))

	require.Equal(t, 1, s.SeenHeadersLen(),
		"slot 100 should be pruned by the provider stability window")
}

// TestObservedHeader_RoundTrip verifies that an ObservedHeader recorded via
// RecordObservedHeader is returned intact by LookupObservedHeader, with the
// prev-hash extracted from BlockHeader.PrevHash().
func TestObservedHeader_RoundTrip(t *testing.T) {
	bus := newTestEventBus(t)
	s := newTestState(t, bus, chainsync.DefaultConfig())

	conn := newTestConnId(42)
	s.AddClientConnId(conn)

	hash := []byte("block-hash")
	prev := []byte("prev-hash")
	point := ocommon.NewPoint(200, hash)
	tip := ochainsync.Tip{Point: point, BlockNumber: 5}
	hdr := testBlockHeader{
		hash:        lcommon.NewBlake2b256(hash),
		prevHash:    lcommon.NewBlake2b256(prev),
		blockNumber: 5,
		slot:        200,
	}

	s.RecordObservedHeader(chainsync.ObservedHeader{
		ConnectionId: conn,
		BlockHeader:  hdr,
		Point:        point,
		Tip:          tip,
		BlockNumber:  5,
		Type:         1,
	})

	got, gotPrev, ok := s.LookupObservedHeader(conn, hash)
	require.True(t, ok)
	require.Equal(t, point, got.Point)
	require.Equal(t, tip, got.Tip)
	require.Equal(t, uint64(5), got.BlockNumber)
	require.Equal(t, uint(1), got.Type)
	require.Equal(t, hdr.PrevHash().Bytes(), gotPrev)

	// LookupObservedHeader must not be visible from a different connection.
	_, _, ok = s.LookupObservedHeader(newTestConnId(99), hash)
	require.False(t, ok)
}

// --- Blockfetch latency metric tests ---

const blockfetchLatencyMetricName = "dingo_chainsync_blockfetch_latency_seconds"

func TestBlockfetchLatencyMetricExposedPerConnection(t *testing.T) {
	bus := newTestEventBus(t)
	reg := prometheus.NewRegistry()
	cfg := chainsync.DefaultConfig()
	cfg.PromRegistry = reg
	s := newTestState(t, bus, cfg)

	conn := newTestConnId(42)
	require.True(t, s.AddClientConnId(conn))

	// First sample sets the EWMA directly to the observed latency.
	s.RecordBlockfetchLatency(conn, 200*time.Millisecond)

	got, ok := gaugeValueForLabels(
		t,
		reg,
		blockfetchLatencyMetricName,
		map[string]string{
			"peer":          conn.RemoteAddr.String(),
			"connection_id": conn.String(),
		},
	)
	require.True(t, ok, "expected a per-connection gauge series after recording")
	require.InDelta(t, 0.2, got, 1e-9)
}

func TestBlockfetchLatencyMetricRemovedOnDisconnect(t *testing.T) {
	bus := newTestEventBus(t)
	reg := prometheus.NewRegistry()
	cfg := chainsync.DefaultConfig()
	cfg.PromRegistry = reg
	s := newTestState(t, bus, cfg)

	conn := newTestConnId(42)
	require.True(t, s.AddClientConnId(conn))
	s.RecordBlockfetchLatency(conn, 200*time.Millisecond)

	labels := map[string]string{
		"peer":          conn.RemoteAddr.String(),
		"connection_id": conn.String(),
	}
	_, ok := gaugeValueForLabels(
		t, reg, blockfetchLatencyMetricName, labels,
	)
	require.True(t, ok, "precondition: gauge series should exist before removal")

	// Removing the connection must delete the per-connection series so
	// cardinality stays bounded by live tracked connections.
	s.RemoveClientConnId(conn)

	_, ok = gaugeValueForLabels(
		t, reg, blockfetchLatencyMetricName, labels,
	)
	require.False(t, ok, "expected per-connection series to be removed on disconnect")
}

func TestBlockfetchLatencyMetricDisconnectPreservesSamePeerConnection(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	reg := prometheus.NewRegistry()
	cfg := chainsync.DefaultConfig()
	cfg.PromRegistry = reg
	s := newTestState(t, bus, cfg)

	remoteA := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	remoteB := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	connA := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4001},
		RemoteAddr: remoteA,
	}
	connB := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4002},
		RemoteAddr: remoteB,
	}
	require.Equal(t, connA.RemoteAddr.String(), connB.RemoteAddr.String())
	require.NotEqual(t, connA.String(), connB.String())
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))

	s.RecordBlockfetchLatency(connA, 200*time.Millisecond)
	s.RecordBlockfetchLatency(connB, 500*time.Millisecond)

	labelsA := map[string]string{
		"peer":          connA.RemoteAddr.String(),
		"connection_id": connA.String(),
	}
	labelsB := map[string]string{
		"peer":          connB.RemoteAddr.String(),
		"connection_id": connB.String(),
	}
	gotA, ok := gaugeValueForLabels(
		t, reg, blockfetchLatencyMetricName, labelsA,
	)
	require.True(t, ok, "precondition: first connection series should exist")
	require.InDelta(t, 0.2, gotA, 1e-9)
	gotB, ok := gaugeValueForLabels(
		t, reg, blockfetchLatencyMetricName, labelsB,
	)
	require.True(t, ok, "precondition: second connection series should exist")
	require.InDelta(t, 0.5, gotB, 1e-9)

	s.RemoveClientConnId(connA)

	_, ok = gaugeValueForLabels(
		t, reg, blockfetchLatencyMetricName, labelsA,
	)
	require.False(t, ok, "removed connection series should be deleted")
	gotB, ok = gaugeValueForLabels(
		t, reg, blockfetchLatencyMetricName, labelsB,
	)
	require.True(t, ok, "same-peer live connection series should remain")
	require.InDelta(t, 0.5, gotB, 1e-9)
}

// gaugeValueForLabels gathers from reg and returns the gauge value for the
// metric family `name` whose labels include all entries in `labels`, plus
// whether such a series exists.
func gaugeValueForLabels(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
	labels map[string]string,
) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matchedLabels := 0
			for _, lp := range m.GetLabel() {
				if expectedValue, ok := labels[lp.GetName()]; ok &&
					expectedValue == lp.GetValue() {
					matchedLabels++
				}
			}
			if matchedLabels == len(labels) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}
