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
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

func deadForkConnId(remotePort int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: remotePort},
	}
}

// deadForkHeaders builds count contiguous mock headers extending from, one
// block and 10 slots apart, named so two seeds never collide.
func deadForkHeaders(
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

func deadForkEvent(
	connId ouroboros.ConnectionId,
	h mockHeader,
	advertised ochainsync.Tip,
) ChainsyncEvent {
	return ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  h,
		Point:        ocommon.NewPoint(h.slot, h.hash.Bytes()),
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

// declinedMismatchHeader builds a header Praos declines (shorter than the tip)
// that does not extend the tip: it counts one mismatch and, in non-genesis
// mode, resolves to no resync.
func declinedMismatchHeader(seed string, tip ochainsync.Tip) mockHeader {
	return mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes(seed)),
		prevHash:    lcommon.NewBlake2b256(testHashBytes(seed + "-parent")),
		blockNumber: tip.BlockNumber - 1,
		slot:        tip.Point.Slot,
	}
}

// Mismatches are counted per connection, so nineteen mismatches delivered by
// a fork peer must not let one mismatch from an honest peer trip the
// persistent-fork resync against that honest peer.
func TestDeadForkSharedMismatchCounterDoesNotBlameHonestPeer(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	rec := newResyncRecorder(t, bus)

	forkConn := deadForkConnId(3002)
	honestConn := fixture.connId
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(100000, testHashBytes("far-tip")),
		BlockNumber: 5000,
	}
	for i := range headerMismatchResyncThreshold - 1 {
		h := declinedMismatchHeader(fmt.Sprintf("fork-%d", i), fixture.currentTip)
		require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
			deadForkEvent(forkConn, h, advertised)))
	}
	require.Equal(t, headerMismatchResyncThreshold-1,
		fixture.ls.headerMismatchCounts[connIdKey(forkConn)],
		"fork peer's mismatches must accumulate for the scenario to hold")

	h := declinedMismatchHeader("honest-0", fixture.currentTip)
	require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
		deadForkEvent(honestConn, h, advertised)))

	for _, e := range rec.drain(t) {
		require.Falsef(t, sameConnectionId(e.ConnectionId, honestConn),
			"honest peer received %q resync caused by fork peer's mismatches",
			e.Reason)
	}
}

// A connection's own mismatches must still reach the persistent-fork resync.
func TestPersistentForkResyncsConnectionOwningMismatches(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	rec := newResyncRecorder(t, bus)

	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(100000, testHashBytes("far-tip")),
		BlockNumber: 5000,
	}
	for i := range headerMismatchResyncThreshold {
		h := declinedMismatchHeader(fmt.Sprintf("own-%d", i), fixture.currentTip)
		require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
			deadForkEvent(fixture.connId, h, advertised)))
	}

	var persistent int
	for _, e := range rec.drain(t) {
		if e.Reason == event.ChainsyncResyncReasonPersistentFork &&
			sameConnectionId(e.ConnectionId, fixture.connId) {
			persistent++
		}
	}
	require.Equal(t, 1, persistent)
}

// Two fork peers whose mismatches interleave must each accumulate their own
// count; neither may reset the other's.
func TestPersistentForkResyncsInterleavedForkPeers(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	rec := newResyncRecorder(t, bus)

	forkA := deadForkConnId(3002)
	forkB := deadForkConnId(3003)
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(100000, testHashBytes("far-tip")),
		BlockNumber: 5000,
	}
	for i := range headerMismatchResyncThreshold {
		for name, connId := range map[string]ouroboros.ConnectionId{
			"a": forkA,
			"b": forkB,
		} {
			h := declinedMismatchHeader(
				fmt.Sprintf("%s-%d", name, i), fixture.currentTip)
			require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
				deadForkEvent(connId, h, advertised)))
		}
	}

	resynced := map[string]int{}
	for _, e := range rec.drain(t) {
		if e.Reason == event.ChainsyncResyncReasonPersistentFork {
			resynced[connIdKey(e.ConnectionId)]++
		}
	}
	require.Positive(t, resynced[connIdKey(forkA)], "fork peer A not resynced")
	require.Positive(t, resynced[connIdKey(forkB)], "fork peer B not resynced")
}

// A failed fork resolution undoes only its own increment, so it must not
// erase another connection's accumulated count.
func TestResolveErrorKeepsOtherConnectionMismatchCount(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	forkConn := deadForkConnId(3002)
	otherConn := deadForkConnId(3003)
	fixture.ls.headerMismatchCounts = map[string]int{
		connIdKey(otherConn): 7,
	}
	ancestorLookupErr := errors.New("ancestor lookup failed")
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, ancestorLookupErr
	}

	header := mockHeader{
		hash: lcommon.NewBlake2b256(testHashBytes("resolve-err-fork")),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("resolve-err-ancestor"),
		),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	err := fixture.ls.handleEventChainsyncBlockHeader(deadForkEvent(
		forkConn,
		header,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(header.slot, header.hash.Bytes()),
			BlockNumber: header.blockNumber,
		},
	))
	require.ErrorIs(t, err, ancestorLookupErr)
	require.Equal(t, 7, fixture.ls.headerMismatchCounts[connIdKey(otherConn)])
	require.Zero(t, fixture.ls.headerMismatchCounts[connIdKey(forkConn)])
}

// A closed connection's mismatch count is dropped with the rest of its
// per-connection state, so the map does not outlive the connections in it.
func TestConnectionClosedDropsOnlyItsMismatchCount(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	closedConn := deadForkConnId(3002)
	otherConn := deadForkConnId(3003)
	fixture.ls.headerMismatchCounts = map[string]int{
		connIdKey(closedConn): 5,
		connIdKey(otherConn):  3,
	}

	fixture.ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: closedConn},
	))

	require.NotContains(t, fixture.ls.headerMismatchCounts, connIdKey(closedConn))
	require.Equal(t, 3, fixture.ls.headerMismatchCounts[connIdKey(otherConn)])
}

// The blockfetch continuation and batch-done handler request a resync while
// holding only chainsyncBlockfetchMutex, so the count map must not rely on
// chainsyncMutex, which the header handler holds while it counts.
func TestHeaderMismatchCountsSafeAgainstBlockfetchResync(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	forkConn := deadForkConnId(3002)
	blockfetchConn := deadForkConnId(3003)
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(100000, testHashBytes("far-tip")),
		BlockNumber: 5000,
	}
	const rounds = 200
	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	// A failed require exits the test goroutine through Goexit, so the worker
	// is joined from a cleanup that runs before the fixture closes the ledger.
	stopWorker := func() {
		stopOnce.Do(func() { close(stop) })
		<-done
	}
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			var pending pendingPublishes
			fixture.ls.chainsyncBlockfetchMutex.Lock()
			fixture.ls.requestChainsyncResync(
				blockfetchConn, "blockfetch continuation", &pending)
			fixture.ls.chainsyncBlockfetchMutex.Unlock()
			pending.flush()
		}
	}()
	t.Cleanup(stopWorker)
	for i := range rounds {
		h := declinedMismatchHeader(fmt.Sprintf("race-%d", i), fixture.currentTip)
		fixture.ls.chainsyncMutex.Lock()
		err := fixture.ls.handleEventChainsyncBlockHeader(
			deadForkEvent(forkConn, h, advertised))
		fixture.ls.chainsyncMutex.Unlock()
		require.NoError(t, err)
	}
	stopWorker()
}

// An active-connection switch says nothing about another connection's
// consecutive mismatches, so it must not reset them.
func TestConnectionSwitchKeepsOtherConnectionMismatchCount(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	previous := fixture.connId
	next := deadForkConnId(3003)
	forkConn := deadForkConnId(3002)
	fixture.ls.lastActiveConnId = &previous
	fixture.ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &next
	}
	fixture.ls.headerMismatchCounts = map[string]int{
		connIdKey(forkConn): headerMismatchResyncThreshold - 1,
	}

	fixture.ls.chainsyncMutex.Lock()
	active, configured := fixture.ls.detectConnectionSwitch(nil)
	fixture.ls.chainsyncMutex.Unlock()

	require.True(t, configured)
	require.NotNil(t, active)
	require.True(t, sameConnectionId(*active, next))
	require.Equal(t, headerMismatchResyncThreshold-1,
		fixture.ls.headerMismatchCounts[connIdKey(forkConn)])
}

// An over-K fork resolution resyncs only the connection that delivered the
// header, so only that connection's count is reset.
func TestOverKForkResyncKeepsOtherConnectionMismatchCount(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "over-k-per-conn")
	bus := newTestEventBus(t, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	otherConn := deadForkConnId(3003)
	fixture.ls.headerMismatchCounts = map[string]int{
		connIdKey(fixture.connId): 3,
		connIdKey(otherConn):      7,
	}

	localTip := fixture.ls.chain.Tip()
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("over-k-per-conn-block")),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: localTip.BlockNumber + 1,
		slot:        localTip.Point.Slot + 10,
	}
	err := fixture.ls.chain.AddBlockHeader(context.Background(), header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        point,
			BlockHeader:  header,
			Tip:          ochainsync.Tip{Point: point, BlockNumber: header.BlockNumber()},
		},
		notFitErr,
		nil,
		0,
	)
	require.NoError(t, err)
	require.True(t, resolved)
	require.NotContains(t, fixture.ls.headerMismatchCounts, connIdKey(fixture.connId))
	require.Equal(t, 7, fixture.ls.headerMismatchCounts[connIdKey(otherConn)])
}
