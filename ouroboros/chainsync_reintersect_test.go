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

package ouroboros

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// reintersectFixture runs dingo's resync handler against a real chainsync
// client whose peer streams blocks as fast as it is asked.
type reintersectFixture struct {
	o         *Ouroboros
	cm        *connmanager.ConnectionManager
	state     *dchainsync.State
	connId    ouroboros.ConnectionId
	intersect ocommon.Point
	logger    *slog.Logger

	findIntersects atomic.Int32
	rollForwards   atomic.Int32
	rollBackwards  atomic.Int32
	// await makes the peer answer every later RequestNext with AwaitReply and
	// never send the block, as a peer at its tip does.
	await atomic.Bool
	// refuseIntersect makes the peer answer every later FindIntersect with
	// IntersectNotFound.
	refuseIntersect atomic.Bool
	// awaitReplies counts the AwaitReply answers the peer has sent.
	awaitReplies atomic.Int32

	mu           sync.Mutex
	offered      [][]ocommon.Point
	needRollback bool
	// ledgerEvents records the rollback and session-reopen events reaching
	// the ledger's chainsync stream, in order: "rollback" or "reopened".
	ledgerEvents []string
}

func newReintersectFixture(
	t *testing.T,
	timeout time.Duration,
) *reintersectFixture {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{EventBus: bus, Logger: logger},
	)
	// t.Context is already cancelled when cleanups run.
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = cm.Stop(stopCtx)
	})

	ls, _ := newTestLedgerStateWithChain(t, 3)
	f := &reintersectFixture{
		cm:        cm,
		intersect: ls.PrimaryChainTip().Point,
		logger:    logger,
	}
	f.o = newOuroboros(OuroborosConfig{EventBus: bus, Logger: logger})
	f.o.eventBus = bus
	f.o.connManager = cm
	f.o.ledgerState = ls
	f.o.chainsyncReintersectTimeout = timeout
	bus.SubscribeFunc(ledger.ChainsyncEventType, func(evt event.Event) {
		e, ok := evt.Data.(ledger.ChainsyncEvent)
		if !ok {
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case e.SessionReopened:
			f.ledgerEvents = append(f.ledgerEvents, "reopened")
		case e.Rollback:
			f.ledgerEvents = append(f.ledgerEvents, "rollback")
		}
	})

	client := f.dial(t)
	f.connId = client.Id()
	f.state = dchainsync.NewState(bus, nil)
	require.True(t, f.state.AddClientConnId(f.connId))
	f.o.chainsyncState = f.state
	f.o.SubscribeChainsyncResync(t.Context())
	f.startSync(t, client)
	return f
}

// dial connects a new client to a new peer over a pipe and registers it with
// the connection manager. Every pipe connection has the same ID, so a second
// dial replaces the first connection under its ID.
func (f *reintersectFixture) dial(t *testing.T) *ouroboros.Connection {
	t.Helper()
	tip := f.o.ledgerState.PrimaryChainTip()
	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() { _ = serverPipe.Close(); _ = clientPipe.Close() })
	serverConfig := ochainsync.NewConfig(
		ochainsync.WithFindIntersectFunc(func(
			_ ochainsync.CallbackContext,
			points []ocommon.Point,
		) (ocommon.Point, ochainsync.Tip, error) {
			n := f.findIntersects.Add(1)
			f.mu.Lock()
			f.offered = append(f.offered, points)
			f.needRollback = true
			f.mu.Unlock()
			if n > 1 && f.refuseIntersect.Load() {
				return ocommon.Point{}, tip, ochainsync.ErrIntersectNotFound
			}
			return f.intersect, tip, nil
		}),
		ochainsync.WithRequestNextFunc(func(cb ochainsync.CallbackContext) error {
			if f.await.Load() {
				f.awaitReplies.Add(1)
				return cb.Server.AwaitReply()
			}
			f.mu.Lock()
			rollback := f.needRollback
			f.needRollback = false
			f.mu.Unlock()
			if rollback {
				return cb.Server.RollBackward(f.intersect, tip)
			}
			return cb.Server.RollForward(1, []byte{0x80}, tip)
		}),
	)
	serverReady := make(chan error, 1)
	var serverConn *ouroboros.Connection
	go func() {
		var err error
		serverConn, err = ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithDelayProtocolStart(true),
			ouroboros.WithLogger(f.logger),
			ouroboros.WithChainSyncConfig(serverConfig),
		)
		serverReady <- err
	}()
	clientConfig := ochainsync.NewConfig(
		ochainsync.WithPipelineLimit(4),
		ochainsync.WithIntersectFoundFunc(f.o.chainsyncClientIntersectFound),
		ochainsync.WithRollForwardRawFunc(func(
			ochainsync.CallbackContext,
			uint,
			[]byte,
			ochainsync.Tip,
		) error {
			f.rollForwards.Add(1)
			return nil
		}),
		// Publishes the rollback to the ledger stream as the production
		// callback does, so its order against the reopen marker is visible.
		ochainsync.WithRollBackwardFunc(func(
			cb ochainsync.CallbackContext,
			point ocommon.Point,
			tip ochainsync.Tip,
		) error {
			f.rollBackwards.Add(1)
			return f.o.eventBus.PublishBlocking(
				ledger.ChainsyncEventType,
				event.NewEvent(
					ledger.ChainsyncEventType,
					ledger.ChainsyncEvent{
						ConnectionId: cb.ConnectionId,
						Rollback:     true,
						Point:        point,
						Tip:          tip,
					},
				),
			)
		}),
	)
	client, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithNetworkMagic(42),
		ouroboros.WithDelayProtocolStart(true),
		ouroboros.WithLogger(f.logger),
		ouroboros.WithChainSyncConfig(clientConfig),
	)
	require.NoError(t, err)
	require.NoError(t, testutil.RequireReceive(
		t, serverReady, 5*time.Second, "server handshake",
	))
	t.Cleanup(func() { _ = client.Close(); _ = serverConn.Close() })
	require.True(t, f.cm.AddConnection(client, false, "127.0.0.1:3001"))
	serverConn.ChainSync().Server.Start()
	return client
}

// startSync starts client's sync at the fixture's intersection and waits for
// the peer to stream.
func (f *reintersectFixture) startSync(
	t *testing.T,
	client *ouroboros.Connection,
) {
	t.Helper()
	forwards := f.rollForwards.Load()
	client.ChainSync().Client.Start()
	require.NoError(t, client.ChainSync().Client.Sync(
		[]ocommon.Point{f.intersect},
	))
	testutil.WaitForCondition(
		t,
		func() bool { return f.rollForwards.Load() > forwards },
		5*time.Second,
		"peer never started streaming",
	)
}

func (f *reintersectFixture) publish(reason string) {
	f.o.eventBus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: f.connId,
				Reason:       reason,
			},
		),
	)
}

func (f *reintersectFixture) connected() bool {
	return f.cm.GetConnectionById(f.connId) != nil
}

// A local resync reason re-intersects the live client on its own connection:
// the peer sees one more FindIntersect, answers it, and keeps streaming.
func TestChainsyncResyncReintersectsInPlace(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return f.findIntersects.Load() == 2 },
		5*time.Second,
		"the resync must send one more FindIntersect",
	)
	forwardsAtIntersect := f.rollForwards.Load()
	testutil.WaitForCondition(
		t,
		func() bool {
			return f.rollBackwards.Load() >= 1 &&
				f.rollForwards.Load() > forwardsAtIntersect
		},
		5*time.Second,
		"the re-intersected sync must roll back to the intersection and stream on",
	)
	require.True(t, f.connected(), "an in-place resync must keep the connection")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.offered[1])
	require.Equal(t, f.intersect, f.offered[1][0],
		"the re-intersect must offer the ledger's own points")
}

// Evidence a peer delivered at or below the new intersection survives the
// re-intersect, and anything above it is dropped.
func TestChainsyncResyncReintersectKeepsHistoryBelowIntersection(
	t *testing.T,
) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	below := newTestBlockHeader(f.intersect.Slot-1, 2, 0xa1)
	at := newTestBlockHeader(f.intersect.Slot, 3, 0xa2)
	above := newTestBlockHeader(f.intersect.Slot+5, 4, 0xa3)
	for _, hdr := range []gledger.BlockHeader{below, at, above} {
		f.state.RecordObservedHeader(dchainsync.ObservedHeader{
			ConnectionId: f.connId,
			BlockHeader:  hdr,
			Point: ocommon.NewPoint(
				hdr.SlotNumber(), hdr.Hash().Bytes(),
			),
		})
	}

	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.WaitForCondition(
		t,
		func() bool { return f.findIntersects.Load() == 2 },
		5*time.Second,
		"the resync must re-intersect",
	)
	testutil.WaitForCondition(
		t,
		func() bool {
			_, _, ok := f.state.LookupObservedHeader(
				f.connId, above.Hash().Bytes(),
			)
			return !ok
		},
		5*time.Second,
		"history above the intersection must be dropped",
	)
	for name, hdr := range map[string][]byte{
		"below": below.Hash().Bytes(),
		"at":    at.Hash().Bytes(),
	} {
		_, _, ok := f.state.LookupObservedHeader(f.connId, hdr)
		require.True(t, ok, "history %s the intersection must survive", name)
	}
	require.True(t, f.connected())
}

// A re-intersect that cannot finish inside the watchdog falls back to the
// close: at its tip a peer answers the drained request only with the next
// block.
func TestChainsyncResyncReintersectWatchdogFallsBackToClose(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 300*time.Millisecond)
	f.await.Store(true)
	// A request answered with AwaitReply stays outstanding, so Reintersect
	// cannot drain before the watchdog expires.
	testutil.WaitForCondition(
		t,
		func() bool { return f.awaitReplies.Load() > 0 },
		5*time.Second,
		"the peer never parked the client in MustReply",
	)

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"a re-intersect that never drains must close the connection",
	)
	require.EqualValues(t, 1, f.findIntersects.Load(),
		"no FindIntersect may be sent while a reply is outstanding")
}

// A peer that does not know any offered point is closed rather than left in
// Idle with no sync running.
func TestChainsyncResyncReintersectNotFoundFallsBackToClose(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	f.refuseIntersect.Store(true)

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"IntersectNotFound must close the connection",
	)
	require.EqualValues(t, 2, f.findIntersects.Load())
}

// A resync that blames the peer closes it even though a client is running.
func TestChainsyncResyncPeerFaultReasonStillCloses(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)

	f.publish(event.ChainsyncResyncReasonRollbackExceedsK)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"a peer-fault resync must close the connection",
	)
	require.EqualValues(t, 1, f.findIntersects.Load(),
		"a peer-fault resync must not re-intersect")
}

// The re-intersected sync resumes as soon as Reintersect returns, so the
// history trim and the withheld-header marker must be settled before the first
// reply of the new cursor, not after Reintersect returns: a header of the new
// cursor delivered in between would be withheld or trimmed.
func TestChainsyncResyncReintersectSettlesStateBeforeSyncResumes(
	t *testing.T,
) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	cfg := ochainsync.NewConfig(f.o.chainsyncClientConnOpts()...)
	require.NotNil(t, cfg.IntersectFoundFunc,
		"production clients must settle a re-intersect on IntersectFound")
	above := newTestBlockHeader(f.intersect.Slot+5, 4, 0xa3)
	f.state.RecordObservedHeader(dchainsync.ObservedHeader{
		ConnectionId: f.connId,
		BlockHeader:  above,
		Point: ocommon.NewPoint(
			above.SlotNumber(), above.Hash().Bytes(),
		),
	})
	f.o.futureHeaderResyncMu.Lock()
	f.o.futureHeaderResyncs[f.connId] = &scheduledChainsyncResync{
		fired:  true,
		reason: event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery,
		cancel: func() {},
	}
	f.o.futureHeaderResyncMu.Unlock()
	returned := make(chan struct{})
	release := make(chan struct{})
	f.o.chainsyncReintersectReturnedHook = func() {
		close(returned)
		<-release
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	f.publish(event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery)
	testutil.RequireReceive(
		t, returned, 5*time.Second, "Reintersect must return",
	)
	forwardsAtReturn := f.rollForwards.Load()
	testutil.WaitForCondition(
		t,
		func() bool {
			return f.rollBackwards.Load() >= 1 &&
				f.rollForwards.Load() > forwardsAtReturn
		},
		5*time.Second,
		"the re-intersected sync must stream on",
	)
	require.False(t, f.o.futureHeaderResyncPending(f.connId),
		"the new cursor's headers must not be withheld")
	_, _, ok := f.state.LookupObservedHeader(f.connId, above.Hash().Bytes())
	require.False(t, ok,
		"history above the intersection must be gone before the new cursor")
	close(release)
}

// The watchdog bounds building the intersect points as well as Reintersect: a
// ledger read that never returns still falls back to the close.
func TestChainsyncResyncReintersectWatchdogBoundsPointBuild(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 300*time.Millisecond)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	f.o.config.ChainsyncIntersectAtLedgerTip = func() bool {
		<-block
		return false
	}

	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		10*time.Second,
		"a re-intersect whose points never build must close the connection",
	)
	require.EqualValues(t, 1, f.findIntersects.Load())
}

// A resync that arrives after the running re-intersect found its intersection
// is not absorbed by it: that run has already settled its state, so the later
// resync re-intersects again.
func TestChainsyncResyncAfterIntersectFoundReintersectsAgain(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	var returns atomic.Int32
	first := make(chan struct{})
	release := make(chan struct{})
	f.o.chainsyncReintersectReturnedHook = func() {
		if returns.Add(1) == 1 {
			close(first)
			<-release
		}
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.RequireReceive(
		t, first, 5*time.Second, "the first re-intersect must return",
	)
	f.publish(event.ChainsyncResyncReasonIngressPromotion)

	testutil.WaitForCondition(
		t,
		func() bool { return f.findIntersects.Load() == 3 },
		5*time.Second,
		"a resync after IntersectFound must re-intersect again",
	)
	close(release)
	testutil.WaitForCondition(
		t,
		func() bool { return returns.Load() == 2 },
		5*time.Second,
		"both re-intersects must return",
	)
	require.True(t, f.connected())
}

// A re-intersect still running for a closed connection neither absorbs a
// resync of the connection that replaced it under the same ID nor closes that
// replacement when it fails.
func TestChainsyncResyncReintersectIgnoresReplacedConnection(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	var builds atomic.Int32
	building := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	f.o.config.ChainsyncIntersectAtLedgerTip = func() bool {
		if builds.Add(1) == 1 {
			close(building)
			<-release
		}
		return false
	}
	var returns atomic.Int32
	f.o.chainsyncReintersectReturnedHook = func() { returns.Add(1) }

	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.RequireReceive(
		t, building, 5*time.Second, "the first re-intersect must start",
	)
	old := f.cm.GetConnectionById(f.connId)
	require.NotNil(t, old)
	require.NoError(t, old.Close())
	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		5*time.Second,
		"the old connection must be removed",
	)
	replacement := f.dial(t)
	require.Equal(t, f.connId, replacement.Id())
	f.startSync(t, replacement)
	findsBefore := f.findIntersects.Load()

	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.WaitForCondition(
		t,
		func() bool { return f.findIntersects.Load() == findsBefore+1 },
		5*time.Second,
		"the replacement's resync must re-intersect it",
	)
	close(release)
	testutil.WaitForCondition(
		t,
		func() bool { return returns.Load() == 2 },
		5*time.Second,
		"both re-intersects must return",
	)
	// The close fallback runs after the returned hook.
	testutil.WaitForCondition(
		t,
		func() bool {
			f.o.chainsyncReintersectMu.Lock()
			defer f.o.chainsyncReintersectMu.Unlock()
			return len(f.o.chainsyncReintersecting) == 0
		},
		5*time.Second,
		"both re-intersects must finish",
	)
	require.Same(t, replacement, f.cm.GetConnectionById(f.connId),
		"the stale re-intersect must not close the replacement")
}

// The rollback a peer sends right after a re-intersect is the opening of a new
// session, not the peer leaving a chain it showed us. The ledger learns that
// from a reopen marker ahead of the rollback on the same ordered stream; an
// initial intersect sends none.
func TestChainsyncResyncReintersectReopensLedgerSession(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)

	f.publish(event.ChainsyncResyncReasonBehindLocalTip)

	testutil.WaitForCondition(
		t,
		func() bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return len(f.ledgerEvents) >= 3
		},
		5*time.Second,
		"the re-intersect's rollback must reach the ledger stream",
	)
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []string{"rollback", "reopened", "rollback"},
		f.ledgerEvents,
		"the reopen marker must precede the re-intersect's opening rollback")
	require.True(t, f.connected())
}

// A re-intersect whose connection is torn down under it has nothing left to
// recover: it returns without the close fallback, which would otherwise drop
// the connection's history as a fresh resync does.
func TestChainsyncResyncReintersectOnTornDownConnectionIsNoOp(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	below := newTestBlockHeader(f.intersect.Slot-1, 2, 0xb1)
	f.state.RecordObservedHeader(dchainsync.ObservedHeader{
		ConnectionId: f.connId,
		BlockHeader:  below,
		Point: ocommon.NewPoint(
			below.SlotNumber(), below.Hash().Bytes(),
		),
	})
	building := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	f.o.config.ChainsyncIntersectAtLedgerTip = func() bool {
		close(building)
		<-release
		return false
	}
	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.RequireReceive(
		t, building, 5*time.Second, "the re-intersect must start",
	)
	conn := f.cm.GetConnectionById(f.connId)
	require.NotNil(t, conn)
	require.NoError(t, conn.Close())
	testutil.WaitForCondition(
		t,
		func() bool { return !f.connected() },
		5*time.Second,
		"the connection must be removed",
	)
	close(release)
	testutil.WaitForCondition(
		t,
		func() bool {
			f.o.chainsyncReintersectMu.Lock()
			defer f.o.chainsyncReintersectMu.Unlock()
			return len(f.o.chainsyncReintersecting) == 0
		},
		5*time.Second,
		"the re-intersect must finish",
	)

	_, _, ok := f.state.LookupObservedHeader(f.connId, below.Hash().Bytes())
	require.True(t, ok,
		"a re-intersect on a torn-down connection must not run the close fallback")
}

// Closing a connection used to end everything recorded against its cursor:
// the observer header gap, a parked deferred-header recovery and the accepted
// header tip all went with the connection ID. A re-intersect keeps the ID, so
// it must end them itself. Left behind, the gap re-arms an ingress-promotion
// resync on every later header, and the parked recovery re-intersects the peer
// again on the next applied block for a header the new cursor replays anyway.
func TestChainsyncResyncReintersectEndsOldCursorState(t *testing.T) {
	t.Parallel()
	f := newReintersectFixture(t, 10*time.Second)
	session := f.state.ClientSession(f.connId)
	f.state.RecordClientObserverHeaderGap(f.connId, session)
	require.True(t, f.state.ClientObserverHeaderGap(f.connId))
	stale := newTestBlockHeader(f.intersect.Slot+5, 4, 0xa4)
	f.o.deferredHeaderRecoveryMu.Lock()
	f.o.deferredHeaderRecovery = map[ouroboros.ConnectionId]deferredHeaderRecoveryState{
		f.connId: {
			point: ocommon.NewPoint(
				stale.SlotNumber(), stale.Hash().Bytes(),
			),
			resolvable: true,
		},
	}
	f.o.deferredHeaderRecoveryMu.Unlock()
	f.o.recordAcceptedHeaderTip(f.connId, stale.Hash().Bytes())
	epoch := f.o.chainsyncCursorEpoch(f.connId)
	rollBackwards := f.rollBackwards.Load()

	f.publish(event.ChainsyncResyncReasonIngressPromotion)
	testutil.WaitForCondition(
		t,
		func() bool { return f.rollBackwards.Load() > rollBackwards },
		5*time.Second,
		"the re-intersected sync must roll back to the intersection",
	)

	require.False(t, f.state.ClientObserverHeaderGap(f.connId),
		"a re-intersect replays the headers the observer skipped")
	f.o.deferredHeaderRecoveryMu.Lock()
	_, parked := f.o.deferredHeaderRecovery[f.connId]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.False(t, parked,
		"a recovery parked for the old cursor must not re-intersect again")
	f.o.acceptedHeaderMu.Lock()
	_, accepted := f.o.acceptedHeaderTips[f.connId]
	f.o.acceptedHeaderMu.Unlock()
	require.False(t, accepted,
		"the old cursor's accepted tip must not extend the new cursor")
	require.Greater(t, f.o.chainsyncCursorEpoch(f.connId), epoch,
		"the new cursor must be distinguishable from the old one")
}

// A resync condemns the client's cursor, but the replies to requests already
// pipelined still arrive while the re-intersect drains them. A close used to
// discard them with the connection. Processed instead, each one is verified
// and fails in the ledger the way the header that triggered the resync did,
// under the ledger's chainsync lock, and a rollback among them would be
// applied to the chain before the new session opens.
func TestDrainedRepliesOfReintersectingClientAreDropped(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.rollback(t, ocommon.Point{})
	for {
		select {
		case <-f.ledgerCh:
			continue
		default:
		}
		break
	}
	client := &ochainsync.Client{}
	cb := ochainsync.CallbackContext{ConnectionId: f.conn, Client: client}
	f.o.chainsyncReintersectMu.Lock()
	f.o.chainsyncReintersecting[f.conn] = &chainsyncReintersectRun{
		client: client,
	}
	f.o.chainsyncReintersectMu.Unlock()

	require.NoError(t, f.o.chainsyncClientRollForwardAt(
		cb, 0, newTestBlockHeader(100, 1, 0xaa), ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("advertised")),
			BlockNumber: 2,
		}, time.Now(),
	))
	require.NoError(t, f.o.chainsyncClientRollBackward(
		cb, ocommon.NewPoint(50, []byte("old-cursor")), ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("advertised")),
			BlockNumber: 2,
		},
	))

	require.EqualValues(t, 0, f.verified.Load(),
		"a reply of the condemned cursor must not be verified")
	select {
	case evt := <-f.ledgerCh:
		t.Fatalf("a reply of the condemned cursor reached the ledger: %+v",
			evt.Data)
	default:
	}

	// Once the peer has agreed to the new intersection the client's replies
	// belong to the new cursor.
	f.o.chainsyncReintersectMu.Lock()
	f.o.chainsyncReintersecting[f.conn].settled = true
	f.o.chainsyncReintersectMu.Unlock()
	require.NoError(t, f.o.chainsyncClientRollForwardAt(
		cb, 0, newTestBlockHeader(100, 1, 0xaa), ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("advertised")),
			BlockNumber: 2,
		}, time.Now(),
	))
	evt := testutil.RequireReceive(
		t, f.ledgerCh, 5*time.Second, "the new cursor's header must be ingested",
	)
	require.NotNil(t, evt.Data.(ledger.ChainsyncEvent).BlockHeader)
}
