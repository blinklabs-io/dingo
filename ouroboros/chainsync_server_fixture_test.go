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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/require"
)

// The tests in this file drive Dingo's real ChainSync server callbacks over a
// real protocol connection using the shared ouroboros-mock harness
// (blinklabs-io/ouroboros-mock#226), and assert the exact protocol messages the
// server emits back.
//
// This is the difference that matters versus calling the callbacks directly:
// a callback that consumes an iterator result but never sends the matching
// RollForward/RollBackward still satisfies a "did the iterator advance?"
// assertion, but fails here, because here the test only sees what actually
// went onto the wire.
//
// # Asynchronous paths
//
// Immediately after sending AwaitReply, chainsyncServerRequestNext resolves the
// peer with o.connManager.GetConnectionById(ctx.ConnectionId) so it can abandon
// the blocked iterator read if the peer goes away. The fixture therefore
// registers the harness's connection (ouroboros-mock v0.16.0's
// Harness.ServerConnection) with Dingo's ConnManager before driving anything.
// Without that registration the lookup fails and the callback tears the
// connection down instead of serving, which is why these paths previously
// needed a local harness.
//
// The only callbacks still invoked directly are the ones whose assertion *is*
// the returned error: the protocol layer converts a callback error into
// connection teardown rather than an observable message, so there is nothing
// on the wire to assert. Those tests still run against the fixture's real
// connection and server.

// chainsyncServerFixture pairs a Dingo Ouroboros instance with a shared
// ouroboros-mock ChainSync harness driving its server callbacks.
type chainsyncServerFixture struct {
	o *Ouroboros
	h *csmock.Harness

	// conn is the harness's server-under-test connection, registered with
	// Dingo's ConnManager so callbacks that resolve their peer through it
	// (the post-AwaitReply async path) can run.
	conn *ouroboros.Connection

	// closedCh receives connmanager connection-closed events, so tests can
	// assert that an async send failure reached normal lifecycle handling.
	closedCh <-chan event.Event

	// connIdMu guards connId, which the server callbacks record from the
	// protocol goroutine while tests read it.
	connIdMu sync.Mutex
	connId   *ouroboros.ConnectionId
}

// callbackContext builds the callback context the protocol would deliver, for
// the few tests that must call a server callback directly because what they
// assert is the error it returns — something the protocol layer converts into
// connection teardown rather than an observable message.
func (f *chainsyncServerFixture) callbackContext() ochainsync.CallbackContext {
	return ochainsync.CallbackContext{
		ConnectionId: f.conn.Id(),
		Server:       f.h.Server(),
	}
}

// registerClientAtOrigin registers a downstream client that has already had
// its initial rollback, so the next RequestNext consults the iterator.
func (f *chainsyncServerFixture) registerClientAtOrigin(
	t *testing.T,
) *dchainsync.ChainsyncClientState {
	t.Helper()
	clientState, err := f.o.chainsyncState.AddClient(
		f.conn.Id(),
		ocommon.NewPointOrigin(),
	)
	require.NoError(t, err)
	clientState.NeedsInitialRollback = false
	return clientState
}

// recordConnId notes the connection ID the harness assigned, so tests can look
// up the server-side client state the callbacks registered for it.
func (f *chainsyncServerFixture) recordConnId(connId ouroboros.ConnectionId) {
	f.connIdMu.Lock()
	defer f.connIdMu.Unlock()
	f.connId = &connId
}

// observedConnId returns the recorded connection ID, or false if no server
// callback has run yet.
func (f *chainsyncServerFixture) observedConnId() (ouroboros.ConnectionId, bool) {
	f.connIdMu.Lock()
	defer f.connIdMu.Unlock()
	if f.connId == nil {
		return ouroboros.ConnectionId{}, false
	}
	return *f.connId, true
}

// newChainsyncServerFixture wires Dingo's chainsync server config into the
// shared harness. The config is built with the same chainsyncServerConnOpts
// helper production uses, so the instrumentation wrappers are exercised rather
// than bypassed.
func newChainsyncServerFixture(
	t *testing.T,
	mode csmock.Mode,
) *chainsyncServerFixture {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)

	ledgerState := newTestLedgerState(t)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: bus,
			Logger:   logger,
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: connManager,
		EventBus:    bus,
		Logger:      logger,
	})
	o.ledgerState = ledgerState
	o.chainsyncState = dchainsync.NewState(bus, ledgerState)

	f := &chainsyncServerFixture{o: o}

	// The harness assigns the connection ID, so observe it as the production
	// callbacks run. These shims only record the ID and delegate; the real
	// callbacks (and their instrumentation wrappers) still do all the work.
	serverCfg := ochainsync.NewConfig(o.chainsyncServerConnOpts()...)
	findIntersect := serverCfg.FindIntersectFunc
	serverCfg.FindIntersectFunc = func(
		ctx ochainsync.CallbackContext,
		points []ocommon.Point,
	) (ocommon.Point, ochainsync.Tip, error) {
		f.recordConnId(ctx.ConnectionId)
		return findIntersect(ctx, points)
	}
	requestNext := serverCfg.RequestNextFunc
	serverCfg.RequestNextFunc = func(ctx ochainsync.CallbackContext) error {
		f.recordConnId(ctx.ConnectionId)
		return requestNext(ctx)
	}

	_, closedCh := bus.Subscribe(connmanager.ConnectionClosedEventType)
	f.closedCh = closedCh

	h, err := csmock.New(csmock.Config{
		Mode:      mode,
		ChainSync: serverCfg,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })

	f.h = h

	// Register the server-under-test connection the way a real peer connection
	// would be. chainsyncServerRequestNext resolves the peer through
	// ConnManager immediately after sending AwaitReply, so without this the
	// async serving path cannot run at all. Requires ouroboros-mock v0.16.0's
	// Harness.ServerConnection.
	f.conn = h.ServerConnection()
	require.NotNil(t, f.conn)
	require.True(
		t,
		connManager.AddConnection(
			f.conn,
			false,
			f.conn.Id().RemoteAddr.String(),
		),
	)

	return f
}

// observe returns the next message the server put on the wire, failing the
// test if none arrives within a bounded window. Synchronization is entirely
// channel-based; nothing in this file sleeps.
func (f *chainsyncServerFixture) observe(t *testing.T) csmock.ServerMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := f.h.Observe(ctx)
	require.NoError(t, err, "expected a chainsync message from the server")
	return msg
}

// appendBlock adds a block to the fixture's chain and returns it with its
// chainsync point.
func (f *chainsyncServerFixture) appendBlock(
	t *testing.T,
	slot, blockNumber uint64,
	hashByte byte,
) (*testBlock, ocommon.Point) {
	t.Helper()
	header, ok := newTestBlockHeader(
		slot,
		blockNumber,
		hashByte,
	).(*testBlockHeader)
	require.True(t, ok)
	block := &testBlock{
		testBlockHeader: header,
		blockType:       1,
		cbor:            []byte{0x80},
	}
	require.NoError(t, f.o.ledgerState.Chain().AddBlock(block, nil))
	return block, ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
}

// setTip publishes a ledger tip so the server reports it to the peer.
func (f *chainsyncServerFixture) setTip(
	block *testBlock,
	point ocommon.Point,
) {
	f.o.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       point,
		BlockNumber: block.BlockNumber(),
	})
}

// registeredClient returns the server-side chainsync client state the server
// registered for the harness connection, if any. It reads through
// LookupClient rather than AddClient so asking the question cannot create the
// state being asserted on.
func (f *chainsyncServerFixture) registeredClient(
	t *testing.T,
) (*dchainsync.ChainsyncClientState, bool) {
	t.Helper()
	connId, ok := f.observedConnId()
	if !ok {
		return nil, false
	}
	return f.o.chainsyncState.LookupClient(connId)
}

// requireConnectionClosed asserts the connection was closed through normal
// connmanager lifecycle handling. The event error is nil because the watcher
// is woken by the connection closing, not by an error being pushed onto its
// channel.
func (f *chainsyncServerFixture) requireConnectionClosed(
	t *testing.T,
	msg string,
) {
	t.Helper()
	evt := testutil.RequireReceive(t, f.closedCh, 5*time.Second, msg)
	closed, ok := evt.Data.(connmanager.ConnectionClosedEvent)
	require.True(t, ok)
	require.Equal(t, f.conn.Id(), closed.ConnectionId)
	require.NoError(
		t,
		closed.Error,
		"close must be published as a graceful lifecycle event, not an "+
			"error pushed onto the connection's error channel",
	)
}

// drainInitialRollback performs the intersect-then-rollback handshake every
// downstream client starts with, leaving the server ready to serve blocks.
func (f *chainsyncServerFixture) drainInitialRollback(
	t *testing.T,
	intersect ocommon.Point,
) {
	t.Helper()
	require.NoError(t, f.h.FindIntersect([]ocommon.Point{intersect}))
	found := f.observe(t)
	require.True(t, found.IsIntersectFound(), "expected IntersectFound")

	require.NoError(t, f.h.RequestNext())
	rollback := f.observe(t)
	require.True(t, rollback.IsRollBackward(), "expected initial RollBackward")
}

// =============================================================================
// FindIntersect
// =============================================================================

// TestChainsyncServerFindIntersectEmitsFoundAndRegistersClient verifies a
// matching point produces an IntersectFound carrying that exact point and the
// current tip, and that the server registered the downstream client at the
// intersect point as a result.
func TestChainsyncServerFindIntersectEmitsFoundAndRegistersClient(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	block, point := f.appendBlock(t, 1, 1, 0x01)
	f.setTip(block, point)

	// No client may exist before the peer has asked for an intersection.
	_, registered := f.registeredClient(t)
	require.False(t, registered, "no client should be registered yet")

	require.NoError(t, f.h.FindIntersect([]ocommon.Point{point}))

	msg := f.observe(t)
	require.True(t, msg.IsIntersectFound(), "expected IntersectFound")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, point, gotPoint, "IntersectFound point")
	gotTip, ok := msg.Tip()
	require.True(t, ok)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"IntersectFound tip",
	)

	// Returning a point without registering the client would leave the
	// server unable to serve the peer, so assert the registration happened
	// and is cursored at the intersection.
	clientState, registered := f.registeredClient(t)
	require.True(
		t,
		registered,
		"FindIntersect returned a point without registering the client",
	)
	require.Equal(t, point, clientState.Cursor)
	require.True(t, clientState.NeedsInitialRollback)
}

// TestChainsyncServerFindIntersectEmitsNotFoundForUnknownPoint verifies an
// in-range but unknown point produces IntersectNotFound carrying the current
// tip, and leaves no client registered.
func TestChainsyncServerFindIntersectEmitsNotFoundForUnknownPoint(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	block, point := f.appendBlock(t, 10, 1, 0x01)
	f.setTip(block, point)

	unknown := ocommon.NewPoint(10, make([]byte, 32))
	require.NoError(t, f.h.FindIntersect([]ocommon.Point{unknown}))

	msg := f.observe(t)
	require.True(t, msg.IsIntersectNotFound(), "expected IntersectNotFound")
	gotTip, ok := msg.Tip()
	require.True(t, ok)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"IntersectNotFound must still report the current tip",
	)

	_, registered := f.registeredClient(t)
	require.False(
		t,
		registered,
		"a failed intersection must not register a downstream client",
	)
}

// TestChainsyncServerFindIntersectAcceptsPointListAtLimit verifies a point
// list exactly at chainsyncMaxFindIntersectPoints is served normally. An empty
// chain intersects any in-bounds request at origin, so IntersectFound at
// origin proves the cap did not short-circuit the request.
func TestChainsyncServerFindIntersectAcceptsPointListAtLimit(t *testing.T) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints)
	require.NoError(t, f.h.FindIntersect(points))

	msg := f.observe(t)
	require.True(
		t,
		msg.IsIntersectFound(),
		"a point list at the limit must be accepted",
	)
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, csmock.OriginPoint(), gotPoint)
}

// TestChainsyncServerFindIntersectRejectsPointListOverLimit verifies an
// over-limit list is rejected with IntersectNotFound before any intersection
// lookup, rather than tearing the connection down. On an empty chain the
// lookup would otherwise have matched origin, so IntersectNotFound here proves
// the cap short-circuited the request.
func TestChainsyncServerFindIntersectRejectsPointListOverLimit(t *testing.T) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints + 1)
	require.NoError(t, f.h.FindIntersect(points))

	msg := f.observe(t)
	require.True(
		t,
		msg.IsIntersectNotFound(),
		"an over-limit point list must be rejected with IntersectNotFound",
	)

	_, registered := f.registeredClient(t)
	require.False(
		t,
		registered,
		"a rejected intersection must not register a downstream client",
	)
}

// TestChainsyncServerFindIntersectAcceptsNormalPointList verifies the point
// count a well-behaved client actually sends is served normally.
func TestChainsyncServerFindIntersectAcceptsNormalPointList(t *testing.T) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncIntersectPointCount)
	require.NoError(t, f.h.FindIntersect(points))

	msg := f.observe(t)
	require.True(t, msg.IsIntersectFound())
}

// TestChainsyncServerFindIntersectDeduplicatesRepeatedPointsForBudget verifies
// duplicate points within one request are deduplicated before the
// per-connection work budget is charged. A list of chainsyncMaxFindIntersectPoints
// copies of the same point is at the point-count limit but collapses to a
// single point after deduplication, so it must be charged as 1 point of work,
// not chainsyncMaxFindIntersectPoints.
func TestChainsyncServerFindIntersectDeduplicatesRepeatedPointsForBudget(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	// An empty chain intersects any in-bounds request at origin, so
	// IntersectFound here only tells us the request wasn't rejected — the
	// assertion that matters is the second request below.
	point := makeFindIntersectPoints(1)[0]
	dup := make([]ocommon.Point, chainsyncMaxFindIntersectPoints)
	for i := range dup {
		dup[i] = point
	}
	require.NoError(t, f.h.FindIntersect(dup))
	require.True(t, f.observe(t).IsIntersectFound())

	// Had the duplicate-heavy request above been charged its full
	// un-deduplicated size, it would have exhausted the entire work budget
	// on its own, and this distinct-point request — within both the
	// point-count and work-budget limits by itself — would be rejected too.
	require.NoError(
		t,
		f.h.FindIntersect(
			makeFindIntersectPoints(chainsyncMaxFindIntersectPoints-1),
		),
	)
	require.True(
		t,
		f.observe(t).IsIntersectFound(),
		"a duplicate-heavy request must not exhaust the work budget meant for distinct points",
	)
}

// TestChainsyncServerFindIntersectRateLimitsRepeatedRequests verifies the
// per-connection work budget bounds cumulative work across many in-bounds
// requests, not just the size of a single request: a second full-size
// request immediately following the first must be rejected even though
// each is within the point-count limit on its own. The gap between the two
// requests is a single wire round trip — far too short for
// chainsyncFindIntersectBudgetRate to refill another full burst — so this
// is deterministic rather than timing-sensitive.
func TestChainsyncServerFindIntersectRateLimitsRepeatedRequests(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints)
	require.NoError(t, f.h.FindIntersect(points))
	require.True(
		t,
		f.observe(t).IsIntersectFound(),
		"first full-size request should be within the work budget",
	)

	require.NoError(t, f.h.FindIntersect(points))
	require.True(
		t,
		f.observe(t).IsIntersectNotFound(),
		"a repeated full-size request over the per-connection work budget must be rejected",
	)
}

// =============================================================================
// RequestNext (synchronous replies)
// =============================================================================

// TestChainsyncServerRequestNextEmitsInitialRollbackToIntersect verifies the
// first RequestNext after an intersection replies with a RollBackward to the
// exact intersection point and current tip, and clears the pending-rollback
// flag so the next reply serves a block instead.
func TestChainsyncServerRequestNextEmitsInitialRollbackToIntersect(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	block, point := f.appendBlock(t, 1, 1, 0x01)
	f.setTip(block, point)

	require.NoError(t, f.h.FindIntersect([]ocommon.Point{point}))
	require.True(t, f.observe(t).IsIntersectFound())

	require.NoError(t, f.h.RequestNext())

	msg := f.observe(t)
	require.True(t, msg.IsRollBackward(), "expected RollBackward")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, point, gotPoint, "initial rollback must target intersect")
	gotTip, ok := msg.Tip()
	require.True(t, ok)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
	)

	clientState, registered := f.registeredClient(t)
	require.True(t, registered)
	require.False(
		t,
		clientState.NeedsInitialRollback,
		"initial rollback must clear the pending-rollback flag",
	)
}

// TestChainsyncServerRequestNextEmitsRollForwardWithExactBlock verifies an
// available iterator block is sent immediately as a RollForward carrying the
// exact block type, block CBOR and tip.
//
// The predecessor of this test asserted only that the iterator had advanced to
// the chain tip, which a callback that drained the iterator and sent nothing
// would also have satisfied.
func TestChainsyncServerRequestNextEmitsRollForwardWithExactBlock(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	block, point := f.appendBlock(t, 1, 1, 0x01)

	require.NoError(t, f.h.RequestNext())

	msg := f.observe(t)
	require.True(t, msg.IsRollForward(), "expected RollForward")
	blockType, blockCbor, gotTip, ok := msg.RollForwardNtC()
	require.True(t, ok)
	require.Equal(t, uint(block.Type()), blockType, "RollForward block type")
	require.Equal(t, block.Cbor(), blockCbor, "RollForward block CBOR")
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"RollForward tip must not lag the block being sent",
	)
}

// TestChainsyncServerRequestNextEmitsRollBackwardOnChainRollback verifies a
// pending iterator rollback is sent immediately as a RollBackward carrying the
// exact rollback point.
//
// As with the roll-forward case, the predecessor asserted only that the
// iterator had been drained.
func TestChainsyncServerRequestNextEmitsRollBackwardOnChainRollback(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	// Serve one block so the iterator is past origin, then roll the chain
	// back so the next synchronous iterator result is a rollback event.
	f.appendBlock(t, 1, 1, 0x01)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward())

	require.NoError(
		t,
		f.o.ledgerState.Chain().Rollback(ocommon.NewPointOrigin()),
	)

	require.NoError(t, f.h.RequestNext())

	msg := f.observe(t)
	require.True(t, msg.IsRollBackward(), "expected RollBackward")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(
		t,
		ocommon.NewPointOrigin(),
		gotPoint,
		"rollback must target the point the chain rolled back to",
	)
}

// =============================================================================
// RequestNext (AwaitReply and the asynchronous replies that follow)
//
// These paths require the peer connection to be resolvable through Dingo's
// ConnManager: chainsyncServerRequestNext looks it up immediately after
// sending AwaitReply so it can abandon the blocked iterator read if the peer
// goes away. The fixture registers the harness connection for exactly that
// reason.
// =============================================================================

// TestChainsyncServerRequestNextEmitsAwaitReplyThenAsyncRollForward verifies
// that an iterator sitting at the chain tip parks the peer with AwaitReply,
// and that a block appended afterwards is served asynchronously as a
// RollForward carrying the exact block type, block CBOR and tip.
//
// The predecessor of the AwaitReply half asserted only that the callback
// returned nil, which a callback that sent nothing at all would also satisfy.
// The async RollForward half could not be covered before at all: the
// post-AwaitReply ConnManager lookup failed, so the callback errored and tore
// the connection down instead of serving.
func TestChainsyncServerRequestNextEmitsAwaitReplyThenAsyncRollForward(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	// Iterator is at the chain tip, so the server parks the peer.
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	// A block arriving after the park must be served by the async goroutine.
	block, point := f.appendBlock(t, 1, 1, 0x01)

	msg := f.observe(t)
	require.True(t, msg.IsRollForward(), "expected async RollForward")
	blockType, blockCbor, gotTip, ok := msg.RollForwardNtC()
	require.True(t, ok)
	require.Equal(t, uint(block.Type()), blockType)
	require.Equal(t, block.Cbor(), blockCbor)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"async RollForward tip must not lag the block being sent",
	)
}

// TestChainsyncServerRequestNextEmitsAsyncRollBackward verifies a rollback
// that happens while the peer is parked in AwaitReply is served
// asynchronously as a RollBackward carrying the exact rollback point.
func TestChainsyncServerRequestNextEmitsAsyncRollBackward(t *testing.T) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	// Serve one block so the iterator is past origin.
	f.appendBlock(t, 1, 1, 0x01)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward())

	// Park the peer, then roll the chain back underneath it.
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	require.NoError(
		t,
		f.o.ledgerState.Chain().Rollback(ocommon.NewPointOrigin()),
	)

	msg := f.observe(t)
	require.True(t, msg.IsRollBackward(), "expected async RollBackward")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, ocommon.NewPointOrigin(), gotPoint)
}

// TestChainsyncServerRequestNextAsyncRollForwardFailureClosesConnection
// verifies that when the asynchronous RollForward send fails — after
// chainsyncServerRequestNext has already returned, so the protocol layer can
// no longer turn an error into teardown — the connection is still closed
// through normal connmanager lifecycle handling rather than left silently
// open with the peer parked in AwaitReply.
func TestChainsyncServerRequestNextAsyncRollForwardFailureClosesConnection(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	// Stop the protocol so the async send fails, then wake the iterator.
	f.h.Server().Stop()
	f.appendBlock(t, 1, 1, 0x01)

	f.requireConnectionClosed(
		t,
		"async RollForward send failure should close the connection",
	)
}

// TestChainsyncServerRequestNextAsyncRollBackwardFailureClosesConnection is
// the rollback counterpart: a rollback send that fails after AwaitReply must
// not leave the downstream peer connection silently open either.
func TestChainsyncServerRequestNextAsyncRollBackwardFailureClosesConnection(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	f.appendBlock(t, 1, 1, 0x01)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward())

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	// Stop the protocol so the async send fails, then roll back.
	f.h.Server().Stop()
	require.NoError(
		t,
		f.o.ledgerState.Chain().Rollback(ocommon.NewPointOrigin()),
	)

	f.requireConnectionClosed(
		t,
		"async RollBackward send failure should close the connection",
	)
}

// TestChainsyncServerRequestNextIteratorCancelDoesNotCloseConnection verifies
// that ordinary iterator cancellation — which is how the async wait unwinds
// during normal connection teardown — is not mistaken for a failure worth
// recycling the connection over.
func TestChainsyncServerRequestNextIteratorCancelDoesNotCloseConnection(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	clientState := f.registerClientAtOrigin(t)

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	clientState.ChainIter.Cancel()

	testutil.RequireNoReceive(
		t,
		f.closedCh,
		100*time.Millisecond,
		"iterator cancellation should not close the connection",
	)
}

// =============================================================================
// RequestNext error paths
//
// These assert the error the callback returns. The protocol layer converts a
// callback error into connection teardown rather than an observable message,
// so the callback is invoked directly; the fixture still supplies the real
// connection and server it runs against.
// =============================================================================

// TestChainsyncServerRequestNextSyncIteratorErrorPropagates verifies a real
// iterator failure is returned rather than being mistaken for the chain-tip
// sentinel (which would silently park the peer instead of surfacing the
// fault).
func TestChainsyncServerRequestNextSyncIteratorErrorPropagates(t *testing.T) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.registerClientAtOrigin(t)

	// Break the backing store so the synchronous iterator returns a real
	// lookup error.
	require.NoError(t, dbtest.CloseDatabase(f.o.ledgerState.Database()))

	err := f.o.chainsyncServerRequestNext(f.callbackContext())

	require.Error(t, err)
	require.NotErrorIs(t, err, chain.ErrIteratorChainTip)
}

// TestChainsyncServerRequestNextAwaitReplyErrorPropagates verifies an
// AwaitReply send failure is returned from the callback, so the protocol layer
// tears the connection down instead of arming an async wait on a dead peer.
func TestChainsyncServerRequestNextAwaitReplyErrorPropagates(t *testing.T) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.registerClientAtOrigin(t)

	// Stop the protocol so the AwaitReply send itself fails.
	f.h.Server().Stop()

	require.Error(t, f.o.chainsyncServerRequestNext(f.callbackContext()))
}

// TestChainsyncServerRequestNextMissingConnectionAfterAwaitReply verifies the
// post-AwaitReply connection lookup fails explicitly when the connection was
// already recycled, rather than arming an async wait with no way to notice the
// peer is gone.
func TestChainsyncServerRequestNextMissingConnectionAfterAwaitReply(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.registerClientAtOrigin(t)

	require.True(t, f.o.connManager.RemoveConnection(f.conn.Id(), f.conn))

	err := f.o.chainsyncServerRequestNext(f.callbackContext())

	require.ErrorContains(t, err, "not found")
}
