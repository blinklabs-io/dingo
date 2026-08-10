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

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
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
// # Coverage boundary
//
// Every path up to and including the *synchronous* RequestNext replies is
// covered here. Paths that reach AwaitReply are not, and stay on the local
// connmanager-backed harness in chainsync_test.go: immediately after sending
// AwaitReply, chainsyncServerRequestNext looks the connection up with
// o.ConnManager.GetConnectionById(ctx.ConnectionId) so it can monitor the peer
// while the iterator blocks. The shared harness creates and owns the
// server-under-test connection internally and does not expose it, so it can
// never be registered with Dingo's ConnManager and that lookup necessarily
// fails. Covering those paths here needs an accessor upstream in
// ouroboros-mock; see the note on blinklabs-io/dingo#2820.

// chainsyncServerFixture pairs a Dingo Ouroboros instance with a shared
// ouroboros-mock ChainSync harness driving its server callbacks.
type chainsyncServerFixture struct {
	o *Ouroboros
	h *csmock.Harness

	// connIdMu guards connId, which the server callbacks record from the
	// protocol goroutine while tests read it.
	connIdMu sync.Mutex
	connId   *ouroboros.ConnectionId
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

	o := NewOuroboros(OuroborosConfig{
		ConnManager: connManager,
		EventBus:    bus,
		Logger:      logger,
	})
	o.LedgerState = ledgerState
	o.ChainsyncState = dchainsync.NewState(bus, ledgerState)

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

	h, err := csmock.New(csmock.Config{
		Mode:      mode,
		ChainSync: serverCfg,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })

	f.h = h
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
	require.NoError(t, f.o.LedgerState.Chain().AddBlock(block, nil))
	return block, ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
}

// setTip publishes a ledger tip so the server reports it to the peer.
func (f *chainsyncServerFixture) setTip(
	block *testBlock,
	point ocommon.Point,
) {
	f.o.LedgerState.SetTipForTesting(ochainsync.Tip{
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
	return f.o.ChainsyncState.LookupClient(connId)
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
		f.o.LedgerState.Chain().Rollback(ocommon.NewPointOrigin()),
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
