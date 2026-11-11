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
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const csjTestJumpBlocks = 10

var errCsjTestRefused = errors.New("test peer refuses FindIntersect")

func csjPoint(block uint64) ocommon.Point {
	return ocommon.NewPoint(block*10, fmt.Appendf(nil, "block-%026d", block))
}

// csjChain returns points for blocks 1..n.
func csjChain(n uint64) []ocommon.Point {
	points := make([]ocommon.Point, 0, n)
	for b := uint64(1); b <= n; b++ {
		points = append(points, csjPoint(b))
	}
	return points
}

type csjAgreement struct {
	jumper ouroboros.ConnectionId
	dynamo ouroboros.ConnectionId
	point  ocommon.Point
}

// csjPeer is one remote peer: a real gouroboros ChainSync server over a pipe
// whose chain holds blocks 1..tipBlock, except that it forks away from the
// dynamo's chain above forkBlock when forkBlock is set.
type csjPeer struct {
	connId    ouroboros.ConnectionId
	spy       *wireSpyConn
	tipBlock  atomic.Uint64
	forkBlock atomic.Uint64
	// failIntersect makes the server fail the protocol on FindIntersect
	// instead of answering it.
	failIntersect atomic.Bool
	// intersects receives the points of every FindIntersect the peer
	// answered.
	intersects chan []ocommon.Point
	// requestNexts receives the server handle of every RequestNext the peer
	// has not answered.
	requestNexts chan *ochainsync.Server
}

// awaitReply makes the peer answer its pending RequestNext with MsgAwaitReply,
// as a server does once it has sent every header up to its tip.
func (p *csjPeer) awaitReply(t *testing.T) {
	t.Helper()
	server := testutil.RequireReceive(
		t,
		p.requestNexts,
		5*time.Second,
		"pending RequestNext",
	)
	require.NoError(t, server.AwaitReply())
}

func (p *csjPeer) holds(point ocommon.Point) bool {
	// Points outside the test chain come from the local ledger's default
	// intersect points, which every peer shares.
	if !bytes.HasPrefix(point.Hash, []byte("block-")) {
		return true
	}
	block := point.Slot / 10
	if block == 0 || block > p.tipBlock.Load() {
		return false
	}
	if fork := p.forkBlock.Load(); fork > 0 && block > fork {
		return false
	}
	return string(point.Hash) == string(csjPoint(block).Hash)
}

type csjHarness struct {
	t          *testing.T
	o          *Ouroboros
	cm         *connmanager.ConnectionManager
	active     atomic.Bool
	fragment   atomic.Uint64 // dynamo fragment head block
	agreements chan csjAgreement
	rejections chan ouroboros.ConnectionId
	stopped    chan ouroboros.ConnectionId
	// unidentified holds connections without a corroboration identity.
	unidentified sync.Map // ouroboros.ConnectionId -> struct{}
	peers        int
	// fragmentReads counts reads of the dynamo's candidate fragment.
	fragmentReads atomic.Int64
}

// csjAddrConn gives each pipe its own remote address, since a ConnectionId is
// the address pair and every net.Pipe reports the same one.
type csjAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c *csjAddrConn) RemoteAddr() net.Addr { return c.remote }

func newCsjHarness(t *testing.T, enabled bool) *csjHarness {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	o, cm, _ := newOutboundStartTestOuroboros(t, logger, bus)
	ls, _ := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls
	// Registered after the ledger and before any peer, so it runs after
	// their connections close and before the ledger closes: it stops the
	// refresh loop and waits for in-flight jump work, which reads the ledger.
	t.Cleanup(o.outboundChainsyncLifecycle.close)
	o.chainsyncState = dchainsync.NewState(bus, nil)
	h := &csjHarness{
		t:          t,
		o:          o,
		cm:         cm,
		agreements: make(chan csjAgreement, 64),
		rejections: make(chan ouroboros.ConnectionId, 64),
		stopped:    make(chan ouroboros.ConnectionId, 64),
	}
	h.active.Store(true)
	o.chainsyncClientStoppedHook = func(id ouroboros.ConnectionId) {
		h.stopped <- id
	}
	o.config.ChainsyncRecordDeliveredPeerTip = func(
		chainselection.PeerTipUpdateEvent,
	) {
	}
	o.config.ChainsyncJumping = ChainsyncJumpingConfig{
		Enabled: enabled,
		Active:  h.active.Load,
		PeerIdentity: func(connId ouroboros.ConnectionId) string {
			if _, ok := h.unidentified.Load(connId); ok {
				return ""
			}
			return "identity-" + connId.String()
		},
		BestPeer: func() *ouroboros.ConnectionId { return nil },
		DynamoFragment: func(
			ouroboros.ConnectionId,
		) ([]ocommon.Point, uint64, bool) {
			h.fragmentReads.Add(1)
			head := h.fragment.Load()
			if head == 0 {
				return nil, 0, false
			}
			return csjChain(head), head, true
		},
		RecordAgreement: func(
			jumper ouroboros.ConnectionId,
			dynamo ouroboros.ConnectionId,
			point ocommon.Point,
			_ ochainsync.Tip,
		) bool {
			h.agreements <- csjAgreement{
				jumper: jumper,
				dynamo: dynamo,
				point:  point,
			}
			return true
		},
		RecordRejection: func(jumper ouroboros.ConnectionId) bool {
			h.rejections <- jumper
			return true
		},
		JumpBlocks: func() uint64 { return csjTestJumpBlocks },
	}
	o.initChainsyncJumping()
	return h
}

// addPeer connects an outbound peer whose server answers FindIntersect from
// its own chain and leaves RequestNext unanswered until awaitReply.
func (h *csjHarness) addPeer(name string, tipBlock uint64) *csjPeer {
	t := h.t
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	peer := &csjPeer{
		intersects:   make(chan []ocommon.Point, 64),
		requestNexts: make(chan *ochainsync.Server, 64),
	}
	peer.tipBlock.Store(tipBlock)
	serverCfg := ochainsync.NewConfig(
		ochainsync.WithFindIntersectFunc(
			func(
				_ ochainsync.CallbackContext,
				points []ocommon.Point,
			) (ocommon.Point, ochainsync.Tip, error) {
				peer.intersects <- points
				if peer.failIntersect.Load() {
					return ocommon.Point{}, ochainsync.Tip{}, errCsjTestRefused
				}
				tip := ochainsync.Tip{
					Point:       csjPoint(peer.tipBlock.Load()),
					BlockNumber: peer.tipBlock.Load(),
				}
				var best ocommon.Point
				found := false
				for _, point := range points {
					if peer.holds(point) && (!found || point.Slot > best.Slot) {
						best, found = point, true
					}
				}
				if !found {
					return ocommon.Point{}, tip, ochainsync.ErrIntersectNotFound
				}
				return best, tip, nil
			},
		),
		// A peer behind its tip is still streaming headers and has no
		// AwaitReply to send; one that reached it replies on awaitReply.
		ochainsync.WithRequestNextFunc(
			func(ctx ochainsync.CallbackContext) error {
				peer.requestNexts <- ctx.Server
				return nil
			},
		),
	)
	serverPipe, clientPipe := net.Pipe()
	serverConnCh := make(chan *ouroboros.Connection, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		c, err := ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(csjTestNetworkMagic),
			ouroboros.WithNodeToNode(true),
			ouroboros.WithChainSyncConfig(serverCfg),
			ouroboros.WithLogger(logger),
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		serverConnCh <- c
	}()
	h.peers++
	peer.spy = newWireSpyConn(&csjAddrConn{
		Conn: clientPipe,
		remote: &net.TCPAddr{
			IP:   net.IPv4(10, 0, 0, byte(h.peers)),
			Port: 3001,
		},
	})
	clientConn, err := ouroboros.New(
		ouroboros.WithConnection(peer.spy),
		ouroboros.WithNetworkMagic(csjTestNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithLogger(logger),
		h.o.chainsyncConnectionConfigOption(t.Context(), true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })
	select {
	case err := <-serverErrCh:
		t.Fatalf("peer connection setup failed: %v", err)
	case serverConn := <-serverConnCh:
		t.Cleanup(func() { _ = serverConn.Close() })
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for peer connection setup")
	}
	require.True(t, h.cm.AddConnection(clientConn, false, name+":3001"))
	peer.connId = clientConn.Id()
	return peer
}

// promote starts peer's ChainSync client, as the outbound connection handler
// does.
func (h *csjHarness) promote(peer *csjPeer) {
	h.t.Helper()
	conn := h.cm.GetConnectionById(peer.connId)
	require.NotNil(h.t, conn)
	require.True(h.t, h.o.startOutboundChainsyncLocked(peer.connId, conn))
}

// deliver reports a header delivered by connId at block, as the roll-forward
// handler does after verification.
func (h *csjHarness) deliver(connId ouroboros.ConnectionId, block uint64) {
	h.o.config.ChainsyncRecordDeliveredPeerTip(
		chainselection.PeerTipUpdateEvent{
			ConnectionId: connId,
			ObservedTip: ochainsync.Tip{
				Point:       csjPoint(block),
				BlockNumber: block,
			},
		},
	)
}

// awaitRound waits for the jump round a delivery started, if any, to finish.
// csjObserveDelivery marks the round in flight before it returns.
func (h *csjHarness) awaitRound() {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		h.o.csj.mu.Lock()
		defer h.o.csj.mu.Unlock()
		return !h.o.csj.roundInFlight
	}, 5*time.Second, time.Millisecond)
}

func (h *csjHarness) requireAgreement(
	jumper, dynamo ouroboros.ConnectionId,
	point ocommon.Point,
) {
	h.t.Helper()
	got := testutil.RequireReceive(
		h.t,
		h.agreements,
		5*time.Second,
		"jump agreement for "+jumper.String(),
	)
	require.Equal(h.t, jumper, got.jumper)
	require.Equal(h.t, dynamo, got.dynamo)
	require.Equal(h.t, point, got.point)
}

// requireAgreements receives one agreement per jumper, in any order.
func (h *csjHarness) requireAgreements(
	dynamo ouroboros.ConnectionId,
	point ocommon.Point,
	jumpers ...ouroboros.ConnectionId,
) {
	h.t.Helper()
	want := make(map[ouroboros.ConnectionId]bool, len(jumpers))
	for _, id := range jumpers {
		want[id] = true
	}
	for range jumpers {
		got := testutil.RequireReceive(
			h.t,
			h.agreements,
			5*time.Second,
			"jump agreement",
		)
		require.True(h.t, want[got.jumper], "unexpected jumper %s", got.jumper)
		delete(want, got.jumper)
		require.Equal(h.t, dynamo, got.dynamo)
		require.Equal(h.t, point, got.point)
	}
}

func (p *csjPeer) requireMessage(
	t *testing.T,
	want uint8,
	what string,
) {
	t.Helper()
	require.Equal(t, chainsyncWireMessage(want), p.spy.next(t, what))
}

// requireStreams waits for the FindIntersect and RequestNext that start a
// stream and returns the points the FindIntersect offered.
func (p *csjPeer) requireStreams(t *testing.T, what string) []ocommon.Point {
	t.Helper()
	p.requireMessage(
		t,
		ochainsync.MessageTypeFindIntersect,
		what+": FindIntersect",
	)
	points := testutil.RequireReceive(
		t,
		p.intersects,
		5*time.Second,
		what+": intersect points",
	)
	p.requireMessage(t, ochainsync.MessageTypeRequestNext, what+": RequestNext")
	return points
}

// requireJump waits for one jump and returns the points it offered.
func (p *csjPeer) requireJump(t *testing.T, what string) []ocommon.Point {
	t.Helper()
	p.requireMessage(t, ochainsync.MessageTypeFindIntersect, what)
	return testutil.RequireReceive(
		t,
		p.intersects,
		5*time.Second,
		what+": points",
	)
}

// startDynamoAndJumpers starts the dynamo, whose candidate fragment then
// reaches block 10, and two jumpers, each of which jumps to block 10 at once.
func (h *csjHarness) startDynamoAndJumpers(
	jumperTips ...uint64,
) (*csjPeer, []*csjPeer) {
	t := h.t
	t.Helper()
	dynamo := h.addPeer("dynamo", 100)
	h.promote(dynamo)
	dynamo.requireStreams(t, "dynamo")
	h.fragment.Store(10)
	jumpers := make([]*csjPeer, 0, len(jumperTips))
	for i, tip := range jumperTips {
		jumper := h.addPeer(fmt.Sprintf("jumper%d", i), tip)
		h.promote(jumper)
		points := jumper.requireJump(t, "first jump")
		require.Equal(
			t,
			csjPoint(10),
			points[0],
			"a jump names the dynamo's head first",
		)
		h.requireAgreement(jumper.connId, dynamo.connId, csjPoint(10))
		observer, tracked := h.o.chainsyncState.ClientObservabilityOnly(
			jumper.connId,
		)
		require.True(t, tracked)
		require.True(t, observer, "a jumper holds no ingress slot")
		jumpers = append(jumpers, jumper)
	}
	return dynamo, jumpers
}

// One dynamo streams; two identified jumpers send only MsgFindIntersect, at
// admission and once per jump interval of the dynamo's progress, and each
// agreement reaches the selector naming the dynamo.
func TestChainsyncJumpingJumpersOnlyFindIntersect(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)

	// Progress below the interval sends nothing, and does not even read
	// the dynamo's candidate fragment, which every delivered header would
	// otherwise do.
	reads := h.fragmentReads.Load()
	h.fragment.Store(15)
	h.deliver(dynamo.connId, 15)
	h.awaitRound()
	require.Equal(t, reads, h.fragmentReads.Load())
	// A delivered header past the interval whose candidate fragment still
	// lags below it sends nothing either: jumps name candidate points.
	h.deliver(dynamo.connId, 20)
	h.awaitRound()
	for _, jumper := range jumpers {
		jumper.spy.requireQuiet(t)
	}
	// The interval is counted from the last jump's head (block 10).
	h.fragment.Store(20)
	h.deliver(dynamo.connId, 21)
	for _, jumper := range jumpers {
		points := jumper.requireJump(t, "interval jump")
		require.Equal(t, csjPoint(20), points[0])
		require.Contains(
			t,
			points,
			csjPoint(10),
			"a jump offers the last agreed point",
		)
	}
	h.requireAgreements(
		dynamo.connId,
		csjPoint(20),
		jumpers[0].connId,
		jumpers[1].connId,
	)

	// Deliveries by a jumper or below the next interval trigger nothing.
	h.deliver(jumpers[0].connId, 40)
	h.deliver(dynamo.connId, 29)
	for _, jumper := range jumpers {
		jumper.spy.requireQuiet(t)
	}
	select {
	case id := <-h.rejections:
		t.Fatalf("unexpected rejection by %s", id)
	default:
	}
}

// Any answer other than the jump point is a rejection, as in the reference's
// processJumpResult: a jumper that forks from the dynamo and one that is
// merely behind it both have the agreement recorded at the dynamo point they
// do hold, are rejected as objectors, and stream, where the Limit on Patience
// and the density disconnector see them. A jumper left behind would pin the
// Limit on Eagerness k past its last agreement. That point is above the local
// chain, so they intersect on the local chain.
func TestChainsyncJumpingObjectorStreamsFromLastAgreedPoint(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)
	behind, forked := jumpers[0], jumpers[1]
	behind.tipBlock.Store(17)
	forked.forkBlock.Store(16)

	h.fragment.Store(20)
	h.deliver(dynamo.connId, 20)
	behind.requireJump(t, "behind jumper jump")
	forkedJump := forked.requireJump(t, "forked jumper jump")
	require.Equal(t, csjPoint(20), forkedJump[0])

	// Both hold block 16 (20 - 4), the highest offered point they share.
	h.requireAgreements(
		dynamo.connId,
		csjPoint(16),
		behind.connId,
		forked.connId,
	)
	rejected := map[ouroboros.ConnectionId]bool{}
	for range 2 {
		rejected[testutil.RequireReceive(
			t,
			h.rejections,
			5*time.Second,
			"objector rejection",
		)] = true
	}
	require.True(t, rejected[forked.connId], "a forked jumper is rejected")
	require.True(t, rejected[behind.connId],
		"a jumper behind the jump point is rejected, not left a jumper")
	for _, objector := range []*csjPeer{forked, behind} {
		points := objector.requireStreams(t, "objector")
		h.requireOnLocalChain(points, "objector intersect")
		require.False(t, h.o.csjIsJumper(objector.connId))
	}
}

// A jumper that shares no offered point is rejected and streams.
func TestChainsyncJumpingNotFoundMakesObjector(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]
	// From now on the peer answers every jump with IntersectNotFound.
	jumper.tipBlock.Store(0)

	h.fragment.Store(20)
	h.deliver(dynamo.connId, 20)
	jumper.requireJump(t, "refused jump")
	require.Equal(
		t,
		jumper.connId,
		testutil.RequireReceive(t, h.rejections, 5*time.Second, "rejection"),
	)
	points := jumper.requireStreams(t, "objector")
	h.requireOnLocalChain(points, "objector intersect")
}

// csjLocalPoint is block n of the harness's local chain
// (newTestLedgerStateWithChain), which every test peer holds.
func csjLocalPoint(n uint64) ocommon.Point {
	return ocommon.NewPoint(n, bytes.Repeat([]byte{byte(n)}, 32))
}

// requireOnLocalChain asserts that the local chain holds every point a
// stream offered other than origin, so the server intersects on it.
func (h *csjHarness) requireOnLocalChain(points []ocommon.Point, what string) {
	h.t.Helper()
	require.NotEmpty(h.t, points, what)
	for _, point := range points {
		if len(point.Hash) == 0 {
			continue
		}
		require.True(
			h.t,
			h.o.ledgerState.Chain().HoldsPoint(h.t.Context(), point),
			"%s: offered %v off the local chain", what, point,
		)
	}
}

// The live failure: the dynamo's last jump (block 10) was above the applied
// chain (blocks 1-5) when its connection closed. A header is authenticated
// only through ancestry the node holds, the applied chain or the same
// connection's earlier headers, and only the departed dynamo delivered the
// headers up to block 10. A promoted jumper that intersected at block 10
// would have every header it delivers withheld from chain selection, so its
// candidate fragment would stay empty, no jump would follow, and the
// remaining jump records would go stale. It must intersect on the local
// chain, and its deliveries must then drive the remaining jumper.
func TestChainsyncJumpingPromotedDynamoAboveLocalChainStreamsFromIt(
	t *testing.T,
) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)

	h.o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: dynamo.connId},
	))

	next, other := jumpers[0], jumpers[1]
	if compareConnIds(other.connId, next.connId) < 0 {
		next, other = other, next
	}
	// Rotation picks the agreed jumper with the lowest connection id.
	points := next.requireStreams(t, "new dynamo")
	h.requireOnLocalChain(points, "new dynamo intersect")
	require.False(t, h.o.csjIsJumper(next.connId))
	require.True(t, h.o.csjIsJumper(other.connId))

	h.fragment.Store(25)
	h.deliver(next.connId, 25)
	jump := other.requireJump(t, "jump to the new dynamo")
	require.Equal(t, csjPoint(25), jump[0])
	h.requireAgreement(other.connId, next.connId, csjPoint(25))
}

// A last agreed point the local chain holds stays the point a promoted
// jumper streams from, ahead of the local chain's own intersect points.
func TestChainsyncJumpingPromotedDynamoOnLocalChainResumesAtAgreedPoint(
	t *testing.T,
) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.csj.cfg.DynamoFragment = func(
		ouroboros.ConnectionId,
	) ([]ocommon.Point, uint64, bool) {
		points := make([]ocommon.Point, 0, 4)
		for n := uint64(1); n <= 4; n++ {
			points = append(points, csjLocalPoint(n))
		}
		return points, 4, true
	}
	dynamo := h.addPeer("dynamo", 100)
	h.promote(dynamo)
	dynamo.requireStreams(t, "dynamo")
	jumper := h.addPeer("jumper", 100)
	h.promote(jumper)
	jumper.requireJump(t, "first jump")
	h.requireAgreement(jumper.connId, dynamo.connId, csjLocalPoint(4))

	h.o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: dynamo.connId},
	))
	points := jumper.requireStreams(t, "new dynamo")
	require.Equal(t, csjLocalPoint(4), points[0])
	h.requireOnLocalChain(points, "new dynamo intersect")
}

// When Genesis selection ends, every jumper streams, and peers promoted
// afterwards stream at once.
func TestChainsyncJumpingGenesisExitDisengages(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)

	h.active.Store(false)
	h.deliver(dynamo.connId, 11)
	for _, jumper := range jumpers {
		points := jumper.requireStreams(t, "disengaged jumper")
		h.requireOnLocalChain(points, "disengaged jumper intersect")
		require.False(t, h.o.csjIsJumper(jumper.connId))
	}

	late := h.addPeer("late", 100)
	h.promote(late)
	late.requireStreams(t, "peer promoted after Genesis")
}

// With ChainSync Jumping off every hot peer streams, as before.
func TestChainsyncJumpingDisabledEveryPeerStreams(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, false)
	require.Nil(t, h.o.csj)
	h.fragment.Store(10)
	for i := range 3 {
		peer := h.addPeer(fmt.Sprintf("peer%d", i), 100)
		h.promote(peer)
		peer.requireStreams(t, "peer with CSJ off")
	}
	select {
	case a := <-h.agreements:
		t.Fatalf("unexpected agreement %+v", a)
	default:
	}
}

func TestCsjJumpPoints(t *testing.T) {
	t.Parallel()
	chain := csjChain(100)
	got := csjJumpPoints(chain)
	want := []ocommon.Point{
		csjPoint(100), csjPoint(99), csjPoint(98), csjPoint(96),
		csjPoint(92), csjPoint(84), csjPoint(68), csjPoint(36),
	}
	require.Equal(t, want, got)
	require.Equal(t, []ocommon.Point{csjPoint(1)}, csjJumpPoints(chain[:1]))
	require.Nil(t, csjJumpPoints(nil))
}

// A governor eligibility change cannot hand a jumper an ingress slot: it
// streams nothing, so the slot would only expose it to the stall recycler.
func TestChainsyncJumpingJumperStaysObserver(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	_, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]

	h.o.HandlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: jumper.connId,
			Eligible:     true,
		},
	))
	observer, tracked := h.o.chainsyncState.ClientObservabilityOnly(
		jumper.connId,
	)
	require.True(t, tracked)
	require.True(t, observer, "a jumper must not take an ingress slot")
}

// Jumps name points of the dynamo's candidate fragment, which only an
// ingress stream extends, so an ingress-ineligible peer is never elected
// dynamo: it streams as an observer, and the next eligible identified peer
// becomes the dynamo.
func TestChainsyncJumpingDynamoNeedsIngress(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	var ineligible atomic.Pointer[ouroboros.ConnectionId]
	h.o.config.ChainsyncIngressEligible = func(
		connId ouroboros.ConnectionId,
	) bool {
		id := ineligible.Load()
		return id == nil || *id != connId
	}

	observer := h.addPeer("observer", 100)
	ineligible.Store(&observer.connId)
	h.promote(observer)
	observer.requireStreams(t, "ingress-ineligible peer")

	dynamo := h.addPeer("dynamo", 100)
	h.promote(dynamo)
	dynamo.requireStreams(t, "first eligible peer")

	h.fragment.Store(10)
	jumper := h.addPeer("jumper", 100)
	h.promote(jumper)
	jumper.requireJump(t, "jumper")
	h.requireAgreement(jumper.connId, dynamo.connId, csjPoint(10))
}

// A jumper's agreement stays live while it is a jumper, though the dynamo
// delivers nothing: each refresh jumps it to the dynamo's candidate head
// again, which renews its jump record before the selector's stale-tip
// threshold drops it from the Limit on Eagerness and the witness count, and
// keeps its connection out of the server's StIdle timeout.
func TestChainsyncJumpingRefreshKeepsIdleJumperLive(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.csj.cfg.RefreshInterval = 20 * time.Millisecond
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]

	for i := range 2 {
		points := jumper.requireJump(t, fmt.Sprintf("refresh jump %d", i))
		require.Equal(t, csjPoint(10), points[0],
			"a refresh jumps to the dynamo's candidate head")
		h.requireAgreement(jumper.connId, dynamo.connId, csjPoint(10))
	}
}

// A dynamo parked by the Limit on Eagerness delivers nothing more, so a
// round that a delivery could not start, because the candidate fragment
// still lagged the delivered header, must still happen once the committed
// fragment catches up.
func TestChainsyncJumpingParkedDynamoStillJumps(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.csj.cfg.RefreshInterval = 20 * time.Millisecond
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]

	h.fragment.Store(15)
	h.deliver(dynamo.connId, 20)
	h.awaitRound()
	h.fragment.Store(20)
	for i := 0; ; i++ {
		require.Less(t, i, 50, "no jump reached the dynamo's candidate head")
		points := jumper.requireJump(t, "jump after the dynamo parked")
		agreement := testutil.RequireReceive(
			t,
			h.agreements,
			5*time.Second,
			"agreement after the dynamo parked",
		)
		if pointsEqual(points[0], csjPoint(20)) {
			require.Equal(t, csjPoint(20), agreement.point)
			break
		}
	}
}

// A jumper elected dynamo claims an ingress slot as it is elected, before
// anything else can take the slot its predecessor freed: jumps name points
// of the dynamo's candidate fragment, which only ingress deliveries extend.
// A jumper that is not ingress-eligible is passed over.
func TestChainsyncJumpingPromotedDynamoClaimsIngress(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.chainsyncState = dchainsync.NewStateWithConfig(
		nil,
		nil,
		dchainsync.Config{MaxClients: 1},
	)
	var ineligible atomic.Pointer[ouroboros.ConnectionId]
	h.o.config.ChainsyncIngressEligible = func(
		connId ouroboros.ConnectionId,
	) bool {
		id := ineligible.Load()
		return id == nil || *id != connId
	}
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)
	skipped, next := jumpers[0], jumpers[1]
	if compareConnIds(next.connId, skipped.connId) < 0 {
		skipped, next = next, skipped
	}
	ineligible.Store(&skipped.connId)

	// An unidentified peer is jumped, and so holds no slot.
	other := h.addPeer("unidentified", 100)
	h.unidentified.Store(other.connId, struct{}{})
	h.promote(other)
	other.requireJump(t, "unidentified jumper")
	require.True(t, h.o.csjIsJumper(other.connId))

	h.o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: dynamo.connId},
	))
	require.False(
		t,
		h.o.reconcileChainsyncIngressAdmission(other.connId, true),
		"the freed slot belongs to the new dynamo",
	)
	points := next.requireStreams(t, "new dynamo")
	h.requireOnLocalChain(points, "new dynamo intersect")
	require.False(t, h.o.csjIsJumper(next.connId))
	observer, tracked := h.o.chainsyncState.ClientObservabilityOnly(
		next.connId,
	)
	require.True(t, tracked)
	require.False(t, observer, "the new dynamo holds an ingress slot")
	require.True(t, h.o.csjIsJumper(skipped.connId),
		"an ingress-ineligible jumper is not elected dynamo")
	skipped.spy.requireQuiet(t)
	require.True(t, h.o.csjIsJumper(other.connId),
		"an unidentified jumper is not elected dynamo")
}

// A peer without a corroboration identity can neither vouch nor be vouched
// for, so streaming its headers only downloads them to hold them. While a
// dynamo exists it is jumped instead. It is never elected dynamo, and when no
// identified peer can be, it streams again.
func TestChainsyncJumpingUnidentifiedPeerJumpsButIsNeverDynamo(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, _ := h.startDynamoAndJumpers()
	other := h.addPeer("unidentified", 100)
	h.unidentified.Store(other.connId, struct{}{})
	h.promote(other)
	points := other.requireJump(t, "unidentified jumper")
	require.Equal(t, csjPoint(10), points[0])
	require.True(t, h.o.csjIsJumper(other.connId))

	h.o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: dynamo.connId},
	))
	other.requireStreams(t, "released unidentified jumper")
	require.False(t, h.o.csjIsJumper(other.connId))
	require.False(t, h.o.csjIsDynamo(other.connId),
		"an unidentified peer is never elected dynamo")
}

// A peer without an identity that started streaming before any dynamo
// existed is taken out of its stream once one does: its session is stopped,
// and it is jumped from then on.
func TestChainsyncJumpingRefreshAdoptsUnidentifiedStreamer(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.demotionTimeout = 200 * time.Millisecond
	other := h.addPeer("unidentified", 100)
	h.unidentified.Store(other.connId, struct{}{})
	h.promote(other)
	other.requireStreams(t, "unidentified streamer before any dynamo")

	h.o.csjRefresh()
	require.True(t, h.o.chainsyncState.HasClientConnId(other.connId),
		"without a dynamo there is nothing to jump to")

	dynamo := h.addPeer("dynamo", 100)
	h.promote(dynamo)
	dynamo.requireStreams(t, "dynamo")
	h.fragment.Store(10)
	h.o.csjRefresh()
	testutil.RequireReceive(t, h.stopped, 5*time.Second, "stopped streamer")
	require.Eventually(t, func() bool {
		return h.o.csjIsJumper(other.connId) ||
			!h.o.chainsyncState.HasClientConnId(other.connId)
	}, 5*time.Second, 5*time.Millisecond,
		"the unidentified peer no longer streams outside CSJ")
}

// jumps_sent counts every jump sent, answered or not, and an answer is
// counted as agreed or rejected, never both.
func TestChainsyncJumpingMetricsCountEachJumpOnce(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.csj.metrics = newChainsyncJumping(
		h.o.csj.cfg,
		prometheus.NewRegistry(),
	).metrics
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)
	forked, failing := jumpers[0], jumpers[1]
	metrics := h.o.csj.metrics
	// The harness sees an agreement inside RecordAgreement, before the
	// worker counts it.
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(metrics.sent) == 2 &&
			promtestutil.ToFloat64(metrics.agreed) == 2
	}, 5*time.Second, 5*time.Millisecond, "initial jumps sent and agreed")

	forked.forkBlock.Store(16)
	failing.failIntersect.Store(true)
	h.fragment.Store(20)
	h.deliver(dynamo.connId, 20)
	forked.requireJump(t, "forked jumper jump")
	failing.requireJump(t, "unanswered jump")
	h.requireAgreement(forked.connId, dynamo.connId, csjPoint(16))
	testutil.RequireReceive(t, h.rejections, 5*time.Second, "rejection")
	h.awaitRound()

	require.InDelta(t, 4, promtestutil.ToFloat64(metrics.sent), 0,
		"an unanswered jump was still sent")
	require.InDelta(t, 2, promtestutil.ToFloat64(metrics.agreed), 0,
		"a fork answer is a rejection, not an agreement")
	require.InDelta(t, 1, promtestutil.ToFloat64(metrics.rejected), 0)
}

// A dynamo whose stream restarts below its last header has a new candidate
// fragment that shares no point with the jumpers' records of the old one, so
// the jumpers are jumped as soon as the fragment holds the new stream's
// first header, not a jump interval later.
func TestChainsyncJumpingDynamoRestartJumpsAtOnce(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]

	h.fragment.Store(15)
	h.deliver(dynamo.connId, 15)
	h.awaitRound()
	h.fragment.Store(11)
	h.deliver(dynamo.connId, 12)
	h.awaitRound()
	h.fragment.Store(12)
	h.deliver(dynamo.connId, 12)
	points := jumper.requireJump(t, "jump after the dynamo restarted")
	require.Equal(t, csjPoint(12), points[0],
		"the jump names the restarted stream's head")
	h.requireAgreement(jumper.connId, dynamo.connId, csjPoint(12))
}
