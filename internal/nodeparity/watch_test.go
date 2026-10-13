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

package nodeparity

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testChainSyncServer is a small, real ChainSync server -- the same shape
// ouroboros-mock's own example documents as "the kind of chain-sync server
// a downstream consumer (e.g. dingo) writes" -- serving a synthetic chain
// built from ouroboros-mock's own BuildChain fixture. Reusing that fixture
// (rather than hand-rolling fake block bytes) is what keeps this a real
// wire-level exercise of WatchBlocks and not a new local protocol mock:
// every message on the wire is genuine gouroboros server/client machinery
// carrying real, decodable block data, exactly as a real cardano-node or
// dingo connection would.
//
// This is the piece the earlier pass of this test file deliberately left
// out: ouroboros-mock's own chainsync harness only supports the opposite
// direction (a scripted client driving a server under test), not "real
// client under test against a scripted server." Standing up a real
// gouroboros server directly -- rather than adding new local
// network-mocking infrastructure -- is what keeps this within CLAUDE.md's
// "extend the shared library" boundary.
type testChainSyncServer struct {
	mu         sync.Mutex
	chain      csmock.Chain
	cursor     int
	rolledBack bool
	// accepted, if non-nil, receives each accepted connection so a test
	// can force a specific session to drop (by closing it) rather than
	// waiting for one to end on its own.
	accepted chan *ouroboros.Connection
	// release, if non-nil, gates every RequestNext reply: requestNext
	// blocks on it before replying. Watcher.Events coalesces (holds at
	// most one pending event), so a server free to answer every
	// RequestNext as fast as the client asks could send several replies
	// before a test drains the first resulting event, silently merging
	// what should be separate events. A test that cares about an exact
	// event count sends on release once per expected event, keeping
	// exactly one reply in flight at a time.
	release chan struct{}
	// stopped is closed when the test that created this server ends.
	// requestNext selects on it alongside release so a session's final
	// RequestNext call (issued after the test has stopped calling
	// allowNext) unblocks and returns an error instead of leaking the
	// whole per-connection goroutine set (recvLoop, sendLoop, stateLoop,
	// and this callback itself -- recvLoop cannot even check its own
	// shutdown signal until handleMessage, which is blocked waiting for
	// this callback, returns) for the rest of the test binary's run.
	// Verified: without this, a single WatchBlocks test leaks 5 goroutines
	// (confirmed via a goroutine-stack dump).
	stopped chan struct{}
}

// allowNext permits testChainSyncServer's next RequestNext reply to
// proceed. Only meaningful when the server was built with a release gate.
func (s *testChainSyncServer) allowNext(t *testing.T) {
	t.Helper()
	select {
	case s.release <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not consume the release signal in time")
	}
}

// newTestChainSyncServer builds a synthetic chain of blockCount Conway
// blocks via ouroboros-mock's BuildChain. The server always gates its
// replies (see release): every test below calls allowNext once per event
// it expects, so event delivery is deterministic rather than relying on
// the client and test happening to be fast enough to avoid coalescing.
func newTestChainSyncServer(
	t *testing.T,
	blockCount int,
) *testChainSyncServer {
	t.Helper()
	chain, err := csmock.BuildChain(1, common.Blake2b256{}, 0, 20, blockCount)
	require.NoError(t, err)
	s := &testChainSyncServer{
		chain:   chain,
		release: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	t.Cleanup(func() { close(s.stopped) })
	return s
}

// findIntersect always intersects at origin and serves the whole chain from
// the start, which is all a test that only cares about receiving
// RollForward events needs -- WatchBlocks always starts from "current tip"
// in production, but the exact intersect point does not matter here.
func (s *testChainSyncServer) findIntersect(
	_ chainsync.CallbackContext,
	_ []pcommon.Point,
) (pcommon.Point, chainsync.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rolledBack = false
	s.cursor = 0
	return csmock.OriginPoint(), s.chain.Tip(), nil
}

// requestNext follows the real node convention csmock's own example
// documents: the first reply after an intersect is a rollback to the
// intersection point, then each subsequent call rolls one real block
// forward, and once the chain is exhausted the client is parked with
// AwaitReply (matching a real node with no new block to report yet).
func (s *testChainSyncServer) requestNext(
	ctx chainsync.CallbackContext,
) error {
	select {
	case <-s.release:
	case <-s.stopped:
		return errors.New("test server stopped")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.rolledBack {
		s.rolledBack = true
		return ctx.Server.RollBackward(csmock.OriginPoint(), s.chain.Tip())
	}
	if s.cursor >= s.chain.Len() {
		return ctx.Server.AwaitReply()
	}
	block := s.chain.Blocks[s.cursor]
	tip := s.chain.Tips[s.cursor]
	s.cursor++
	return ctx.Server.RollForward(uint(block.Type()), block.Cbor(), tip)
}

// serve accepts connections on listener until it closes, completing a real
// NtC handshake on each and serving ChainSync from s. Each accepted
// connection is handled in its own goroutine and torn down when the
// connection's error channel fires, so this never needs its own explicit
// stop signal beyond closing listener.
func (s *testChainSyncServer) serve(listener net.Listener, magic uint32) {
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				oconn, err := ouroboros.New(
					ouroboros.WithConnection(conn),
					ouroboros.WithServer(true),
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithChainSyncConfig(chainsync.NewConfig(
						chainsync.WithFindIntersectFunc(s.findIntersect),
						chainsync.WithRequestNextFunc(s.requestNext),
					)),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				defer oconn.Close() //nolint:errcheck
				if s.accepted != nil {
					s.accepted <- oconn
				}
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// newConnectedTestWatcher starts a test ChainSync server serving blockCount
// real blocks and a real WatchBlocks pointed at it, both cleaned up
// automatically. Every test below calls server.allowNext(t) once per event
// it expects, then drains that event: the server's first reply to any
// client is always the RollBackward-to-intersection-point every real node
// sends before rolling forward, so that first event is expected noise, not
// the thing under test.
func newConnectedTestWatcher(
	t *testing.T, blockCount int,
) (*Watcher, *testChainSyncServer) {
	t.Helper()
	const magic = 42

	server := newTestChainSyncServer(t, blockCount)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(listener, magic)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := WatchBlocks(ctx, listener.Addr().String(), magic, nil)
	t.Cleanup(w.Close)
	return w, server
}

// requireEvent drains one event from w.Events within timeout, failing the
// test with msg if none arrives.
func requireEvent(t *testing.T, w *Watcher, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-w.Events:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// requireNextEvent releases exactly one gated server reply and then drains
// the resulting event, so a test asserting on a precise sequence or count
// of events keeps only one reply in flight at a time -- see
// testChainSyncServer.release.
func requireNextEvent(
	t *testing.T,
	w *Watcher,
	server *testChainSyncServer,
	timeout time.Duration,
	msg string,
) {
	t.Helper()
	server.allowNext(t)
	requireEvent(t, w, timeout, msg)
}

// TestWatchBlocks_ConnectsAndReceivesInitialEvent is the end-to-end
// counterpart to the reconnect/coalescing unit tests above: it points the
// real WatchBlocks (completely unmodified production code, dialing a real
// TCP address) at a real gouroboros ChainSync server, and asserts that the
// wire-level path between them -- dial, real NtC handshake, FindIntersect,
// and Sync -- actually works end to end, which no other test in this
// package exercises. This only proves *some* event arrives after
// connecting, not specifically which message produced it: gouroboros's
// client keeps advancing through the server's whole scripted sequence on
// its own, so even if the very first reply's callback were broken, a
// later real RollForward in the same sequence would still satisfy this
// check within the timeout. RollForward specifically is isolated by the
// tests below, which drain events in order rather than accepting the
// first one that arrives.
func TestWatchBlocks_ConnectsAndReceivesAnEvent(t *testing.T) {
	t.Parallel()

	w, server := newConnectedTestWatcher(t, 5)
	requireNextEvent(
		t, w, server, 5*time.Second,
		"WatchBlocks must deliver at least one BlockEvent after connecting",
	)
}

// TestWatchBlocks_ReceivesRealRollForwardEvents isolates RollForward
// specifically, which TestWatchBlocks_ConnectsAndReceivesAnEvent does not:
// draining two events in order guarantees the second one is a genuine
// RollForward, since the server's script sends exactly one RollBackward
// (always first) and then only RollForwards -- there is no second
// RollBackward it could be instead. Verified adversarially: this test
// fails (times out on the second event) if the RollForward callback's
// notify is removed, while ConnectsAndReceivesAnEvent alone would not
// have caught that (it accepts whichever event arrives first, and the
// sync loop advances past a broken callback into later real
// RollForwards on its own).
func TestWatchBlocks_ReceivesRealRollForwardEvents(t *testing.T) {
	t.Parallel()

	w, server := newConnectedTestWatcher(t, 5)
	requireNextEvent(
		t, w, server, 5*time.Second,
		"must receive the initial RollBackward event",
	)
	requireNextEvent(
		t, w, server, 5*time.Second,
		"WatchBlocks must deliver a BlockEvent for a real RollForward",
	)
}

// TestWatchBlocks_ReceivesMultipleRealEvents extends the single-RollForward
// case across several real blocks: with the consumer draining Events
// between each, WatchBlocks must keep delivering fresh events for every
// new block rather than the channel latching stuck after the first one.
func TestWatchBlocks_ReceivesMultipleRealEvents(t *testing.T) {
	t.Parallel()

	w, server := newConnectedTestWatcher(t, 5)
	// 1 initial RollBackward + 3 real RollForwards.
	for i := range 4 {
		requireNextEvent(
			t, w, server, 5*time.Second,
			fmt.Sprintf("only received %d/4 events before timing out", i),
		)
	}
}

// gatedListener holds its port for the whole test, so no parallel test can
// bind it. Until open is called, Accept closes each connection instead of
// returning it, which makes a dial fail fast at the handshake. Releasing a
// port to simulate an absent server is not safe: another parallel test's
// listener can take it, and a real session to that server ends when that
// test does.
type gatedListener struct {
	net.Listener
	opened   chan struct{}
	openOnce sync.Once
}

func newGatedListener(l net.Listener) *gatedListener {
	return &gatedListener{Listener: l, opened: make(chan struct{})}
}

// newTestGatedListener binds a closed gatedListener on an OS-assigned
// loopback port and closes it when the test ends. Leave it unopened to
// stand in for a node that refuses every session.
func newTestGatedListener(t *testing.T) *gatedListener {
	t.Helper()
	base, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gate := newGatedListener(base)
	t.Cleanup(func() { _ = gate.Close() })
	return gate
}

// refusingAddr returns the address of a gatedListener that is never opened.
// Something must call Accept for the gate to drop a connection; without it
// the kernel completes the TCP handshake and the dial stalls in the
// Ouroboros handshake until dialTimeout.
func refusingAddr(t *testing.T) string {
	t.Helper()
	gate := newTestGatedListener(t)
	go func() { _, _ = gate.Accept() }()
	return gate.Addr().String()
}

func (g *gatedListener) open() {
	g.openOnce.Do(func() { close(g.opened) })
}

func (g *gatedListener) Accept() (net.Conn, error) {
	for {
		conn, err := g.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case <-g.opened:
			return conn, nil
		default:
			_ = conn.Close()
		}
	}
}

// TestGatedListener_DropsConnectionsUntilOpened pins the gate the reconnect
// test relies on: connections made before open are closed without being
// handed to the caller, and connections made after it are.
func TestGatedListener_DropsConnectionsUntilOpened(t *testing.T) {
	t.Parallel()

	gate := newTestGatedListener(t)

	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, acceptErr := gate.Accept(); acceptErr == nil {
			accepted <- conn
		}
	}()

	dropped, err := net.Dial("tcp", gate.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropped.Close() })
	require.NoError(t, dropped.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = dropped.Read(make([]byte, 1))
	require.Error(t, err)
	var netErr net.Error
	require.False(
		t, errors.As(err, &netErr) && netErr.Timeout(),
		"a connection made before open must be closed by the listener",
	)
	select {
	case <-accepted:
		t.Fatal("Accept handed out a connection made before open")
	default:
	}

	gate.open()
	served, err := net.Dial("tcp", gate.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = served.Close() })
	select {
	case conn := <-accepted:
		require.NoError(t, conn.Close())
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not hand out a connection made after open")
	}
}

// TestWatchBlocks_ReconnectsQuicklyAfterEstablishedSessionDrops is an
// end-to-end regression test for a backoff-ordering bug: followBlocks used
// to reset the backoff to watcherMinBackoff only *after* waiting out
// whatever it had already grown to from earlier failures, so a session
// that established and then dropped still waited out a stale, grown delay
// once before the reset took effect. It should instead reconnect quickly,
// using watcherMinBackoff for that one wait.
//
// This is verified by parsing the logged "reconnecting in %s" duration
// directly, rather than measuring real elapsed time: the log line is
// written with the exact backoff value about to be used, so this is a
// deterministic check of the same property, not a timing-sensitive one.
func TestWatchBlocks_ReconnectsQuicklyAfterEstablishedSessionDrops(
	t *testing.T,
) {
	t.Parallel()

	const magic = 42

	// The gated listener drops every connection until it is opened, so each
	// early attempt fails fast without the port ever being released.
	listener := newTestGatedListener(t)
	addr := listener.Addr().String()
	server := newTestChainSyncServer(t, 5)
	server.accepted = make(chan *ouroboros.Connection, 1)
	server.serve(listener, magic)

	var mu sync.Mutex
	var logs []string
	logf := func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	logCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(logs)
	}
	lastLog := func() string {
		mu.Lock()
		defer mu.Unlock()
		return logs[len(logs)-1]
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := WatchBlocks(ctx, addr, magic, logf)
	t.Cleanup(w.Close)

	// Let several dial attempts fail first (the gate is closed), so the
	// watcher's backoff grows well past watcherMinBackoff before the
	// server ever answers.
	testutil.WaitForCondition(t, func() bool {
		return logCount() >= 4
	}, 5*time.Second, "watcher must log several failed attempts before the server starts accepting")
	require.NotContains(
		t, lastLog(), watcherMinBackoff.String(),
		"precondition: backoff must have grown past the minimum by now",
	)

	// Now let the server answer, and let the watcher establish.
	listener.open()

	requireNextEvent(
		t, w, server, 10*time.Second,
		"watcher must eventually connect once the server accepts",
	)

	// Force this specific session to end, and capture the log count so we
	// can identify the *next* one below.
	before := logCount()
	select {
	case conn := <-server.accepted:
		require.NoError(t, conn.Close())
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the accepted connection")
	}

	testutil.WaitForCondition(t, func() bool {
		return logCount() > before
	}, 5*time.Second, "watcher must log the dropped, established session")

	assert.Contains(
		t, lastLog(), watcherMinBackoff.String(),
		"an established session that drops must reconnect using "+
			"watcherMinBackoff, not a backoff grown from earlier failures",
	)
}

// TestNextBackoff_DoublesUntilCap covers the reconnect delay's growth for a
// session that never gets established (a node that will not talk to us at
// all): each failure must double the previous delay, and the delay must
// never exceed watcherMaxBackoff no matter how many failures accumulate.
func TestNextBackoff_DoublesUntilCap(t *testing.T) {
	t.Parallel()

	backoff := watcherMinBackoff
	seen := []time.Duration{backoff}
	for range 10 {
		backoff = nextBackoff(backoff, false)
		seen = append(seen, backoff)
	}
	for i := 1; i < len(seen); i++ {
		prev, cur := seen[i-1], seen[i]
		wantDoubled := min(2*prev, watcherMaxBackoff)
		assert.Equal(t, wantDoubled, cur, "step %d", i)
		assert.LessOrEqual(t, cur, watcherMaxBackoff)
	}
	assert.Equal(
		t, watcherMaxBackoff, seen[len(seen)-1],
		"backoff must have reached the cap well within 10 doublings",
	)
}

// TestNextBackoff_ResetsOnEstablished covers the other half of the
// reconnect policy: a session that got as far as following the chain and
// then dropped resets to the minimum delay on its next attempt, regardless
// of how large the backoff had grown -- that failure mode looks like a
// node restart, not a node that refuses to talk to us, so it should be
// retried quickly.
func TestNextBackoff_ResetsOnEstablished(t *testing.T) {
	t.Parallel()

	grown := nextBackoff(
		nextBackoff(nextBackoff(watcherMinBackoff, false), false),
		false,
	)
	require.Greater(
		t,
		grown,
		watcherMinBackoff,
		"precondition: backoff must have grown",
	)

	reset := nextBackoff(grown, true)
	assert.Equal(t, watcherMinBackoff, reset)
}

// TestBlockEventSignal_CoalescesBursts covers the coalescing behavior
// newBlockEventSignal exists for: several notify calls in a row, with
// nothing draining the channel in between, must leave exactly one pending
// event -- not block, not panic, and not queue up a backlog that would
// make a slow consumer process stale bursts one at a time.
func TestBlockEventSignal_CoalescesBursts(t *testing.T) {
	t.Parallel()

	events, notify := newBlockEventSignal()
	for range 5 {
		notify()
	}
	assert.Len(
		t,
		events,
		1,
		"a burst of notifies must coalesce to one pending event",
	)
}

// TestBlockEventSignal_DeliversAgainAfterDrain covers the other half of
// the coalescing contract: coalescing must not turn into permanently
// dropping events. Once a caller drains the pending event, the next
// notify must deliver a fresh one.
func TestBlockEventSignal_DeliversAgainAfterDrain(t *testing.T) {
	t.Parallel()

	events, notify := newBlockEventSignal()
	notify()
	<-events // drain
	assert.Empty(t, events, "channel must be empty right after drain")

	notify()
	assert.Len(t, events, 1, "a notify after drain must deliver a new event")
}

// TestBlockEventSignal_NotifyNeverBlocks covers the property that makes
// this safe to call from a ChainSync callback: notify must never block the
// caller, even under a sustained flood with nobody ever reading Events.
// A ChainSync callback that blocked here would stall the whole protocol
// session, not just this watcher.
func TestBlockEventSignal_NotifyNeverBlocks(t *testing.T) {
	t.Parallel()

	_, notify := newBlockEventSignal()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			notify()
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("notify blocked under a flood with no reader draining Events")
	}
}

// TestWatchBlocks_CloseStopsPromptly covers Watcher lifecycle management
// against a node that will never accept a session: WatchBlocks starts a
// background reconnect loop immediately, and Close must cancel it and wait
// for that goroutine to actually exit, rather than returning while it is
// still running (which would leak the goroutine) or hanging forever
// waiting on a connection that will never succeed.
func TestWatchBlocks_CloseStopsPromptly(t *testing.T) {
	t.Parallel()

	addr := refusingAddr(t)
	w := WatchBlocks(context.Background(), addr, 2, nil)

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Watcher.Close did not return promptly against an unreachable address",
		)
	}
}

// TestWatchBlocks_CloseStopsPromptlyAgainstUnresponsivePeer covers a peer
// that completes the real NtC handshake and then stalls before replying to
// the first ChainSync request (GetCurrentTip's FindIntersect) -- a dead or
// hung node, as opposed to TestWatchBlocks_CloseStopsPromptly's "nothing is
// listening at all" case, and distinct from a peer that never completes
// the handshake at all (that phase is bounded by dialTimeout, not this
// path -- see TestDial_HandshakeStallIsBoundedByDialTimeout's watch.go
// counterpart, watchSession's own dialCtx-scoped closer). GetCurrentTip and
// Sync are synchronous protocol calls with no per-call timeout of their
// own: each blocks until the peer replies or the connection is closed out
// from under it. Without watchSession closing the connection the instant
// its context is cancelled, Close would block for as long as the peer
// keeps the socket open (in production, indefinitely), rather than
// returning promptly. Using a fake peer that never completes the
// handshake at all would only exercise the earlier dial-time closer, not
// this one -- a regression that removed this later closer would still
// pass such a test.
func TestWatchBlocks_CloseStopsPromptlyAgainstUnresponsivePeer(t *testing.T) {
	t.Parallel()

	const magic = 42
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	findIntersectCalled := make(chan struct{})
	stall := make(chan struct{}) // never closed: FindIntersect never replies
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		oconn, err := ouroboros.New(
			ouroboros.WithConnection(conn),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(magic),
			ouroboros.WithChainSyncConfig(chainsync.NewConfig(
				chainsync.WithFindIntersectFunc(
					func(
						_ chainsync.CallbackContext, _ []pcommon.Point,
					) (pcommon.Point, chainsync.Tip, error) {
						close(findIntersectCalled)
						<-stall
						return pcommon.Point{}, chainsync.Tip{}, nil
					},
				),
			)),
		)
		if err != nil {
			return
		}
		defer oconn.Close() //nolint:errcheck
		<-oconn.ErrorChan()
	}()

	w := WatchBlocks(context.Background(), listener.Addr().String(), magic, nil)

	// Wait for GetCurrentTip's FindIntersect to actually reach the server,
	// not just for the handshake to finish: cancelling immediately after
	// the handshake races the dial-time closer (stopDialCancel, scoped to
	// dialCtx) against ouroboros.New's own return, since dialCtx is a
	// child of this same ctx -- which can tear down the raw connection
	// through that earlier closer instead of the later one (stopOnCancel)
	// this test exists to cover, making the test pass even with
	// stopOnCancel removed. Waiting for FindIntersect guarantees
	// stopDialCancel has already deregistered by the time Close runs.
	select {
	case <-findIntersectCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("server never observed the watcher's GetCurrentTip request")
	}

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Watcher.Close did not return promptly against a peer that " +
				"completed the handshake and then never replied to GetCurrentTip",
		)
	}
}

// TestWatchBlocks_RetriesOnUnreachableAddr covers the actual retry
// behavior end to end (short of a real ChainSync server, which would
// require new shared mock infrastructure this package does not add
// locally): pointed at a listener that drops every connection, the watcher
// must keep attempting to reconnect on its own, logging each attempt,
// rather than giving up after the first failure.
func TestWatchBlocks_RetriesOnUnreachableAddr(t *testing.T) {
	t.Parallel()

	addr := refusingAddr(t)

	var mu sync.Mutex
	attempts := 0
	logf := func(string, ...any) {
		mu.Lock()
		attempts++
		mu.Unlock()
	}

	w := WatchBlocks(context.Background(), addr, 2, logf)
	defer w.Close()

	testutil.WaitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 2
	}, 3*time.Second, "watcher must keep retrying a connection that never succeeds")
}

// TestWatchBlocks_StalledHandshakeTriggersReconnectWithinDialTimeout covers
// a peer that accepts the connection and then never completes the NtC
// handshake, with no external cancellation (matching a real Watcher, whose
// ctx normally only cancels on process shutdown). watchSession's own
// dialCtx-scoped closer must still bound this phase by dialTimeout, the
// same as Dial's identical fix (TestDial_HandshakeStallIsBoundedByDialTimeout):
// regression test for a bug where this closer was registered against the
// long-lived watcher ctx instead, leaving a stalled handshake unbounded
// except by the watcher's own eventual shutdown.
func TestWatchBlocks_StalledHandshakeTriggersReconnectWithinDialTimeout(
	t *testing.T,
) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		// Accept and hold the connection open without ever writing to it,
		// so ouroboros.New blocks in the handshake indefinitely unless
		// watchSession's dialCtx-scoped closer bounds it.
		t.Cleanup(func() { _ = conn.Close() })
	}()

	var mu sync.Mutex
	var logs []string
	logf := func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}

	w := WatchBlocks(context.Background(), listener.Addr().String(), 42, logf)
	defer w.Close()

	testutil.WaitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(logs) >= 1
	}, dialTimeout+5*time.Second, "watcher must log a reconnect attempt within dialTimeout+margin against a stalled handshake, with no external cancellation")
}
