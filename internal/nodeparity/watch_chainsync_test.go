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

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
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
	w, server := newConnectedTestWatcher(t, 5)
	// 1 initial RollBackward + 3 real RollForwards.
	for i := range 4 {
		requireNextEvent(
			t, w, server, 5*time.Second,
			fmt.Sprintf("only received %d/4 events before timing out", i),
		)
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
func TestWatchBlocks_ReconnectsQuicklyAfterEstablishedSessionDrops(t *testing.T) {
	const magic = 42

	// Reserve an address nothing is listening on yet (see unreachableAddr):
	// a listener that exists but never calls Accept would still complete
	// the TCP handshake via the kernel's backlog and then hang waiting for
	// the Ouroboros handshake response that never comes, which fails slow
	// rather than fast. Closing the listener first guarantees a real,
	// fast ECONNREFUSED for every attempt until the address is rebound
	// below.
	addr := unreachableAddr(t)

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

	// Let several dial attempts fail first (nobody is accepting yet), so
	// the watcher's backoff grows well past watcherMinBackoff before the
	// server ever answers.
	testutil.WaitForCondition(t, func() bool {
		return logCount() >= 4
	}, 5*time.Second, "watcher must log several failed attempts before the server starts accepting")
	require.NotContains(
		t, lastLog(), watcherMinBackoff.String(),
		"precondition: backoff must have grown past the minimum by now",
	)

	// Now bind the same address for real and let the watcher establish.
	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server := newTestChainSyncServer(t, 5)
	server.accepted = make(chan *ouroboros.Connection, 1)
	server.serve(listener, magic)

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
