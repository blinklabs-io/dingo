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
}

// newTestChainSyncServer builds a synthetic chain of blockCount Conway
// blocks via ouroboros-mock's BuildChain.
func newTestChainSyncServer(
	t *testing.T,
	blockCount int,
) *testChainSyncServer {
	t.Helper()
	chain, err := csmock.BuildChain(1, common.Blake2b256{}, 0, 20, blockCount)
	require.NoError(t, err)
	return &testChainSyncServer{chain: chain}
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
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// newConnectedTestWatcher starts a test ChainSync server serving blockCount
// real blocks and a real WatchBlocks pointed at it, both cleaned up
// automatically. Every test below drains the first event before asserting
// anything further: the server's first reply to any client is always the
// RollBackward-to-intersection-point every real node sends before rolling
// forward, so that event is expected noise, not the thing under test.
func newConnectedTestWatcher(
	t *testing.T, blockCount int,
) *Watcher {
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
	return w
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
	w := newConnectedTestWatcher(t, 5)
	requireEvent(
		t, w, 5*time.Second,
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
	w := newConnectedTestWatcher(t, 5)
	requireEvent(
		t,
		w,
		5*time.Second,
		"must receive the initial RollBackward event",
	)
	requireEvent(
		t, w, 5*time.Second,
		"WatchBlocks must deliver a BlockEvent for a real RollForward",
	)
}

// TestWatchBlocks_ReceivesMultipleRealEvents extends the single-RollForward
// case across several real blocks: with the consumer draining Events
// between each, WatchBlocks must keep delivering fresh events for every
// new block rather than the channel latching stuck after the first one.
func TestWatchBlocks_ReceivesMultipleRealEvents(t *testing.T) {
	w := newConnectedTestWatcher(t, 5)
	// 1 initial RollBackward + 3 real RollForwards.
	for i := range 4 {
		requireEvent(
			t, w, 5*time.Second,
			fmt.Sprintf("only received %d/4 events before timing out", i),
		)
	}
}
