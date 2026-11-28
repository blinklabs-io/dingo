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

package dingo

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

const peerDisconnectTestBound = 2 * time.Second

func startPeerDisconnectWorkerForTest(t *testing.T, n *Node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n.startPeerDisconnectWorker(ctx)
	t.Cleanup(func() {
		cancel()
		n.waitPeerDisconnectWorker()
	})
}

// drainPeerDisconnects waits until the worker has run every disconnect queued
// before the call.
func drainPeerDisconnects(t *testing.T, n *Node) {
	t.Helper()
	done := make(chan struct{})
	n.queuePeerDisconnect("test drain", func() func() {
		close(done)
		return nil
	})
	testutil.RequireReceive(t, done, 5*time.Second, "queued disconnects to run")
}

// requireReturnsOrDump fails with every goroutine's stack when done is not
// closed within the bound, so a deadlock shows who waits for whom.
func requireReturnsOrDump(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(peerDisconnectTestBound):
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf(
			"%s did not return within %s:\n%s",
			what,
			peerDisconnectTestBound,
			buf,
		)
	}
}

// A Restore or Truncate holds networkingCoreMu while stopping the connection
// manager, and that Stop waits for every running callback of the connections
// it closes. A chain-sync callback that reports a historic peer must
// therefore return without waiting for networkingCoreMu; the disconnect runs
// once the lock is free.
func TestPeerDisconnectHookDoesNotWaitForNetworkingCoreHolder(t *testing.T) {
	t.Parallel()
	n, _ := newMetricsTestNode(t)
	closed := make(chan ouroboros.ConnectionId, 1)
	n.closePeerConnOverride = func(id ouroboros.ConnectionId, _ string) {
		closed <- id
	}
	startPeerDisconnectWorkerForTest(t, n)
	connID := newNodeTestConnId(3401)

	n.networkingCoreMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			n.networkingCoreMu.Unlock()
		}
	})
	callbackDone := make(chan struct{})
	go func() {
		defer close(callbackDone)
		n.onHistoricityDisconnect(chainselection.HistoricityDisconnect{
			ConnectionId: connID,
		})
	}()
	requireReturnsOrDump(
		t,
		callbackDone,
		"callback reporting a historic peer while networkingCoreMu is held",
	)
	testutil.RequireNoReceive(
		t,
		closed,
		50*time.Millisecond,
		"disconnect ran while networkingCoreMu was held",
	)

	locked = false
	n.networkingCoreMu.Unlock()
	got := testutil.RequireReceive(t, closed, 5*time.Second, "peer close")
	require.Equal(t, connID, got)
}

// The historicity hook usually reports the connection whose own callback ran
// it. A Close that waits for the connection's running callbacks must then not
// be made from that callback, or it waits for itself.
func TestPeerDisconnectHookDoesNotCloseFromReportingCallback(t *testing.T) {
	t.Parallel()
	n, _ := newMetricsTestNode(t)
	testCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	callbackDone := make(chan struct{})
	closeDone := make(chan struct{})
	n.closePeerConnOverride = func(ouroboros.ConnectionId, string) {
		// A Close that joins the connection's callbacks returns only once
		// the reporting callback has.
		select {
		case <-callbackDone:
			close(closeDone)
		case <-testCtx.Done():
		}
	}
	startPeerDisconnectWorkerForTest(t, n)

	go func() {
		defer close(callbackDone)
		n.onHistoricityDisconnect(chainselection.HistoricityDisconnect{
			ConnectionId: newNodeTestConnId(3402),
		})
	}()
	requireReturnsOrDump(
		t,
		callbackDone,
		"callback reporting its own connection as historic",
	)
	requireReturnsOrDump(t, closeDone, "close of the reported connection")
}

// A density report from one connection's callback closes another
// connection; that Close waits for the other connection's callback, which
// can be reporting a peer of its own at the same moment.
func TestPeerDisconnectHooksDoNotWaitForEachOther(t *testing.T) {
	t.Parallel()
	n, _ := newMetricsTestNode(t)
	testCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	connA := newNodeTestConnId(3403)
	connB := newNodeTestConnId(3404)
	callbackDone := map[ouroboros.ConnectionId]chan struct{}{
		connA: make(chan struct{}),
		connB: make(chan struct{}),
	}
	bothInHook := make(chan struct{})
	closedB := make(chan struct{})
	n.closePeerConnOverride = func(id ouroboros.ConnectionId, _ string) {
		select {
		case <-callbackDone[id]:
			if id == connB {
				close(closedB)
			}
		case <-testCtx.Done():
		}
	}
	startPeerDisconnectWorkerForTest(t, n)

	// B's callback has entered its own report before A's report runs.
	go func() {
		defer close(callbackDone[connB])
		<-bothInHook
		n.onGenesisDensityDisconnect(chainselection.GenesisDensityDisconnect{
			ConnectionId: connA,
		})
	}()
	go func() {
		defer close(callbackDone[connA])
		close(bothInHook)
		n.onGenesisDensityDisconnect(chainselection.GenesisDensityDisconnect{
			ConnectionId: connB,
		})
	}()
	requireReturnsOrDump(t, callbackDone[connA], "callback of A")
	requireReturnsOrDump(t, callbackDone[connB], "callback of B")
	requireReturnsOrDump(t, closedB, "close of B")
}
