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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/require"
)

// waitForLeiosServeWaiter blocks until a serving wait has registered for the
// fixture's connection. The registry is written from the protocol goroutine
// and read here, so the predicate takes the lock.
func waitForLeiosServeWaiter(t *testing.T, f *chainsyncServerFixture) {
	t.Helper()
	testutil.WaitForCondition(
		t,
		func() bool {
			f.o.leiosServeWaitersMu.Lock()
			defer f.o.leiosServeWaitersMu.Unlock()
			return len(f.o.leiosServeWaiters[f.conn.Id()]) > 0
		},
		5*time.Second,
		"leios serve waiter to register for the connection",
	)
}

// TestLeiosServeWaitReleasedByRealPeerDisconnect is the issue #3514 regression
// test. It runs against the real NtC chainsync server connection the shared
// ouroboros-mock harness builds, and tears that connection down the way a peer
// actually does (Harness.Disconnect closes the driver end of the bearer)
// rather than by closing a channel the test made up.
//
// The configured closure-wait window is an hour, so nothing except the
// disconnect can end the wait inside the assertion budget.
//
// Against the first attempt at this fix -- which bound the wait to
// Protocol.DoneChan() -- this test fails by timing out: the serving callback
// runs inside gouroboros's recvLoop, recvLoop closes recvDoneChan only when it
// returns, and doneChan closes only after that, so DoneChan() cannot close
// while the callback it would release is still running.
func TestLeiosServeWaitReleasedByRealPeerDisconnect(t *testing.T) {
	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: time.Hour,
	})

	certRB := testDijkstraCertRBRaw(t, 80, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa1
	// No closure is ever stored for ebHash, so the serving path parks.
	block := models.Block{Cbor: certRB, Slot: 80, Hash: []byte{0x80}}

	type result struct {
		cbor []byte
		err  error
	}
	results := make(chan result, 1)
	go func() {
		cbor, err := f.o.serveLeiosCertRbWithWait(block, ebHash, f.conn.Id())
		results <- result{cbor: cbor, err: err}
	}()

	// Only disconnect once the wait is actually parked, so the disconnect
	// exercises the release path rather than the already-gone fast path.
	waitForLeiosServeWaiter(t, f)

	require.NoError(t, f.h.Disconnect())

	got := testutil.RequireReceive(
		t,
		results,
		10*time.Second,
		"CertRB closure wait to be released by the peer disconnect",
	)
	require.Error(t, got.err)
	require.ErrorIs(t, got.err, errLeiosClosureUnresolved)
	require.Nil(t, got.cbor)
	require.Contains(t, got.err.Error(), "cancelled")

	// The release must also clear the registry rather than leaking an entry
	// per closed connection.
	f.o.leiosServeWaitersMu.Lock()
	remaining := len(f.o.leiosServeWaiters)
	f.o.leiosServeWaitersMu.Unlock()
	require.Zero(t, remaining)
}

// TestLeiosServeWaitStillBoundedByTimeout keeps the timeout bound honest: a
// connection that stays up must still end the wait at the configured window,
// and report timeout rather than cancelled.
func TestLeiosServeWaitStillBoundedByTimeout(t *testing.T) {
	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: 50 * time.Millisecond,
	})

	certRB := testDijkstraCertRBRaw(t, 81, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa2
	block := models.Block{Cbor: certRB, Slot: 81, Hash: []byte{0x81}}

	got, err := f.o.serveLeiosCertRbWithWait(block, ebHash, f.conn.Id())
	require.Error(t, err)
	require.ErrorIs(t, err, errLeiosClosureUnresolved)
	require.Nil(t, got)
	require.Contains(t, err.Error(), "timeout")
}

// TestLeiosServeWaiterNotRegisteredForClosedConnection covers the race the
// liveness re-check in registerLeiosServeWaiter closes: a connection already
// removed from the manager must not produce a wait that nothing will ever
// release, since its ConnClosedFunc may already have run.
func TestLeiosServeWaiterNotRegisteredForClosedConnection(t *testing.T) {
	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtC, OuroborosConfig{
		EnableLeios:             true,
		LeiosClosureWaitTimeout: time.Hour,
	})
	connId := f.conn.Id()

	// Drop the connection from the manager, then start a wait for it.
	require.NoError(t, f.h.Disconnect())
	testutil.WaitForCondition(
		t,
		func() bool {
			return f.o.connManager.GetConnectionById(connId) == nil
		},
		5*time.Second,
		"connection to be removed from the connection manager",
	)

	certRB := testDijkstraCertRBRaw(t, 82, make([]byte, lcommon.Blake2b256Size))
	var ebHash lcommon.Blake2b256
	ebHash[0] = 0xa3
	block := models.Block{Cbor: certRB, Slot: 82, Hash: []byte{0x82}}

	type result struct {
		cbor []byte
		err  error
	}
	results := make(chan result, 1)
	go func() {
		cbor, err := f.o.serveLeiosCertRbWithWait(block, ebHash, connId)
		results <- result{cbor: cbor, err: err}
	}()

	got := testutil.RequireReceive(
		t,
		results,
		10*time.Second,
		"closure wait to return immediately for an already-closed connection",
	)
	require.Error(t, got.err)
	require.ErrorIs(t, got.err, errLeiosClosureUnresolved)
	require.Nil(t, got.cbor)
	require.Contains(t, got.err.Error(), "cancelled")
}
