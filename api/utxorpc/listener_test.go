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

package utxorpc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// The shutdown protocol this test exercises is covered in depth, with the
// windows constructed rather than raced, in internal/apilistener. What is
// checked here is that this package is wired to it -- that a utxorpc server
// keeps the promise its Stop makes, including on the force-close escalation
// paths that are this listener's own.

// TestServerRebindsAfterStop is the production path this fix exists for: a
// live database restore or truncate quiesces the API capabilities and
// reinitializeAPIServers brings them back up on the same configured port (see
// node_lifecycle.go). A Stop that returned while the socket was still bound
// left that restart failing with EADDRINUSE. The constructed tests in
// internal/apilistener assert closure on the original listener object; dialing
// a released ephemeral address here could instead reach another package's
// listener when the suite runs concurrently.
func TestServerRebindsAfterStop(t *testing.T) {
	u, addr := startOnFreePort(
		t, t.Context(),
		apiconfig.EffectiveTLS{},
	)
	stopUtxorpc(t, u)

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	portNum, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	restarted := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     host,
		Port:     uint(portNum),
	})
	require.NoError(
		t, restarted.Start(t.Context()),
		"a capability restart must rebind the port Stop released",
	)
	stopUtxorpc(t, restarted)
}

// TestStartIsRefusedWhileAnotherStartHoldsTheGate pins the start gate this
// package is wired to. Nothing else in the package observes it -- the
// already-published rejection comes from Publish -- so without this test the
// BeginStart/EndStart pair can be removed from Start with the suite green.
func TestStartIsRefusedWhileAnotherStartHoldsTheGate(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: bus,
		Host:     "127.0.0.1",
	})
	u.config.Port = 0

	held, err := u.listener.BeginStart()
	require.NoError(t, err)

	err = u.Start(t.Context())
	require.ErrorContains(
		t, err, "start already in progress",
		"Start must take the listener's start gate before publishing",
	)
	require.Nil(
		t, u.listener.Server(),
		"a refused Start must not publish a server",
	)

	u.listener.EndStart(held)
	require.NoError(
		t, u.Start(t.Context()),
		"the gate must be available again once the holder releases it",
	)
	stopUtxorpc(t, u)
}

// TestServerShutdownOnContextCancel asserts cancelling the context passed to
// Start releases the port, which is how the node stops this API during its own
// shutdown. TestUtxorpc_StopObservesCtxCancellation covers the Stop context,
// not this one, so without this test the listener.Watch call can be removed
// from Start with the suite green.
func TestServerShutdownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, addr := startOnFreePort(t, ctx, apiconfig.EffectiveTLS{})

	cancel()

	testutil.WaitForCondition(
		t,
		func() bool { return !portAccepts(addr) },
		5*time.Second,
		"listener still accepting after context cancel",
	)
}

// portAccepts reports whether a TCP connection to addr succeeds.
func portAccepts(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
