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
	"github.com/stretchr/testify/require"
)

// The shutdown protocol these two tests exercise is covered in depth, with the
// windows constructed rather than raced, in internal/apilistener. What is
// checked here is that this package is wired to it -- that a utxorpc server
// keeps the promise its Stop makes, including on the force-close escalation
// paths that are this listener's own.

// stopNow shuts u down under a bounded context, matching the package's existing
// convention (stopUtxorpc in tls_auth_test.go): a hang in the teardown path
// should fail the test rather than stall the suite until go test's own timeout.
func stopNow(t *testing.T, u *Utxorpc) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	return u.Stop(ctx)
}

// TestServerStopReleasesPortBeforeServeRegisters covers the window between
// net.Listen and Serve registering that listener with the http.Server.
// http.Server.Shutdown closes only registered listeners, so a Stop landing
// inside the window used to return with the port still bound.
func TestServerStopReleasesPortBeforeServeRegisters(t *testing.T) {
	for i := range 100 {
		u, addr := startOnFreePort(
			t, t.Context(),
			apiconfig.EffectiveTLS{}, apiconfig.EffectiveAuth{},
		)

		require.NoError(t, stopNow(t, u))

		require.False(
			t, portAccepts(addr),
			"listener still accepting when Stop returned "+
				"(iteration %d)", i,
		)
	}
}

// TestServerRebindsAfterStop is the production path this fix exists for: a
// live database restore or truncate quiesces the API capabilities and
// reinitializeAPIServers brings them back up on the same configured port (see
// node_lifecycle.go). A Stop that returned while the socket was still bound
// left that restart failing with EADDRINUSE.
func TestServerRebindsAfterStop(t *testing.T) {
	u, addr := startOnFreePort(
		t, t.Context(),
		apiconfig.EffectiveTLS{}, apiconfig.EffectiveAuth{},
	)
	require.NoError(t, stopNow(t, u))

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
	require.NoError(t, stopNow(t, restarted))
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
