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
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
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
		apiconfig.EffectiveTLS{}, apiconfig.EffectiveAuth{},
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
