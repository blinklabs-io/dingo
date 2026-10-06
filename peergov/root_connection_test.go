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

package peergov

import (
	"io"
	"log/slog"
	"net"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
)

func TestIsConfiguredRootConnection(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	connId := func(host string) ouroboros.ConnectionId {
		return ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 3001},
			RemoteAddr: &net.TCPAddr{
				IP:   net.ParseIP(host),
				Port: 3001,
			},
		}
	}
	local, public := connId("10.1.0.1"), connId("10.1.0.2")
	gossip, inbound := connId("10.1.0.3"), connId("10.1.0.4")
	disconnected := connId("10.1.0.5")
	responderOnly := connId("10.1.0.6")
	pg.mu.Lock()
	pg.peers = []*Peer{
		{
			Source:     PeerSourceTopologyLocalRoot,
			Connection: &PeerConnection{Id: local, IsClient: true},
		},
		{
			Source:     PeerSourceTopologyPublicRoot,
			Connection: &PeerConnection{Id: public, IsClient: true},
		},
		{
			Source:     PeerSourceP2PGossip,
			Connection: &PeerConnection{Id: gossip, IsClient: true},
		},
		{
			Source:     PeerSourceInboundConn,
			Connection: &PeerConnection{Id: inbound, IsClient: true},
		},
		{
			Source:     PeerSourceTopologyLocalRoot,
			Connection: &PeerConnection{Id: responderOnly},
		},
	}
	pg.mu.Unlock()

	assert.True(t, pg.IsConfiguredRootConnection(local))
	assert.True(t, pg.IsConfiguredRootConnection(public))
	assert.False(t, pg.IsConfiguredRootConnection(gossip))
	assert.False(t, pg.IsConfiguredRootConnection(inbound))
	assert.False(t, pg.IsConfiguredRootConnection(disconnected))
	assert.False(
		t,
		pg.IsConfiguredRootConnection(responderOnly),
		"a root connection that cannot act as a client is not eligible",
	)
}
