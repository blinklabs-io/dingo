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
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A peer list at its hard cap rejects a new inbound peer. The connection is
// already registered with the connection manager, so rejecting the record
// without closing the connection would hold a global inbound slot and a
// per-IP slot that governance has no record to prune.
//
// The wait must stay well below the muxer's 120s segment-read timeout: the
// test client is idle, so the server closes the connection on its own at
// that point and a longer wait would pass without the fix.
func TestInboundArrivalAtPeerCapReleasesConnectionSlots(t *testing.T) {
	const rejectedCloseWait = 30 * time.Second
	t.Parallel()
	const magic = 764824073
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	_, inboundCh := bus.Subscribe(connmanager.InboundConnectionEventType)
	connMgr := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:       logger,
			PromRegistry: prometheus.NewRegistry(),
			EventBus:     bus,
			Listeners: []connmanager.ListenerConfig{{
				Listener: ln,
				ConnectionOpts: []ouroboros.ConnectionOptionFunc{
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithNodeToNode(true),
					ouroboros.WithServer(true),
					ouroboros.WithFullDuplex(true),
				},
			}},
		},
	)
	require.NoError(t, connMgr.Start(context.Background()))
	t.Cleanup(func() {
		_ = connMgr.Stop(context.Background())
	})

	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	client, err := ouroboros.New(
		ouroboros.WithConnection(raw),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithFullDuplex(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	arrival := testutil.RequireReceive(
		t, inboundCh, testutil.AsyncWait, "inbound connection event",
	)
	data, ok := arrival.Data.(connmanager.InboundConnectionEvent)
	require.True(t, ok)
	require.NotNil(t, connMgr.GetConnectionById(data.ConnectionId))
	const ipKey = "127.0.0.1"
	require.Equal(t, 1, connMgr.InboundCount(), "slot held before arrival")
	require.Equal(t, 1, connMgr.IPConnCount(ipKey), "ip slot held before arrival")

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:       logger,
		EventBus:     event.NewEventBus(nil, nil),
		PromRegistry: prometheus.NewRegistry(),
		ConnManager:  connMgr,
	})
	pg.mu.Lock()
	for i := range pg.maxPeerListSize() {
		addr := fmt.Sprintf("10.%d.%d.1:3001", i/250, i%250)
		pg.peers = append(pg.peers, &Peer{
			Address:           addr,
			NormalizedAddress: addr,
			Source:            PeerSourceInboundConn,
			State:             PeerStateCold,
		})
	}
	require.True(t, pg.isAtPeerCapLocked())
	pg.mu.Unlock()

	pg.handleInboundConnectionEvent(arrival)

	assert.Equal(t, float64(1), promtestutil.ToFloat64(
		pg.metrics.inboundLifecycle.WithLabelValues("rejected"),
	))
	testutil.WaitForCondition(
		t,
		func() bool {
			return connMgr.GetConnectionById(data.ConnectionId) == nil &&
				connMgr.InboundCount() == 0 &&
				connMgr.IPConnCount(ipKey) == 0
		},
		rejectedCloseWait,
		"cap-rejected connection must release its inbound and per-IP slots",
	)
	assert.Equal(t, 0, connMgr.InboundCount())
	assert.Equal(t, 0, connMgr.IPConnCount(ipKey))
}
