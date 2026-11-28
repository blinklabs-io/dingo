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
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func closeLaneConnId(port int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(44, 0, 0, 2), Port: port},
	}
}

// Close events must reach the governor even when they pile up while a
// governor pass holds its lock. A close the governor never sees leaves its
// peer hot on a dead connection for good: nothing redials it, and its dial
// worker stops because the peer still has a client connection.
//
// Not t.Parallel: shortens the process-wide subscriber delivery timeout.
func TestConnectionClosedEventsSurviveGovernorLockBacklog(t *testing.T) {
	restore := event.SetChannelDeliveryTimeoutForTesting(10 * time.Millisecond)
	t.Cleanup(restore)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          bus,
		DisableOutbound:   true,
		ReconcileInterval: time.Hour,
	})
	heldId := closeLaneConnId(1)
	peer := &Peer{
		Address:           "44.0.0.2:1",
		NormalizedAddress: "44.0.0.2:1",
		Source:            PeerSourceInboundConn,
		State:             PeerStateHot,
		Connection:        &PeerConnection{Id: heldId, IsClient: true},
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()
	require.NoError(t, pg.Start(t.Context()))
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	publish := func(id ouroboros.ConnectionId) {
		bus.Publish(
			connmanager.ConnectionClosedEventType,
			event.NewEvent(
				connmanager.ConnectionClosedEventType,
				connmanager.ConnectionClosedEvent{ConnectionId: id},
			),
		)
	}

	// A governor pass holds the lock while other connections close: the
	// handler blocks on the lock and its buffer fills past capacity.
	pg.mu.Lock()
	backlogDone := make(chan struct{})
	go func() {
		defer close(backlogDone)
		for i := range event.DefaultSubscriberBuffer + 2 {
			publish(closeLaneConnId(10000 + i))
		}
	}()
	// Give a detaching publisher well past its delivery timeout to give
	// up; a blocking one is still waiting for the lock when this returns.
	select {
	case <-backlogDone:
	case <-time.After(500 * time.Millisecond):
	}
	pg.mu.Unlock()

	heldDone := make(chan struct{})
	go func() {
		defer close(heldDone)
		publish(heldId)
	}()
	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peer.Connection == nil && peer.State == PeerStateCold
	}, 5*time.Second, 5*time.Millisecond,
		"the peer must drop the closed connection")
	select {
	case <-backlogDone:
	case <-time.After(5 * time.Second):
		t.Fatal("backlog publisher did not finish")
	}
	select {
	case <-heldDone:
	case <-time.After(5 * time.Second):
		t.Fatal("close publisher did not finish")
	}
}
