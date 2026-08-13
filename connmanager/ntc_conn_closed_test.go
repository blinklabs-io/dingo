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

package connmanager

import (
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

// Each connection gets its own ConnectionManager: newUnstartedConnection
// returns connections whose ConnectionId is the zero value, so two of them
// registered with one manager collide and the second replaces the first. A
// replaced connection's close is already suppressed as stale, which would make
// these assertions pass for the wrong reason.
func newConnManagerWithCloseEvents(
	t *testing.T,
) (*ConnectionManager, <-chan event.Event) {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, closeEvents := bus.Subscribe(ConnectionClosedEventType)
	return NewConnectionManager(ConnectionManagerConfig{EventBus: bus}),
		closeEvents
}

// ConnectionClosedEventType exists to drive node-to-node peer management: its
// subscribers remove the connection from chain selection, peer governance,
// chainsync client state, and the mempool consumer set. None of that applies
// to a node-to-client connection, and the event payload carries no way for a
// subscriber to tell the two apart.
//
// Publishing it for NtC closes is not merely redundant. A local client that
// reconnects rapidly -- devnet's txpump opens and closes a connection roughly
// 90 times a second -- floods the topic, fills the 1024-slot delivery buffer,
// and wedges the subscriber permanently ("event delivery stalled: subscriber
// not draining"). Once that happens the NtN chainsync recovery driven by these
// events never runs again: the node quietly stops following the chain while
// continuing to forge, and only a restart clears it.
func TestNtCConnectionCloseDoesNotPublishPeerEvent(t *testing.T) {
	cm, closeEvents := newConnManagerWithCloseEvents(t)
	conn := newUnstartedConnection(t)

	require.True(
		t,
		cm.addNtCConnectionWithIPKey(conn, true, "127.0.0.1:3002", ""),
	)

	conn.ErrorChan() <- errors.New("ntc connection closed")
	waitForConnectionManagerWatchers(t, cm)

	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		t.Fatalf(
			"NtC close published a peer connection-closed event: %v",
			data.Error,
		)
	case <-time.After(200 * time.Millisecond):
	}
}

// The control for the test above: the same harness must still observe the
// event for a node-to-node connection, or NtN peer cleanup never happens.
func TestNtNConnectionClosePublishesPeerEvent(t *testing.T) {
	cm, closeEvents := newConnManagerWithCloseEvents(t)
	conn := newUnstartedConnection(t)

	require.True(t, cm.AddConnection(conn, false, "1.2.3.4:3001"))

	connErr := errors.New("ntn connection closed")
	conn.ErrorChan() <- connErr

	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		require.ErrorIs(t, data.Error, connErr)
	case <-time.After(time.Second):
		t.Fatal("expected close event for the NtN connection")
	}

	waitForConnectionManagerWatchers(t, cm)
}

// Suppressing the event must not suppress the connection manager's own
// cleanup: the NtC connection still has to leave the connection table.
func TestNtCConnectionCloseStillRemovesConnection(t *testing.T) {
	cm, _ := newConnManagerWithCloseEvents(t)
	conn := newUnstartedConnection(t)

	require.True(
		t,
		cm.addNtCConnectionWithIPKey(conn, true, "127.0.0.1:3002", ""),
	)
	require.NotNil(t, cm.GetConnectionById(conn.Id()))

	conn.ErrorChan() <- errors.New("ntc connection closed")
	waitForConnectionManagerWatchers(t, cm)

	require.Nil(
		t,
		cm.GetConnectionById(conn.Id()),
		"NtC connection was not removed from the connection manager",
	)
}
