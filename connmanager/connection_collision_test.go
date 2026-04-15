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
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func newUnstartedConnection(t *testing.T) *ouroboros.Connection {
	t.Helper()
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	return conn
}

func waitForConnectionManagerWatchers(
	t *testing.T,
	cm *ConnectionManager,
) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		cm.goroutineWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection manager watchers")
	}
}

func TestAddConnectionRejectsInboundCollisionWithOutbound(t *testing.T) {
	cm := NewConnectionManager(ConnectionManagerConfig{})
	outbound := newUnstartedConnection(t)
	inbound := newUnstartedConnection(t)

	require.True(t, cm.AddConnection(outbound, false, "1.2.3.4:3001"))
	require.False(t, cm.AddConnection(inbound, true, "1.2.3.4:3001"))
	require.Same(t, outbound, cm.GetConnectionById(outbound.Id()))
	require.False(t, cm.IsInboundConnection(outbound.Id()))

	outbound.ErrorChan() <- nil
	waitForConnectionManagerWatchers(t, cm)
}

func TestReplacedConnectionCloseDoesNotPublishStaleEvent(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, closeEvents := bus.Subscribe(ConnectionClosedEventType)

	cm := NewConnectionManager(ConnectionManagerConfig{EventBus: bus})
	inbound := newUnstartedConnection(t)
	outbound := newUnstartedConnection(t)
	staleErr := errors.New("stale connection closed")
	liveErr := errors.New("live connection closed")

	require.True(t, cm.AddConnection(inbound, true, "1.2.3.4:3001"))
	require.True(t, cm.AddConnection(outbound, false, "1.2.3.4:3001"))
	require.Same(t, outbound, cm.GetConnectionById(outbound.Id()))

	inbound.ErrorChan() <- staleErr
	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		t.Fatalf("received stale close event: %v", data.Error)
	case <-time.After(100 * time.Millisecond):
	}

	outbound.ErrorChan() <- liveErr
	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		require.ErrorIs(t, data.Error, liveErr)
	case <-time.After(time.Second):
		t.Fatal("expected close event for live connection")
	}

	select {
	case evt := <-closeEvents:
		data, ok := evt.Data.(ConnectionClosedEvent)
		require.True(t, ok)
		t.Fatalf("received unexpected extra close event: %v", data.Error)
	case <-time.After(100 * time.Millisecond):
	}

	waitForConnectionManagerWatchers(t, cm)
}
