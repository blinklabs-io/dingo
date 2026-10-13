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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

func requireOpen(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(msg)
	default:
	}
}

func TestConnectionDoneReleasesHandlersWhileManagerKeepsErrorValue(
	t *testing.T,
) {
	t.Parallel()

	closedErr := make(chan error, 1)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedFunc: func(_ ouroboros.ConnectionId, _ bool, err error) {
			closedErr <- err
		},
	})
	conn := newUnstartedConnection(t)
	require.True(t, cm.AddConnection(conn, false, "1.2.3.4:3001"))

	got, done, closeErr := cm.GetConnectionWithDoneAndError(conn.Id())
	require.Same(t, conn, got)
	requireOpen(t, done, "done closed before any connection error")

	const handlers = 4
	var released sync.WaitGroup
	for range handlers {
		released.Go(func() { <-done })
	}

	injected := errors.New("peer reset")
	conn.ErrorChan() <- injected

	handlersDone := make(chan struct{})
	go func() { released.Wait(); close(handlersDone) }()
	testutil.RequireReceive(t, handlersDone, 5*time.Second,
		"handlers waiting on done were not released")
	require.ErrorIs(t,
		testutil.RequireReceive(t, closedErr, 5*time.Second,
			"manager never reported the connection error"),
		injected,
		"the manager must receive the error value itself",
	)
	<-done
	require.ErrorIs(t, closeErr(), injected)
	waitForConnectionManagerWatchers(t, cm)
}

func TestConnectionDoneClosesWhenErrorChannelCloses(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{})
	conn := newUnstartedConnection(t)
	require.True(t, cm.AddConnection(conn, false, "1.2.3.4:3001"))
	_, done := cm.GetConnectionWithDone(conn.Id())

	close(conn.ErrorChan())

	testutil.RequireReceive(t, done, 5*time.Second,
		"done not closed by error channel closure")
	waitForConnectionManagerWatchers(t, cm)
}

func TestGetConnectionWithDoneUnknownConnection(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{})
	conn, done := cm.GetConnectionWithDone(newUnstartedConnection(t).Id())
	require.Nil(t, conn)
	require.Nil(t, done)
}

func TestConnectionDoneOfReplacedConnectionClosesIndependently(t *testing.T) {
	t.Parallel()

	cm := NewConnectionManager(ConnectionManagerConfig{})
	inbound := newUnstartedConnection(t)
	outbound := newUnstartedConnection(t)
	require.True(t, cm.AddConnection(inbound, true, "1.2.3.4:3001"))
	_, inboundDone := cm.GetConnectionWithDone(inbound.Id())
	require.True(t, cm.AddConnection(outbound, false, "1.2.3.4:3001"))
	_, outboundDone := cm.GetConnectionWithDone(outbound.Id())

	inbound.ErrorChan() <- errors.New("evicted connection closed")
	testutil.RequireReceive(t, inboundDone, 5*time.Second,
		"evicted connection's done not closed")
	requireOpen(t, outboundDone, "replacement's done closed by the evicted connection's error")

	outbound.ErrorChan() <- nil
	testutil.RequireReceive(t, outboundDone, 5*time.Second,
		"replacement's done not closed")
	waitForConnectionManagerWatchers(t, cm)
}
