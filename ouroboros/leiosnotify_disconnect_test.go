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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/stretchr/testify/require"
)

func TestLeiosNotifyIdleRequestReleasedByDisconnect(t *testing.T) {
	f := newChainsyncServerFixtureWithConfig(t, csmock.ModeNtN, OuroborosConfig{EnableLeios: true})
	// Exercise the production callback with a real managed connection. The
	// standalone protocol's completion remains open throughout the disconnect.
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	t.Cleanup(server.Stop)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = f.o.leiosnotifyServerRequestNext(leiosnotify.CallbackContext{Server: server, ConnectionId: f.conn.Id()})
	}()
	key := leiosConnectionIdString(f.conn.Id())
	testutil.WaitForCondition(t, func() bool {
		f.o.leiosEBLog.mu.Lock()
		defer f.o.leiosEBLog.mu.Unlock()
		_, ok := f.o.leiosEBLog.cursors[key]
		return ok
	}, 5*time.Second, "idle notification request to register its cursor")
	require.NoError(t, f.h.Disconnect())
	testutil.RequireReceive(t, finished, 5*time.Second, "idle callback to exit on connection closure")
	// Releasing the callback and removing its cursor are successive close
	// actions; callback completion alone does not acknowledge cursor cleanup.
	testutil.WaitForCondition(t, func() bool {
		f.o.leiosEBLog.mu.Lock()
		defer f.o.leiosEBLog.mu.Unlock()
		_, retained := f.o.leiosEBLog.cursors[key]
		return !retained
	}, 5*time.Second, "closed connection cursor to be removed")
	select {
	case <-server.DoneChan():
		t.Fatal("test must not close protocol completion to release callback")
	default:
	}
}

func TestLeiosNotifyClosedReservationDoesNotRecreateCursor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	key := "peer"
	done := make(chan struct{})
	o.leiosEBLog.registerConn(key)
	close(done)
	o.leiosEBLog.removeConn(key)
	entry, _ := o.leiosEBLog.nextWhileConnected(key, done)
	require.Nil(t, entry)
	o.leiosEBLog.mu.Lock()
	_, retained := o.leiosEBLog.cursors[key]
	o.leiosEBLog.mu.Unlock()
	require.False(t, retained)
}

func TestLeiosNotifyDisconnectAfterOfferReturnReleasesCursor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	connID := gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	key := leiosConnectionIdString(connID)
	done, cancel := o.registerLeiosServeWaiter(connID)
	defer cancel()
	o.leiosEBLog.registerConn(key)
	point := ocommon.NewPoint(1, []byte{1})
	o.leiosEBLog.append(leiosForgedEBEntry{point: &point})
	entry, _ := o.leiosEBLog.nextWhileConnected(key, done)
	require.NotNil(t, entry)
	// The callback returned an offer, but its send has not completed. The
	// waiter is deregistered before the connection-close callback runs.
	cancel()
	o.ReleaseLeiosServeWaiters(connID)
	require.Empty(t, o.leiosEBLog.cursors)
	require.Empty(t, o.leiosEBLog.reservations)
	require.Len(t, o.leiosEBLog.retries, 1, "undelivered offer must remain retryable")
}

func TestLeiosNotifyCompletedProtocolReturnsWithoutCursor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	server.Stop()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = o.leiosnotifyServerRequestNext(leiosnotify.CallbackContext{Server: server})
	}()
	testutil.RequireReceive(t, finished, time.Second, "completed protocol callback to return")
	require.Empty(t, o.leiosEBLog.cursors)
	require.Empty(t, o.leiosServeWaiters)
}

// next lets log-only tests reserve entries without a connection lifecycle.
func (l *leiosForgedEBLog) next(connKey string) (*leiosForgedEBEntry, chan struct{}) {
	return l.nextWhileConnected(connKey, nil)
}
