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

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// ConnClosedFunc is the only close notification an NtC connection gets --
// ConnectionClosedEventType is published for NtN closes only (see
// ntc_conn_closed_test.go). This pair of tests proves the callback
// distinguishes the two: before issue #3508's fix, ConnClosedFunc carried no
// isNtC parameter at all, so nothing downstream could tell an NtC close from
// an NtN one and wire NtC-specific chainsync teardown to it.
//
// Each test gets its own ConnectionManager: newUnstartedConnection returns
// connections whose ConnectionId is the zero value, so two of them
// registered with one manager would collide (see ntc_conn_closed_test.go).

func TestConnClosedFunc_ReceivesIsNtCTrueForNtCClose(t *testing.T) {
	type call struct {
		isNtC bool
		err   error
	}
	calls := make(chan call, 1)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedFunc: func(_ ouroboros.ConnectionId, isNtC bool, err error) {
			calls <- call{isNtC: isNtC, err: err}
		},
	})
	conn := newUnstartedConnection(t)
	require.True(
		t,
		cm.addNtCConnectionWithIPKey(conn, true, "127.0.0.1:3002", ""),
	)

	closeErr := errors.New("ntc connection closed")
	conn.ErrorChan() <- closeErr
	waitForConnectionManagerWatchers(t, cm)

	select {
	case c := <-calls:
		require.True(t, c.isNtC, "NtC connection close must report isNtC=true")
		require.ErrorIs(t, c.err, closeErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ConnClosedFunc call")
	}
}

func TestConnClosedFunc_ReceivesIsNtCFalseForNtNClose(t *testing.T) {
	type call struct {
		isNtC bool
		err   error
	}
	calls := make(chan call, 1)
	cm := NewConnectionManager(ConnectionManagerConfig{
		ConnClosedFunc: func(_ ouroboros.ConnectionId, isNtC bool, err error) {
			calls <- call{isNtC: isNtC, err: err}
		},
	})
	conn := newUnstartedConnection(t)
	require.True(t, cm.AddConnection(conn, false, "1.2.3.4:3001"))

	closeErr := errors.New("ntn connection closed")
	conn.ErrorChan() <- closeErr
	waitForConnectionManagerWatchers(t, cm)

	select {
	case c := <-calls:
		require.False(t, c.isNtC, "NtN connection close must report isNtC=false")
		require.ErrorIs(t, c.err, closeErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ConnClosedFunc call")
	}
}
