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
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ouroboros "github.com/blinklabs-io/gouroboros"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
)

func TestKeepaliveClientResponsePublishesPeerActivity(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	_, evtCh := bus.Subscribe(chainselection.PeerActivityEventType)
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 3001},
	}

	o.keepaliveClientResponse(connId, 42)

	select {
	case evt := <-evtCh:
		activityEvt, ok := evt.Data.(chainselection.PeerActivityEvent)
		require.True(t, ok, "expected PeerActivityEvent")
		assert.Equal(t, connId.String(), activityEvt.ConnectionId.String())
	default:
		t.Fatal("expected peer activity event")
	}
}

// keepaliveTimeoutFor builds an Ouroboros with the given KeepAliveTimeout and
// returns the pong-wait Timeout that its keepaliveConnOpts produce, resolved
// through gouroboros' NewConfig (which starts from the 10s default).
func keepaliveTimeoutFor(cfgTimeout time.Duration) time.Duration {
	o := newOuroboros(OuroborosConfig{KeepAliveTimeout: cfgTimeout})
	return okeepalive.NewConfig(o.keepaliveConnOpts()...).Timeout
}

func TestKeepaliveConnOptsTimeout(t *testing.T) {
	t.Parallel()

	// Unset: gouroboros default (10s) is left in place.
	assert.Equal(
		t,
		time.Duration(okeepalive.DefaultKeepAliveTimeout)*time.Second,
		keepaliveTimeoutFor(0),
	)
	// Set below the spec maximum: applied verbatim.
	belowServerTimeout := okeepalive.ServerTimeout / 2
	assert.Equal(t, belowServerTimeout, keepaliveTimeoutFor(belowServerTimeout))
	// The Musashi value (spec maximum) is applied.
	assert.Equal(
		t,
		okeepalive.ServerTimeout,
		keepaliveTimeoutFor(okeepalive.ServerTimeout),
	)
	// Above the spec maximum: clamped down to it.
	assert.Equal(
		t,
		okeepalive.ServerTimeout,
		keepaliveTimeoutFor(okeepalive.ServerTimeout+30*time.Second),
	)
}

// TestClassifyKeepaliveTimeoutClose pins which close reasons count as a
// keep-alive pong timeout: only the client's Server-state transition timeout,
// bare or wrapped. Another protocol's timeout and the server's ping-wait
// timeout share the message shape and must not match.
func TestClassifyKeepaliveTimeoutClose(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{
			"keepalive timeout",
			errors.New(
				"keep-alive: timeout waiting on transition from protocol state Server",
			),
			true,
		},
		{
			"keepalive timeout wrapped as gouroboros forwards it",
			fmt.Errorf(
				"protocol error: %w",
				errors.New(
					"keep-alive: timeout waiting on transition from protocol state Server",
				),
			),
			true,
		},
		{
			// The server side waiting on the peer's next ping is not a
			// pong timeout.
			"keepalive server ping-wait timeout",
			errors.New(
				"keep-alive: timeout waiting on transition from protocol state Client",
			),
			false,
		},
		{
			"other protocol timeout",
			errors.New(
				"chain-sync: timeout waiting on transition from protocol state Idle",
			),
			false,
		},
		{
			"keepalive non-timeout error",
			errors.New(
				"keep-alive: unexpected cookie in response, expected 4 but received 5",
			),
			false,
		},
		{"unrelated error", errors.New("connection reset by peer"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, classifyKeepaliveTimeoutClose(tc.err))
		})
	}
}

// TestHandleConnClosedEventRecordsKeepaliveTimeoutOutcome checks that
// HandleConnClosedEvent increments dingo_keepalive_timeout_total only when
// the connection's close reason is a keep-alive timeout, and leaves it
// unchanged for an unrelated close reason -- the counter must distinguish
// outcomes, not just count every connection close.
func TestHandleConnClosedEventRecordsKeepaliveTimeoutOutcome(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 3001},
	}

	o.HandleConnClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error:        errors.New("blockfetch: connection reset by peer"),
		},
	})
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(o.protocolMetrics.keepaliveTimeouts),
		"an unrelated close reason must not count as a keep-alive timeout",
	)

	o.HandleConnClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error: errors.New(
				"keep-alive: timeout waiting on transition from protocol state Server",
			),
		},
	})
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(o.protocolMetrics.keepaliveTimeouts),
		"a keep-alive timeout close must be counted",
	)
}

// TestHandleConnClosedEventCountsRealKeepaliveTimeout drives a real
// gouroboros connection against a peer that completes the NtN handshake and
// then never answers the first keep-alive ping, and feeds the error the
// connection actually reports into HandleConnClosedEvent. gouroboros wraps
// every mini-protocol error it forwards ("protocol error: %w" in
// connection.go), so a classifier written against the bare protocol.go
// message shape never matches in production; this pins the shape
// connmanager really hands to ConnectionClosedEvent.
func TestHandleConnClosedEventCountsRealKeepaliveTimeout(t *testing.T) {
	t.Parallel()

	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		[]ouroboros_mock.ConversationEntry{
			ouroboros_mock.ConversationEntryHandshakeRequestGeneric,
			ouroboros_mock.ConversationEntryHandshakeNtNResponse,
			ouroboros_mock.ConversationEntryKeepAliveRequest,
			// No response: the client's pong wait must time out.
		},
	)
	kaCfg := okeepalive.NewConfig(
		okeepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
		okeepalive.WithPeriod(time.Minute),
		okeepalive.WithTimeout(200*time.Millisecond),
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(kaCfg),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = oConn.Close() })

	var closeErr error
	select {
	case closeErr = <-oConn.ErrorChan():
	case <-time.After(5 * time.Second):
		t.Fatal("keep-alive pong timeout was never reported")
	}
	require.Error(t, closeErr)
	require.True(
		t,
		classifyKeepaliveTimeoutClose(closeErr),
		"real keep-alive pong timeout not classified: %q",
		closeErr.Error(),
	)

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	o.HandleConnClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: oConn.Id(),
			Error:        closeErr,
		},
	})
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(o.protocolMetrics.keepaliveTimeouts),
	)
}

func servedTestConnId(port int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.7"), Port: port},
	}
}

func TestRecordServedActivityIsThrottledPerConnection(t *testing.T) {
	t.Parallel()
	var reports atomic.Int64
	o := &Ouroboros{servedActivityHook: func(ouroboros.ConnectionId) {
		reports.Add(1)
	}}
	a, b := servedTestConnId(40001), servedTestConnId(40002)
	for range 1000 {
		o.recordServedActivity(a)
	}
	assert.Equal(t, int64(1), reports.Load(),
		"a burst on one connection reports once")
	o.recordServedActivity(b)
	assert.Equal(t, int64(2), reports.Load(),
		"a different connection reports independently")
	o.forgetServedActivity(a)
	o.recordServedActivity(a)
	assert.Equal(t, int64(3), reports.Load(),
		"closing a connection clears its throttle state")
}

func TestKeepaliveServerPingRecordsServedActivity(t *testing.T) {
	t.Parallel()
	var reports atomic.Int64
	o := &Ouroboros{servedActivityHook: func(ouroboros.ConnectionId) {
		reports.Add(1)
	}}
	cfg := okeepalive.NewConfig(o.keepaliveConnOpts()...)
	require.NotNil(t, cfg.OnKeepAliveReceived)
	cfg.OnKeepAliveReceived(servedTestConnId(40003), 1)
	assert.Equal(t, int64(1), reports.Load())
}

func TestRecordServedActivitySkipsNonTCPConnections(t *testing.T) {
	t.Parallel()
	var reports atomic.Int64
	o := &Ouroboros{servedActivityHook: func(ouroboros.ConnectionId) {
		reports.Add(1)
	}}
	unixConn := ouroboros.ConnectionId{
		LocalAddr:  &net.UnixAddr{Name: "/run/dingo.socket", Net: "unix"},
		RemoteAddr: &net.UnixAddr{Name: "@", Net: "unix"},
	}
	for range 100 {
		o.recordServedActivity(unixConn)
	}
	assert.Equal(t, int64(0), reports.Load(),
		"node-to-client unix connections are not reported to the governor")
}

func TestRecordServedActivityReportsAgainAfterInterval(t *testing.T) {
	t.Parallel()
	var reports atomic.Int64
	o := &Ouroboros{
		servedActivityHook: func(ouroboros.ConnectionId) {
			reports.Add(1)
		},
		servedActivityInterval: 20 * time.Millisecond,
	}
	connId := servedTestConnId(40004)
	o.recordServedActivity(connId)
	o.recordServedActivity(connId)
	require.Equal(t, int64(1), reports.Load(),
		"a second call inside the interval is throttled")
	require.Eventually(t, func() bool {
		o.recordServedActivity(connId)
		return reports.Load() == 2
	}, 2*time.Second, 5*time.Millisecond,
		"the throttle must re-arm once the interval has passed")
}
