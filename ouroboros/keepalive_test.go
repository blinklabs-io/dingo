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
	"net"
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

// TestClassifyKeepaliveTimeoutClose covers #4782's keep-alive outcome
// metric: it must fire only for the keep-alive protocol's own
// state-transition timeout (gouroboros protocol.go's "%s: timeout waiting on
// transition from protocol state %s", scoped by the "keep-alive: " prefix),
// not for a nil close reason, a clean shutdown, or another protocol's error
// -- including another protocol's own timeout, which shares the same
// generic message shape and would false-positive on a substring match alone.
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
			"keepalive timeout, different state",
			errors.New(
				"keep-alive: timeout waiting on transition from protocol state Client",
			),
			true,
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
