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

package bark

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

// remoteCollector records the RemoteEvents that reach a node's EventBus.
type remoteCollector struct {
	mu   sync.Mutex
	seen []RemoteEvent
}

func newRemoteCollector(bus *event.EventBus) *remoteCollector {
	c := &remoteCollector{}
	bus.SubscribeFunc(RemoteEventType, func(evt event.Event) {
		re, ok := evt.Data.(RemoteEvent)
		if !ok {
			return
		}
		c.mu.Lock()
		c.seen = append(c.seen, re)
		c.mu.Unlock()
	})
	return c
}

func (c *remoteCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

func (c *remoteCollector) addresses(t *testing.T) []string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, re := range c.seen {
		var d eventv1alpha1.PeerChurnEventData
		require.NoError(t, re.Event.GetTypedData().UnmarshalTo(&d))
		out = append(out, d.GetAddress())
	}
	return out
}

// Node A's events reach node B's EventBus with the source attached, and a
// dropped connection neither loses nor repeats an event on node B.
func TestRemoteEventsRepublishWithSourceAndSurviveDisconnect(t *testing.T) {
	t.Parallel()
	nodeA := newEventFixture(t, nil, false)
	busB := event.NewEventBus(nil, nil)
	t.Cleanup(busB.Close)
	collected := newRemoteCollector(busB)

	remote, err := NewRemoteEvents(RemoteEventsConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: busB,
		NodeID:   "node-b",
		Sources: []RemoteEventSource{{
			Name:       "a",
			URL:        nodeA.srv.URL,
			EventTypes: []string{"peergov.*"},
		}},
		HTTPClient:     nodeA.srv.Client(),
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, remote.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})
	waitActive(t, nodeA, 1)

	nodeA.publishChurn("a:1")
	nodeA.publishChurn("b:1")
	require.Eventually(t, func() bool { return collected.count() == 2 },
		5*time.Second, 5*time.Millisecond)

	nodeA.srv.CloseClientConnections()
	nodeA.publishChurn("c:1")
	nodeA.publishChurn("d:1")
	require.Eventually(t, func() bool { return collected.count() == 4 },
		10*time.Second, 5*time.Millisecond)

	require.Equal(t,
		[]string{"a:1", "b:1", "c:1", "d:1"}, collected.addresses(t))
	collected.mu.Lock()
	defer collected.mu.Unlock()
	for _, re := range collected.seen {
		require.Equal(t, "a", re.Source)
		require.Equal(t, "node-a", re.Event.GetOriginNodeId())
	}
}

func TestNewRemoteEventsRejectsBadSources(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	good := RemoteEventSource{Name: "a", URL: "http://127.0.0.1:1"}
	for name, sources := range map[string][]RemoteEventSource{
		"no name":      {{URL: good.URL}},
		"no url":       {{Name: "a"}},
		"bad scheme":   {{Name: "a", URL: "ftp://host"}},
		"no host":      {{Name: "a", URL: "http://"}},
		"duplicate":    {good, good},
		"unparseable":  {{Name: "a", URL: "://"}},
		"relative url": {{Name: "a", URL: "/events"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewRemoteEvents(RemoteEventsConfig{
				EventBus: bus, NodeID: "n", Sources: sources,
			})
			require.Error(t, err)
		})
	}
	_, err := NewRemoteEvents(RemoteEventsConfig{
		EventBus: bus, NodeID: "n", Sources: []RemoteEventSource{good},
	})
	require.NoError(t, err)
}

// gateTransport counts requests and can refuse them, which holds a
// reconnecting subscriber off a node while its events pile up.
type gateTransport struct {
	next    http.RoundTripper
	blocked atomic.Bool
	calls   atomic.Int64
}

func (g *gateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	g.calls.Add(1)
	if g.blocked.Load() {
		return nil, errors.New("gated")
	}
	return g.next.RoundTrip(r)
}

func (c *remoteCollector) has(t *testing.T, addr string) bool {
	t.Helper()
	return slices.Contains(c.addresses(t), addr)
}

func startRemote(
	t *testing.T,
	bus *event.EventBus,
	client *http.Client,
	src RemoteEventSource,
) *RemoteEvents {
	t.Helper()
	remote, err := NewRemoteEvents(RemoteEventsConfig{
		Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:       bus,
		NodeID:         "node-b",
		Sources:        []RemoteEventSource{src},
		HTTPClient:     client,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, remote.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})
	return remote
}

// When the followed node no longer buffers the position a source stopped at,
// the source starts again from that node's current position: it does not
// retry the lost position forever, and the events in between are gone.
func TestRemoteEventsRestartFromHeadWhenPositionIsGone(t *testing.T) {
	t.Parallel()
	nodeA := newEventFixture(t, func(c *BarkConfig) {
		c.EventBufferSize = 2
	}, false)
	gate := &gateTransport{next: nodeA.srv.Client().Transport}
	busB := event.NewEventBus(nil, nil)
	t.Cleanup(busB.Close)
	collected := newRemoteCollector(busB)
	startRemote(t, busB, &http.Client{Transport: gate},
		RemoteEventSource{Name: "a", URL: nodeA.srv.URL})
	waitActive(t, nodeA, 1)

	nodeA.publishChurn("a:1")
	nodeA.publishChurn("b:1")
	require.Eventually(t, func() bool { return collected.count() == 2 },
		5*time.Second, 5*time.Millisecond)

	gate.blocked.Store(true)
	nodeA.srv.CloseClientConnections()
	waitActive(t, nodeA, 0)
	for _, addr := range []string{"c:1", "d:1", "e:1", "f:1"} {
		nodeA.publishChurn(addr)
	}
	require.Eventually(t, func() bool {
		return nodeA.hub.position() == 6
	}, 5*time.Second, 5*time.Millisecond)
	gate.blocked.Store(false)

	// Published until one arrives: the subscription may still be
	// re-establishing when the first is sent.
	require.Eventually(t, func() bool {
		nodeA.publishChurn("z:1")
		return collected.has(t, "z:1")
	}, 10*time.Second, 20*time.Millisecond)
	for _, lost := range []string{"c:1", "d:1", "e:1", "f:1"} {
		require.False(t, collected.has(t, lost), lost)
	}
}

// A source that the followed node refuses for a reason retrying cannot fix
// is retried with backoff instead of abandoned, so fixing the remote node
// does not require restarting this one.
func TestRemoteEventsKeepRetryingAfterTerminalError(t *testing.T) {
	t.Parallel()
	nodeA := newEventFixture(t, nil, false)
	gate := &gateTransport{next: nodeA.srv.Client().Transport}
	busB := event.NewEventBus(nil, nil)
	t.Cleanup(busB.Close)
	startRemote(t, busB, &http.Client{Transport: gate}, RemoteEventSource{
		Name: "a", URL: nodeA.srv.URL, EventTypes: []string{"nope.*"},
	})
	require.Eventually(t, func() bool {
		return gate.calls.Load() >= 3
	}, 5*time.Second, 5*time.Millisecond)
}

// syncBuffer is a log sink the source goroutines can write while the test
// reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), s)
}

// An unreachable source is reported once per outage, however many times the
// client retries, and its return is reported too. The client retries
// silently, so these lines are an operator's only sign of the outage.
func TestRemoteEventsLogUnreachableSourceOncePerOutage(t *testing.T) {
	t.Parallel()
	nodeA := newEventFixture(t, nil, false)
	gate := &gateTransport{next: nodeA.srv.Client().Transport}
	gate.blocked.Store(true)
	busB := event.NewEventBus(nil, nil)
	t.Cleanup(busB.Close)
	logs := &syncBuffer{}
	remote, err := NewRemoteEvents(RemoteEventsConfig{
		Logger:         slog.New(slog.NewTextHandler(logs, nil)),
		EventBus:       busB,
		NodeID:         "node-b",
		Sources:        []RemoteEventSource{{Name: "a", URL: nodeA.srv.URL}},
		HTTPClient:     &http.Client{Transport: gate},
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, remote.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})

	require.Eventually(t, func() bool {
		return gate.calls.Load() >= 3
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, logs.count("remote event source unreachable"))

	gate.blocked.Store(false)
	waitActive(t, nodeA, 1)
	require.Eventually(t, func() bool {
		nodeA.publishChurn("a:1")
		return logs.count("remote event source reachable again") == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 1, logs.count("remote event source unreachable"))
}
