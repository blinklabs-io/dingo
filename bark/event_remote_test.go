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
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	eventconnect "github.com/blinklabs-io/bark/proto/v1alpha1/event/eventv1alpha1connect"
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
		ctx, cancel := context.WithTimeout(context.Background(), eventTestWait)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})
	waitActive(t, nodeA, 1)

	nodeA.publishChurn("a:1")
	nodeA.publishChurn("b:1")
	require.Eventually(t, func() bool { return collected.count() == 2 },
		eventTestWait, 5*time.Millisecond)

	nodeA.srv.CloseClientConnections()
	nodeA.publishChurn("c:1")
	nodeA.publishChurn("d:1")
	require.Eventually(t, func() bool { return collected.count() == 4 },
		eventTestWait, 5*time.Millisecond)

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

// restartableEventService keeps one listener while replacing the serving
// event hub, matching a process restart on a stable address.
type restartableEventService struct {
	eventconnect.UnimplementedEventServiceHandler
	current atomic.Pointer[eventServiceHandler]
}

type resumeCursor struct {
	runID    string
	sequence uint64
}

// terminalResumeService ends the first subscription after one acknowledged
// event so RemoteEvents must carry its cursor into a new client call.
type terminalResumeService struct {
	eventconnect.UnimplementedEventServiceHandler
	attempt atomic.Int64
	resumed chan resumeCursor
}

func (s *terminalResumeService) SubscribeWithAck(
	ctx context.Context,
	stream *connect.BidiStream[
		eventv1alpha1.SubscribeWithAckRequest,
		eventv1alpha1.SubscribeWithAckResponse,
	],
) error {
	req, err := stream.Receive()
	if err != nil {
		return err
	}
	start := req.GetStart()
	if s.attempt.Add(1) == 1 {
		if err := stream.Send(&eventv1alpha1.SubscribeWithAckResponse{
			Ready: &eventv1alpha1.SubscribeWithAckReady{RunId: "run-1"},
		}); err != nil {
			return err
		}
		if err := stream.Send(&eventv1alpha1.SubscribeWithAckResponse{
			Event: &eventv1alpha1.Event{
				RunId: "run-1", Sequence: 1, EventType: "test.event",
			},
		}); err != nil {
			return err
		}
		if _, err := stream.Receive(); err != nil {
			return err
		}
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("end first subscription"),
		)
	}
	s.resumed <- resumeCursor{
		runID: start.GetLastAckedRunId(), sequence: start.GetLastAckedSequence(),
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *restartableEventService) use(f *eventFixture) {
	s.current.Store(&eventServiceHandler{hub: f.hub})
}

func (s *restartableEventService) SubscribeWithAck(
	ctx context.Context,
	stream *connect.BidiStream[
		eventv1alpha1.SubscribeWithAckRequest,
		eventv1alpha1.SubscribeWithAckResponse,
	],
) error {
	return s.current.Load().SubscribeWithAck(ctx, stream)
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
		ctx, cancel := context.WithTimeout(context.Background(), eventTestWait)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})
	return remote
}

func TestRemoteEventsResumeBufferedEventsAfterSourceRestart(t *testing.T) {
	t.Parallel()
	oldRun := newEventFixture(t, nil, false)
	newRun := newEventFixture(t, nil, false)
	service := &restartableEventService{}
	service.use(oldRun)
	mux := http.NewServeMux()
	path, handler := eventconnect.NewEventServiceHandler(service)
	mux.Handle(path, handler)
	front := httptest.NewUnstartedServer(mux)
	front.EnableHTTP2 = true
	front.StartTLS()
	t.Cleanup(front.Close)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	collected := newRemoteCollector(bus)
	startRemote(t, bus, front.Client(), RemoteEventSource{
		Name: "a", URL: front.URL,
	})
	waitActive(t, oldRun, 1)

	oldRun.publishChurn("old:1")
	require.Eventually(t, func() bool { return collected.count() == 1 },
		eventTestWait, 5*time.Millisecond)
	newRun.publishChurn("new:1")
	newRun.publishChurn("new:2")
	require.Eventually(t, func() bool { return eventPosition(newRun.hub) == 2 },
		eventTestWait, 5*time.Millisecond)

	service.use(newRun)
	oldRun.hub.close()
	oldRun.srv.CloseClientConnections()
	require.Eventually(t, func() bool { return collected.count() == 3 },
		eventTestWait, 5*time.Millisecond)
	require.Equal(t,
		[]string{"old:1", "new:1", "new:2"},
		collected.addresses(t),
	)
}

func TestRemoteEventsKeepInitialPositionAcrossReconnect(t *testing.T) {
	t.Parallel()
	source := newEventFixture(t, nil, false)
	gate := &gateTransport{next: source.srv.Client().Transport}
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	collected := newRemoteCollector(bus)
	startRemote(t, bus, &http.Client{Transport: gate}, RemoteEventSource{
		Name: "a", URL: source.srv.URL,
	})
	waitActive(t, source, 1)

	gate.blocked.Store(true)
	source.srv.CloseClientConnections()
	waitActive(t, source, 0)
	source.publishChurn("a:1")
	source.publishChurn("b:1")
	require.Eventually(t, func() bool { return eventPosition(source.hub) == 2 },
		eventTestWait, 5*time.Millisecond)
	gate.blocked.Store(false)

	require.Eventually(t, func() bool { return collected.count() == 2 },
		eventTestWait, 5*time.Millisecond)
	require.Equal(t, []string{"a:1", "b:1"}, collected.addresses(t))
}

func TestRemoteEventsCarryRunAcrossOuterRetry(t *testing.T) {
	t.Parallel()
	service := &terminalResumeService{resumed: make(chan resumeCursor, 1)}
	mux := http.NewServeMux()
	path, handler := eventconnect.NewEventServiceHandler(service)
	mux.Handle(path, handler)
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	startRemote(t, bus, srv.Client(), RemoteEventSource{
		Name: "a", URL: srv.URL,
	})

	select {
	case cursor := <-service.resumed:
		require.Equal(t, "run-1", cursor.runID)
		require.Equal(t, uint64(1), cursor.sequence)
	case <-time.After(eventTestWait):
		t.Fatal("remote source did not retry after the terminal response")
	}
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
		eventTestWait, 5*time.Millisecond)

	gate.blocked.Store(true)
	nodeA.srv.CloseClientConnections()
	waitActive(t, nodeA, 0)
	for _, addr := range []string{"c:1", "d:1", "e:1", "f:1"} {
		nodeA.publishChurn(addr)
	}
	require.Eventually(t, func() bool {
		return eventPosition(nodeA.hub) == 6
	}, eventTestWait, 5*time.Millisecond)
	gate.blocked.Store(false)

	// Published until one arrives: the subscription may still be
	// re-establishing when the first is sent.
	require.Eventually(t, func() bool {
		nodeA.publishChurn("z:1")
		return collected.has(t, "z:1")
	}, eventTestWait, 20*time.Millisecond)
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
	}, eventTestWait, 5*time.Millisecond)
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
		ctx, cancel := context.WithTimeout(context.Background(), eventTestWait)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})

	require.Eventually(t, func() bool {
		return gate.calls.Load() >= 3
	}, eventTestWait, 5*time.Millisecond)
	require.Equal(t, 1, logs.count("remote event source unreachable"))

	gate.blocked.Store(false)
	waitActive(t, nodeA, 1)
	require.Eventually(t, func() bool {
		nodeA.publishChurn("a:1")
		return logs.count("remote event source reachable again") == 1
	}, eventTestWait, 20*time.Millisecond)
	require.Equal(t, 1, logs.count("remote event source unreachable"))
}

// TestRemoteEventsRefuseOversizedMessage checks a source's message over
// the source's message limit is not accepted, even when it arrives
// compressed, and that the refusal is logged rather than retried silently.
func TestRemoteEventsRefuseOversizedMessage(t *testing.T) {
	t.Parallel()
	const maxMessage = 64 << 10
	nodeA := newEventFixture(t, nil, false)
	busB := event.NewEventBus(nil, nil)
	t.Cleanup(busB.Close)
	got := newRemoteCollector(busB)
	logs := &syncBuffer{}
	remote, err := NewRemoteEvents(RemoteEventsConfig{
		Logger:          slog.New(slog.NewTextHandler(logs, nil)),
		EventBus:        busB,
		NodeID:          "node-b",
		Sources:         []RemoteEventSource{{Name: "a", URL: nodeA.srv.URL}},
		HTTPClient:      nodeA.srv.Client(),
		InitialBackoff:  10 * time.Millisecond,
		MaxBackoff:      20 * time.Millisecond,
		maxMessageBytes: maxMessage,
	})
	require.NoError(t, err)
	require.NoError(t, remote.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), eventTestWait)
		defer cancel()
		require.NoError(t, remote.Stop(ctx))
	})

	waitActive(t, nodeA, 1)
	nodeA.publishChurn(strings.Repeat("a", maxMessage+1))
	require.Eventually(t, func() bool {
		return logs.count("remote event source stream refused") == 1
	}, eventTestWait, 5*time.Millisecond)
	require.Zero(t, got.count())
}

// TestRemoteEventsKeepRedirectsOnSourceOrigin checks the default client
// follows a redirect within the source's origin and refuses one to another
// origin without contacting it.
func TestRemoteEventsKeepRedirectsOnSourceOrigin(t *testing.T) {
	t.Parallel()
	var otherHits atomic.Int32
	other := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			otherHits.Add(1)
		}),
	)
	t.Cleanup(other.Close)
	var sameHits atomic.Int32
	source := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/same":
				sameHits.Add(1)
			case "/to-same":
				http.Redirect(w, r, "/same", http.StatusTemporaryRedirect)
			default:
				http.Redirect(
					w,
					r,
					other.URL+"/internal",
					http.StatusTemporaryRedirect,
				)
			}
		}),
	)
	t.Cleanup(source.Close)
	r := &RemoteEvents{}
	// The configured URL may carry credentials; they are not part of the
	// origin.
	configured := strings.Replace(source.URL, "http://", "http://user:pw@", 1)
	client, ok := r.httpClient(configured).(*http.Client)
	require.True(t, ok)
	// The default client speaks h2c to plaintext sources; the test servers
	// speak HTTP/1.1, so only the redirect policy is kept.
	client = &http.Client{CheckRedirect: client.CheckRedirect}

	resp, err := client.Get(source.URL + "/to-same")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int32(1), sameHits.Load())

	resp, err = client.Get(source.URL + "/to-other")
	if resp != nil {
		require.NoError(t, resp.Body.Close())
	}
	require.ErrorContains(t, err, "redirect to origin")
	require.Zero(t, otherHits.Load())
}
