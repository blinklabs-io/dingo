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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/bark/eventclient"
	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	"github.com/blinklabs-io/dingo/event"
)

// RemoteEventType is the EventBus type under which events received from
// other nodes are republished. The Event Service does not serve it, so a
// remote event is never sent back out to a subscriber of this node.
const RemoteEventType event.EventType = "bark.remote_event"

const (
	remoteInitialBackoff = time.Second
	remoteMaxBackoff     = 30 * time.Second
)

// RemoteEvent is an event received from another node's Event Service. Source
// is the name of the configured source it arrived from; Event keeps the
// remote node's envelope, including its origin_node_id.
type RemoteEvent struct {
	Source string
	Event  *eventv1alpha1.Event
}

// RemoteEventSource names one node whose events this node follows.
type RemoteEventSource struct {
	// Name labels the source on republished events and in logs. It must be
	// unique among sources.
	Name string
	// URL is the base URL of the remote node's Bark server.
	URL string
	// EventTypes selects event type names or category patterns such as
	// "peergov.*". Empty selects every type the remote node serves.
	EventTypes []string
	// OriginNodeIDs limits events to those created on the named nodes.
	// Empty selects every origin.
	OriginNodeIDs []string
}

// RemoteEventsConfig configures a RemoteEvents subscriber.
type RemoteEventsConfig struct {
	Logger   *slog.Logger
	EventBus *event.EventBus
	// NodeID identifies this node. It prefixes each subscription's ID, so
	// the remote node tells apart the nodes that follow it.
	NodeID  string
	Sources []RemoteEventSource
	// HTTPClient overrides the client used to reach every source. The
	// default speaks HTTP/2 over TLS for https URLs and over plaintext for
	// http URLs.
	HTTPClient connect.HTTPClient
	// InitialBackoff and MaxBackoff bound the wait before a reconnect.
	// Zero selects one second and thirty seconds.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// RemoteEvents follows the Event Service of other nodes and republishes what
// it receives on the local EventBus as RemoteEvent. Each source reconnects
// on its own and resumes after the last event it handled, so once a source
// has handled an event, a dropped connection loses nothing and does not
// repeat an event locally while the remote node keeps running.
//
// Position zero means the remote node's current position, so a source that
// drops before its first handled event resumes there and skips what was
// published while it was disconnected. The position is also lost when this
// node restarts, and the remote node's sequence restarts with its process, so
// events across either restart can be skipped without an error.
type RemoteEvents struct {
	cfg RemoteEventsConfig

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRemoteEvents validates cfg and returns a subscriber that is not yet
// running.
func NewRemoteEvents(cfg RemoteEventsConfig) (*RemoteEvents, error) {
	if cfg.EventBus == nil {
		return nil, errors.New("remote events: EventBus is required")
	}
	if cfg.NodeID == "" {
		return nil, errors.New("remote events: NodeID is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	seen := make(map[string]struct{}, len(cfg.Sources))
	for _, src := range cfg.Sources {
		if err := validateRemoteSource(src); err != nil {
			return nil, err
		}
		if _, dup := seen[src.Name]; dup {
			return nil, fmt.Errorf(
				"remote events: duplicate source name %q", src.Name,
			)
		}
		seen[src.Name] = struct{}{}
	}
	return &RemoteEvents{cfg: cfg}, nil
}

func validateRemoteSource(src RemoteEventSource) error {
	if src.Name == "" {
		return errors.New("remote events: source name is required")
	}
	u, err := url.Parse(src.URL)
	if err != nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf(
			"remote events: source %q needs an http or https URL, got %q",
			src.Name, src.URL,
		)
	}
	return nil
}

// Start begins following every source. It returns at once; the sources run
// until ctx is done or Stop is called. Calling Start twice is an error.
func (r *RemoteEvents) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return errors.New("remote events: already started")
	}
	ctx, r.cancel = context.WithCancel(ctx)
	for _, src := range r.cfg.Sources {
		r.wg.Go(func() { r.follow(ctx, src) })
	}
	return nil
}

// Stop ends every source and waits for them to return.
func (r *RemoteEvents) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *RemoteEvents) httpClient(baseURL string) connect.HTTPClient {
	if r.cfg.HTTPClient != nil {
		return r.cfg.HTTPClient
	}
	// Streaming calls are long-lived, so the client sets no overall timeout.
	if u, err := url.Parse(baseURL); err == nil && u.Scheme == "http" {
		protocols := &http.Protocols{}
		protocols.SetUnencryptedHTTP2(true)
		return &http.Client{Transport: &http.Transport{Protocols: protocols}}
	}
	return &http.Client{}
}

// follow subscribes to one source until ctx is done. The Event Service
// client reconnects through transient failures on its own; follow handles
// what it gives up on, waiting and starting again, so an operator who
// fixes the remote node does not have to restart this one.
func (r *RemoteEvents) follow(ctx context.Context, src RemoteEventSource) {
	log := r.cfg.Logger.With("component", "remote-events", "source", src.Name)
	reach := &reachabilityClient{next: r.httpClient(src.URL), log: log}
	client := eventclient.New(reach, src.URL)
	initial := r.cfg.InitialBackoff
	if initial <= 0 {
		initial = remoteInitialBackoff
	}
	maxBackoff := r.cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = remoteMaxBackoff
	}
	var last uint64
	wait := initial
	for ctx.Err() == nil {
		err := client.SubscribeWithAck(ctx, eventclient.AckConfig{
			SubscriberID:      r.cfg.NodeID + "/" + src.Name,
			EventTypes:        src.EventTypes,
			OriginNodeIDs:     src.OriginNodeIDs,
			LastAckedSequence: last,
			InitialBackoff:    initial,
			MaxBackoff:        maxBackoff,
		}, func(_ context.Context, ev *eventv1alpha1.Event) error {
			r.cfg.EventBus.Publish(
				RemoteEventType,
				event.NewEvent(
					RemoteEventType,
					RemoteEvent{Source: src.Name, Event: ev},
				),
			)
			last = ev.GetSequence()
			reach.reachable()
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if connect.CodeOf(err) == connect.CodeOutOfRange {
			// The remote node no longer buffers the position this node
			// stopped at. Resuming from its current position is the only
			// way forward; the events in between are gone.
			log.Warn(
				"remote node no longer buffers the resume position; "+
					"restarting from its current position",
				"last_sequence", last,
			)
			last = 0
		} else {
			log.Warn("remote event subscription ended", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, maxBackoff)
	}
}

// reachabilityClient logs when a source stops answering and when it answers
// again. The Event Service client retries failed connections internally and
// reports nothing, so without it an unreachable source would be silent for
// the whole outage. Only the first failure of an outage is logged, because
// the client retries every few seconds.
type reachabilityClient struct {
	next connect.HTTPClient
	log  *slog.Logger
	down atomic.Bool
}

func (c *reachabilityClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.next.Do(req)
	if req.Context().Err() != nil {
		return resp, err
	}
	// A 5xx answer comes from a proxy or a failing server rather than the
	// Event Service, so it counts as unreachable too.
	if err == nil && resp.StatusCode < http.StatusInternalServerError {
		c.reachable()
		return resp, err
	}
	if c.down.CompareAndSwap(false, true) {
		if err != nil {
			c.log.Warn(
				"remote event source unreachable; retrying",
				"host", req.URL.Host, "error", err,
			)
		} else {
			c.log.Warn(
				"remote event source unreachable; retrying",
				"host", req.URL.Host, "status", resp.StatusCode,
			)
		}
	}
	return resp, err
}

// reachable ends a reported outage. It is also called for each event
// received, because a streaming response's headers can wait for the first
// event, so Do may not return for a while after the source is back.
func (c *reachabilityClient) reachable() {
	if c.down.CompareAndSwap(true, false) {
		c.log.Info("remote event source reachable again")
	}
}
