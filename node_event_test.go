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

package dingo

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

func newRemoteEventsNode(
	t *testing.T,
	sources ...internalconfig.BarkEventSource,
) *Node {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Node{
		ctx:      ctx,
		eventBus: bus,
		config: Config{
			logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			cfg:    &internalconfig.Config{BarkEventSources: sources},
		},
	}
}

func TestStartRemoteEventsIsOffWithoutSources(t *testing.T) {
	t.Parallel()
	n := newRemoteEventsNode(t)
	require.NoError(t, n.startRemoteEvents())
	require.Nil(t, n.remoteEvents)
}

func TestStartRemoteEventsRejectsInvalidSource(t *testing.T) {
	t.Parallel()
	n := newRemoteEventsNode(t, internalconfig.BarkEventSource{
		Name: "a", URL: "ftp://nowhere",
	})
	require.Error(t, n.startRemoteEvents())
	require.Nil(t, n.remoteEvents)
}

// The node subscribes to each configured source's EventService, and stops
// doing so when the subscriber is stopped.
func TestStartRemoteEventsSubscribesToConfiguredSources(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/SubscribeWithAck") {
				hits.Add(1)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		},
	))
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = protocols
	srv.Start()
	t.Cleanup(srv.Close)

	n := newRemoteEventsNode(t, internalconfig.BarkEventSource{
		Name: "a", URL: srv.URL,
	})
	require.NoError(t, n.startRemoteEvents())
	require.NotNil(t, n.remoteEvents)
	require.Eventually(t, func() bool { return hits.Load() > 0 },
		10*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, n.remoteEvents.Stop(ctx))
	stopped := hits.Load()
	require.Never(t, func() bool { return hits.Load() > stopped },
		300*time.Millisecond, 20*time.Millisecond)
}

// The source filters are copied in and out of the configuration, so neither
// the caller's slices nor a getter result can change this node's filters.
func TestBarkEventSourcesAreCopiedAtTheConfigBoundary(t *testing.T) {
	t.Parallel()

	types := []string{"chain.update"}
	origins := []string{"node-a"}
	cfg := NewConfig(WithBarkEventSources(BarkEventSource{
		Name: "a", URL: "http://a:9090",
		EventTypes: types, OriginNodeIDs: origins,
	}))
	types[0] = "mempool.*"
	origins[0] = "node-z"

	got := cfg.BarkEventSources()
	require.Equal(t, []string{"chain.update"}, got[0].EventTypes)
	require.Equal(t, []string{"node-a"}, got[0].OriginNodeIDs)

	got[0].EventTypes[0] = "mempool.*"
	got[0].OriginNodeIDs[0] = "node-z"
	again := cfg.BarkEventSources()
	require.Equal(t, []string{"chain.update"}, again[0].EventTypes)
	require.Equal(t, []string{"node-a"}, again[0].OriginNodeIDs)
}
