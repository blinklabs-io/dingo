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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/bark/eventclient"
	"github.com/blinklabs-io/bark/eventtype"
	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	eventconnect "github.com/blinklabs-io/bark/proto/v1alpha1/event/eventv1alpha1connect"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

type eventFixture struct {
	srv    *httptest.Server
	bus    *event.EventBus
	hub    *eventHub
	client eventconnect.EventServiceClient
}

func newEventFixture(
	t *testing.T,
	mutate func(*BarkConfig),
	publish bool,
) *eventFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	cfg := BarkConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:    bus,
		EventNodeID: "node-a",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	hub := newEventHub(cfg)
	hub.start()
	mux := http.NewServeMux()
	path, handler := eventconnect.NewEventServiceHandler(
		&eventServiceHandler{hub: hub, publishEnabled: publish},
	)
	mux.Handle(path, handler)
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	// Cleanups run last-in first-out: the hub must end its streams before
	// the server waits for them.
	t.Cleanup(srv.Close)
	t.Cleanup(hub.close)
	return &eventFixture{
		srv: srv,
		bus: bus,
		hub: hub,
		client: eventconnect.NewEventServiceClient(
			srv.Client(),
			srv.URL,
		),
	}
}

func (f *eventFixture) publishChurn(addr string) {
	f.bus.Publish(
		peergov.PeerChurnEventType,
		event.NewEvent(
			peergov.PeerChurnEventType,
			peergov.PeerChurnEvent{Address: addr, OldState: "hot", NewState: "warm"},
		),
	)
}

func recvEvent(
	t *testing.T,
	recv func() (*eventv1alpha1.Event, error),
) *eventv1alpha1.Event {
	t.Helper()
	type result struct {
		ev  *eventv1alpha1.Event
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ev, err := recv()
		ch <- result{ev, err}
	}()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")
		return nil
	}
}

// subStream wraps a server stream whose Subscribe call returns only once the
// server sends its first message, so the call runs on its own goroutine.
type subStream struct {
	ready  chan struct{}
	stream *connect.ServerStreamForClient[eventv1alpha1.SubscribeResponse]
	err    error
}

func (s *subStream) wait(t *testing.T) {
	t.Helper()
	select {
	case <-s.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe never returned")
	}
	require.NoError(t, s.err)
}

func (s *subStream) Receive() bool {
	<-s.ready
	return s.err == nil && s.stream.Receive()
}

func (s *subStream) Msg() *eventv1alpha1.SubscribeResponse {
	return s.stream.Msg()
}

func (s *subStream) Err() error {
	<-s.ready
	if s.err != nil {
		return s.err
	}
	return s.stream.Err()
}

func startSubscribe(
	t *testing.T,
	f *eventFixture,
	types ...string,
) *subStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &subStream{ready: make(chan struct{})}
	go func() {
		defer close(s.ready)
		s.stream, s.err = f.client.Subscribe(ctx, connect.NewRequest(
			&eventv1alpha1.SubscribeRequest{
				Filter: &eventv1alpha1.EventFilter{EventTypes: types},
			},
		))
	}()
	return s
}

// openSubscribe starts a subscription and waits until the hub has counted it,
// so a following publish cannot race the subscription.
func openSubscribe(
	t *testing.T,
	f *eventFixture,
	types ...string,
) *subStream {
	t.Helper()
	s := startSubscribe(t, f, types...)
	require.Eventually(t, func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		return f.hub.active > 0
	}, 5*time.Second, 5*time.Millisecond)
	return s
}

func TestEventSubscribeDeliversConvertedEvents(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := openSubscribe(t, f, eventtype.PeerChurn)

	f.publishChurn("10.0.0.1:3001")
	ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
		if !stream.Receive() {
			return nil, stream.Err()
		}
		return stream.Msg().GetEvent(), nil
	})
	require.Equal(t, eventtype.PeerChurn, ev.GetEventType())
	require.Equal(t, "node-a", ev.GetOriginNodeId())
	require.NotEmpty(t, ev.GetId())
	var data eventv1alpha1.PeerChurnEventData
	require.NoError(t, ev.GetTypedData().UnmarshalTo(&data))
	require.Equal(t, "10.0.0.1:3001", data.GetAddress())
	require.Equal(t, "warm", data.GetNewState())
}

func TestEventSubscribeFilterExcludesOtherTypes(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := openSubscribe(t, f, eventtype.MempoolAddTx)

	f.publishChurn("a:1")
	f.bus.Publish(
		mempool.AddTransactionEventType,
		event.NewEvent(
			mempool.AddTransactionEventType,
			mempool.AddTransactionEvent{Hash: "abcd", Body: []byte{1, 2, 3}},
		),
	)
	ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
		if !stream.Receive() {
			return nil, stream.Err()
		}
		return stream.Msg().GetEvent(), nil
	})
	require.Equal(t, eventtype.MempoolAddTx, ev.GetEventType())
	var data eventv1alpha1.TransactionEventData
	require.NoError(t, ev.GetTypedData().UnmarshalTo(&data))
	require.Equal(t, uint64(3), data.GetSizeBytes())
}

func TestEventSubscribeRejectsUnknownType(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := startSubscribe(t, f, "nope.x")
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(stream.Err()))
}

// A subscriber that never reads must not slow the node's own EventBus
// publishers or other subscribers.
func TestEventSlowSubscriberDoesNotStallBusOrOthers(t *testing.T) {
	f := newEventFixture(t, nil, false)
	_ = openSubscribe(t, f) // never read from
	fast := startSubscribe(t, f)
	waitActive(t, f, 2)

	start := time.Now()
	for i := range 200 {
		f.publishChurn("p:" + string(rune('a'+i%26)))
	}
	require.Less(t, time.Since(start), 2*time.Second)

	// The fast reader still sees events after the burst.
	f.publishChurn("tail:1")
	deadline := time.After(5 * time.Second)
	for {
		ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
			if !fast.Receive() {
				return nil, fast.Err()
			}
			return fast.Msg().GetEvent(), nil
		})
		var d eventv1alpha1.PeerChurnEventData
		require.NoError(t, ev.GetTypedData().UnmarshalTo(&d))
		if d.GetAddress() == "tail:1" {
			return
		}
		select {
		case <-deadline:
			t.Fatal("fast subscriber never saw the last event")
		default:
		}
	}
}

func TestEventHubDetachesLaggingReader(t *testing.T) {
	f := newEventFixture(t, func(c *BarkConfig) { c.EventBufferSize = 4 }, false)
	for range 10 {
		f.hub.append(&eventv1alpha1.Event{EventType: eventtype.PeerChurn})
	}
	_, _, lagged, _ := f.hub.read(1, func(*eventv1alpha1.Event) bool { return true }, 10)
	require.True(t, lagged, "events 2-6 left the 4-slot ring")
	evs, next, lagged, _ := f.hub.read(6, func(*eventv1alpha1.Event) bool { return true }, 10)
	require.False(t, lagged)
	require.Len(t, evs, 4)
	require.Equal(t, uint64(10), next)
}

type ackSession struct {
	stream *connect.BidiStreamForClient[
		eventv1alpha1.SubscribeWithAckRequest,
		eventv1alpha1.SubscribeWithAckResponse,
	]
	cancel context.CancelFunc
}

func openAck(
	t *testing.T,
	f *eventFixture,
	id string,
	lastAcked uint64,
) *ackSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := f.client.SubscribeWithAck(ctx)
	require.NoError(t, stream.Send(&eventv1alpha1.SubscribeWithAckRequest{
		Message: &eventv1alpha1.SubscribeWithAckRequest_Start{
			Start: &eventv1alpha1.SubscribeWithAckStart{
				SubscriberId:      id,
				LastAckedSequence: lastAcked,
			},
		},
	}))
	return &ackSession{stream: stream, cancel: cancel}
}

func (a *ackSession) recv(t *testing.T) *eventv1alpha1.Event {
	t.Helper()
	return recvEvent(t, func() (*eventv1alpha1.Event, error) {
		msg, err := a.stream.Receive()
		if err != nil {
			return nil, err
		}
		return msg.GetEvent(), nil
	})
}

func (a *ackSession) ack(t *testing.T, seq uint64) {
	t.Helper()
	require.NoError(t, a.stream.Send(&eventv1alpha1.SubscribeWithAckRequest{
		Message: &eventv1alpha1.SubscribeWithAckRequest_Ack{
			Ack: &eventv1alpha1.SubscribeWithAckAck{Sequence: seq},
		},
	}))
}

func waitActive(t *testing.T, f *eventFixture, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		return f.hub.active == n
	}, 5*time.Second, 5*time.Millisecond)
}

// The acknowledged position survives a dropped stream: reconnecting with it
// yields exactly the events that were not acknowledged, none lost.
func TestEventAckResumesAfterDisconnect(t *testing.T) {
	f := newEventFixture(t, nil, false)
	first := openAck(t, f, "sub-1", 0)
	waitActive(t, f, 1)

	for _, a := range []string{"a:1", "b:1", "c:1"} {
		f.publishChurn(a)
	}
	var seqs []uint64
	for range 3 {
		seqs = append(seqs, first.recv(t).GetSequence())
	}
	first.ack(t, seqs[1])
	require.NoError(t, first.stream.CloseRequest())
	waitActive(t, f, 0)

	f.publishChurn("d:1")
	f.publishChurn("e:1")

	second := openAck(t, f, "sub-1", seqs[1])
	var got []string
	for range 3 {
		var d eventv1alpha1.PeerChurnEventData
		require.NoError(t, second.recv(t).GetTypedData().UnmarshalTo(&d))
		got = append(got, d.GetAddress())
	}
	require.Equal(t, []string{"c:1", "d:1", "e:1"}, got)
}

// A filtered ack stream must not count events the filter rejected toward
// its send window: the client has nothing to acknowledge for them.
func TestEventAckFilteredStreamIgnoresRejectedEvents(t *testing.T) {
	t.Parallel()
	f := newEventFixture(t, func(c *BarkConfig) {
		c.EventBufferSize = 4
	}, false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := f.client.SubscribeWithAck(ctx)
	require.NoError(t, stream.Send(&eventv1alpha1.SubscribeWithAckRequest{
		Message: &eventv1alpha1.SubscribeWithAckRequest_Start{
			Start: &eventv1alpha1.SubscribeWithAckStart{
				SubscriberId: "filtered",
				Filter: &eventv1alpha1.EventFilter{
					EventTypes: []string{eventtype.EpochTransition},
				},
			},
		},
	}))
	waitActive(t, f, 1)

	// One at a time, so the stream scans each rejected event on its own
	// instead of reading them all in one batch.
	for i := range 3 {
		f.publishChurn(fmt.Sprintf("r:%d", i))
		require.Eventually(t, func() bool {
			return f.hub.position() == uint64(i+1)
		}, 5*time.Second, 5*time.Millisecond)
	}
	f.bus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, event.EpochTransitionEvent{
			PreviousEpoch: 1, NewEpoch: 2,
		}),
	)
	ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
		resp, err := stream.Receive()
		if err != nil {
			return nil, err
		}
		return resp.GetEvent(), nil
	})
	require.Equal(t, eventtype.EpochTransition, ev.GetEventType())
}

// A one-event buffer still delivers to an ack subscriber: the send window is
// never zero.
func TestEventAckDeliversWithSingleEventBuffer(t *testing.T) {
	t.Parallel()
	f := newEventFixture(t, func(c *BarkConfig) {
		c.EventBufferSize = 1
	}, false)
	sess := openAck(t, f, "tiny", 0)
	waitActive(t, f, 1)
	f.publishChurn("r:1")
	require.NotNil(t, sess.recv(t))
}

func TestEventAckOutOfRangeWhenNotBuffered(t *testing.T) {
	f := newEventFixture(t, func(c *BarkConfig) { c.EventBufferSize = 4 }, false)
	for range 10 {
		f.hub.append(&eventv1alpha1.Event{EventType: eventtype.PeerChurn})
	}
	s := openAck(t, f, "sub-old", 2)
	_, err := s.stream.Receive()
	require.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))

	future := openAck(t, f, "sub-future", 999)
	_, err = future.stream.Receive()
	require.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))
}

func TestEventAckRejectsDuplicateSubscriberID(t *testing.T) {
	f := newEventFixture(t, nil, false)
	_ = openAck(t, f, "dup", 0)
	waitActive(t, f, 1)
	second := openAck(t, f, "dup", 0)
	_, err := second.stream.Receive()
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestEventAckRequiresStartFirst(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := f.client.SubscribeWithAck(context.Background())
	require.NoError(t, stream.Send(&eventv1alpha1.SubscribeWithAckRequest{
		Message: &eventv1alpha1.SubscribeWithAckRequest_Ack{
			Ack: &eventv1alpha1.SubscribeWithAckAck{Sequence: 1},
		},
	}))
	_, err := stream.Receive()
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestEventSubscriberLimits(t *testing.T) {
	f := newEventFixture(t, func(c *BarkConfig) {
		c.EventMaxSubscribers = 1
	}, false)
	_ = openSubscribe(t, f)
	second := startSubscribe(t, f)
	require.False(t, second.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(second.Err()))

	g := newEventFixture(t, func(c *BarkConfig) {
		c.EventMaxSubscribersPerClient = 1
	}, false)
	_ = openSubscribe(t, g)
	third := startSubscribe(t, g)
	require.False(t, third.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(third.Err()))
}

func TestEventSubscriberLimitsUnlimitedByDefault(t *testing.T) {
	t.Parallel()
	f := newEventFixture(t, nil, false)
	const streams = 80
	for range streams {
		_ = startSubscribe(t, f)
	}
	require.Eventually(t, func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		return f.hub.active == streams
	}, 5*time.Second, 5*time.Millisecond)
}

func TestEventPublishDisabledByDefault(t *testing.T) {
	f := newEventFixture(t, nil, false)
	_, err := f.client.Publish(context.Background(), connect.NewRequest(
		&eventv1alpha1.PublishRequest{Event: &eventv1alpha1.Event{
			Id: "x", EventType: eventtype.PeerChurn,
		}},
	))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	_, err = f.client.PublishBatch(context.Background(), connect.NewRequest(
		&eventv1alpha1.PublishBatchRequest{},
	))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestEventPublishAndBatch(t *testing.T) {
	f := newEventFixture(t, nil, true)
	stream := openSubscribe(t, f)

	payload, err := anypb.New(&eventv1alpha1.PeerChurnEventData{Address: "r:1"})
	require.NoError(t, err)
	resp, err := f.client.PublishBatch(context.Background(), connect.NewRequest(
		&eventv1alpha1.PublishBatchRequest{Events: []*eventv1alpha1.Event{
			{Id: "ok", EventType: eventtype.PeerChurn, OriginNodeId: "node-b", TypedData: payload},
			{Id: "bad", EventType: "nope.x"},
			{Id: "self", EventType: eventtype.PeerChurn, OriginNodeId: "node-a"},
			{Id: "wrongpayload", EventType: eventtype.MempoolAddTx, TypedData: payload},
			{EventType: eventtype.PeerChurn},
			{Id: "noorigin", EventType: eventtype.PeerChurn},
		}},
	))
	require.NoError(t, err)
	res := resp.Msg.GetResults()
	require.Len(t, res, 6)
	require.Nil(t, res[0].GetFailure())
	require.Equal(t,
		eventv1alpha1.PublishFailureCode_PUBLISH_FAILURE_CODE_UNKNOWN_EVENT_TYPE,
		res[1].GetFailure().GetCode())
	for _, i := range []int{2, 3, 4, 5} {
		require.Equal(t,
			eventv1alpha1.PublishFailureCode_PUBLISH_FAILURE_CODE_INVALID_EVENT,
			res[i].GetFailure().GetCode(), "result %d", i)
	}

	ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
		if !stream.Receive() {
			return nil, stream.Err()
		}
		return stream.Msg().GetEvent(), nil
	})
	require.Equal(t, "ok", ev.GetId())
	require.Equal(t, "node-b", ev.GetOriginNodeId())
	require.NotZero(t, ev.GetSequence())

	_, err = f.client.Publish(context.Background(), connect.NewRequest(
		&eventv1alpha1.PublishRequest{Event: &eventv1alpha1.Event{
			Id: "p", EventType: "nope.x",
		}},
	))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestEventListAndSchema(t *testing.T) {
	f := newEventFixture(t, nil, false)
	list, err := f.client.ListEventTypes(context.Background(),
		connect.NewRequest(&eventv1alpha1.ListEventTypesRequest{}))
	require.NoError(t, err)
	require.Len(t, list.Msg.GetEventTypes(), len(eventtype.Infos()))

	schema, err := f.client.GetEventSchema(context.Background(),
		connect.NewRequest(&eventv1alpha1.GetEventSchemaRequest{
			EventType: eventtype.PeerChurn,
		}))
	require.NoError(t, err)
	require.Equal(t,
		"bark.v1alpha1.event.PeerChurnEventData",
		schema.Msg.GetPayloadMessage())
	require.NotEmpty(t, schema.Msg.GetPayloadDescriptors().GetFile())

	_, err = f.client.GetEventSchema(context.Background(),
		connect.NewRequest(&eventv1alpha1.GetEventSchemaRequest{EventType: "nope.x"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestEventAdapterConvertsRollbackAndEpoch(t *testing.T) {
	a := &eventAdapter{
		eraNameForEpoch: func(uint64) (string, error) { return "conway", nil },
	}
	out := a.convert(
		event.NewEvent("ledger.epoch_transition", event.EpochTransitionEvent{
			PreviousEpoch: 4, NewEpoch: 5, BoundarySlot: 500,
		}),
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)
	require.Len(t, out, 1)
	require.Equal(t, eventtype.EpochTransition, out[0].GetEventType())
	var d eventv1alpha1.EpochTransitionEventData
	require.NoError(t, out[0].GetTypedData().UnmarshalTo(&d))
	require.Equal(t, uint64(5), d.GetNewEpoch())
	require.Equal(t, "conway", d.GetEra())
}

func TestEventAdapterConvertsRollback(t *testing.T) {
	a := &eventAdapter{}
	out := a.convert(
		event.NewEvent("chain.update", chain.ChainRollbackEvent{
			Point: ocommon.Point{Slot: 90, Hash: []byte{9}},
			RolledBackBlocks: []models.Block{
				{Slot: 100}, {Slot: 95},
			},
		}),
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)
	require.Len(t, out, 1)
	require.Equal(t, eventtype.ChainUpdate, out[0].GetEventType())
	var payload eventv1alpha1.ChainUpdateEventData
	require.NoError(t, out[0].GetTypedData().UnmarshalTo(&payload))
	d := payload.GetRollback()
	require.NotNil(t, d)
	require.Equal(t, uint64(100), d.GetFromSlot())
	require.Equal(t, uint64(90), d.GetToSlot())
	require.Equal(t, uint32(2), d.GetBlocksRolledBack())
	require.Equal(t, []byte{9}, d.GetNewTipHash())
}

func TestEventSubscribeCategoryPattern(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := openSubscribe(t, f, "peergov.*")

	f.bus.Publish(
		mempool.AddTransactionEventType,
		event.NewEvent(
			mempool.AddTransactionEventType,
			mempool.AddTransactionEvent{Hash: "abcd"},
		),
	)
	f.publishChurn("a:1")
	ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
		if !stream.Receive() {
			return nil, stream.Err()
		}
		return stream.Msg().GetEvent(), nil
	})
	require.Equal(t, eventtype.PeerChurn, ev.GetEventType())
}

func TestEventSubscribeRejectsUnknownPattern(t *testing.T) {
	f := newEventFixture(t, nil, false)
	stream := startSubscribe(t, f, "nope.*")
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(stream.Err()))
}

func TestEventSubscribeOriginFilter(t *testing.T) {
	f := newEventFixture(t, nil, false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan *eventv1alpha1.Event, 1)
	// Connect holds the response headers until the first event, so the
	// call cannot be made inline.
	go func() {
		stream, err := f.client.Subscribe(ctx, connect.NewRequest(
			&eventv1alpha1.SubscribeRequest{
				Filter: &eventv1alpha1.EventFilter{
					OriginNodeIds: []string{"node-a"},
				},
			},
		))
		if err == nil && stream.Receive() {
			got <- stream.Msg().GetEvent()
		}
	}()
	waitActive(t, f, 1)

	f.publishChurn("local:1")
	ev := recvEvent(t, func() (*eventv1alpha1.Event, error) {
		return <-got, nil
	})
	require.Equal(t, "node-a", ev.GetOriginNodeId())
}

func TestEventSubscribeOriginFilterExcludesOtherNodes(t *testing.T) {
	f := newEventFixture(t, nil, false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	got := make(chan *eventv1alpha1.Event, 1)
	go func() {
		stream, err := f.client.Subscribe(ctx, connect.NewRequest(
			&eventv1alpha1.SubscribeRequest{
				Filter: &eventv1alpha1.EventFilter{
					OriginNodeIds: []string{"node-other"},
				},
			},
		))
		if err == nil && stream.Receive() {
			got <- stream.Msg().GetEvent()
		}
	}()
	waitActive(t, f, 1)

	f.publishChurn("local:1")
	select {
	case ev := <-got:
		t.Fatalf("received an event from %q, want none", ev.GetOriginNodeId())
	case <-time.After(300 * time.Millisecond):
	}
}

// An ack subscriber using the shipped client keeps every event, in order,
// across a dropped connection.
func TestEventClientAckRecoversFromDroppedConnection(t *testing.T) {
	f := newEventFixture(t, nil, false)
	client := eventclient.New(f.srv.Client(), f.srv.URL)

	var mu sync.Mutex
	var got []string
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- client.SubscribeWithAck(ctx, eventclient.AckConfig{
			SubscriberID:   "e2e",
			EventTypes:     []string{"peergov.*"},
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		}, func(_ context.Context, ev *eventv1alpha1.Event) error {
			var d eventv1alpha1.PeerChurnEventData
			if err := ev.GetTypedData().UnmarshalTo(&d); err != nil {
				return err
			}
			mu.Lock()
			got = append(got, d.GetAddress())
			mu.Unlock()
			return nil
		})
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(got)
	}
	waitActive(t, f, 1)
	f.publishChurn("a:1")
	f.publishChurn("b:1")
	require.Eventually(t, func() bool { return count() == 2 },
		5*time.Second, 5*time.Millisecond)

	f.srv.CloseClientConnections()
	f.publishChurn("c:1")
	f.publishChurn("d:1")

	require.Eventually(t, func() bool { return count() == 4 },
		10*time.Second, 5*time.Millisecond)
	mu.Lock()
	require.Equal(t, []string{"a:1", "b:1", "c:1", "d:1"}, got)
	mu.Unlock()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("SubscribeWithAck did not return after its context was canceled")
	}
}
