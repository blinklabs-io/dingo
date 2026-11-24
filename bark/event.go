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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/bark/eventtype"
	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	eventconnect "github.com/blinklabs-io/bark/proto/v1alpha1/event/eventv1alpha1connect"
	"github.com/blinklabs-io/dingo/event"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultEventBufferSize is how many delivered events the Event Service
	// keeps for resuming and lagging subscribers.
	DefaultEventBufferSize = 4096
	// eventIngestQueue bounds the events waiting to be converted. Beyond it,
	// events are dropped for remote subscribers only.
	eventIngestQueue = 4096
	// eventAckTimeout is how long an acknowledging subscriber may leave a
	// full send window unacknowledged before its stream is closed.
	eventAckTimeout = 30 * time.Second
)

// eventHub turns the node's EventBus traffic into the Event Service's
// delivery stream. The EventBus side never waits on a remote subscriber:
// bus handlers only enqueue, a single goroutine converts, and subscribers
// read a shared bounded ring at their own pace. A subscriber that falls more
// than the ring behind is detached instead of slowing anything down.
type eventHub struct {
	logger     *slog.Logger
	nodeID     string
	idPrefix   string
	adapter    *eventAdapter
	bus        *event.EventBus
	subs       []busSubscription
	ingest     chan event.Event
	dropped    atomic.Uint64
	closeOnce  sync.Once
	closed     chan struct{}
	maxSubs    int
	maxPerHost int
	window     uint64

	mu      sync.Mutex
	ring    []*eventv1alpha1.Event
	last    uint64
	wake    chan struct{}
	active  int
	perHost map[string]int
	ackIDs  map[string]struct{}
}

type busSubscription struct {
	eventType event.EventType
	id        event.EventSubscriberId
}

func newEventHub(cfg BarkConfig) *eventHub {
	size := cfg.EventBufferSize
	if size <= 0 {
		size = DefaultEventBufferSize
	}
	maxSubs := max(cfg.EventMaxSubscribers, 0)
	maxPerHost := max(cfg.EventMaxSubscribersPerClient, 0)
	nodeID := cfg.EventNodeID
	return &eventHub{
		logger:   cfg.Logger,
		nodeID:   nodeID,
		idPrefix: nodeID + "-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		adapter: &eventAdapter{
			epochForSlot:    cfg.EventEpochForSlot,
			eraNameForEpoch: cfg.EventEraNameForEpoch,
		},
		bus:        cfg.EventBus,
		ingest:     make(chan event.Event, eventIngestQueue),
		closed:     make(chan struct{}),
		maxSubs:    maxSubs,
		maxPerHost: maxPerHost,
		// A send window of half the ring leaves the other half for
		// events arriving while the client catches up on acks.
		window:  max(uint64(size/2), 1),
		ring:    make([]*eventv1alpha1.Event, size),
		wake:    make(chan struct{}),
		perHost: make(map[string]int),
		ackIDs:  make(map[string]struct{}),
	}
}

// start subscribes to the EventBus and begins converting events.
func (h *eventHub) start() {
	for _, name := range h.adapter.sourceTypes() {
		t := event.EventType(name)
		// A large buffer absorbs bursts; the handler itself only enqueues,
		// so the bus never detaches this subscriber for being slow.
		id := h.bus.SubscribeFuncWithBuffer(t, eventIngestQueue, h.enqueue)
		if id != 0 {
			h.subs = append(h.subs, busSubscription{eventType: t, id: id})
		}
	}
	go h.convertLoop()
}

func (h *eventHub) enqueue(evt event.Event) {
	select {
	case h.ingest <- evt:
	default:
		if n := h.dropped.Add(1); n == 1 || n%1000 == 0 {
			h.logger.Warn(
				"event service dropped events for remote subscribers",
				"dropped_total", n,
			)
		}
	}
}

func (h *eventHub) convertLoop() {
	for {
		select {
		case <-h.closed:
			return
		case evt := <-h.ingest:
			for _, out := range h.adapter.convert(evt, h.logger) {
				h.append(out)
			}
		}
	}
}

// close detaches from the EventBus and ends every stream.
func (h *eventHub) close() {
	h.closeOnce.Do(func() {
		close(h.closed)
		for _, s := range h.subs {
			h.bus.Unsubscribe(s.eventType, s.id)
		}
	})
}

// append assigns the next sequence to ev and wakes the subscribers.
func (h *eventHub) append(ev *eventv1alpha1.Event) {
	h.mu.Lock()
	h.appendLocked(ev)
	h.mu.Unlock()
}

// appendLocked is append for a caller that holds h.mu.
func (h *eventHub) appendLocked(ev *eventv1alpha1.Event) {
	h.last++
	ev.Sequence = h.last
	if ev.GetId() == "" {
		ev.Id = h.idPrefix + "-" + strconv.FormatUint(h.last, 10)
	}
	if ev.GetOriginNodeId() == "" {
		ev.OriginNodeId = h.nodeID
	}
	if ev.GetTimestamp() == nil {
		ev.Timestamp = timestamppb.Now()
	}
	h.ring[(h.last-1)%uint64(len(h.ring))] = ev
	close(h.wake)
	h.wake = make(chan struct{})
}

// position returns the sequence of the newest event.
func (h *eventHub) position() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.last
}

// oldestLocked returns the lowest sequence still in the ring.
func (h *eventHub) oldestLocked() uint64 {
	size := uint64(len(h.ring))
	if h.last <= size {
		return 1
	}
	return h.last - size + 1
}

// lagged reports whether events after cursor have already left the ring.
func (h *eventHub) lagged(cursor uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.laggedLocked(cursor)
}

func (h *eventHub) laggedLocked(cursor uint64) bool {
	return cursor+1 < h.oldestLocked() && cursor < h.last
}

// read returns up to max matching events after cursor, the new cursor, and a
// channel closed when more events arrive. lagged is true when events after
// cursor have already left the ring.
func (h *eventHub) read(
	cursor uint64,
	match func(*eventv1alpha1.Event) bool,
	max int,
) (out []*eventv1alpha1.Event, next uint64, lagged bool, wake <-chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.laggedLocked(cursor) {
		return nil, cursor, true, nil
	}
	next = cursor
	for seq := cursor + 1; seq <= h.last && len(out) < max; seq++ {
		ev := h.ring[(seq-1)%uint64(len(h.ring))]
		next = seq
		if match(ev) {
			out = append(out, ev)
		}
	}
	return out, next, false, h.wake
}

// admit reserves a stream slot for the caller, or reports which limit is hit.
// admitAtHead is admit that also returns the head sequence in the same
// critical section, so a subscription counted as active has a start position
// that no later event can precede.
func (h *eventHub) admitAtHead(
	host string,
) (release func(), head uint64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err = h.admitLocked(host)
	return release, h.last, err
}

func (h *eventHub) admit(host string) (release func(), err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.admitLocked(host)
}

func (h *eventHub) admitLocked(host string) (release func(), err error) {
	if h.maxSubs > 0 && h.active >= h.maxSubs {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			errors.New("too many event subscribers"),
		)
	}
	if h.maxPerHost > 0 && h.perHost[host] >= h.maxPerHost {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			errors.New("too many event subscribers from this address"),
		)
	}
	h.active++
	h.perHost[host]++
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			h.active--
			if h.perHost[host]--; h.perHost[host] <= 0 {
				delete(h.perHost, host)
			}
			h.mu.Unlock()
		})
	}, nil
}

func (h *eventHub) claimSubscriberID(id string) (release func(), ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, taken := h.ackIDs[id]; taken {
		return nil, false
	}
	h.ackIDs[id] = struct{}{}
	return func() {
		h.mu.Lock()
		delete(h.ackIDs, id)
		h.mu.Unlock()
	}, true
}

// eventServiceHandler implements the EventService on top of eventHub.
type eventServiceHandler struct {
	hub            *eventHub
	publishEnabled bool
}

var _ eventconnect.EventServiceHandler = (*eventServiceHandler)(nil)

func filterMatcher(
	f *eventv1alpha1.EventFilter,
) (func(*eventv1alpha1.Event) bool, error) {
	matchType, err := eventtype.Matcher(f.GetEventTypes())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	origins := make(map[string]struct{}, len(f.GetOriginNodeIds()))
	for _, id := range f.GetOriginNodeIds() {
		origins[id] = struct{}{}
	}
	return func(ev *eventv1alpha1.Event) bool {
		if !matchType(ev.GetEventType()) {
			return false
		}
		if len(origins) == 0 {
			return true
		}
		_, ok := origins[ev.GetOriginNodeId()]
		return ok
	}, nil
}

func (s *eventServiceHandler) Subscribe(
	ctx context.Context,
	req *connect.Request[eventv1alpha1.SubscribeRequest],
	stream *connect.ServerStream[eventv1alpha1.SubscribeResponse],
) error {
	match, err := filterMatcher(req.Msg.GetFilter())
	if err != nil {
		return err
	}
	host := archiveCallerKey(req.Peer().Addr)
	release, cursor, err := s.hub.admitAtHead(host)
	if err != nil {
		return err
	}
	defer release()
	log := s.hub.logger.With("rpc", "Subscribe", "client", host)
	log.Info("event subscription opened")
	defer log.Info("event subscription closed")
	lagErr := func() error {
		log.Warn("event subscriber fell behind and was detached")
		return connect.NewError(
			connect.CodeResourceExhausted,
			errors.New("subscriber fell behind and was detached"),
		)
	}
	guard := s.hub.guardSends(ctx)
	defer guard.stop()

	for {
		events, next, lagged, wake := s.hub.read(cursor, match, 64)
		if lagged {
			return lagErr()
		}
		cursor = next
		if len(events) > 0 {
			if err := guard.begin(cursor, lagErr); err != nil {
				return err
			}
			for _, ev := range events {
				if err := stream.Send(
					&eventv1alpha1.SubscribeResponse{Event: ev},
				); err != nil {
					return guard.sendError(err, lagErr)
				}
			}
			guard.end()
			continue
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return nil
		case <-s.hub.closed:
			return errEventServiceClosing()
		}
	}
}

func (s *eventServiceHandler) SubscribeWithAck(
	ctx context.Context,
	stream *connect.BidiStream[
		eventv1alpha1.SubscribeWithAckRequest,
		eventv1alpha1.SubscribeWithAckResponse,
	],
) error {
	first, err := stream.Receive()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("first message must be start"),
		)
	}
	id := start.GetSubscriberId()
	if id == "" {
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("subscriber_id is required"),
		)
	}
	match, err := filterMatcher(start.GetFilter())
	if err != nil {
		return err
	}
	host := archiveCallerKey(stream.Peer().Addr)
	release, err := s.hub.admit(host)
	if err != nil {
		return err
	}
	defer release()
	releaseID, ok := s.hub.claimSubscriberID(id)
	if !ok {
		return connect.NewError(
			connect.CodeAlreadyExists,
			errors.New("subscriber_id already has an open stream"),
		)
	}
	defer releaseID()

	cursor := start.GetLastAckedSequence()
	head := s.hub.position()
	if cursor == 0 {
		cursor = head
	}
	s.hub.mu.Lock()
	oldest := s.hub.oldestLocked()
	s.hub.mu.Unlock()
	if cursor > head || (cursor < head && cursor+1 < oldest) {
		return connect.NewError(
			connect.CodeOutOfRange,
			fmt.Errorf(
				"sequence %d is not buffered; buffered range is %d-%d",
				cursor, oldest, head,
			),
		)
	}
	log := s.hub.logger.With(
		"rpc", "SubscribeWithAck", "client", host, "subscriber_id", id,
	)
	log.Info("event subscription opened", "resume_from", cursor)
	defer log.Info("event subscription closed")

	var win ackWindow
	ackCh := make(chan struct{}, 1)
	recvErr := make(chan error, 1)
	var sent atomic.Uint64
	sent.Store(cursor)
	go func() {
		for {
			msg, err := stream.Receive()
			if err != nil {
				recvErr <- err
				return
			}
			ack := msg.GetAck()
			if ack == nil {
				recvErr <- connect.NewError(
					connect.CodeInvalidArgument,
					errors.New("only ack messages may follow start"),
				)
				return
			}
			seq := ack.GetSequence()
			if seq > sent.Load() {
				recvErr <- connect.NewError(
					connect.CodeInvalidArgument,
					errors.New("ack for a sequence that was not sent"),
				)
				return
			}
			win.ack(seq)
			select {
			case ackCh <- struct{}{}:
			default:
			}
		}
	}()

	lagErr := func() error {
		log.Warn("acknowledging subscriber fell out of the buffer")
		return connect.NewError(
			connect.CodeOutOfRange,
			errors.New("subscriber fell behind the buffered events"),
		)
	}
	guard := s.hub.guardSends(ctx)
	defer guard.stop()

	for {
		if win.outstanding() >= s.hub.window {
			timer := time.NewTimer(eventAckTimeout)
			select {
			case <-ackCh:
				timer.Stop()
			case err := <-recvErr:
				timer.Stop()
				return ackStreamEnd(err)
			case <-timer.C:
				log.Warn("event subscriber stopped acknowledging")
				return connect.NewError(
					connect.CodeDeadlineExceeded,
					errors.New("acknowledgements stalled"),
				)
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-s.hub.closed:
				timer.Stop()
				return errEventServiceClosing()
			}
			continue
		}
		events, next, lagged, wake := s.hub.read(cursor, match, 64)
		if lagged {
			return lagErr()
		}
		cursor = next
		if len(events) > 0 {
			if err := guard.begin(cursor, lagErr); err != nil {
				return err
			}
			for _, ev := range events {
				// Recorded before the send: the client may acknowledge the
				// event before Send returns here.
				sent.Store(ev.GetSequence())
				win.add(ev.GetSequence())
				if err := stream.Send(
					&eventv1alpha1.SubscribeWithAckResponse{Event: ev},
				); err != nil {
					return guard.sendError(err, lagErr)
				}
			}
			guard.end()
			continue
		}
		select {
		case <-wake:
		case err := <-recvErr:
			return ackStreamEnd(err)
		case <-ctx.Done():
			return nil
		case <-s.hub.closed:
			return errEventServiceClosing()
		}
	}
}

func errEventServiceClosing() error {
	return connect.NewError(
		connect.CodeUnavailable,
		errors.New("event service is shutting down"),
	)
}

type responseControllerKey struct{}

// newEventServiceHTTPHandler mounts the Event Service with each call's
// http.ResponseController in its context, which sendGuard needs to end a
// blocked send.
func newEventServiceHTTPHandler(
	h *eventServiceHandler,
	opts ...connect.HandlerOption,
) (string, http.Handler) {
	path, handler := eventconnect.NewEventServiceHandler(h, opts...)
	return path, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(
				r.Context(),
				responseControllerKey{},
				http.NewResponseController(w),
			)
			handler.ServeHTTP(w, r.WithContext(ctx))
		},
	)
}

const (
	sendExpiredLagged int32 = iota + 1
	sendExpiredClosing
)

// sendGuard ends a stream.Send that cannot usefully complete. A client that
// stops reading blocks Send on HTTP/2 flow control, and the send loop then
// never returns to its lag and shutdown checks. The guard makes those same
// checks while a batch is in flight and, when one fires, expires the write so
// the blocked Send fails. It adds no timeout: a client that is slow but still
// within the ring is never cut off.
type sendGuard struct {
	hub *eventHub
	rc  *http.ResponseController
	// sending is the cursor after the batch in flight plus one, or zero
	// between batches.
	sending atomic.Uint64
	expired atomic.Int32
	done    chan struct{}
}

// guardSends starts a guard for one stream. Without a response controller in
// ctx the guard still reports lag and shutdown at each batch, but cannot end a
// Send that is already blocked.
func (h *eventHub) guardSends(ctx context.Context) *sendGuard {
	g := &sendGuard{hub: h, done: make(chan struct{})}
	g.rc, _ = ctx.Value(responseControllerKey{}).(*http.ResponseController)
	if g.rc != nil {
		go g.watch(ctx)
	}
	return g
}

func (g *sendGuard) stop() { close(g.done) }

// begin marks the batch ending at cursor as in flight. It is stored before
// the checks below, and the watcher reads it after observing a change, so a
// lag or close is seen by at least one of them.
func (g *sendGuard) begin(cursor uint64, lagErr func() error) error {
	g.sending.Store(cursor + 1)
	select {
	case <-g.hub.closed:
		return errEventServiceClosing()
	default:
	}
	if g.hub.lagged(cursor) {
		return lagErr()
	}
	return nil
}

func (g *sendGuard) end() { g.sending.Store(0) }

// sendError maps a failed Send to the reason the guard ended it, if any.
func (g *sendGuard) sendError(err error, lagErr func() error) error {
	switch g.expired.Load() {
	case sendExpiredLagged:
		return lagErr()
	case sendExpiredClosing:
		return errEventServiceClosing()
	}
	return err
}

func (g *sendGuard) watch(ctx context.Context) {
	for {
		g.hub.mu.Lock()
		wake := g.hub.wake
		g.hub.mu.Unlock()
		if s := g.sending.Load(); s != 0 && g.hub.lagged(s-1) {
			g.expire(sendExpiredLagged)
			return
		}
		select {
		case <-wake:
		case <-g.hub.closed:
			if g.sending.Load() != 0 {
				g.expire(sendExpiredClosing)
			}
			return
		case <-g.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (g *sendGuard) expire(reason int32) {
	g.expired.Store(reason)
	// A deadline in the past fails the pending write at once.
	_ = g.rc.SetWriteDeadline(time.Unix(1, 0))
}

// ackWindow tracks the sequences delivered to an acknowledging subscriber
// and not yet acknowledged. The send window is a count of these, not a
// sequence distance: sequences are global, so the gap between sent and
// acknowledged positions also spans events the subscriber's filter rejected.
type ackWindow struct {
	mu      sync.Mutex
	pending []uint64
}

func (w *ackWindow) add(seq uint64) {
	w.mu.Lock()
	w.pending = append(w.pending, seq)
	w.mu.Unlock()
}

// ack forgets every delivered sequence up to and including seq.
func (w *ackWindow) ack(seq uint64) {
	w.mu.Lock()
	n := 0
	for n < len(w.pending) && w.pending[n] <= seq {
		n++
	}
	w.pending = w.pending[n:]
	w.mu.Unlock()
}

func (w *ackWindow) outstanding() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return uint64(len(w.pending))
}

// ackStreamEnd maps the end of the client's side of the stream: a clean
// close or cancellation is not an error.
func ackStreamEnd(err error) error {
	if ce, ok := errors.AsType[*connect.Error](err); ok {
		if ce.Code() == connect.CodeCanceled {
			return nil
		}
		return err
	}
	return nil
}

func (s *eventServiceHandler) checkPublishAllowed() error {
	if !s.publishEnabled {
		return connect.NewError(
			connect.CodePermissionDenied,
			errors.New("publishing is disabled on this node"),
		)
	}
	return nil
}

// validatePublished checks one inbound event and returns the failure to
// report, or nil when it can be delivered.
func (s *eventServiceHandler) validatePublished(
	ev *eventv1alpha1.Event,
) *eventv1alpha1.PublishFailure {
	if ev == nil || ev.GetEventType() == "" || ev.GetId() == "" {
		return &eventv1alpha1.PublishFailure{
			Code:    eventv1alpha1.PublishFailureCode_PUBLISH_FAILURE_CODE_INVALID_EVENT,
			Message: "event needs an id and an event_type",
		}
	}
	info, ok := eventtype.Lookup(ev.GetEventType())
	if !ok {
		return &eventv1alpha1.PublishFailure{
			Code:    eventv1alpha1.PublishFailureCode_PUBLISH_FAILURE_CODE_UNKNOWN_EVENT_TYPE,
			Message: fmt.Sprintf("event type %q is not registered", ev.GetEventType()),
		}
	}
	if ev.GetOriginNodeId() == "" || ev.GetOriginNodeId() == s.hub.nodeID {
		return &eventv1alpha1.PublishFailure{
			Code:    eventv1alpha1.PublishFailureCode_PUBLISH_FAILURE_CODE_INVALID_EVENT,
			Message: "event needs a remote origin_node_id",
		}
	}
	if td := ev.GetTypedData(); td != nil {
		if !info.HasTypedPayload || td.MessageName() != protoreflect.FullName(info.PayloadMessage) {
			return &eventv1alpha1.PublishFailure{
				Code:    eventv1alpha1.PublishFailureCode_PUBLISH_FAILURE_CODE_INVALID_EVENT,
				Message: "typed_data does not match the event type's payload",
			}
		}
	}
	return nil
}

// deliver puts a validated remote event on the delivery stream. The event is
// copied so the caller's message is never shared with subscribers.
func (s *eventServiceHandler) deliver(ev *eventv1alpha1.Event) {
	cp, ok := proto.Clone(ev).(*eventv1alpha1.Event)
	if !ok {
		return
	}
	cp.Sequence = 0
	s.hub.append(cp)
}

func (s *eventServiceHandler) Publish(
	_ context.Context,
	req *connect.Request[eventv1alpha1.PublishRequest],
) (*connect.Response[eventv1alpha1.PublishResponse], error) {
	if err := s.checkPublishAllowed(); err != nil {
		return nil, err
	}
	if failure := s.validatePublished(req.Msg.GetEvent()); failure != nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New(failure.GetMessage()),
		)
	}
	s.deliver(req.Msg.GetEvent())
	return connect.NewResponse(&eventv1alpha1.PublishResponse{}), nil
}

func (s *eventServiceHandler) PublishBatch(
	_ context.Context,
	req *connect.Request[eventv1alpha1.PublishBatchRequest],
) (*connect.Response[eventv1alpha1.PublishBatchResponse], error) {
	if err := s.checkPublishAllowed(); err != nil {
		return nil, err
	}
	events := req.Msg.GetEvents()
	results := make([]*eventv1alpha1.PublishBatchResult, 0, len(events))
	for _, ev := range events {
		res := &eventv1alpha1.PublishBatchResult{EventId: ev.GetId()}
		if failure := s.validatePublished(ev); failure != nil {
			res.Failure = failure
		} else {
			s.deliver(ev)
		}
		results = append(results, res)
	}
	return connect.NewResponse(
		&eventv1alpha1.PublishBatchResponse{Results: results},
	), nil
}

func infoToProto(i eventtype.Info) *eventv1alpha1.EventTypeInfo {
	return &eventv1alpha1.EventTypeInfo{
		Name:            i.Name,
		Category:        i.Category,
		Description:     i.Description,
		HasTypedPayload: i.HasTypedPayload,
	}
}

func (s *eventServiceHandler) ListEventTypes(
	context.Context,
	*connect.Request[eventv1alpha1.ListEventTypesRequest],
) (*connect.Response[eventv1alpha1.ListEventTypesResponse], error) {
	infos := eventtype.Infos()
	out := make([]*eventv1alpha1.EventTypeInfo, 0, len(infos))
	for _, i := range infos {
		out = append(out, infoToProto(i))
	}
	return connect.NewResponse(
		&eventv1alpha1.ListEventTypesResponse{EventTypes: out},
	), nil
}

func (s *eventServiceHandler) GetEventSchema(
	_ context.Context,
	req *connect.Request[eventv1alpha1.GetEventSchemaRequest],
) (*connect.Response[eventv1alpha1.GetEventSchemaResponse], error) {
	info, ok := eventtype.Lookup(req.Msg.GetEventType())
	if !ok {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf("event type %q is not registered", req.Msg.GetEventType()),
		)
	}
	resp := &eventv1alpha1.GetEventSchemaResponse{Info: infoToProto(info)}
	if info.HasTypedPayload {
		set, err := eventtype.PayloadDescriptors(info.Name)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		resp.PayloadMessage = info.PayloadMessage
		resp.PayloadDescriptors = set
	}
	return connect.NewResponse(resp), nil
}
