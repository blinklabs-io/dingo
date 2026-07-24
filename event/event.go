// Copyright 2024 Blink Labs Software
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

package event

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// EventQueueSize is the high-burst buffer used by subscribers that may
	// receive bulk-sync bursts (e.g. chainsync/blockfetch ingest in the
	// ledger). Subscribers opt in to this size via the *WithBuffer
	// variants. Sized to absorb the worst case from #1556 / #1914. Buffer
	// size no longer decides whether events survive — a full buffer
	// backpressures the publisher rather than dropping (#2932) — it decides
	// how large a burst passes through without slowing ingestion.
	EventQueueSize = 100000
	// DefaultSubscriberBuffer is the per-subscriber channel buffer used by
	// Subscribe/SubscribeFunc when no explicit size is requested. Most
	// subscribers (peergov, governance, async housekeeping, etc.) only
	// receive sparse traffic and do not need 100k slots; sizing the
	// default down keeps idle steady-state heap small while leaving the
	// burst headroom available to opt-in callers via SubscribeWithBuffer
	// / SubscribeFuncWithBuffer. See blinklabs-io/dingo#2106.
	DefaultSubscriberBuffer = 1024
	AsyncQueueSize          = 1000
	AsyncWorkerPoolSize     = 4
	RemoteDeliverTimeout    = 5 * time.Second
)

// ErrEventBusStopped is returned by PublishBlocking when the EventBus is
// stopping or closed before or during delivery.
var ErrEventBusStopped = errors.New("event bus stopped")

var errChannelSubscriberClosed = errors.New("channel subscriber closed")

// deliveryStallWarnInterval is how long a single delivery may wait for
// subscriber capacity before the subscriber is reported as stalled, and how
// often that report repeats while the wait continues. Backpressure is normal
// under load, so this deliberately reports a stall rather than each wait.
// Overridden in tests.
var deliveryStallWarnInterval = 30 * time.Second

type EventType string

type EventSubscriberId int

type EventHandlerFunc func(Event)

type Event struct {
	Timestamp time.Time
	Data      any
	Type      EventType
}

func NewEvent(eventType EventType, eventData any) Event {
	return Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      eventData,
	}
}

func (e *EventBus) HasSubscribers(eventType EventType) bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	subs, ok := e.subscriberSnapshots[eventType]
	return ok && len(subs) > 0
}

// asyncEvent wraps an event with its type for the async queue
type asyncEvent struct {
	eventType EventType
	event     Event
}

type subscriberEntry struct {
	id         EventSubscriberId
	sub        Subscriber
	channelSub *channelSubscriber
	kind       string
}

type EventBus struct {
	subscribers         map[EventType]map[EventSubscriberId]Subscriber
	subscriberSnapshots map[EventType][]subscriberEntry
	// channelSubsById tracks every channelSubscriber by its subscriber ID,
	// independently of e.subscribers above (which unsubscribe removes the
	// entry from as soon as any one caller processes it). See unsubscribe's
	// doc comment for why this independent lookup is required: without it,
	// a plain Unsubscribe racing a concurrent UnsubscribeAndWait for the
	// same subId could make UnsubscribeAndWait return without ever
	// waiting. Removed once a SubscribeFunc dispatch goroutine exits (or,
	// for a subscriber with no such goroutine, directly inside
	// unsubscribe) -- subIds are never reused, so a stale entry can never
	// be confused with a later, different subscriber.
	channelSubsById map[EventSubscriberId]*channelSubscriber
	metrics         *eventMetrics
	lastSubId       EventSubscriberId
	mu              sync.RWMutex
	Logger          *slog.Logger
	subscriberWg    sync.WaitGroup // Tracks SubscribeFunc goroutines

	// Async publishing infrastructure
	asyncQueue chan asyncEvent
	asyncWg    sync.WaitGroup
	stopCh     chan struct{}
	closed     bool
	stopped    bool
	stopSeq    uint64
	stopMu     sync.RWMutex
	stopOpMu   sync.Mutex // Serializes Stop() calls to prevent duplicate worker pools
}

// NewEventBus creates a new EventBus with async worker pool
func NewEventBus(
	promRegistry prometheus.Registerer,
	logger *slog.Logger,
) *EventBus {
	e := &EventBus{
		subscribers:         make(map[EventType]map[EventSubscriberId]Subscriber),
		subscriberSnapshots: make(map[EventType][]subscriberEntry),
		channelSubsById:     make(map[EventSubscriberId]*channelSubscriber),
		Logger:              logger,
		asyncQueue:          make(chan asyncEvent, AsyncQueueSize),
		stopCh:              make(chan struct{}),
	}
	if promRegistry != nil {
		e.initMetrics(promRegistry)
	}
	// Start async worker pool
	for range AsyncWorkerPoolSize {
		e.asyncWg.Add(1)
		go e.asyncWorker()
	}
	return e
}

// asyncWorker processes events from the async queue
func (e *EventBus) asyncWorker() {
	defer e.asyncWg.Done()
	for {
		// Prioritize shutdown before attempting to receive queued async work.
		select {
		case <-e.stopCh:
			return
		default:
		}

		select {
		case <-e.stopCh:
			return
		case ae, ok := <-e.asyncQueue:
			if !ok {
				return
			}
			// Drop queued work if shutdown began after the dequeue but before
			// Publish ran so Stop/Close do not deliver buffered async events.
			select {
			case <-e.stopCh:
				return
			default:
			}
			// Publish directly. A slow in-memory subscriber parks this
			// worker until it drains, which is the backpressure that
			// keeps events from being dropped; shutdown closes stopCh
			// first, releasing any parked delivery. Remote subscribers
			// are time-bounded by deliverWithTimeout in Publish.
			e.Publish(ae.eventType, ae.event)
		}
	}
}

// Subscriber is a delivery abstraction that allows the EventBus to deliver
// events to in-memory channels and to network-backed subscribers via the
// same interface.
// Implementations must ensure Close() is idempotent and safe to call multiple times.
type Subscriber interface {
	Deliver(Event) error
	Close()
}

// channelSubscriber is the in-memory subscriber adapter that preserves the
// existing channel-based API. Delivery waits for buffer capacity rather than
// dropping: a subscriber that falls behind backpressures its publishers
// instead of silently losing events (blinklabs-io/dingo#2932). Close closes
// the channel so SubscribeFunc goroutines exit.
type channelSubscriber struct {
	ch     chan Event
	logger *slog.Logger
	// closeReq is closed by Close before it takes mu for write. A waiting
	// send holds mu for read, so signalling first is what lets Close make
	// progress; see the comment on deliverWait.
	closeReq chan struct{}
	// busStop is the owning EventBus's stop channel, snapshotted at
	// subscribe time. Shutdown closes it before anything else, which
	// releases waiting sends whose subscriber has not been closed yet.
	busStop <-chan struct{}
	// onBlocked, when set, reports that a delivery had to wait for
	// capacity. Set once at subscribe time, before the subscriber is
	// reachable by any publisher.
	onBlocked func()
	closeOnce sync.Once
	mu        sync.RWMutex
	closed    bool

	// eventType is the type this subscriber was registered under —
	// checked in unsubscribe against the eventType a caller passes in,
	// since channelSubsById is keyed by subId alone (see its own doc
	// comment for why) and subIds are never reused but are never scoped
	// to a single eventType either: without this check, Unsubscribe or
	// UnsubscribeAndWait called with a subId that's valid but for a
	// DIFFERENT eventType than the one passed in would still find and
	// close this subscriber via channelSubsById, silently tearing down
	// an unrelated subscription instead of matching nothing.
	eventType EventType

	// done, when non-nil, is closed by SubscribeFuncWithBuffer's dispatch
	// goroutine right before it exits -- including after it finishes any
	// handler call already in flight when Close was called. It is nil for
	// subscribers created via Subscribe/SubscribeWithBuffer, which have no
	// bus-owned goroutine to wait for; the caller there owns its own read
	// loop. See waitDone.
	done chan struct{}
}

func newChannelSubscriber(
	eventType EventType,
	buffer int,
	logger *slog.Logger,
) *channelSubscriber {
	return &channelSubscriber{
		ch:        make(chan Event, buffer),
		logger:    logger,
		closeReq:  make(chan struct{}),
		eventType: eventType,
	}
}

// waitDone blocks until the owning SubscribeFunc dispatch goroutine has
// fully exited, if this subscriber has one. No-op otherwise.
func (c *channelSubscriber) waitDone() {
	if c.done == nil {
		return
	}
	<-c.done
}

// deliverWait hands evt to the subscriber channel, waiting for buffer capacity
// instead of dropping the event. It returns errChannelSubscriberClosed when
// the subscriber is closed or the bus stops before the event can be handed
// off.
//
// The read lock is what keeps Close from closing c.ch out from under an
// in-flight send (which would panic). Holding it across a wait is only safe
// because Close closes c.closeReq before it asks for the write lock: every
// waiting send wakes, returns, and releases the read lock, so Close cannot be
// starved by a full buffer.
func (c *channelSubscriber) deliverWait(evt Event) (err error) {
	// Recover from unexpected panics (e.g. if a remote Subscriber
	// implementation misbehaves).
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("channel deliver panic: %v", r)
		}
	}()

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return errChannelSubscriberClosed
	}

	select {
	case c.ch <- evt:
		return nil
	default:
	}

	// Buffer is full: the publisher waits rather than losing the event.
	if c.onBlocked != nil {
		c.onBlocked()
	}
	stall := time.NewTimer(deliveryStallWarnInterval)
	defer stall.Stop()
	for {
		select {
		case c.ch <- evt:
			return nil
		case <-c.closeReq:
			return errChannelSubscriberClosed
		case <-c.busStop:
			return errChannelSubscriberClosed
		case <-stall.C:
			if c.logger != nil {
				c.logger.Warn(
					"event delivery stalled: subscriber not draining",
					"type", evt.Type,
					"buffer", cap(c.ch),
					"stalled_for", deliveryStallWarnInterval,
				)
			}
			stall.Reset(deliveryStallWarnInterval)
		}
	}
}

// Deliver waits for subscriber capacity and reports success even when the
// subscriber is shutting down, so Publish does not treat teardown as a
// delivery failure and unsubscribe.
func (c *channelSubscriber) Deliver(evt Event) error {
	err := c.deliverWait(evt)
	if errors.Is(err, errChannelSubscriberClosed) {
		return nil
	}
	return err
}

// DeliverBlocking is Deliver with the closed-subscriber case surfaced, so
// PublishBlocking can distinguish teardown from successful delivery.
func (c *channelSubscriber) DeliverBlocking(evt Event) error {
	return c.deliverWait(evt)
}

func (c *channelSubscriber) Close() {
	// Release waiting sends before asking for the write lock; they hold the
	// read lock, so the write lock would otherwise wait on a wait that only
	// Close can end.
	c.closeOnce.Do(func() {
		close(c.closeReq)
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.ch)
}

// subscribeInternal does the actual subscription work without checking stopped.
// Callers must hold stopMu.RLock or have otherwise ensured the EventBus is not stopped.
func (e *EventBus) subscribeInternal(
	eventType EventType,
	buffer int,
) (EventSubscriberId, *channelSubscriber) {
	if buffer <= 0 {
		buffer = DefaultSubscriberBuffer
	}
	// Read under stopMu.RLock, held by the caller: shutdown swaps stopCh
	// when the bus is restarted. A subscriber created while shutdown is in
	// progress snapshots the already-closed channel, so a delivery to it
	// returns immediately instead of waiting on a bus that is going away.
	busStop := e.stopCh
	e.mu.Lock()
	defer e.mu.Unlock()
	// Create channel-backed subscriber
	chSub := newChannelSubscriber(eventType, buffer, e.Logger)
	chSub.busStop = busStop
	if e.metrics != nil {
		chSub.onBlocked = func() {
			e.metrics.deliveryBlocked.WithLabelValues(
				string(eventType), "in-memory",
			).Inc()
		}
	}
	// Increment subscriber ID
	subId := e.lastSubId + 1
	e.lastSubId = subId
	// Add new subscriber
	if _, ok := e.subscribers[eventType]; !ok {
		e.subscribers[eventType] = make(map[EventSubscriberId]Subscriber)
	}
	evtTypeSubs := e.subscribers[eventType]
	evtTypeSubs[subId] = chSub
	e.channelSubsById[subId] = chSub
	e.refreshSubscriberSnapshotLocked(eventType)
	if e.metrics != nil {
		e.metrics.subscribers.WithLabelValues(string(eventType), "in-memory").
			Inc()
	}
	return subId, chSub
}

// Subscribe allows a consumer to receive events of a particular type via a channel.
// Returns (0, nil) if the EventBus is stopped or closed.
func (e *EventBus) Subscribe(
	eventType EventType,
) (EventSubscriberId, <-chan Event) {
	return e.SubscribeWithBuffer(eventType, DefaultSubscriberBuffer)
}

// SubscribeWithBuffer is like Subscribe but lets the caller pick the
// per-subscriber channel buffer. Use this for subscribers that need to
// tolerate bursts larger than DefaultSubscriberBuffer (e.g. chainsync
// or blockfetch ingest during bulk catch-up). A non-positive buffer
// falls back to DefaultSubscriberBuffer.
func (e *EventBus) SubscribeWithBuffer(
	eventType EventType,
	buffer int,
) (EventSubscriberId, <-chan Event) {
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return 0, nil
	}
	subId, chSub := e.subscribeInternal(eventType, buffer)
	e.stopMu.RUnlock()
	return subId, chSub.ch
}

// SubscribeFunc allows a consumer to receive events of a particular type via a callback function.
// Returns 0 if the EventBus is stopped or closed.
func (e *EventBus) SubscribeFunc(
	eventType EventType,
	handlerFunc EventHandlerFunc,
) EventSubscriberId {
	return e.SubscribeFuncWithBuffer(
		eventType,
		DefaultSubscriberBuffer,
		handlerFunc,
	)
}

// SubscribeFuncWithBuffer is like SubscribeFunc but lets the caller pick
// the per-subscriber channel buffer. See SubscribeWithBuffer for details.
func (e *EventBus) SubscribeFuncWithBuffer(
	eventType EventType,
	buffer int,
	handlerFunc EventHandlerFunc,
) EventSubscriberId {
	// Hold stopMu.RLock through Add(1) to prevent Stop() from calling Wait()
	// before we increment the counter. This prevents the race where:
	// 1. Stop() sets stopped=true and proceeds to subscriberWg.Wait()
	// 2. SubscribeFunc() calls Add(1) after Wait() started with counter=0
	// Which would cause a panic or leave the goroutine blocked forever.
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return 0
	}
	subId, chSub := e.subscribeInternal(eventType, buffer)
	e.subscriberWg.Add(1)
	// Set before returning subId to the caller: nothing else can reach
	// Unsubscribe(subId) -- and therefore waitDone -- until this call
	// returns, so there is no window where a concurrent Unsubscribe could
	// observe done as nil and skip waiting.
	chSub.done = make(chan struct{})
	e.stopMu.RUnlock()

	go func(evtCh <-chan Event, handlerFunc EventHandlerFunc, done chan struct{}) {
		defer close(done)
		defer e.subscriberWg.Done()
		// This dispatch goroutine is channelSubsById[subId]'s only owner
		// once it's running, so it self-cleans its own entry here rather
		// than relying on unsubscribe to do it — unsubscribe may run
		// before, during, or after this goroutine's lifetime relative to
		// any given caller, and subIds are never reused, so there is no
		// risk of removing a later, different subscriber's entry.
		defer func() {
			e.mu.Lock()
			delete(e.channelSubsById, subId)
			e.mu.Unlock()
		}()
		for {
			evt, ok := <-evtCh
			if !ok {
				return
			}
			e.safeHandlerCall(handlerFunc, evt)
		}
	}(chSub.ch, handlerFunc, chSub.done)
	return subId
}

// safeHandlerCall invokes a SubscribeFunc handler with panic recovery so that
// a misbehaving handler cannot crash the node.
func (e *EventBus) safeHandlerCall(
	handlerFunc EventHandlerFunc,
	evt Event,
) {
	defer func() {
		if r := recover(); r != nil {
			logger := e.Logger
			if logger == nil {
				logger = slog.Default()
			}
			logger.Error(
				"SubscribeFunc handler panicked",
				"event_type", evt.Type,
				"panic", r,
			)
		}
	}()
	handlerFunc(evt)
}

// Unsubscribe stops delivery of events for a particular type for an existing subscriber
func (e *EventBus) Unsubscribe(eventType EventType, subId EventSubscriberId) {
	e.unsubscribe(eventType, subId, false)
}

// UnsubscribeAndWait is like Unsubscribe, but for SubscribeFunc/
// SubscribeFuncWithBuffer subscribers it additionally blocks until that
// subscriber's dispatch goroutine has fully exited -- including finishing
// any handler call already in flight when this is called. Plain Subscribe/
// SubscribeWithBuffer subscribers have no bus-owned goroutine, so this
// behaves exactly like Unsubscribe for them.
//
// Use this wherever a caller unsubscribes and then, in the same teardown
// sequence, mutates or discards state that the handler closure reads
// without its own synchronization (e.g. a component field nilled out right
// after Close()) -- plain Unsubscribe only stops *future* deliveries, so a
// handler goroutine that already dequeued an event can still be executing
// concurrently with that teardown. Do not call this from within the
// subscriber's own handler: waiting for a goroutine to exit from inside
// that same goroutine deadlocks forever.
//
// Safe to call concurrently with a plain Unsubscribe for the same subId
// (e.g. from two different teardown paths racing each other): whichever
// call actually removes the e.subscribers entry, this one still finds and
// waits on the subscriber via channelSubsById, so the race cannot turn
// this into a no-op.
func (e *EventBus) UnsubscribeAndWait(eventType EventType, subId EventSubscriberId) {
	e.unsubscribe(eventType, subId, true)
}

func (e *EventBus) unsubscribe(
	eventType EventType,
	subId EventSubscriberId,
	wait bool,
) {
	e.mu.Lock()
	var subToClose Subscriber
	if evtTypeSubs, ok := e.subscribers[eventType]; ok {
		if sub, ok2 := evtTypeSubs[subId]; ok2 {
			subToClose = sub
			delete(evtTypeSubs, subId)
			if len(evtTypeSubs) == 0 {
				delete(e.subscribers, eventType)
				delete(e.subscriberSnapshots, eventType)
			} else {
				e.refreshSubscriberSnapshotLocked(eventType)
			}
			if e.metrics != nil {
				kind := "remote"
				if _, ok := sub.(*channelSubscriber); ok {
					kind = "in-memory"
				}
				e.metrics.subscribers.WithLabelValues(string(eventType), kind).
					Dec()
			}
		}
	}
	// Looked up independently of subToClose above: e.subscribers only
	// ever holds one entry per subId, so whichever caller runs first
	// (Unsubscribe or UnsubscribeAndWait) removes it -- leaving a second,
	// concurrent caller for the same subId with subToClose == nil even
	// though the subscriber itself hasn't finished tearing down yet.
	// channelSubsById is keyed by subId and only cleaned up once the
	// subscriber is fully done (its own dispatch goroutine, or right here
	// for a subscriber with no such goroutine), so a second caller can
	// still find and wait on it here regardless of who removed the
	// e.subscribers entry.
	//
	// Guarded by chSub.eventType == eventType: channelSubsById is keyed
	// by subId alone, with no eventType dimension, so a caller passing a
	// subId that's valid but registered under a DIFFERENT eventType than
	// the one it passed in must find nothing here -- without this check,
	// such a call would still find and close that unrelated subscriber.
	chSub := e.channelSubsById[subId]
	if chSub != nil && chSub.eventType != eventType {
		chSub = nil
	}
	if chSub != nil && chSub.done == nil {
		// No dispatch goroutine will ever clean this one up itself.
		delete(e.channelSubsById, subId)
	}
	e.mu.Unlock()

	if subToClose != nil {
		if _, isChannelSub := subToClose.(*channelSubscriber); !isChannelSub {
			subToClose.Close()
		}
	}
	if chSub != nil {
		// Close is idempotent, so this is safe even if a concurrent
		// caller already closed the same subscriber via subToClose above.
		chSub.Close()
		if wait {
			chSub.waitDone()
		}
	}
}

// deliverWithTimeout calls sub.Deliver with a timeout for non-channel
// subscribers. channelSubscriber.Deliver is called directly: it waits for
// buffer capacity by design, and bounding that wait would put the drop this
// package no longer performs back into the delivery path. For other (e.g.
// network-backed) implementations, the call is bounded by
// RemoteDeliverTimeout to prevent worker stalls.
//
// Bounded goroutine leak on timeout: when the timeout fires, the
// goroutine running sub.Deliver remains alive until Deliver returns.
// Because the done channel is buffered (size 1), the goroutine will
// not block when it eventually writes its result -- it will complete
// and be reclaimed. The caller (Publish) unsubscribes the slow
// subscriber immediately after a timeout, preventing any further
// goroutine spawns for that subscriber. Therefore at most one
// goroutine can be outstanding per timed-out subscriber.
//
// True cancellation would require adding context support to the
// Subscriber interface, which is out of scope for this change.
func (e *EventBus) deliverWithTimeout(
	sub Subscriber,
	evt Event,
) error {
	if _, ok := sub.(*channelSubscriber); ok {
		return sub.Deliver(evt)
	}

	// Slow path: bound remote Deliver calls with a timeout.
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("subscriber deliver panic: %v", r)
			}
		}()
		done <- sub.Deliver(evt)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(RemoteDeliverTimeout):
		if e.metrics != nil {
			e.metrics.deliveryTimeouts.WithLabelValues(
				string(evt.Type),
			).Inc()
		}
		return fmt.Errorf(
			"subscriber deliver timeout after %s",
			RemoteDeliverTimeout,
		)
	}
}

// Publish allows a producer to send an event of a particular type to all subscribers
func (e *EventBus) Publish(eventType EventType, evt Event) {
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return
	}
	defer e.stopMu.RUnlock()

	e.mu.RLock()
	subList := e.subscriberSnapshots[eventType]
	e.mu.RUnlock()
	if len(subList) == 0 {
		if e.metrics != nil {
			e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
		}
		return
	}
	for _, item := range subList {
		var deliverErr error
		if item.channelSub != nil {
			deliverErr = item.channelSub.Deliver(evt)
		} else {
			deliverErr = e.deliverWithTimeout(item.sub, evt)
		}

		if deliverErr != nil {
			e.Unsubscribe(eventType, item.id)
			if e.metrics != nil {
				e.metrics.deliveryErrors.WithLabelValues(string(eventType), item.kind).
					Inc()
			}
			if e.Logger != nil {
				e.Logger.Debug(
					"event delivery error",
					"type",
					eventType,
					"err",
					deliverErr,
				)
			} else {
				slog.Default().Debug(
					"event delivery error",
					"type",
					eventType,
					"err",
					deliverErr,
				)
			}
		}
	}
	if e.metrics != nil {
		e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
	}
}

// PublishBlocking delivers an event to all subscribers without dropping
// in-memory channel events when subscriber buffers are full. This should be
// reserved for ordering-critical streams where loss is worse than applying
// producer backpressure. It returns ErrEventBusStopped when the bus is
// stopping or closed before or during delivery.
func (e *EventBus) PublishBlocking(eventType EventType, evt Event) error {
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return ErrEventBusStopped
	}
	stopSeq := e.stopSeq
	stopCh := e.stopCh
	e.stopMu.RUnlock()

	e.mu.RLock()
	subList := append([]subscriberEntry(nil), e.subscriberSnapshots[eventType]...)
	e.mu.RUnlock()
	if len(subList) == 0 {
		if e.metrics != nil {
			e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
		}
		return nil
	}
	var firstErr error
	for _, item := range subList {
		var deliverErr error
		if item.channelSub != nil {
			deliverErr = item.channelSub.DeliverBlocking(evt)
		} else {
			deliverErr = e.deliverWithTimeout(item.sub, evt)
		}

		if errors.Is(deliverErr, errChannelSubscriberClosed) {
			if e.stoppedSince(stopSeq, stopCh) {
				deliverErr = ErrEventBusStopped
			} else {
				deliverErr = nil
			}
		}
		if deliverErr != nil {
			if firstErr == nil {
				firstErr = deliverErr
			}
			e.Unsubscribe(eventType, item.id)
			if e.metrics != nil {
				e.metrics.deliveryErrors.WithLabelValues(string(eventType), item.kind).
					Inc()
			}
			if e.Logger != nil {
				e.Logger.Debug(
					"event delivery error",
					"type",
					eventType,
					"err",
					deliverErr,
				)
			} else {
				slog.Default().Debug(
					"event delivery error",
					"type",
					eventType,
					"err",
					deliverErr,
				)
			}
		}
	}
	if e.metrics != nil {
		e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
	}
	if firstErr == nil && e.stoppedSince(stopSeq, stopCh) {
		firstErr = ErrEventBusStopped
	}
	return firstErr
}

// stoppedSince reports whether the EventBus began stopping since the caller
// snapshotted stopSeq and stopCh. shutdown closes stopCh before it marks the
// bus stopped, so the channel has to be consulted as well as the flags.
func (e *EventBus) stoppedSince(
	stopSeq uint64,
	stopCh chan struct{},
) bool {
	select {
	case <-stopCh:
		return true
	default:
	}
	e.stopMu.RLock()
	defer e.stopMu.RUnlock()
	return e.stopped || e.closed || e.stopSeq != stopSeq
}

// PublishAsync enqueues an event for asynchronous delivery to all subscribers.
// It hands the event to the shared async queue and returns without waiting for
// subscriber delivery. Use this for events that do not need to be delivered
// synchronously with the publisher's call stack.
//
// When the queue is full the caller waits for space rather than losing the
// event, so a backlog slows producers instead of discarding work. Returns
// false only when the EventBus is stopped or closed.
func (e *EventBus) PublishAsync(eventType EventType, evt Event) bool {
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return false
	}
	q := e.asyncQueue
	stopCh := e.stopCh
	// Released before waiting on the queue: shutdown needs stopMu for
	// write, and it is shutdown that releases us.
	e.stopMu.RUnlock()

	ae := asyncEvent{eventType: eventType, event: evt}
	select {
	case q <- ae:
	default:
		// Queue is full: wait for space rather than losing the event.
		if e.metrics != nil {
			e.metrics.asyncEnqueueBlocked.WithLabelValues(string(eventType)).
				Inc()
		}
		select {
		case q <- ae:
		case <-stopCh:
			return false
		}
	}

	// shutdown closes stopCh before it marks the bus stopped, so an enqueue
	// can still land in a queue whose workers have already exited. Report
	// that as a failed publish rather than as a delivery that never happens.
	select {
	case <-stopCh:
		return false
	default:
	}
	return true
}

// RegisterSubscriber allows external adapters (e.g., network-backed subscribers)
// to register with the EventBus. It returns the assigned subscriber id.
// Returns 0 if the EventBus is stopped or closed.
func (e *EventBus) RegisterSubscriber(
	eventType EventType,
	sub Subscriber,
) EventSubscriberId {
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return 0
	}
	defer e.stopMu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	subId := e.lastSubId + 1
	e.lastSubId = subId
	if _, ok := e.subscribers[eventType]; !ok {
		e.subscribers[eventType] = make(map[EventSubscriberId]Subscriber)
	}
	e.subscribers[eventType][subId] = sub
	e.refreshSubscriberSnapshotLocked(eventType)
	if e.metrics != nil {
		e.metrics.subscribers.WithLabelValues(string(eventType), "remote").Inc()
	}
	return subId
}

// Stop closes all subscriber channels and clears the subscribers map.
// This ensures that SubscribeFunc goroutines exit cleanly during shutdown.
// The EventBus can still be reused after Stop() is called.
func (e *EventBus) Stop() {
	e.shutdown(true)
}

// Close permanently shuts down the EventBus and its worker pool.
// Unlike Stop, Close does not restart async workers, so the EventBus
// cannot be reused.
func (e *EventBus) Close() {
	e.shutdown(false)
}

func (e *EventBus) shutdown(restart bool) {
	if e == nil {
		return
	}
	// Serialize Stop() calls to prevent race conditions that could spawn
	// duplicate worker pools when called concurrently
	e.stopOpMu.Lock()
	defer e.stopOpMu.Unlock()

	// Signal quiesce before taking stopMu for write. Publishers park on a
	// full subscriber buffer or a full async queue while holding stopMu for
	// read, and stopCh is what releases them, so closing it after acquiring
	// the write lock would deadlock shutdown against its own backpressure.
	// stopOpMu serializes shutdowns and stopped only goes false->true
	// inside one, so this read cannot race a second close.
	e.stopMu.RLock()
	wasAlreadyStopped := e.stopped
	stopCh := e.stopCh
	e.stopMu.RUnlock()
	if !wasAlreadyStopped {
		close(stopCh)
	}

	// Mark as stopped to prevent new async publishes during shutdown.
	// Close permanently marks the bus as closed so future Stop() calls
	// cannot restart the worker pool.
	e.stopMu.Lock()
	if !restart {
		e.closed = true
	}
	e.stopped = true
	e.stopSeq++
	e.stopMu.Unlock()

	if !wasAlreadyStopped {
		// Wait for the async workers to finish
		e.asyncWg.Wait()
	}

	e.mu.Lock()
	// Copy and clear subscribers
	subsCopy := e.subscribers
	e.subscribers = make(map[EventType]map[EventSubscriberId]Subscriber)
	e.subscriberSnapshots = make(map[EventType][]subscriberEntry)
	e.mu.Unlock()

	// Close subscribers outside of lock
	for _, evtTypeSubs := range subsCopy {
		for _, sub := range evtTypeSubs {
			sub.Close()
		}
	}

	// Wait for SubscribeFunc goroutines to complete after closing their channels
	e.subscriberWg.Wait()

	// Every SubscribeFunc dispatch goroutine already removed its own
	// channelSubsById entry as it exited above (subscriberWg.Wait() only
	// returns once they all have), but a plain Subscribe/
	// SubscribeWithBuffer subscriber (done == nil) has no such goroutine
	// to do that for itself -- unsubscribe() only clears its entry when
	// a caller explicitly calls Unsubscribe/UnsubscribeAndWait for it,
	// which shutdown (called via Stop/Close) does not do on a caller's
	// behalf. Left alone, every such subscriber ever created survives
	// here in memory indefinitely; on an EventBus restarted and reused
	// across repeated Stop()/Start() cycles (Stop supports exactly that
	// via restart=true), each cycle's abandoned plain-Subscribe entries
	// pile up without bound. Sweep them out now that they're all closed
	// (sub.Close() above) and definitely never coming back.
	e.mu.Lock()
	for subId, chSub := range e.channelSubsById {
		if chSub.done == nil {
			delete(e.channelSubsById, subId)
		}
	}
	e.mu.Unlock()

	// Reset subscriber metrics if they exist
	if e.metrics != nil {
		e.metrics.subscribers.Reset()
	}

	if !restart {
		return
	}

	// Reinitialize async infrastructure to allow continued use
	e.stopMu.Lock()
	if e.closed {
		e.stopMu.Unlock()
		return
	}
	e.asyncQueue = make(chan asyncEvent, AsyncQueueSize)
	e.stopCh = make(chan struct{})
	e.stopped = false
	e.stopMu.Unlock()

	// Restart async worker pool
	for range AsyncWorkerPoolSize {
		e.asyncWg.Add(1)
		go e.asyncWorker()
	}
}

func (e *EventBus) refreshSubscriberSnapshotLocked(eventType EventType) {
	subs, ok := e.subscribers[eventType]
	if !ok || len(subs) == 0 {
		delete(e.subscriberSnapshots, eventType)
		return
	}
	snapshot := make([]subscriberEntry, 0, len(subs))
	for id, sub := range subs {
		entry := subscriberEntry{
			id:   id,
			sub:  sub,
			kind: "remote",
		}
		if channelSub, ok := sub.(*channelSubscriber); ok {
			entry.channelSub = channelSub
			entry.kind = "in-memory"
		}
		snapshot = append(snapshot, entry)
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].id < snapshot[j].id
	})
	e.subscriberSnapshots[eventType] = snapshot
}
