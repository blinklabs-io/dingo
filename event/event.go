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
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	EventQueueSize      = 20
	AsyncQueueSize      = 1000
	AsyncWorkerPoolSize = 4
)

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

// asyncEvent wraps an event with its type for the async queue
type asyncEvent struct {
	eventType EventType
	event     Event
}

type EventBus struct {
	subscribers map[EventType]map[EventSubscriberId]Subscriber
	metrics     *eventMetrics
	lastSubId   EventSubscriberId
	mu          sync.RWMutex
	Logger      *slog.Logger

	// Async publishing infrastructure
	asyncQueue chan asyncEvent
	asyncWg    sync.WaitGroup
	stopCh     chan struct{}
	stopped    bool
	stopMu     sync.RWMutex
	stopOpMu   sync.Mutex // Serializes Stop() calls to prevent duplicate worker pools
}

// NewEventBus creates a new EventBus with async worker pool
func NewEventBus(
	promRegistry prometheus.Registerer,
	logger *slog.Logger,
) *EventBus {
	e := &EventBus{
		subscribers: make(map[EventType]map[EventSubscriberId]Subscriber),
		Logger:      logger,
		asyncQueue:  make(chan asyncEvent, AsyncQueueSize),
		stopCh:      make(chan struct{}),
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
		select {
		case <-e.stopCh:
			return
		case ae, ok := <-e.asyncQueue:
			if !ok {
				return
			}
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
// existing channel-based API. Deliver blocks by sending on the underlying
// channel (preserving current semantics). Close closes the channel so
// SubscribeFunc goroutines exit.
type channelSubscriber struct {
	ch     chan Event
	mu     sync.RWMutex
	closed bool
}

func newChannelSubscriber(buffer int) *channelSubscriber {
	return &channelSubscriber{
		ch: make(chan Event, buffer),
	}
}

func (c *channelSubscriber) Deliver(evt Event) (err error) {
	// Protect against races with Close by acquiring a read lock.
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		// Subscriber already closed; drop the event without returning an error.
		return nil
	}
	// Ensure we release the read lock after the send completes so that Close
	// will wait for in-flight sends to finish before closing the channel.
	defer c.mu.RUnlock()

	// Normal send (may block, preserving current semantics). Recover from
	// unexpected panics just in case a remote Subscriber implementation misbehaves.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("channel deliver panic: %v", r)
		}
	}()

	c.ch <- evt
	return nil
}

func (c *channelSubscriber) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.ch)
	c.mu.Unlock()
}

// Subscribe allows a consumer to receive events of a particular type via a channel
func (e *EventBus) Subscribe(
	eventType EventType,
) (EventSubscriberId, <-chan Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Create channel-backed subscriber
	chSub := newChannelSubscriber(EventQueueSize)
	// Increment subscriber ID
	subId := e.lastSubId + 1
	e.lastSubId = subId
	// Add new subscriber
	if _, ok := e.subscribers[eventType]; !ok {
		e.subscribers[eventType] = make(map[EventSubscriberId]Subscriber)
	}
	evtTypeSubs := e.subscribers[eventType]
	evtTypeSubs[subId] = chSub
	if e.metrics != nil {
		e.metrics.subscribers.WithLabelValues(string(eventType), "in-memory").
			Inc()
	}
	return subId, chSub.ch
}

// SubscribeFunc allows a consumer to receive events of a particular type via a callback function
func (e *EventBus) SubscribeFunc(
	eventType EventType,
	handlerFunc EventHandlerFunc,
) EventSubscriberId {
	subId, evtCh := e.Subscribe(eventType)
	go func(evtCh <-chan Event, handlerFunc EventHandlerFunc) {
		for {
			evt, ok := <-evtCh
			if !ok {
				return
			}
			handlerFunc(evt)
		}
	}(evtCh, handlerFunc)
	return subId
}

// Unsubscribe stops delivery of events for a particular type for an existing subscriber
func (e *EventBus) Unsubscribe(eventType EventType, subId EventSubscriberId) {
	e.mu.Lock()
	var subToClose Subscriber
	if evtTypeSubs, ok := e.subscribers[eventType]; ok {
		if sub, ok2 := evtTypeSubs[subId]; ok2 {
			subToClose = sub
			delete(evtTypeSubs, subId)
			if len(evtTypeSubs) == 0 {
				delete(e.subscribers, eventType)
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
	e.mu.Unlock()

	if subToClose != nil {
		subToClose.Close()
	}
}

// Publish allows a producer to send an event of a particular type to all subscribers
func (e *EventBus) Publish(eventType EventType, evt Event) {
	// Build list of channels inside read lock to avoid map race condition
	e.mu.RLock()
	subs, ok := e.subscribers[eventType]
	type subItem struct {
		id  EventSubscriberId
		sub Subscriber
	}
	subList := make([]subItem, 0, len(subs))
	if ok {
		for id, sub := range subs {
			subList = append(subList, subItem{id: id, sub: sub})
		}
	}
	e.mu.RUnlock()
	// Send event on gathered subscribers (preserving per-subscriber blocking semantics)
	for _, item := range subList {
		// Protect against panics inside subscriber Deliver implementations.
		var deliverErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					deliverErr = fmt.Errorf("subscriber deliver panic: %v", r)
				}
			}()
			deliverErr = item.sub.Deliver(evt)
		}()

		if deliverErr != nil {
			// Unregister the failing subscriber
			e.Unsubscribe(eventType, item.id)
			// Record metric if available
			if e.metrics != nil {
				kind := "remote"
				if _, ok := item.sub.(*channelSubscriber); ok {
					kind = "in-memory"
				}
				e.metrics.deliveryErrors.WithLabelValues(string(eventType), kind).
					Inc()
			}
			// Log the delivery error/panic for observability using provided logger if present
			if e.Logger != nil {
				e.Logger.Debug(
					"event delivery error",
					"type",
					eventType,
					"err",
					deliverErr,
				)
			} else {
				slog.Default().Debug("event delivery error", "type", eventType, "err", deliverErr)
			}
		}
	}
	if e.metrics != nil {
		e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
	}
}

// PublishAsync enqueues an event for asynchronous delivery to all subscribers.
// This method returns immediately without blocking on subscriber delivery.
// Use this for non-critical events where immediate delivery is not required.
// Returns false if the EventBus is stopped or the async queue is full.
func (e *EventBus) PublishAsync(eventType EventType, evt Event) bool {
	e.stopMu.RLock()
	if e.stopped {
		e.stopMu.RUnlock()
		return false
	}
	e.stopMu.RUnlock()

	select {
	case e.asyncQueue <- asyncEvent{eventType: eventType, event: evt}:
		return true
	default:
		// Queue is full, log and drop the event
		if e.Logger != nil {
			e.Logger.Warn(
				"async event queue full, dropping event",
				"type",
				eventType,
			)
		}
		if e.metrics != nil {
			e.metrics.deliveryErrors.WithLabelValues(string(eventType), "async-dropped").
				Inc()
		}
		return false
	}
}

// RegisterSubscriber allows external adapters (e.g., network-backed subscribers)
// to register with the EventBus. It returns the assigned subscriber id.
func (e *EventBus) RegisterSubscriber(
	eventType EventType,
	sub Subscriber,
) EventSubscriberId {
	e.mu.Lock()
	defer e.mu.Unlock()
	subId := e.lastSubId + 1
	e.lastSubId = subId
	if _, ok := e.subscribers[eventType]; !ok {
		e.subscribers[eventType] = make(map[EventSubscriberId]Subscriber)
	}
	e.subscribers[eventType][subId] = sub
	if e.metrics != nil {
		e.metrics.subscribers.WithLabelValues(string(eventType), "remote").Inc()
	}
	return subId
}

// Stop closes all subscriber channels and clears the subscribers map.
// This ensures that SubscribeFunc goroutines exit cleanly during shutdown.
// The EventBus can still be reused after Stop() is called.
func (e *EventBus) Stop() {
	// Serialize Stop() calls to prevent race conditions that could spawn
	// duplicate worker pools when called concurrently
	e.stopOpMu.Lock()
	defer e.stopOpMu.Unlock()

	// Mark as stopped to prevent new async publishes during shutdown
	e.stopMu.Lock()
	wasAlreadyStopped := e.stopped
	e.stopped = true
	e.stopMu.Unlock()

	if !wasAlreadyStopped {
		// Signal async workers to stop and wait for them to finish
		close(e.stopCh)
		e.asyncWg.Wait()
	}

	e.mu.Lock()
	// Copy and clear subscribers
	subsCopy := e.subscribers
	e.subscribers = make(map[EventType]map[EventSubscriberId]Subscriber)
	e.mu.Unlock()

	// Close subscribers outside of lock
	for _, evtTypeSubs := range subsCopy {
		for _, sub := range evtTypeSubs {
			sub.Close()
		}
	}

	// Reset subscriber metrics if they exist
	if e.metrics != nil {
		e.metrics.subscribers.Reset()
	}

	// Reinitialize async infrastructure to allow continued use
	e.stopMu.Lock()
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
