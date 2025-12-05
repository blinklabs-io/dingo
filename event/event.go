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
	EventQueueSize = 20
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

type EventBus struct {
	subscribers map[EventType]map[EventSubscriberId]Subscriber
	metrics     *eventMetrics
	lastSubId   EventSubscriberId
	mu          sync.RWMutex
	Logger      *slog.Logger
}

// NewEventBus creates a new EventBus
func NewEventBus(
	promRegistry prometheus.Registerer,
	logger *slog.Logger,
) *EventBus {
	e := &EventBus{
		subscribers: make(map[EventType]map[EventSubscriberId]Subscriber),
		Logger:      logger,
	}
	if promRegistry != nil {
		e.initMetrics(promRegistry)
	}
	return e
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
				e.Logger.Debug("event delivery error", "type", eventType, "err", deliverErr)
			} else {
				slog.Default().Debug("event delivery error", "type", eventType, "err", deliverErr)
			}
		}
	}
	if e.metrics != nil {
		e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
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
func (e *EventBus) Stop() {
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
}
