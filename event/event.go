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
	subscribers map[EventType]map[EventSubscriberId]chan Event
	metrics     *eventMetrics
	lastSubId   EventSubscriberId
	mu          sync.RWMutex
}

// NewEventBus creates a new EventBus
func NewEventBus(promRegistry prometheus.Registerer) *EventBus {
	e := &EventBus{
		subscribers: make(map[EventType]map[EventSubscriberId]chan Event),
	}
	if promRegistry != nil {
		e.initMetrics(promRegistry)
	}
	return e
}

// Subscribe allows a consumer to receive events of a particular type via a
// channel
func (e *EventBus) Subscribe(
	eventType EventType,
) (EventSubscriberId, <-chan Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Create event channel
	evtCh := make(chan Event, EventQueueSize)
	// Increment subscriber ID
	subId := e.lastSubId + 1
	e.lastSubId = subId
	// Add new subscriber
	if _, ok := e.subscribers[eventType]; !ok {
		e.subscribers[eventType] = make(map[EventSubscriberId]chan Event)
	}
	evtTypeSubs := e.subscribers[eventType]
	evtTypeSubs[subId] = evtCh
	if e.metrics != nil {
		e.metrics.subscribers.WithLabelValues(string(eventType)).Inc()
	}
	return subId, evtCh
}

// SubscribeFunc allows a consumer to receive events of a particular type via a
// callback function
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

// Unsubscribe stops delivery of events for a particular type for an existing
// subscriber
func (e *EventBus) Unsubscribe(eventType EventType, subId EventSubscriberId) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if evtTypeSubs, ok := e.subscribers[eventType]; ok {
		delete(evtTypeSubs, subId)
	}
	if e.metrics != nil {
		e.metrics.subscribers.WithLabelValues(string(eventType)).Dec()
	}
}

// Publish allows a producer to send an event of a particular type to all
// subscribers
func (e *EventBus) Publish(eventType EventType, evt Event) {
	// Build list of channels inside read lock to avoid map race condition
	e.mu.RLock()
	subs, ok := e.subscribers[eventType]
	subChans := make([]chan Event, 0, len(subs))
	if ok {
		for _, subCh := range subs {
			subChans = append(subChans, subCh)
		}
	}
	e.mu.RUnlock()
	// Send event on gathered channels
	for _, subCh := range subChans {
		// NOTE: this is purposely a blocking operation to prevent dropping data
		// XXX: do we maybe want to detect a blocked channel and temporarily set
		// it aside
		// to get the event sent to the other subscribers?
		subCh <- evt
	}
	if e.metrics != nil {
		e.metrics.eventsTotal.WithLabelValues(string(eventType)).Inc()
	}
}
