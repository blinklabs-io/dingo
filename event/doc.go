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

// Package event provides Dingo's EventBus: an in-process publish/
// subscribe primitive that lets components communicate without
// holding references to each other.
//
// Components use typed events for asynchronous cross-component
// notifications. Synchronous state reads still use direct calls,
// callbacks, or narrow interfaces supplied by the node composition
// layer. This keeps event traffic explicit without forcing every
// query through the bus.
//
// # Publishing
//
//	eventBus.Publish(
//	    chain.ChainForkEventType,
//	    event.NewEvent(chain.ChainForkEventType, chain.ChainForkEvent{...}),
//	)
//
// Use PublishAsync for events that do not need to be delivered
// synchronously with the publisher's call stack.
//
// # Delivery guarantees
//
// The bus does not drop events. When a subscriber's channel buffer or
// the shared async queue is full, the publisher waits for capacity
// rather than discarding the event, so ingestion slows instead of
// losing work that subscribers derive state from. Waiting is bounded
// only by shutdown: Stop, Close, and Unsubscribe all release publishers
// parked on a full buffer. Buffers themselves stay bounded, so
// backpressure never trades event loss for unbounded memory.
//
// The practical consequence is that a subscriber which stops draining
// stalls its publishers. Subscribers that take a channel from Subscribe
// must drain it for as long as they hold the subscription and must
// Unsubscribe when they stop. A delivery parked for a long time is
// reported by the event_delivery_blocked_total metric and an "event
// delivery stalled" warning.
//
// # Subscribing
//
//	eventBus.SubscribeFunc(chain.ChainForkEventType, func(evt event.Event) {
//	    e, ok := evt.Data.(chain.ChainForkEvent)
//	    if !ok { return }
//	    // handle e
//	})
//
// The bus runs a pool of async worker goroutines (default 4) to
// dispatch subscribers. Subscriber callbacks must be non-blocking; if
// a callback needs to do real work, push it onto its own goroutine.
// A slow subscriber backpressures the bus and delays delivery of
// unrelated events: the async workers are a shared pool, so a
// subscriber that parks them holds up every async event type.
//
// Event type constants live alongside the package that owns the
// event: ChainForkEventType in chain, ChainSwitchEventType in
// chainselection, PeerEligibilityChangedEventType in peergov, etc.
package event
