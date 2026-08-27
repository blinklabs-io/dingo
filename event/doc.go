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
// synchronously with the publisher's call stack, and PublishOrdered
// for those that additionally need to reach subscribers in the order
// they were published.
//
// # Ordering guarantees
//
// Publish and PublishBlocking deliver on the caller's goroutine, so a
// single publisher's events reach each subscriber in call order.
//
// PublishAsync does not preserve order. It hands the event to a shared
// queue drained by AsyncWorkerPoolSize workers that race each other
// into Publish, so two events enqueued in order can be delivered in
// either order -- observed as several inversions per few hundred
// events, even from one publishing goroutine. Use it only where
// subscribers treat each event independently.
//
// PublishOrdered is the async path that does preserve order. Each event
// type gets its own FIFO drained by exactly one worker, so events
// published to one type arrive in publish order, and a slow subscriber
// delays only its own event type rather than every async event. The
// guarantee is per event type and only over publishes that are
// themselves sequenced: concurrent publishers still race to enqueue,
// and nothing is promised across different event types.
//
// ledger.tx uses PublishOrdered. A subscriber deriving state from it can
// rely on a block's transactions arriving in index order, and on a
// rollback's undo events (Rollback: true) arriving before any
// transaction event the ledger emits afterwards. Forward Apply events are
// registered with the database transaction's AfterCommit hook, so a rollback
// or failed commit publishes none. See
// blinklabs-io/dingo#2287: while those undo events were emitted from a
// detached goroutine, a subscriber could apply an undo after the redo
// that followed it and stay wrong indefinitely.
//
// A lane orders what reaches it; it cannot order two publishers racing
// to reach it. Where events for one stream come from more than one
// goroutine, as ledger.tx does, the publishers need their own
// happens-before -- see the ledger.tx section of ARCHITECTURE.md for how
// the rollback and block-apply paths establish theirs.
//
// A healthy subscriber drains a full lane; the ordinary policy detaches a
// stalled one after the delivery timeout. A caller on a goroutine something
// else waits for before the bus stops must still use PublishOrderedContext and
// cancel that context when it needs a shorter bound than the subscriber-
// delivery timeout.
//
// # Delivery guarantees
//
// The bus does not drop events for a live subscriber. When a subscriber's
// channel buffer or the shared async queue is full, the publisher waits for
// capacity rather than discarding the event, so ingestion slows instead of
// losing work that subscribers derive state from. A subscriber that remains
// full for the delivery timeout is detached by the ordinary subscription
// policy: events already accepted into its channel retain their order, while
// the event that cannot be accepted and later events continue only to healthy
// subscribers. A lossless owner can explicitly select the blocking policy when
// detaching its stream would make recovery unsafe. This bounds a dead ordinary
// subscriber's impact without trading unbounded memory for liveness. Stop,
// Close, and Unsubscribe also release publishers parked on a full buffer.
//
// The practical consequence is that a slow subscriber backpressures its
// publishers until it drains or is detached. A publisher must not hold a lock
// that a subscriber of the same event acquires: once the buffer fills,
// the subscriber waits for the lock and the publisher waits for the capacity
// the subscriber would free, and neither proceeds until the delivery bound
// detaches that subscriber. Queue such
// events and publish them after releasing the lock (see ledger's
// pendingPublishes). Subscribers that take a channel from Subscribe
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
// PublishOrdered's per-type lanes are exempt from that specific
// coupling -- a parked lane worker holds up only its own event type --
// but they are single workers, so a slow subscriber stalls that type
// more readily than four shared workers would.
//
// Event type constants live alongside the package that owns the
// event: ChainForkEventType in chain, ChainSwitchEventType in
// chainselection, PeerEligibilityChangedEventType in peergov, etc.
package event
