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

package ouroboros

import (
	"sync"

	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
)

// trackingRegisterer wraps the node's Prometheus registry and remembers every
// collector registered through it, so an Ouroboros can hand all of them back
// on Close.
//
// Tracking rather than keeping named handles for each metric is deliberate:
// collectors get added to this package regularly, and a hand-maintained list
// would silently fall behind. Since a missed collector means promauto panics
// on duplicate registration the next time a live restore rebuilds Ouroboros,
// the bookkeeping has to be automatic to be safe.
//
// A nil inner registerer is supported and makes every method a no-op, matching
// promauto.With(nil) semantics used elsewhere for metrics-disabled builds.
type trackingRegisterer struct {
	inner      prometheus.Registerer
	mu         sync.Mutex
	collectors []prometheus.Collector
}

func newTrackingRegisterer(inner prometheus.Registerer) *trackingRegisterer {
	return &trackingRegisterer{inner: inner}
}

func (r *trackingRegisterer) Register(c prometheus.Collector) error {
	if r == nil || r.inner == nil {
		return nil
	}
	if err := r.inner.Register(c); err != nil {
		return err
	}
	r.mu.Lock()
	r.collectors = append(r.collectors, c)
	r.mu.Unlock()
	return nil
}

func (r *trackingRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := r.Register(c); err != nil {
			panic(err)
		}
	}
}

func (r *trackingRegisterer) Unregister(c prometheus.Collector) bool {
	if r == nil || r.inner == nil {
		return false
	}
	ok := r.inner.Unregister(c)
	r.mu.Lock()
	for i, tracked := range r.collectors {
		if tracked == c {
			r.collectors = append(r.collectors[:i], r.collectors[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
	return ok
}

// unregisterAll hands every tracked collector back to the underlying registry
// and forgets them, leaving the registry able to accept an identically-named
// collector from a replacement Ouroboros.
func (r *trackingRegisterer) unregisterAll() {
	if r == nil || r.inner == nil {
		return
	}
	r.mu.Lock()
	collectors := r.collectors
	r.collectors = nil
	r.mu.Unlock()
	for _, c := range collectors {
		r.inner.Unregister(c)
	}
}

// subscription records one EventBus registration made by this Ouroboros on its
// own behalf, so Close can take it back off a bus that outlives the instance.
type subscription struct {
	eventType event.EventType
	id        event.EventSubscriberId
}

// subscribeTracked subscribes and records the registration for Close.
func (o *Ouroboros) subscribeTracked(
	eventType event.EventType,
	handler event.EventHandlerFunc,
) {
	if o.eventBus == nil {
		return
	}
	id := o.eventBus.SubscribeFunc(eventType, handler)
	o.subscriptionsMu.Lock()
	o.subscriptions = append(
		o.subscriptions,
		subscription{eventType: eventType, id: id},
	)
	o.subscriptionsMu.Unlock()
}

// Close releases everything this Ouroboros owns that outlives it: EventBus
// subscriptions, Prometheus collectors, and the background Leios
// endorser-block persistence writer.
//
// It exists because Ouroboros takes its dependencies at construction and so
// cannot be retained across a live snapshot/restore. That operation discards
// and rebuilds the ledger state, mempool, chainsync state, connection manager
// and peer governor, so it must discard and rebuild Ouroboros too — and the
// EventBus and Prometheus registry it was sharing both survive the cycle.
// Without this, each restore would leave stale handlers permanently attached
// and the replacement's metric registration would panic on duplicates.
//
// Close is idempotent, so Run()'s deferred shutdown and an explicit
// live-restore teardown can both call it.
func (o *Ouroboros) Close() error {
	o.subscriptionsMu.Lock()
	subs := o.subscriptions
	o.subscriptions = nil
	o.subscriptionsMu.Unlock()
	if o.eventBus != nil {
		for _, sub := range subs {
			// UnsubscribeAndWait, not Unsubscribe: Close is called on the
			// live restore path immediately before the ledger state,
			// mempool and chainsync state this instance holds are closed
			// and replaced. Returning while a handler is still mid-flight
			// would let it touch a closed dependency.
			o.eventBus.UnsubscribeAndWait(sub.eventType, sub.id)
		}
	}
	o.StopLeiosPersistWriter()
	o.registerer.unregisterAll()
	return nil
}
