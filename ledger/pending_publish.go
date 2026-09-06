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

package ledger

import (
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
)

// pendingPublishes collects events produced while a lock is held and
// publishes them once it has been released.
//
// Publishing to the EventBus while holding ls.chainsyncMutex can deadlock
// the node. EventBus delivery blocks when a subscriber's buffer is full —
// that is deliberate, the bus backpressures rather than dropping events —
// and ChainsyncResyncEventType's subscriber calls
// LedgerState.RecoverAfterLocalRollback, which takes that same mutex. Once
// the buffer fills, the subscriber parks waiting for the lock the
// publisher is holding, the publisher parks waiting for buffer capacity
// the subscriber would have freed, and neither ever proceeds.
//
// The damage does not stay local. handleConnectionClosedEvent also takes
// chainsyncMutex, so its ledger.conn_closed channel stops draining; the
// node.go handler that translates connmanager.conn_closed into
// ledger.conn_closed then blocks inside its own callback, which stops
// connmanager.conn_closed from draining, and every subsequent connection
// close parks another publisher goroutine. A DevNet run reproduced this as
// ~217k "event delivery stalled: subscriber not draining
// type=connmanager.conn_closed" warnings in five minutes while the node
// kept forging but stopped answering Node-to-Node handshakes entirely.
//
// Callers register the flush with defer *before* taking the lock, so that
// defer's LIFO order runs it after the unlock:
//
//	var pending pendingPublishes
//	defer pending.flush()
//	ls.chainsyncMutex.Lock()
//	defer ls.chainsyncMutex.Unlock()
//	...
//	pending.add(ls.config.EventBus, SomeEventType, event.NewEvent(...))
//
// Delivery is unchanged apart from happening a moment later: Publish is
// already asynchronous from the subscriber's point of view (it hands the
// event to a buffered channel drained by a dispatch goroutine), so no
// caller can have depended on a handler running before the publishing
// function returned. Ordering between events queued by one call is
// preserved.
type pendingPublishes struct {
	events []pendingPublish
	// chainDrains holds chains whose chain-level sequencer must be drained
	// after this queue flushes. The mutex-holding paths that add a block or
	// roll back the chain no longer hand their chain.update event back for
	// requeueing here; the chain enqueues it on its own shared sequencer under
	// c.mutex (so publication order matches chain-mutation order across every
	// handler), and the caller registers the chain here so flush() drains that
	// sequencer once the outer ledger mutex is released. See
	// chain.Chain.PublishPendingChainUpdates and drainChain.
	chainDrains []*chain.Chain
}

type pendingPublish struct {
	bus       *event.EventBus
	eventType event.EventType
	evt       event.Event
}

// add queues an event to be published after the caller's lock is
// released. A nil bus is ignored, matching the nil checks at the call
// sites.
//
// A nil receiver publishes immediately. That is what lets a helper be
// called from both locked and unlocked paths without duplicating it: the
// locked caller threads its own queue down, and an unlocked caller passes
// nil to get the original behaviour.
func (p *pendingPublishes) add(
	bus *event.EventBus,
	eventType event.EventType,
	evt event.Event,
) {
	if bus == nil {
		return
	}
	if p == nil {
		bus.Publish(eventType, evt)
		return
	}
	p.events = append(p.events, pendingPublish{
		bus:       bus,
		eventType: eventType,
		evt:       evt,
	})
}

// drainChain registers a chain whose deferred chain.update / chain.fork events
// must be published once the caller's outer mutex is released. Registration is
// idempotent per chain: the block-add drain enqueues several blocks' events but
// only needs one drain.
//
// A nil receiver drains immediately, matching add's nil-receiver contract: the
// unlocked / test path holds no deadlock-prone mutex, so publishing inline is
// safe. A nil chain is ignored.
func (p *pendingPublishes) drainChain(c *chain.Chain) {
	if c == nil {
		return
	}
	if p == nil {
		c.PublishPendingChainUpdates()
		return
	}
	for _, existing := range p.chainDrains {
		if existing == c {
			return
		}
	}
	p.chainDrains = append(p.chainDrains, c)
}

// flush publishes everything queued, in the order it was added, then drains any
// registered chain sequencers. The chain drains run last and only after every
// directly-queued event has been published; each publishes its own events FIFO
// in chain-mutation order (see chain.Chain.PublishPendingChainUpdates).
func (p *pendingPublishes) flush() {
	for _, pub := range p.events {
		pub.bus.Publish(pub.eventType, pub.evt)
	}
	p.events = nil
	for _, c := range p.chainDrains {
		c.PublishPendingChainUpdates()
	}
	p.chainDrains = nil
}
