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

package event

import "context"

// OrderedQueueSize is the per-event-type buffer behind PublishOrdered. It is
// larger than AsyncQueueSize because an ordered lane is drained by exactly one
// worker rather than by the shared pool, so it has to absorb the same bursts
// with less drain throughput. The ledger publishes ledger.tx from an
// after-commit callback, and a publisher that parks delays the block-apply
// pipeline even though the transaction is already durable, so the buffer is
// sized to swallow a bulk-sync batch's transactions rather than to bound
// memory tightly. It is still bounded: past this point the publisher waits,
// exactly as it already did on a full shared async queue.
const OrderedQueueSize = 10000

// orderedLane is one event type's FIFO plus the single worker that drains it.
// One worker is the whole mechanism: the shared async pool cannot preserve
// order because AsyncWorkerPoolSize workers dequeue concurrently and race each
// other into Publish, so two events enqueued in order can be delivered to a
// subscriber in either order (blinklabs-io/dingo#2287).
type orderedLane struct {
	queue chan Event
	// stopCh is the bus stop channel captured when the lane was created.
	// e.stopCh is swapped by a Stop/restart cycle, so the worker must watch
	// the generation it was started under rather than re-reading the field.
	stopCh chan struct{}
}

// PublishOrdered enqueues an event for asynchronous delivery that preserves
// publisher order. Events published to the same event type are delivered to
// each subscriber in the order they were published, which PublishAsync does
// not promise.
//
// Ordering holds per event type, and only among publishes that are themselves
// ordered: two goroutines publishing concurrently to one type still race to
// enqueue, and nothing is promised across different event types. A single
// producer sequence -- a ledger rollback's transaction undo events followed by
// the next block's transaction events -- is exactly the case this is for.
//
// Like PublishAsync it does not drop for a live subscriber: a full lane makes
// the publisher wait for capacity rather than discarding the event. A stalled
// subscriber is detached after the delivery timeout, which lets its lane make
// progress for healthy subscribers; shutdown also releases the wait. Returns
// false when the EventBus is stopped or closed.
//
// Each event type gets its own lane, so a slow subscriber delays only its own
// event type instead of holding up every async event as it would on the shared
// pool.
func (e *EventBus) PublishOrdered(eventType EventType, evt Event) bool {
	return e.PublishOrderedContext(context.Background(), eventType, evt)
}

// PublishOrderedContext is PublishOrdered that also abandons the publish when
// ctx is done. A stalled subscriber is detached after the delivery timeout,
// but a caller on a shutdown-critical goroutine -- one something else waits
// for before the EventBus itself stops, such as a LedgerState the node closes
// while keeping the bus running for a live restore -- must pass a context it
// cancels when it needs a shorter bound.
//
// Abandoning is not a drop in the delivery-guarantee sense: the event was
// never accepted, and the false return says so.
func (e *EventBus) PublishOrderedContext(
	ctx context.Context,
	eventType EventType,
	evt Event,
) bool {
	// Check cancellation before accepting anything. Deferring it to the
	// full-lane wait below would let a publish on an already-cancelled
	// context still be enqueued and delivered whenever the lane happens to
	// have room, which is the common case.
	select {
	case <-ctx.Done():
		return false
	default:
	}
	e.stopMu.RLock()
	if e.stopped || e.closed {
		e.stopMu.RUnlock()
		return false
	}
	lane := e.orderedLane(eventType)
	stopCh := lane.stopCh
	// Released before waiting on the lane: shutdown needs stopMu for write,
	// and it is shutdown that releases us.
	e.stopMu.RUnlock()

	select {
	case lane.queue <- evt:
	default:
		// Lane is full: wait for space rather than losing the event.
		if e.metrics != nil {
			e.metrics.asyncEnqueueBlocked.WithLabelValues(string(eventType)).
				Inc()
		}
		select {
		case lane.queue <- evt:
		case <-stopCh:
			return false
		case <-ctx.Done():
			return false
		}
	}

	// shutdown closes stopCh before it marks the bus stopped, so an enqueue
	// can still land in a lane whose worker has already exited. Report that
	// as a failed publish rather than as a delivery that never happens.
	select {
	case <-stopCh:
		return false
	default:
	}
	return true
}

// orderedLane returns the lane for an event type, creating it and starting its
// worker on first use. The caller must hold stopMu for read and must have
// observed the bus as neither stopped nor closed: that is what makes the
// asyncWg.Add here safe against shutdown's asyncWg.Wait, since shutdown cannot
// take stopMu for write until the caller releases it.
func (e *EventBus) orderedLane(eventType EventType) *orderedLane {
	e.orderedMu.Lock()
	defer e.orderedMu.Unlock()
	if lane, ok := e.orderedLanes[eventType]; ok {
		return lane
	}
	if e.orderedLanes == nil {
		e.orderedLanes = make(map[EventType]*orderedLane)
	}
	lane := &orderedLane{
		queue:  make(chan Event, OrderedQueueSize),
		stopCh: e.stopCh,
	}
	e.orderedLanes[eventType] = lane
	e.asyncWg.Add(1)
	go e.orderedWorker(lane, eventType)
	return lane
}

// orderedWorker drains one lane. There is exactly one per lane, so the order it
// calls Publish in is the order events were enqueued in, and Publish itself
// hands each event to every subscriber before returning.
func (e *EventBus) orderedWorker(lane *orderedLane, eventType EventType) {
	defer e.asyncWg.Done()
	for {
		// Prioritize shutdown before attempting to receive queued work.
		select {
		case <-lane.stopCh:
			return
		default:
		}

		select {
		case <-lane.stopCh:
			return
		case evt := <-lane.queue:
			// Drop queued work if shutdown began after the dequeue but
			// before Publish ran, so Stop/Close do not deliver buffered
			// events. This matches asyncWorker.
			select {
			case <-lane.stopCh:
				return
			default:
			}
			// A slow in-memory subscriber parks this worker until it
			// drains, which is the backpressure that keeps events from
			// being dropped; shutdown closes stopCh first, releasing any
			// parked delivery.
			e.Publish(eventType, evt)
		}
	}
}

// resetOrderedLanes drops every lane so the next PublishOrdered rebuilds them
// against the current stop channel. Called from shutdown once the previous
// generation's workers have exited.
func (e *EventBus) resetOrderedLanes() {
	e.orderedMu.Lock()
	defer e.orderedMu.Unlock()
	e.orderedLanes = nil
}
