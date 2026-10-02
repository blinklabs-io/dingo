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

package dingo

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/ledger/forging"
)

// nodeHealth holds the cheap always-available signals the node's readiness
// probe classifies. It is a value field on Node rather than a pointer, and
// every method is safe on the zero value, so a Node built directly in a
// test needs no extra initialization.
//
// A live database Restore or Truncate replaces n.ledgerState; this struct
// deliberately does not, so the probe never has to chase a pointer another
// goroutine is swapping. The rebuilt ledger reports into the same struct
// because ledgerStateConfig closes over n, not over the ledger.
// Every field is guarded by mu: recordTipGap's generation check and its
// store have to be one critical section, or a tick a superseded ledger had
// already dequeued could land between them and restore a stale reading.
// That makes mu the only synchronization these fields need.
type nodeHealth struct {
	mu          sync.Mutex
	generation  uint64
	tipGapSlots uint64
	tipGapKnown bool
	// lastTick is when the slot-tick loop last reported, or the slot clock
	// last reported a pause behind the era-history horizon. It is the node's
	// heartbeat for liveness and is cleared with the gap.
	lastTick time.Time
}

// eventLoopStallLimit is how long the slot clock may stay silent after it has
// ticked before the node is reported wedged. Slots are at most twenty seconds
// (Byron), and the tick handler reads lock-free snapshots precisely so that
// it keeps running through catch-up, so a silence this long is a stuck loop
// and not a busy one.
const eventLoopStallLimit = 5 * time.Minute

// recordTipGap stores the wall-clock-to-tip distance observed on a slot tick.
func (h *nodeHealth) recordTipGap(generation uint64, gapSlots uint64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if generation != h.generation {
		return
	}
	h.tipGapSlots = gapSlots
	h.tipGapKnown = true
	h.lastTick = time.Now()
}

// recordSlotClockAlive refreshes the liveness heartbeat while the slot clock
// pauses its ticks behind the era-history horizon, without touching the tip
// gap. It never starts a heartbeat that no tick has: liveness stays
// unconstrained until the first tick.
func (h *nodeHealth) recordSlotClockAlive(generation uint64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if generation != h.generation || h.lastTick.IsZero() {
		return
	}
	h.lastTick = time.Now()
}

// forgetTipGap returns the probe to its "no chain tip yet" state. Called
// when the ledger that was reporting is torn down for a live database
// Restore or Truncate: the last gap it reported describes a chain the node
// is no longer following, and leaving it in place would let /readyz answer
// 200 through the whole rebuild.
func (h *nodeHealth) forgetTipGap() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.generation++
	h.tipGapKnown = false
	h.tipGapSlots = 0
	h.lastTick = time.Time{}
}

// currentGeneration identifies the ledger instance allowed to report health.
// The caller captures it when building that ledger's callbacks; teardown
// advances it before clearing the old reading. The mutex makes the generation
// check and reading update one operation, so a buffered tick cannot restore a
// stale value after teardown.
func (h *nodeHealth) currentGeneration() uint64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.generation
}

// TipGapSlots reports the distance in slots between the wall-clock slot and
// the node's chain tip, as observed on the most recent slot-clock tick. The
// second return is false until the node has processed its first tick, which
// covers database open, Mithril bootstrap and ledger startup.
//
// This is the same quantity the dingo_tip_gap_slots gauge exports, read
// directly so a health probe does not depend on the Prometheus listener.
func (n *Node) TipGapSlots() (uint64, bool) {
	if n == nil {
		return 0, false
	}
	n.health.mu.Lock()
	defer n.health.mu.Unlock()
	if !n.health.tipGapKnown {
		return 0, false
	}
	return n.health.tipGapSlots, true
}

// forgerReadiness is what the readiness probe reads from the block forger.
type forgerReadiness interface {
	IsRunning() bool
	CredentialsUsable() error
}

// blockProducerReadiness reports why a block producer cannot forge right now.
func blockProducerReadiness(forger forgerReadiness) error {
	if !forger.IsRunning() {
		return errors.New("block forger is not running")
	}
	if err := forger.CredentialsUsable(); err != nil {
		return fmt.Errorf("block producer credentials unusable: %w", err)
	}
	return nil
}

// errLifecycleBusy reports that a startup, shutdown, or live restore or
// truncate holds the lifecycle gates, so the components they replace cannot
// be read safely.
var errLifecycleBusy = errors.New("node lifecycle operation in progress")

// tryLifecycleGates takes the startup and live lifecycle gates without
// waiting, or reports which operation holds them. Startup, live
// restore/truncate and shutdown are the only writers of the database, ledger
// and forging components, and each holds its gate for the whole replacement.
func (n *Node) tryLifecycleGates() (release func(), err error) {
	if !n.startupLifecycleMu.TryLock() {
		return nil, fmt.Errorf(
			"%w: node is starting or shutting down", errLifecycleBusy,
		)
	}
	if !n.liveLifecycleMu.TryLock() {
		n.startupLifecycleMu.Unlock()
		return nil, fmt.Errorf(
			"%w: database restore or truncate in progress", errLifecycleBusy,
		)
	}
	return func() {
		n.liveLifecycleMu.Unlock()
		n.startupLifecycleMu.Unlock()
	}, nil
}

// checkSettledComponents runs capture under the lifecycle gates to read the
// components a probe checks, then runs the check capture returns after the
// gates are released. A probe that cannot take the gates has found a node
// that is not ready by definition.
//
// The check must not run under the gates: the chain-switch and chainsync
// callback handlers TryLock liveLifecycleMu and drop their work when it is
// held, so a probe waiting on a slow database read would make them drop
// events on a healthy node. A component replaced after the release fails its
// check (a closed database refuses the read, a stopped forger is not running),
// which is the right answer for a node in mid-replacement.
func (n *Node) checkSettledComponents(
	capture func() (check func() error, err error),
) error {
	release, err := n.tryLifecycleGates()
	if err != nil {
		return err
	}
	check, err := capture()
	release()
	if err != nil {
		return err
	}
	return check()
}

// DatabaseReady reports why the metadata database cannot serve, or nil.
func (n *Node) DatabaseReady() error {
	return n.checkSettledComponents(func() (func() error, error) {
		db := n.db
		if db == nil {
			return nil, errors.New("database is not open")
		}
		return func() error {
			if _, err := db.GetTip(nil); err != nil {
				return fmt.Errorf("database read failed: %w", err)
			}
			return nil
		}, nil
	})
}

// BlockProducerReady reports why a node configured as a block producer cannot
// forge, or nil. A relay has no forging state to be ready for.
func (n *Node) BlockProducerReady() error {
	if !n.config.blockProducer {
		return nil
	}
	return n.checkSettledComponents(func() (func() error, error) {
		forger := n.blockForger
		if forger == nil {
			return nil, errors.New("block forger is not initialized")
		}
		return func() error { return blockProducerReadiness(forger) }, nil
	})
}

var _ forgerReadiness = (*forging.BlockForger)(nil)

// EventLoopResponsive reports why the node's event loop should be considered
// wedged, or nil. A node that has not ticked yet reports nil: database open,
// Mithril bootstrap and ledger startup all precede the first tick.
func (n *Node) EventLoopResponsive() error {
	if n == nil {
		return nil
	}
	n.health.mu.Lock()
	lastTick := n.health.lastTick
	n.health.mu.Unlock()
	if lastTick.IsZero() {
		return nil
	}
	if silent := time.Since(lastTick); silent > eventLoopStallLimit {
		return fmt.Errorf(
			"slot clock has not ticked for %s",
			silent.Round(time.Second),
		)
	}
	return nil
}
