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

package dmq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// Event types published by MessageMempool. See AGENTS.md's "Key events"
// table.
const (
	AddMessageEventType    event.EventType = "dmq.add_message"
	RemoveMessageEventType event.EventType = "dmq.remove_message"
)

// AddMessageEvent is published whenever a new DMQ message is admitted.
type AddMessageEvent struct {
	MessageID []byte
}

// RemoveMessageEvent is published whenever a DMQ message leaves the pool,
// whether by TTL expiry or explicit removal.
type RemoveMessageEvent struct {
	MessageID []byte
}

// Sentinel errors returned by MessageMempool.Add.
var (
	// ErrInvalidMessageID is returned when a message's ID (explicit or
	// computed) is not exactly 32 bytes.
	ErrInvalidMessageID = errors.New(
		"dmq: message id must be exactly 32 bytes",
	)
	// ErrExpired is returned when a submitted message's expiresAt has
	// already passed.
	ErrExpired = errors.New("dmq: message is already expired")
	// ErrFull is returned when admitting a message would exceed
	// Config.Capacity or Config.MaxMessages.
	ErrFull = errors.New("dmq: message mempool is full")
)

// DefaultCleanupInterval is used when Config.CleanupInterval is zero.
const DefaultCleanupInterval = 30 * time.Second

// Config configures a MessageMempool.
type Config struct {
	// Capacity is the maximum total size, in CBOR-encoded bytes, of all
	// messages held at once. Zero means unlimited.
	Capacity int64
	// MaxMessages is the maximum message count held at once. Zero means
	// unlimited.
	MaxMessages int
	// CleanupInterval controls how often the background goroutine sweeps
	// for expired messages. Zero uses DefaultCleanupInterval.
	CleanupInterval time.Duration
	// EventBus, when non-nil, receives AddMessageEventType and
	// RemoveMessageEventType notifications.
	EventBus *event.EventBus
	// Now, when non-nil, replaces time.Now for expiry checks. Tests use
	// this for deterministic TTL behavior.
	Now func() time.Time
}

// entry is one message held in the pool, tagged with its arrival sequence.
type entry struct {
	seq  uint64
	id   [32]byte
	msg  ocommon.DmqMessage
	size int64
}

// peerCursor tracks how far one peer has been fed the arrival-ordered
// message log.
type peerCursor struct {
	mu      sync.Mutex
	lastSeq uint64
}

// MessageMempool is a CIP-0137 DMQ message pool. See the package doc comment
// for its scope.
type MessageMempool struct {
	mu       sync.RWMutex // guards byID, order, nextSeq, curBytes
	byID     map[[32]byte]*entry
	order    []*entry // ascending by seq; append-only except compaction
	nextSeq  uint64
	curBytes int64

	peersMu sync.Mutex // guards peers
	peers   map[string]*peerCursor

	capacity        int64
	maxMessages     int
	cleanupInterval time.Duration
	eventBus        *event.EventBus
	now             func() time.Time

	done      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// NewMessageMempool constructs a MessageMempool. Call Start to begin the
// background TTL sweep.
func NewMessageMempool(cfg Config) *MessageMempool {
	interval := cfg.CleanupInterval
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &MessageMempool{
		byID:            make(map[[32]byte]*entry),
		peers:           make(map[string]*peerCursor),
		capacity:        cfg.Capacity,
		maxMessages:     cfg.MaxMessages,
		cleanupInterval: interval,
		eventBus:        cfg.EventBus,
		now:             now,
		done:            make(chan struct{}),
	}
}

// Start launches the background TTL-expiry goroutine. Calling Start more
// than once is a no-op.
func (p *MessageMempool) Start() {
	p.startOnce.Do(func() {
		p.wg.Add(1)
		go p.expireLoop()
	})
}

// Stop halts the background goroutine, waiting up to ctx's deadline for it
// to exit. Stop is idempotent. It returns an unprefixed error; a caller
// composing it into a larger shutdown should add its own component name
// (e.g. "dmq mempool shutdown: %w"), matching every other component's Stop
// in this codebase.
func (p *MessageMempool) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.done) })

	waitDone := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *MessageMempool) expireLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.removeExpired()
		case <-p.done:
			return
		}
	}
}

// Add inserts msg into the pool. It returns (true, nil) when the message is
// newly admitted, (false, nil) when a message with the same ID is already
// present, and (false, err) when the message is rejected outright: an
// unset/malformed ID (ErrInvalidMessageID), an already-expired message
// (ErrExpired), or a full pool (ErrFull).
//
// A message with no MessageID set has one computed and attached before any
// other check, matching gouroboros' own MarshalCBOR/ID behavior.
func (p *MessageMempool) Add(msg ocommon.DmqMessage) (bool, error) {
	id := msg.ID()
	if len(id) == 0 {
		computed, err := ocommon.ComputeDmqMessageID(msg.Payload)
		if err != nil {
			return false, fmt.Errorf("dmq: compute message id: %w", err)
		}
		msg.SetMessageID(computed)
		id = computed
	}
	if len(id) != 32 {
		return false, ErrInvalidMessageID
	}
	var key [32]byte
	copy(key[:], id)

	// Cheap early duplicate check before paying for a CBOR encode below;
	// re-checked under the write lock since this read is not atomic with
	// the insert.
	p.mu.RLock()
	_, exists := p.byID[key]
	p.mu.RUnlock()
	if exists {
		return false, nil
	}

	if !msg.IsValidAt(p.now()) {
		return false, ErrExpired
	}

	encoded, err := msg.MarshalCBOR()
	if err != nil {
		return false, fmt.Errorf("dmq: encode message: %w", err)
	}
	size := int64(len(encoded))

	p.mu.Lock()
	if _, exists := p.byID[key]; exists {
		p.mu.Unlock()
		return false, nil
	}
	if p.capacity > 0 && p.curBytes+size > p.capacity {
		p.mu.Unlock()
		return false, ErrFull
	}
	if p.maxMessages > 0 && len(p.byID) >= p.maxMessages {
		p.mu.Unlock()
		return false, ErrFull
	}
	p.nextSeq++
	e := &entry{seq: p.nextSeq, id: key, msg: msg, size: size}
	p.byID[key] = e
	p.order = append(p.order, e)
	p.curBytes += size
	p.mu.Unlock()

	if p.eventBus != nil {
		p.eventBus.Publish(
			AddMessageEventType,
			event.NewEvent(AddMessageEventType, AddMessageEvent{MessageID: id}),
		)
	}
	return true, nil
}

// Get returns the pooled message with the given ID, if present.
func (p *MessageMempool) Get(id []byte) (ocommon.DmqMessage, bool) {
	if len(id) != 32 {
		return ocommon.DmqMessage{}, false
	}
	var key [32]byte
	copy(key[:], id)

	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.byID[key]
	if !ok {
		return ocommon.DmqMessage{}, false
	}
	return e.msg, true
}

// NextForPeer returns the next message peerID has not yet seen, in arrival
// order, and advances that peer's cursor past it. It returns the zero
// message and false once the peer has caught up to the current log. An
// unknown peerID is registered on first call, starting from the beginning
// of the currently retained log -- a newly connected peer sees the pool's
// full backlog before anything arriving after it connected.
func (p *MessageMempool) NextForPeer(peerID string) (ocommon.DmqMessage, bool) {
	cursor := p.cursorFor(peerID)

	p.mu.RLock()
	defer p.mu.RUnlock()

	cursor.mu.Lock()
	defer cursor.mu.Unlock()

	idx := sort.Search(len(p.order), func(i int) bool {
		return p.order[i].seq > cursor.lastSeq
	})
	if idx >= len(p.order) {
		return ocommon.DmqMessage{}, false
	}
	e := p.order[idx]
	cursor.lastSeq = e.seq
	return e.msg, true
}

// RemovePeer releases the FIFO cursor tracked for peerID. Callers should
// invoke it when a peer connection closes so its cursor does not linger.
func (p *MessageMempool) RemovePeer(peerID string) {
	p.peersMu.Lock()
	delete(p.peers, peerID)
	p.peersMu.Unlock()
}

func (p *MessageMempool) cursorFor(peerID string) *peerCursor {
	p.peersMu.Lock()
	defer p.peersMu.Unlock()
	c, ok := p.peers[peerID]
	if !ok {
		c = &peerCursor{}
		p.peers[peerID] = c
	}
	return c
}

// Len returns the number of messages currently held in the pool.
func (p *MessageMempool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.byID)
}

// SizeBytes returns the total CBOR-encoded size, in bytes, of every message
// currently held in the pool.
func (p *MessageMempool) SizeBytes() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.curBytes
}

// removeExpired sweeps the pool for messages whose CIP-0137 expiresAt has
// passed, removing them and compacting the retained order. Event
// publication happens after the lock is released.
func (p *MessageMempool) removeExpired() {
	now := p.now()

	p.mu.Lock()
	var removedIDs [][32]byte
	kept := make([]*entry, 0, len(p.order))
	for _, e := range p.order {
		if e.msg.IsValidAt(now) {
			kept = append(kept, e)
			continue
		}
		delete(p.byID, e.id)
		p.curBytes -= e.size
		removedIDs = append(removedIDs, e.id)
	}
	p.order = kept
	p.mu.Unlock()

	if p.eventBus == nil {
		return
	}
	for _, id := range removedIDs {
		p.eventBus.Publish(
			RemoveMessageEventType,
			event.NewEvent(
				RemoveMessageEventType,
				RemoveMessageEvent{MessageID: id[:]},
			),
		)
	}
}
