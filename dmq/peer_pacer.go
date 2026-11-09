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
	"sync"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// maxHeldMessages bounds the messages one connection keeps back. Past it the
// connection stops reading ahead in the pool, so a large backlog from one pool
// stays in the pool, which already bounds it, instead of being copied per
// connection.
const maxHeldMessages = 256

// poolPaceSlack is added to the configured interval. dmq-node measures the gap
// between two messages from when each reaches it, and this stack measures it
// from when each was announced, so network and scheduling jitter can leave the
// gap dmq-node sees a few milliseconds short: a live run was refused at
// 59.998 seconds against a 60 second limit.
const poolPaceSlack = time.Second

// DefaultMinPoolMessageInterval is dmq-node's minimum delay between two
// messages signed by the same pool (minSigDelay in DMQ/Policy.hs). CIP-0137
// does not state it.
const DefaultMinPoolMessageInterval = 60 * time.Second

// poolPacer spaces the messages one connection announces for the same pool.
// dmq-node ends the whole connection when a pool's message arrives sooner
// than its minimum delay after that pool's previous one, so announcing both
// at once drops the connection and stalls every other pool's messages behind
// it. A message held back keeps its place for its own pool only: messages
// from other pools are announced past it.
type poolPacer struct {
	mu       sync.Mutex
	interval time.Duration
	// last is when each pool's most recent message was announced.
	last map[lcommon.Blake2b224]time.Time
	// held are messages waiting for their pool's interval, in arrival order.
	held []pacedMessage
}

type pacedMessage struct {
	pool lcommon.Blake2b224
	msg  ocommon.DmqMessage
}

func newPoolPacer(interval time.Duration) *poolPacer {
	if interval <= 0 {
		return nil
	}
	return &poolPacer{
		interval: interval + poolPaceSlack,
		last:     make(map[lcommon.Blake2b224]time.Time),
	}
}

// admitLocked reports whether a message of pool may be announced at now, and
// records the announcement when it may.
func (p *poolPacer) admitLocked(pool lcommon.Blake2b224, now time.Time) bool {
	if last, ok := p.last[pool]; ok && now.Sub(last) < p.interval {
		return false
	}
	p.last[pool] = now
	return true
}

// releaseHeld returns up to limit held messages whose pool's interval has
// passed, in arrival order. A held message for which usable is false is
// dropped without counting as an announcement, so an expired or already known
// one does not delay a later message of its pool. Entries for pools whose
// interval has passed are forgotten, since an absent pool is treated the same.
func (p *poolPacer) releaseHeld(
	now time.Time,
	limit int,
	usable func(ocommon.DmqMessage) bool,
) []ocommon.DmqMessage {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for pool, last := range p.last {
		if now.Sub(last) >= p.interval {
			delete(p.last, pool)
		}
	}
	var out []ocommon.DmqMessage
	kept := p.held[:0]
	for _, h := range p.held {
		if !usable(h.msg) {
			continue
		}
		if len(out) < limit && p.admitLocked(h.pool, now) {
			out = append(out, h.msg)
			continue
		}
		kept = append(kept, h)
	}
	clear(p.held[len(kept):])
	p.held = kept
	return out
}

// admit reports whether msg may be announced now. When it may not, the pacer
// keeps it and releaseHeld returns it once its pool's interval has passed.
func (p *poolPacer) admit(msg ocommon.DmqMessage, now time.Time) bool {
	if p == nil {
		return true
	}
	pool := lcommon.Blake2b224Hash(msg.ColdVerificationKey)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.admitLocked(pool, now) {
		return true
	}
	p.held = append(p.held, pacedMessage{pool: pool, msg: msg})
	return false
}

// full reports whether the pacer holds as many messages as it may.
func (p *poolPacer) full() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.held) >= maxHeldMessages
}

// nextRelease reports how long until a held message can be announced. It
// reports false when nothing is held.
func (p *poolPacer) nextRelease(now time.Time) (time.Duration, bool) {
	if p == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var wait time.Duration
	found := false
	for _, h := range p.held {
		d := p.last[h.pool].Add(p.interval).Sub(now)
		if !found || d < wait {
			wait, found = d, true
		}
	}
	return max(wait, 0), found
}
