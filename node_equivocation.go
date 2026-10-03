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
	"bytes"
	"encoding/hex"
	"log/slog"
	"strconv"
	"sync"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// maxEquivocationRolledBack bounds the rolled-back blocks retained for
// comparison. A rollback is at most k blocks deep, so this covers the deepest
// one and older entries cannot meet a block on the live chain again.
const maxEquivocationRolledBack = 2160
const maxEquivocationReportedPairs = 2160

// rolledBackBlock is a block that left the chain, kept to recognise a
// competing block from the same pool.
type rolledBackBlock struct {
	hash   []byte
	pool   string
	slot   uint64
	number uint64
}

type equivocationPair [2]string

// equivocationDetector counts blocks that compete with an earlier block of the
// same pool: same issuer, a different hash, and the same slot or block number.
// Two pools can never legitimately share a cold key, so each such pair means
// the key is forging in two places at once.
//
// A competitor can only be seen once the chain has switched to or away from
// it: the losing block is read from the rollback that removes it and compared
// with each block added afterwards. Blocks are decoded only when a candidate
// pair shares a slot or number, so the per-block cost on a normal sync is one
// scan of a normally empty list.
type equivocationDetector struct {
	logger        *slog.Logger
	counter       *prometheus.CounterVec
	selfPoolID    string
	rolledBack    []rolledBackBlock
	reported      map[equivocationPair]struct{}
	reportedOrder []equivocationPair
	mu            sync.Mutex
}

// newEquivocationDetector creates a detector reporting through a
// dingo_equivocation_total counter registered on registry (nil disables the
// counter but keeps the warning log).
func newEquivocationDetector(
	registry prometheus.Registerer,
	logger *slog.Logger,
) *equivocationDetector {
	d := &equivocationDetector{logger: logger}
	if registry != nil {
		d.counter = promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_equivocation_total",
				Help: "pairs of competing blocks from one pool at the same slot or block number, by pool and whether the pool is this node's own",
			},
			[]string{"pool_id", "self_key"},
		)
	}
	return d
}

// setSelfPoolID records this node's own pool ID so its equivocations are
// labelled self_key="true".
func (d *equivocationDetector) setSelfPoolID(poolID string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.selfPoolID = poolID
}

// handleChainUpdate processes a chain.update event.
func (d *equivocationDetector) handleChainUpdate(evt event.Event) {
	switch data := evt.Data.(type) {
	case chain.ChainRollbackEvent:
		d.recordRolledBack(data.RolledBackBlocks)
	case chain.ChainBlockEvent:
		d.checkAdded(data.Block)
	}
}

func (d *equivocationDetector) recordRolledBack(blocks []models.Block) {
	entries := make([]rolledBackBlock, 0, len(blocks))
	for _, block := range blocks {
		pool, ok := blockPoolID(block)
		if !ok {
			continue
		}
		entries = append(entries, rolledBackBlock{
			hash:   block.Hash,
			pool:   pool,
			slot:   block.Slot,
			number: block.Number,
		})
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := make(map[string]struct{}, len(d.rolledBack)+len(entries))
	for _, prior := range d.rolledBack {
		seen[string(prior.hash)] = struct{}{}
	}
	for _, entry := range entries {
		hash := string(entry.hash)
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		d.rolledBack = append(d.rolledBack, entry)
	}
	if over := len(d.rolledBack) - maxEquivocationRolledBack; over > 0 {
		d.rolledBack = append(d.rolledBack[:0], d.rolledBack[over:]...)
	}
}

func (d *equivocationDetector) checkAdded(block models.Block) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var rivals []rolledBackBlock
	for _, prior := range d.rolledBack {
		if (prior.slot == block.Slot || prior.number == block.Number) &&
			!bytes.Equal(prior.hash, block.Hash) {
			rivals = append(rivals, prior)
		}
	}
	if len(rivals) == 0 {
		return
	}
	pool, ok := blockPoolID(block)
	if !ok {
		return
	}
	for _, rival := range rivals {
		if rival.pool != pool {
			continue
		}
		pair := orderedEquivocationPair(rival.hash, block.Hash)
		if _, ok := d.reported[pair]; ok {
			continue
		}
		d.rememberEquivocationPair(pair)
		d.report(pool, block, rival)
	}
}

func orderedEquivocationPair(first, second []byte) equivocationPair {
	if bytes.Compare(first, second) > 0 {
		first, second = second, first
	}
	return equivocationPair{string(first), string(second)}
}

func (d *equivocationDetector) rememberEquivocationPair(pair equivocationPair) {
	if d.reported == nil {
		d.reported = make(map[equivocationPair]struct{})
	}
	d.reported[pair] = struct{}{}
	d.reportedOrder = append(d.reportedOrder, pair)
	if over := len(d.reportedOrder) - maxEquivocationReportedPairs; over > 0 {
		for _, expired := range d.reportedOrder[:over] {
			delete(d.reported, expired)
		}
		d.reportedOrder = append(d.reportedOrder[:0], d.reportedOrder[over:]...)
	}
}

// report must be called with d.mu held.
func (d *equivocationDetector) report(
	pool string,
	block models.Block,
	rival rolledBackBlock,
) {
	self := d.selfPoolID != "" && d.selfPoolID == pool
	d.logger.Warn(
		"equivocation detected: competing blocks from one pool",
		"pool_id", pool,
		"self_key", self,
		"slot", block.Slot,
		"block_number", block.Number,
		"hash", hex.EncodeToString(block.Hash),
		"competing_slot", rival.slot,
		"competing_block_number", rival.number,
		"competing_hash", hex.EncodeToString(rival.hash),
	)
	if d.counter != nil {
		d.counter.WithLabelValues(pool, strconv.FormatBool(self)).Inc()
	}
}

// blockPoolID returns the pool ID of the block's issuer.
func blockPoolID(block models.Block) (string, bool) {
	decoded, err := block.Decode()
	if err != nil {
		return "", false
	}
	return decoded.IssuerVkey().PoolId(), true
}

// subscribeEquivocationDetector feeds chain.update events to the detector. It
// is an observer: a detached subscription loses the metric, not node state.
func (n *Node) subscribeEquivocationDetector() {
	if n.equivocation == nil {
		return
	}
	n.subscribeDetachableEvent(
		chain.ChainUpdateEventType,
		n.equivocation.handleChainUpdate,
	)
}
