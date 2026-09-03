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

// Package devnet provides a test harness for running integration tests
// against a private Cardano DevNet consisting of Dingo and cardano-node
// instances connected via Docker Compose.
//
// The chain-observation types in this file carry no build tag so the
// state machine driving the DevNet scenarios is unit-tested on every
// ordinary `go test ./...` run, without Docker. The pieces that dial real
// nodes are gated behind the `devnet` tag.
package devnet

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// maxRetainedHeaders bounds the per-node observed-header window. A
// scenario only ever reasons about recent chain state, and an unbounded
// slice would grow for the life of a soak run.
const maxRetainedHeaders = 4096

// ChainTip holds the chain tip information retrieved from a node.
type ChainTip struct {
	SlotNumber  uint64 `json:"slot"`
	BlockNumber uint64 `json:"block"`
	Hash        []byte `json:"hash"`
}

// ChainPoint is a slot/hash pair identifying a position on a chain. The
// zero value is the origin.
type ChainPoint struct {
	Slot uint64 `json:"slot"`
	Hash []byte `json:"hash"`
}

// IsOrigin reports whether the point refers to the chain origin, which
// ChainSync encodes as an empty point rather than a slot/hash pair.
func (p ChainPoint) IsOrigin() bool {
	return p.Slot == 0 && len(p.Hash) == 0
}

// ObservedHeader is one block header seen in a ChainSync RollForward.
type ObservedHeader struct {
	Slot        uint64 `json:"slot"`
	BlockNumber uint64 `json:"block"`
	Hash        []byte `json:"hash"`
	// BodySize is the header's declared block body size. It is what
	// lets a scenario tell a block carrying transactions from an empty
	// one without a node-to-client connection, which the mixed
	// cardano-node topology does not expose.
	BodySize uint64 `json:"bodySize"`
}

// ChainSnapshot is a consistent point-in-time copy of one node's
// observed chain, safe to read without holding the observer's lock.
// The json tags matter: a snapshot is what
// NodeControl.CaptureFailureArtifacts writes to observed-chains.json, and
// that file is often the only surviving record of what a node's chain did
// once the DevNet has been torn down.
type ChainSnapshot struct {
	Node             string           `json:"node"`
	Tip              ChainTip         `json:"tip"`
	ServerTip        ChainTip         `json:"serverTip"`
	Headers          []ObservedHeader `json:"headers"`
	RollForwards     int              `json:"rollForwards"`
	RollBackwards    int              `json:"rollBackwards"`
	MaxRollbackDepth uint64           `json:"maxRollbackDepth"`
	Connects         int              `json:"connects"`
	Disconnects      int              `json:"disconnects"`
	Connected        bool             `json:"connected"`
	LastError        string           `json:"lastError,omitempty"`
}

// HashAt returns the observed header hash at slot, if the slot is still
// inside the retained window.
func (s ChainSnapshot) HashAt(slot uint64) ([]byte, bool) {
	for _, h := range s.Headers {
		if h.Slot == slot {
			return h.Hash, true
		}
	}
	return nil, false
}

// String renders the snapshot as a single diagnostic line. Await failures
// embed it so a timeout says what the node was actually doing.
func (s ChainSnapshot) String() string {
	return fmt.Sprintf(
		"%s{tip=slot %d/block %d, connected=%t, fwd=%d, back=%d,"+
			" maxRollback=%d, lastErr=%q}",
		s.Node, s.Tip.SlotNumber, s.Tip.BlockNumber, s.Connected,
		s.RollForwards, s.RollBackwards, s.MaxRollbackDepth, s.LastError,
	)
}

// broadcaster wakes every waiter whenever observed state changes. The
// channel is closed and replaced rather than sent on, so any number of
// waiters observe a single change without the signaller blocking.
type broadcaster struct {
	mu sync.Mutex
	ch chan struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{ch: make(chan struct{})}
}

// wait returns the channel closed by the next signal.
func (b *broadcaster) wait() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ch
}

func (b *broadcaster) signal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	close(b.ch)
	b.ch = make(chan struct{})
}

// ObservedChain accumulates the ChainSync messages one node sends and
// exposes them as timeout-bound conditions. It replaces polling a node's
// tip over repeated short-lived connections: the chain view is pushed by
// the node, so a condition becomes true as soon as the protocol event
// that satisfies it arrives.
//
// All methods are safe for concurrent use; the observer goroutine writes
// while scenario assertions read.
type ObservedChain struct {
	node   string
	notify *broadcaster

	mu               sync.Mutex
	headers          []ObservedHeader
	tip              ChainTip
	serverTip        ChainTip
	rollForwards     int
	rollBackwards    int
	maxRollbackDepth uint64
	connects         int
	disconnects      int
	connected        bool
	lastErr          string
}

// NewObservedChain returns an observer for a single named node.
func NewObservedChain(node string) *ObservedChain {
	return &ObservedChain{node: node, notify: newBroadcaster()}
}

// RollForward records a header the node sent, extending the observed
// chain. serverTip is the node's own reported tip from the same message.
func (c *ObservedChain) RollForward(h ObservedHeader, serverTip ChainTip) {
	c.mu.Lock()
	c.headers = append(c.headers, h)
	if len(c.headers) > maxRetainedHeaders {
		// Retain the newest window. Copy into a fresh slice so the
		// dropped headers become collectable instead of being pinned
		// by the backing array.
		trimmed := make([]ObservedHeader, maxRetainedHeaders)
		copy(trimmed, c.headers[len(c.headers)-maxRetainedHeaders:])
		c.headers = trimmed
	}
	c.tip = ChainTip{
		SlotNumber:  h.Slot,
		BlockNumber: h.BlockNumber,
		Hash:        h.Hash,
	}
	c.serverTip = serverTip
	c.rollForwards++
	c.mu.Unlock()
	c.notify.signal()
}

// RollBackward records a rollback to point, dropping every observed
// header above it. A rollback to the origin clears the observed chain.
//
// The recorded depth counts dropped headers still inside the retained
// window, so a rollback deeper than that window under-reports rather
// than over-reports.
func (c *ObservedChain) RollBackward(point ChainPoint, serverTip ChainTip) {
	c.mu.Lock()
	before := len(c.headers)
	if point.IsOrigin() {
		c.headers = nil
		c.tip = ChainTip{}
	} else {
		keep := 0
		for _, h := range c.headers {
			if h.Slot > point.Slot {
				break
			}
			keep++
		}
		c.headers = c.headers[:keep]
		// Recover the block number from the retained chain: the
		// rollback point is normally a header we already saw, but a
		// point below the retained window leaves the nearest header
		// below it as the best available answer.
		var blockNum uint64
		if keep > 0 {
			blockNum = c.headers[keep-1].BlockNumber
		}
		c.tip = ChainTip{
			SlotNumber:  point.Slot,
			BlockNumber: blockNum,
			Hash:        point.Hash,
		}
	}
	// Truncation only ever shrinks the slice, so the difference is
	// non-negative.
	//nolint:gosec // before >= len(c.headers) by construction
	if dropped := uint64(before - len(c.headers)); dropped >
		c.maxRollbackDepth {
		c.maxRollbackDepth = dropped
	}
	c.serverTip = serverTip
	c.rollBackwards++
	c.mu.Unlock()
	c.notify.signal()
}

// Connected records that the observer established (or re-established) a
// ChainSync session with the node.
func (c *ObservedChain) Connected() {
	c.mu.Lock()
	c.connects++
	c.connected = true
	c.lastErr = ""
	c.mu.Unlock()
	c.notify.signal()
}

// Disconnected records that the ChainSync session dropped. The observed
// chain is deliberately preserved so a reconnect resumes from it.
func (c *ObservedChain) Disconnected(err error) {
	c.mu.Lock()
	c.disconnects++
	c.connected = false
	if err != nil {
		c.lastErr = err.Error()
	}
	c.mu.Unlock()
	c.notify.signal()
}

// Snapshot returns a deep-enough copy for assertions to read safely.
func (c *ObservedChain) Snapshot() ChainSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	headers := make([]ObservedHeader, len(c.headers))
	copy(headers, c.headers)
	return ChainSnapshot{
		Node:             c.node,
		Tip:              c.tip,
		ServerTip:        c.serverTip,
		Headers:          headers,
		RollForwards:     c.rollForwards,
		RollBackwards:    c.rollBackwards,
		MaxRollbackDepth: c.maxRollbackDepth,
		Connects:         c.connects,
		Disconnects:      c.disconnects,
		Connected:        c.connected,
		LastError:        c.lastErr,
	}
}

// Await blocks until cond holds for this node's observed chain or ctx
// expires. cond is evaluated once before waiting, then again after every
// observed protocol event, so no polling interval is involved.
func (c *ObservedChain) Await(
	ctx context.Context,
	desc string,
	cond func(ChainSnapshot) bool,
) error {
	for {
		changed := c.notify.wait()
		snap := c.Snapshot()
		if cond(snap) {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf(
				"devnet: %q not satisfied by %s: %w",
				desc, snap, ctx.Err(),
			)
		}
	}
}

// ChainGroup observes several nodes at once and lets a scenario wait on
// conditions that span them. Every member shares one change broadcaster,
// so a group wait costs a single channel receive regardless of how many
// nodes the topology has.
type ChainGroup struct {
	notify *broadcaster
	order  []string
	chains map[string]*ObservedChain
}

// NewChainGroup returns a group observing the named nodes.
func NewChainGroup(nodes ...string) *ChainGroup {
	g := &ChainGroup{
		notify: newBroadcaster(),
		order:  make([]string, 0, len(nodes)),
		chains: make(map[string]*ObservedChain, len(nodes)),
	}
	for _, n := range nodes {
		if _, dup := g.chains[n]; dup {
			continue
		}
		g.chains[n] = &ObservedChain{node: n, notify: g.notify}
		g.order = append(g.order, n)
	}
	return g
}

// Chain returns the observer for a node, or nil if it is not in the
// group.
func (g *ChainGroup) Chain(node string) *ObservedChain {
	return g.chains[node]
}

// Chains returns every observer in the order the group was built.
func (g *ChainGroup) Chains() []*ObservedChain {
	out := make([]*ObservedChain, 0, len(g.order))
	for _, n := range g.order {
		if c := g.chains[n]; c != nil {
			out = append(out, c)
		}
	}
	return out
}

// Snapshots returns one snapshot per node, in group order.
func (g *ChainGroup) Snapshots() []ChainSnapshot {
	out := make([]ChainSnapshot, 0, len(g.order))
	for _, n := range g.order {
		if c := g.chains[n]; c != nil {
			out = append(out, c.Snapshot())
		}
	}
	return out
}

// Await blocks until cond holds across every node's observed chain or
// ctx expires. On timeout the error reports each node's state.
func (g *ChainGroup) Await(
	ctx context.Context,
	desc string,
	cond func([]ChainSnapshot) bool,
) error {
	for {
		changed := g.notify.wait()
		snaps := g.Snapshots()
		if cond(snaps) {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			parts := make([]string, 0, len(snaps))
			for _, s := range snaps {
				parts = append(parts, s.String())
			}
			return fmt.Errorf(
				"devnet: %q not satisfied by [%s]: %w",
				desc, strings.Join(parts, ", "), ctx.Err(),
			)
		}
	}
}

// AgreementResult reports whether a set of nodes agree on the chain at
// the deepest slot they have all observed.
type AgreementResult struct {
	Slot   uint64
	Agree  bool
	Hashes map[string]string
}

// String renders the per-node hashes for failure messages.
func (r AgreementResult) String() string {
	nodes := make([]string, 0, len(r.Hashes))
	for n := range r.Hashes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		parts = append(parts, fmt.Sprintf("%s=%s", n, r.Hashes[n]))
	}
	return fmt.Sprintf(
		"slot %d agree=%t [%s]", r.Slot, r.Agree, strings.Join(parts, " "),
	)
}

// AgreementAtDeepestCommonSlot picks the highest slot every snapshot
// observed a header for and compares the hashes there. Using a slot all
// nodes actually reached makes the check a deterministic expected point
// rather than a tolerance window: nodes at different tips still get
// compared on the chain they share, and a fork shows up as a hash
// mismatch instead of being masked by a slot-distance allowance.
//
// The second return is false when the snapshots share no observed slot,
// which means the comparison could not be made at all.
func AgreementAtDeepestCommonSlot(
	snaps []ChainSnapshot,
) (AgreementResult, bool) {
	if len(snaps) < 2 {
		return AgreementResult{}, false
	}
	byNode := make([]map[uint64]string, len(snaps))
	for i, s := range snaps {
		m := make(map[uint64]string, len(s.Headers))
		for _, h := range s.Headers {
			m[h.Slot] = hex.EncodeToString(h.Hash)
		}
		byNode[i] = m
	}
	var (
		bestSlot  uint64
		haveSlot  bool
		bestFound = false
	)
	for slot := range byNode[0] {
		common := true
		for _, m := range byNode[1:] {
			if _, ok := m[slot]; !ok {
				common = false
				break
			}
		}
		if !common {
			continue
		}
		if !bestFound || slot > bestSlot {
			bestSlot, bestFound, haveSlot = slot, true, true
		}
	}
	if !haveSlot {
		return AgreementResult{}, false
	}
	result := AgreementResult{
		Slot:   bestSlot,
		Agree:  true,
		Hashes: make(map[string]string, len(snaps)),
	}
	var first string
	for i, s := range snaps {
		h := byNode[i][bestSlot]
		result.Hashes[s.Node] = h
		if i == 0 {
			first = h
		} else if h != first {
			result.Agree = false
		}
	}
	return result, true
}

// AgreedHeaderAbove returns the lowest header above minSlot that every
// snapshot observed with an identical hash, and whether one exists.
//
// This is the propagation check: a header only becomes "agreed" once it
// has reached every node in the topology, including the non-forging
// relay, so a block that never diffused is never mistaken for one that
// did. Taking the lowest such header rather than the deepest makes the
// answer the first block produced after the caller's baseline, which is
// what a propagation assertion is actually about.
func AgreedHeaderAbove(
	snaps []ChainSnapshot,
	minSlot uint64,
) (ObservedHeader, bool) {
	if len(snaps) == 0 {
		return ObservedHeader{}, false
	}
	candidates := make([]ObservedHeader, 0, len(snaps[0].Headers))
	for _, h := range snaps[0].Headers {
		if h.Slot > minSlot {
			candidates = append(candidates, h)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Slot < candidates[j].Slot
	})
	for _, cand := range candidates {
		agreed := true
		for _, s := range snaps[1:] {
			other, ok := s.HashAt(cand.Slot)
			if !ok || !bytes.Equal(other, cand.Hash) {
				agreed = false
				break
			}
		}
		if agreed {
			return cand, true
		}
	}
	return ObservedHeader{}, false
}

// MinTipSlot returns the lowest tip slot across snapshots, i.e. how far
// the slowest node has got.
func MinTipSlot(snaps []ChainSnapshot) uint64 {
	if len(snaps) == 0 {
		return 0
	}
	minSlot := snaps[0].Tip.SlotNumber
	for _, s := range snaps[1:] {
		if s.Tip.SlotNumber < minSlot {
			minSlot = s.Tip.SlotNumber
		}
	}
	return minSlot
}

// MaxTipSlot returns the highest tip slot across snapshots.
func MaxTipSlot(snaps []ChainSnapshot) uint64 {
	var maxSlot uint64
	for _, s := range snaps {
		if s.Tip.SlotNumber > maxSlot {
			maxSlot = s.Tip.SlotNumber
		}
	}
	return maxSlot
}

// MaxBlockNumber returns the highest observed block height across
// snapshots. The scenario uses it to require forward progress between
// disruption phases, so that each outage starts from a network that is
// demonstrably still producing rather than merely converged.
func MaxBlockNumber(snaps []ChainSnapshot) uint64 {
	var maxBlock uint64
	for _, s := range snaps {
		if s.Tip.BlockNumber > maxBlock {
			maxBlock = s.Tip.BlockNumber
		}
	}
	return maxBlock
}

// MaxServerTipSlot returns the highest slot any node reported as its own
// tip.
//
// This is not the same as MaxTipSlot: an observer intersects at origin
// and replays history before it reaches the tip, so its observed tip lags
// during catch-up. Assertions about what the chain is doing *now* — a
// baseline for "forged after this point", or the next epoch boundary to
// cross — must use the peer's reported tip, or replayed history could
// satisfy them without the network having done anything.
func MaxServerTipSlot(snaps []ChainSnapshot) uint64 {
	var maxSlot uint64
	for _, s := range snaps {
		if s.ServerTip.SlotNumber > maxSlot {
			maxSlot = s.ServerTip.SlotNumber
		}
	}
	return maxSlot
}
