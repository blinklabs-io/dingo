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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
)

// Deps is the complete set of components Ouroboros needs before it can serve
// or initiate any mini-protocol.
//
// These cannot be constructor arguments because the wiring graph is cyclic:
// ledger.NewLedgerState takes EndorserBlockTxsByHash, FetchEndorserBlockByPoint
// and BlockfetchClientRequestRange from an existing *Ouroboros;
// connmanager.NewConnectionManager takes ConfigureListeners and
// OutboundConnOpts; and peergov.NewPeerGovernor takes RequestPeersFromPeer.
// Ouroboros must therefore exist before any of them, and Wire is the explicit
// setup-time gate that closes the cycle with validation.
//
// Wire is safe to call before the dependencies are used but must complete
// before listeners open or outbound connections start.
type Deps struct {
	LedgerState    *ledger.LedgerState
	Mempool        mempool.Service
	ChainsyncState *chainsync.State
	ConnManager    *connmanager.ConnectionManager
	PeerGov        *peergov.PeerGovernor
}

// ErrNotWired reports that a dependency was needed before Wire supplied it.
var ErrNotWired = errors.New("ouroboros: dependencies not wired")

// Wire installs the node's dependencies and validates that every required one
// is present, returning an error naming the first missing field. It replaces
// the previous pattern of assigning exported fields one at a time from the
// node package, which let an Ouroboros exist in a partially-wired state whose
// only symptom was a nil dereference at first protocol use, and which had to
// be kept in sync across both the Run() startup path and the live
// snapshot/restore rebuild path independently.
//
// Wire is idempotent in the sense that it may be called again with a fresh set
// of dependencies. The live restore path in node_lifecycle.go relies on that:
// it stops and discards the ledger state, mempool, chainsync state, connection
// manager and peer governor, rebuilds them against the restored database, then
// rewires this same long-lived Ouroboros instance with the replacements.
//
// Wire is not safe against concurrent protocol activity. Callers must hold the
// node quiesced across a rewire, exactly as the live restore path does; the
// dependencies are read unsynchronized on the protocol hot paths, so a rewire
// racing live traffic would be a data race. This is the same constraint the
// previous exported-field assignments carried, made explicit.
func (o *Ouroboros) Wire(d Deps) error {
	// The EventBus arrives through OuroborosConfig rather than Deps, but it is
	// no less required. NewOuroboros has no error return, so this is the only
	// place it can be reported.
	if o.eventBus == nil {
		return fmt.Errorf("%w: EventBus", ErrNotWired)
	}
	for _, missing := range []struct {
		name  string
		isNil bool
	}{
		{"LedgerState", d.LedgerState == nil},
		{"Mempool", d.Mempool == nil},
		{"ChainsyncState", d.ChainsyncState == nil},
		{"ConnManager", d.ConnManager == nil},
		{"PeerGov", d.PeerGov == nil},
	} {
		if missing.isNil {
			return fmt.Errorf("%w: %s", ErrNotWired, missing.name)
		}
	}
	o.ledgerState = d.LedgerState
	o.mempool = d.Mempool
	o.chainsyncState = d.ChainsyncState
	o.connManager = d.ConnManager
	o.peerGov = d.PeerGov
	return nil
}

// Wired reports whether every required dependency is present. Handlers that
// the node subscribes to the EventBus before Wire runs use this to drop an
// early event with a diagnostic instead of dereferencing a nil dependency.
func (o *Ouroboros) Wired() bool {
	return o.eventBus != nil &&
		o.ledgerState != nil &&
		o.mempool != nil &&
		o.chainsyncState != nil &&
		o.connManager != nil &&
		o.peerGov != nil
}

// SetLeiosVotes wires the Leios vote handler. It is separate from Wire because
// the Leios prototype is optional and its manager is built and started on its
// own path (node_leios.go), which also reruns across live restore cycles. A
// nil handler is rejected: disabling Leios is a config decision, so a nil here
// is a wiring bug that would otherwise silently stop vote diffusion.
func (o *Ouroboros) SetLeiosVotes(h LeiosVoteHandler) error {
	if h == nil {
		return fmt.Errorf("%w: LeiosVotes", ErrNotWired)
	}
	o.leiosVotes = h
	return nil
}

// SetLeiosPipeline wires the Leios pipeline handler. See SetLeiosVotes for why
// this is not part of Wire and why nil is rejected.
func (o *Ouroboros) SetLeiosPipeline(h LeiosPipelineHandler) error {
	if h == nil {
		return fmt.Errorf("%w: LeiosPipeline", ErrNotWired)
	}
	o.leiosPipeline = h
	return nil
}

// LedgerState returns the wired ledger state, or nil before Wire runs.
func (o *Ouroboros) LedgerState() *ledger.LedgerState { return o.ledgerState }

// Mempool returns the wired mempool, or nil before Wire runs.
func (o *Ouroboros) Mempool() mempool.Service { return o.mempool }

// ChainsyncState returns the wired chainsync state, or nil before Wire runs.
func (o *Ouroboros) ChainsyncState() *chainsync.State { return o.chainsyncState }

// ConnManager returns the wired connection manager, or nil before Wire runs.
func (o *Ouroboros) ConnManager() *connmanager.ConnectionManager {
	return o.connManager
}

// PeerGov returns the wired peer governor, or nil before Wire runs.
func (o *Ouroboros) PeerGov() *peergov.PeerGovernor { return o.peerGov }

// EventBus returns the event bus supplied through OuroborosConfig.
func (o *Ouroboros) EventBus() *event.EventBus { return o.eventBus }

// LeiosVotes returns the Leios vote handler, or nil when Leios is disabled.
func (o *Ouroboros) LeiosVotes() LeiosVoteHandler { return o.leiosVotes }

// LeiosPipeline returns the Leios pipeline handler, or nil when Leios is
// disabled.
func (o *Ouroboros) LeiosPipeline() LeiosPipelineHandler { return o.leiosPipeline }
