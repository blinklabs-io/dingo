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

// ErrMissingDependency reports that a required dependency was absent at
// construction.
var ErrMissingDependency = errors.New("ouroboros: missing dependency")

// validateDependencies rejects a config that cannot produce a usable
// Ouroboros, naming the first missing field. This is what makes a
// partially-wired instance unrepresentable in production: the constructor is
// the only way to obtain one, and it refuses to build without the full set.
func (cfg OuroborosConfig) validateDependencies() error {
	for _, dep := range []struct {
		name  string
		isNil bool
	}{
		{"EventBus", cfg.EventBus == nil},
		{"LedgerState", cfg.LedgerState == nil},
		{"Mempool", cfg.Mempool == nil},
		{"ChainsyncState", cfg.ChainsyncState == nil},
		{"ConnManager", cfg.ConnManager == nil},
		{"PeerGov", cfg.PeerGov == nil},
	} {
		if dep.isNil {
			return fmt.Errorf("%w: %s", ErrMissingDependency, dep.name)
		}
	}
	return nil
}

// hasDependencies reports whether every required dependency is present.
//
// NewOuroboros guarantees this for production instances, so the protocol
// handlers that consult it are guarding against in-package test instances
// built through newOuroboros, not against a startup ordering hazard.
func (o *Ouroboros) hasDependencies() bool {
	return o.eventBus != nil &&
		o.ledgerState != nil &&
		o.mempool != nil &&
		o.chainsyncState != nil &&
		o.connManager != nil &&
		o.peerGov != nil
}

// SetLeiosVotes wires the Leios vote handler. It is not a constructor argument
// because the Leios prototype is optional and its manager is built and started
// on its own path (node_leios.go) after Ouroboros exists, and is rebuilt
// independently across live restore cycles. A nil handler is rejected:
// disabling Leios is a config decision, so a nil here is a wiring bug that
// would otherwise silently stop vote diffusion.
func (o *Ouroboros) SetLeiosVotes(h LeiosVoteHandler) error {
	if h == nil {
		return fmt.Errorf("%w: LeiosVotes", ErrMissingDependency)
	}
	o.leiosVotes = h
	return nil
}

// SetLeiosPipeline wires the Leios pipeline handler. See SetLeiosVotes for why
// this is not a constructor argument and why nil is rejected.
func (o *Ouroboros) SetLeiosPipeline(h LeiosPipelineHandler) error {
	if h == nil {
		return fmt.Errorf("%w: LeiosPipeline", ErrMissingDependency)
	}
	o.leiosPipeline = h
	return nil
}

// LedgerState returns the ledger state. NewOuroboros guarantees it is non-nil.
func (o *Ouroboros) LedgerState() *ledger.LedgerState { return o.ledgerState }

// Mempool returns the mempool. NewOuroboros guarantees it is non-nil.
func (o *Ouroboros) Mempool() mempool.Service { return o.mempool }

// ChainsyncState returns the chainsync state. NewOuroboros guarantees it is non-nil.
func (o *Ouroboros) ChainsyncState() *chainsync.State { return o.chainsyncState }

// ConnManager returns the connection manager. NewOuroboros guarantees it is non-nil.
func (o *Ouroboros) ConnManager() *connmanager.ConnectionManager {
	return o.connManager
}

// PeerGov returns the peer governor. NewOuroboros guarantees it is non-nil.
func (o *Ouroboros) PeerGov() *peergov.PeerGovernor { return o.peerGov }

// EventBus returns the event bus supplied through OuroborosConfig.
func (o *Ouroboros) EventBus() *event.EventBus { return o.eventBus }

// LeiosVotes returns the Leios vote handler, or nil when Leios is disabled.
func (o *Ouroboros) LeiosVotes() LeiosVoteHandler { return o.leiosVotes }

// LeiosPipeline returns the Leios pipeline handler, or nil when Leios is
// disabled.
func (o *Ouroboros) LeiosPipeline() LeiosPipelineHandler { return o.leiosPipeline }
