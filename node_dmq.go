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

	"github.com/blinklabs-io/dingo/dmq"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// configPopulateDMQNetworkMagic resolves the DMQ network magic from the
// topic and Cardano network when none was configured. Only an enabled DMQ
// stack needs one.
func (n *Node) configPopulateDMQNetworkMagic() error {
	dmqCfg := &n.config.cfg.DMQ
	if !dmqCfg.Enabled || dmqCfg.NetworkMagic != 0 || dmqCfg.NetworkMagicExplicit() {
		return nil
	}
	magic, ok := dmq.TopicNetworkMagic(dmqCfg.Topic, n.config.cfg.Network)
	if !ok {
		return fmt.Errorf(
			"dmq: no network magic for topic %q on network %q: "+
				"set dmq.networkMagic",
			dmqCfg.Topic,
			n.config.cfg.Network,
		)
	}
	dmqCfg.NetworkMagic = magic
	return nil
}

// dmqStakeAuthority answers the DMQ authenticator's chain-state lookups.
// Stake comes from the distribution that elects leaders at the wall-clock
// slot, so a DMQ message is accepted from exactly the pools that may forge
// now. The stake, KES and current-slot checks all read that one slot: the
// ledger tip can lag it, possibly into the previous epoch.
//
// Every failure wraps dmq.ErrChainStateUnavailable, because a node that
// cannot answer has no grounds to call the message invalid.
//
// The DMQ stack keeps serving through a live restore or truncate, so it cannot
// read n.ledgerState, which that operation closes and replaces. The node
// clears the authority's ledger state before closing storage and sets the
// rebuilt one afterwards; clearing waits for any lookup in flight.
type dmqStakeAuthority struct {
	mu          sync.RWMutex
	ledgerState *ledger.LedgerState
}

func (a *dmqStakeAuthority) setLedgerState(ls *ledger.LedgerState) {
	a.mu.Lock()
	a.ledgerState = ls
	a.mu.Unlock()
}

func (a *dmqStakeAuthority) attached() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ledgerState != nil
}

var errDMQLedgerDetached = errors.New("ledger state unavailable")

func dmqUnavailable(err error) error {
	return fmt.Errorf("%w: %w", dmq.ErrChainStateUnavailable, err)
}

// wallClockSlot must be called with a.mu held.
func (a *dmqStakeAuthority) wallClockSlot() (uint64, error) {
	if a.ledgerState == nil {
		return 0, dmqUnavailable(errDMQLedgerDetached)
	}
	slot, ok, err := a.ledgerState.WallClockSlotFromConfirmedHistory()
	if err != nil {
		return 0, dmqUnavailable(err)
	}
	if !ok {
		return 0, dmqUnavailable(
			errors.New("confirmed era history does not cover wall clock"),
		)
	}
	return slot, nil
}

func (a *dmqStakeAuthority) PoolActiveStake(
	pool ocommon.PoolKeyHash,
) (uint64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	slot, err := a.wallClockSlot()
	if err != nil {
		return 0, err
	}
	return a.poolStakeAtLocked(slot, pool)
}

// poolStakeAtLocked is PoolActiveStake at a given slot. a.mu must be held.
func (a *dmqStakeAuthority) poolStakeAtLocked(
	slot uint64,
	pool ocommon.PoolKeyHash,
) (uint64, error) {
	if a.ledgerState == nil {
		return 0, dmqUnavailable(errDMQLedgerDetached)
	}
	stake, err := a.ledgerState.ElectingPoolStake(slot, pool)
	if err != nil {
		return 0, dmqUnavailable(err)
	}
	return stake, nil
}

func (a *dmqStakeAuthority) CurrentKESPeriod() (uint64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	slot, err := a.wallClockSlot()
	if err != nil {
		return 0, err
	}
	slotsPerKESPeriod := a.ledgerState.SlotsPerKESPeriod()
	if slotsPerKESPeriod == 0 {
		return 0, dmqUnavailable(errors.New("slots per KES period is zero"))
	}
	return slot / slotsPerKESPeriod, nil
}

func (a *dmqStakeAuthority) CurrentSlot() (uint64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.wallClockSlot()
}

func (a *dmqStakeAuthority) PoolOpCertIssueNumber(
	pool ocommon.PoolKeyHash,
) (uint64, bool, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.ledgerState == nil {
		return 0, false, dmqUnavailable(errDMQLedgerDetached)
	}
	issue, found, err := a.ledgerState.LatestOpCertSequence(pool)
	if err != nil {
		return 0, false, dmqUnavailable(err)
	}
	return issue, found, nil
}

// startDMQ starts the DMQ stack when it is enabled. Its collectors register
// against the retained registry because the stack is not rebuilt by a live
// restore, which unregisters everything the rebuildable wrapper holds.
func (n *Node) startDMQ() error {
	cfg := n.config.cfg.DMQ
	if !cfg.Enabled {
		return nil
	}
	// Read the topology before the DMQ stack opens its socket, so that a bad
	// file stops startup instead of surfacing when peers are first dialed.
	// Services that start earlier in Run are stopped by the usual startup
	// failure path. Configuration validation has already rejected a missing
	// file; this read is the one that parses it and is authoritative.
	if cfg.TopologyFile != "" {
		topo, err := dmq.LoadTopology(cfg.TopologyFile, n.config.logger)
		if err != nil {
			return fmt.Errorf("loading dmq topology: %w", err)
		}
		peers := len(topo.Peers())
		n.config.logger.Info(
			"loaded dmq topology",
			"component", "dmq",
			"file", cfg.TopologyFile,
			"peers", peers,
		)
		if peers == 0 {
			n.config.logger.Warn(
				"dmq topology names no peers",
				"component", "dmq",
				"file", cfg.TopologyFile,
			)
		}
	}
	n.dmqStake.setLedgerState(n.ledgerState)
	authenticator, err := ocommon.NewMessageAuthenticator(
		ocommon.MessageAuthenticatorConfig{
			StakeAuthority:        &n.dmqStake,
			CurrentSlot:           n.dmqStake.CurrentSlot,
			PoolOpCertIssueNumber: n.dmqStake.PoolOpCertIssueNumber,
			SlotsPerKESPeriod:     n.ledgerState.SlotsPerKESPeriod(),
			MaxKESEvolutions:      n.config.MaxKESEvolutions(),
			Logger:                n.config.logger,
		},
	)
	if err != nil {
		return fmt.Errorf("creating dmq authenticator: %w", err)
	}
	stack, err := dmq.NewStack(dmq.StackConfig{
		Logger:       n.config.logger,
		PromRegistry: n.retainedComponentPromRegistry(),
		NetworkMagic: cfg.NetworkMagic,
		SocketPath:   cfg.SocketPath,
		// #nosec G115 -- validation bounds both to fit int64 once scaled
		MessageTTL: time.Duration(cfg.MessageTTL) * time.Second,
		// #nosec G115 -- validation bounds both to fit int64 once scaled
		MaxMempoolBytes:  int64(cfg.MaxMempoolSize) << 20,
		Authenticator:    authenticator,
		CurrentKESPeriod: n.dmqStake.CurrentKESPeriod,
		MaxKESEvolutions: n.config.MaxKESEvolutions(),
	})
	if err != nil {
		return fmt.Errorf("creating dmq stack: %w", err)
	}
	if err := stack.Start(n.ctx); err != nil { //nolint:contextcheck
		return fmt.Errorf("starting dmq stack: %w", err)
	}
	n.dmqStack = stack
	return nil
}
