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
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Reasons reported by dingo_chainselection_stalled_total.
const (
	// chainSelectionStallNoSelectablePeer is the ordinary stall: no tracked
	// peer passed the selectability checks.
	chainSelectionStallNoSelectablePeer = "no_selectable_peer"
	// chainSelectionStallGenesisCorroboration is a stall caused by the Genesis
	// corroboration gate denying the densest fast source.
	chainSelectionStallGenesisCorroboration = "genesis_corroboration"
)

// chainSelectionMetrics counts chain-selection transitions that are otherwise
// only visible in the log: how often selection stalled with no selectable peer,
// and how often a peer was (re)registered from a chainsync rollback, which is
// the post-recycle path that keeps a stall from lasting until the next block.
type chainSelectionMetrics struct {
	stalls                *prometheus.CounterVec
	rollbackRegistrations *prometheus.CounterVec
}

// registerChainSelectionMetrics registers the chain-selection counters. It runs
// in New(), against the pre-wrap registerer, because these counters live for
// the node's entire lifetime: the ChainSelector is not rebuilt by a live
// database restore/truncate, so they must not be unregistered by
// rebuildableRegisterer.unregisterAll (see metrics_registerer.go).
//
// Every label value is materialized here so a scrape reports an explicit 0
// instead of a missing series before the first occurrence -- a stall counter
// that only appears once the node has stalled is useless for alerting.
func (n *Node) registerChainSelectionMetrics() {
	if n.config.promRegistry == nil {
		return
	}
	factory := promauto.With(n.config.promRegistry)
	metrics := &chainSelectionMetrics{
		stalls: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_chainselection_stalled_total",
				Help: "times chain selection transitioned to having no selectable peer",
			},
			[]string{"reason"},
		),
		rollbackRegistrations: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_chainselection_rollback_registrations_total",
				Help: "attempts to register a peer into chain selection from a chainsync rollback on an untracked connection, by outcome",
			},
			[]string{"outcome"},
		),
	}
	for _, reason := range []string{
		chainSelectionStallNoSelectablePeer,
		chainSelectionStallGenesisCorroboration,
	} {
		metrics.stalls.WithLabelValues(reason)
	}
	for _, outcome := range []chainselection.RollbackRegistrationOutcome{
		chainselection.RollbackRegistrationRegistered,
		chainselection.RollbackRegistrationClosedConnection,
		chainselection.RollbackRegistrationImplausibleTip,
		chainselection.RollbackRegistrationAtCapacity,
	} {
		metrics.rollbackRegistrations.WithLabelValues(string(outcome))
	}
	n.chainSelectionMetrics = metrics
}

// recordChainSelectionStall counts one selected-to-none transition. Safe to
// call when metrics are disabled.
func (n *Node) recordChainSelectionStall(genesisCorroboration bool) {
	if n.chainSelectionMetrics == nil {
		return
	}
	reason := chainSelectionStallNoSelectablePeer
	if genesisCorroboration {
		reason = chainSelectionStallGenesisCorroboration
	}
	n.chainSelectionMetrics.stalls.WithLabelValues(reason).Inc()
}

// recordRollbackRegistration counts one attempt to register a peer from a
// chainsync rollback. Safe to call when metrics are disabled.
func (n *Node) recordRollbackRegistration(
	outcome chainselection.RollbackRegistrationOutcome,
) {
	if n.chainSelectionMetrics == nil {
		return
	}
	n.chainSelectionMetrics.rollbackRegistrations.
		WithLabelValues(string(outcome)).
		Inc()
}
