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
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/internal/promutil"
	"github.com/prometheus/client_golang/prometheus"
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
	gddDisconnects        prometheus.Counter
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
func (n *Node) registerChainSelectionMetrics(r *promutil.Registration) {
	if n.config.promRegistry == nil {
		return
	}
	metrics := &chainSelectionMetrics{
		stalls: promutil.Register(r, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_chainselection_stalled_total",
				Help: "times chain selection transitioned to having no selectable peer",
			},
			[]string{"reason"},
		)),
		rollbackRegistrations: promutil.Register(r, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_chainselection_rollback_registrations_total",
				Help: "attempts to register a peer into chain selection from a chainsync rollback on an untracked connection, by outcome",
			},
			[]string{"outcome"},
		)),
		gddDisconnects: promutil.Register(r, prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "dingo_chainselection_gdd_disconnects_total",
				Help: "peers the Genesis Density Disconnector reported for serving a provably sparser chain, counted whether or not the connection was still open to close",
			},
		)),
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
	// Both read 0 while the cap is inactive (caught up) so a scrape never
	// reports a stale limit.
	promutil.Register(r, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_chainselection_loe_block_number",
			Help: "highest block number chain selection may reach under the Limit on Eagerness, 0 when the cap is inactive",
		},
		func() float64 { return n.eagernessLimitGauge(false) },
	))
	promutil.Register(r, prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_chainselection_loe_intersection_slot",
			Help: "slot of the point the Limit on Eagerness is measured from: the point common to all candidate fragments, or the last one they shared once they stop overlapping; 0 when inactive or measured from the local tip",
		},
		func() float64 { return n.eagernessLimitGauge(true) },
	))
}

// eagernessLimitGauge reads the current Limit on Eagerness at scrape time:
// the intersection slot when slot is true, the block-number limit otherwise.
// It reads chainSelectorForGauges rather than chainSelector because the gauges
// are registered in New, before Run creates the selector, and a scrape may
// race that assignment.
func (n *Node) eagernessLimitGauge(slot bool) float64 {
	selector := n.chainSelectorForGauges.Load()
	if selector == nil {
		return 0
	}
	limit := selector.EagernessLimit()
	if !limit.Active {
		return 0
	}
	if slot {
		return float64(limit.Point.Slot)
	}
	return float64(limit.BlockNumber)
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

// genesisDensityDenyDuration bounds how long a peer disconnected for serving a
// provably sparser chain stays on the deny list. Density is measured against
// the current candidates, so the denial is temporary rather than permanent.
const genesisDensityDenyDuration = 10 * time.Minute

// onGenesisDensityDisconnect acts on a peer the Genesis Density Disconnector
// found provably sparser: it counts the report, denies the peer for
// genesisDensityDenyDuration when its connection ID carries a remote address,
// and closes its connection if it is still open.
func (n *Node) onGenesisDensityDisconnect(
	d chainselection.GenesisDensityDisconnect,
) {
	if n.chainSelectionMetrics != nil {
		n.chainSelectionMetrics.gddDisconnects.Inc()
	}
	n.networkingCoreMu.Lock()
	defer n.networkingCoreMu.Unlock()
	// A connection ID without a remote address cannot be denied, so the log
	// reports whether the deny happened rather than implying it.
	denied := false
	if d.ConnectionId.RemoteAddr != nil {
		denied = n.denyPeer(
			d.ConnectionId.RemoteAddr.String(),
			genesisDensityDenyDuration,
		)
	}
	msg := "disconnecting peer serving a provably sparser chain"
	if d.EagernessStandoff {
		msg = "disconnecting peer that lost a standoff at the limit on eagerness"
	}
	n.config.logger.Warn(
		msg,
		"connection_id", d.ConnectionId.String(),
		"dominating_connection_id", d.DominatingConnectionId.String(),
		"intersection_slot", d.Intersection.Slot,
		"genesis_window_slots", d.WindowSlots,
		"dominating_density", d.DominatingDensity,
		"max_density", d.MaxDensity,
		"denied", denied,
		"deny_duration", genesisDensityDenyDuration,
	)
	if n.connManager == nil {
		return
	}
	if conn := n.connManager.GetConnectionById(d.ConnectionId); conn != nil {
		conn.Close()
	}
}

// denyPeer applies a denial to the current peer governor. Live database
// lifecycle operations replace the governor while retaining the selector, so
// the pointer must remain stable until the denial has been recorded.
func (n *Node) denyPeer(address string, duration time.Duration) bool {
	n.peerGovMu.RLock()
	defer n.peerGovMu.RUnlock()
	if n.peerGov == nil {
		return false
	}
	n.peerGov.DenyPeer(address, duration)
	return true
}
