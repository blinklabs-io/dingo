// Copyright 2025 Blink Labs Software
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

package peergov

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type peerGovernorMetrics struct {
	coldPeers        prometheus.Gauge
	warmPeers        prometheus.Gauge
	hotPeers         prometheus.Gauge
	activePeers      prometheus.Gauge
	establishedPeers prometheus.Gauge
	knownPeers       prometheus.Gauge
	// Churn counters
	coldPeersPromotions  prometheus.Counter
	warmPeersPromotions  prometheus.Counter
	warmPeersDemotions   prometheus.Counter
	increasedKnownPeers  prometheus.Counter
	decreasedKnownPeers  prometheus.Counter
	increasedActivePeers prometheus.Counter
	decreasedActivePeers prometheus.Counter
	// Per-source promotion/demotion metrics (matching Cardano node naming)
	warmNonRootPeersDemotions   prometheus.Counter
	warmNonRootPeersPromotions  prometheus.Counter
	activeNonRootPeersDemotions prometheus.Counter
	// Per-source metrics (Phase 7: Enhanced Observability)
	peersBySource           *prometheus.GaugeVec   // Peers by source and state
	churnDemotionsBySource  *prometheus.CounterVec // Churn demotions by source
	churnPromotionsBySource *prometheus.CounterVec // Churn promotions by source
}

func (p *PeerGovernor) initMetrics() {
	promautoFactory := promauto.With(p.config.PromRegistry)
	p.metrics = &peerGovernorMetrics{}
	p.metrics.coldPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_cold",
		Help: "number of cold peers",
	})
	p.metrics.warmPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_warm",
		Help: "number of warm peers",
	})
	p.metrics.hotPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_hot",
		Help: "number of hot peers",
	})
	p.metrics.activePeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_ActivePeers",
		Help: "number of active peers",
	})
	p.metrics.establishedPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_EstablishedPeers",
		Help: "number of established peers",
	})
	p.metrics.knownPeers = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_KnownPeers",
		Help: "number of known peers",
	})
	// Churn counters
	p.metrics.coldPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_ColdPeersPromotions",
			Help: "number of cold peers promoted to warm",
		},
	)
	p.metrics.warmPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmPeersPromotions",
			Help: "number of warm peers promoted to hot",
		},
	)
	p.metrics.warmPeersDemotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmPeersDemotions",
			Help: "number of hot peers demoted to warm",
		},
	)
	p.metrics.increasedKnownPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_IncreasedKnownPeers",
			Help: "number of peers added to known set",
		},
	)
	p.metrics.decreasedKnownPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_DecreasedKnownPeers",
			Help: "number of peers removed from known set",
		},
	)
	p.metrics.increasedActivePeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_IncreasedActivePeers",
			Help: "number of active peers increased",
		},
	)
	p.metrics.decreasedActivePeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_DecreasedActivePeers",
			Help: "number of active peers decreased",
		},
	)
	// Per-source promotion/demotion metrics (matching Cardano node naming)
	p.metrics.warmNonRootPeersDemotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmNonRootPeersDemotions",
			Help: "number of non-root warm peers demoted",
		},
	)
	p.metrics.warmNonRootPeersPromotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_WarmNonRootPeersPromotions",
			Help: "number of non-root warm peers promoted to hot",
		},
	)
	p.metrics.activeNonRootPeersDemotions = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_ActiveNonRootPeersDemotions",
			Help: "number of non-root active peers demoted",
		},
	)
	// Per-source metrics (Phase 7: Enhanced Observability)
	p.metrics.peersBySource = promautoFactory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_peerSelection_peers_by_source",
			Help: "number of peers by source and state",
		},
		[]string{"source", "state"},
	)
	p.metrics.churnDemotionsBySource = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_demotions_by_source",
			Help: "number of churn demotions by source",
		},
		[]string{"source"},
	)
	p.metrics.churnPromotionsBySource = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_churn_promotions_by_source",
			Help: "number of churn promotions by source",
		},
		[]string{"source"},
	)
}

// updatePeerMetrics updates the Prometheus metrics for peer counts.
// This function assumes p.mu is already held by the caller.
func (p *PeerGovernor) updatePeerMetrics() {
	if p.metrics == nil {
		return
	}
	// NOTE: Caller must hold p.mu

	coldCount := 0
	warmCount := 0
	hotCount := 0
	activeCount := 0
	establishedCount := 0
	knownCount := 0

	// Per-source state tracking: map[source]map[state]count
	type sourceStateKey struct {
		source string
		state  string
	}
	sourceCounts := make(map[sourceStateKey]int)

	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		knownCount++
		sourceLabel := peer.Source.String()
		var stateLabel string

		switch peer.State {
		case PeerStateCold:
			coldCount++
			stateLabel = "cold"
		case PeerStateWarm:
			warmCount++
			stateLabel = "warm"
			// Warm peers have established connections
			if peer.Connection != nil {
				establishedCount++
			}
		case PeerStateHot:
			hotCount++
			stateLabel = "hot"
			// Hot peers have established connections and are active
			if peer.Connection != nil {
				establishedCount++
				activeCount++
			}
		}

		// Track per-source counts
		key := sourceStateKey{source: sourceLabel, state: stateLabel}
		sourceCounts[key]++
	}

	p.metrics.coldPeers.Set(float64(coldCount))
	p.metrics.warmPeers.Set(float64(warmCount))
	p.metrics.hotPeers.Set(float64(hotCount))
	p.metrics.activePeers.Set(float64(activeCount))
	p.metrics.establishedPeers.Set(float64(establishedCount))
	p.metrics.knownPeers.Set(float64(knownCount))

	// Update per-source gauges
	// Reset all known source/state combinations to 0 first
	sources := []string{
		"topology-local-root", "topology-public-root", "topology-bootstrap",
		"ledger", "gossip", "inbound", "unknown",
	}
	states := []string{"cold", "warm", "hot"}
	for _, src := range sources {
		for _, st := range states {
			p.metrics.peersBySource.WithLabelValues(src, st).Set(0)
		}
	}
	// Now set actual counts
	for key, count := range sourceCounts {
		p.metrics.peersBySource.WithLabelValues(key.source, key.state).Set(
			float64(count),
		)
	}
}
