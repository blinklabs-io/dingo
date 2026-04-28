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
	"time"

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
	// Explicit inbound governance budget/usage metrics (phase 1)
	inboundWarmTarget prometheus.Gauge
	inboundHotQuota   prometheus.Gauge
	inboundWarmHeld   prometheus.Gauge
	inboundHotHeld    prometheus.Gauge
	inboundPruned     prometheus.Counter
	// Inbound admission metadata metrics (phase 2)
	inboundArrivalsTotal   prometheus.Counter
	inboundTopologyMatched prometheus.Gauge
	inboundDuplexHeld      prometheus.Gauge
	inboundPrunedByReason  *prometheus.CounterVec
	inboundLifecycle       *prometheus.CounterVec
	inboundHotQuotaUsage   prometheus.Gauge
	inboundWarmOccupancy   prometheus.Gauge
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
	p.metrics.inboundWarmTarget = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_InboundWarmTarget",
		Help: "configured inbound warm peer target",
	})
	p.metrics.inboundHotQuota = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_InboundHotQuota",
		Help: "configured inbound hot peer quota",
	})
	p.metrics.inboundWarmHeld = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_InboundWarmHeld",
		Help: "current number of inbound peers held warm",
	})
	p.metrics.inboundHotHeld = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_InboundHotHeld",
		Help: "current number of inbound peers held hot",
	})
	p.metrics.inboundPruned = promautoFactory.NewCounter(prometheus.CounterOpts{
		Name: "cardano_node_metrics_peerSelection_InboundPruned",
		Help: "total inbound peers pruned from governed sets",
	})
	p.metrics.inboundArrivalsTotal = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_InboundArrivalsTotal",
			Help: "total inbound connection events observed since startup (includes re-arrivals)",
		},
	)
	p.metrics.inboundTopologyMatched = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_peerSelection_InboundTopologyMatched",
			Help: "current number of peers whose inbound arrival was identified as a configured topology peer",
		},
	)
	p.metrics.inboundDuplexHeld = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_peerSelection_InboundDuplexHeld",
			Help: "current number of inbound peers on full-duplex connections",
		},
	)
	p.metrics.inboundPrunedByReason = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_InboundPrunedByReason",
			Help: "total inbound peers pruned by policy reason",
		},
		[]string{"reason"},
	)
	p.metrics.inboundLifecycle = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_peerSelection_InboundLifecycleTotal",
			Help: "total inbound lifecycle transitions by stage",
		},
		[]string{"stage"},
	)
	p.metrics.inboundHotQuotaUsage = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_InboundHotQuotaUsage",
		Help: "fraction of inbound hot quota currently occupied (>=0; may exceed 1 when over quota)",
	})
	p.metrics.inboundWarmOccupancy = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_peerSelection_InboundWarmTargetOccupancy",
		Help: "fraction of inbound warm target currently occupied (>=0; may exceed 1 when over target)",
	})
}

func (p *PeerGovernor) recordInboundLifecycle(stage string) {
	if p.metrics == nil || p.metrics.inboundLifecycle == nil {
		return
	}
	p.metrics.inboundLifecycle.WithLabelValues(stage).Inc()
}

// inboundCounts is the phase-2 census of inbound peers used to populate
// both Prometheus metrics and QuotaStatusEvent. Centralizing this avoids
// drift between the two surfaces.
type inboundCounts struct {
	// Warm and Hot are provisional-window-filtered budget counts: peers
	// younger than InboundProvisionalWindow are excluded.
	Warm int
	Hot  int
	// TopologyMatched is the count of peers with a non-empty
	// InboundTopologyMatch.
	TopologyMatched int
	// Duplex is the count of peers with a currently-live inbound
	// client-capable connection.
	Duplex int
}

// censusInboundCounts computes the phase-2 inbound census. Must be
// called with p.mu held.
func (p *PeerGovernor) censusInboundCounts() inboundCounts {
	var c inboundCounts
	now := time.Now()
	window := p.config.InboundProvisionalWindow
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.InboundTopologyMatch != "" {
			c.TopologyMatched++
		}
		if peer.Connection != nil && peer.Connection.IsClient {
			// "Current inbound duplex" must be derived from live transport
			// direction, not sticky arrival metadata.
			isLiveInbound := peer.Source == PeerSourceInboundConn
			if p.config.ConnManager != nil {
				isLiveInbound = p.config.ConnManager.IsInboundConnection(
					peer.Connection.Id,
				)
			}
			if isLiveInbound {
				c.Duplex++
			}
		}
		if peer.Source != PeerSourceInboundConn {
			// Topology peers that inbound-matched still govern through
			// their configured source, so they are not charged against
			// the inbound budget. They do count toward topology-match
			// observability above. Duplex capability is still included in
			// the shared census above.
			continue
		}
		// Provisional-window filter: a negative window disables the
		// grace, zero is normalized to the default in NewPeerGovernor.
		if window > 0 && !peer.FirstSeen.IsZero() &&
			now.Sub(peer.FirstSeen) < window {
			continue
		}
		switch peer.State {
		case PeerStateCold:
			// Cold inbound peers do not hold a slot in either budget.
		case PeerStateWarm:
			c.Warm++
		case PeerStateHot:
			c.Hot++
		}
	}
	return c
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
	// Explicit inbound budget/usage gauges. Use the provisional-window
	// filter so flash-connects cannot inflate reported usage; phase 3
	// enforcement will read the same filtered view.
	p.metrics.inboundWarmTarget.Set(float64(p.config.InboundWarmTarget))
	p.metrics.inboundHotQuota.Set(float64(p.config.InboundHotQuota))
	census := p.censusInboundCounts()
	p.metrics.inboundWarmHeld.Set(float64(census.Warm))
	p.metrics.inboundHotHeld.Set(float64(census.Hot))
	if p.config.InboundHotQuota > 0 {
		p.metrics.inboundHotQuotaUsage.Set(float64(census.Hot) / float64(p.config.InboundHotQuota))
	} else {
		p.metrics.inboundHotQuotaUsage.Set(0)
	}
	if p.config.InboundWarmTarget > 0 {
		p.metrics.inboundWarmOccupancy.Set(float64(census.Warm) / float64(p.config.InboundWarmTarget))
	} else {
		p.metrics.inboundWarmOccupancy.Set(0)
	}
	p.metrics.inboundTopologyMatched.Set(float64(census.TopologyMatched))
	p.metrics.inboundDuplexHeld.Set(float64(census.Duplex))
}
