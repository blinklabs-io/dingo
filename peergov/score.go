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
	"math"
	"time"
)

// Scoring weights and parameters. These are conservative defaults and
// intended to be tuned via configuration in follow-up work.
//
// Weight distribution:
// - BlockFetch latency: 35% - Primary signal for download performance
// - BlockFetch success rate: 25% - Reliability of block fetches
// - Connection stability: 10% - Connection durability
// - Header arrival rate: 15% - ChainSync throughput
// - Tip slot delta: 15% - How current the peer's chain is
const (
	defaultLatencyWeight    = 0.35
	defaultSuccessWeight    = 0.25
	defaultStabilityWeight  = 0.10
	defaultHeaderRateWeight = 0.15
	defaultTipDeltaWeight   = 0.15

	// Minimal latency (ms) used to normalize inverse latency. Avoid div-by-zero.
	minLatencyMs = 1.0
	// EMA smoothing factor (alpha) used when updating observed metrics.
	defaultEMAAlpha = 0.2
	// Reference header rate (headers/sec) for normalization. A peer matching this
	// rate gets a score of ~0.5, higher rates approach 1.0.
	// Note: Cardano blocks arrive every ~20 seconds normally, so steady-state rate
	// is ~0.05/sec. During initial sync, rates can be much higher. We use 1.0 as
	// a reference point that distinguishes active sync from steady-state.
	referenceHeaderRate = 1.0
	// Maximum tip slot delta (slots) before score reaches minimum. Peers this far
	// behind get a very low score for the tip delta component.
	maxTipSlotDelta = 1000
)

// UpdateBlockFetchObservation updates the peer's block fetch metrics using an
// observed latency (in milliseconds) and success (0 or 1). Uses an exponential
// moving average for smoothing.
func (p *Peer) UpdateBlockFetchObservation(latencyMs float64, success bool) {
	if latencyMs <= 0 {
		latencyMs = minLatencyMs
	}
	alpha := p.getEMAAlpha()
	// Initialize latency EMA on first observation
	if !p.BlockFetchLatencyInit {
		p.BlockFetchLatencyMs = latencyMs
		p.BlockFetchLatencyInit = true
	} else {
		p.BlockFetchLatencyMs = ema(p.BlockFetchLatencyMs, latencyMs, alpha)
	}
	var successF float64
	if success {
		successF = 1.0
	}
	// Initialize success rate EMA on first observation. We must treat a 0
	// observed success (all failures) as a valid value, so track initialization
	// separately.
	if !p.BlockFetchSuccessInit {
		p.BlockFetchSuccessRate = successF
		p.BlockFetchSuccessInit = true
	} else {
		p.BlockFetchSuccessRate = ema(p.BlockFetchSuccessRate, successF, alpha)
	}
	p.LastBlockFetchTime = time.Now()
	// Recompute composite score
	p.UpdatePeerScore()
}

// UpdateConnectionStability updates the connection stability observation. The
// value should be in [0..1]. Uses EMA smoothing as well.
func (p *Peer) UpdateConnectionStability(observed float64) {
	observed = clamp01(observed)
	alpha := p.getEMAAlpha()
	if !p.ConnectionStabilityInit {
		p.ConnectionStability = observed
		p.ConnectionStabilityInit = true
	} else {
		p.ConnectionStability = ema(p.ConnectionStability, observed, alpha)
	}
	p.UpdatePeerScore()
}

// UpdateChainSyncObservation updates the peer's ChainSync performance metrics.
// headerRate is the observed headers per second during the measurement period.
// TipSlotDelta = ourTip - peerTip (negative = peer ahead, positive = we are ahead).
// Uses EMA smoothing for header rate.
func (p *Peer) UpdateChainSyncObservation(headerRate float64, tipDelta int64) {
	// Clamp header rate to reasonable bounds
	if headerRate < 0 {
		headerRate = 0
	}
	alpha := p.getEMAAlpha()
	// Initialize or update header rate with EMA
	if !p.HeaderArrivalRateInit {
		p.HeaderArrivalRate = headerRate
		p.HeaderArrivalRateInit = true
	} else {
		p.HeaderArrivalRate = ema(p.HeaderArrivalRate, headerRate, alpha)
	}
	// Tip delta is stored directly (not smoothed) as it represents current state
	p.TipSlotDelta = tipDelta
	p.TipSlotDeltaInit = true
	p.ChainSyncLastUpdate = time.Now()
	p.UpdatePeerScore()
}

// UpdatePeerScore computes the composite PerformanceScore from the available
// metrics. Missing metrics will be treated conservatively.
func (p *Peer) UpdatePeerScore() {
	// Normalize metrics
	latencyMs := p.BlockFetchLatencyMs
	if !p.BlockFetchLatencyInit || latencyMs <= 0 {
		latencyMs = 1000.0 // unknown => penalize with high latency
	}

	// Map latency in ms to a bounded 0..1 latency score where lower latency
	// yields a higher score. The chosen mapping gives reasonable spacing for
	// latencies in the 10ms..2000ms range without requiring external scaling.
	latencyScore := 1.0 / (1.0 + latencyMs/200.0)

	success := p.BlockFetchSuccessRate
	stability := p.ConnectionStability

	// If a metric has not been initialized, apply conservative defaults.
	if !p.BlockFetchSuccessInit {
		success = 0.5
	}
	if !p.ConnectionStabilityInit {
		stability = 0.5
	}

	// Calculate ChainSync metrics scores
	var headerRateScore float64
	if p.HeaderArrivalRateInit {
		// Map header rate to 0..1 score. Higher rate = higher score.
		// Using a similar sigmoid-like mapping as latency but inverted.
		// A rate of referenceHeaderRate gives ~0.5, higher rates approach 1.0.
		headerRateScore = p.HeaderArrivalRate / (p.HeaderArrivalRate + referenceHeaderRate)
	} else {
		headerRateScore = 0.5 // conservative default
	}

	var tipDeltaScore float64
	if p.TipSlotDeltaInit {
		// Map tip delta to 0..1 score.
		// tipDelta = ourTip - peerTip
		// - Negative or zero: peer is at or ahead of us (good for syncing) → high score
		// - Positive: peer is behind us (can't help us sync) → lower score
		if p.TipSlotDelta <= 0 {
			// Peer is at or ahead of our tip - perfect score
			tipDeltaScore = 1.0
		} else {
			// Peer is behind us. The further behind, the lower the score.
			// Uses sigmoid-like mapping: at maxTipSlotDelta slots behind,
			// score is 0.5; continues to decrease asymptotically toward 0.
			delta := float64(p.TipSlotDelta)
			tipDeltaScore = 1.0 - (delta / (delta + float64(maxTipSlotDelta)))
		}
	} else {
		tipDeltaScore = 0.5 // conservative default
	}

	// Total weight for normalization
	totalWeight := defaultLatencyWeight + defaultSuccessWeight + defaultStabilityWeight +
		defaultHeaderRateWeight + defaultTipDeltaWeight

	// Weighted aggregate (already in 0..1 range)
	score := (latencyScore*defaultLatencyWeight +
		success*defaultSuccessWeight +
		stability*defaultStabilityWeight +
		headerRateScore*defaultHeaderRateWeight +
		tipDeltaScore*defaultTipDeltaWeight) / totalWeight

	// Ensure within bounds
	score = clamp01(score)

	// Keep the score stable (if NaN etc)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		score = 0.0
	}
	p.PerformanceScore = score
}

func ema(prev, observed, alpha float64) float64 {
	return prev*(1.0-alpha) + observed*alpha
}

// getEMAAlpha returns the configured EMA alpha or the default if not set.
// The returned value is clamped to [0,1] to ensure valid EMA behavior.
func (p *Peer) getEMAAlpha() float64 {
	if p.EMAAlpha > 0 {
		if p.EMAAlpha > 1 {
			return 1.0
		}
		return p.EMAAlpha
	}
	return defaultEMAAlpha
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
