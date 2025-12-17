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
const (
	defaultLatencyWeight   = 0.5
	defaultSuccessWeight   = 0.35
	defaultStabilityWeight = 0.15
	// Minimal latency (ms) used to normalize inverse latency. Avoid div-by-zero.
	minLatencyMs = 1.0
	// EMA smoothing factor (alpha) used when updating observed metrics.
	defaultEMAAlpha = 0.2
)

// UpdateBlockFetchObservation updates the peer's block fetch metrics using an
// observed latency (in milliseconds) and success (0 or 1). Uses an exponential
// moving average for smoothing.
func (p *Peer) UpdateBlockFetchObservation(latencyMs float64, success bool) {
	if latencyMs <= 0 {
		latencyMs = minLatencyMs
	}
	// Initialize latency EMA on first observation
	if !p.BlockFetchLatencyInit {
		p.BlockFetchLatencyMs = latencyMs
		p.BlockFetchLatencyInit = true
	} else {
		p.BlockFetchLatencyMs = ema(p.BlockFetchLatencyMs, latencyMs, defaultEMAAlpha)
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
		p.BlockFetchSuccessRate = ema(p.BlockFetchSuccessRate, successF, defaultEMAAlpha)
	}
	p.LastBlockFetchTime = time.Now()
	// Recompute composite score
	p.UpdatePeerScore()
}

// UpdateConnectionStability updates the connection stability observation. The
// value should be in [0..1]. Uses EMA smoothing as well.
func (p *Peer) UpdateConnectionStability(observed float64) {
	observed = clamp01(observed)
	if !p.ConnectionStabilityInit {
		p.ConnectionStability = observed
		p.ConnectionStabilityInit = true
	} else {
		p.ConnectionStability = ema(p.ConnectionStability, observed, defaultEMAAlpha)
	}
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

	// Weighted aggregate (already in 0..1 range)
	score := (latencyScore*defaultLatencyWeight + success*defaultSuccessWeight + stability*defaultStabilityWeight) / (defaultLatencyWeight + defaultSuccessWeight + defaultStabilityWeight)

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

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
