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
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestUpdateBlockFetchObservationAndScore(t *testing.T) {
	p := &Peer{}

	// First observation: 200ms, success=true
	p.UpdateBlockFetchObservation(200, true)
	if p.BlockFetchLatencyMs == 0 {
		t.Fatalf("expected latency to be initialized")
	}
	if p.BlockFetchSuccessRate <= 0 {
		t.Fatalf("expected success rate to be initialized")
	}
	if p.PerformanceScore <= 0 {
		t.Fatalf(
			"expected performance score to be >0, got %v",
			p.PerformanceScore,
		)
	}

	prevScore := p.PerformanceScore

	// A worse latency should reduce the score
	p.UpdateBlockFetchObservation(1000, true)
	if p.BlockFetchLatencyMs <= 200 {
		t.Fatalf(
			"expected latency EMA to increase, got %v",
			p.BlockFetchLatencyMs,
		)
	}
	if p.PerformanceScore <= 0 || p.PerformanceScore >= prevScore {
		t.Fatalf(
			"expected performance score to decrease after worse latency; prev=%v now=%v",
			prevScore,
			p.PerformanceScore,
		)
	}
}

func TestConnectionStabilityAndScore(t *testing.T) {
	p := &Peer{}
	p.UpdateConnectionStability(0.9)
	if p.ConnectionStability < 0.8 {
		t.Fatalf(
			"expected stability to be set high, got %v",
			p.ConnectionStability,
		)
	}
	if p.PerformanceScore == 0 {
		t.Fatalf("expected score to be non-zero after stability update")
	}

	// Low stability should reduce the score
	prev := p.PerformanceScore
	p.UpdateConnectionStability(0.1)
	if p.PerformanceScore >= prev {
		t.Fatalf(
			"expected score to decrease when stability drops from 0.9 to 0.1; prev=%v now=%v",
			prev,
			p.PerformanceScore,
		)
	}
}

func TestLastBlockFetchTimeUpdated(t *testing.T) {
	p := &Peer{}
	before := time.Now()
	p.UpdateBlockFetchObservation(150, true)
	if p.LastBlockFetchTime.Before(before) {
		t.Fatalf("expected LastBlockFetchTime to be updated")
	}
}

func TestUpdateChainSyncObservationAndScore(t *testing.T) {
	p := &Peer{}

	// First observation: 5 headers/sec, peer at same tip (delta=0)
	p.UpdateChainSyncObservation(5.0, 0)

	if !p.HeaderArrivalRateInit {
		t.Fatalf("expected HeaderArrivalRateInit to be true")
	}
	if p.HeaderArrivalRate != 5.0 {
		t.Fatalf(
			"expected HeaderArrivalRate to be 5.0, got %v",
			p.HeaderArrivalRate,
		)
	}
	if !p.TipSlotDeltaInit {
		t.Fatalf("expected TipSlotDeltaInit to be true")
	}
	if p.TipSlotDelta != 0 {
		t.Fatalf("expected TipSlotDelta to be 0, got %v", p.TipSlotDelta)
	}
	if p.PerformanceScore <= 0 {
		t.Fatalf(
			"expected performance score to be >0, got %v",
			p.PerformanceScore,
		)
	}
	if p.ChainSyncLastUpdate.IsZero() {
		t.Fatalf("expected ChainSyncLastUpdate to be set")
	}

	prevScore := p.PerformanceScore

	// Higher header rate should improve the score
	p.UpdateChainSyncObservation(20.0, 0)
	if p.PerformanceScore <= prevScore {
		t.Fatalf(
			"expected performance score to increase with higher header rate; prev=%v now=%v",
			prevScore,
			p.PerformanceScore,
		)
	}
}

func TestChainSyncTipDeltaScoring(t *testing.T) {
	// Test that peers behind us get lower scores
	p1 := &Peer{}
	p2 := &Peer{}

	// p1: peer at our tip (delta=0)
	p1.UpdateChainSyncObservation(5.0, 0)

	// p2: peer 500 slots behind us (delta=500, meaning we're ahead)
	p2.UpdateChainSyncObservation(5.0, 500)

	if p2.PerformanceScore >= p1.PerformanceScore {
		t.Fatalf(
			"expected peer behind us to have lower score; at-tip=%v behind=%v",
			p1.PerformanceScore,
			p2.PerformanceScore,
		)
	}
}

func TestChainSyncPeerAheadScoring(t *testing.T) {
	// Test that peers ahead of us get good scores (we're syncing from them)
	p1 := &Peer{}
	p2 := &Peer{}

	// p1: peer at our tip (delta=0)
	p1.UpdateChainSyncObservation(5.0, 0)

	// p2: peer 100 slots ahead of us (delta=-100)
	p2.UpdateChainSyncObservation(5.0, -100)

	// Peer ahead should have same or better tip delta score (they're not behind)
	if p2.PerformanceScore < p1.PerformanceScore {
		t.Fatalf(
			"expected peer ahead of us to have >= score; at-tip=%v ahead=%v",
			p1.PerformanceScore,
			p2.PerformanceScore,
		)
	}
}

func TestChainSyncHeaderRateEMA(t *testing.T) {
	const eps = 1e-9 // Epsilon for float comparison
	p := &Peer{}

	// First observation
	p.UpdateChainSyncObservation(10.0, 0)
	if abs(p.HeaderArrivalRate-10.0) > eps {
		t.Fatalf(
			"expected initial rate to be 10.0, got %v",
			p.HeaderArrivalRate,
		)
	}

	// Second observation with higher rate - should use EMA
	p.UpdateChainSyncObservation(20.0, 0)
	// EMA with alpha=0.2: 10.0*0.8 + 20.0*0.2 = 12.0
	expected := 12.0
	if abs(p.HeaderArrivalRate-expected) > eps {
		t.Fatalf(
			"expected EMA to give %v, got %v",
			expected,
			p.HeaderArrivalRate,
		)
	}
}

// abs returns the absolute value of x.
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestChainSyncNegativeHeaderRateClamped(t *testing.T) {
	p := &Peer{}

	// Negative rate should be clamped to 0
	p.UpdateChainSyncObservation(-5.0, 0)
	if p.HeaderArrivalRate != 0 {
		t.Fatalf(
			"expected negative rate to be clamped to 0, got %v",
			p.HeaderArrivalRate,
		)
	}
}

func TestCombinedScoreWithAllMetrics(t *testing.T) {
	p := &Peer{}

	// Set all metrics to good values
	p.UpdateBlockFetchObservation(100, true) // Low latency, success
	p.UpdateConnectionStability(1.0)         // Perfect stability
	p.UpdateChainSyncObservation(10.0, 0)    // Good rate, at tip

	// Score should be high (close to 1.0)
	if p.PerformanceScore < 0.7 {
		t.Fatalf(
			"expected high score with all good metrics, got %v",
			p.PerformanceScore,
		)
	}

	// Now set all metrics to poor values
	p2 := &Peer{}
	p2.UpdateBlockFetchObservation(2000, false) // High latency, failure
	p2.UpdateConnectionStability(0.0)           // No stability
	p2.UpdateChainSyncObservation(0.1, 1000)    // Low rate, very behind

	// Score should be low
	if p2.PerformanceScore > 0.3 {
		t.Fatalf(
			"expected low score with all poor metrics, got %v",
			p2.PerformanceScore,
		)
	}

	// Good peer should have higher score than poor peer
	if p.PerformanceScore <= p2.PerformanceScore {
		t.Fatalf(
			"expected good peer to have higher score; good=%v poor=%v",
			p.PerformanceScore,
			p2.PerformanceScore,
		)
	}
}

func TestConfigurableEMAAlpha(t *testing.T) {
	const eps = 1e-9 // Epsilon for float comparison

	// Test with default alpha (0.2)
	p1 := &Peer{}
	p1.UpdateChainSyncObservation(10.0, 0)
	p1.UpdateChainSyncObservation(20.0, 0)
	// Default EMA: 10.0*0.8 + 20.0*0.2 = 12.0
	if abs(p1.HeaderArrivalRate-12.0) > eps {
		t.Fatalf(
			"expected default EMA (alpha=0.2) to give 12.0, got %v",
			p1.HeaderArrivalRate,
		)
	}

	// Test with custom alpha (0.5)
	p2 := &Peer{EMAAlpha: 0.5}
	p2.UpdateChainSyncObservation(10.0, 0)
	p2.UpdateChainSyncObservation(20.0, 0)
	// Custom EMA: 10.0*0.5 + 20.0*0.5 = 15.0
	if abs(p2.HeaderArrivalRate-15.0) > eps {
		t.Fatalf(
			"expected custom EMA (alpha=0.5) to give 15.0, got %v",
			p2.HeaderArrivalRate,
		)
	}

	// Test with high alpha (0.9) - very responsive
	p3 := &Peer{EMAAlpha: 0.9}
	p3.UpdateChainSyncObservation(10.0, 0)
	p3.UpdateChainSyncObservation(20.0, 0)
	// High alpha EMA: 10.0*0.1 + 20.0*0.9 = 19.0
	if abs(p3.HeaderArrivalRate-19.0) > eps {
		t.Fatalf(
			"expected high alpha EMA (alpha=0.9) to give 19.0, got %v",
			p3.HeaderArrivalRate,
		)
	}
}

func TestScoreMetricsDecayAfterInactivity(t *testing.T) {
	peer := &Peer{
		BlockFetchLatencyMs:     minLatencyMs,
		BlockFetchSuccessRate:   1,
		ConnectionStability:     1,
		HeaderArrivalRate:       10,
		BlockFetchLatencyInit:   true,
		BlockFetchSuccessInit:   true,
		ConnectionStabilityInit: true,
		HeaderArrivalRateInit:   true,
		ScoreLastUpdate:         time.Now().Add(-2 * scoreDecayHalfLife),
	}

	peer.UpdateBlockFetchObservation(100, false)

	if peer.BlockFetchSuccessRate >= 0.7 {
		t.Fatalf(
			"stale success history should decay before a failure, got %v",
			peer.BlockFetchSuccessRate,
		)
	}
	if peer.ConnectionStability <= 0.5 {
		t.Fatalf(
			"unrelated stale metrics should move toward neutral, got %v",
			peer.ConnectionStability,
		)
	}
}

func TestGetEMAAlpha(t *testing.T) {
	// Zero EMAAlpha should return default
	p1 := &Peer{EMAAlpha: 0}
	if alpha := p1.getEMAAlpha(); alpha != 0.2 {
		t.Fatalf("expected default alpha 0.2 when EMAAlpha=0, got %v", alpha)
	}

	// Negative EMAAlpha should return default
	p2 := &Peer{EMAAlpha: -0.1}
	if alpha := p2.getEMAAlpha(); alpha != 0.2 {
		t.Fatalf("expected default alpha 0.2 when EMAAlpha<0, got %v", alpha)
	}

	// Positive EMAAlpha should return the configured value
	p3 := &Peer{EMAAlpha: 0.4}
	if alpha := p3.getEMAAlpha(); alpha != 0.4 {
		t.Fatalf("expected configured alpha 0.4, got %v", alpha)
	}

	// EMAAlpha > 1 should be clamped to 1.0 to prevent invalid EMA behavior
	p4 := &Peer{EMAAlpha: 1.5}
	if alpha := p4.getEMAAlpha(); alpha != 1.0 {
		t.Fatalf("expected clamped alpha 1.0 when EMAAlpha>1, got %v", alpha)
	}

	// EMAAlpha exactly 1.0 should be returned as-is
	p5 := &Peer{EMAAlpha: 1.0}
	if alpha := p5.getEMAAlpha(); alpha != 1.0 {
		t.Fatalf("expected alpha 1.0, got %v", alpha)
	}
}

// A stale latency EMA must decay toward the score-neutral latency, not toward
// the unknown-latency penalty. Every other component decays to its 0.5-score
// baseline, so aging out a quiet peer's history must not actively demote it.
func TestStaleLatencyDecaysTowardNeutralNotPenalty(t *testing.T) {
	fast := &Peer{
		BlockFetchLatencyMs:   10,
		BlockFetchLatencyInit: true,
		ScoreLastUpdate:       time.Now().Add(-10 * scoreDecayHalfLife),
	}
	fast.decayScoreMetrics(time.Now())
	if fast.BlockFetchLatencyMs > neutralLatencyMs {
		t.Fatalf(
			"fast history must not decay past neutral %v, got %v",
			neutralLatencyMs, fast.BlockFetchLatencyMs,
		)
	}

	// Symmetry: a slow peer's stale history improves toward the same baseline.
	slow := &Peer{
		BlockFetchLatencyMs:   2_000,
		BlockFetchLatencyInit: true,
		ScoreLastUpdate:       time.Now().Add(-10 * scoreDecayHalfLife),
	}
	slow.decayScoreMetrics(time.Now())
	if slow.BlockFetchLatencyMs < neutralLatencyMs {
		t.Fatalf(
			"slow history must not decay below neutral %v, got %v",
			neutralLatencyMs, slow.BlockFetchLatencyMs,
		)
	}

	// After full decay the latency component is score-neutral (0.5), matching
	// the conservative default used for the other components.
	fast.BlockFetchLatencyMs = neutralLatencyMs
	fast.UpdatePeerScore()
	latencyScore := 1.0 / (1.0 + fast.BlockFetchLatencyMs/neutralLatencyMs)
	if latencyScore != 0.5 {
		t.Fatalf("neutral latency should score 0.5, got %v", latencyScore)
	}
}

// agePeerScoresLocked must decay a quiet peer's score so churn and quota
// ranking never read a favorable score that predates the decay window.
func TestAgePeerScoresLockedDecaysQuietPeers(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	quiet := &Peer{
		BlockFetchLatencyMs:     minLatencyMs,
		BlockFetchSuccessRate:   1,
		ConnectionStability:     1,
		HeaderArrivalRate:       10,
		BlockFetchLatencyInit:   true,
		BlockFetchSuccessInit:   true,
		ConnectionStabilityInit: true,
		HeaderArrivalRateInit:   true,
		ScoreLastUpdate:         time.Now().Add(-4 * scoreDecayHalfLife),
	}
	quiet.UpdatePeerScore()
	before := quiet.PerformanceScore
	pg.peers = []*Peer{quiet}

	pg.agePeerScoresLocked(time.Now())

	if quiet.PerformanceScore >= before {
		t.Fatalf(
			"a quiet peer's score should decay from %v, got %v",
			before, quiet.PerformanceScore,
		)
	}
	if quiet.BlockFetchSuccessRate >= 1 {
		t.Fatalf(
			"stale success rate should decay toward neutral, got %v",
			quiet.BlockFetchSuccessRate,
		)
	}
}

// A peer that has never reported an observation keeps its initial score, so it
// still ranks below observed peers instead of jumping to the conservative
// default (which sits above the default MinScoreThreshold).
func TestAgePeerScoresLockedSkipsNeverObservedPeers(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	fresh := &Peer{}
	pg.peers = []*Peer{nil, fresh}

	pg.agePeerScoresLocked(time.Now())

	if fresh.PerformanceScore != 0 {
		t.Fatalf(
			"never-observed peer should keep its initial score, got %v",
			fresh.PerformanceScore,
		)
	}
	if !fresh.ScoreLastUpdate.IsZero() {
		t.Fatal("aging must not stamp a never-observed peer as observed")
	}
}
