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
