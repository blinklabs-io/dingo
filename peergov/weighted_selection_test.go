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

package peergov

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWeightedSample_ProportionalToStake(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		{Hostname: "a.example.com", Port: 3001, Stake: 90},
		{Hostname: "b.example.com", Port: 3001, Stake: 10},
	}
	const iterations = 10000
	aFirst := 0
	for range iterations {
		got := weightedSample(relays, 1)
		require.Len(t, got, 1)
		if got[0].Hostname == "a.example.com" {
			aFirst++
		}
	}
	ratio := float64(aFirst) / iterations
	assert.InDelta(t, 0.9, ratio, 0.03)
}

func TestWeightedSample_NGreaterThanRelays(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		{Hostname: "a.example.com", Stake: 5},
		{Hostname: "b.example.com", Stake: 1},
		{Hostname: "c.example.com", Stake: 0},
	}
	got := weightedSample(relays, 10)
	assert.ElementsMatch(t, relays, got)
}

func TestWeightedSample_ZeroStakeGetsFloorWeight(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		{Hostname: "staked.example.com", Stake: 3},
		{Hostname: "unstaked.example.com", Stake: 0},
	}
	const iterations = 10000
	unstakedFirst := 0
	for range iterations {
		if weightedSample(relays, 1)[0].Hostname == "unstaked.example.com" {
			unstakedFirst++
		}
	}
	assert.Positive(t, unstakedFirst)
	assert.Less(t, unstakedFirst, iterations/2)
}

func TestWeightedSample_EmptyRelays(t *testing.T) {
	t.Parallel()
	assert.Empty(t, weightedSample(nil, 3))
	assert.Empty(t, weightedSample([]PoolRelay{}, 3))
}

func TestWeightedSample_ZeroN(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{{Hostname: "a.example.com", Stake: 1}}
	assert.Empty(t, weightedSample(relays, 0))
	assert.Empty(t, weightedSample(relays, -1))
}

func TestWeightedSample_SingleRelay(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{{Hostname: "a.example.com", Stake: 7}}
	for range 100 {
		assert.Equal(t, relays, weightedSample(relays, 1))
	}
}

func TestWeightedSample_NoDuplicates(t *testing.T) {
	t.Parallel()
	relays := make([]PoolRelay, 50)
	for i := range relays {
		relays[i] = PoolRelay{
			Hostname: fmt.Sprintf("r%d.example.com", i),
			Stake:    uint64(i * i),
		}
	}
	for range 200 {
		got := weightedSample(relays, 30)
		require.Len(t, got, 30)
		seen := map[string]struct{}{}
		for _, relay := range got {
			_, dup := seen[relay.Hostname]
			require.False(t, dup, "duplicate %s", relay.Hostname)
			seen[relay.Hostname] = struct{}{}
		}
	}
}

func TestWeightedSample_AllZeroStakeUniform(t *testing.T) {
	t.Parallel()
	relays := make([]PoolRelay, 4)
	for i := range relays {
		relays[i] = PoolRelay{Hostname: fmt.Sprintf("r%d.example.com", i)}
	}
	const iterations = 20000
	counts := map[string]int{}
	for range iterations {
		counts[weightedSample(relays, 1)[0].Hostname]++
	}
	for _, relay := range relays {
		assert.InDelta(
			t,
			0.25,
			float64(counts[relay.Hostname])/iterations,
			0.03,
		)
	}
}

func TestWeightedSamplePoolStakeIsNotMultipliedByRelayCount(t *testing.T) {
	t.Parallel()
	for _, stake := range []uint64{0, 100} {
		t.Run(fmt.Sprintf("stake_%d", stake), func(t *testing.T) {
			poolA := []byte{0xaa}
			poolB := []byte{0xbb}
			relays := make([]PoolRelay, 0, 51)
			for i := range 50 {
				relays = append(relays, PoolRelay{
					Hostname:    fmt.Sprintf("a-%d.example.com", i),
					PoolKeyHash: poolA,
					Stake:       stake,
				})
			}
			relays = append(relays, PoolRelay{
				Hostname: "b.example.com", PoolKeyHash: poolB, Stake: stake,
			})

			for range 100 {
				got := weightedSample(relays, 2)
				require.Len(t, got, 2)
				require.NotEqual(
					t,
					string(got[0].PoolKeyHash),
					string(got[1].PoolKeyHash),
					"one pool's relay count must not multiply its sampling weight",
				)
			}
		})
	}
}

func TestWeightedSampleRepeatsPoolsOnlyInLaterRounds(t *testing.T) {
	t.Parallel()
	poolA := []byte{0xaa}
	poolB := []byte{0xbb}
	relays := []PoolRelay{
		{Hostname: "a-1.example.com", PoolKeyHash: poolA, Stake: 100},
		{Hostname: "a-2.example.com", PoolKeyHash: poolA, Stake: 100},
		{Hostname: "b.example.com", PoolKeyHash: poolB, Stake: 1},
	}
	got := weightedSample(relays, len(relays))
	require.Len(t, got, len(relays))
	require.NotEqual(t, string(got[0].PoolKeyHash), string(got[1].PoolKeyHash))
	require.Equal(t, string(poolA), string(got[2].PoolKeyHash))
}

// Stakes near the uint64 limit must not overflow the running total: if they
// did, the draw range would collapse and one relay would always win.
func TestWeightedSample_HugeStakesDoNotOverflow(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		{Hostname: "a.example.com", Stake: math.MaxUint64},
		{Hostname: "b.example.com", Stake: math.MaxUint64},
		{Hostname: "c.example.com", Stake: math.MaxUint64},
		{Hostname: "d.example.com", Stake: math.MaxUint64},
		{Hostname: "e.example.com", Stake: 5},
	}
	assert.ElementsMatch(t, relays, weightedSample(relays, len(relays)))
	first := map[string]int{}
	for range 400 {
		first[weightedSample(relays, 1)[0].Hostname]++
	}
	for _, host := range []string{
		"a.example.com", "b.example.com", "c.example.com", "d.example.com",
	} {
		assert.Positive(t, first[host], "%s never drawn first", host)
	}
}

// The Fenwick descent has to stay correct as entries are removed; a heavy
// relay's chance of being drawn second reflects the remaining weights only.
func TestWeightedSample_RemovalKeepsRemainingProportions(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		{Hostname: "a.example.com", Stake: 1000},
		{Hostname: "b.example.com", Stake: 300},
		{Hostname: "c.example.com", Stake: 100},
	}
	const iterations = 20000
	bSecondGivenAFirst, aFirst := 0, 0
	for range iterations {
		got := weightedSample(relays, 2)
		if got[0].Hostname != "a.example.com" {
			continue
		}
		aFirst++
		if got[1].Hostname == "b.example.com" {
			bSecondGivenAFirst++
		}
	}
	require.Positive(t, aFirst)
	assert.InDelta(
		t,
		0.75,
		float64(bSecondGivenAFirst)/float64(aFirst),
		0.03,
	)
}

func newStakeDiscoveryGovernor(
	relays []PoolRelay,
	target int,
) *PeerGovernor {
	return NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerTarget:   target,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      relays,
			currentSlot: 1000,
		},
	})
}

// stakeRelay builds an IP relay so discovery needs no DNS.
func stakeRelay(ip string, stake uint64) PoolRelay {
	parsed := net.ParseIP(ip)
	return PoolRelay{IPv4: &parsed, Port: 3001, Stake: stake}
}

func TestDiscoverLedgerPeers_SamplesWeighted(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		stakeRelay("44.0.2.1", 1_000_000),
		stakeRelay("44.0.2.2", 2_000_000),
		stakeRelay("44.0.2.3", 3_000_000),
	}
	pg := newStakeDiscoveryGovernor(relays, 2)
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 2)
}

func TestDiscoverLedgerPeers_RespectsSampleSize(t *testing.T) {
	t.Parallel()
	relays := make([]PoolRelay, 100)
	for i := range relays {
		relays[i] = stakeRelay(
			fmt.Sprintf("44.0.3.%d", i+1),
			uint64(i+1)*1_000_000,
		)
	}
	pg := newStakeDiscoveryGovernor(relays, 10)
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 10)
}

// With a target of one over a whale and a minnow, the whale must win the
// large majority of rounds. A uniform shuffle would split them evenly.
func TestDiscoverLedgerPeers_BiasedTowardHighStake(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		stakeRelay("44.0.4.1", 99_000_000),
		stakeRelay("44.0.4.2", 1_000_000),
	}
	const rounds = 400
	whale := 0
	for range rounds {
		pg := newStakeDiscoveryGovernor(relays, 1)
		pg.discoverLedgerPeers()
		require.Len(t, pg.peers, 1)
		if pg.peers[0].Address == "44.0.4.1:3001" {
			whale++
		}
	}
	assert.Greater(t, whale, rounds*8/10)
}

func TestDiscoverLedgerPeers_PeerCarriesStake(t *testing.T) {
	t.Parallel()
	pg := newStakeDiscoveryGovernor(
		[]PoolRelay{stakeRelay("44.0.5.1", 42_000_000)},
		1,
	)
	pg.discoverLedgerPeers()
	require.Len(t, pg.peers, 1)
	assert.Equal(t, uint64(42_000_000), pg.peers[0].StakeLovelace)
}

func TestEndToEnd_WeightedDiscoveryAndScoring(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		stakeRelay("44.0.6.1", 500_000_000_000_000), // 500M ADA
		stakeRelay("44.0.6.2", 50_000_000_000_000),  // 50M ADA
		stakeRelay("44.0.6.3", 5_000_000_000_000),   // 5M ADA
		stakeRelay("44.0.6.4", 500_000_000_000),     // 500k ADA
		stakeRelay("44.0.6.5", 1_000_000),           // 1 ADA
	}
	pg := newStakeDiscoveryGovernor(relays, 3)
	pg.discoverLedgerPeers()
	require.Len(t, pg.peers, 3)

	stakeByAddr := map[string]uint64{}
	for _, relay := range relays {
		stakeByAddr[relay.IPv4.String()+":3001"] = relay.Stake
	}
	for _, peer := range pg.peers {
		assert.Equal(t, stakeByAddr[peer.Address], peer.StakeLovelace)
		assert.Positive(t, peer.StakeLovelace)
		peer.UpdateBlockFetchObservation(50, true)
		peer.UpdateConnectionStability(1)
		assert.Positive(t, peer.PerformanceScore)
	}

	// Identical observations: the only difference left is stake.
	var highest, lowest *Peer
	for _, peer := range pg.peers {
		if highest == nil || peer.StakeLovelace > highest.StakeLovelace {
			highest = peer
		}
		if lowest == nil || peer.StakeLovelace < lowest.StakeLovelace {
			lowest = peer
		}
	}
	assert.Greater(t, highest.PerformanceScore, lowest.PerformanceScore)
}

func TestWeightedSamplePreservesLargeWeightsWhenTotalFits(t *testing.T) {
	t.Parallel()
	relays := []PoolRelay{
		{Hostname: "large", Stake: 3 * (math.MaxUint64 / 4)},
		{Hostname: "small", Stake: math.MaxUint64 / 8},
	}
	firstLarge := 0
	for range 10000 {
		if weightedSample(relays, 1)[0].Hostname == "large" {
			firstLarge++
		}
	}
	assert.InDelta(t, 6.0/7.0, float64(firstLarge)/10000, 0.03)
}
