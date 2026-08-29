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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func urgentDiscoveryGovernor(target int) *PeerGovernor {
	return NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   target,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
}

// Emergency discovery exists for transient starvation. Sustained starvation
// must escalate its cadence toward the normal refresh interval instead of
// running at the base emergency cadence indefinitely.
func TestEmergencyLedgerRefreshInterval_EscalatesAndCaps(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		MinHotPeers:                        10,
		LedgerPeerTarget:                   20,
		LedgerPeerRefreshInterval:          time.Hour,
		EmergencyLedgerPeerRefreshInterval: 30 * time.Second,
	})

	want := []time.Duration{
		30 * time.Second,
		1 * time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		8 * time.Minute,
		16 * time.Minute,
		32 * time.Minute,
		time.Hour, // 64m would exceed the normal interval; capped there
		time.Hour,
		time.Hour,
	}
	for rounds, expected := range want {
		//nolint:gosec // bounded loop index
		pg.emergencyRefreshRounds.Store(uint32(rounds))
		assert.Equal(t, expected, pg.emergencyLedgerRefreshInterval(),
			"interval after %d consecutive emergency rounds", rounds)
	}
}

// The escalated interval must actually gate discovery: a second round inside
// the interval the first one earned is suppressed, and a round past it runs.
func TestDiscoverLedgerPeers_EmergencyBackoffThrottlesRepeatRounds(
	t *testing.T,
) {
	pg := urgentDiscoveryGovernor(5)

	// Round one runs at the base emergency cadence.
	pg.discoverLedgerPeers()
	first := countPeersBySource(pg, PeerSourceP2PLedger)
	require.Positive(t, first, "urgent node must replenish on the first round")
	require.Equal(t, uint32(1), pg.emergencyRefreshRounds.Load())

	// 40s later: past the 30s base interval, inside the 60s round one earned.
	pg.lastLedgerPeerRefresh.Store(
		time.Now().Add(-40 * time.Second).UnixNano(),
	)
	pg.discoverLedgerPeers()
	assert.Equal(t, first, countPeersBySource(pg, PeerSourceP2PLedger),
		"a round inside the escalated interval must be suppressed")
	assert.Equal(t, uint32(1), pg.emergencyRefreshRounds.Load(),
		"a suppressed round must not count toward the backoff")

	// 90s later: past the escalated interval, so discovery runs again.
	pg.lastLedgerPeerRefresh.Store(
		time.Now().Add(-90 * time.Second).UnixNano(),
	)
	pg.discoverLedgerPeers()
	assert.Greater(t, countPeersBySource(pg, PeerSourceP2PLedger), first,
		"a round past the escalated interval must replenish")
	assert.Equal(t, uint32(2), pg.emergencyRefreshRounds.Load())
}

// Recovery resets the backoff, so the next starvation event is served at the
// base emergency cadence rather than an hour later.
func TestDiscoverLedgerPeers_EmergencyBackoffResetsWhenNotUrgent(
	t *testing.T,
) {
	pg := urgentDiscoveryGovernor(5)
	pg.emergencyRefreshRounds.Store(5)
	addEligibleUpstreamPeers(pg, pg.config.MinHotPeers)

	require.False(t, pg.ledgerPeersUrgent())
	pg.discoverLedgerPeers()

	assert.Equal(t, uint32(0), pg.emergencyRefreshRounds.Load(),
		"a recovered node must return to the base emergency cadence")
}

// The first emergency round after startup must not be delayed: a collapsed
// peer pool still recovers in seconds.
func TestDiscoverLedgerPeers_EmergencyFirstRoundUsesBaseInterval(
	t *testing.T,
) {
	pg := urgentDiscoveryGovernor(5)
	assert.Equal(t,
		pg.config.EmergencyLedgerPeerRefreshInterval,
		pg.emergencyLedgerRefreshInterval(),
		"the first emergency round must use the base cadence",
	)
}
