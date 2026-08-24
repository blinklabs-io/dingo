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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingResolver installs a lookupIPAddr stub that counts calls and returns
// a fixed result, restoring the previous resolver on cleanup.
func countingResolver(
	t *testing.T,
	ips []net.IP,
	err error,
) *atomic.Int64 {
	t.Helper()
	calls := new(atomic.Int64)
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls.Add(1)
		return ips, err
	}
	t.Cleanup(func() { lookupIPAddr = old })
	return calls
}

func discardGovernor() *PeerGovernor {
	return NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
}

func nxdomain(host string) error {
	return errors.New("lookup " + host + ": no such host")
}

// A relay address that already belongs to a known peer must not be resolved
// again. Ledger discovery re-offers the full relay set every round, so
// resolving before the exists check re-resolves every healthy connected peer
// and every dead hostname on every round.
func TestAddLedgerPeer_KnownPeerSkipsDNSResolution(t *testing.T) {
	calls := countingResolver(t, nil, nxdomain("relay.example.com"))
	pg := discardGovernor()
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:           "relay.example.com:3001",
		NormalizedAddress: "44.0.0.7:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
	})
	pg.mu.Unlock()

	require.False(t, pg.addLedgerPeer("relay.example.com:3001"),
		"an already-known relay must not be added twice")
	assert.Equal(t, int64(0), calls.Load(),
		"an already-known ledger relay must not be re-resolved")

	pg.mu.Lock()
	_, known := pg.ledgerKnownAddrs["44.0.0.7:3001"]
	pg.mu.Unlock()
	assert.True(t, known,
		"skipping resolution must still record the peer as ledger-known")
}

// A relay hostname already on the deny list must not be resolved. Deny
// entries for unresolvable hostnames are keyed on the lowercased hostname,
// which is exactly what the pre-resolution check can compare against.
func TestAddLedgerPeer_DeniedHostnameSkipsDNSResolution(t *testing.T) {
	calls := countingResolver(t, nil, nxdomain("dead.example.com"))
	pg := discardGovernor()
	pg.mu.Lock()
	pg.denyList["dead.example.com:3001"] = time.Now().
		Add(defaultDenyDuration)
	pg.mu.Unlock()

	require.False(t, pg.addLedgerPeer("DEAD.example.com:3001"),
		"a denied relay must not be added")
	assert.Equal(t, int64(0), calls.Load(),
		"a denied ledger relay must not be resolved")
}

// The negative case for the two above: an unknown, undenied relay must still
// be resolved and added, so the reordering cannot silently stop discovery.
func TestAddLedgerPeer_UnknownRelayStillResolves(t *testing.T) {
	calls := countingResolver(t, []net.IP{net.ParseIP("44.0.0.9")}, nil)
	pg := discardGovernor()

	require.True(t, pg.addLedgerPeer("fresh.example.com:3001"))
	assert.Equal(t, int64(1), calls.Load(),
		"a fresh relay hostname must still be resolved once")

	pg.mu.Lock()
	defer pg.mu.Unlock()
	require.Len(t, pg.peers, 1)
	assert.Equal(t, "44.0.0.9:3001", pg.peers[0].NormalizedAddress,
		"the peer must be keyed on the resolved address")
}

// A hostname that failed to resolve is cached as a negative result, so the
// next discovery round skips the lookup entirely rather than repeating it.
func TestResolveLedgerDiscoveryAddress_NegativeCacheSuppressesLookups(
	t *testing.T,
) {
	calls := countingResolver(t, nil, nxdomain("dead.example.com"))
	pg := discardGovernor()
	ctx := context.Background()

	first := pg.resolveLedgerDiscoveryAddress(ctx, "dead.example.com:3001")
	second := pg.resolveLedgerDiscoveryAddress(ctx, "dead.example.com:3001")

	assert.Equal(t, "dead.example.com:3001", first,
		"a failed resolution still falls back to the bare hostname")
	assert.Equal(t, first, second,
		"a cached failure must return the same fallback address")
	assert.Equal(t, int64(1), calls.Load(),
		"a cached resolution failure must not be looked up again")
}

// The negative cache is bounded in time: once an entry expires the hostname
// is resolved for real again, so a relay that starts resolving recovers
// instead of staying pinned as dead.
func TestResolveLedgerDiscoveryAddress_NegativeCacheExpiresAndRecovers(
	t *testing.T,
) {
	failing := new(atomic.Bool)
	failing.Store(true)
	calls := new(atomic.Int64)
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls.Add(1)
		if failing.Load() {
			return nil, nxdomain("flaky.example.com")
		}
		return []net.IP{net.ParseIP("44.0.0.4")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = old })

	pg := discardGovernor()
	ctx := context.Background()
	require.Equal(t, "flaky.example.com:3001",
		pg.resolveLedgerDiscoveryAddress(ctx, "flaky.example.com:3001"))
	require.Equal(t, int64(1), calls.Load())

	pg.negativeDNSMu.Lock()
	pg.negativeDNS["flaky.example.com"] = time.Now().Add(-time.Second)
	pg.negativeDNSMu.Unlock()
	failing.Store(false)

	got := pg.resolveLedgerDiscoveryAddress(ctx, "flaky.example.com:3001")
	assert.Equal(t, int64(2), calls.Load(),
		"an expired negative-cache entry must be re-resolved")
	assert.Equal(t, "44.0.0.4:3001", got,
		"a recovered hostname must resolve to its real address")

	pg.negativeDNSMu.Lock()
	_, cached := pg.negativeDNS["flaky.example.com"]
	pg.negativeDNSMu.Unlock()
	assert.False(t, cached,
		"a successful resolution must leave no cached failure behind")
}

// The cache is bounded in size: pool-published relay hostnames are untrusted
// input, so an unbounded map would be a memory sink.
func TestNegativeDNSCacheIsBounded(t *testing.T) {
	countingResolver(t, nil, errors.New("no such host"))
	pg := discardGovernor()
	ctx := context.Background()

	for i := range negativeDNSCacheMaxEntries + 64 {
		host := "dead" + strconv.Itoa(i) + ".example.com"
		pg.resolveLedgerDiscoveryAddress(ctx, host+":3001")
	}

	pg.negativeDNSMu.Lock()
	size := len(pg.negativeDNS)
	pg.negativeDNSMu.Unlock()
	assert.LessOrEqual(t, size, negativeDNSCacheMaxEntries,
		"the negative DNS cache must stay bounded")
}

// A pool publishing a dead relay hostname is a fact about the chain, not an
// operator-actionable fault, so it must not be logged at WARN.
func TestResolveLedgerDiscoveryAddress_FailureNotLoggedAtWarn(t *testing.T) {
	countingResolver(t, nil, nxdomain("dead.example.com"))

	var infoBuf bytes.Buffer
	pgInfo := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(&infoBuf, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	})
	pgInfo.resolveLedgerDiscoveryAddress(
		context.Background(),
		"dead.example.com:3001",
	)
	assert.NotContains(t, infoBuf.String(),
		"failed to resolve ledger relay hostname",
		"a dead pool-published relay must not warn the operator")

	var debugBuf bytes.Buffer
	pgDebug := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(&debugBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	})
	pgDebug.resolveLedgerDiscoveryAddress(
		context.Background(),
		"other-dead.example.com:3001",
	)
	assert.Contains(t, debugBuf.String(),
		"failed to resolve ledger relay hostname",
		"the failure must stay observable at debug level")
}
