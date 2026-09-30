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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzNormalizeAddress(f *testing.F) {
	f.Add("")
	f.Add("Example.COM:3001")
	f.Add("[0:0:0:0:0:0:0:1]:3001")
	f.Add("malformed:address:with:colons")

	f.Fuzz(func(t *testing.T, address string) {
		var p PeerGovernor
		normalized := p.normalizeAddress(address)
		if p.normalizeAddress(normalized) != normalized {
			t.Fatalf("normalizeAddress is not idempotent: %q -> %q",
				normalized,
				p.normalizeAddress(normalized),
			)
		}

		host, port, err := net.SplitHostPort(address)
		if err != nil {
			if normalized != strings.ToLower(address) {
				t.Fatalf(
					"malformed address normalized to %q, want lowercase %q",
					normalized,
					strings.ToLower(address),
				)
			}
			return
		}
		normalizedHost, normalizedPort, err := net.SplitHostPort(normalized)
		if err != nil {
			t.Fatalf("normalized address is not host:port: %q", normalized)
		}
		if normalizedPort != port {
			t.Fatalf("normalized port = %q, want %q", normalizedPort, port)
		}
		if net.ParseIP(host) == nil && normalizedHost != strings.ToLower(host) {
			t.Fatalf(
				"normalized hostname = %q, want %q",
				normalizedHost,
				strings.ToLower(host),
			)
		}
	})
}

func FuzzAddressHost(f *testing.F) {
	f.Add("")
	f.Add("Example.COM:3001")
	f.Add("[0:0:0:0:0:0:0:1]:3001")

	f.Fuzz(func(t *testing.T, address string) {
		host := addressHost(address)
		inputHost, _, err := net.SplitHostPort(address)
		if err != nil {
			if host != "" {
				t.Fatalf(
					"addressHost(%q) = %q, want empty on parse failure",
					address,
					host,
				)
			}
			return
		}
		if ip := net.ParseIP(inputHost); ip != nil {
			if host != ip.String() {
				t.Fatalf("addressHost IP = %q, want %q", host, ip.String())
			}
			return
		}
		if host != strings.ToLower(inputHost) {
			t.Fatalf(
				"addressHost hostname = %q, want %q",
				host,
				strings.ToLower(inputHost),
			)
		}
	})
}

// FuzzIsRoutableAddr checks the string wrapper, not the routability policy
// itself: whatever host isRoutableAddr extracts must be judged exactly as
// IsRoutableIP judges it, and a host that is not an IP literal must be
// accepted as a hostname. The policy's own address classes are enumerated in
// TestIsRoutableIP, so restating them here would only duplicate that table and
// drift from it — which is what happened when unreachablePrefixes was added.
func FuzzIsRoutableAddr(f *testing.F) {
	f.Add("")
	f.Add("127.0.0.1:3001")
	f.Add("10.0.0.1:3001")
	f.Add("8.8.8.8:3001")
	f.Add("relay.example.com:3001")
	// Seeds inside unreachablePrefixes, which net.IP reports as global
	// unicast. Without these the corpus never reaches that branch.
	f.Add("100.64.0.1:3001")
	f.Add("192.0.0.1:3001")
	f.Add("198.18.0.1:3001")
	f.Add("240.0.0.1:3001")
	f.Add("[100::1]:3001")
	f.Add("[::ffff:100.64.0.1]:3001")

	f.Fuzz(func(t *testing.T, address string) {
		routable := isRoutableAddr(address)
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			host = address
		}
		ip := net.ParseIP(host)
		if ip == nil {
			if !routable {
				t.Fatalf(
					"hostname or malformed address %q should be treated as routable",
					address,
				)
			}
			return
		}
		if want := IsRoutableIP(ip); routable != want {
			t.Fatalf(
				"isRoutableAddr(%q)=%v disagrees with IsRoutableIP(%v)=%v",
				address,
				routable,
				ip,
				want,
			)
		}
	})
}

func BenchmarkReconcile(b *testing.B) {
	for _, peerCount := range []int{100, 500, 1000} {
		b.Run(strconv.Itoa(peerCount), func(b *testing.B) {
			pg := NewPeerGovernor(PeerGovernorConfig{
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
				MinHotPeers:                    20,
				TargetNumberOfActivePeers:      20,
				TargetNumberOfEstablishedPeers: 50,
				TargetNumberOfKnownPeers:       150,
			})
			basePeers := benchmarkPeers(peerCount)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				pg.peers = cloneBenchmarkPeers(basePeers)
				pg.denyList = make(map[string]time.Time)
				pg.bootstrapExited = false
				pg.lastBootstrapExit = time.Time{}
				b.StartTimer()
				pg.reconcile(b.Context())
			}
		})
	}
}

func benchmarkPeers(count int) []*Peer {
	now := time.Now()
	peers := make([]*Peer, 0, count)
	for i := range count {
		peer := &Peer{
			Address: net.JoinHostPort(
				"198.51.100."+strconv.Itoa(i%250+1),
				strconv.Itoa(3000+i),
			),
			NormalizedAddress: net.JoinHostPort(
				"198.51.100."+strconv.Itoa(i%250+1),
				strconv.Itoa(3000+i),
			),
			FirstSeen:    now.Add(-2 * time.Hour),
			LastActivity: now.Add(-1 * time.Minute),
			GroupID:      "group-" + strconv.Itoa(i%16),
			Valency:      2,
			WarmValency:  4,
		}
		switch i % 6 {
		case 0:
			peer.Source = PeerSourceTopologyLocalRoot
		case 1:
			peer.Source = PeerSourceTopologyPublicRoot
		case 2:
			peer.Source = PeerSourceP2PGossip
		case 3:
			peer.Source = PeerSourceP2PLedger
		case 4:
			peer.Source = PeerSourceInboundConn
		default:
			peer.Source = PeerSourceUnknown
		}
		switch {
		case i < 20:
			peer.State = PeerStateHot
			peer.Connection = benchmarkPeerConnection(i)
			if i%3 == 0 {
				peer.LastActivity = now.Add(-30 * time.Minute)
			}
		case i < count/2:
			peer.State = PeerStateWarm
			peer.Connection = benchmarkPeerConnection(i)
			peer.BlockFetchLatencyMs = 120 + float64(i%40)
			peer.BlockFetchLatencyInit = true
			peer.BlockFetchSuccessRate = 0.95
			peer.BlockFetchSuccessInit = true
			peer.ConnectionStability = 0.90
			peer.ConnectionStabilityInit = true
			peer.HeaderArrivalRate = 4 + float64(i%5)
			peer.HeaderArrivalRateInit = true
			peer.TipSlotDelta = int64(-(i % 10))
			peer.TipSlotDeltaInit = true
			if peer.Source == PeerSourceInboundConn {
				peer.FirstSeen = now.Add(-30 * time.Minute)
			}
			peer.UpdatePeerScore()
		default:
			peer.State = PeerStateCold
			if i%5 == 0 {
				peer.Connection = benchmarkPeerConnection(i)
			}
			if i%9 == 0 {
				peer.ReconnectCount = 5
			}
		}
		peers = append(peers, peer)
	}
	return peers
}

func cloneBenchmarkPeers(peers []*Peer) []*Peer {
	cloned := make([]*Peer, 0, len(peers))
	for _, peer := range peers {
		if peer == nil {
			cloned = append(cloned, nil)
			continue
		}
		peerCopy := *peer
		if peer.Connection != nil {
			connCopy := *peer.Connection
			peerCopy.Connection = &connCopy
		}
		cloned = append(cloned, &peerCopy)
	}
	return cloned
}

func benchmarkPeerConnection(i int) *PeerConnection {
	return &PeerConnection{
		Id: ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 3001 + i,
			},
			RemoteAddr: &net.TCPAddr{
				IP:   net.IPv4(198, 51, 100, byte(i%250+1)),
				Port: 4001 + i,
			},
		},
		IsClient: true,
	}
}

func newDialSpreadGovernor() *PeerGovernor {
	return NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
}

// TestResolveDialAddress_IPv4LiteralUnchanged verifies that an IPv4 literal
// peer is dialed exactly as before: no DNS resolution, no rotation. The
// injected resolver would return a different address if it were consulted,
// so an unchanged result proves the resolver was not used.
func TestResolveDialAddress_IPv4LiteralUnchanged(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("203.0.113.99")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	const addr = "195.191.47.210:3001"
	assert.Equal(t, addr, pg.resolveDialAddress(context.Background(), addr))
}

// TestResolveDialAddress_IPv6LiteralUnchanged verifies IPv6 literals are
// returned unchanged (bracketed host:port preserved).
func TestResolveDialAddress_IPv6LiteralUnchanged(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("2001:db8::1")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	const addr = "[2001:db8::dead:beef]:3001"
	assert.Equal(t, addr, pg.resolveDialAddress(context.Background(), addr))
}

// TestResolveDialAddress_MalformedAddressUnchanged verifies that an address
// without a host:port split (no resolvable form) is returned unchanged so
// the dialer can report the error as before.
func TestResolveDialAddress_MalformedAddressUnchanged(t *testing.T) {
	pg := newDialSpreadGovernor()
	const addr = "not-a-host-port"
	assert.Equal(t, addr, pg.resolveDialAddress(context.Background(), addr))
}

// TestResolveDialAddress_ResolutionFailureFallsBack verifies that when DNS
// resolution fails, the original hostname:port is returned so the dialer
// can attempt its own resolution — identical to the pre-spread behavior.
func TestResolveDialAddress_ResolutionFailureFallsBack(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return nil, errors.New("lookup failed")
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	const addr = "relay.example.com:3001"
	assert.Equal(t, addr, pg.resolveDialAddress(context.Background(), addr))
}

// TestResolveDialAddress_EmptyResolutionFallsBack verifies that an empty
// (but non-error) resolution result falls back to the original address.
func TestResolveDialAddress_EmptyResolutionFallsBack(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	const addr = "relay.example.com:3001"
	assert.Equal(t, addr, pg.resolveDialAddress(context.Background(), addr))
}

// TestResolveDialAddress_SingleIPHostname verifies that a hostname resolving
// to exactly one record always yields that record's IP:port.
func TestResolveDialAddress_SingleIPHostname(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("198.51.100.7")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	for range 20 {
		assert.Equal(
			t,
			"198.51.100.7:3001",
			pg.resolveDialAddress(
				context.Background(),
				"relay.example.com:3001",
			),
		)
	}
}

// TestResolveDialAddress_MultiIPRotates verifies the core fix: a hostname
// that resolves to many records (mixed IPv4/IPv6, mirroring the observed
// load-balancer relay with 5 IPv4 + 11 IPv6) spreads the dial target across
// more than one backend across repeated attempts, and every returned target
// is a valid member of the resolved set with the original port preserved.
func TestResolveDialAddress_MultiIPRotates(t *testing.T) {
	resolved := []net.IP{
		net.ParseIP("192.0.2.1"),
		net.ParseIP("192.0.2.2"),
		net.ParseIP("192.0.2.3"),
		net.ParseIP("192.0.2.4"),
		net.ParseIP("192.0.2.5"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("2001:db8::2"),
		net.ParseIP("2001:db8::3"),
	}
	valid := make(map[string]struct{}, len(resolved))
	for _, ip := range resolved {
		valid[net.JoinHostPort(ip.String(), "3001")] = struct{}{}
	}

	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		// Return a fresh copy so callers cannot mutate the fixture.
		out := make([]net.IP, len(resolved))
		copy(out, resolved)
		return out, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	seen := make(map[string]struct{})
	for range 200 {
		got := pg.resolveDialAddress(
			context.Background(),
			"leios-node.play.dev.cardano.org:3001",
		)
		_, ok := valid[got]
		require.Truef(
			t,
			ok,
			"dial target %q is not one of the resolved records",
			got,
		)
		seen[got] = struct{}{}
	}
	// The whole point of the fix: repeated attempts must not pin to a
	// single backend. With 8 records over 200 draws, seeing fewer than 2
	// distinct targets is statistically impossible (~5*(1/8)^200).
	assert.Greaterf(
		t,
		len(seen),
		1,
		"expected dial target to spread across multiple backends, saw only %v",
		seen,
	)
}

// setLocalAddrFamilies injects a deterministic local-family detection result
// for the duration of a test so filtering does not depend on the real host's
// interfaces, and restores the previous detector on cleanup.
func setLocalAddrFamilies(t *testing.T, hasV4, hasV6 bool) {
	t.Helper()
	old := localAddrFamilies
	localAddrFamilies = func() (bool, bool) { return hasV4, hasV6 }
	t.Cleanup(func() { localAddrFamilies = old })
}

func TestSupportedDialFamilies_UsesCachedResultWithinTTL(t *testing.T) {
	old := localAddrFamilies
	calls := 0
	localAddrFamilies = func() (bool, bool) {
		calls++
		if calls == 1 {
			return true, false
		}
		return false, true
	}
	t.Cleanup(func() { localAddrFamilies = old })

	pg := newDialSpreadGovernor()
	hasV4, hasV6 := pg.supportedDialFamilies()
	assert.True(t, hasV4)
	assert.False(t, hasV6)

	hasV4, hasV6 = pg.supportedDialFamilies()
	assert.True(t, hasV4)
	assert.False(t, hasV6)
	assert.Equal(t, 1, calls)
}

func TestSupportedDialFamilies_RefreshesAfterTTL(t *testing.T) {
	old := localAddrFamilies
	calls := 0
	localAddrFamilies = func() (bool, bool) {
		calls++
		if calls == 1 {
			return true, false
		}
		return false, true
	}
	t.Cleanup(func() { localAddrFamilies = old })

	pg := newDialSpreadGovernor()
	hasV4, hasV6 := pg.supportedDialFamilies()
	require.True(t, hasV4)
	require.False(t, hasV6)

	pg.dialFamilyMu.Lock()
	pg.dialFamilyCheckedAt = time.Now().Add(-dialFamilyCacheTTL - time.Second)
	pg.dialFamilyMu.Unlock()

	hasV4, hasV6 = pg.supportedDialFamilies()
	assert.False(t, hasV4)
	assert.True(t, hasV6)
	assert.Equal(t, 2, calls)
}

// mixedFamilyResolver installs a lookupIPAddr returning the given IPs (fresh
// copy each call) and restores the previous resolver on cleanup.
func mixedFamilyResolver(t *testing.T, ips []net.IP) {
	t.Helper()
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		out := make([]net.IP, len(ips))
		copy(out, ips)
		return out, nil
	}
	t.Cleanup(func() { lookupIPAddr = old })
}

// dialTargetIsV4 parses a resolveDialAddress result and reports whether the
// selected host is an IPv4 address.
func dialTargetIsV4(t *testing.T, target string) bool {
	t.Helper()
	host, _, err := net.SplitHostPort(target)
	require.NoError(t, err)
	ip := net.ParseIP(host)
	require.NotNilf(t, ip, "dial target host %q is not an IP", host)
	return ip.To4() != nil
}

var mixedV4V6Records = []net.IP{
	net.ParseIP("192.0.2.1"),
	net.ParseIP("192.0.2.2"),
	net.ParseIP("192.0.2.3"),
	net.ParseIP("2001:db8::1"),
	net.ParseIP("2001:db8::2"),
}

// TestResolveDialAddress_V4OnlyHostFiltersV6 verifies that on a v4-only host
// the IPv6 records are filtered out so no dial is wasted on an unreachable
// family; every selected target is IPv4.
func TestResolveDialAddress_V4OnlyHostFiltersV6(t *testing.T) {
	mixedFamilyResolver(t, mixedV4V6Records)
	setLocalAddrFamilies(t, true, false)

	pg := newDialSpreadGovernor()
	for range 100 {
		got := pg.resolveDialAddress(
			context.Background(),
			"relay.example.com:3001",
		)
		assert.Truef(
			t,
			dialTargetIsV4(t, got),
			"expected IPv4 target, got %q",
			got,
		)
	}
}

// TestResolveDialAddress_V6OnlyHostFiltersV4 verifies the symmetric case: on a
// v6-only host every selected target is IPv6.
func TestResolveDialAddress_V6OnlyHostFiltersV4(t *testing.T) {
	mixedFamilyResolver(t, mixedV4V6Records)
	setLocalAddrFamilies(t, false, true)

	pg := newDialSpreadGovernor()
	for range 100 {
		got := pg.resolveDialAddress(
			context.Background(),
			"relay.example.com:3001",
		)
		assert.Falsef(
			t,
			dialTargetIsV4(t, got),
			"expected IPv6 target, got %q",
			got,
		)
	}
}

// TestResolveDialAddress_DualStackKeepsBoth verifies that on a dual-stack host
// no family is filtered: both IPv4 and IPv6 targets are reachable across
// repeated attempts.
func TestResolveDialAddress_DualStackKeepsBoth(t *testing.T) {
	mixedFamilyResolver(t, mixedV4V6Records)
	setLocalAddrFamilies(t, true, true)

	pg := newDialSpreadGovernor()
	sawV4, sawV6 := false, false
	for range 300 {
		got := pg.resolveDialAddress(
			context.Background(),
			"relay.example.com:3001",
		)
		if dialTargetIsV4(t, got) {
			sawV4 = true
		} else {
			sawV6 = true
		}
	}
	assert.True(
		t,
		sawV4,
		"expected at least one IPv4 target on dual-stack host",
	)
	assert.True(
		t,
		sawV6,
		"expected at least one IPv6 target on dual-stack host",
	)
}

// TestResolveDialAddress_EmptyAfterFilterFallsBackToAll verifies the safety
// fallback: when no resolved record matches a supported family (here a v6-only
// host but v4-only records), the peer is not stranded — the full record set is
// used so a target is still selected.
func TestResolveDialAddress_EmptyAfterFilterFallsBackToAll(t *testing.T) {
	v4Only := []net.IP{
		net.ParseIP("192.0.2.10"),
		net.ParseIP("192.0.2.11"),
	}
	mixedFamilyResolver(t, v4Only)
	setLocalAddrFamilies(t, false, true) // host claims v6-only

	valid := map[string]struct{}{
		"192.0.2.10:3001": {},
		"192.0.2.11:3001": {},
	}
	pg := newDialSpreadGovernor()
	for range 50 {
		got := pg.resolveDialAddress(
			context.Background(),
			"relay.example.com:3001",
		)
		_, ok := valid[got]
		assert.Truef(t, ok, "expected fallback to full v4 set, got %q", got)
	}
}

// TestResolveDialAddress_DetectionInconclusiveFallsBackToAll verifies that when
// family detection is inconclusive (neither family detected) no filtering is
// applied: both families remain reachable across repeated attempts.
func TestResolveDialAddress_DetectionInconclusiveFallsBackToAll(t *testing.T) {
	mixedFamilyResolver(t, mixedV4V6Records)
	setLocalAddrFamilies(t, false, false) // inconclusive

	pg := newDialSpreadGovernor()
	sawV4, sawV6 := false, false
	for range 300 {
		got := pg.resolveDialAddress(
			context.Background(),
			"relay.example.com:3001",
		)
		if dialTargetIsV4(t, got) {
			sawV4 = true
		} else {
			sawV6 = true
		}
	}
	assert.True(t, sawV4, "inconclusive detection must not filter out IPv4")
	assert.True(t, sawV6, "inconclusive detection must not filter out IPv6")
}

// TestResolveDialAddress_CanceledContextFallsBack verifies that when the
// passed context is already done (e.g. governor shutdown), a context-honoring
// resolver returns the context error and resolveDialAddress falls back to the
// original hostname:port instead of stranding the peer.
func TestResolveDialAddress_CanceledContextFallsBack(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(ctx context.Context, _ string) ([]net.IP, error) {
		// Mirror net.Resolver: honor the context and surface its error.
		return nil, ctx.Err()
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pg := newDialSpreadGovernor()
	const addr = "relay.example.com:3001"
	assert.Equal(t, addr, pg.resolveDialAddress(ctx, addr))
}

// TestResolveDialAddress_BoundsLookupWithDeadline verifies the core review
// fix: the resolver is always invoked with a deadline-bounded context so a
// hung or slow resolver cannot block the outbound-dial loop.
func TestResolveDialAddress_BoundsLookupWithDeadline(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	var sawDeadline bool
	lookupIPAddr = func(ctx context.Context, _ string) ([]net.IP, error) {
		_, sawDeadline = ctx.Deadline()
		return []net.IP{net.ParseIP("198.51.100.7")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	got := pg.resolveDialAddress(context.Background(), "relay.example.com:3001")
	assert.Equal(t, "198.51.100.7:3001", got)
	assert.True(
		t,
		sawDeadline,
		"resolver must receive a deadline-bounded context",
	)
}

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

type countingLedgerPeerProvider struct {
	calls atomic.Int32
}

func (p *countingLedgerPeerProvider) GetPoolRelays() ([]PoolRelay, error) {
	p.calls.Add(1)
	return nil, nil
}

func (*countingLedgerPeerProvider) CurrentSlot() uint64 {
	return 1
}

type slowLedgerPeerProvider struct {
	started chan struct{}
	release <-chan struct{}
	calls   atomic.Int32
}

func (p *slowLedgerPeerProvider) GetPoolRelays() ([]PoolRelay, error) {
	p.calls.Add(1)
	p.started <- struct{}{}
	<-p.release
	return nil, nil
}

func (*slowLedgerPeerProvider) CurrentSlot() uint64 {
	return 1
}

type panicLedgerPeerProvider struct {
	panicOnCall atomic.Bool
	calls       atomic.Int32
}

func (p *panicLedgerPeerProvider) GetPoolRelays() ([]PoolRelay, error) {
	p.calls.Add(1)
	if p.panicOnCall.Load() {
		panic("ledger provider panic")
	}
	return nil, nil
}

func (*panicLedgerPeerProvider) CurrentSlot() uint64 {
	return 1
}

// addEligibleUpstreamPeers appends n connected, chain-selection-eligible
// upstream peers (client connections from a topology source).
func addEligibleUpstreamPeers(pg *PeerGovernor, n int) {
	pg.mu.Lock()
	for i := range n {
		addr := "10.10.0." + strconv.Itoa(i+1) + ":3001"
		pg.peers = append(pg.peers, &Peer{
			Address:           addr,
			NormalizedAddress: addr,
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		})
	}
	pg.mu.Unlock()
}

func countPeersBySource(pg *PeerGovernor, src PeerSource) int {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	n := 0
	for _, peer := range pg.peers {
		if peer != nil && peer.Source == src {
			n++
		}
	}
	return n
}

func fiftyLedgerRelays() []PoolRelay {
	relays := make([]PoolRelay, 50)
	for i := range relays {
		ip := net.ParseIP(
			"44.0." + strconv.Itoa(i/2) + "." + strconv.Itoa(i%2+1),
		)
		relays[i] = PoolRelay{IPv4: &ip, Port: 3001}
	}
	return relays
}

// The node is "urgent" for ledger-peer replenishment while it has fewer
// connected upstreams than its hot-peer target, and never when discovery is
// disabled.
func TestLedgerPeersUrgent(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:      10,
		LedgerPeerTarget: 20,
	})
	minHot := pg.config.MinHotPeers

	assert.True(t, pg.ledgerPeersUrgent(),
		"a node with no connected upstreams must be urgent")

	addEligibleUpstreamPeers(pg, minHot-1)
	assert.True(t, pg.ledgerPeersUrgent(),
		"still urgent while below the hot-peer target")

	addEligibleUpstreamPeers(pg, 1) // now at minHot
	assert.False(t, pg.ledgerPeersUrgent(),
		"not urgent once the hot-peer target is met")

	pgDisabled := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:      10,
		LedgerPeerTarget: -1, // discovery disabled
	})
	assert.False(t, pgDisabled.ledgerPeersUrgent(),
		"ledger discovery disabled is never urgent")
}

// When the node is peer-starved, discovery must ignore the (hourly) refresh
// interval and replenish immediately, so a collapsed pool never wedges the
// node while relays are still available.
func TestDiscoverLedgerPeers_EmergencyBypassesRefreshInterval(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   5,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
	// Refreshed 1 minute ago: within the hourly interval (normal gate would
	// block) but past the 30s emergency interval.
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-1 * time.Minute).UnixNano())
	// No upstreams -> urgent -> emergency cadence bypasses the hourly gate.
	pg.discoverLedgerPeers()

	assert.Equal(t, 5, countPeersBySource(pg, PeerSourceP2PLedger),
		"urgent node must replenish ledger peers despite a recent refresh")
}

// When the known ledger-peer target is already full of unusable peers, urgent
// discovery must still add fresh candidates instead of returning early.
func TestDiscoverLedgerPeers_EmergencyBypassesSatisfiedTarget(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   2,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
	pg.mu.Lock()
	pg.peers = append(pg.peers,
		&Peer{
			Address:           "44.0.0.1:3001",
			NormalizedAddress: "44.0.0.1:3001",
			Source:            PeerSourceP2PLedger,
			State:             PeerStateCold,
		},
		&Peer{
			Address:           "44.0.0.2:3001",
			NormalizedAddress: "44.0.0.2:3001",
			Source:            PeerSourceP2PLedger,
			State:             PeerStateCold,
		},
	)
	pg.mu.Unlock()
	assert.Equal(t, 0, pg.ledgerPeerDeficit(),
		"known ledger-peer target should appear satisfied")
	// Refreshed 1 minute ago: within the hourly interval but past the default
	// emergency interval, so only the urgent path may proceed.
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-1 * time.Minute).UnixNano())

	pg.discoverLedgerPeers()

	assert.Greater(
		t,
		countPeersBySource(pg, PeerSourceP2PLedger),
		2,
		"urgent discovery must add fresh ledger peers even when target appears satisfied",
	)
}

func TestDiscoverLedgerPeers_EmergencyLogField(t *testing.T) {
	var logBuf bytes.Buffer
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(&logBuf, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   1,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	assert.Contains(t, logBuf.String(), `"emergency":true`,
		"emergency ledger discovery log should include emergency=true")
}

// A healthy node (at its hot-peer target) must NOT bypass the refresh
// interval: a recent refresh suppresses discovery as normal.
func TestDiscoverLedgerPeers_NoBypassWhenNotUrgent(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   5,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
	addEligibleUpstreamPeers(
		pg,
		pg.config.MinHotPeers,
	) // at hot target: not urgent
	// Refreshed 1 minute ago: inside the hourly interval, so a non-urgent node
	// must not discover (no emergency bypass applies).
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-1 * time.Minute).UnixNano())

	pg.discoverLedgerPeers()

	assert.Equal(
		t,
		0,
		countPeersBySource(pg, PeerSourceP2PLedger),
		"non-urgent node must respect the refresh interval and add no ledger peers",
	)
}

func TestDiscoverLedgerPeers_SlowProviderRemainsSingleFlight(t *testing.T) {
	release := make(chan struct{})
	provider := &slowLedgerPeerProvider{
		started: make(chan struct{}, 2),
		release: release,
	}
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		UseLedgerAfterSlot:                 0,
		MinHotPeers:                        2,
		LedgerPeerTarget:                   1,
		LedgerPeerProvider:                 provider,
		EmergencyLedgerPeerRefreshInterval: time.Nanosecond,
		LedgerPeerRefreshInterval:          4 * time.Minute,
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		pg.discoverLedgerPeers()
	}()
	defer func() {
		testutil.RequireReceive(t, firstDone, time.Second,
			"first discovery must finish after its provider is released")
	}()
	defer close(release)

	testutil.RequireReceive(t, provider.started, time.Second,
		"first discovery must enter the provider")
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		pg.discoverLedgerPeers()
	}()
	testutil.RequireReceive(t, secondDone, time.Second,
		"a later emergency tick must return while discovery is in flight")
	require.Equal(t, int32(1), provider.calls.Load(),
		"a slow provider must not be entered by overlapping discoveries")
}

func TestDiscoverLedgerPeers_CanceledRefreshReleasesClaimAndDelay(
	t *testing.T,
) {
	provider := &countingLedgerPeerProvider{}
	pg := NewPeerGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot:                 0,
		MinHotPeers:                        2,
		LedgerPeerTarget:                   1,
		LedgerPeerProvider:                 provider,
		EmergencyLedgerPeerRefreshInterval: time.Nanosecond,
		LedgerPeerRefreshInterval:          4 * time.Minute,
	})
	lastRefresh := time.Now().Add(-time.Minute).UnixNano()
	pg.lastLedgerPeerRefresh.Store(lastRefresh)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pg.discoverLedgerPeersContext(ctx)
	require.Equal(t, int32(0), provider.calls.Load())
	require.Zero(t, pg.ledgerDiscoveryInFlight.Load())
	require.Equal(t, lastRefresh, pg.lastLedgerPeerRefresh.Load())

	pg.discoverLedgerPeers()
	require.Equal(t, int32(1), provider.calls.Load(),
		"the next discovery must retry immediately after cancellation")
}

func TestDiscoverLedgerPeers_CanceledSlowProviderReleasesClaimAndDelay(
	t *testing.T,
) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProvider := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseProvider()
	provider := &slowLedgerPeerProvider{
		started: make(chan struct{}, 2),
		release: release,
	}
	pg := NewPeerGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot:                 0,
		MinHotPeers:                        2,
		LedgerPeerTarget:                   1,
		LedgerPeerProvider:                 provider,
		EmergencyLedgerPeerRefreshInterval: time.Nanosecond,
		LedgerPeerRefreshInterval:          4 * time.Minute,
	})
	lastRefresh := time.Now().Add(-time.Minute).UnixNano()
	pg.lastLedgerPeerRefresh.Store(lastRefresh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pg.discoverLedgerPeersContext(ctx)
	}()
	testutil.RequireReceive(t, provider.started, time.Second,
		"discovery must enter the slow provider")
	cancel()
	releaseProvider()
	testutil.RequireReceive(t, done, time.Second,
		"canceled discovery must finish after its provider returns")
	require.Equal(t, int32(1), provider.calls.Load())
	require.Zero(t, pg.ledgerDiscoveryInFlight.Load())
	require.Equal(t, lastRefresh, pg.lastLedgerPeerRefresh.Load())

	pg.discoverLedgerPeers()
	require.Equal(t, int32(2), provider.calls.Load(),
		"the next discovery must retry immediately after cancellation")
}

func TestDiscoverLedgerPeers_PanickingProviderReleasesClaimAndDelay(
	t *testing.T,
) {
	provider := &panicLedgerPeerProvider{}
	provider.panicOnCall.Store(true)
	pg := NewPeerGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot:                 0,
		MinHotPeers:                        2,
		LedgerPeerTarget:                   1,
		LedgerPeerProvider:                 provider,
		EmergencyLedgerPeerRefreshInterval: time.Nanosecond,
		LedgerPeerRefreshInterval:          4 * time.Minute,
	})
	lastRefresh := time.Now().Add(-time.Minute).UnixNano()
	pg.lastLedgerPeerRefresh.Store(lastRefresh)

	func() {
		defer func() {
			require.Equal(t, "ledger provider panic", recover())
		}()
		pg.discoverLedgerPeers()
	}()
	require.Zero(t, pg.ledgerDiscoveryInFlight.Load())
	require.Equal(t, lastRefresh, pg.lastLedgerPeerRefresh.Load())

	provider.panicOnCall.Store(false)
	pg.discoverLedgerPeers()
	require.Equal(t, int32(2), provider.calls.Load(),
		"the next discovery must retry immediately after a provider panic")
}

// deadDialAddress is a loopback address with nothing listening, so an outbound
// dial to it fails fast and drives the reconnect fail path.
const deadDialAddress = "127.0.0.1:1"

// newReconnectGateTestGovernor wires a PeerGovernor with a real connection
// manager whose dials to deadDialAddress fail fast, so the outbound reconnect
// gate can be exercised end to end.
func newReconnectGateTestGovernor(t *testing.T, threshold int) *PeerGovernor {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   logger,
		EventBus: newMockEventBus(),
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
		MaxReconnectFailureThreshold: threshold,
		DenyDuration:                 30 * time.Minute,
	})
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.mu.Unlock()
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })
	return pg
}

// setupReconnectGateTest installs a target peer at the dead dial address and,
// when withUpstream is set, a connected chain-selection-eligible upstream so
// the node is not stranded. The gate only drops never-connected discovered or
// public-root peers while eligible upstreams remain. Returns the target peer.
func setupReconnectGateTest(
	pg *PeerGovernor,
	source PeerSource,
	everConnected bool,
	withUpstream bool,
) *Peer {
	target := &Peer{
		Address:           deadDialAddress,
		NormalizedAddress: deadDialAddress,
		Source:            source,
		State:             PeerStateCold,
		EverConnected:     everConnected,
	}
	peers := []*Peer{target}
	if withUpstream {
		peers = append(peers, &Peer{
			Address:           "203.0.113.10:3001",
			NormalizedAddress: "203.0.113.10:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		})
	}
	pg.mu.Lock()
	pg.peers = peers
	pg.mu.Unlock()
	return target
}

func peersContainAddress(peers []*Peer, address string) bool {
	for _, peer := range peers {
		if peer != nil && peer.NormalizedAddress == address {
			return true
		}
	}
	return false
}

// A discovered (peer-share gossip) peer that has never connected must be
// dropped and denied after its first failed dial when other upstreams remain.
func TestCreateOutboundConnection_DropsNeverConnectedGossipPeer(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, false, true)

	go pg.createOutboundConnection(target, false)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			_, denied := pg.denyList[deadDialAddress]
			return !peersContainAddress(pg.peers, deadDialAddress) && denied
		},
		5*time.Second,
		10*time.Millisecond,
		"never-connected gossip peer must be dropped and denied after a failed dial",
	)
}

// A public-root peer that has never connected must likewise be dropped, even
// though it is a topology-sourced peer.
func TestCreateOutboundConnection_DropsNeverConnectedPublicRootPeer(
	t *testing.T,
) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(
		pg,
		PeerSourceTopologyPublicRoot,
		false,
		true,
	)

	go pg.createOutboundConnection(target, false)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			_, denied := pg.denyList[deadDialAddress]
			return !peersContainAddress(pg.peers, deadDialAddress) && denied
		},
		5*time.Second,
		10*time.Millisecond,
		"never-connected public-root peer must be dropped and denied after a failed dial",
	)
}

// A local-root peer that has never connected is trusted and must keep retrying,
// never dropped or denied, even while other upstreams exist.
func TestCreateOutboundConnection_RetainsNeverConnectedLocalRootPeer(
	t *testing.T,
) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(
		pg,
		PeerSourceTopologyLocalRoot,
		false,
		true,
	)

	go pg.createOutboundConnection(target, false)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peersContainAddress(pg.peers, deadDialAddress) &&
			target.ReconnectCount > 0
	}, 5*time.Second, 10*time.Millisecond,
		"never-connected local-root peer must keep retrying, not be dropped")

	pg.mu.Lock()
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.False(
		t,
		denied,
		"local-root peer must never be denied by the never-connected gate",
	)
}

// A discovered peer that connected at least once must keep retrying on a later
// failure; a transient loss is worth recovering. A high failure threshold keeps
// the unrelated fail-fast path from interfering with the assertion.
func TestCreateOutboundConnection_RetainsEverConnectedGossipPeer(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 1000)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, true, true)

	go pg.createOutboundConnection(target, false)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			return peersContainAddress(pg.peers, deadDialAddress) &&
				target.ReconnectCount > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"gossip peer that connected once must keep retrying on a later failure, not be dropped",
	)

	pg.mu.Lock()
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.False(
		t,
		denied,
		"ever-connected gossip peer must not be denied by the never-connected gate",
	)
}

// When the node has no eligible upstream, a never-connected gossip peer is the
// only lead back onto the network and must be kept and retried rather than
// dropped, preserving the anti-stranding emergency redial path.
func TestCreateOutboundConnection_RetainsNeverConnectedGossipPeerWhenNoUpstream(
	t *testing.T,
) {
	pg := newReconnectGateTestGovernor(t, 1000)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, false, false)

	go pg.createOutboundConnection(target, false)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			return peersContainAddress(pg.peers, deadDialAddress) &&
				target.ReconnectCount > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"never-connected gossip peer must be retried when the node has no upstream left",
	)

	pg.mu.Lock()
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.False(
		t,
		denied,
		"last-lead gossip peer must not be denied when no upstream remains",
	)
}

// A peer can gain a client-capable inbound connection while an outbound dial is
// still in flight. If that outbound dial then fails, the never-connected gate
// must not remove the now-healthy upstream.
func TestNeverConnectedDropGate_RetainsCurrentClientConnection(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, false, true)

	pg.mu.Lock()
	target.Connection = &PeerConnection{IsClient: true}
	drop := pg.shouldDropNeverConnectedPeerAfterDialFailureLocked(target)
	pg.mu.Unlock()

	assert.False(
		t,
		drop,
		"current client-capable connection must suppress the never-connected drop",
	)
}

// When no eligible upstream remains, the fail-fast removal of a repeatedly
// failing non-topology peer must not delete the node's last leads: a deleted
// peer is never redialed when its deny entry expires, so the known set would
// stay empty.
func TestCreateOutboundConnection_RetainsFailingLedgerPeerWhenNoUpstream(
	t *testing.T,
) {
	t.Parallel()
	pg := newReconnectGateTestGovernor(t, 1)
	target := setupReconnectGateTest(pg, PeerSourceP2PLedger, true, false)

	go pg.createOutboundConnection(target, false)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			return target.ReconnectCount > 1
		},
		5*time.Second,
		10*time.Millisecond,
		"failed dials must exceed the fail-fast threshold",
	)

	pg.mu.Lock()
	present := peersContainAddress(pg.peers, deadDialAddress)
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.True(
		t,
		present,
		"last-lead ledger peer must stay in the known set when no upstream remains",
	)
	assert.False(
		t,
		denied,
		"last-lead ledger peer must not be denied when no upstream remains",
	)
}

// With another eligible upstream present, the fail-fast removal still applies.
func TestCreateOutboundConnection_RemovesFailingLedgerPeerWhenUpstreamRemains(
	t *testing.T,
) {
	t.Parallel()
	pg := newReconnectGateTestGovernor(t, 1)
	target := setupReconnectGateTest(pg, PeerSourceP2PLedger, true, true)

	go pg.createOutboundConnection(target, false)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			_, denied := pg.denyList[deadDialAddress]
			return !peersContainAddress(pg.peers, deadDialAddress) && denied
		},
		5*time.Second,
		10*time.Millisecond,
		"repeatedly failing ledger peer must be removed while an upstream remains",
	)
}

// TestIsRoutableIP pins the routability policy shared by gossip, ledger, and
// peer-sharing candidates. The accepted cases are as load-bearing as the
// rejected ones: RFC 5737 and RFC 3849 documentation addresses are rejected,
// so a policy regression breaks this test before it reaches the dial path.
func TestIsRoutableIP(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		// Covered by net.IP's own class predicates.
		{"ipv4 public", "44.0.0.1", true},
		{"ipv4 loopback", "127.0.0.1", false},
		{"ipv4 private 10/8", "10.0.0.1", false},
		{"ipv4 private 172.16/12", "172.16.0.1", false},
		{"ipv4 private 192.168/16", "192.168.1.1", false},
		{"ipv4 link-local", "169.254.0.1", false},
		{"ipv4 multicast", "224.0.0.1", false},
		{"ipv4 unspecified", "0.0.0.0", false},
		{"ipv6 public", "2001:4860:4860::8888", true},
		{"ipv6 loopback", "::1", false},
		{"ipv6 unique local", "fd00::1", false},
		{"ipv6 link-local", "fe80::1", false},
		{"ipv6 multicast", "ff02::1", false},
		{"ipv6 unspecified", "::", false},

		// Reported as global unicast by net.IP, rejected here because they
		// reach nothing or reach a host we did not intend.
		{"ipv4 cgnat shared space", "100.64.0.1", false},
		{"ipv4 cgnat upper bound", "100.127.255.255", false},
		{"ipv4 ietf protocol assignments", "192.0.0.1", false},
		// Rejected with the rest of the block although IANA marks these two
		// as globally reachable: PCP anycast (RFC 7723) and TURN anycast
		// (RFC 8155) are never Cardano relays.
		{"ipv4 pcp anycast", "192.0.0.9", false},
		{"ipv4 turn anycast", "192.0.0.10", false},
		{"ipv4 benchmarking", "198.18.0.1", false},
		{"ipv4 reserved future use", "240.0.0.1", false},
		{"ipv4 broadcast", "255.255.255.255", false},
		{"ipv6 discard only", "100::1", false},
		{"ipv4 this network", "0.0.0.1", false},
		{"ipv4 deprecated 6to4 anycast", "192.88.99.1", false},
		{"ipv6 benchmarking", "2001:2::1", false},
		{"ipv6 local-use translation", "64:ff9b:1::1", false},
		{"ipv6 orchid deprecated", "2001:10::1", false},

		// IANA marks these Globally Reachable, so they must stay accepted.
		// They sit next to rejected ranges and would be easy to sweep up.
		{"ipv4 as112", "192.31.196.1", true},
		{"ipv4 amt", "192.52.193.1", true},
		{"ipv6 as112", "2001:4:112::1", true},
		{"ipv6 amt", "2001:3::1", true},
		{"ipv4 translation nat64", "64:ff9b::1", true},

		// Just outside the rejected ranges, to prove the bounds.
		{"ipv4 below cgnat", "100.63.255.255", true},
		{"ipv4 above cgnat", "100.128.0.0", true},
		{"ipv4 below benchmarking", "198.17.255.255", true},
		{"ipv4 above benchmarking", "198.20.0.0", true},
		{"ipv4 below reserved", "239.255.255.255", false}, // multicast
		{"ipv4 outside protocol assignments", "192.0.1.1", true},
		{"ipv4 top of this network", "0.255.255.255", false},
		{"ipv4 above this network", "1.0.0.0", true},
		{"ipv4 below 6to4 anycast", "192.88.98.255", true},
		{"ipv4 top of 6to4 anycast", "192.88.99.255", false},
		{"ipv4 above 6to4 anycast", "192.88.100.0", true},
		{
			"ipv6 below benchmarking",
			"2001:1:ffff:ffff:ffff:ffff:ffff:ffff",
			true,
		},
		{
			"ipv6 top of benchmarking",
			"2001:2:0:ffff:ffff:ffff:ffff:ffff",
			false,
		},
		{"ipv6 above benchmarking", "2001:2:1::", true},
		{"ipv6 above local-use translation", "64:ff9b:2::", true},
		{
			"ipv6 below orchid",
			"2001:f:ffff:ffff:ffff:ffff:ffff:ffff",
			true,
		},
		{
			"ipv6 top of orchid",
			"2001:1f:ffff:ffff:ffff:ffff:ffff:ffff",
			false,
		},
		// ORCHIDv2 abuts the deprecated ORCHID block and is Globally Reachable
		// in the IANA registry, but RFC 7343 keeps it out of IPv6 headers, so
		// it is rejected as a peer candidate and the /28 boundaries are pinned
		// on both sides.
		{"ipv6 orchidv2", "2001:20::1", false},
		{
			"ipv6 orchidv2 upper bound",
			"2001:2f:ffff:ffff:ffff:ffff:ffff:ffff",
			false,
		},
		{"ipv6 above orchidv2", "2001:30::", true},

		// Documentation-only ranges are not valid peer candidates.
		{"ipv4 test-net-1", "192.0.2.1", false},
		{"ipv4 test-net-2", "198.51.100.1", false},
		{"ipv4 test-net-3", "203.0.113.1", false},
		{"ipv6 documentation", "2001:db8::1", false},

		// The policy must stop at each documentation prefix boundary.
		{"just below test-net-1", "192.0.1.255", true},
		{"just above test-net-1", "192.0.3.0", true},
		{"just below test-net-2", "198.51.99.255", true},
		{"just above test-net-2", "198.51.101.0", true},
		{"just below test-net-3", "203.0.112.255", true},
		{"just above test-net-3", "203.0.114.0", true},
		{
			"just below documentation",
			"2001:db7:ffff:ffff:ffff:ffff:ffff:ffff",
			true,
		},
		{"just above documentation", "2001:db9::1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "test case address must parse")
			require.Equal(t, tt.want, IsRoutableIP(ip))
		})
	}
}

// TestIsRoutableIPMalformed covers values that never come off the wire but
// would read as routable if the length were not checked: every net.IP class
// predicate reports false for a length other than 4 or 16.
func TestIsRoutableIPMalformed(t *testing.T) {
	require.False(t, IsRoutableIP(nil))
	require.False(t, IsRoutableIP(net.IP{}))
	require.False(t, IsRoutableIP(net.IP{1, 2, 3}))
	require.False(t, IsRoutableIP(make(net.IP, 5)))
}

// TestIsRoutableIPUnmapsV4 verifies that an IPv4-mapped IPv6 address is
// matched against the IPv4 prefixes. Without the unmap it would miss every
// one of them and be accepted.
func TestIsRoutableIPUnmapsV4(t *testing.T) {
	mapped := net.ParseIP("::ffff:100.64.0.1")
	require.NotNil(t, mapped)
	require.Len(t, mapped, net.IPv6len, "want the 16-byte mapped form")
	require.False(t, IsRoutableIP(mapped))
}
