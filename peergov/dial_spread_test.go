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
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			pg.resolveDialAddress(context.Background(), "relay.example.com:3001"),
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
		got := pg.resolveDialAddress(context.Background(), "leios-node.play.dev.cardano.org:3001")
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
		got := pg.resolveDialAddress(context.Background(), "relay.example.com:3001")
		assert.Truef(t, dialTargetIsV4(t, got), "expected IPv4 target, got %q", got)
	}
}

// TestResolveDialAddress_V6OnlyHostFiltersV4 verifies the symmetric case: on a
// v6-only host every selected target is IPv6.
func TestResolveDialAddress_V6OnlyHostFiltersV4(t *testing.T) {
	mixedFamilyResolver(t, mixedV4V6Records)
	setLocalAddrFamilies(t, false, true)

	pg := newDialSpreadGovernor()
	for range 100 {
		got := pg.resolveDialAddress(context.Background(), "relay.example.com:3001")
		assert.Falsef(t, dialTargetIsV4(t, got), "expected IPv6 target, got %q", got)
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
		got := pg.resolveDialAddress(context.Background(), "relay.example.com:3001")
		if dialTargetIsV4(t, got) {
			sawV4 = true
		} else {
			sawV6 = true
		}
	}
	assert.True(t, sawV4, "expected at least one IPv4 target on dual-stack host")
	assert.True(t, sawV6, "expected at least one IPv6 target on dual-stack host")
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
		got := pg.resolveDialAddress(context.Background(), "relay.example.com:3001")
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
		got := pg.resolveDialAddress(context.Background(), "relay.example.com:3001")
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
