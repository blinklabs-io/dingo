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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolveLedgerDialTarget_LockedInIPUnaffectedByRebindAttempt is the core
// regression test for issue #2435: once a ledger relay hostname has been
// resolved (peer.NormalizedAddress is already an IP), resolveLedgerDialTarget
// must return that same IP without consulting the resolver again. A
// compromised authoritative DNS server that answers a second lookup with an
// internal address must have no effect, because no second lookup happens.
func TestResolveLedgerDialTarget_LockedInIPUnaffectedByRebindAttempt(t *testing.T) {
	calls := 0
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls++
		// If this were consulted, it would rebind to an internal address.
		return []net.IP{net.ParseIP("10.0.0.5")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	peer := &Peer{
		Address:           "relay.example.com:3001",
		NormalizedAddress: "198.51.100.7:3001", // resolved at discovery time
		Source:            PeerSourceP2PLedger,
	}

	got, err := pg.resolveLedgerDialTarget(context.Background(), peer)
	require.NoError(t, err)
	assert.Equal(t, "198.51.100.7:3001", got)
	assert.Equal(t, 0, calls, "a locked-in IP must not trigger a fresh DNS lookup")
	assert.Equal(t, "198.51.100.7:3001", peer.NormalizedAddress)
}

// TestResolveLedgerDialTarget_LocksInResolutionOnFallbackHostname verifies the
// fallback path: if discovery-time resolution failed and NormalizedAddress is
// still a hostname, the first dial attempt resolves once, checks routability,
// and writes the resolved IP back so every later attempt reuses it instead of
// resolving again.
func TestResolveLedgerDialTarget_LocksInResolutionOnFallbackHostname(t *testing.T) {
	calls := 0
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls++
		return []net.IP{net.ParseIP("203.0.113.5")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	peer := &Peer{
		Address:           "relay2.example.com:3001",
		NormalizedAddress: "relay2.example.com:3001", // discovery resolution had failed
		Source:            PeerSourceP2PLedger,
	}
	pg.peers = []*Peer{peer}

	got, err := pg.resolveLedgerDialTarget(context.Background(), peer)
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.5:3001", got)
	assert.Equal(t, 1, calls)
	assert.Equal(t, "203.0.113.5:3001", peer.NormalizedAddress)

	// Change the resolver to return a different address entirely. A second
	// call must still return the locked-in result without consulting it.
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls++
		return []net.IP{net.ParseIP("10.9.9.9")}, nil
	}
	got, err = pg.resolveLedgerDialTarget(context.Background(), peer)
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.5:3001", got)
	assert.Equal(t, 1, calls, "resolution must not happen again once locked in")
}

// TestResolveLedgerDialTarget_RejectsUnroutableResolutionAndDoesNotLockIn
// verifies that when the fallback resolution (discovery-time lookup failed)
// yields a non-routable IP, the dial is refused rather than silently probing
// a private address, and the hostname is left un-locked so a later attempt
// gets a fresh chance to resolve to something routable.
func TestResolveLedgerDialTarget_RejectsUnroutableResolutionAndDoesNotLockIn(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.1.2.3")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	peer := &Peer{
		Address:           "relay3.example.com:3001",
		NormalizedAddress: "relay3.example.com:3001",
		Source:            PeerSourceP2PLedger,
	}
	pg.peers = []*Peer{peer}

	_, err := pg.resolveLedgerDialTarget(context.Background(), peer)
	require.ErrorIs(t, err, ErrUnroutableAddress)
	assert.Equal(
		t,
		"relay3.example.com:3001",
		peer.NormalizedAddress,
		"an unroutable resolution must not be locked in",
	)
}

// TestResolveLedgerDialTarget_ResolutionFailurePropagatesError verifies a
// resolver error on the fallback path is surfaced (so the caller applies its
// normal dial-failure backoff) instead of falling back to dialing the raw
// hostname, which would hand resolution back to the OS resolver unchecked.
func TestResolveLedgerDialTarget_ResolutionFailurePropagatesError(t *testing.T) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return nil, errors.New("lookup failed")
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	peer := &Peer{
		Address:           "relay4.example.com:3001",
		NormalizedAddress: "relay4.example.com:3001",
		Source:            PeerSourceP2PLedger,
	}

	_, err := pg.resolveLedgerDialTarget(context.Background(), peer)
	assert.Error(t, err)
}

// TestCreateOutboundConnection_LedgerPeerDialsLockedIPNotRebindTarget is an
// end-to-end check that the outbound dial path for a ledger-sourced peer
// actually calls resolveLedgerDialTarget (and not the generic
// resolveDialAddress, which re-resolves on every attempt). A peer whose
// NormalizedAddress is already locked to a dead loopback IP must have its
// dial fail against that IP; the injected resolver — which would answer with
// a completely different address — must never be consulted.
func TestCreateOutboundConnection_LedgerPeerDialsLockedIPNotRebindTarget(t *testing.T) {
	calls := 0
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls++
		return []net.IP{net.ParseIP("203.0.113.9")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   logger,
		EventBus: newMockEventBus(),
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
		DenyDuration: 30 * time.Minute,
	})
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.mu.Unlock()
	t.Cleanup(pg.Stop)

	target := &Peer{
		Address:           "relay.example.com:1",
		NormalizedAddress: deadDialAddress, // locked-in resolved IP, nothing listening
		Source:            PeerSourceP2PLedger,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	// An eligible upstream present makes the never-connected-peer drop gate
	// fire on the very first failed dial, so the test does not have to wait
	// out the (much slower) excessive-failures backoff path.
	pg.peers = []*Peer{
		target,
		{
			Address:           "203.0.113.10:3001",
			NormalizedAddress: "203.0.113.10:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		},
	}
	pg.mu.Unlock()

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		_, denied := pg.denyList[deadDialAddress]
		return denied
	}, 5*time.Second, 10*time.Millisecond,
		"failed dial to the locked-in address must still drive the normal deny path")

	assert.Equal(
		t,
		0,
		calls,
		"ledger peer with an already-resolved NormalizedAddress must not re-resolve at dial time",
	)
}

// safeLogBuffer is a mutex-guarded io.Writer/String() pair. slog's built-in
// handlers serialize their own writes, but that does not protect a test
// goroutine reading the buffer concurrently with a background dial
// goroutine's writes; bytes.Buffer itself is not safe for concurrent use.
type safeLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestCreateOutboundConnection_LedgerPeerFallbackDialsExactRoutabilityCheckedIP
// closes a coverage gap: TestCreateOutboundConnection_LedgerPeerDialsLockedIPNotRebindTarget
// starts from an already-locked IP, so it never actually calls isRoutableAddr.
// This test drives a peer through the FALLBACK path instead — discovery-time
// DNS failed, NormalizedAddress is still a hostname — through the real
// ConnectionManager, and asserts that the address the connection manager logs
// immediately before its OS-level dial call is the exact IP that
// resolveLedgerDialTarget just resolved and passed through isRoutableAddr.
// There is no separate "checked" value and "dialed" value to drift apart:
// they are the same string, returned once and handed straight to the dialer.
func TestCreateOutboundConnection_LedgerPeerFallbackDialsExactRoutabilityCheckedIP(
	t *testing.T,
) {
	const resolvedIP = "203.0.113.77"
	const resolvedAddr = resolvedIP + ":3001"

	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP(resolvedIP)}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	logBuf := &safeLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(
		logBuf,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))

	// A cancelable (not t.Context()) base context: the assertion below only
	// needs the pre-dial log line, which is written synchronously before the
	// OS dial call, so the in-flight dial (which may otherwise run for the
	// connection manager's 10s timeout against an address nothing answers
	// on) is aborted immediately afterward instead of being waited out.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   logger,
		EventBus: newMockEventBus(),
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
		DenyDuration: 30 * time.Minute,
	})
	pg.mu.Lock()
	pg.ctx = ctx
	pg.stopCh = make(chan struct{})
	pg.mu.Unlock()
	t.Cleanup(pg.Stop)

	target := &Peer{
		Address:           "relay5.example.com:3001",
		NormalizedAddress: "relay5.example.com:3001", // discovery-time DNS had failed
		Source:            PeerSourceP2PLedger,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{target}
	pg.mu.Unlock()

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		return strings.Contains(
			logBuf.String(),
			"establishing TCP connection to: "+resolvedAddr,
		)
	}, 5*time.Second, 10*time.Millisecond,
		"the connection manager must dial exactly the IP resolveLedgerDialTarget "+
			"resolved and routability-checked, not a separately re-resolved value")

	cancel()
}

// TestResolveLedgerDialTarget_FallbackPrefersLocallySupportedFamily verifies
// that a dual-stack relay hostname whose discovery-time resolution failed
// locks in a record the local host can actually dial. Unlike
// resolveDialAddress, this resolution never runs again for the peer's
// lifetime, so pinning to the first (possibly unusable) DNS answer would
// strand a v4-only host on an IPv6 record forever.
func TestResolveLedgerDialTarget_FallbackPrefersLocallySupportedFamily(t *testing.T) {
	setLocalAddrFamilies(t, true, false) // v4-only host

	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		// AAAA record ordered first, mirroring real-world DNS answers that
		// are not sorted by local reachability.
		return []net.IP{
			net.ParseIP("2001:db8::1"),
			net.ParseIP("198.51.100.9"),
		}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	peer := &Peer{
		Address:           "relay6.example.com:3001",
		NormalizedAddress: "relay6.example.com:3001",
		Source:            PeerSourceP2PLedger,
	}
	pg.peers = []*Peer{peer}

	got, err := pg.resolveLedgerDialTarget(context.Background(), peer)
	require.NoError(t, err)
	assert.Equal(
		t,
		"198.51.100.9:3001",
		got,
		"must lock in the IPv4 record on a v4-only host, not the first (IPv6) DNS answer",
	)
	assert.Equal(t, "198.51.100.9:3001", peer.NormalizedAddress)
}

// TestResolveLedgerDialTarget_DoesNotStealNormalizedAddressFromExistingPeer
// verifies a peer-identity guard: if a ledger peer's fallback resolution
// lands on the same IP:port that a distinct, existing peer entry already
// uses as its NormalizedAddress (e.g. the same relay already known via
// gossip or topology), the resolution must not be written back.
// peerIndexByAddress assumes NormalizedAddress uniquely identifies a peer;
// silently duplicating it would let a lookup keyed on that IP return the
// wrong peer object and misdirect this peer's connection bookkeeping onto
// it — potentially leaving this peer stuck endlessly reconnecting.
func TestResolveLedgerDialTarget_DoesNotStealNormalizedAddressFromExistingPeer(
	t *testing.T,
) {
	oldLookupIPAddr := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("198.51.100.42")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookupIPAddr })

	pg := newDialSpreadGovernor()
	existing := &Peer{
		Address:           "198.51.100.42:3001",
		NormalizedAddress: "198.51.100.42:3001",
		Source:            PeerSourceP2PGossip,
	}
	ledgerPeer := &Peer{
		Address:           "relay7.example.com:3001",
		NormalizedAddress: "relay7.example.com:3001", // discovery resolution had failed
		Source:            PeerSourceP2PLedger,
	}
	pg.peers = []*Peer{existing, ledgerPeer}

	got, err := pg.resolveLedgerDialTarget(context.Background(), ledgerPeer)
	require.NoError(t, err)
	assert.Equal(
		t,
		"198.51.100.42:3001",
		got,
		"this attempt must still dial the resolved, routability-checked IP",
	)
	assert.Equal(
		t,
		"relay7.example.com:3001",
		ledgerPeer.NormalizedAddress,
		"must not overwrite NormalizedAddress onto a value another peer already owns",
	)
	assert.Same(
		t,
		existing,
		pg.peers[0],
		"existing peer entry must be untouched",
	)
	assert.Equal(t, "198.51.100.42:3001", existing.NormalizedAddress)
}

// TestAddLedgerPeerPrefersLocallySupportedFamily verifies the common
// discovery-time path, not just the resolveLedgerDialTarget fallback: when
// addLedgerPeer resolves a relay hostname with mixed A/AAAA records, the
// record it stores in NormalizedAddress must be one the local host can
// actually dial. Without this, a v4-only host would lock onto whatever
// record DNS happens to answer first — here an AAAA record — and
// resolveLedgerDialTarget's fast path would then dial that unreachable
// family for the peer's entire lifetime, since (unlike resolveDialAddress)
// nothing ever re-resolves it.
func TestAddLedgerPeerPrefersLocallySupportedFamily(t *testing.T) {
	setLocalAddrFamilies(t, true, false) // v4-only host

	oldLookupIP := lookupIP
	lookupIP = func(string) ([]net.IP, error) {
		return []net.IP{
			net.ParseIP("2001:db8::1"), // AAAA returned first
			net.ParseIP("198.51.100.9"),
		}, nil
	}
	t.Cleanup(func() { lookupIP = oldLookupIP })

	pg := newDialSpreadGovernor()
	require.True(t, pg.addLedgerPeer("relay.example.com:3001"))
	require.Len(t, pg.peers, 1)
	assert.Equal(
		t,
		"198.51.100.9:3001",
		pg.peers[0].NormalizedAddress,
		"must store the IPv4 record on a v4-only host, not the first (IPv6) DNS answer",
	)

	got, err := pg.resolveLedgerDialTarget(t.Context(), pg.peers[0])
	require.NoError(t, err)
	assert.Equal(t, "198.51.100.9:3001", got)
}
