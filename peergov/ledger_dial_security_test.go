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
