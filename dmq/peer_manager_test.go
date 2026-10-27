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

package dmq

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// dialLog records the addresses a manager dials. A guarded dial goes to the
// real address its fake one is mapped to, and is refused when it has none.
type dialLog struct {
	mu       sync.Mutex
	plain    []string
	guarded  []string
	redirect map[string]string
}

func (d *dialLog) dial(
	ctx context.Context,
	record *[]string,
	address, target string,
) (net.Conn, error) {
	d.mu.Lock()
	*record = append(*record, address)
	d.mu.Unlock()
	if target == "" {
		return nil, errors.New("test: address is not routable")
	}
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", target)
}

func (d *dialLog) plainDial(
	ctx context.Context, address string,
) (net.Conn, error) {
	return d.dial(ctx, &d.plain, address, address)
}

func (d *dialLog) guardedDial(
	ctx context.Context, address string,
) (net.Conn, error) {
	d.mu.Lock()
	target := d.redirect[address]
	d.mu.Unlock()
	return d.dial(ctx, &d.guarded, address, target)
}

func (d *dialLog) plainCount(address string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, a := range d.plain {
		if a == address {
			n++
		}
	}
	return n
}

func (d *dialLog) totals() (plain, guarded int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.plain), len(d.guarded)
}

// managed is a stack whose manager dials through a dialLog, with short waits
// so that a test does not sit through the production ones.
//
// redirect maps the addresses a guarded dial is asked for to real ones. It is
// fixed before the stack starts, so the manager never reads it while a test
// writes it.
func managed(
	t *testing.T,
	cfg PeerConfig,
	redirect ...map[string]string,
) (*Stack, *dialLog, *prometheus.Registry) {
	t.Helper()
	log := &dialLog{redirect: make(map[string]string)}
	for _, r := range redirect {
		maps.Copy(log.redirect, r)
	}
	if cfg.MaxPeers == 0 {
		cfg.MaxPeers = 10
	}
	cfg.ReconcileInterval = 20 * time.Millisecond
	cfg.BackoffBase = 10 * time.Millisecond
	cfg.BackoffMax = 50 * time.Millisecond
	cfg.ledgerRetry = 10 * time.Millisecond
	cfg.dialPlain = log.plainDial
	cfg.dialGuarded = log.guardedDial
	cfg.ListenAddrs = append(
		[]ListenAddr{{Address: loopbackAny}}, cfg.ListenAddrs...,
	)
	s, reg := newTestStack(t, StackConfig{Peers: &cfg})
	return s, log, reg
}

// settle runs a review and waits for the dials it started to finish.
func settle(t *testing.T, s *Stack) {
	t.Helper()
	m := s.peerNet.manager
	m.reconcile(t.Context())
	waitFor(t, "dials finish", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inflight == 0
	})
}

func group(valency uint, peers ...Peer) RootGroup {
	return RootGroup{Peers: peers, Valency: valency}
}

func TestPeerManagerKeepsALocalRootAtItsValency(t *testing.T) {
	t.Parallel()
	r1, _ := newListeningStack(t, PeerConfig{})
	r2, _ := newListeningStack(t, PeerConfig{})
	r3, _ := newListeningStack(t, PeerConfig{})
	p1, p2, p3 := listenPeer(t, r1), listenPeer(t, r2), listenPeer(t, r3)

	m, log, _ := managed(t, PeerConfig{Topology: &Topology{
		LocalRoots: []RootGroup{group(2, p1, p2, p3)},
	}})
	waitFor(t, "two peers", func() bool { return m.PeerCount() == 2 })

	for range 3 {
		settle(t, m)
	}
	assert.Equal(t, 2, m.PeerCount(), "the valency is a target, not a floor")
	plain, _ := log.totals()
	assert.Equal(t, 2, plain)
	assert.Equal(t, 1, log.plainCount(p1.Address()), "roots are taken in order")
	assert.Equal(t, 1, log.plainCount(p2.Address()))
	assert.Equal(t, 0, log.plainCount(p3.Address()))
}

func TestPeerManagerFillsToTheMinimumFromPublicRoots(t *testing.T) {
	t.Parallel()
	var peers []Peer
	for range 4 {
		r, _ := newListeningStack(t, PeerConfig{})
		peers = append(peers, listenPeer(t, r))
	}
	m, log, _ := managed(t, PeerConfig{
		MinPeers: 2,
		Topology: &Topology{PublicRoots: []RootGroup{group(0, peers...)}},
	})
	waitFor(t, "two peers", func() bool { return m.PeerCount() == 2 })
	for range 3 {
		settle(t, m)
	}
	assert.Equal(t, 2, m.PeerCount())
	plain, guarded := log.totals()
	assert.Equal(t, 2, plain)
	assert.Zero(t, guarded)
}

// A connection a peer opened to us is a peer like any other, so it is not
// replaced by one we dial.
func TestPeerManagerCountsInboundPeersTowardTheMinimum(t *testing.T) {
	t.Parallel()
	remote, _ := newListeningStack(t, PeerConfig{})
	release := make(chan struct{})
	var lookups atomic.Int32
	m, log, _ := managed(t, PeerConfig{
		MinPeers: 1,
		LedgerPeers: func(ctx context.Context) ([]Peer, error) {
			lookups.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []Peer{{Host: "93.184.216.34", Port: 4001}}, nil
		},
	}, map[string]string{
		"93.184.216.34:4001": listenPeer(t, remote).Address(),
	})

	waitFor(t, "the lookup starts", func() bool { return lookups.Load() > 0 })
	inbound, _ := newTestStack(t, StackConfig{})
	dialStack(t, inbound, m)
	waitFor(t, "the inbound peer", func() bool { return m.PeerCount() == 1 })

	close(release)
	settle(t, m)
	_, guarded := log.totals()
	assert.Zero(t, guarded, "nothing is dialed once an inbound peer fills it")
	assert.Equal(t, 1, m.PeerCount())
}

func TestPeerManagerFallsBackFromAPeerThatFails(t *testing.T) {
	t.Parallel()
	good, _ := newListeningStack(t, PeerConfig{})
	bad := Peer{Host: "127.0.0.1", Port: 1}
	m, log, reg := managed(t, PeerConfig{Topology: &Topology{
		LocalRoots: []RootGroup{group(1, bad, listenPeer(t, good))},
	}})
	waitFor(t, "the working peer", func() bool { return m.PeerCount() == 1 })
	assert.GreaterOrEqual(t, log.plainCount(bad.Address()), 1)
	waitFor(t, "the failure is counted", func() bool {
		return metricValue(
			t, reg, "dingo_dmq_peer_dials_total", "failure",
		) >= 1
	})
}

func TestPeerManagerRetriesAFailedPeerAfterABackoff(t *testing.T) {
	t.Parallel()
	bad := Peer{Host: "127.0.0.1", Port: 1}
	_, log, _ := managed(t, PeerConfig{Topology: &Topology{
		LocalRoots: []RootGroup{group(1, bad)},
	}})
	waitFor(t, "repeated attempts", func() bool {
		return log.plainCount(bad.Address()) >= 3
	})
}

func TestPeerManagerNeverExceedsMaxPeers(t *testing.T) {
	t.Parallel()
	var peers []Peer
	for range 3 {
		r, _ := newListeningStack(t, PeerConfig{})
		peers = append(peers, listenPeer(t, r))
	}
	m, log, _ := managed(t, PeerConfig{
		MinPeers: 2, MaxPeers: 2,
		Topology: &Topology{LocalRoots: []RootGroup{group(3, peers...)}},
	})
	waitFor(t, "two peers", func() bool { return m.PeerCount() == 2 })
	for range 3 {
		settle(t, m)
	}
	assert.Equal(t, 2, m.PeerCount())
	plain, _ := log.totals()
	assert.Equal(t, 2, plain, "the third root is never dialed")
}

func TestPeerManagerDoesNotDialItself(t *testing.T) {
	t.Parallel()
	probe, err := net.Listen("tcp4", loopbackAny)
	require.NoError(t, err)
	port := probe.Addr().(*net.TCPAddr).Port
	require.NoError(t, probe.Close())
	self := Peer{Host: "127.0.0.1", Port: uint(port)}
	other, _ := newListeningStack(t, PeerConfig{})

	m, log, _ := managed(t, PeerConfig{
		ListenAddrs: []ListenAddr{{Address: self.Address()}},
		Topology: &Topology{
			LocalRoots: []RootGroup{group(2, self, listenPeer(t, other))},
		},
	})
	waitFor(t, "the other peer", func() bool { return m.PeerCount() == 1 })
	settle(t, m)
	assert.Zero(t, log.plainCount(self.Address()))
	assert.Equal(t, 1, m.PeerCount())
}

func TestPeerManagerDialsLedgerPeersOnlyThroughTheGuard(t *testing.T) {
	t.Parallel()
	remote, _ := newListeningStack(t, PeerConfig{})
	var lookups atomic.Int32
	m, log, _ := managed(t, PeerConfig{
		MinPeers: 1,
		LedgerPeers: func(context.Context) ([]Peer, error) {
			lookups.Add(1)
			return []Peer{
				{Host: "127.0.0.1", Port: 4001},
				{Host: "10.0.0.5", Port: 4001},
				{Host: "localhost", Port: 4001},
				{Host: "pool.localhost", Port: 4001},
				{Host: "93.184.216.34", Port: 4001},
			}, nil
		},
	}, map[string]string{
		"93.184.216.34:4001": listenPeer(t, remote).Address(),
	})

	waitFor(t, "the public peer", func() bool { return m.PeerCount() == 1 })
	settle(t, m)
	plain, guarded := log.totals()
	assert.Zero(t, plain, "ledger peers never take the plain dial")
	assert.Equal(t, 1, guarded, "only the routable peer is dialed")
	assert.Equal(t, int32(1), lookups.Load(), "the lookup is cached")
}

func TestPeerManagerRetriesALedgerLookupThatFails(t *testing.T) {
	t.Parallel()
	remote, _ := newListeningStack(t, PeerConfig{})
	var calls atomic.Int32
	m, _, _ := managed(t, PeerConfig{
		MinPeers: 1,
		LedgerPeers: func(context.Context) ([]Peer, error) {
			if calls.Add(1) < 3 {
				return nil, ErrChainStateUnavailable
			}
			return []Peer{{Host: "93.184.216.34", Port: 4001}}, nil
		},
	}, map[string]string{
		"93.184.216.34:4001": listenPeer(t, remote).Address(),
	})
	waitFor(t, "the peer once the ledger answers", func() bool {
		return m.PeerCount() == 1
	})
	assert.GreaterOrEqual(t, calls.Load(), int32(3))
}

func TestPeerManagerRedialsAPeerThatDrops(t *testing.T) {
	t.Parallel()
	remote, _ := newListeningStack(t, PeerConfig{})
	root := listenPeer(t, remote)
	m, log, reg := managed(t, PeerConfig{Topology: &Topology{
		LocalRoots: []RootGroup{group(1, root)},
	}})
	waitFor(t, "connected", func() bool { return m.PeerCount() == 1 })

	var first *PeerConn
	m.peerMu.Lock()
	for p := range m.peers {
		first = p
	}
	m.peerMu.Unlock()
	require.NotNil(t, first)
	first.Close()

	waitFor(t, "redialed", func() bool {
		return metricValue(
			t, reg, "dingo_dmq_peer_dials_total", "success",
		) >= 2 && m.PeerCount() == 1
	})
	assert.GreaterOrEqual(t, log.plainCount(root.Address()), 2)
}

// Stop cancels a dial that is still waiting, and waits for it.
func TestPeerManagerStopEndsADialInProgress(t *testing.T) {
	t.Parallel()
	started := make(chan struct{}, 1)
	cfg := PeerConfig{
		MaxPeers: 2,
		Topology: &Topology{
			LocalRoots: []RootGroup{group(1, Peer{Host: "192.0.2.1", Port: 1})},
		},
		DialTimeout: time.Hour,
		dialPlain: func(ctx context.Context, _ string) (net.Conn, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	s, err := NewStack(StackConfig{
		SocketPath:    shortSocketPath(t),
		Authenticator: newTestAuthenticator(),
		Peers:         &cfg,
	})
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	<-started

	done := make(chan error, 1)
	go func() { done <- s.Stop(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(peerWait):
		require.FailNow(t, "Stop did not return while a dial was in progress")
	}
}

func TestBackoffDoublesToItsMaximumWithJitter(t *testing.T) {
	t.Parallel()
	m := &peerManager{
		cfg: &PeerConfig{BackoffBase: time.Second, BackoffMax: 8 * time.Second},
		rng: rand.New(rand.NewPCG(1, 2)),
	}
	for failures, nominal := range map[int]time.Duration{
		1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second,
		4: 8 * time.Second, 5: 8 * time.Second, 9: 8 * time.Second,
	} {
		for range 20 {
			got := m.backoffLocked(failures)
			assert.GreaterOrEqual(t, got, nominal*8/10, "failures=%d", failures)
			assert.LessOrEqual(t, got, nominal*12/10, "failures=%d", failures)
		}
	}
}

func TestPublicPeersDropsWhatTheLedgerMustNotMakeUsDial(t *testing.T) {
	t.Parallel()
	got := publicPeers([]Peer{
		{Host: "127.0.0.1", Port: 1},
		{Host: "::1", Port: 1},
		{Host: "[::1]", Port: 1},
		{Host: "10.1.2.3", Port: 1},
		{Host: "192.168.0.1", Port: 1},
		{Host: "169.254.169.254", Port: 1},
		{Host: "fe80::1", Port: 1},
		{Host: "0.0.0.0", Port: 1},
		{Host: "localhost", Port: 1},
		{Host: "LocalHost.", Port: 1},
		{Host: "db.localhost", Port: 1},
		{Host: "93.184.216.34", Port: 4001},
		{Host: "2606:2800:220:1:248:1893:25c8:1946", Port: 4001},
		{Host: "relay.example.org", Port: 4001},
	})
	assert.Equal(t, []Peer{
		{Host: "93.184.216.34", Port: 4001},
		{Host: "2606:2800:220:1:248:1893:25c8:1946", Port: 4001},
		{Host: "relay.example.org", Port: 4001},
	}, got)
}

func TestSelfCheckMatchesOnlyOurOwnPorts(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp4", loopbackAny)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	port := uint(ln.Addr().(*net.TCPAddr).Port)
	c := newSelfCheck([]net.Listener{ln})

	assert.True(t, c.is(Peer{Host: "127.0.0.1", Port: port}))
	assert.True(t, c.is(Peer{Host: "[::1]", Port: port}))
	assert.True(t, c.is(Peer{Host: "0.0.0.0", Port: port}))
	assert.False(t, c.is(Peer{Host: "127.0.0.1", Port: port + 1}),
		"another port is another service")
	assert.False(t, c.is(Peer{Host: "93.184.216.34", Port: port}))
	assert.False(t, c.is(Peer{Host: "relay.example.org", Port: port}),
		"a name is not resolved")
	for addr := range c.ips {
		if addr.IsLoopback() || addr.IsUnspecified() || !addr.IsValid() {
			continue
		}
		assert.True(t, c.is(Peer{Host: addr.String(), Port: port}))
		break
	}
}

// The dial a stack uses for peers the ledger offered refuses the addresses it
// must not reach, whatever the ledger said.
func TestDefaultGuardedDialRefusesLoopbackAndPrivateAddresses(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp4", loopbackAny)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	s, _ := newTestStack(t, StackConfig{Peers: &PeerConfig{MaxPeers: 1}})
	cfg := s.peerNet.manager.cfg
	for _, address := range []string{
		ln.Addr().String(), "10.1.2.3:4001", "localhost:4001",
	} {
		conn, err := cfg.dialGuarded(t.Context(), address)
		if conn != nil {
			_ = conn.Close()
		}
		require.Error(t, err, address)
	}
	conn, err := cfg.dialPlain(t.Context(), ln.Addr().String())
	require.NoError(t, err, "a configured peer may be on loopback")
	_ = conn.Close()
}

func TestPeerManagerForgetsPeersItHasNotTriedForLong(t *testing.T) {
	t.Parallel()
	now := time.Now()
	m := &peerManager{
		cfg: &PeerConfig{BackoffMax: time.Minute},
		entries: map[string]*peerEntry{
			"stale":   {retryAt: now.Add(-3 * time.Minute)},
			"waiting": {retryAt: now.Add(time.Minute)},
			"recent":  {retryAt: now.Add(-10 * time.Second)},
			"busy":    {retryAt: now.Add(-time.Hour), dialing: true},
		},
	}
	m.prune(now)
	assert.NotContains(t, m.entries, "stale")
	assert.Contains(t, m.entries, "waiting")
	assert.Contains(t, m.entries, "recent")
	assert.Contains(t, m.entries, "busy")
}

// However many peers are wanted, only a few connections are attempted at once.
func TestPeerManagerBoundsConcurrentDials(t *testing.T) {
	t.Parallel()
	var peers []Peer
	for i := range 3 * maxConcurrentDials {
		peers = append(peers, Peer{Host: "192.0.2.1", Port: uint(1000 + i)})
	}
	var active, highest atomic.Int32
	release := make(chan struct{})
	cfg := PeerConfig{
		MaxPeers: len(peers), MinPeers: len(peers),
		Topology: &Topology{PublicRoots: []RootGroup{group(0, peers...)}},
		dialPlain: func(ctx context.Context, _ string) (net.Conn, error) {
			now := active.Add(1)
			for {
				seen := highest.Load()
				if now <= seen || highest.CompareAndSwap(seen, now) {
					break
				}
			}
			defer active.Add(-1)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, errors.New("test: not connected")
		},
		ReconcileInterval: 10 * time.Millisecond,
		BackoffBase:       time.Hour,
		BackoffMax:        time.Hour,
	}
	stack, _ := newTestStack(t, StackConfig{Peers: &cfg})
	waitFor(t, "the dials fill their limit", func() bool {
		return active.Load() == int32(maxConcurrentDials)
	})
	// Reviews go on while the dials wait; none starts another.
	for range 20 {
		stack.peerNet.manager.reconcile(t.Context())
	}
	assert.Equal(t, int32(maxConcurrentDials), highest.Load())
	close(release)
}

// Among many peers the manager settles at the limit, and replaces the peers it
// loses from the others.
func TestPeerManagerSettlesAmongManyPeersAndReplacesLostOnes(t *testing.T) {
	t.Parallel()
	const remotes, limit = 24, 10
	stacks := make([]*Stack, 0, remotes)
	peers := make([]Peer, 0, remotes)
	for range remotes {
		r, _ := newListeningStack(t, PeerConfig{})
		stacks = append(stacks, r)
		peers = append(peers, listenPeer(t, r))
	}
	m, log, _ := managed(t, PeerConfig{
		MinPeers: limit, MaxPeers: limit,
		Topology: &Topology{PublicRoots: []RootGroup{group(0, peers...)}},
	})
	waitFor(t, "the limit is reached", func() bool {
		return m.PeerCount() == limit
	})
	for range 5 {
		settle(t, m)
	}
	require.Equal(t, limit, m.PeerCount(), "the limit is never passed")

	// Lose three of the peers it holds.
	lost := 0
	for _, r := range stacks {
		if lost == 3 {
			break
		}
		if r.PeerCount() == 1 {
			require.NoError(t, r.Stop(t.Context()))
			lost++
		}
	}
	require.Equal(t, 3, lost)
	waitFor(t, "the lost peers are replaced", func() bool {
		return m.PeerCount() == limit &&
			connectedCount(m.peerNet.manager) == limit
	})
	for range 5 {
		settle(t, m)
	}
	assert.Equal(t, limit, m.PeerCount())
	plain, _ := log.totals()
	assert.Greater(t, plain, limit, "replacements were dialed")
}

// Stopping a stack with live peers, a dial in progress and a ledger lookup
// leaves no goroutine behind.
//
// Not t.Parallel: goleak inspects every goroutine in the process.
func TestPeerNetworkLeavesNoGoroutinesBehind(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	build := func(cfg PeerConfig) *Stack {
		cfg.ReconcileInterval = 10 * time.Millisecond
		s, err := NewStack(StackConfig{
			PromRegistry:   prometheus.NewRegistry(),
			NetworkMagic:   testDMQMagic,
			SocketPath:     shortSocketPath(t),
			Authenticator:  newTestAuthenticator(),
			MessageBodyMin: 1,
			Peers:          &cfg,
		})
		require.NoError(t, err)
		require.NoError(t, s.Start(t.Context()))
		return s
	}
	remote := build(PeerConfig{
		MaxPeers:    4,
		ListenAddrs: []ListenAddr{{Address: loopbackAny}},
	})
	root := listenPeer(t, remote)
	dialer := build(PeerConfig{
		MinPeers: 2, MaxPeers: 4,
		ListenAddrs: []ListenAddr{{Address: loopbackAny}},
		Topology: &Topology{
			LocalRoots: []RootGroup{group(1, root)},
			// A peer that never answers keeps a dial in progress.
			PublicRoots: []RootGroup{
				group(0, Peer{Host: "192.0.2.1", Port: 9}),
			},
		},
		LedgerPeers: func(ctx context.Context) ([]Peer, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		dialPlain: func(ctx context.Context, address string) (net.Conn, error) {
			if address == root.Address() {
				var d net.Dialer
				return d.DialContext(ctx, "tcp", address)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	waitFor(t, "connected", func() bool { return dialer.PeerCount() == 1 })
	addMessages(t, dialer, "leak", 2)
	waitFor(
		t,
		"a message crosses",
		func() bool { return remote.pool.Len() == 2 },
	)

	ctx, cancel := context.WithTimeout(context.Background(), peerWait)
	defer cancel()
	require.NoError(t, dialer.Stop(ctx))
	require.NoError(t, remote.Stop(ctx))
}

// connectedCount counts the outbound connections a manager holds.
func connectedCount(m *peerManager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.entries {
		if e.conn != nil {
			n++
		}
	}
	return n
}
