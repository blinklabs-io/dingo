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
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/netguard"
)

const (
	// maxConcurrentDials bounds the connection attempts in flight at once.
	maxConcurrentDials = 4
	// stableAfter is how long a connection must last for its peer to count as
	// reliable: one that ends sooner is treated as a failure, so a peer that
	// accepts and drops is not redialed in a tight loop.
	stableAfter = time.Minute
	// ledgerRetry is the wait before the ledger peers are looked up again
	// after a failure, such as a ledger that has not synced.
	ledgerRetry = time.Minute
	// minWake keeps the manager from spinning when a retry is due at once.
	minWake = 10 * time.Millisecond
)

// candidate is a peer the manager chose to dial.
type candidate struct {
	peer Peer
	// guarded peers came from outside the operator's configuration, so their
	// addresses are checked before they are dialed.
	guarded bool
}

type peerEntry struct {
	peer     Peer
	conn     *PeerConn
	dialing  bool
	failures int
	retryAt  time.Time
	since    time.Time
}

func (e *peerEntry) busy() bool { return e.conn != nil || e.dialing }

// ledgerCache holds the peers last learned from the ledger.
type ledgerCache struct {
	peers      []Peer
	fetched    time.Time
	nextTry    time.Time
	refreshing bool
}

// peerManager keeps a Stack's outbound connections: its local roots up to
// their valency, then as many public-root and ledger peers as bring the peer
// count to MinPeers, never beyond MaxPeers. It is independent of Cardano peer
// governance.
type peerManager struct {
	s      *Stack
	cfg    *PeerConfig
	logger *slog.Logger
	wake   chan struct{}
	self   selfCheck
	wg     sync.WaitGroup

	mu       sync.Mutex
	rng      *rand.Rand
	entries  map[string]*peerEntry
	inflight int
	ledger   ledgerCache
}

func newPeerManager(
	s *Stack,
	cfg *PeerConfig,
	listeners []net.Listener,
) *peerManager {
	m := &peerManager{
		s:       s,
		cfg:     cfg,
		logger:  s.logger,
		wake:    make(chan struct{}, 1),
		self:    newSelfCheck(listeners),
		entries: make(map[string]*peerEntry),
		// #nosec G404 -- picking peers and spreading retries need no secrecy
		rng: rand.New(rand.NewPCG(
			uint64(time.Now().UnixNano()), 0x9e3779b97f4a7c15,
		)),
	}
	if cfg.dialPlain == nil {
		dialer := &net.Dialer{Timeout: cfg.DialTimeout}
		cfg.dialPlain = func(
			ctx context.Context, address string,
		) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		}
		if cfg.dialGuarded == nil {
			cfg.dialGuarded = func(
				ctx context.Context, address string,
			) (net.Conn, error) {
				return netguard.DialContext(
					ctx, "tcp", address, dialer.DialContext, nil,
				)
			}
		}
	}
	if cfg.dialGuarded == nil {
		cfg.dialGuarded = cfg.dialPlain
	}
	return m
}

// kick asks the manager to review its connections soon.
func (m *peerManager) kick() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// run reviews the connections until ctx ends, on every change and at least
// every ReconcileInterval, and sooner when a failed peer becomes due.
func (m *peerManager) run(ctx context.Context) {
	defer m.wg.Wait()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
		case <-timer.C:
		}
		wait := m.reconcile(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}
}

// reconcile starts the dials a review calls for and returns how long to wait
// before the next one.
func (m *peerManager) reconcile(ctx context.Context) time.Duration {
	m.mu.Lock()
	now := m.cfg.now()
	m.prune(now)
	picked, dueAt, ledgerWanted := m.plan(now)
	for _, c := range picked {
		e := m.entryLocked(c.peer)
		e.dialing = true
		m.inflight++
	}
	if ledgerWanted {
		m.refreshLedgerLocked(ctx, now)
	}
	m.mu.Unlock()

	for _, c := range picked {
		m.wg.Go(func() { m.dial(ctx, c) })
	}
	wait := m.cfg.ReconcileInterval
	if !dueAt.IsZero() {
		wait = min(wait, max(dueAt.Sub(now), minWake))
	}
	return wait
}

func (m *peerManager) entryLocked(p Peer) *peerEntry {
	key := p.key()
	e := m.entries[key]
	if e == nil {
		e = &peerEntry{peer: p}
		m.entries[key] = e
	}
	return e
}

// prune forgets peers that have not been tried for long, so the table does
// not grow with every address the ledger has ever offered.
func (m *peerManager) prune(now time.Time) {
	for key, e := range m.entries {
		if !e.busy() && now.After(e.retryAt.Add(m.cfg.BackoffMax)) {
			delete(m.entries, key)
		}
	}
}

// plan chooses the peers to dial. It also reports when the earliest peer held
// back by a failure is due, and whether the ledger peers would help. The
// caller holds m.mu.
func (m *peerManager) plan(
	now time.Time,
) (picked []candidate, dueAt time.Time, ledgerWanted bool) {
	room := min(
		m.cfg.MaxPeers-m.s.PeerCount()-m.inflight,
		maxConcurrentDials-m.inflight,
	)
	if room <= 0 {
		return nil, time.Time{}, false
	}
	taken := make(map[string]struct{})
	eligible := func(p Peer) bool {
		key := p.key()
		if _, dup := taken[key]; dup {
			return false
		}
		if m.self.is(p) {
			return false
		}
		if e := m.entries[key]; e != nil {
			if e.busy() {
				return false
			}
			if now.Before(e.retryAt) {
				if dueAt.IsZero() || e.retryAt.Before(dueAt) {
					dueAt = e.retryAt
				}
				return false
			}
		}
		return true
	}
	take := func(c candidate) {
		taken[c.peer.key()] = struct{}{}
		picked = append(picked, c)
	}

	if topo := m.cfg.Topology; topo != nil {
		for _, g := range topo.LocalRoots {
			target := len(g.Peers)
			if g.Valency < uint(target) {
				target = int(g.Valency)
			}
			have := 0
			for _, p := range g.Peers {
				if e := m.entries[p.key()]; e != nil && e.busy() {
					have++
				}
			}
			for _, p := range g.Peers {
				if have >= target || len(picked) >= room {
					break
				}
				if eligible(p) {
					take(candidate{peer: p})
					have++
				}
			}
		}
	}

	need := m.cfg.MinPeers - m.s.PeerCount() - m.inflight - len(picked)
	need = min(need, room-len(picked))
	if need <= 0 {
		return picked, dueAt, false
	}
	for _, p := range m.publicRootPeersLocked() {
		if need <= 0 {
			break
		}
		if eligible(p) {
			take(candidate{peer: p})
			need--
		}
	}
	if m.cfg.LedgerPeers == nil {
		return picked, dueAt, false
	}
	ledgerPeers := slicesCloneShuffled(m.rng, m.ledger.peers)
	for _, p := range ledgerPeers {
		if need <= 0 {
			break
		}
		if eligible(p) {
			take(candidate{peer: p, guarded: true})
			need--
		}
	}
	return picked, dueAt, need > 0
}

// publicRootPeersLocked returns the public roots' peers in random order.
func (m *peerManager) publicRootPeersLocked() []Peer {
	if m.cfg.Topology == nil {
		return nil
	}
	var peers []Peer
	for _, g := range m.cfg.Topology.PublicRoots {
		peers = append(peers, g.Peers...)
	}
	m.rng.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
	return peers
}

func slicesCloneShuffled(rng *rand.Rand, in []Peer) []Peer {
	out := make([]Peer, len(in))
	copy(out, in)
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// dial connects to one chosen peer and runs it as a peer connection.
func (m *peerManager) dial(ctx context.Context, c candidate) {
	dial := m.cfg.dialPlain
	if c.guarded {
		dial = m.cfg.dialGuarded
	}
	dialCtx, cancel := context.WithTimeout(ctx, m.cfg.DialTimeout)
	conn, err := dial(dialCtx, c.peer.Address())
	cancel()
	if err != nil {
		m.dialFailed(ctx, c.peer, err)
		return
	}
	peer, err := m.s.AttachPeer(ctx, conn, true)
	if err != nil {
		m.dialFailed(ctx, c.peer, err)
		return
	}
	m.s.dials.WithLabelValues(dialSuccess).Inc()
	m.mu.Lock()
	e := m.entryLocked(c.peer)
	e.conn = peer
	e.dialing = false
	e.since = m.cfg.now()
	m.inflight--
	m.mu.Unlock()
	m.logger.Info("dmq peer connected", "peer", c.peer.Address())
	m.wg.Go(func() { m.watch(ctx, c.peer, peer) })
}

func (m *peerManager) dialFailed(ctx context.Context, p Peer, err error) {
	m.s.dials.WithLabelValues(dialFailure).Inc()
	if ctx.Err() == nil {
		m.logger.Debug(
			"dmq peer dial failed", "peer", p.Address(), "error", err,
		)
	}
	m.mu.Lock()
	e := m.entryLocked(p)
	e.dialing = false
	m.inflight--
	e.failures++
	e.retryAt = m.cfg.now().Add(m.backoffLocked(e.failures))
	m.mu.Unlock()
	m.kick()
}

// watch waits for a peer connection to end, then lets the manager replace it
// once its backoff is over.
func (m *peerManager) watch(ctx context.Context, p Peer, conn *PeerConn) {
	select {
	case <-conn.Done():
	case <-ctx.Done():
		return
	}
	m.mu.Lock()
	e := m.entryLocked(p)
	now := m.cfg.now()
	e.conn = nil
	if now.Sub(e.since) < stableAfter {
		e.failures++
	} else {
		e.failures = 0
	}
	e.retryAt = now.Add(m.backoffLocked(max(e.failures, 1)))
	m.mu.Unlock()
	m.logger.Info(
		"dmq peer disconnected", "peer", p.Address(), "error", conn.Err(),
	)
	m.kick()
}

// backoffLocked is the wait after the given number of failures in a row: the
// base doubled for each one past the first, up to the maximum, give or take
// a fifth so that peers that failed together do not return together.
func (m *peerManager) backoffLocked(failures int) time.Duration {
	d := m.cfg.BackoffBase
	for i := 1; i < failures && d < m.cfg.BackoffMax; i++ {
		d *= 2
	}
	d = min(d, m.cfg.BackoffMax)
	return time.Duration(float64(d) * (0.8 + 0.4*m.rng.Float64()))
}

// refreshLedgerLocked starts a lookup of the ledger peers when the last is
// stale or none succeeded yet. A lookup that succeeded and found no usable
// peer counts as done, so an empty answer is not asked for again on every
// review. The caller holds m.mu.
func (m *peerManager) refreshLedgerLocked(ctx context.Context, now time.Time) {
	l := &m.ledger
	if l.refreshing || now.Before(l.nextTry) {
		return
	}
	if !l.fetched.IsZero() && now.Sub(l.fetched) < m.cfg.LedgerRefresh {
		return
	}
	l.refreshing = true
	m.wg.Go(func() {
		peers, err := m.cfg.LedgerPeers(ctx)
		m.mu.Lock()
		l.refreshing = false
		done := m.cfg.now()
		if err != nil {
			l.nextTry = done.Add(m.cfg.ledgerRetry)
			m.mu.Unlock()
			if ctx.Err() == nil {
				m.logger.Debug("dmq ledger peer lookup failed", "error", err)
			}
			return
		}
		l.peers = publicPeers(peers)
		usable := len(l.peers)
		l.fetched = done
		l.nextTry = time.Time{}
		m.mu.Unlock()
		m.logger.Info(
			"dmq ledger peers refreshed",
			"offered", len(peers), "usable", usable,
		)
		m.kick()
	})
}

// publicPeers keeps the peers whose address may be dialed on the word of
// the ledger: not a local name, and not an IP literal in a private, loopback
// or otherwise special range. A name that resolves to one is refused when it
// is dialed.
func publicPeers(peers []Peer) []Peer {
	out := make([]Peer, 0, len(peers))
	for _, p := range peers {
		host := strings.Trim(p.Host, "[]")
		if netguard.IsBlockedHost(host) {
			continue
		}
		if addr, err := netip.ParseAddr(host); err == nil &&
			netguard.IsBlockedIP(net.IP(addr.AsSlice())) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// selfCheck recognizes the stack's own listen addresses, so that a peer list
// naming this node does not make it dial itself.
type selfCheck struct {
	ports map[uint]struct{}
	ips   map[netip.Addr]struct{}
}

func newSelfCheck(listeners []net.Listener) selfCheck {
	c := selfCheck{
		ports: make(map[uint]struct{}),
		ips:   make(map[netip.Addr]struct{}),
	}
	for _, ln := range listeners {
		if tcp, ok := ln.Addr().(*net.TCPAddr); ok && tcp.Port > 0 {
			c.ports[uint(tcp.Port)] = struct{}{} // #nosec G115 -- positive
		}
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return c
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if addr, ok := netip.AddrFromSlice(ipNet.IP); ok {
			c.ips[addr.Unmap()] = struct{}{}
		}
	}
	return c
}

// is reports whether the peer is a listen address of this stack. A host name
// is not resolved, so only an IP literal can match.
func (c selfCheck) is(p Peer) bool {
	if _, ok := c.ports[p.Port]; !ok {
		return false
	}
	addr, err := netip.ParseAddr(strings.Trim(p.Host, "[]"))
	if err != nil {
		return false
	}
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsUnspecified() {
		return true
	}
	_, local := c.ips[addr]
	return local
}
