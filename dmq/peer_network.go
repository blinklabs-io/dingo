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
	"fmt"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"
)

// ErrTooManyPeers is returned by AttachPeer when PeerConfig.MaxPeers
// connections are already open.
var ErrTooManyPeers = errors.New("dmq: too many peer connections")

const (
	// defaultMaxInboundPerSource bounds the open connections one source may
	// hold. A source is an IPv4 address or an IPv6 /64, so one host cannot
	// take every peer slot.
	defaultMaxInboundPerSource = 4
	// acceptRetryMin and acceptRetryMax bound the pause after a failed
	// accept, such as one for want of file descriptors.
	acceptRetryMin = 50 * time.Millisecond
	acceptRetryMax = time.Second
)

// ListenAddr is an address a Stack accepts peer connections on.
type ListenAddr struct {
	// Address is host:port. An IPv4 literal binds TCP over IPv4 only, an
	// IPv6 literal over IPv6 only, so an IPv4 and an IPv6 address may share a
	// port. Port zero picks a free one.
	Address string
	// SkipIfUnsupported lets the Stack start without this address when the
	// host has no support for its address family, with a warning. Any other
	// failure to bind is an error.
	SkipIfUnsupported bool
}

// PeerConfig configures a Stack's node-to-node peers: the listener that
// accepts them and the manager that dials them. Without it a Stack serves
// local clients only and a caller supplies connections to AttachPeer.
type PeerConfig struct {
	// ListenAddrs are the addresses to accept peers on. None accepts none.
	ListenAddrs []ListenAddr
	// Topology names the peers to dial. Its local roots are kept connected
	// to their valency; its public roots, and the ledger peers, fill the
	// stack up to MinPeers.
	Topology *Topology
	// LedgerPeers returns peers learned from the ledger, already resolved to
	// addresses. Nil uses none. Its addresses are untrusted: one that is
	// loopback, private or otherwise not publicly routable is never dialed.
	LedgerPeers func(ctx context.Context) ([]Peer, error)
	// MinPeers is how many peer connections the stack tries to hold, inbound
	// ones included.
	MinPeers int
	// MaxPeers is the most peer connections it holds, inbound ones included.
	// A connection that would exceed it is refused. Required.
	MaxPeers int
	// MaxInboundPerSource is the most connections one source may hold. Zero
	// uses four.
	MaxInboundPerSource int
	// DialTimeout bounds one connection attempt. Zero uses ten seconds.
	DialTimeout time.Duration
	// ReconcileInterval is how often the manager reviews its connections
	// when nothing has changed. Zero uses fifteen seconds.
	ReconcileInterval time.Duration
	// LedgerRefresh is how often the ledger peers are looked up again. Zero
	// uses thirty minutes.
	LedgerRefresh time.Duration
	// BackoffBase and BackoffMax bound the wait before a peer that failed is
	// dialed again, which doubles with each failure. Zero uses five seconds
	// and fifteen minutes.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// Tests replace these.
	dialPlain   func(ctx context.Context, address string) (net.Conn, error)
	dialGuarded func(ctx context.Context, address string) (net.Conn, error)
	now         func() time.Time
	ledgerRetry time.Duration
	listen      func(ctx context.Context, network, address string) (net.Listener, error)
}

func (c *PeerConfig) validate() error {
	switch {
	case c.MaxPeers <= 0:
		return errors.New("dmq: peer config requires MaxPeers above zero")
	case c.MinPeers < 0:
		return errors.New("dmq: peer config MinPeers must not be negative")
	case c.MinPeers > c.MaxPeers:
		return errors.New("dmq: peer config MinPeers exceeds MaxPeers")
	case c.MaxInboundPerSource < 0:
		return errors.New(
			"dmq: peer config MaxInboundPerSource must not be negative",
		)
	}
	for _, la := range c.ListenAddrs {
		if _, _, err := net.SplitHostPort(la.Address); err != nil {
			return fmt.Errorf(
				"dmq: peer listen address %q: %w", la.Address, err,
			)
		}
	}
	return nil
}

func (c *PeerConfig) applyDefaults() {
	if c.MaxInboundPerSource == 0 {
		c.MaxInboundPerSource = defaultMaxInboundPerSource
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = 15 * time.Second
	}
	if c.LedgerRefresh <= 0 {
		c.LedgerRefresh = 30 * time.Minute
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 5 * time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 15 * time.Minute
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.ledgerRetry <= 0 {
		c.ledgerRetry = ledgerRetry
	}
}

// peerNetwork is a Stack's listeners and peer manager.
type peerNetwork struct {
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	listeners []net.Listener
	manager   *peerManager
	sources   *sourceLimiter
}

// PeerCount returns the open node-to-node peer connections, inbound and
// outbound, including ones still in their handshake.
func (s *Stack) PeerCount() int {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	return len(s.peers)
}

// PeerListenAddrs returns the addresses the stack accepts peers on, which
// shows the port chosen for a ListenAddr whose port was zero.
func (s *Stack) PeerListenAddrs() []net.Addr {
	s.peerMu.Lock()
	pn := s.peerNet
	s.peerMu.Unlock()
	if pn == nil {
		return nil
	}
	addrs := make([]net.Addr, 0, len(pn.listeners))
	for _, ln := range pn.listeners {
		addrs = append(addrs, ln.Addr())
	}
	return addrs
}

// startPeerNetwork opens the listeners and starts the peer manager.
func (s *Stack) startPeerNetwork( //nolint:contextcheck
	ctx context.Context,
) error {
	cfg := s.cfg.Peers
	if cfg == nil {
		return nil
	}
	// The network lives until Stop, not until the context Start was given
	// ends, like the rest of the stack.
	netCtx, cancel := context.WithCancel(s.ctx)
	pn := &peerNetwork{
		cancel:  cancel,
		sources: newSourceLimiter(cfg.MaxInboundPerSource),
	}
	var lc net.ListenConfig
	listen := cfg.listen
	if listen == nil {
		listen = lc.Listen
	}
	for _, la := range cfg.ListenAddrs {
		ln, err := listen(ctx, listenNetwork(la.Address), la.Address)
		if err != nil {
			if la.SkipIfUnsupported && addressFamilyUnsupported(err) {
				s.logger.Warn(
					"dmq peer listener skipped: address family unsupported",
					"address", la.Address, "error", err,
				)
				continue
			}
			cancel()
			for _, opened := range pn.listeners {
				_ = opened.Close()
			}
			return fmt.Errorf(
				"dmq: listen for peers on %s: %w",
				la.Address,
				err,
			)
		}
		s.logger.Info(
			"dmq peer listener started",
			"address",
			ln.Addr().String(),
		)
		pn.listeners = append(pn.listeners, ln)
	}
	pn.manager = newPeerManager(s, cfg, pn.listeners)
	s.peerMu.Lock()
	s.peerNet = pn
	s.peerMu.Unlock()
	for _, ln := range pn.listeners {
		pn.wg.Go(func() { s.acceptPeers(netCtx, pn, ln) })
	}
	pn.wg.Go(func() { pn.manager.run(netCtx) })
	return nil
}

// stopPeerNetwork closes the listeners and stops the manager and everything
// they started, so that no new peer appears while the open ones are closed.
func (s *Stack) stopPeerNetwork(ctx context.Context) error {
	s.peerMu.Lock()
	pn := s.peerNet
	s.peerMu.Unlock()
	if pn == nil {
		return nil
	}
	pn.cancel()
	var errs []error
	for _, ln := range pn.listeners {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	done := make(chan struct{})
	go func() {
		pn.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("dmq: stop peer network: %w", ctx.Err()))
	}
	return errors.Join(errs...)
}

// listenNetwork picks the network for an address, so that an IPv4 and an IPv6
// address can listen on one port.
func listenNetwork(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "tcp"
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "tcp"
	}
	if addr.Unmap().Is4() {
		return "tcp4"
	}
	return "tcp6"
}

// addressFamilyUnsupported reports whether a bind failed because the host
// lacks the address family, which is not worth refusing to start for. A
// specific address that is not on the host is a different error.
func addressFamilyUnsupported(err error) bool {
	return errors.Is(err, syscall.EAFNOSUPPORT) ||
		errors.Is(err, syscall.EPROTONOSUPPORT)
}

// acceptPeers hands each connection to the stack until the listener closes.
func (s *Stack) acceptPeers(
	ctx context.Context,
	pn *peerNetwork,
	ln net.Listener,
) {
	pause := acceptRetryMin
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Warn("dmq peer accept failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(pause):
			}
			pause = min(pause*2, acceptRetryMax)
			continue
		}
		pause = acceptRetryMin
		pn.wg.Go(func() { s.takeInboundPeer(ctx, pn, conn) })
	}
}

// takeInboundPeer runs one accepted connection as a peer, refusing it when its
// source already holds its share or the stack is full.
func (s *Stack) takeInboundPeer(
	ctx context.Context,
	pn *peerNetwork,
	conn net.Conn,
) {
	source := sourceOf(conn.RemoteAddr())
	if !pn.sources.acquire(source) {
		s.logger.Debug(
			"dmq peer refused: source holds its share",
			"peer", conn.RemoteAddr().String(),
		)
		_ = conn.Close()
		return
	}
	peer, err := s.AttachPeer(ctx, conn, false)
	if err != nil {
		pn.sources.release(source)
		s.logger.Debug(
			"dmq inbound peer refused",
			"peer", conn.RemoteAddr().String(), "error", err,
		)
		return
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
	}
	pn.sources.release(source)
}

// sourceOf names the source of a connection: its IPv4 address, or the /64 of
// its IPv6 address, which one subscriber may hold wholesale.
func sourceOf(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp == nil {
		return addr.String()
	}
	ip, ok := netip.AddrFromSlice(tcp.IP)
	if !ok {
		return addr.String()
	}
	ip = ip.Unmap()
	if ip.Is6() {
		prefix, err := ip.Prefix(64)
		if err == nil {
			return prefix.String()
		}
	}
	return ip.String()
}

// sourceLimiter counts the connections each source holds.
type sourceLimiter struct {
	mu    sync.Mutex
	limit int
	held  map[string]int
}

func newSourceLimiter(limit int) *sourceLimiter {
	return &sourceLimiter{limit: limit, held: make(map[string]int)}
}

func (l *sourceLimiter) acquire(source string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held[source] >= l.limit {
		return false
	}
	l.held[source]++
	return true
}

func (l *sourceLimiter) release(source string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held[source] <= 1 {
		delete(l.held, source)
		return
	}
	l.held[source]--
}
