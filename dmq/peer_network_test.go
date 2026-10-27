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
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const loopbackAny = "127.0.0.1:0"

// shortSocketPath is a Unix socket path short enough for macOS, whose limit
// t.TempDir's long names can exceed.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dmq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "dmq.sock")
}

func newTestAuthenticator() *ocommon.MessageAuthenticator {
	return ocommon.NewNoOpAuthenticator(nil)
}

// newListeningStack is a stack that accepts peers on a loopback port.
func newListeningStack(
	t *testing.T,
	cfg PeerConfig,
) (*Stack, *prometheus.Registry) {
	t.Helper()
	cfg.ListenAddrs = append(
		[]ListenAddr{{Address: loopbackAny}}, cfg.ListenAddrs...,
	)
	if cfg.MaxPeers == 0 {
		cfg.MaxPeers = 10
	}
	return newTestStack(t, StackConfig{Peers: &cfg})
}

// listenPeer is where a stack accepts peers, as a Peer.
func listenPeer(t *testing.T, s *Stack) Peer {
	t.Helper()
	addrs := s.PeerListenAddrs()
	require.NotEmpty(t, addrs)
	tcp, ok := addrs[0].(*net.TCPAddr)
	require.True(t, ok)
	return Peer{Host: tcp.IP.String(), Port: uint(tcp.Port)}
}

// dialStack connects a plain stack to a listening one and returns the
// dialer's side of the connection.
func dialStack(t *testing.T, from, to *Stack) *PeerConn {
	t.Helper()
	conn, err := net.DialTimeout(
		"tcp", listenPeer(t, to).Address(), peerWait,
	)
	require.NoError(t, err)
	peer, err := from.AttachPeer(t.Context(), conn, true)
	require.NoError(t, err)
	return peer
}

func TestPeerNetworkAcceptsPeersAndExchangesMessages(t *testing.T) {
	t.Parallel()
	listener, _ := newListeningStack(t, PeerConfig{})
	dialer, _ := newTestStack(t, StackConfig{})

	dialStack(t, dialer, listener)
	waitFor(t, "the listener holds the peer", func() bool {
		return listener.PeerCount() == 1
	})

	addMessages(t, dialer, "from-dialer", 2)
	addMessages(t, listener, "from-listener", 2)
	waitFor(t, "messages cross in both directions", func() bool {
		return listener.pool.Len() == 4 && dialer.pool.Len() == 4
	})
}

func TestPeerNetworkRefusesPeersBeyondMaxPeers(t *testing.T) {
	t.Parallel()
	listener, _ := newListeningStack(t, PeerConfig{
		MaxPeers: 1, MaxInboundPerSource: 5,
	})
	first, _ := newTestStack(t, StackConfig{})
	second, _ := newTestStack(t, StackConfig{})

	dialStack(t, first, listener)
	waitFor(
		t,
		"the first peer",
		func() bool { return listener.PeerCount() == 1 },
	)

	conn, err := net.DialTimeout(
		"tcp", listenPeer(t, listener).Address(), peerWait,
	)
	require.NoError(t, err)
	_, err = second.AttachPeer(t.Context(), conn, true)
	require.Error(t, err, "the second connection is refused")
	assert.Equal(t, 1, listener.PeerCount())

	// The limit also holds for a connection handed over directly.
	a, b := tcpPair(t)
	t.Cleanup(func() { _ = b.Close() })
	_, err = listener.AttachPeer(t.Context(), a, false)
	require.ErrorIs(t, err, ErrTooManyPeers)
}

func TestPeerNetworkLimitsOneSource(t *testing.T) {
	t.Parallel()
	listener, _ := newListeningStack(t, PeerConfig{
		MaxPeers: 5, MaxInboundPerSource: 1,
	})
	first, _ := newTestStack(t, StackConfig{})
	second, _ := newTestStack(t, StackConfig{})

	held := dialStack(t, first, listener)
	waitFor(
		t,
		"the first peer",
		func() bool { return listener.PeerCount() == 1 },
	)

	conn, err := net.DialTimeout(
		"tcp", listenPeer(t, listener).Address(), peerWait,
	)
	require.NoError(t, err)
	_, err = second.AttachPeer(t.Context(), conn, true)
	require.Error(t, err, "a second connection from the same source")

	// Once the first connection ends, its share is free again.
	held.Close()
	waitFor(t, "the listener releases the source", func() bool {
		return listener.PeerCount() == 0
	})
	dialStack(t, second, listener)
	waitFor(t, "the source may connect again", func() bool {
		return listener.PeerCount() == 1
	})
}

func TestPeerNetworkStopClosesTheListenerAndItsPeers(t *testing.T) {
	t.Parallel()
	listener, _ := newListeningStack(t, PeerConfig{})
	dialer, _ := newTestStack(t, StackConfig{})
	peer := dialStack(t, dialer, listener)
	waitFor(t, "the peer", func() bool { return listener.PeerCount() == 1 })
	address := listenPeer(t, listener).Address()

	require.NoError(t, listener.Stop(t.Context()))

	requireClosed(t, "the dialer's connection ends", peer.Done())
	_, err := net.DialTimeout("tcp", address, 2*time.Second)
	require.Error(t, err, "nothing listens after Stop")
}

func TestPeerNetworkFailsToStartOnAnAddressInUse(t *testing.T) {
	t.Parallel()
	taken, err := net.Listen("tcp4", loopbackAny)
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })

	s, err := NewStack(StackConfig{
		SocketPath:    shortSocketPath(t),
		Authenticator: newTestAuthenticator(),
		Peers: &PeerConfig{
			MaxPeers:    1,
			ListenAddrs: []ListenAddr{{Address: taken.Addr().String()}},
		},
	})
	require.NoError(t, err)
	require.Error(t, s.Start(t.Context()))
}

func TestPeerNetworkListensOnIPv6WhenTheHostHasIt(t *testing.T) {
	t.Parallel()
	probe, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	_ = probe.Close()
	listener, _ := newListeningStack(t, PeerConfig{
		ListenAddrs: []ListenAddr{{Address: "[::1]:0"}},
	})
	addrs := listener.PeerListenAddrs()
	require.Len(t, addrs, 2)
	assert.Equal(t, "tcp", addrs[0].Network())
	assert.True(
		t,
		addrs[1].(*net.TCPAddr).IP.To4() == nil,
		"the second is IPv6",
	)
}

func TestPeerConfigValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  PeerConfig
		want string
	}{
		{"no maximum", PeerConfig{}, "MaxPeers"},
		{"negative minimum", PeerConfig{MaxPeers: 1, MinPeers: -1}, "negative"},
		{
			"minimum above maximum",
			PeerConfig{MaxPeers: 1, MinPeers: 2},
			"exceeds",
		},
		{"negative per-source limit", PeerConfig{
			MaxPeers: 1, MaxInboundPerSource: -1,
		}, "MaxInboundPerSource"},
		{"an address without a port", PeerConfig{
			MaxPeers: 1, ListenAddrs: []ListenAddr{{Address: "127.0.0.1"}},
		}, "listen address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewStack(StackConfig{
				SocketPath:    shortSocketPath(t),
				Authenticator: newTestAuthenticator(),
				Peers:         &tt.cfg,
			})
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestListenNetworkKeepsIPv4AndIPv6Apart(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "tcp4", listenNetwork("0.0.0.0:4001"))
	assert.Equal(t, "tcp6", listenNetwork("[::]:4001"))
	assert.Equal(t, "tcp6", listenNetwork("[2001:db8::1]:4001"))
	assert.Equal(t, "tcp", listenNetwork("localhost:4001"))
	assert.Equal(t, "tcp", listenNetwork("not an address"))
}

func TestAddressFamilyUnsupportedIsNotAnyBindFailure(t *testing.T) {
	t.Parallel()
	wrap := func(err error) error {
		return &net.OpError{Op: "listen", Err: &netSyscallErr{err}}
	}
	assert.True(t, addressFamilyUnsupported(wrap(syscall.EAFNOSUPPORT)))
	assert.False(t, addressFamilyUnsupported(wrap(syscall.EADDRNOTAVAIL)))
	assert.False(t, addressFamilyUnsupported(wrap(syscall.EADDRINUSE)))
	assert.False(t, addressFamilyUnsupported(errors.New("other")))
}

// netSyscallErr wraps an errno the way the net package reports one.
type netSyscallErr struct{ err error }

func (e *netSyscallErr) Error() string { return e.err.Error() }
func (e *netSyscallErr) Unwrap() error { return e.err }

func TestSourceOfGroupsAnIPv6SubscriberByItsSixtyFour(t *testing.T) {
	t.Parallel()
	tcp := func(ip string) net.Addr {
		return &net.TCPAddr{IP: net.ParseIP(ip), Port: 1}
	}
	assert.Equal(t, "192.0.2.7", sourceOf(tcp("192.0.2.7")))
	assert.Equal(t, "192.0.2.7", sourceOf(tcp("::ffff:192.0.2.7")))
	assert.Equal(
		t,
		sourceOf(tcp("2001:db8:1:2::1")),
		sourceOf(tcp("2001:db8:1:2:ffff::9")),
	)
	assert.NotEqual(
		t, sourceOf(tcp("2001:db8:1:2::1")), sourceOf(tcp("2001:db8:1:3::1")),
	)
}

func TestSourceLimiterCountsEachSourceSeparately(t *testing.T) {
	t.Parallel()
	l := newSourceLimiter(2)
	require.True(t, l.acquire("a"))
	require.True(t, l.acquire("a"))
	assert.False(t, l.acquire("a"))
	assert.True(t, l.acquire("b"))
	l.release("a")
	assert.True(t, l.acquire("a"))
	l.release("a")
	l.release("a")
	l.release("b")
	assert.Empty(t, l.held, "released sources are forgotten")
}

// The stacks of two topics share one registry, each with its own pool,
// socket, port and magic, and every metric carries its topic.
func TestTwoTopicsShareARegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	build := func(topic string, magic uint32) *Stack {
		s, err := NewStack(StackConfig{
			PromRegistry:   reg,
			Topic:          topic,
			NetworkMagic:   magic,
			SocketPath:     shortSocketPath(t),
			Authenticator:  newTestAuthenticator(),
			MessageBodyMin: 1,
			Peers: &PeerConfig{
				MaxPeers:    2,
				ListenAddrs: []ListenAddr{{Address: loopbackAny}},
			},
		})
		require.NoError(t, err)
		require.NoError(t, s.Start(t.Context()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(
				context.Background(), 10*time.Second,
			)
			defer cancel()
			require.NoError(t, s.Stop(ctx))
		})
		return s
	}
	a := build("mithril", 1)
	b := build("other", 2)

	addMessages(t, a, "only-a", 3)
	assert.Equal(t, 3, a.pool.Len())
	assert.Equal(t, 0, b.pool.Len(), "a pool is private to its topic")
	assert.NotEqual(t, listenPeer(t, a).Port, listenPeer(t, b).Port)

	families, err := reg.Gather()
	require.NoError(t, err)
	topics := map[string]map[string]bool{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "topic" {
					if topics[f.GetName()] == nil {
						topics[f.GetName()] = map[string]bool{}
					}
					topics[f.GetName()][l.GetValue()] = true
				}
			}
		}
	}
	for _, name := range []string{
		"dingo_dmq_messages_received_total",
		"dingo_dmq_mempool_messages",
		"dingo_dmq_peers",
		"dingo_dmq_validation_failures_total",
		"dingo_dmq_peer_dials_total",
	} {
		assert.Equal(
			t, map[string]bool{"mithril": true, "other": true}, topics[name],
			fmt.Sprintf("%s carries both topics", name),
		)
	}
}

// A listener that fails to accept is retried, not abandoned.
func TestPeerNetworkKeepsAcceptingAfterAFailedAccept(t *testing.T) {
	t.Parallel()
	stack, _ := newListeningStack(t, PeerConfig{})
	pn := stack.peerNet

	real, err := net.Listen("tcp4", loopbackAny)
	require.NoError(t, err)
	t.Cleanup(func() { _ = real.Close() })
	flaky := &flakyListener{Listener: real}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	pn.wg.Go(func() { stack.acceptPeers(ctx, pn, flaky) })

	dialer, _ := newTestStack(t, StackConfig{})
	conn, err := net.DialTimeout("tcp", real.Addr().String(), peerWait)
	require.NoError(t, err)
	_, err = dialer.AttachPeer(t.Context(), conn, true)
	require.NoError(t, err, "the loop went on after its first accept failed")
	assert.GreaterOrEqual(t, flaky.failures.Load(), int32(2))
}

// flakyListener fails its first accept.
type flakyListener struct {
	net.Listener
	failures atomic.Int32
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.failures.Add(1) == 1 {
		return nil, errors.New("test: accept failed")
	}
	return l.Listener.Accept()
}

// The defaults bind 0.0.0.0 and :: on one port, which an IPv6 wildcard that
// also claims IPv4 would refuse.
func TestPeerNetworkSharesOnePortBetweenIPv4AndIPv6Wildcards(t *testing.T) {
	t.Parallel()
	probe, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	_ = probe.Close()
	free, err := net.Listen("tcp4", "0.0.0.0:0")
	require.NoError(t, err)
	port := free.Addr().(*net.TCPAddr).Port
	require.NoError(t, free.Close())

	stack, _ := newTestStack(t, StackConfig{Peers: &PeerConfig{
		MaxPeers: 2,
		ListenAddrs: []ListenAddr{
			{Address: net.JoinHostPort("0.0.0.0", strconv.Itoa(port))},
			{Address: net.JoinHostPort("::", strconv.Itoa(port))},
		},
	}})
	require.Len(t, stack.PeerListenAddrs(), 2)
}

// refuseTCP6 is a listen that fails for IPv6 the way a host without it does,
// and listens normally otherwise.
func refuseTCP6(err error) func(
	context.Context, string, string,
) (net.Listener, error) {
	return func(
		ctx context.Context, network, address string,
	) (net.Listener, error) {
		if network == "tcp6" {
			return nil, &net.OpError{
				Op: "listen", Err: &netSyscallErr{err},
			}
		}
		var lc net.ListenConfig
		return lc.Listen(ctx, network, address)
	}
}

func TestPeerNetworkStartsWithoutAnAddressFamilyTheHostLacks(t *testing.T) {
	t.Parallel()
	stack, _ := newTestStack(t, StackConfig{Peers: &PeerConfig{
		MaxPeers: 2,
		listen:   refuseTCP6(syscall.EAFNOSUPPORT),
		ListenAddrs: []ListenAddr{
			{Address: loopbackAny},
			{Address: "[::]:4001", SkipIfUnsupported: true},
		},
	}})
	require.Len(t, stack.PeerListenAddrs(), 1, "only the IPv4 listener runs")
}

func TestPeerNetworkRefusesToStartWithoutASupportedRequiredFamily(
	t *testing.T,
) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		addr ListenAddr
		err  error
	}{
		{"required", ListenAddr{Address: "[::]:4001"}, syscall.EAFNOSUPPORT},
		{"a specific address the host lacks", ListenAddr{
			Address: "[2001:db8::1]:4001", SkipIfUnsupported: true,
		}, syscall.EADDRNOTAVAIL},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := NewStack(StackConfig{
				SocketPath:    shortSocketPath(t),
				Authenticator: newTestAuthenticator(),
				Peers: &PeerConfig{
					MaxPeers:    1,
					listen:      refuseTCP6(tt.err),
					ListenAddrs: []ListenAddr{tt.addr},
				},
			})
			require.NoError(t, err)
			require.Error(t, s.Start(t.Context()))
		})
	}
}
