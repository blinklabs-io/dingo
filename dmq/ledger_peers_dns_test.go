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
	"net"
	"sync"
	"testing"
	"time"

	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

// srvRecord is one SRV answer a test DNS server gives.
type srvRecord struct {
	priority, weight, port uint16
	target                 string
}

// dnsServer answers SRV queries over UDP from a table, so that discovery runs
// through the standard library's resolver and the wire format of a real DNS
// exchange. A name that is not in the table gets NXDOMAIN.
type dnsServer struct {
	conn    *net.UDPConn
	answers map[string][]srvRecord

	mu      sync.Mutex
	queried []string
}

func newDNSServer(t *testing.T, answers map[string][]srvRecord) *dnsServer {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	s := &dnsServer{conn: conn, answers: answers}
	t.Cleanup(func() { _ = conn.Close() })
	go s.serve()
	return s
}

// resolver is a resolver that asks only this server.
func (s *dnsServer) resolver() *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", s.conn.LocalAddr().String())
		},
	}
}

func (s *dnsServer) asked() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queried...)
}

func (s *dnsServer) serve() {
	buf := make([]byte, 1500)
	for {
		n, from, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var p dnsmessage.Parser
		header, err := p.Start(buf[:n])
		if err != nil {
			continue
		}
		q, err := p.Question()
		if err != nil {
			continue
		}
		name := q.Name.String()
		s.mu.Lock()
		s.queried = append(s.queried, name)
		s.mu.Unlock()

		records, known := s.answers[name]
		rcode := dnsmessage.RCodeSuccess
		if !known {
			rcode = dnsmessage.RCodeNameError
		}
		b := dnsmessage.NewBuilder(nil, dnsmessage.Header{
			ID: header.ID, Response: true, Authoritative: true,
			RecursionAvailable: true, RCode: rcode,
		})
		_ = b.StartQuestions()
		_ = b.Question(q)
		_ = b.StartAnswers()
		for _, r := range records {
			_ = b.SRVResource(dnsmessage.ResourceHeader{
				Name: q.Name, Type: dnsmessage.TypeSRV,
				Class: dnsmessage.ClassINET, TTL: 60,
			}, dnsmessage.SRVResource{
				Priority: r.priority, Weight: r.weight, Port: r.port,
				Target: dnsmessage.MustNewName(r.target),
			})
		}
		msg, err := b.Finish()
		if err != nil {
			continue
		}
		_, _ = s.conn.WriteToUDP(msg, from)
	}
}

// Discovery works through a real resolver and a real DNS exchange, not only
// the fake the other tests use.
func TestResolveLedgerPeersThroughARealResolver(t *testing.T) {
	t.Parallel()
	server := newDNSServer(t, map[string][]srvRecord{
		testPrefix + ".a.example.org.": {
			{priority: 10, weight: 1, port: 3141, target: "low.a.example.org."},
			{
				priority: 10,
				weight:   50,
				port:     3141,
				target:   "high.a.example.org.",
			},
			{
				priority: 5,
				weight:   1,
				port:     3142,
				target:   "first.a.example.org.",
			},
			{priority: 0, weight: 0, port: 3141, target: "."},
		},
		testPrefix + ".b.example.org.": {
			{
				priority: 1,
				weight:   1,
				port:     6161,
				target:   "relay.b.example.org.",
			},
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	peers, err := ResolveLedgerPeers(ctx, LedgerPeerLookup{
		Resolver: server.resolver(),
		Prefix:   testPrefix,
		Timeout:  5 * time.Second,
	}, []string{"a.example.org", "missing.example.org", "b.example.org"})
	require.NoError(t, err)
	assert.Equal(t, []Peer{
		{Host: "first.a.example.org", Port: 3142},
		{Host: "high.a.example.org", Port: 3141},
		{Host: "low.a.example.org", Port: 3141},
		{Host: "relay.b.example.org", Port: 6161},
	}, peers, "ordered by priority and weight, '.' skipped, NXDOMAIN skipped")
	assert.Contains(t, server.asked(), testPrefix+".a.example.org.")
}

// A snapshot of pools, an SRV lookup over real DNS, and the peers that come
// out: the whole chain the node uses, ending in what the manager would dial.
func TestLedgerPeerSourceThroughARealResolver(t *testing.T) {
	t.Parallel()
	server := newDNSServer(t, map[string][]srvRecord{
		testPrefix + ".pool.example.org.": {
			{port: 3141, target: "dmq.pool.example.org."},
			{port: 3141, target: "localhost."},
		},
	})
	source := LedgerPeerSource(
		func(context.Context) (olocalstatequery.LedgerPeerSnapshotResult, error) {
			return snapshotOf(
				[]olocalstatequery.RelayAccessPoint{
					srvRelay("Pool.Example.org."),
				},
				[]olocalstatequery.RelayAccessPoint{
					srvRelay("pool.example.org"),
				},
				[]olocalstatequery.RelayAccessPoint{
					ipv4Relay("192.0.2.1", 3001),
				},
			), nil
		},
		LedgerPeerLookup{Resolver: server.resolver(), Prefix: testPrefix},
	)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	peers, err := source(ctx)
	require.NoError(t, err)
	assert.Equal(t, []Peer{
		{Host: "dmq.pool.example.org", Port: 3141},
		{Host: "localhost", Port: 3141},
	}, peers, "the resolver returns what DNS said")
	assert.Len(t, server.asked(), 1, "a domain is looked up once")
	// What DNS said is not trusted: the manager drops a local name.
	assert.Equal(t, []Peer{{Host: "dmq.pool.example.org", Port: 3141}},
		publicPeers(peers))
}
