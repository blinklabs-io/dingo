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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func srvRelay(domain string) olocalstatequery.RelayAccessPoint {
	return olocalstatequery.RelayAccessPoint{
		Kind:   olocalstatequery.RelayKindSRV,
		Domain: &domain,
	}
}

func domainRelay(domain string, port uint16) olocalstatequery.RelayAccessPoint {
	return olocalstatequery.RelayAccessPoint{
		Kind:   olocalstatequery.RelayKindDomain,
		Domain: &domain,
		Port:   &port,
	}
}

func ipv4Relay(ip string, port uint16) olocalstatequery.RelayAccessPoint {
	addr := net.ParseIP(ip)
	return olocalstatequery.RelayAccessPoint{
		Kind: olocalstatequery.RelayKindIPv4,
		IPv4: &addr,
		Port: &port,
	}
}

func snapshotOf(
	pools ...[]olocalstatequery.RelayAccessPoint,
) olocalstatequery.LedgerPeerSnapshotResult {
	result := olocalstatequery.LedgerPeerSnapshotResult{}
	for _, relays := range pools {
		result.Pools = append(result.Pools, olocalstatequery.PoolLedgerPeers{
			Detail: olocalstatequery.PoolLedgerPeersDetail{Relays: relays},
		})
	}
	return result
}

func TestTopicSRVPrefix(t *testing.T) {
	t.Parallel()
	prefix, ok := TopicSRVPrefix("mithril")
	assert.True(t, ok)
	assert.Equal(t, "_dmq._mithril._cardano._tcp", prefix)
	_, ok = TopicSRVPrefix("unknown")
	assert.False(t, ok)
}

func TestLedgerRelayDomainsKeepsOnlySRVRelays(t *testing.T) {
	t.Parallel()
	snapshot := snapshotOf(
		[]olocalstatequery.RelayAccessPoint{
			ipv4Relay("192.0.2.1", 3001),
			domainRelay("cardano-only.example.com", 3001),
			srvRelay("Pool-A.Example.com"),
		},
		[]olocalstatequery.RelayAccessPoint{
			srvRelay("pool-b.example.com."),
			srvRelay(" pool-a.example.com "),
			srvRelay(""),
		},
		nil,
		[]olocalstatequery.RelayAccessPoint{{
			Kind: olocalstatequery.RelayKindSRV,
		}},
	)
	assert.Equal(
		t,
		[]string{"pool-a.example.com", "pool-b.example.com"},
		LedgerRelayDomains(snapshot),
	)
}

func TestLedgerRelayDomainsEmptySnapshot(t *testing.T) {
	t.Parallel()
	assert.Empty(t, LedgerRelayDomains(
		olocalstatequery.LedgerPeerSnapshotResult{},
	))
}

// fakeSRV answers LookupSRV from a table and records what it was asked.
type fakeSRV struct {
	mu      sync.Mutex
	answers map[string][]*net.SRV
	errs    map[string]error
	asked   []string
	// onLookup, when set, runs inside every lookup before it answers.
	onLookup func(ctx context.Context, name string) error
}

func (f *fakeSRV) LookupSRV(
	ctx context.Context,
	service, proto, name string,
) (string, []*net.SRV, error) {
	f.mu.Lock()
	f.asked = append(f.asked, service+"|"+proto+"|"+name)
	answers, errLookup := f.answers[name], f.errs[name]
	f.mu.Unlock()
	if f.onLookup != nil {
		if err := f.onLookup(ctx, name); err != nil {
			return "", nil, err
		}
	}
	return "", answers, errLookup
}

const testPrefix = "_dmq._mithril._cardano._tcp"

func TestResolveLedgerPeersOrdersAndFilters(t *testing.T) {
	t.Parallel()
	resolver := &fakeSRV{
		answers: map[string][]*net.SRV{
			testPrefix + ".a.example.com": {
				{
					Target:   "low-weight.a.example.com.",
					Port:     3141,
					Priority: 10,
					Weight:   1,
				},
				{
					Target:   "high-weight.a.example.com.",
					Port:     3141,
					Priority: 10,
					Weight:   50,
				},
				{
					Target:   "preferred.a.example.com.",
					Port:     3142,
					Priority: 5,
					Weight:   1,
				},
				{Target: ".", Port: 3141, Priority: 0, Weight: 0},
				{
					Target:   "no-port.a.example.com.",
					Port:     0,
					Priority: 1,
					Weight:   1,
				},
			},
			testPrefix + ".b.example.com": {
				{Target: "b.example.com", Port: 3141, Priority: 1},
				// Already listed under a, in another case.
				{Target: "Preferred.A.Example.com.", Port: 3142, Priority: 2},
			},
		},
	}
	peers, err := ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
		[]string{"a.example.com", "b.example.com"},
	)
	require.NoError(t, err)
	assert.Equal(t, []Peer{
		{Host: "preferred.a.example.com", Port: 3142},
		{Host: "high-weight.a.example.com", Port: 3141},
		{Host: "low-weight.a.example.com", Port: 3141},
		{Host: "b.example.com", Port: 3141},
	}, peers)
	// The full name is queried, with no separate service or protocol.
	assert.ElementsMatch(t, []string{
		"||" + testPrefix + ".a.example.com",
		"||" + testPrefix + ".b.example.com",
	}, resolver.asked)
}

func TestResolveLedgerPeersDoesNotReorderTheResolversRecords(t *testing.T) {
	t.Parallel()
	records := []*net.SRV{
		{Target: "z.example.com.", Port: 1, Priority: 9},
		{Target: "a.example.com.", Port: 1, Priority: 1},
	}
	resolver := &fakeSRV{answers: map[string][]*net.SRV{
		testPrefix + ".d.example.com": records,
	}}
	_, err := ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
		[]string{"d.example.com"},
	)
	require.NoError(t, err)
	assert.Equal(t, "z.example.com.", records[0].Target)
}

func TestResolveLedgerPeersSkipsFailedDomains(t *testing.T) {
	t.Parallel()
	resolver := &fakeSRV{
		answers: map[string][]*net.SRV{
			testPrefix + ".good.example.com": {
				{Target: "good.example.com.", Port: 3141},
			},
		},
		errs: map[string]error{
			testPrefix + ".bad.example.com": errors.New("no such host"),
		},
	}
	peers, err := ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
		[]string{"bad.example.com", "none.example.com", "good.example.com"},
	)
	require.NoError(t, err)
	assert.Equal(t, []Peer{{Host: "good.example.com", Port: 3141}}, peers)
}

func TestResolveLedgerPeersNoDomains(t *testing.T) {
	t.Parallel()
	peers, err := ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{Resolver: &fakeSRV{}, Prefix: testPrefix},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, peers)
}

func TestResolveLedgerPeersRequiresResolverAndPrefix(t *testing.T) {
	t.Parallel()
	_, err := ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{Prefix: testPrefix},
		[]string{"a.example.com"},
	)
	require.ErrorContains(t, err, "requires a resolver")
	_, err = ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{Resolver: &fakeSRV{}},
		[]string{"a.example.com"},
	)
	require.ErrorContains(t, err, "requires an SRV prefix")
}

// A lookup that never answers is cut off by the per-lookup timeout, so one
// slow domain does not stall the rest.
func TestResolveLedgerPeersBoundsEachLookup(t *testing.T) {
	t.Parallel()
	resolver := &fakeSRV{
		answers: map[string][]*net.SRV{
			testPrefix + ".fast.example.com": {
				{Target: "fast.example.com.", Port: 3141},
			},
		},
		onLookup: func(ctx context.Context, name string) error {
			if name == testPrefix+".stuck.example.com" {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}
	peers, err := ResolveLedgerPeers(
		context.Background(),
		LedgerPeerLookup{
			Resolver: resolver,
			Prefix:   testPrefix,
			Timeout:  20 * time.Millisecond,
		},
		[]string{"stuck.example.com", "fast.example.com"},
	)
	require.NoError(t, err)
	assert.Equal(t, []Peer{{Host: "fast.example.com", Port: 3141}}, peers)
}

func TestResolveLedgerPeersStopsWhenContextEnds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	resolver := &fakeSRV{onLookup: func(ctx context.Context, _ string) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}}
	_, err := ResolveLedgerPeers(
		ctx,
		LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
		[]string{"a.example.com", "b.example.com", "c.example.com"},
	)
	require.ErrorIs(t, err, context.Canceled)
}

// No more than ledgerLookupConcurrency lookups run at once, however many
// domains the ledger holds.
func TestResolveLedgerPeersBoundsConcurrency(t *testing.T) {
	t.Parallel()
	total := ledgerLookupConcurrency * 3
	domains := make([]string, total)
	for i := range domains {
		domains[i] = "d" + string(
			rune('a'+i%26),
		) + string(
			rune('a'+i/26),
		) + ".example.com"
	}
	var inFlight, maxInFlight atomic.Int32
	release := make(chan struct{})
	resolver := &fakeSRV{onLookup: func(ctx context.Context, _ string) error {
		now := inFlight.Add(1)
		for {
			seen := maxInFlight.Load()
			if now <= seen || maxInFlight.CompareAndSwap(seen, now) {
				break
			}
		}
		defer inFlight.Add(-1)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}}
	done := make(chan error, 1)
	go func() {
		_, err := ResolveLedgerPeers(
			context.Background(),
			LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
			domains,
		)
		done <- err
	}()
	testutil.WaitForCondition(t, func() bool {
		return inFlight.Load() == int32(ledgerLookupConcurrency)
	}, 5*time.Second, "lookups did not fill the limit")
	close(release)
	require.NoError(t, <-done)
	assert.LessOrEqual(t, maxInFlight.Load(), int32(ledgerLookupConcurrency))
}

func TestLedgerPeerSourceTurnsASnapshotIntoPeers(t *testing.T) {
	t.Parallel()
	resolver := &fakeSRV{answers: map[string][]*net.SRV{
		testPrefix + ".a.example.com": {
			{Target: "relay.a.example.com.", Port: 3141},
		},
	}}
	snapshot := func(context.Context) (
		olocalstatequery.LedgerPeerSnapshotResult, error,
	) {
		return snapshotOf(
			[]olocalstatequery.RelayAccessPoint{
				srvRelay("a.example.com"), ipv4Relay("192.0.2.1", 3001),
			},
			[]olocalstatequery.RelayAccessPoint{
				domainRelay("b.example.com", 3001),
			},
		), nil
	}
	source := LedgerPeerSource(
		snapshot, LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
	)
	peers, err := source(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []Peer{{Host: "relay.a.example.com", Port: 3141}}, peers)
	assert.Len(t, resolver.asked, 1, "only SRV relays are looked up")
}

func TestLedgerPeerSourceReturnsTheSnapshotsOwnError(t *testing.T) {
	t.Parallel()
	resolver := &fakeSRV{}
	source := LedgerPeerSource(
		func(context.Context) (olocalstatequery.LedgerPeerSnapshotResult, error) {
			return olocalstatequery.LedgerPeerSnapshotResult{}, ErrChainStateUnavailable
		},
		LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
	)
	_, err := source(t.Context())
	require.ErrorIs(t, err, ErrChainStateUnavailable)
	assert.Empty(t, resolver.asked)
}

func TestLedgerPeerSourceWithNoSRVRelaysDoesNoLookups(t *testing.T) {
	t.Parallel()
	resolver := &fakeSRV{}
	source := LedgerPeerSource(
		func(context.Context) (olocalstatequery.LedgerPeerSnapshotResult, error) {
			return snapshotOf(
				[]olocalstatequery.RelayAccessPoint{
					ipv4Relay("192.0.2.1", 3001),
				},
			), nil
		},
		LedgerPeerLookup{Resolver: resolver, Prefix: testPrefix},
	)
	peers, err := source(t.Context())
	require.NoError(t, err)
	assert.Empty(t, peers)
	assert.Empty(t, resolver.asked)
}
