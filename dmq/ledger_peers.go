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
	"io"
	"log/slog"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

const (
	// defaultLedgerLookupTimeout bounds one SRV lookup when the caller sets
	// none.
	defaultLedgerLookupTimeout = 5 * time.Second
	// ledgerLookupConcurrency bounds the SRV lookups in flight at once.
	ledgerLookupConcurrency = 8
)

// SRVResolver looks up DNS SRV records. *net.Resolver satisfies it.
type SRVResolver interface {
	LookupSRV(
		ctx context.Context,
		service, proto, name string,
	) (string, []*net.SRV, error)
}

// LedgerRelayDomains returns the domains of the SRV relays that pools
// registered, in snapshot order and without repeats. Only SRV relays can name
// a DMQ endpoint: an IP or domain relay carries the pool's Cardano port, not
// the port a DMQ node listens on, so those are left out.
func LedgerRelayDomains(
	snapshot olocalstatequery.LedgerPeerSnapshotResult,
) []string {
	seen := make(map[string]struct{})
	var domains []string
	for _, pool := range snapshot.Pools {
		for _, relay := range pool.Detail.Relays {
			if relay.Kind != olocalstatequery.RelayKindSRV ||
				relay.Domain == nil {
				continue
			}
			domain := strings.TrimSuffix(
				strings.ToLower(strings.TrimSpace(*relay.Domain)), ".",
			)
			if domain == "" {
				continue
			}
			if _, ok := seen[domain]; ok {
				continue
			}
			seen[domain] = struct{}{}
			domains = append(domains, domain)
		}
	}
	return domains
}

// LedgerPeerLookup configures ResolveLedgerPeers.
type LedgerPeerLookup struct {
	// Resolver answers the SRV queries. Required.
	Resolver SRVResolver
	// Prefix is prepended to each domain, for example the result of
	// TopicSRVPrefix. Required.
	Prefix string
	// Timeout bounds one lookup. Zero uses five seconds.
	Timeout time.Duration
	Logger  *slog.Logger
}

// ResolveLedgerPeers looks up the DMQ peers advertised under each domain's SRV
// record. Peers come back in domain order, and within a domain by priority
// (lowest first), then weight (highest first), then name and port, so the
// result is the same for the same DNS answers. A domain whose lookup fails or
// has no usable record is skipped and logged, because a pool that registered
// an SRV relay need not run a DMQ node. An SRV record whose target is "." says
// the service is unavailable and is skipped, as is a record with port 0.
//
// It returns an error only when ctx ends before the lookups finish.
func ResolveLedgerPeers(
	ctx context.Context,
	lookup LedgerPeerLookup,
	domains []string,
) ([]Peer, error) {
	if lookup.Resolver == nil {
		return nil, errors.New("dmq: ledger peer lookup requires a resolver")
	}
	if lookup.Prefix == "" {
		return nil, errors.New("dmq: ledger peer lookup requires an SRV prefix")
	}
	logger := lookup.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	timeout := lookup.Timeout
	if timeout <= 0 {
		timeout = defaultLedgerLookupTimeout
	}
	// Each lookup writes its own slot, so the result keeps domain order
	// without a lock.
	found := make([][]Peer, len(domains))
	slots := make(chan struct{}, ledgerLookupConcurrency)
	var wg sync.WaitGroup
	for i, domain := range domains {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Go(func() {
			defer func() { <-slots }()
			found[i] = lookupDomainPeers(ctx, lookup, timeout, domain, logger)
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("dmq: resolve ledger peers: %w", err)
	}
	topo := &Topology{PublicRoots: make([]RootGroup, 0, len(found))}
	for _, peers := range found {
		topo.PublicRoots = append(topo.PublicRoots, RootGroup{Peers: peers})
	}
	return topo.Peers(), nil
}

// LedgerPeerSource returns a function for PeerConfig.LedgerPeers. Each call
// reads a snapshot, takes the domains of the SRV relays pools registered in
// it, and looks up the topic's DMQ peers under each. The snapshot's own error
// is returned as it is, so a caller can tell a ledger that cannot answer yet
// from a lookup that found nothing.
func LedgerPeerSource(
	snapshot func(
		context.Context,
	) (olocalstatequery.LedgerPeerSnapshotResult, error),
	lookup LedgerPeerLookup,
) func(context.Context) ([]Peer, error) {
	return func(ctx context.Context) ([]Peer, error) {
		snap, err := snapshot(ctx)
		if err != nil {
			return nil, err
		}
		domains := LedgerRelayDomains(snap)
		if len(domains) == 0 {
			return nil, nil
		}
		return ResolveLedgerPeers(ctx, lookup, domains)
	}
}

func lookupDomainPeers(
	ctx context.Context,
	lookup LedgerPeerLookup,
	timeout time.Duration,
	domain string,
	logger *slog.Logger,
) []Peer {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	name := lookup.Prefix + "." + domain
	// The name already carries its service and protocol labels, so none are
	// passed separately.
	_, records, err := lookup.Resolver.LookupSRV(ctx, "", "", name)
	if err != nil {
		logger.Debug(
			"dmq ledger peer SRV lookup failed",
			"name", name, "error", err,
		)
		return nil
	}
	// The resolver may hand back a slice it keeps, so sort a copy.
	records = slices.Clone(records)
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i], records[j]
		switch {
		case a.Priority != b.Priority:
			return a.Priority < b.Priority
		case a.Weight != b.Weight:
			return a.Weight > b.Weight
		case a.Target != b.Target:
			return a.Target < b.Target
		default:
			return a.Port < b.Port
		}
	})
	peers := make([]Peer, 0, len(records))
	for _, record := range records {
		host := strings.TrimSuffix(record.Target, ".")
		if host == "" || record.Port == 0 {
			continue
		}
		peers = append(peers, Peer{Host: host, Port: uint(record.Port)})
	}
	return peers
}
