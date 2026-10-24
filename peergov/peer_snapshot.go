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
	"net"

	"github.com/blinklabs-io/dingo/topology"
)

// LoadPeerSnapshot seeds ledger peers from cardano-node's peer-snapshot.json.
// It is intended for Ouroboros Genesis startup, where the snapshot provides
// historical ledger peers before the local ledger can answer relay queries.
func (p *PeerGovernor) LoadPeerSnapshot(
	ctx context.Context,
	snapshot *topology.PeerSnapshotConfig,
) int {
	if p == nil || snapshot == nil || !snapshot.HasRelays() ||
		p.config.DisableOutbound {
		return 0
	}
	relays := PoolRelaysFromPeerSnapshot(snapshot)
	p.mu.Lock()
	p.peerSnapshotRelays = relays
	p.mu.Unlock()
	added := p.addLedgerRelaysContext(ctx, relays, 0)
	p.config.Logger.Info(
		"loaded peer snapshot",
		"snapshot_slot", snapshot.Point.BlockPointSlot,
		"relay_candidates", len(relays),
		"added", added,
	)
	return added
}

func PoolRelaysFromPeerSnapshot(
	snapshot *topology.PeerSnapshotConfig,
) []PoolRelay {
	if snapshot == nil {
		return nil
	}
	accessPoints := snapshot.RelayAccessPoints()
	relays := make([]PoolRelay, 0, len(accessPoints))
	for _, accessPoint := range accessPoints {
		if relay, ok := poolRelayFromSnapshotAccessPoint(accessPoint); ok {
			relays = append(relays, relay)
		}
	}
	return relays
}

func poolRelayFromSnapshotAccessPoint(
	accessPoint topology.TopologyConfigP2PAccessPoint,
) (PoolRelay, bool) {
	if accessPoint.Address == "" {
		return PoolRelay{}, false
	}
	ip := net.ParseIP(accessPoint.Address)
	if ip == nil {
		return PoolRelay{
			Hostname:    accessPoint.Address,
			Port:        accessPoint.Port,
			IsMultiHost: accessPoint.Port == 0,
		}, true
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		ipCopy := append(net.IP(nil), ipv4...)
		return PoolRelay{
			IPv4: &ipCopy,
			Port: accessPoint.Port,
		}, true
	}
	ipCopy := append(net.IP(nil), ip...)
	return PoolRelay{
		IPv6: &ipCopy,
		Port: accessPoint.Port,
	}, true
}

func (p *PeerGovernor) addLedgerRelaysContext(
	ctx context.Context,
	relays []PoolRelay,
	extraAdds int,
) int {
	// Order every relay rather than only the deficit: an address can be
	// denied, unroutable or already known, so the walk below must be able to
	// continue past it until the deficit is filled.
	added := 0
	ordered := weightedSample(relays, len(relays))
	addresses := make([][]string, len(ordered))
	origins := make([][]string, len(ordered))
	maxAddresses := 0
	for i, relay := range ordered {
		addresses[i] = relay.Addresses()
		origins[i] = append([]string(nil), addresses[i]...)
		maxAddresses = max(maxAddresses, len(addresses[i]))
	}
	seen := make(map[string]struct{})
	// Offer one address per relay before a dual-stack relay gets an alternate.
	for round := 0; round < maxAddresses; round++ {
		for i, relay := range ordered {
			if round >= len(addresses[i]) {
				continue
			}
			addr := addresses[i][round]
			normalized := p.normalizeAddress(addr)
			if _, duplicate := seen[normalized]; duplicate {
				continue
			}
			seen[normalized] = struct{}{}
			if err := ctx.Err(); err != nil {
				return added
			}
			if p.ledgerPeerDeficit() <= 0 && added >= extraAdds {
				return added
			}
			origin := origins[i][round]
			host, _, err := net.SplitHostPort(addr)
			if err == nil && net.ParseIP(host) != nil && addr == origin {
				if p.addLedgerPeerContext(ctx, addr, relay.Stake, relay.StakeKnown) {
					added++
				}
				continue
			}
			if err == nil && net.ParseIP(host) == nil {
				p.mu.Lock()
				denied := p.isDeniedLocked(p.normalizeAddress(origin))
				p.mu.Unlock()
				if denied {
					continue
				}
				resolved := p.resolveLedgerDiscoveryAddresses(ctx, addr)
				addr = resolved[0]
				addresses[i][round] = addr
				for _, alternate := range resolved[1:] {
					addresses[i] = append(addresses[i], alternate)
					origins[i] = append(origins[i], origin)
				}
				maxAddresses = max(maxAddresses, len(addresses[i]))
				resolvedKey := p.normalizeAddress(addr)
				if resolvedKey != normalized {
					if _, duplicate := seen[resolvedKey]; duplicate {
						continue
					}
					seen[resolvedKey] = struct{}{}
				}
			}
			if p.addResolvedLedgerPeerContext(
				ctx,
				addr,
				p.normalizeAddress(addr),
				origin,
				relay.Stake,
				relay.StakeKnown,
			) {
				added++
			}
		}
	}
	return added
}
