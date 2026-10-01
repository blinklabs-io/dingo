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
			Hostname: accessPoint.Address,
			Port:     accessPoint.Port,
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
	for _, relay := range weightedSample(relays, len(relays)) {
		for _, addr := range relay.Addresses() {
			if err := ctx.Err(); err != nil {
				return added
			}
			if p.ledgerPeerDeficit() <= 0 && added >= extraAdds {
				return added
			}
			if p.addLedgerPeerContext(ctx, addr, relay.Stake) {
				added++
			}
		}
	}
	return added
}
