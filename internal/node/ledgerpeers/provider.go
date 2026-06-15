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

package ledgerpeers

import (
	"net"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/peergov"
)

type relayProvider interface {
	GetPoolRelays() ([]ledger.PoolRelay, error)
	CurrentSlot() uint64
}

// Provider adapts ledger relay data to the peer-governance provider interface.
type Provider struct {
	relays relayProvider
}

// NewProvider creates a peer-governance ledger peer provider from a neutral
// ledger relay provider.
func NewProvider(relays relayProvider) *Provider {
	return &Provider{
		relays: relays,
	}
}

// GetPoolRelays returns active stake pool relays in peer-governance terms.
func (p *Provider) GetPoolRelays() ([]peergov.PoolRelay, error) {
	relays, err := p.relays.GetPoolRelays()
	if err != nil {
		return nil, err
	}
	return toPeerGovRelays(relays), nil
}

// CurrentSlot returns the current chain tip slot number.
func (p *Provider) CurrentSlot() uint64 {
	return p.relays.CurrentSlot()
}

func toPeerGovRelays(relays []ledger.PoolRelay) []peergov.PoolRelay {
	result := make([]peergov.PoolRelay, len(relays))
	for i, relay := range relays {
		result[i] = peergov.PoolRelay{
			Hostname: relay.Hostname,
			Port:     relay.Port,
			IPv4:     copyIP(relay.IPv4),
			IPv6:     copyIP(relay.IPv6),
		}
	}
	return result
}

func copyIP(ip *net.IP) *net.IP {
	if ip == nil {
		return nil
	}
	ipCopy := make(net.IP, len(*ip))
	copy(ipCopy, *ip)
	return &ipCopy
}

var _ peergov.LedgerPeerProvider = (*Provider)(nil)
