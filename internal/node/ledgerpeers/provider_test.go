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
	"errors"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type testRelayProvider struct {
	relays []ledger.PoolRelay
	slot   uint64
	err    error
}

func (p *testRelayProvider) GetPoolRelays() ([]ledger.PoolRelay, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.relays, nil
}

func (p *testRelayProvider) CurrentSlot() uint64 {
	return p.slot
}

func TestProviderConvertsLedgerRelays(t *testing.T) {
	ipv4 := net.ParseIP("44.0.1.1").To4()
	ipv6 := net.ParseIP("2001:db8::1")
	provider := NewProvider(&testRelayProvider{
		relays: []ledger.PoolRelay{
			{
				Hostname: "relay.example.com",
				Port:     3002,
			},
			{
				IPv4: &ipv4,
				Port: 3003,
			},
			{
				IPv6: &ipv6,
				Port: 3004,
			},
			{
				Hostname: "default-port.example.com",
			},
		},
	})

	relays, err := provider.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, relays, 4)

	require.Equal(t, "relay.example.com", relays[0].Hostname)
	require.Equal(t, uint(3002), relays[0].Port)

	require.NotNil(t, relays[1].IPv4)
	require.Equal(t, net.ParseIP("44.0.1.1").To4(), *relays[1].IPv4)
	require.Equal(t, uint(3003), relays[1].Port)

	require.NotNil(t, relays[2].IPv6)
	require.Equal(t, net.ParseIP("2001:db8::1"), *relays[2].IPv6)
	require.Equal(t, uint(3004), relays[2].Port)

	require.Equal(t, "default-port.example.com", relays[3].Hostname)
	require.Zero(t, relays[3].Port)
}

func TestProviderCurrentSlot(t *testing.T) {
	provider := NewProvider(&testRelayProvider{slot: 12345})

	require.Equal(t, uint64(12345), provider.CurrentSlot())
}

func TestProviderReturnsRelayCopy(t *testing.T) {
	ipv4 := net.ParseIP("44.0.1.1").To4()
	source := &testRelayProvider{
		relays: []ledger.PoolRelay{
			{
				Hostname: "relay.example.com",
				IPv4:     &ipv4,
				Port:     3001,
			},
		},
	}
	provider := NewProvider(source)

	relays, err := provider.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, relays, 1)
	relays[0].Hostname = "mutated.example.com"
	(*relays[0].IPv4)[0] = 99

	require.Equal(t, "relay.example.com", source.relays[0].Hostname)
	require.Equal(t, net.ParseIP("44.0.1.1").To4(), *source.relays[0].IPv4)
}

func TestProviderReturnsRelayProviderError(t *testing.T) {
	expectedErr := errors.New("relay read failed")
	provider := NewProvider(&testRelayProvider{err: expectedErr})

	relays, err := provider.GetPoolRelays()
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, relays)
}
