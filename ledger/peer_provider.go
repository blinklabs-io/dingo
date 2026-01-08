// Copyright 2025 Blink Labs Software
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

package ledger

import (
	"errors"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/peergov"
)

// LedgerPeerProviderAdapter implements peergov.LedgerPeerProvider using
// the ledger state and database to discover stake pool relays.
type LedgerPeerProviderAdapter struct {
	ledgerState *LedgerState
	db          *database.Database
}

// NewLedgerPeerProvider creates a new LedgerPeerProvider adapter.
// Returns an error if ledgerState or db is nil.
func NewLedgerPeerProvider(
	ledgerState *LedgerState,
	db *database.Database,
) (*LedgerPeerProviderAdapter, error) {
	if ledgerState == nil {
		return nil, errors.New("ledgerState cannot be nil")
	}
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	return &LedgerPeerProviderAdapter{
		ledgerState: ledgerState,
		db:          db,
	}, nil
}

// GetPoolRelays returns all active pool relays from the ledger.
func (p *LedgerPeerProviderAdapter) GetPoolRelays() ([]peergov.PoolRelay, error) {
	relays, err := p.db.GetActivePoolRelays(nil)
	if err != nil {
		return nil, err
	}

	result := make([]peergov.PoolRelay, 0, len(relays))
	for _, relay := range relays {
		pr := peergov.PoolRelay{
			Hostname: relay.Hostname,
			Port:     relay.Port,
		}
		if relay.Ipv4 != nil {
			pr.IPv4 = relay.Ipv4
		}
		if relay.Ipv6 != nil {
			pr.IPv6 = relay.Ipv6
		}
		result = append(result, pr)
	}

	return result, nil
}

// CurrentSlot returns the current chain tip slot number.
func (p *LedgerPeerProviderAdapter) CurrentSlot() uint64 {
	tip := p.ledgerState.Tip()
	return tip.Point.Slot
}

// Ensure LedgerPeerProviderAdapter implements LedgerPeerProvider
var _ peergov.LedgerPeerProvider = (*LedgerPeerProviderAdapter)(nil)
