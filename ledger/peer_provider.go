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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/peergov"
)

const defaultRelayCacheTTL = 1 * time.Minute

// LedgerPeerProviderAdapter implements peergov.LedgerPeerProvider using
// the ledger state and database to discover stake pool relays.
type LedgerPeerProviderAdapter struct {
	ledgerState *LedgerState
	db          *database.Database

	// Cache for pool relays
	cacheMu      sync.RWMutex
	cachedRelays []peergov.PoolRelay
	cacheTime    time.Time
	cacheTTL     time.Duration
	cacheGen     uint64
}

// NewLedgerPeerProvider creates a new LedgerPeerProvider adapter.
// Returns an error if ledgerState or db is nil.
func NewLedgerPeerProvider(
	ledgerState *LedgerState,
	db *database.Database,
	eventBus *event.EventBus,
) (*LedgerPeerProviderAdapter, error) {
	if ledgerState == nil {
		return nil, errors.New("ledgerState cannot be nil")
	}
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	adapter := &LedgerPeerProviderAdapter{
		ledgerState: ledgerState,
		db:          db,
		cacheTTL:    defaultRelayCacheTTL,
	}
	if eventBus != nil {
		eventBus.SubscribeFunc(
			PoolStateRestoredEventType,
			func(_ event.Event) {
				adapter.InvalidateCache()
			},
		)
	}
	return adapter, nil
}

// GetPoolRelays returns all active pool relays from the ledger.
func (p *LedgerPeerProviderAdapter) GetPoolRelays() (
	[]peergov.PoolRelay,
	error,
) {
	// Check cache first (read lock)
	p.cacheMu.RLock()
	if p.cachedRelays != nil && time.Since(p.cacheTime) < p.cacheTTL {
		result := copyPoolRelays(p.cachedRelays)
		p.cacheMu.RUnlock()
		return result, nil
	}
	genBefore := p.cacheGen
	p.cacheMu.RUnlock()

	// Cache miss or expired - fetch from database
	relays, err := p.db.GetActivePoolRelays(nil)
	if err != nil {
		return nil, fmt.Errorf("GetActivePoolRelays: fetch relays: %w", err)
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

	// Update cache (double-check under write lock)
	p.cacheMu.Lock()
	if p.cacheGen == genBefore &&
		(p.cachedRelays == nil || time.Since(p.cacheTime) >= p.cacheTTL) {
		p.cachedRelays = result
		p.cacheTime = time.Now()
	}
	p.cacheMu.Unlock()

	return copyPoolRelays(result), nil
}

// InvalidateCache clears the cached pool relays, forcing the next
// GetPoolRelays call to fetch fresh data from the database.
func (p *LedgerPeerProviderAdapter) InvalidateCache() {
	p.cacheMu.Lock()
	p.cachedRelays = nil
	p.cacheTime = time.Time{}
	p.cacheGen++
	p.cacheMu.Unlock()
}

// CurrentSlot returns the current chain tip slot number.
func (p *LedgerPeerProviderAdapter) CurrentSlot() uint64 {
	tip := p.ledgerState.Tip()
	return tip.Point.Slot
}

// copyPoolRelays returns a deep copy of the given relay slice so that
// callers cannot mutate cached state through shared IP pointers.
func copyPoolRelays(relays []peergov.PoolRelay) []peergov.PoolRelay {
	result := make([]peergov.PoolRelay, len(relays))
	for i, r := range relays {
		result[i] = peergov.PoolRelay{
			Hostname: r.Hostname,
			Port:     r.Port,
		}
		if r.IPv4 != nil {
			ipCopy := make(net.IP, len(*r.IPv4))
			copy(ipCopy, *r.IPv4)
			result[i].IPv4 = &ipCopy
		}
		if r.IPv6 != nil {
			ipCopy := make(net.IP, len(*r.IPv6))
			copy(ipCopy, *r.IPv6)
			result[i].IPv6 = &ipCopy
		}
	}
	return result
}

// Ensure LedgerPeerProviderAdapter implements LedgerPeerProvider
var _ peergov.LedgerPeerProvider = (*LedgerPeerProviderAdapter)(nil)
