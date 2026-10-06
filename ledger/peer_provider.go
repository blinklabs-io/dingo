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
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
)

const defaultRelayCacheTTL = 1 * time.Minute

// PoolRelay represents a stake pool relay as exposed by the ledger/database
// boundary.
type PoolRelay struct {
	Hostname string
	IPv4     *net.IP
	IPv6     *net.IP
	Port     uint
	// PoolKeyHash identifies the pool that registered this relay.
	PoolKeyHash []byte
	// Stake is the delegated stake, in lovelace, of the pool owning the
	// relay. StakeKnown reports whether the lookup succeeded.
	Stake uint64
	// StakeKnown distinguishes a successful zero-stake lookup from absent data.
	StakeKnown bool
	// IsMultiHost marks a MultiHostName relay: a hostname with no port,
	// whose port comes from an SRV record.
	IsMultiHost bool
}

// PoolRelayProvider exposes active stake pool relays from the ledger/database
// without depending on peer-governance policy types.
type PoolRelayProvider struct {
	ledgerState *LedgerState
	db          *database.Database

	// eventBus and subID let Close unsubscribe the cache-invalidation
	// handler below. Without this, a live database restore/truncate
	// (node_lifecycle.go), which constructs a fresh PoolRelayProvider on
	// every cycle, leaks one more permanently-active EventBus subscription
	// per cycle -- each pointing at an otherwise-unreachable, abandoned
	// provider instance.
	eventBus *event.EventBus
	subID    event.EventSubscriberId

	// stakeByPools returns delegated stake keyed by pool key hash. It is a
	// field so a test can make the lookup fail, which the database does not
	// do on demand.
	stakeByPools func(poolKeyHashes [][]byte) (map[string]uint64, error)

	// Cache for pool relays
	cacheMu      sync.RWMutex
	cachedRelays []PoolRelay
	cacheTime    time.Time
	cacheTTL     time.Duration
	cacheGen     uint64
}

// NewPoolRelayProvider creates a new ledger pool relay provider.
// Returns an error if ledgerState or db is nil.
func NewPoolRelayProvider(
	ledgerState *LedgerState,
	db *database.Database,
	eventBus *event.EventBus,
) (*PoolRelayProvider, error) {
	if ledgerState == nil {
		return nil, errors.New("ledgerState cannot be nil")
	}
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	provider := &PoolRelayProvider{
		ledgerState: ledgerState,
		db:          db,
		cacheTTL:    defaultRelayCacheTTL,
		eventBus:    eventBus,
	}
	provider.stakeByPools = func(
		poolKeyHashes [][]byte,
	) (map[string]uint64, error) {
		stakes, _, err := db.GetStakeByPools(poolKeyHashes, nil)
		return stakes, err
	}
	if eventBus != nil {
		provider.subID = eventBus.SubscribeFunc(
			PoolStateRestoredEventType,
			func(_ event.Event) {
				provider.InvalidateCache()
			},
		)
	}
	return provider, nil
}

// Close unsubscribes the cache-invalidation handler registered in
// NewPoolRelayProvider. Safe to call on a provider constructed with a nil
// eventBus (no-op) and safe to call more than once.
func (p *PoolRelayProvider) Close() {
	if p.eventBus == nil || p.subID == 0 {
		return
	}
	p.eventBus.UnsubscribeAndWait(PoolStateRestoredEventType, p.subID)
	p.subID = 0
}

// GetPoolRelays returns all active pool relays from the ledger.
func (p *PoolRelayProvider) GetPoolRelays() (
	[]PoolRelay,
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

	// Stake only weights peer sampling, so a failed lookup degrades to
	// unweighted discovery instead of failing it.
	stakes, err := p.lookupStake(relays)
	if err != nil {
		slog.Warn(
			"failed to fetch pool stake for ledger relays",
			"component", "ledger",
			"error", err,
		)
	}

	result := make([]PoolRelay, 0, len(relays))
	for _, relay := range relays {
		_, known := stakes[string(relay.PoolKeyHash)]
		pr := PoolRelay{
			Hostname: relay.Hostname,
			Port:     relay.Port,
			PoolKeyHash: append(
				[]byte(nil), relay.PoolKeyHash...,
			),
			Stake:      stakes[string(relay.PoolKeyHash)],
			StakeKnown: known && err == nil,
			// The relay row stores no relay type, so a hostname with no port
			// is treated as MultiHostName. A SingleHostName registered with
			// a null port matches too; its SRV lookup finds nothing and it
			// falls back to the default port it would have dialed anyway.
			IsMultiHost: relay.Hostname != "" &&
				relay.Port == 0 &&
				relay.Ipv4 == nil &&
				relay.Ipv6 == nil,
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

// lookupStake batch-fetches the delegated stake of every pool owning one of
// relays. The returned map is nil on error.
func (p *PoolRelayProvider) lookupStake(
	relays []models.PoolRegistrationRelay,
) (map[string]uint64, error) {
	seen := make(map[string]struct{}, len(relays))
	hashes := make([][]byte, 0, len(relays))
	for _, relay := range relays {
		key := string(relay.PoolKeyHash)
		if len(relay.PoolKeyHash) == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		hashes = append(hashes, relay.PoolKeyHash)
	}
	if len(hashes) == 0 {
		return nil, nil
	}
	return p.stakeByPools(hashes)
}

// InvalidateCache clears the cached pool relays, forcing the next
// GetPoolRelays call to fetch fresh data from the database.
func (p *PoolRelayProvider) InvalidateCache() {
	p.cacheMu.Lock()
	p.cachedRelays = nil
	p.cacheTime = time.Time{}
	p.cacheGen++
	p.cacheMu.Unlock()
}

// CurrentSlot returns the current chain tip slot number.
func (p *PoolRelayProvider) CurrentSlot() uint64 {
	tip := p.ledgerState.Tip()
	return tip.Point.Slot
}

// copyPoolRelays returns a deep copy of the given relay slice so that
// callers cannot mutate cached state through shared IP pointers.
func copyPoolRelays(relays []PoolRelay) []PoolRelay {
	result := make([]PoolRelay, len(relays))
	for i, r := range relays {
		result[i] = PoolRelay{
			Hostname:    r.Hostname,
			Port:        r.Port,
			PoolKeyHash: append([]byte(nil), r.PoolKeyHash...),
			Stake:       r.Stake,
			StakeKnown:  r.StakeKnown,
			IsMultiHost: r.IsMultiHost,
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
