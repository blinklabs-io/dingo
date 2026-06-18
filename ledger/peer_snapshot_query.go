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

package ledger

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"sort"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// bigLedgerPeerQuota is the cumulative relative-stake threshold that
// delimits the "big" ledger peers: walking pools from highest to lowest
// stake, a pool belongs to the big set while the stake accumulated by the
// pools before it has not yet reached this quota (so the first pool that
// reaches or crosses the quota is the last one included). The value mirrors
// the bigLedgerPeerQuota default (0.9) used by ouroboros-network's
// PeerSelection.LedgerPeers. It is applied only when a client requests
// LedgerPeerKindBig; LedgerPeerKindAll returns every relay-advertising pool.
//
// Tuning the big-peer quota is part of the broader peer-governance roadmap
// (#1787-#1796) and intentionally out of scope here; this constant only
// shapes the GetLedgerPeerSnapshot query result.
var bigLedgerPeerQuota = big.NewRat(9, 10)

// queryLedgerPeerSnapshot answers the LocalStateQuery GetLedgerPeerSnapshot
// query (NtC v19+). It returns the relay endpoints advertised by currently
// active stake pools together with each pool's relative and cumulative stake,
// which is exactly the data a dmq-node-compatible client needs for ledger
// peer discovery.
//
// Point-in-time behavior: LocalStateQuery Acquire/Release are still no-ops
// pending ViewManager snapshot isolation (#382), so the snapshot reflects the
// current chain tip rather than the acquired point. The reported slot is the
// current tip slot. When that isolation lands, only the data-sourcing here
// needs to observe the acquired view; the query surface stays the same.
func (ls *LedgerState) queryLedgerPeerSnapshot(
	peerKind olocalstatequery.LedgerPeerKind,
) (any, error) {
	txn := ls.db.Transaction(false)
	defer txn.Release()

	// Read the tip from the same read transaction as the pool/stake data so
	// the reported snapshot slot describes the exact point the relays and
	// stake were observed at, even under a concurrent block apply or rollback.
	tip, err := ls.db.GetTip(txn)
	if err != nil {
		return nil, fmt.Errorf("GetLedgerPeerSnapshot: get tip: %w", err)
	}
	slot := withOriginSlot(tip.Point)

	pkhBytes, err := ls.db.Metadata().GetActivePoolKeyHashes(txn.Metadata())
	if err != nil {
		// Epoch data may not be synced yet for the current tip (early sync);
		// there are simply no ledger peers to report at that point.
		if errors.Is(err, types.ErrNoEpochData) {
			return emptyLedgerPeerSnapshot(slot), nil
		}
		return nil, fmt.Errorf(
			"GetLedgerPeerSnapshot: get active pools: %w",
			err,
		)
	}
	if len(pkhBytes) == 0 {
		return emptyLedgerPeerSnapshot(slot), nil
	}

	stakeByPool, _, err := ls.db.Metadata().GetStakeByPools(
		pkhBytes,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"GetLedgerPeerSnapshot: get stake by pools: %w",
			err,
		)
	}

	pkhs := make([]lcommon.PoolKeyHash, 0, len(pkhBytes))
	for _, b := range pkhBytes {
		pkhs = append(pkhs, lcommon.PoolKeyHash(lcommon.NewBlake2b224(b)))
	}
	pools, err := ls.db.GetPools(pkhs, txn)
	if err != nil {
		return nil, fmt.Errorf("GetLedgerPeerSnapshot: get pools: %w", err)
	}

	return assembleLedgerPeerSnapshot(slot, stakeByPool, pools, peerKind), nil
}

// emptyLedgerPeerSnapshot builds a well-formed empty (V1) snapshot at the
// given point. A node with no active pools still returns a valid result.
func emptyLedgerPeerSnapshot(
	slot olocalstatequery.WithOriginSlot,
) olocalstatequery.LedgerPeerSnapshotResult {
	return olocalstatequery.LedgerPeerSnapshotResult{
		Version: 0, // LedgerPeerSnapshotV1
		Slot:    slot,
		Pools:   []olocalstatequery.PoolLedgerPeers{},
	}
}

// withOriginSlot encodes a chain point as a WithOrigin SlotNo. The origin
// point (slot 0 with an empty hash) maps to Origin; a real slot-0 block
// (slot 0 with a non-empty hash) is reported as At slot 0.
func withOriginSlot(point ocommon.Point) olocalstatequery.WithOriginSlot {
	return olocalstatequery.WithOriginSlot{
		HasSlot: point.Slot != 0 || len(point.Hash) > 0,
		Slot:    point.Slot,
	}
}

// poolLedgerPeer is an intermediate, sortable view of a single pool's
// contribution to the snapshot.
type poolLedgerPeer struct {
	keyHash []byte
	stake   uint64
	relays  []olocalstatequery.RelayAccessPoint
}

// assembleLedgerPeerSnapshot is the pure mapping from ledger/database state to
// a LedgerPeerSnapshot result. It is split out from queryLedgerPeerSnapshot so
// the stake math, ordering, big-peer selection, and relay mapping can be
// tested without a live database.
//
//   - stakeByPool is keyed by string(poolKeyHash) and holds the delegated
//     stake of every active pool (including pools without relays). Its sum is
//     the denominator for relative stake; pools missing from the map are
//     treated as zero stake.
//   - pools carries the per-pool registration detail (including relays) for
//     the same active pools.
//
// Pools that advertise no usable relay are omitted from the result list but
// still contribute to the total-stake denominator, matching the Cardano
// ledger's pool-distribution semantics.
func assembleLedgerPeerSnapshot(
	slot olocalstatequery.WithOriginSlot,
	stakeByPool map[string]uint64,
	pools []models.Pool,
	peerKind olocalstatequery.LedgerPeerKind,
) olocalstatequery.LedgerPeerSnapshotResult {
	result := emptyLedgerPeerSnapshot(slot)

	// Total stake across all active pools is the relative-stake denominator.
	totalStake := new(big.Int)
	for _, s := range stakeByPool {
		totalStake.Add(totalStake, new(big.Int).SetUint64(s))
	}
	if totalStake.Sign() == 0 {
		// No measurable stake yet (e.g. early chain); nothing to weight.
		return result
	}

	entries := make([]poolLedgerPeer, 0, len(pools))
	for _, pool := range pools {
		relays := relayAccessPointsFromPool(pool)
		if len(relays) == 0 {
			continue
		}
		entries = append(entries, poolLedgerPeer{
			keyHash: pool.PoolKeyHash,
			stake:   stakeByPool[string(pool.PoolKeyHash)],
			relays:  relays,
		})
	}

	// Order by descending stake (the big-peer accumulation order), with the
	// pool key hash as a deterministic tie-breaker.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].stake != entries[j].stake {
			return entries[i].stake > entries[j].stake
		}
		return bytes.Compare(entries[i].keyHash, entries[j].keyHash) < 0
	})

	acc := new(big.Rat)
	for _, e := range entries {
		// For the big-peer set, stop once the stake accumulated by the
		// preceding pools has reached the quota. The pool that first reaches
		// or crosses the quota is the last one included; pools after it
		// (including zero-stake relayed pools sitting exactly on a 0.9
		// boundary) are excluded.
		if peerKind == olocalstatequery.LedgerPeerKindBig &&
			acc.Cmp(bigLedgerPeerQuota) >= 0 {
			break
		}
		poolStake := new(big.Rat).SetFrac(
			new(big.Int).SetUint64(e.stake),
			totalStake,
		)
		acc = new(big.Rat).Add(acc, poolStake)
		result.Pools = append(result.Pools, olocalstatequery.PoolLedgerPeers{
			AccumulatedStake: &cbor.Rat{Rat: new(big.Rat).Set(acc)},
			Detail: olocalstatequery.PoolLedgerPeersDetail{
				PoolStake: &cbor.Rat{Rat: poolStake},
				Relays:    e.relays,
			},
		})
	}
	return result
}

// relayAccessPointsFromPool extracts the relays of a pool's most recent
// registration and maps each stored relay row to one or more
// RelayAccessPoint values. GetPools orders Registration newest-first, so
// Registration[0] is the active registration.
func relayAccessPointsFromPool(
	pool models.Pool,
) []olocalstatequery.RelayAccessPoint {
	if len(pool.Registration) == 0 {
		return nil
	}
	stored := pool.Registration[0].Relays
	out := make([]olocalstatequery.RelayAccessPoint, 0, len(stored))
	for _, relay := range stored {
		out = append(out, relayAccessPoints(relay)...)
	}
	return out
}

// relayAccessPoints maps a single stored relay row to RelayAccessPoint(s).
//
// Dingo normalises each on-chain PoolRelay into one row carrying optional
// IPv4/IPv6 addresses, an optional hostname, and a port:
//
//   - SingleHostAddr  -> IPv4 and/or IPv6 set, port set (a dual-stack relay
//     yields one access point per address family).
//   - SingleHostName  -> hostname + port, no IP (an A/AAAA-record domain).
//   - MultiHostName   -> hostname only, no port (an SRV record; the resolver
//     supplies the port).
//
// Rows that carry no usable endpoint are dropped.
func relayAccessPoints(
	relay models.PoolRegistrationRelay,
) []olocalstatequery.RelayAccessPoint {
	var out []olocalstatequery.RelayAccessPoint
	// On-chain relay ports are Word16; the column is a wider uint, so guard
	// the narrowing conversion. An out-of-range value is malformed data and
	// is treated as no port.
	port := uint16(0)
	if relay.Port <= math.MaxUint16 {
		port = uint16(relay.Port)
	}
	if relay.Ipv4 != nil {
		ip := append(net.IP(nil), *relay.Ipv4...)
		p := port
		out = append(out, olocalstatequery.RelayAccessPoint{
			Kind: olocalstatequery.RelayKindIPv4,
			IPv4: &ip,
			Port: &p,
		})
	}
	if relay.Ipv6 != nil {
		ip := append(net.IP(nil), *relay.Ipv6...)
		p := port
		out = append(out, olocalstatequery.RelayAccessPoint{
			Kind: olocalstatequery.RelayKindIPv6,
			IPv6: &ip,
			Port: &p,
		})
	}
	if len(out) > 0 {
		return out
	}
	if relay.Hostname == "" {
		return nil
	}
	host := relay.Hostname
	// Branch on the validated port, not relay.Port: a malformed out-of-range
	// port narrows to 0 above, so such a row falls through to MultiHostName
	// rather than emitting a SingleHostName with a bogus port 0.
	if port != 0 {
		// SingleHostName: a domain resolved directly, with an explicit port.
		p := port
		return []olocalstatequery.RelayAccessPoint{{
			Kind:   olocalstatequery.RelayKindDomain,
			Domain: &host,
			Port:   &p,
		}}
	}
	// MultiHostName: an SRV record; the port comes from the SRV lookup.
	return []olocalstatequery.RelayAccessPoint{{
		Kind:   olocalstatequery.RelayKindSRV,
		Domain: &host,
	}}
}
