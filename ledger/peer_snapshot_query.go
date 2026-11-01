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
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"sort"

	"github.com/blinklabs-io/dingo/database"
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
// pools before it has not exceeded this quota (so a pool whose predecessors
// total exactly the quota is still included). The value mirrors the
// bigLedgerPeerQuota default (0.9) used by ouroboros-network's
// PeerSelection.LedgerPeers. It is applied only when a client requests
// LedgerPeerKindBig; LedgerPeerKindAll returns every relay-advertising pool.
//
// Tuning the big-peer quota is part of the broader peer-governance roadmap
// and intentionally out of scope here; this constant only shapes the
// GetLedgerPeerSnapshot query result.
var bigLedgerPeerQuota = big.NewRat(9, 10)

// queryLedgerPeerSnapshot answers the LocalStateQuery GetLedgerPeerSnapshot
// query (NtC v19+). It returns the relay endpoints advertised by currently
// active stake pools together with each pool's relative and cumulative stake,
// which is exactly the data a dmq-node-compatible client needs for ledger
// peer discovery.
//
// The legacy request form ([34], NtC v19 to v22) is answered with the
// version 1 layout. The NtC v23 form ([34, kind]) is answered with version 2
// for big ledger peers or version 3 for all ledger peers, both of which also
// carry the tip's block hash and the network magic.
//
// Point-in-time behavior: the snapshot reads through txn, so inside a
// QueryView it describes the tip the view was acquired at; without one it
// describes the current tip. The reported slot is that tip's slot.
func (ls *LedgerState) queryLedgerPeerSnapshot(
	ctx context.Context,
	q *olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery,
	txn *database.Txn,
) (any, error) {
	var magic uint32
	if q.WithPeerKind {
		var err error
		magic, err = ls.peerSnapshotNetworkMagic()
		if err != nil {
			return nil, err
		}
	}
	txn, release := ls.readTxn(ctx, txn)
	defer release()

	// Read the tip from the same read transaction as the pool/stake data so
	// the reported snapshot slot describes the exact point the relays and
	// stake were observed at, even under a concurrent block apply or rollback.
	tip, err := ls.db.GetTip(txn)
	if err != nil {
		return nil, fmt.Errorf("GetLedgerPeerSnapshot: get tip: %w", err)
	}
	result, err := ls.ledgerPeerSnapshotAtTip(ctx, q.PeerKind, tip.Point, txn)
	if err != nil {
		return nil, err
	}
	if !q.WithPeerKind {
		return result, nil
	}
	return forLedgerPeerSnapshotV23(
		result,
		q.PeerKind,
		tip.Point,
		magic,
	)
}

// LedgerPeerSnapshot returns the relays that active pools registered, with
// their stake, as of the current tip: what GetLedgerPeerSnapshot reports for
// every ledger peer. It lets a component of the same process learn peers from
// the ledger without going through a local socket.
func (ls *LedgerState) LedgerPeerSnapshot(
	ctx context.Context,
) (olocalstatequery.LedgerPeerSnapshotResult, error) {
	txn, release := ls.readTxn(ctx, nil)
	defer release()
	tip, err := ls.db.GetTip(txn)
	if err != nil {
		return olocalstatequery.LedgerPeerSnapshotResult{}, fmt.Errorf(
			"ledger peer snapshot: get tip: %w", err,
		)
	}
	return ls.ledgerPeerSnapshotAtTip(
		ctx, olocalstatequery.LedgerPeerKindAll, tip.Point, txn,
	)
}

// peerSnapshotNetworkMagic returns the network magic the version 23 snapshot
// carries, taken from the Shelley genesis as GetGenesisConfig does.
func (ls *LedgerState) peerSnapshotNetworkMagic() (uint32, error) {
	cfg := ls.config.CardanoNodeConfig
	if cfg == nil || cfg.ShelleyGenesis() == nil {
		return 0, errors.New(
			"GetLedgerPeerSnapshot: shelley genesis unavailable for network magic",
		)
	}
	return cfg.ShelleyGenesis().NetworkMagic, nil
}

// forLedgerPeerSnapshotV23 turns a version-agnostic snapshot into the version
// 2 (big peers) or version 3 (all peers) result: it stamps the wire version,
// the tip's block hash and the network magic. Version 3 entries carry no
// accumulated stake. The block hash is nil at the genesis point, which
// encodes as [0].
func forLedgerPeerSnapshotV23(
	result olocalstatequery.LedgerPeerSnapshotResult,
	peerKind olocalstatequery.LedgerPeerKind,
	tip ocommon.Point,
	magic uint32,
) (olocalstatequery.LedgerPeerSnapshotResult, error) {
	result.NetworkMagic = magic
	result.BlockHash = nil
	if result.Slot.HasSlot {
		if len(tip.Hash) != lcommon.Blake2b256Size {
			return result, fmt.Errorf(
				"GetLedgerPeerSnapshot: tip hash is %d bytes, expected %d",
				len(tip.Hash),
				lcommon.Blake2b256Size,
			)
		}
		result.BlockHash = append([]byte(nil), tip.Hash...)
	}
	if peerKind == olocalstatequery.LedgerPeerKindBig {
		result.Version = 2
		return result, nil
	}
	result.Version = 3
	pools := make([]olocalstatequery.PoolLedgerPeers, len(result.Pools))
	for i, pool := range result.Pools {
		pool.AccumulatedStake = nil
		pools[i] = pool
	}
	result.Pools = pools
	return result, nil
}

// ledgerPeerSnapshotAtTip reads the active pools, their stake and relays
// through txn and assembles the version-agnostic snapshot for the tip.
func (ls *LedgerState) ledgerPeerSnapshotAtTip(
	ctx context.Context,
	peerKind olocalstatequery.LedgerPeerKind,
	tipPoint ocommon.Point,
	txn *database.Txn,
) (olocalstatequery.LedgerPeerSnapshotResult, error) {
	slot := withOriginSlot(tipPoint)
	empty := emptyLedgerPeerSnapshot(slot)

	pkhBytes, err := ls.db.Metadata().GetActivePoolKeyHashes(txn.Metadata())
	if err != nil {
		// Epoch data may not be synced yet for the current tip (early sync);
		// there are simply no ledger peers to report at that point.
		if errors.Is(err, types.ErrNoEpochData) {
			return empty, nil
		}
		return empty, fmt.Errorf(
			"GetLedgerPeerSnapshot: get active pools: %w",
			err,
		)
	}
	if len(pkhBytes) == 0 {
		return empty, nil
	}

	stakeByPool, _, err := ls.db.Metadata().GetStakeByPools(
		pkhBytes,
		txn.Metadata(),
	)
	if err != nil {
		return empty, fmt.Errorf(
			"GetLedgerPeerSnapshot: get stake by pools: %w",
			err,
		)
	}

	pkhs, err := poolKeyHashesFromActivePoolBytes(pkhBytes)
	if err != nil {
		return empty, err
	}
	pools, err := ls.db.GetPools(ctx, pkhs, txn)
	if err != nil {
		return empty, fmt.Errorf(
			"GetLedgerPeerSnapshot: get pools: %w",
			err,
		)
	}

	return assembleLedgerPeerSnapshot(slot, stakeByPool, pools, peerKind), nil
}

func poolKeyHashesFromActivePoolBytes(
	poolKeyHashBytes [][]byte,
) ([]lcommon.PoolKeyHash, error) {
	poolKeyHashes := make([]lcommon.PoolKeyHash, 0, len(poolKeyHashBytes))
	for _, raw := range poolKeyHashBytes {
		poolKeyHash, err := lcommon.NewBlake2b224Checked(raw)
		if err != nil {
			return nil, fmt.Errorf(
				"GetLedgerPeerSnapshot active pool key: %w",
				err,
			)
		}
		poolKeyHashes = append(poolKeyHashes, lcommon.PoolKeyHash(poolKeyHash))
	}
	return poolKeyHashes, nil
}

// emptyLedgerPeerSnapshot builds a well-formed empty snapshot at the given
// point. A node with no active pools still returns a valid result.
//
// Version 0 is gouroboros's "unset" sentinel (LedgerPeerSnapshotResult docs)
// and encodes as wire version 1, the layout of the legacy request form. The
// version 23 handler replaces it with version 2 or 3 (forLedgerPeerSnapshotV23)
// before the result leaves the ledger.
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
//     stake of every active pool (including pools without relays). Pools
//     missing from the map are treated as zero stake.
//   - pools carries the per-pool registration detail (including relays) for
//     the same active pools.
//
// Pools that advertise no usable relay are omitted from the result list. They
// contribute to the all-peer denominator, while the big-peer denominator is
// renormalized over pools that can actually be selected as peers.
func assembleLedgerPeerSnapshot(
	slot olocalstatequery.WithOriginSlot,
	stakeByPool map[string]uint64,
	pools []models.Pool,
	peerKind olocalstatequery.LedgerPeerKind,
) olocalstatequery.LedgerPeerSnapshotResult {
	result := emptyLedgerPeerSnapshot(slot)

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

	// All-peer snapshots retain the ledger distribution's denominator. Big
	// peer snapshots match the reference implementation by renormalizing over
	// pools with a usable relay, which are the only pools eligible for the set.
	totalStake := new(big.Int)
	if peerKind == olocalstatequery.LedgerPeerKindBig {
		for _, entry := range entries {
			totalStake.Add(totalStake, new(big.Int).SetUint64(entry.stake))
		}
	} else {
		for _, stake := range stakeByPool {
			totalStake.Add(totalStake, new(big.Int).SetUint64(stake))
		}
	}
	if totalStake.Sign() == 0 {
		return result
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
		// The reference set keeps taking pools while the stake accumulated by
		// their predecessors is at most the quota. A pool immediately after an
		// exact 0.9 boundary is therefore included.
		if peerKind == olocalstatequery.LedgerPeerKindBig &&
			acc.Cmp(bigLedgerPeerQuota) > 0 {
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
//   - SingleHostAddr  -> IPv4 and/or IPv6 set, port set (IPv4 takes
//     precedence when both address families are present).
//   - SingleHostName  -> hostname + port, no IP (an A/AAAA-record domain).
//   - MultiHostName   -> hostname only, no port (an SRV record; the resolver
//     supplies the port).
//
// Rows that carry no usable endpoint are dropped.
func relayAccessPoints(
	relay models.PoolRegistrationRelay,
) []olocalstatequery.RelayAccessPoint {
	// On-chain relay ports are Word16; the column is a wider uint, so guard
	// the narrowing conversion. An out-of-range value is malformed data and
	// is treated as no port.
	port := uint16(0)
	if relay.Port <= math.MaxUint16 {
		port = uint16(relay.Port)
	}
	// A SingleHostAddr without a usable port cannot become a ledger relay
	// access point. Hostname rows retain their Domain/SRV mapping below.
	if port == 0 && (relay.Ipv4 != nil || relay.Ipv6 != nil) {
		return nil
	}
	if relay.Ipv4 != nil {
		ip := append(net.IP(nil), *relay.Ipv4...)
		p := port
		return []olocalstatequery.RelayAccessPoint{{
			Kind: olocalstatequery.RelayKindIPv4,
			IPv4: &ip,
			Port: &p,
		}}
	}
	if relay.Ipv6 != nil {
		ip := append(net.IP(nil), *relay.Ipv6...)
		p := port
		return []olocalstatequery.RelayAccessPoint{{
			Kind: olocalstatequery.RelayKindIPv6,
			IPv6: &ip,
			Port: &p,
		}}
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
