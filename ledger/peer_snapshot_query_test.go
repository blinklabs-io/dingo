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
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poolKeyHash28 builds a readable 28-byte pool key hash from a byte pattern.
func poolKeyHash28(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

// poolWithRelays builds a models.Pool whose latest registration carries the
// given relays, mirroring what GetPools returns (Registration newest-first).
func poolWithRelays(
	keyHash []byte,
	relays ...models.PoolRegistrationRelay,
) models.Pool {
	return models.Pool{
		PoolKeyHash: keyHash,
		Registration: []models.PoolRegistration{
			{Relays: relays},
		},
	}
}

func ipv4Relay(ip string, port uint) models.PoolRegistrationRelay {
	parsed := net.ParseIP(ip).To4()
	return models.PoolRegistrationRelay{Ipv4: &parsed, Port: port}
}

func ipv6Relay(ip string, port uint) models.PoolRegistrationRelay {
	parsed := net.ParseIP(ip)
	return models.PoolRegistrationRelay{Ipv6: &parsed, Port: port}
}

// atSlot builds a WithOrigin SlotNo for a real (non-origin) slot.
func atSlot(slot uint64) olocalstatequery.WithOriginSlot {
	return olocalstatequery.WithOriginSlot{HasSlot: true, Slot: slot}
}

// --- dispatch + empty snapshot --------------------------------------------

// TestQueryLedgerPeerSnapshotDispatch proves the query routes through
// ls.Query -> queryBlock -> queryShelley to the snapshot handler and returns
// a well-formed empty (V1) result against an empty database, with the slot
// taken from the current tip.
func TestQueryLedgerPeerSnapshotDispatch(t *testing.T) {
	ls := &LedgerState{db: newTestDB(t)}
	// The handler reads the snapshot slot from the same DB transaction as the
	// pool data, so seed the tip in the database rather than in-memory state.
	require.NoError(t, ls.db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(4242, []byte("tip"))},
		nil,
	))

	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
				PeerKind: olocalstatequery.LedgerPeerKindAll,
			},
		},
	}

	result, err := ls.Query(query)
	require.NoError(t, err)

	snapshot, ok := result.(olocalstatequery.LedgerPeerSnapshotResult)
	require.True(t, ok, "expected LedgerPeerSnapshotResult, got %T", result)
	assert.Equal(t, uint64(0), snapshot.Version, "snapshot is V1 (version 0)")
	assert.True(t, snapshot.Slot.HasSlot)
	assert.Equal(t, uint64(4242), snapshot.Slot.Slot)
	assert.Empty(t, snapshot.Pools, "empty DB yields no pools")
}

// TestQueryLedgerPeerSnapshotEmptyAtOrigin proves a node still at chain origin
// (tip slot 0) reports a WithOrigin/Origin slot and no pools.
func TestQueryLedgerPeerSnapshotEmptyAtOrigin(t *testing.T) {
	ls := &LedgerState{db: newTestDB(t)}

	result, err := ls.queryLedgerPeerSnapshot(olocalstatequery.LedgerPeerKindAll)
	require.NoError(t, err)

	snapshot := result.(olocalstatequery.LedgerPeerSnapshotResult)
	assert.False(t, snapshot.Slot.HasSlot, "tip slot 0 maps to Origin")
	assert.Empty(t, snapshot.Pools)
}

// TestQueryLedgerPeerSnapshotRealSlotZero proves a real slot-0 block (slot 0
// with a non-empty hash) is reported as At slot 0, not collapsed to Origin.
func TestQueryLedgerPeerSnapshotRealSlotZero(t *testing.T) {
	ls := &LedgerState{db: newTestDB(t)}
	require.NoError(t, ls.db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(0, []byte("genesis-block"))},
		nil,
	))

	result, err := ls.queryLedgerPeerSnapshot(olocalstatequery.LedgerPeerKindAll)
	require.NoError(t, err)

	snapshot := result.(olocalstatequery.LedgerPeerSnapshotResult)
	assert.True(t, snapshot.Slot.HasSlot, "slot 0 with a hash is a real point")
	assert.Equal(t, uint64(0), snapshot.Slot.Slot)
}

func TestAssembleLedgerPeerSnapshotNoStake(t *testing.T) {
	// Pools exist with relays but no delegated stake -> empty weighted set.
	pools := []models.Pool{
		poolWithRelays(poolKeyHash28(0x01), ipv4Relay("1.2.3.4", 3001)),
	}
	snapshot := assembleLedgerPeerSnapshot(
		atSlot(100),
		map[string]uint64{},
		pools,
		olocalstatequery.LedgerPeerKindAll,
	)
	assert.True(t, snapshot.Slot.HasSlot)
	assert.Equal(t, uint64(100), snapshot.Slot.Slot)
	assert.Empty(t, snapshot.Pools)
}

// --- stake population + ordering ------------------------------------------

// TestAssembleLedgerPeerSnapshotStakeAndOrdering proves relative stake is
// pool/total, that pools are emitted highest-stake-first, and that the
// accumulated stake is the running cumulative sum.
func TestAssembleLedgerPeerSnapshotStakeAndOrdering(t *testing.T) {
	khSmall := poolKeyHash28(0x0A)
	khBig := poolKeyHash28(0x0B)
	pools := []models.Pool{
		// Deliberately list the smaller-stake pool first to prove sorting.
		poolWithRelays(khSmall, ipv4Relay("10.0.0.1", 3001)),
		poolWithRelays(khBig, ipv4Relay("10.0.0.2", 3001)),
	}
	stake := map[string]uint64{
		string(khSmall): 10,
		string(khBig):   30,
	} // total = 40

	snapshot := assembleLedgerPeerSnapshot(
		atSlot(500),
		stake,
		pools,
		olocalstatequery.LedgerPeerKindAll,
	)

	require.Len(t, snapshot.Pools, 2)

	// First entry must be the larger pool: 30/40 = 3/4.
	first := snapshot.Pools[0]
	assert.Equal(t, "3/4", first.Detail.PoolStake.String())
	assert.Equal(t, "3/4", first.AccumulatedStake.String())
	require.Len(t, first.Detail.Relays, 1)
	assert.Equal(t, olocalstatequery.RelayKindIPv4, first.Detail.Relays[0].Kind)

	// Second entry: 10/40 = 1/4, cumulative 3/4 + 1/4 = 1/1.
	second := snapshot.Pools[1]
	assert.Equal(t, "1/4", second.Detail.PoolStake.String())
	assert.Equal(t, "1/1", second.AccumulatedStake.String())
}

// TestAssembleLedgerPeerSnapshotRelaylessPoolCounted proves a pool without
// relays is excluded from the result list yet still counts toward the
// total-stake denominator.
func TestAssembleLedgerPeerSnapshotRelaylessPoolCounted(t *testing.T) {
	khRelay := poolKeyHash28(0x01)
	khNoRelay := poolKeyHash28(0x02)
	pools := []models.Pool{
		poolWithRelays(khRelay, ipv4Relay("1.2.3.4", 3001)),
		poolWithRelays(khNoRelay), // no relays
	}
	stake := map[string]uint64{
		string(khRelay):   10,
		string(khNoRelay): 90,
	} // total = 100

	snapshot := assembleLedgerPeerSnapshot(
		atSlot(1),
		stake,
		pools,
		olocalstatequery.LedgerPeerKindAll,
	)

	require.Len(t, snapshot.Pools, 1, "relayless pool is not listed")
	// 10/100 = 1/10 proves the relayless pool's 90 stayed in the denominator.
	assert.Equal(t, "1/10", snapshot.Pools[0].Detail.PoolStake.String())
}

// --- big-peer selection ----------------------------------------------------

// TestAssembleLedgerPeerSnapshotBigPeerQuota proves LedgerPeerKindBig trims
// the set at the cumulative-stake quota (0.9) while LedgerPeerKindAll keeps
// every relay-advertising pool.
func TestAssembleLedgerPeerSnapshotBigPeerQuota(t *testing.T) {
	khA := poolKeyHash28(0xA0) // 95%
	khB := poolKeyHash28(0xB0) // 4%
	khC := poolKeyHash28(0xC0) // 1%
	pools := []models.Pool{
		poolWithRelays(khA, ipv4Relay("10.0.0.1", 3001)),
		poolWithRelays(khB, ipv4Relay("10.0.0.2", 3001)),
		poolWithRelays(khC, ipv4Relay("10.0.0.3", 3001)),
	}
	stake := map[string]uint64{
		string(khA): 95,
		string(khB): 4,
		string(khC): 1,
	} // total = 100

	all := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, all.Pools, 3, "All returns every relay-advertising pool")

	big := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindBig,
	)
	// 0.95 already exceeds the 0.9 quota, so only the top pool is big.
	require.Len(t, big.Pools, 1)
	assert.Equal(t, "19/20", big.Pools[0].Detail.PoolStake.String())
}

// TestAssembleLedgerPeerSnapshotBigPeerExactThreshold proves that a pool which
// lands the cumulative stake exactly on the 0.9 quota terminates the big set:
// pools after it (including same-stake or zero-stake relayed pools) are
// excluded rather than admitted by an off-by-one boundary.
func TestAssembleLedgerPeerSnapshotBigPeerExactThreshold(t *testing.T) {
	khA := poolKeyHash28(0xA0) // 80%
	khB := poolKeyHash28(0xB0) // 10% -> cumulative exactly 0.9
	khC := poolKeyHash28(0xC0) // 10% -> must be excluded
	pools := []models.Pool{
		poolWithRelays(khA, ipv4Relay("10.0.0.1", 3001)),
		poolWithRelays(khB, ipv4Relay("10.0.0.2", 3001)),
		poolWithRelays(khC, ipv4Relay("10.0.0.3", 3001)),
	}
	stake := map[string]uint64{
		string(khA): 80,
		string(khB): 10,
		string(khC): 10,
	} // total = 100

	big := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindBig,
	)
	require.Len(t, big.Pools, 2, "exact 0.9 boundary terminates the big set")
	// The second (boundary) pool's cumulative stake is exactly 9/10.
	assert.Equal(t, "9/10", big.Pools[1].AccumulatedStake.String())

	// LedgerPeerKindAll still returns all three relay-advertising pools.
	all := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, all.Pools, 3)
}

// --- relay-kind mapping ----------------------------------------------------

// TestAssembleLedgerPeerSnapshotRelayKinds proves every stored relay shape is
// mapped to the correct RelayAccessPoint kind, including multi-host (SRV) and
// dual-stack single-host-address relays.
func TestAssembleLedgerPeerSnapshotRelayKinds(t *testing.T) {
	hostPort := "named.example.com"
	srvHost := "srv.example.com"
	dualV4 := net.ParseIP("9.9.9.9").To4()
	dualV6 := net.ParseIP("2001:db8::99")

	kh := poolKeyHash28(0x01)
	pool := poolWithRelays(
		kh,
		ipv4Relay("1.2.3.4", 3001),
		ipv6Relay("2001:db8::1", 3002),
		models.PoolRegistrationRelay{Hostname: hostPort, Port: 3003},
		models.PoolRegistrationRelay{Hostname: srvHost}, // port 0 -> SRV
		// dual-stack single-host-address row -> two access points
		models.PoolRegistrationRelay{Ipv4: &dualV4, Ipv6: &dualV6, Port: 3004},
	)
	stake := map[string]uint64{string(kh): 1}

	snapshot := assembleLedgerPeerSnapshot(
		atSlot(1), stake, []models.Pool{pool}, olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, snapshot.Pools, 1)
	relays := snapshot.Pools[0].Detail.Relays
	require.Len(t, relays, 6, "dual-stack row expands to two access points")

	// IPv4
	assert.Equal(t, olocalstatequery.RelayKindIPv4, relays[0].Kind)
	require.NotNil(t, relays[0].IPv4)
	assert.Equal(t, "1.2.3.4", relays[0].IPv4.String())
	require.NotNil(t, relays[0].Port)
	assert.Equal(t, uint16(3001), *relays[0].Port)

	// IPv6
	assert.Equal(t, olocalstatequery.RelayKindIPv6, relays[1].Kind)
	require.NotNil(t, relays[1].IPv6)
	assert.Equal(t, "2001:db8::1", relays[1].IPv6.String())
	require.NotNil(t, relays[1].Port)
	assert.Equal(t, uint16(3002), *relays[1].Port)

	// SingleHostName (domain + port)
	assert.Equal(t, olocalstatequery.RelayKindDomain, relays[2].Kind)
	require.NotNil(t, relays[2].Domain)
	assert.Equal(t, hostPort, *relays[2].Domain)
	require.NotNil(t, relays[2].Port)
	assert.Equal(t, uint16(3003), *relays[2].Port)

	// MultiHostName (SRV, no port)
	assert.Equal(t, olocalstatequery.RelayKindSRV, relays[3].Kind)
	require.NotNil(t, relays[3].Domain)
	assert.Equal(t, srvHost, *relays[3].Domain)
	assert.Nil(t, relays[3].Port, "SRV relay carries no port")

	// Dual-stack expansion: IPv4 then IPv6, both with the row's port.
	assert.Equal(t, olocalstatequery.RelayKindIPv4, relays[4].Kind)
	assert.Equal(t, "9.9.9.9", relays[4].IPv4.String())
	assert.Equal(t, uint16(3004), *relays[4].Port)
	assert.Equal(t, olocalstatequery.RelayKindIPv6, relays[5].Kind)
	assert.Equal(t, "2001:db8::99", relays[5].IPv6.String())
	assert.Equal(t, uint16(3004), *relays[5].Port)
}

// TestAssembleLedgerPeerSnapshotMalformedPort proves a hostname relay with an
// out-of-range (malformed) port narrows to no port and is treated as an SRV
// (MultiHostName) record rather than a SingleHostName with a bogus port 0.
func TestAssembleLedgerPeerSnapshotMalformedPort(t *testing.T) {
	host := "bad-port.example.com"
	kh := poolKeyHash28(0x01)
	pool := poolWithRelays(
		kh,
		models.PoolRegistrationRelay{Hostname: host, Port: 70000},
	)
	stake := map[string]uint64{string(kh): 1}

	snapshot := assembleLedgerPeerSnapshot(
		atSlot(1), stake, []models.Pool{pool}, olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, snapshot.Pools, 1)
	relays := snapshot.Pools[0].Detail.Relays
	require.Len(t, relays, 1)
	assert.Equal(t, olocalstatequery.RelayKindSRV, relays[0].Kind)
	require.NotNil(t, relays[0].Domain)
	assert.Equal(t, host, *relays[0].Domain)
	assert.Nil(t, relays[0].Port, "malformed port must not be emitted")
}

// TestAssembleLedgerPeerSnapshotResultEncodes proves the assembled result
// round-trips through the gouroboros CBOR marshaller, i.e. it is a valid
// wire-format GetLedgerPeerSnapshot response.
func TestAssembleLedgerPeerSnapshotResultEncodes(t *testing.T) {
	kh := poolKeyHash28(0x01)
	pool := poolWithRelays(kh, ipv4Relay("1.2.3.4", 3001))
	snapshot := assembleLedgerPeerSnapshot(
		atSlot(7), map[string]uint64{string(kh): 1}, []models.Pool{pool},
		olocalstatequery.LedgerPeerKindAll,
	)

	encoded, err := snapshot.MarshalCBOR()
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var decoded olocalstatequery.LedgerPeerSnapshotResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))
	assert.Equal(t, uint64(7), decoded.Slot.Slot)
	require.Len(t, decoded.Pools, 1)
	assert.Equal(t, "1/1", decoded.Pools[0].Detail.PoolStake.String())
	require.Len(t, decoded.Pools[0].Detail.Relays, 1)
	assert.Equal(
		t,
		olocalstatequery.RelayKindIPv4,
		decoded.Pools[0].Detail.Relays[0].Kind,
	)
}
