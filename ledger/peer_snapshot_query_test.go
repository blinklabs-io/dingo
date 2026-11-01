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
	"encoding/hex"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
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
	t.Parallel()

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

	result, err := ls.Query(t.Context(), query, QueryPoint{})
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
	t.Parallel()

	ls := &LedgerState{db: newTestDB(t)}

	result, err := ls.queryLedgerPeerSnapshot(
		context.Background(),
		&olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
			PeerKind: olocalstatequery.LedgerPeerKindAll,
		},
		nil,
	)
	require.NoError(t, err)

	snapshot := result.(olocalstatequery.LedgerPeerSnapshotResult)
	assert.False(t, snapshot.Slot.HasSlot, "tip slot 0 maps to Origin")
	assert.Empty(t, snapshot.Pools)
}

// TestQueryLedgerPeerSnapshotRealSlotZero proves a real slot-0 block (slot 0
// with a non-empty hash) is reported as At slot 0, not collapsed to Origin.
func TestQueryLedgerPeerSnapshotRealSlotZero(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{db: newTestDB(t)}
	require.NoError(t, ls.db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(0, []byte("genesis-block"))},
		nil,
	))

	result, err := ls.queryLedgerPeerSnapshot(
		context.Background(),
		&olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
			PeerKind: olocalstatequery.LedgerPeerKindAll,
		},
		nil,
	)
	require.NoError(t, err)

	snapshot := result.(olocalstatequery.LedgerPeerSnapshotResult)
	assert.True(t, snapshot.Slot.HasSlot, "slot 0 with a hash is a real point")
	assert.Equal(t, uint64(0), snapshot.Slot.Slot)
}

func TestAssembleLedgerPeerSnapshotNoStake(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

// TestAssembleLedgerPeerSnapshotRelaylessPoolDenominators proves all-peer
// snapshots retain the full ledger denominator while big-peer snapshots
// renormalize over pools that advertise a usable relay.
func TestAssembleLedgerPeerSnapshotRelaylessPoolDenominators(t *testing.T) {
	t.Parallel()

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

	all := assembleLedgerPeerSnapshot(
		atSlot(1),
		stake,
		pools,
		olocalstatequery.LedgerPeerKindAll,
	)

	require.Len(t, all.Pools, 1, "relayless pool is not listed")
	// 10/100 = 1/10 proves the relayless pool's 90 stayed in the denominator.
	assert.Equal(t, "1/10", all.Pools[0].Detail.PoolStake.String())

	big := assembleLedgerPeerSnapshot(
		atSlot(1),
		stake,
		pools,
		olocalstatequery.LedgerPeerKindBig,
	)
	require.Len(t, big.Pools, 1)
	assert.Equal(t, "1/1", big.Pools[0].Detail.PoolStake.String())
}

// --- big-peer selection ----------------------------------------------------

// TestAssembleLedgerPeerSnapshotBigPeerQuota proves LedgerPeerKindBig trims
// the set at the cumulative-stake quota (0.9) while LedgerPeerKindAll keeps
// every relay-advertising pool.
func TestAssembleLedgerPeerSnapshotBigPeerQuota(t *testing.T) {
	t.Parallel()

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

// TestAssembleLedgerPeerSnapshotBigPeerExactThreshold proves the reference
// quota includes a pool whose predecessors total exactly 0.9.
func TestAssembleLedgerPeerSnapshotBigPeerExactThreshold(t *testing.T) {
	t.Parallel()

	khA := poolKeyHash28(0xA0) // 80%
	khB := poolKeyHash28(0xB0) // 10% -> cumulative exactly 0.9
	khC := poolKeyHash28(0xC0) // prior cumulative is exactly 0.9
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
	require.Len(t, big.Pools, 3, "pool after exact 0.9 boundary is included")
	// The second (boundary) pool's cumulative stake is exactly 9/10.
	assert.Equal(t, "9/10", big.Pools[1].AccumulatedStake.String())
	assert.Equal(t, "1/1", big.Pools[2].AccumulatedStake.String())

	// LedgerPeerKindAll still returns all three relay-advertising pools.
	all := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, all.Pools, 3)
}

// --- relay-kind mapping ----------------------------------------------------

// TestAssembleLedgerPeerSnapshotRelayKinds proves every stored relay shape is
// mapped to the correct RelayAccessPoint kind, including multi-host (SRV) and
// IPv4 precedence for dual-stack single-host-address relays.
func TestAssembleLedgerPeerSnapshotRelayKinds(t *testing.T) {
	t.Parallel()

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
		// A dual-stack SingleHostAddr maps to IPv4, matching the reference.
		models.PoolRegistrationRelay{Ipv4: &dualV4, Ipv6: &dualV6, Port: 3004},
	)
	stake := map[string]uint64{string(kh): 1}

	snapshot := assembleLedgerPeerSnapshot(
		atSlot(
			1,
		),
		stake,
		[]models.Pool{pool},
		olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, snapshot.Pools, 1)
	relays := snapshot.Pools[0].Detail.Relays
	require.Len(t, relays, 5)

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

	// Dual-stack SingleHostAddr chooses IPv4 rather than expanding the row.
	assert.Equal(t, olocalstatequery.RelayKindIPv4, relays[4].Kind)
	assert.Equal(t, "9.9.9.9", relays[4].IPv4.String())
	assert.Equal(t, uint16(3004), *relays[4].Port)
}

func TestAssembleLedgerPeerSnapshotDropsUnusableAddressRelays(t *testing.T) {
	t.Parallel()

	khPortless := poolKeyHash28(0x01)
	khNoAddress := poolKeyHash28(0x02)
	khIPv6 := poolKeyHash28(0x03)
	portlessIP := net.ParseIP("1.2.3.4").To4()
	pools := []models.Pool{
		poolWithRelays(
			khPortless,
			models.PoolRegistrationRelay{Ipv4: &portlessIP},
		),
		poolWithRelays(
			khNoAddress,
			models.PoolRegistrationRelay{Port: 3001},
		),
		poolWithRelays(khIPv6, ipv6Relay("2001:db8::1", 3001)),
	}
	stake := map[string]uint64{
		string(khPortless):  3,
		string(khNoAddress): 2,
		string(khIPv6):      1,
	}

	all := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindAll,
	)
	require.Len(t, all.Pools, 1)
	assert.Equal(t, "1/6", all.Pools[0].Detail.PoolStake.String())
	assert.Equal(
		t,
		olocalstatequery.RelayKindIPv6,
		all.Pools[0].Detail.Relays[0].Kind,
	)

	big := assembleLedgerPeerSnapshot(
		atSlot(1), stake, pools, olocalstatequery.LedgerPeerKindBig,
	)
	require.Len(t, big.Pools, 1)
	assert.Equal(t, "1/1", big.Pools[0].Detail.PoolStake.String())
}

// TestAssembleLedgerPeerSnapshotMalformedPort proves a hostname relay with an
// out-of-range (malformed) port narrows to no port and is treated as an SRV
// (MultiHostName) record rather than a SingleHostName with a bogus port 0.
func TestAssembleLedgerPeerSnapshotMalformedPort(t *testing.T) {
	t.Parallel()

	host := "bad-port.example.com"
	kh := poolKeyHash28(0x01)
	pool := poolWithRelays(
		kh,
		models.PoolRegistrationRelay{Hostname: host, Port: 70000},
	)
	stake := map[string]uint64{string(kh): 1}

	snapshot := assembleLedgerPeerSnapshot(
		atSlot(
			1,
		),
		stake,
		[]models.Pool{pool},
		olocalstatequery.LedgerPeerKindAll,
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
	t.Parallel()

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

// --- version 23 request form ------------------------------------------------

// peerSnapshotV23Ledger builds a ledger over an empty database whose Shelley
// genesis carries the preview network magic (2).
func peerSnapshotV23Ledger(t *testing.T) *LedgerState {
	t.Helper()
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	require.EqualValues(t, 2, cfg.ShelleyGenesis().NetworkMagic)
	return &LedgerState{
		db:     newTestDB(t),
		config: LedgerStateConfig{CardanoNodeConfig: cfg},
	}
}

func peerSnapshotV23Query(kind olocalstatequery.LedgerPeerKind) *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
				Type:         olocalstatequery.QueryTypeShelleyGetLedgerPeerSnapshot,
				PeerKind:     kind,
				WithPeerKind: true,
			},
		},
	}
}

// encodePeerSnapshotReply encodes the handler's reply the way the server does,
// and returns the snapshot bytes: the server wraps a bare snapshot in the
// single-element era result wrapper (0x81), which is stripped here.
func encodePeerSnapshotReply(t *testing.T, result any) string {
	t.Helper()
	snap, ok := result.(olocalstatequery.LedgerPeerSnapshotResult)
	require.True(t, ok, "expected LedgerPeerSnapshotResult, got %T", result)
	encoded, err := cbor.Encode(snap)
	require.NoError(t, err)
	return hex.EncodeToString(encoded)
}

// TestQueryLedgerPeerSnapshotV23EmptyBytes pins the version 23 replies to the
// layout ouroboros-network's encodeLedgerPeerSnapshot writes:
//
//	big (kind 1): [2, [point, magic, [_ ...]]]
//	all (kind 0): [3, [point, magic, [_ ...]]]
//	point: [0] at genesis, [1, slot, hash(32)] otherwise
func TestQueryLedgerPeerSnapshotV23EmptyBytes(t *testing.T) {
	t.Parallel()

	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	hashHex := hex.EncodeToString(tipHash)
	tests := []struct {
		name    string
		kind    olocalstatequery.LedgerPeerKind
		tip     bool
		wantHex string
	}{
		{"big at genesis", olocalstatequery.LedgerPeerKindBig, false,
			"820283" + "8100" + "02" + "9fff"},
		{"all at genesis", olocalstatequery.LedgerPeerKindAll, false,
			"820383" + "8100" + "02" + "9fff"},
		{"big at block", olocalstatequery.LedgerPeerKindBig, true,
			"820283" + "8301191092" + "5820" + hashHex + "02" + "9fff"},
		{"all at block", olocalstatequery.LedgerPeerKindAll, true,
			"820383" + "8301191092" + "5820" + hashHex + "02" + "9fff"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls := peerSnapshotV23Ledger(t)
			if tc.tip {
				require.NoError(t, ls.db.SetTip(
					ochainsync.Tip{Point: ocommon.NewPoint(4242, tipHash)},
					nil,
				))
			}
			result, err := ls.Query(
				t.Context(),
				peerSnapshotV23Query(tc.kind),
				QueryPoint{},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantHex, encodePeerSnapshotReply(t, result))
		})
	}
}

// TestLedgerPeerSnapshotV23PoolBytes pins the per-pool entries of the version
// 2 and 3 replies: [acc stake, pool stake, relays] for big peers and
// [pool stake, relays] for all peers, with stakes as untagged [num, den] and
// relays as an indefinite list. The one pool holds all stake and advertises
// the IPv4 relay 1.2.3.4:3001.
func TestLedgerPeerSnapshotV23PoolBytes(t *testing.T) {
	t.Parallel()

	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	const (
		stake = "820101"                 // [1, 1]
		relay = "8301190bb99f01020304ff" // [1, 3001, [_ 1,2,3,4]]
	)
	pointHex := "8301191092" + "5820" + hex.EncodeToString(tipHash)
	bigEntry := "83" + stake + stake + "9f" + relay + "ff"
	allEntry := "82" + stake + "9f" + relay + "ff"

	tests := []struct {
		name    string
		kind    olocalstatequery.LedgerPeerKind
		wantHex string
	}{
		{"big", olocalstatequery.LedgerPeerKindBig,
			"820283" + pointHex + "02" + "9f" + bigEntry + "ff"},
		{"all", olocalstatequery.LedgerPeerKindAll,
			"820383" + pointHex + "02" + "9f" + allEntry + "ff"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snapshot := assembleLedgerPeerSnapshot(
				atSlot(4242),
				map[string]uint64{string(poolKeyHash28(0x11)): 5_000_000},
				[]models.Pool{
					poolWithRelays(poolKeyHash28(0x11), ipv4Relay("1.2.3.4", 3001)),
				},
				tc.kind,
			)
			result, err := forLedgerPeerSnapshotV23(
				snapshot,
				tc.kind,
				ocommon.NewPoint(4242, tipHash),
				2,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantHex, encodePeerSnapshotReply(t, result))
		})
	}
}

// TestLedgerPeerSnapshotV23RejectsMalformedTipHash proves a non-genesis tip
// without a 32-byte hash is an error rather than a snapshot the client cannot
// decode.
func TestLedgerPeerSnapshotV23RejectsMalformedTipHash(t *testing.T) {
	t.Parallel()

	_, err := forLedgerPeerSnapshotV23(
		emptyLedgerPeerSnapshot(atSlot(7)),
		olocalstatequery.LedgerPeerKindBig,
		ocommon.NewPoint(7, []byte("short")),
		2,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tip hash")
}

// TestQueryLedgerPeerSnapshotV23RequiresNetworkMagic proves the version 23
// form fails without a Shelley genesis to take the magic from, while the legacy
// form (which carries no magic) does not need one.
func TestQueryLedgerPeerSnapshotV23RequiresNetworkMagic(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{db: newTestDB(t)}
	_, err := ls.Query(
		t.Context(),
		peerSnapshotV23Query(olocalstatequery.LedgerPeerKindBig),
		QueryPoint{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network magic")

	legacy := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
				Type:     olocalstatequery.QueryTypeShelleyGetLedgerPeerSnapshot,
				PeerKind: olocalstatequery.LedgerPeerKindBig,
			},
		},
	}
	result, err := ls.Query(t.Context(), legacy, QueryPoint{})
	require.NoError(t, err)
	// Legacy form: version 1 layout, [1, [[0], [_ ]]] at genesis.
	assert.Equal(t, "82018281009fff", encodePeerSnapshotReply(t, result))
}

// TestLedgerPeerSnapshotExportedMatchesTheQuery proves the exported accessor
// reports what the LocalStateQuery handler does for every ledger peer.
func TestLedgerPeerSnapshotExportedMatchesTheQuery(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{db: newTestDB(t)}
	require.NoError(t, ls.db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(4242, []byte("tip"))},
		nil,
	))

	got, err := ls.LedgerPeerSnapshot(t.Context())
	require.NoError(t, err)

	want, err := ls.queryLedgerPeerSnapshot(
		t.Context(),
		&olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
			PeerKind: olocalstatequery.LedgerPeerKindAll,
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, want, any(got))
	assert.True(t, got.Slot.HasSlot)
	assert.Equal(t, uint64(4242), got.Slot.Slot)
}
