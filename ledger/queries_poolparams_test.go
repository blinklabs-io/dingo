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
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gshelley "github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func hexBytes(t *testing.T, parts ...string) []byte {
	t.Helper()
	out, err := hex.DecodeString(strings.Join(parts, ""))
	require.NoError(t, err)
	return out
}

func stakePoolParamsQuery(pools ...[]byte) *olocalstatequery.BlockQuery {
	ids := make([]ledger.PoolId, 0, len(pools))
	for _, pool := range pools {
		ids = append(ids, ledger.PoolId(pool))
	}
	return shelleyBlockQuery(&olocalstatequery.ShelleyStakePoolParamsQuery{
		Type:    olocalstatequery.QueryTypeShelleyStakePoolParams,
		PoolIds: cbor.NewSetType(ids, true),
	})
}

// seedStakePoolParamsPool registers a pool with every optional field set.
func seedStakePoolParamsPool(
	t *testing.T,
	ls *LedgerState,
	poolKeyHash []byte,
	rewardTag uint8,
) {
	t.Helper()
	seedStakePoolParamsRegistration(t, ls, poolKeyHash, rewardTag, 1, 1_000_000)
}

// seedStakePoolParamsRegistration records one registration of the pool at
// addedSlot with the given pledge.
func seedStakePoolParamsRegistration(
	t *testing.T,
	ls *LedgerState,
	poolKeyHash []byte,
	rewardTag uint8,
	addedSlot uint64,
	pledge uint64,
) {
	t.Helper()
	// Imported ledger relays already carry the ledger's wire-order bytes.
	ipv4 := net.IP{1, 1, 168, 192}
	relays := []models.PoolRegistrationRelay{
		{Ipv4: &ipv4, Port: 3001},
		{Hostname: "relay.example", Port: 3001},
	}
	owners := []models.PoolRegistrationOwner{
		{KeyHash: repeatedBytes(28, 0x44)},
		{KeyHash: repeatedBytes(28, 0x33)},
	}
	vrf := repeatedBytes(32, 0xAA)
	reward := repeatedBytes(28, 0x22)
	margin := &dbtypes.Rat{Rat: big.NewRat(3, 10)}
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:                poolKeyHash,
			VrfKeyHash:                 vrf,
			RewardAccount:              reward,
			RewardAccountCredentialTag: rewardTag,
			Margin:                     margin,
			Pledge:                     dbtypes.Uint64(pledge),
			Cost:                       dbtypes.Uint64(340_000_000),
		},
		&models.PoolRegistration{
			PoolKeyHash:                poolKeyHash,
			VrfKeyHash:                 vrf,
			RewardAccount:              reward,
			RewardAccountCredentialTag: rewardTag,
			Margin:                     margin,
			Pledge:                     dbtypes.Uint64(pledge),
			Cost:                       dbtypes.Uint64(340_000_000),
			MetadataUrl:                "https://a.io/p",
			MetadataHash:               repeatedBytes(32, 0x55),
			Owners:                     owners,
			Relays:                     relays,
			AddedSlot:                  addedSlot,
		},
		nil,
	))
}

// setStakePoolParamsLiveState places the live tip at tipSlot inside an epoch
// that starts at epochStart.
func setStakePoolParamsLiveState(
	ls *LedgerState,
	epochID uint64,
	epochStart uint64,
	tipSlot uint64,
) {
	ls.currentEpoch = models.Epoch{
		EpochId:       epochID,
		StartSlot:     epochStart,
		LengthInSlots: 100,
	}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x01)),
	}
	ls.publishSnapshotsLocked()
}

// TestQueryStakePoolParams_WireEncoding compares the whole reply against
// literal bytes laid out from the ledger's StakePoolParams codec: a map
// keyed by pool hash inside the era wrapper, each value a 9-element array
// with the owners as a tag-258 set in ascending order and the margin as a
// tag-30 rational.
func TestQueryStakePoolParams_WireEncoding(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 0)
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x88), 0)

	// 0x77 is not registered and 0x88 is registered but not requested.
	got, err := ls.Query(
		t.Context(),
		stakePoolParamsQuery(repeatedBytes(28, 0x11), repeatedBytes(28, 0x77)),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)

	want := hexBytes(t,
		"81", // era wrapper
		"a1", // one pool
		"581c"+strings.Repeat("11", 28),
		"89",                              // StakePoolParams, 9 fields
		"581c"+strings.Repeat("11", 28),   // operator
		"5820"+strings.Repeat("aa", 32),   // vrf
		"1a000f4240",                      // pledge 1000000
		"1a1443fd00",                      // cost 340000000
		"d81e82030a",                      // margin 3/10
		"581de0"+strings.Repeat("22", 28), // key-hash reward account, testnet
		"d90102", "82",                    // owners as a set
		"581c"+strings.Repeat("33", 28),
		"581c"+strings.Repeat("44", 28),
		"82", // relays
		"8400190bb9440101a8c0f6",
		"8301190bb96d72656c61792e6578616d706c65",
		"82", "6e68747470733a2f2f612e696f2f70", // metadata url
		"5820"+strings.Repeat("55", 32),
	)
	require.Equal(t, hex.EncodeToString(want), hex.EncodeToString(gotCbor))
}

// TestQueryStakePoolParams_ScriptRewardAccount checks the reward account
// header carries the credential type stored at registration.
func TestQueryStakePoolParams_ScriptRewardAccount(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 1)

	got, err := ls.Query(
		t.Context(),
		stakePoolParamsQuery(repeatedBytes(28, 0x11)),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Contains(
		t,
		hex.EncodeToString(gotCbor),
		"581df0"+strings.Repeat("22", 28),
	)
}

// TestQueryStakePoolParams_EmptyFilter answers an empty set with an empty
// map, not every pool.
func TestQueryStakePoolParams_EmptyFilter(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 0)

	got, err := ls.Query(t.Context(), stakePoolParamsQuery(), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(t, "81a0", hex.EncodeToString(gotCbor))
}

// TestQueryStakePoolParams_ReregistrationIsFuture reports the parameters in
// effect, not a re-registration made this epoch: the ledger holds that as
// future parameters until the next epoch boundary.
func TestQueryStakePoolParams_ReregistrationIsFuture(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 1, 100, 160)
	pool := repeatedBytes(28, 0x11)
	seedStakePoolParamsRegistration(t, ls, pool, 0, 5, 1_000_000)
	seedStakePoolParamsRegistration(t, ls, pool, 0, 150, 2_000_000)

	got, err := ls.Query(t.Context(), stakePoolParamsQuery(pool), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	encoded := hex.EncodeToString(gotCbor)
	require.Contains(t, encoded, strings.Repeat("aa", 32)+"1a000f4240")
	require.NotContains(t, encoded, strings.Repeat("aa", 32)+"1a001e8480")
}

// TestQueryStakePoolParams_PortlessHostnameIsMultiHost maps a hostname relay
// stored without a port to MultiHostName, the only relay form registered
// without one.
func TestQueryStakePoolParams_PortlessHostnameIsMultiHost(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	pool := repeatedBytes(28, 0x11)
	vrf := repeatedBytes(32, 0xAA)
	reward := repeatedBytes(28, 0x22)
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrf,
			RewardAccount: reward,
		},
		&models.PoolRegistration{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrf,
			RewardAccount: reward,
			Relays: []models.PoolRegistrationRelay{
				{Hostname: "relay.example"},
			},
			AddedSlot: 1,
		},
		nil,
	))

	got, err := ls.Query(t.Context(), stakePoolParamsQuery(pool), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Contains(
		t,
		hex.EncodeToString(gotCbor),
		"81"+"82026d72656c61792e6578616d706c65",
	)
}

func TestQueryStakePoolParams_PreservesPortlessSingleHostName(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	pool := repeatedBytes(28, 0x11)
	vrf := repeatedBytes(32, 0xAA)
	reward := repeatedBytes(28, 0x22)
	relayType := lcommon.PoolRelayTypeSingleHostName
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pool, VrfKeyHash: vrf, RewardAccount: reward,
		},
		&models.PoolRegistration{
			PoolKeyHash: pool, VrfKeyHash: vrf, RewardAccount: reward,
			Relays: []models.PoolRegistrationRelay{{
				Type: &relayType, Hostname: "relay.example",
			}},
			AddedSlot: 1,
		},
		nil,
	))

	got, err := ls.Query(t.Context(), stakePoolParamsQuery(pool), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Contains(
		t,
		hex.EncodeToString(gotCbor),
		"81"+"8301f66d72656c61792e6578616d706c65",
	)
}

// TestQueryLedgerTip answers the live tip when unpinned and the acquired
// point when pinned.
func TestQueryLedgerTip(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	pinnedHash := repeatedBytes(32, 0xAB)
	tipHash := repeatedBytes(32, 0xCD)
	seedBlockAtSlot(t, ls, 100, pinnedHash)
	tip := ochainsync.Tip{Point: ocommon.NewPoint(200, tipHash)}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip
	ls.publishSnapshotsLocked()

	query := shelleyBlockQuery(&olocalstatequery.ShelleyLedgerTipQuery{})
	live, err := ls.Query(t.Context(), query, QueryPoint{})
	require.NoError(t, err)
	liveCbor, err := cbor.Encode(live)
	require.NoError(t, err)
	require.Equal(
		t,
		hex.EncodeToString(
			hexBytes(t, "8182", "18c8", "5820"+strings.Repeat("cd", 32)),
		),
		hex.EncodeToString(liveCbor),
	)

	pinned, err := ls.Query(t.Context(), query, QueryPoint{Slot: 100, Hash: pinnedHash})
	require.NoError(t, err)
	pinnedCbor, err := cbor.Encode(pinned)
	require.NoError(t, err)
	require.Equal(
		t,
		hex.EncodeToString(
			hexBytes(t, "8182", "1864", "5820"+strings.Repeat("ab", 32)),
		),
		hex.EncodeToString(pinnedCbor),
	)
}

// TestQueryProposedProtocolParamsUpdates answers an empty map: Conway
// replaced the update proposal mechanism with governance actions.
func TestQueryProposedProtocolParamsUpdates(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.publishSnapshotsLocked()
	got, err := ls.Query(
		t.Context(),
		shelleyBlockQuery(
			&olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery{},
		),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(t, "81a0", hex.EncodeToString(gotCbor))
}

// TestShelleyExtraConfigCBORNoInjection encodes an absent injection section
// as NoInjection, [0], rather than as an empty embedded map.
func TestShelleyExtraConfigCBORNoInjection(t *testing.T) {
	t.Parallel()

	genesis := &gshelley.ShelleyGenesis{
		ExtraConfig: &gshelley.ShelleyGenesisExtraConfig{},
	}
	got, err := shelleyExtraConfigCBOR(genesis, 0)
	require.NoError(t, err)
	require.Equal(t, "8183810081008100", hex.EncodeToString(got))
}

// TestQueryProposedProtocolParamsUpdatesBeforeConway refuses the query in an
// era that still carries update proposals, rather than answering an empty
// map that may be wrong.
func TestQueryProposedProtocolParamsUpdatesBeforeConway(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.BabbageEraDesc
	ls.publishSnapshotsLocked()
	_, err := ls.Query(
		t.Context(),
		shelleyBlockQuery(
			&olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery{},
		),
		QueryPoint{},
	)
	require.Error(t, err)
}

// TestGenesisConfigResultExtraConfigBytes compares the injection data of the
// Musashi genesis against literal bytes laid out from ShelleyExtraConfig: a
// three-field record of funds, pools and stake credentials, each an
// InjectionData with the embedded-data tag 2 around a map. Map entries are in
// the encoder's deterministic order (shorter encoded key first).
func TestGenesisConfigResultExtraConfigBytes(t *testing.T) {
	t.Parallel()

	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	result, err := genesisConfigResult(cfg.ShelleyGenesis())
	require.NoError(t, err)

	want := hexBytes(
		t,
		"81", // StrictMaybe SJust
		"83", // ShelleyExtraConfig
		// initial funds
		"8202a2",
		"581d605d11c3d8a44b5aeb60831099a6d122a408ea33ca1faa1102bd4a80cc",
		"1b006a94d74f430000",
		"5839002ac6275715d4fcc740134a578e619876752821c786cb4701c5756ee7dda643b05c248cbff2d80fd45dff2b4cb260cd03ea6fa299da3955ef",
		"1a35a4e900",
		// stake pools
		"8202a1",
		"581cfd32267bc1c702ad9b530d4e9f9939ce93bbb99f55f31fbe4989ea88",
		"89",
		"581cfd32267bc1c702ad9b530d4e9f9939ce93bbb99f55f31fbe4989ea88",
		"5820f8eb3533e40984adc744cb326fd410c200dab831f915224c8515a06cedaabdc5",
		"00",
		"00",
		"d81e820001",
		"581de0c002c1f5bccc09b7d2865b8ba5bc6fb280b2f5f0e63cb11da79f413e",
		"80", // owners: the Shelley encoding has no set tag
		"80",
		"f6",
		// stake credentials
		"8202a1",
		"581cdda643b05c248cbff2d80fd45dff2b4cb260cd03ea6fa299da3955ef",
		"581cfd32267bc1c702ad9b530d4e9f9939ce93bbb99f55f31fbe4989ea88",
	)
	require.Equal(
		t,
		hex.EncodeToString(want),
		hex.EncodeToString(result.ExtraConfig),
	)
}

// Genesis relay addresses originate as text, while the ledger codec stores
// each 32-bit word in little-endian order.
func TestGenesisConfigResultConvertsRelayAddressByteOrder(t *testing.T) {
	t.Parallel()

	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	pools := cfg.ShelleyGenesis().ExtraConfig.StakePools.Data
	require.Len(t, pools, 1)
	for id, pool := range pools {
		pool.Relays = []byte(
			`[{"type":0,"port":3001,"ipv4":"192.168.1.1","ipv6":"2001:db8::1"}]`,
		)
		pools[id] = pool
	}

	result, err := genesisConfigResult(cfg.ShelleyGenesis())
	require.NoError(t, err)
	encoded := hex.EncodeToString(result.ExtraConfig)
	require.Contains(t, encoded, "440101a8c0")
	require.Contains(t, encoded, "50b80d0120000000000000000001000000")
}

// TestQueryProposedProtocolParamsUpdatesPinnedBeforeConway resolves the era
// of the acquired point: a point in a Shelley epoch is refused even though
// the live era is Conway, and a point in a Conway epoch answers the empty map.
func TestQueryProposedProtocolParamsUpdatesPinnedBeforeConway(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))
	query := &olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery{}

	_, err := ls.queryShelleyLeaf(t.Context(), query, QueryPoint{Slot: 350}, nil, 0)
	require.Error(t, err)

	got, err := ls.queryShelleyLeaf(t.Context(), query, QueryPoint{Slot: 620}, nil, 0)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(t, "81a0", hex.EncodeToString(gotCbor))
}

// TestWireOrderRelays checks the byte order of genesis-declared relay
// addresses against the ledger's queryStakePoolRelays golden, where 10.0.0.5
// is 0500000a and 2001:db8::1 is b80d0120 00000000 00000000 01000000.
func TestWireOrderRelays(t *testing.T) {
	t.Parallel()

	v4 := net.ParseIP("10.0.0.5")
	v6 := net.ParseIP("2001:db8::1")
	host := "relay.example"
	in := []lcommon.PoolRelay{
		{Type: 0, Ipv4: &v4, Ipv6: &v6},
		{Type: 1, Hostname: &host},
	}
	out := wireOrderRelays(in)
	require.Equal(t, "0500000a", hex.EncodeToString(*out[0].Ipv4))
	require.Equal(
		t,
		"b80d012000000000000000000100"+"0000",
		hex.EncodeToString(*out[0].Ipv6),
	)
	require.Equal(t, &host, out[1].Hostname)
	require.Equal(t, "10.0.0.5", in[0].Ipv4.String(), "input is not modified")
}

// TestStakePoolParamsBlsKeyEncoding places the BLS key as the third element
// of a ten-element record, and leaves the nine-element record unchanged
// without one.
func TestStakePoolParamsBlsKeyEncoding(t *testing.T) {
	t.Parallel()

	margin := cbor.Rat{Rat: big.NewRat(1, 2)}
	owners := cbor.NewSetType([]ledger.Blake2b224{}, true)
	params := stakePoolParams{
		Pledge:   1,
		Cost:     2,
		Margin:   &margin,
		Owners:   &owners,
		Relays:   []lcommon.PoolRelay{},
		BlsKey:   nil,
		Metadata: nil,
	}
	plain, err := cbor.Encode(params)
	require.NoError(t, err)
	require.Equal(t, byte(0x89), plain[0])

	params.BlsKey = &lcommon.LeiosKey{
		PublicKey:       repeatedBytes(96, 0x66),
		PossessionProof: repeatedBytes(48, 0x77),
	}
	withKey, err := cbor.Encode(params)
	require.NoError(t, err)
	require.Equal(t, byte(0x8a), withKey[0])
	keyCbor, err := cbor.Encode(params.BlsKey)
	require.NoError(t, err)
	var fields []cbor.RawMessage
	_, err = cbor.Decode(withKey, &fields)
	require.NoError(t, err)
	require.Len(t, fields, 10)
	require.Equal(t, []byte(keyCbor), []byte(fields[2]))
}

// TestQueryStakePoolParams_BlsKeyFromProtocolVersion12 reports the registered
// BLS key only once the live protocol version is 12.
func TestQueryStakePoolParams_BlsKeyFromProtocolVersion12(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		major  uint
		fields byte
	}{{11, 0x89}, {12, 0x8a}} {
		db := newTestDB(t)
		ls := newPoolDistr2Ledger(t, db)
		ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
		setStakePoolParamsLiveState(ls, 0, 0, 10)
		pool := repeatedBytes(28, 0x11)
		pp := &conway.ConwayProtocolParameters{}
		pp.ProtocolVersion.Major = tc.major
		ls.currentPParams = pp
		ls.publishSnapshotsLocked()
		require.NoError(t, ls.db.Metadata().ImportPool(
			&models.Pool{
				PoolKeyHash:             pool,
				VrfKeyHash:              repeatedBytes(32, 0xAA),
				RewardAccount:           repeatedBytes(28, 0x22),
				LeiosKeyPublic:          repeatedBytes(96, 0x66),
				LeiosKeyPossessionProof: repeatedBytes(48, 0x77),
			},
			&models.PoolRegistration{
				PoolKeyHash:             pool,
				VrfKeyHash:              repeatedBytes(32, 0xAA),
				RewardAccount:           repeatedBytes(28, 0x22),
				LeiosKeyPublic:          repeatedBytes(96, 0x66),
				LeiosKeyPossessionProof: repeatedBytes(48, 0x77),
				AddedSlot:               1,
			},
			nil,
		))

		got, err := ls.Query(t.Context(), stakePoolParamsQuery(pool), QueryPoint{})
		require.NoError(t, err)
		gotCbor, err := cbor.Encode(got)
		require.NoError(t, err)
		// 81 a1 581c<pool> then the record header.
		require.Equal(t, tc.fields, gotCbor[2+2+28+0], "pv %d", tc.major)
		if tc.fields == 0x8a {
			require.Contains(
				t,
				hex.EncodeToString(gotCbor),
				"5860"+strings.Repeat("66", 96)+"5830"+strings.Repeat("77", 48),
			)
		}
	}
}

func TestQueryStakePoolParams_GenesisRelayWireOrderAndBlsKey(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 12
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()

	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesis := cfg.ShelleyGenesis()
	key := &lcommon.LeiosKey{
		PublicKey:       repeatedBytes(96, 0x66),
		PossessionProof: repeatedBytes(48, 0x77),
	}
	encodedKey, err := json.Marshal(struct {
		PublicKey       string `json:"blsPubKey"`
		PossessionProof string `json:"blsPossessionProof"`
	}{
		PublicKey:       hex.EncodeToString(key.PublicKey),
		PossessionProof: hex.EncodeToString(key.PossessionProof),
	})
	require.NoError(t, err)
	var poolID string
	for id, pool := range genesis.ExtraConfig.StakePools.Data {
		pool.LeiosKey = json.RawMessage(encodedKey)
		genesis.ExtraConfig.StakePools.Data[id] = pool
		poolID = id
		break
	}
	require.NotEmpty(t, poolID)
	pools, delegations, err := initialPools(genesis)
	require.NoError(t, err)
	stakeDelegations, err := genesisStakeDelegations(delegations)
	require.NoError(t, err)
	cert := pools[poolID]
	ipv4 := net.IPv4(192, 168, 1, 1)
	ipv6 := net.ParseIP("2001:db8::1").To16()
	port := uint32(3001)
	cert.Relays = []lcommon.PoolRelay{{
		Type: lcommon.PoolRelayTypeSingleHostAddress,
		Port: &port,
		Ipv4: &ipv4,
		Ipv6: &ipv6,
	}}
	pools[poolID] = cert
	require.NoError(t, ls.db.SetGenesisStaking(
		pools,
		stakeDelegations,
		0,
		nil,
		nil,
	))

	poolHash, err := hex.DecodeString(poolID)
	require.NoError(t, err)
	got, err := ls.Query(t.Context(), stakePoolParamsQuery(poolHash), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	encoded := hex.EncodeToString(gotCbor)
	require.Contains(t, encoded, "440101a8c0")
	require.Contains(t, encoded, "50b80d0120000000000000000001000000")
	require.Contains(
		t,
		encoded,
		"5860"+strings.Repeat("66", 96)+"5830"+strings.Repeat("77", 48),
	)
}

func TestQueryStakePoolParams_SnapshotRelayWireOrder(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	setStakePoolParamsLiveState(ls, 0, 0, 10)
	poolID := repeatedBytes(28, 0x11)
	vrf := repeatedBytes(32, 0xAA)
	reward := repeatedBytes(28, 0x22)
	ipv4 := net.IP{1, 1, 168, 192}
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: poolID, VrfKeyHash: vrf, RewardAccount: reward,
		},
		&models.PoolRegistration{
			PoolKeyHash: poolID, VrfKeyHash: vrf, RewardAccount: reward,
			AddedSlot: 5,
			Relays:    []models.PoolRegistrationRelay{{Ipv4: &ipv4, Port: 3001}},
		},
		nil,
	))

	got, err := ls.Query(t.Context(), stakePoolParamsQuery(poolID), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Contains(t, hex.EncodeToString(gotCbor), "440101a8c0")
}

func TestInitialPoolsReadsGenesisLeiosKey(t *testing.T) {
	t.Parallel()

	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	genesis := cfg.ShelleyGenesis()
	require.NotNil(t, genesis.ExtraConfig)
	require.NotEmpty(t, genesis.ExtraConfig.StakePools.Data)

	key := &lcommon.LeiosKey{
		PublicKey:       repeatedBytes(96, 0x66),
		PossessionProof: repeatedBytes(48, 0x77),
	}
	encoded, err := json.Marshal(struct {
		PublicKey       string `json:"blsPubKey"`
		PossessionProof string `json:"blsPossessionProof"`
	}{
		PublicKey:       hex.EncodeToString(key.PublicKey),
		PossessionProof: hex.EncodeToString(key.PossessionProof),
	})
	require.NoError(t, err)
	var poolID string
	for id, pool := range genesis.ExtraConfig.StakePools.Data {
		pool.LeiosKey = encoded
		genesis.ExtraConfig.StakePools.Data[id] = pool
		poolID = id
		break
	}

	pools, _, err := initialPools(genesis)
	require.NoError(t, err)
	got := pools[poolID].LeiosKey
	require.NotNil(t, got)
	require.Equal(t, key.PublicKey, got.PublicKey)
	require.Equal(t, key.PossessionProof, got.PossessionProof)
}
