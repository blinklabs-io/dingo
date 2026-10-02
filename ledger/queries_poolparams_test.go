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
	"math/big"
	"net"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func shelleyBlockQuery(leaf any) *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{Query: leaf},
	}
}

func hexBytes(t *testing.T, parts ...string) []byte {
	t.Helper()
	out, err := hex.DecodeString(strings.Join(parts, ""))
	require.NoError(t, err)
	return out
}

func stakePoolParamsQuery(pools ...[]byte) *olocalstatequery.BlockQuery {
	ids := make([]ledger.PoolId, 0, len(pools))
	for _, pool := range pools {
		ids = append(ids, ledger.PoolId(ledger.NewBlake2b224(pool)))
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
	ipv4 := net.IPv4(192, 168, 1, 1)
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
			Pledge:                     dbtypes.Uint64(1_000_000),
			Cost:                       dbtypes.Uint64(340_000_000),
		},
		&models.PoolRegistration{
			PoolKeyHash:                poolKeyHash,
			VrfKeyHash:                 vrf,
			RewardAccount:              reward,
			RewardAccountCredentialTag: rewardTag,
			Margin:                     margin,
			Pledge:                     dbtypes.Uint64(1_000_000),
			Cost:                       dbtypes.Uint64(340_000_000),
			MetadataUrl:                "https://a.io/p",
			MetadataHash:               repeatedBytes(32, 0x55),
			Owners:                     owners,
			Relays:                     relays,
			AddedSlot:                  1,
		},
		nil,
	))
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
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 0)
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x88), 0)

	// 0x77 is not registered and 0x88 is registered but not requested.
	got, err := ls.Query(
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
		"8400190bb944c0a80101f6",
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
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 1)

	got, err := ls.Query(
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
	seedStakePoolParamsPool(t, ls, repeatedBytes(28, 0x11), 0)

	got, err := ls.Query(stakePoolParamsQuery(), QueryPoint{})
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(t, "81a0", hex.EncodeToString(gotCbor))
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
	live, err := ls.Query(query, QueryPoint{})
	require.NoError(t, err)
	liveCbor, err := cbor.Encode(live)
	require.NoError(t, err)
	require.Equal(
		t,
		hex.EncodeToString(hexBytes(t, "8182", "18c8", "5820"+strings.Repeat("cd", 32))),
		hex.EncodeToString(liveCbor),
	)

	pinned, err := ls.Query(query, QueryPoint{Slot: 100, Hash: pinnedHash})
	require.NoError(t, err)
	pinnedCbor, err := cbor.Encode(pinned)
	require.NoError(t, err)
	require.Equal(
		t,
		hex.EncodeToString(hexBytes(t, "8182", "1864", "5820"+strings.Repeat("ab", 32))),
		hex.EncodeToString(pinnedCbor),
	)
}

// TestQueryProposedProtocolParamsUpdates answers an empty map: Conway
// replaced the update proposal mechanism with governance actions.
func TestQueryProposedProtocolParamsUpdates(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	got, err := ls.Query(
		shelleyBlockQuery(&olocalstatequery.ShelleyProposedProtocolParamsUpdatesQuery{}),
		QueryPoint{},
	)
	require.NoError(t, err)
	gotCbor, err := cbor.Encode(got)
	require.NoError(t, err)
	require.Equal(t, "81a0", hex.EncodeToString(gotCbor))
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

	want := hexBytes(t,
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
		"00", "00",
		"d81e820001",
		"581de0c002c1f5bccc09b7d2865b8ba5bc6fb280b2f5f0e63cb11da79f413e",
		"d9010280",
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
