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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// poolStateNodeInEpochReregistered is the GetPoolState reply cardano-node
// 11.1.3 sent over NtC v23 for the devnet pool 6340ad23... in the epoch it
// registered and then re-registered with a higher cost: the first registration
// in effect, the second as future parameters, no retirement, and the deposit.
// The reply is the result element of MsgResult, so it carries the era wrapper.
const poolStateNodeInEpochReregistered = "" +
	"8184a1581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a84" +
	"6989581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a8469" +
	"582018a7f015b6b2f09f00178c09c1230b02a85aa0614fe9bf982e5985e06634" +
	"abc21a000f42401a1443fd00d81e82010a581de06b7cc75fe8d4062466ddaad4" +
	"38358b6a0f7f2b6b971679f87504b8b7d9010281581c6b7cc75fe8d4062466dd" +
	"aad438358b6a0f7f2b6b971679f87504b8b7828400190bb9440101a8c0f68301" +
	"190bb97172656c61792e6578616d706c652e636f6d82781a68747470733a2f2f" +
	"6578616d706c652e636f6d2f702e6a736f6e582067208fc064dff23c8311c14f" +
	"62712cc572cad299f4d363eaae4080fda36a16d7a1581c6340ad239fad55d94f" +
	"415adeb861889076889cf5bf11a8248b7a846989581c6340ad239fad55d94f41" +
	"5adeb861889076889cf5bf11a8248b7a8469582018a7f015b6b2f09f00178c09" +
	"c1230b02a85aa0614fe9bf982e5985e06634abc21a000f42401a14dc9380d81e" +
	"82010a581de06b7cc75fe8d4062466ddaad438358b6a0f7f2b6b971679f87504" +
	"b8b7d9010281581c6b7cc75fe8d4062466ddaad438358b6a0f7f2b6b971679f8" +
	"7504b8b7828400190bb9440101a8c0f68301190bb97172656c61792e6578616d" +
	"706c652e636f6d82781a68747470733a2f2f6578616d706c652e636f6d2f702e" +
	"6a736f6e582067208fc064dff23c8311c14f62712cc572cad299f4d363eaae40" +
	"80fda36a16d7a0a1581c6340ad239fad55d94f415adeb861889076889cf5bf11" +
	"a8248b7a84691a1dcd6500"

// poolStateNodeNextEpoch is the reply for the same pool one epoch boundary
// later: the second registration is in effect and nothing is pending.
const poolStateNodeNextEpoch = "" +
	"8184a1581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a84" +
	"6989581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a8469" +
	"582018a7f015b6b2f09f00178c09c1230b02a85aa0614fe9bf982e5985e06634" +
	"abc21a000f42401a14dc9380d81e82010a581de06b7cc75fe8d4062466ddaad4" +
	"38358b6a0f7f2b6b971679f87504b8b7d9010281581c6b7cc75fe8d4062466dd" +
	"aad438358b6a0f7f2b6b971679f87504b8b7828400190bb9440101a8c0f68301" +
	"190bb97172656c61792e6578616d706c652e636f6d82781a68747470733a2f2f" +
	"6578616d706c652e636f6d2f702e6a736f6e582067208fc064dff23c8311c14f" +
	"62712cc572cad299f4d363eaae4080fda36a16d7a0a0a1581c6340ad239fad55" +
	"d94f415adeb861889076889cf5bf11a8248b7a84691a1dcd6500"

// poolStateNodeRetiring is the reply once the pool has a pending retirement
// for epoch 15, announced in epoch 13.
const poolStateNodeRetiring = "" +
	"8184a1581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a84" +
	"6989581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a8469" +
	"582018a7f015b6b2f09f00178c09c1230b02a85aa0614fe9bf982e5985e06634" +
	"abc21a000f42401a14dc9380d81e82010a581de06b7cc75fe8d4062466ddaad4" +
	"38358b6a0f7f2b6b971679f87504b8b7d9010281581c6b7cc75fe8d4062466dd" +
	"aad438358b6a0f7f2b6b971679f87504b8b7828400190bb9440101a8c0f68301" +
	"190bb97172656c61792e6578616d706c652e636f6d82781a68747470733a2f2f" +
	"6578616d706c652e636f6d2f702e6a736f6e582067208fc064dff23c8311c14f" +
	"62712cc572cad299f4d363eaae4080fda36a16d7a0a1581c6340ad239fad55d9" +
	"4f415adeb861889076889cf5bf11a8248b7a84690fa1581c6340ad239fad55d9" +
	"4f415adeb861889076889cf5bf11a8248b7a84691a1dcd6500"

// poolStateNodeEmpty is the reply for a pool that is not registered, the
// empty filter, and a pool whose retirement has been reaped: four empty maps
// in the era wrapper, as cardano-node sent for the devnet pool after its
// retirement epoch.
const poolStateNodeEmpty = "8184a0a0a0a0"

const (
	poolStateDevnetPoolHex = "6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a8469"
	poolStateDevnetVrfHex  = "18a7f015b6b2f09f00178c09c1230b02a85aa0614fe9bf982e5985e06634abc2"
	poolStateDevnetKeyHex  = "6b7cc75fe8d4062466ddaad438358b6a0f7f2b6b971679f87504b8b7"
	poolStateDevnetMetaHex = "67208fc064dff23c8311c14f62712cc572cad299f4d363eaae4080fda36a16d7"
	poolStateDevnetDeposit = 500_000_000
)

func poolStateDevnetBytes(t *testing.T, h string) []byte {
	t.Helper()
	out, err := hex.DecodeString(h)
	require.NoError(t, err)
	return out
}

// seedPoolStateDevnetRegistration records the registration the devnet pool
// sent: owner and reward account one key hash, an address relay and a
// single-host-name relay, and metadata. The relay address is held in the
// ledger's wire byte order, as every stored registration is.
func seedPoolStateDevnetRegistration(
	t *testing.T,
	ls *LedgerState,
	addedSlot uint64,
	cost uint64,
) {
	t.Helper()
	pool := poolStateDevnetBytes(t, poolStateDevnetPoolHex)
	vrf := poolStateDevnetBytes(t, poolStateDevnetVrfHex)
	key := poolStateDevnetBytes(t, poolStateDevnetKeyHex)
	ipv4 := net.IP{1, 1, 168, 192}
	margin := &dbtypes.Rat{Rat: big.NewRat(1, 10)}
	require.NoError(t, ls.db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrf,
			RewardAccount: key,
			Margin:        margin,
			Pledge:        dbtypes.Uint64(1_000_000),
			Cost:          dbtypes.Uint64(cost),
		},
		&models.PoolRegistration{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrf,
			RewardAccount: key,
			Margin:        margin,
			Pledge:        dbtypes.Uint64(1_000_000),
			Cost:          dbtypes.Uint64(cost),
			DepositAmount: dbtypes.Uint64(poolStateDevnetDeposit),
			MetadataUrl:   "https://example.com/p.json",
			MetadataHash:  poolStateDevnetBytes(t, poolStateDevnetMetaHex),
			Owners:        []models.PoolRegistrationOwner{{KeyHash: key}},
			Relays: []models.PoolRegistrationRelay{
				{Ipv4: &ipv4, Port: 3001},
				{Hostname: "relay.example.com", Port: 3001},
			},
			AddedSlot: addedSlot,
		},
		nil,
	))
}

// poolStateQuery builds GetPoolState. A nil pools slice is the unfiltered
// form; a non-nil one, even empty, is an explicit pool set.
func poolStateQuery(pools [][]byte) *olocalstatequery.BlockQuery {
	q := &olocalstatequery.ShelleyPoolStateQuery{
		Type: olocalstatequery.QueryTypeShelleyPoolState,
	}
	if pools != nil {
		ids := make([]gledger.PoolId, 0, len(pools))
		for _, pool := range pools {
			ids = append(ids, gledger.PoolId(pool))
		}
		q.Pools = []cbor.SetType[gledger.PoolId]{cbor.NewSetType(ids, true)}
	}
	return shelleyBlockQuery(q)
}

func runPoolStateHex(
	t *testing.T,
	ls *LedgerState,
	query *olocalstatequery.BlockQuery,
	at QueryPoint,
) string {
	t.Helper()
	got, err := ls.Query(t.Context(), query, at)
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	return hex.EncodeToString(wire)
}

// newPoolStateTimeline builds the devnet pool's history across two epochs of
// 100 slots: registered at slot 5, re-registered with a higher cost at slot
// 50, a retirement for epoch 15 announced at slot 120, and the tip at slot
// 160. Blocks at slots 60, 110 and 130 give a point inside each stage.
func newPoolStateTimeline(t *testing.T) *LedgerState {
	t.Helper()
	ls := newStakePoolStateLedger(t)
	placeStakePoolStateTip(t, ls, 160)
	seedPoolStateDevnetRegistration(t, ls, 5, 340_000_000)
	seedPoolStateDevnetRegistration(t, ls, 50, 350_000_000)
	seedStakePoolRetirement(
		t, ls, poolStateDevnetBytes(t, poolStateDevnetPoolHex), 15, 120,
	)
	for _, slot := range []uint64{60, 110, 130} {
		seedBlockAtSlot(t, ls, slot, repeatedBytes(32, byte(slot)))
	}
	return ls
}

func poolStateAt(slot uint64) QueryPoint {
	return QueryPoint{Slot: slot, Hash: repeatedBytes(32, byte(slot))}
}

// TestQueryPoolState_NodeReplyInEpochOfReregistration pins the reply for a
// pool re-registered in the epoch it registered: the first registration is in
// effect and the second is future parameters until the boundary.
func TestQueryPoolState_NodeReplyInEpochOfReregistration(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	pool := poolStateDevnetBytes(t, poolStateDevnetPoolHex)
	require.Equal(
		t,
		poolStateNodeInEpochReregistered,
		runPoolStateHex(t, ls, poolStateQuery([][]byte{pool}), poolStateAt(60)),
	)
}

// TestQueryPoolState_NodeReplyNextEpoch pins the reply one boundary later,
// when the re-registration has taken effect and nothing is pending.
func TestQueryPoolState_NodeReplyNextEpoch(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	pool := poolStateDevnetBytes(t, poolStateDevnetPoolHex)
	require.Equal(
		t,
		poolStateNodeNextEpoch,
		runPoolStateHex(t, ls, poolStateQuery([][]byte{pool}), poolStateAt(110)),
	)
}

// TestQueryPoolState_NodeReplyRetiring pins the reply while a retirement is
// pending, and shows the live query at the timeline's tip answers it too.
func TestQueryPoolState_NodeReplyRetiring(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	pool := poolStateDevnetBytes(t, poolStateDevnetPoolHex)
	for name, at := range map[string]QueryPoint{
		"pinned": poolStateAt(130),
		"live":   {},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(
				t,
				poolStateNodeRetiring,
				runPoolStateHex(t, ls, poolStateQuery([][]byte{pool}), at),
			)
		})
	}
}

// TestQueryPoolState_UnfilteredAnswersEveryRegisteredPool answers the
// no-filter form with every registered pool and no unregistered one.
func TestQueryPoolState_UnfilteredAnswersEveryRegisteredPool(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	other := repeatedBytes(28, 0x11)
	seedStakePoolParamsRegistration(t, ls, other, 0, 6, 7)
	got, err := ls.Query(t.Context(), poolStateQuery(nil), QueryPoint{})
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	var wrapped []olocalstatequery.PoolStateResult
	_, err = cbor.Decode(wire, &wrapped)
	require.NoError(t, err)
	require.Len(t, wrapped, 1)
	result := wrapped[0]
	require.Len(t, result.PState, 2)
	require.Contains(t, result.PState, gledger.NewBlake2b224(other))
	require.Contains(
		t,
		result.PState,
		gledger.NewBlake2b224(poolStateDevnetBytes(t, poolStateDevnetPoolHex)),
	)
	require.Len(t, result.Retiring, 1, "only the devnet pool is retiring")
}

// TestQueryPoolState_AbsentPoolsAnswerEmptyMaps covers the negative cases: a
// pool that was never registered, the explicit empty set, a retired pool, and
// a pinned point before the pool's first registration.
func TestQueryPoolState_AbsentPoolsAnswerEmptyMaps(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	pool := poolStateDevnetBytes(t, poolStateDevnetPoolHex)
	seedBlockAtSlot(t, ls, 3, repeatedBytes(32, 3))
	tests := map[string]struct {
		query *olocalstatequery.BlockQuery
		at    QueryPoint
	}{
		"never registered": {
			query: poolStateQuery([][]byte{repeatedBytes(28, 0x77)}),
		},
		"empty set": {query: poolStateQuery([][]byte{})},
		"before first registration": {
			query: poolStateQuery([][]byte{pool}),
			at:    poolStateAt(3),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(
				t,
				poolStateNodeEmpty,
				runPoolStateHex(t, ls, tt.query, tt.at),
			)
		})
	}

	t.Run("retired", func(t *testing.T) {
		ls := newStakePoolStateLedger(t)
		placeStakePoolStateTip(t, ls, 260)
		seedPoolStateDevnetRegistration(t, ls, 5, 340_000_000)
		seedStakePoolRetirement(t, ls, pool, 2, 120)
		require.Equal(
			t,
			poolStateNodeEmpty,
			runPoolStateHex(t, ls, poolStateQuery([][]byte{pool}), QueryPoint{}),
			"the retirement epoch has begun, so the pool is reaped",
		)
	})
}

// TestQueryPoolState_ViaGetCBOR answers the query inside GetCBOR, the form
// cardano-cli sends for pool-state.
func TestQueryPoolState_ViaGetCBOR(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	pool := poolStateDevnetBytes(t, poolStateDevnetPoolHex)
	inner := poolStateQuery([][]byte{pool}).Query.(*olocalstatequery.ShelleyQuery).Query
	got, err := ls.Query(
		t.Context(),
		shelleyBlockQuery(&olocalstatequery.ShelleyCborQuery{Query: inner}),
		poolStateAt(130),
	)
	require.NoError(t, err)
	wire, err := cbor.Encode(got)
	require.NoError(t, err)
	want, err := cbor.Encode([]any{cbor.Tag{
		Number:  cbor.CborTagCbor,
		Content: poolStateDevnetBytes(t, poolStateNodeRetiring[2:]),
	}})
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(want), hex.EncodeToString(wire))
}

// TestQueryPoolState_PinnedPointWithoutEpochRejected refuses a pinned point
// no epoch record covers rather than answering for the live state.
func TestQueryPoolState_PinnedPointWithoutEpochRejected(t *testing.T) {
	t.Parallel()
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	seedEpochs(t, ls, map[uint64]uint64{100: 1})
	require.NoError(t, ls.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(160, repeatedBytes(32, 0x0B)),
	}, nil))
	setStakePoolParamsLiveState(ls, 1, 100, 160)
	seedBlockAtSlot(t, ls, 60, repeatedBytes(32, 60))
	_, err := ls.Query(
		t.Context(),
		poolStateQuery(nil),
		poolStateAt(60),
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryPoolState_OverLimitFilterRejected bounds the filter like every
// other caller-sized query.
func TestQueryPoolState_OverLimitFilterRejected(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	pools := make([][]byte, 0, MaxLocalStateQueryItems+1)
	for i := range MaxLocalStateQueryItems + 1 {
		pool := repeatedBytes(28, 0)
		pool[0], pool[1] = byte(i>>8), byte(i)
		pools = append(pools, pool)
	}
	_, err := ls.Query(t.Context(), poolStateQuery(pools), QueryPoint{})
	require.ErrorIs(t, err, ErrLocalStateQueryLimitExceeded)
}

// TestQueryPoolState_PinnedPointIgnoresLaterRegistrations answers a past point
// with the deposit the pool held then and no future parameters, although a
// registration added since would be both at the tip.
func TestQueryPoolState_PinnedPointIgnoresLaterRegistrations(t *testing.T) {
	t.Parallel()
	ls := newStakePoolStateLedger(t)
	placeStakePoolStateTip(t, ls, 160)
	seedBlockAtSlot(t, ls, 130, repeatedBytes(32, 130))
	pool := repeatedBytes(28, 0x11)
	vrf := repeatedBytes(32, 0xAA)
	for _, reg := range []struct {
		slot    uint64
		deposit uint64
	}{{5, 100}, {150, 300}} {
		require.NoError(t, ls.db.Metadata().ImportPool(
			&models.Pool{
				PoolKeyHash:   pool,
				VrfKeyHash:    vrf,
				RewardAccount: repeatedBytes(28, 0x22),
			},
			&models.PoolRegistration{
				PoolKeyHash:   pool,
				VrfKeyHash:    vrf,
				RewardAccount: repeatedBytes(28, 0x22),
				AddedSlot:     reg.slot,
				DepositAmount: dbtypes.Uint64(reg.deposit),
			},
			nil,
		))
	}
	decode := func(at QueryPoint) olocalstatequery.PoolStateResult {
		t.Helper()
		got, err := ls.Query(t.Context(), poolStateQuery([][]byte{pool}), at)
		require.NoError(t, err)
		wire, err := cbor.Encode(got)
		require.NoError(t, err)
		var wrapped []olocalstatequery.PoolStateResult
		_, err = cbor.Decode(wire, &wrapped)
		require.NoError(t, err)
		require.Len(t, wrapped, 1)
		return wrapped[0]
	}
	id := gledger.NewBlake2b224(pool)

	pinned := decode(poolStateAt(130))
	require.Empty(t, pinned.FState)
	require.Equal(t, map[gledger.Blake2b224]uint64{id: 100}, pinned.Deposits)

	live := decode(QueryPoint{})
	require.Contains(t, live.FState, id)
	require.Equal(t, map[gledger.Blake2b224]uint64{id: 300}, live.Deposits)
}

// TestQueryViewPoolStateUsesFrozenEpoch proves a held view answers
// GetPoolState for the epoch and tip it froze, not for the live epoch a
// boundary has since advanced to: in the frozen epoch one pool is retiring and
// another has a re-registration pending, and in the live epoch the retirement
// is over and nothing is pending.
func TestQueryViewPoolStateUsesFrozenEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	retiring := repeatedBytes(28, 0x11)
	pending := repeatedBytes(28, 0x12)

	// Epoch 3 starts at slot 300 and the frozen tip is slot 350.
	seedStakePoolParamsPool(t, ls, retiring, 0)
	seedStakePoolParamsPool(t, ls, pending, 0)
	seedStakePoolParamsRegistration(t, ls, pending, 0, 320, 2_000_000)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	seedStakePoolRetirement(t, ls, retiring, 5, 330)
	view := freezeEpochThenCrossBoundary(
		t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
	)
	ls.currentEpoch = models.Epoch{
		EpochId: 6, StartSlot: 600, LengthInSlots: 100,
	}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}
	ls.publishSnapshotsLocked()
	query := poolStateQuery([][]byte{retiring, pending})
	decode := func(got any) olocalstatequery.PoolStateResult {
		t.Helper()
		wire, err := cbor.Encode(got)
		require.NoError(t, err)
		var wrapped []olocalstatequery.PoolStateResult
		_, err = cbor.Decode(wire, &wrapped)
		require.NoError(t, err)
		require.Len(t, wrapped, 1)
		return wrapped[0]
	}
	retiringID := gledger.NewBlake2b224(retiring)
	pendingID := gledger.NewBlake2b224(pending)

	got, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	frozen := decode(got)
	require.Contains(t, frozen.PState, retiringID)
	require.Equal(t, map[gledger.Blake2b224]uint64{retiringID: 5}, frozen.Retiring)
	require.Contains(t, frozen.FState, pendingID)
	require.Equal(t, uint64(1_000_000), frozen.PState[pendingID].Pledge)
	require.Equal(t, uint64(2_000_000), frozen.FState[pendingID].Pledge)
}

// TestQueryViewStakePoolParamsUsesFrozenEpoch proves a held view answers
// GetStakePoolParams from the snapshot and epoch it froze, as GetPoolState
// does: in the frozen epoch the re-registration is still pending, so the
// first registration's pledge is in effect, while the live epoch already has
// the re-registration in effect.
func TestQueryViewStakePoolParamsUsesFrozenEpoch(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	pending := repeatedBytes(28, 0x13)

	// Epoch 3 starts at slot 300 and the frozen tip is slot 350.
	seedStakePoolParamsPool(t, ls, pending, 0)
	seedStakePoolParamsRegistration(t, ls, pending, 0, 320, 2_000_000)
	seedEpochs(t, ls, map[uint64]uint64{300: 3, 600: 6})
	view := freezeEpochThenCrossBoundary(
		t, ls, db, eras.ConwayEraDesc, eras.ConwayEraDesc,
	)
	ls.currentEpoch = models.Epoch{
		EpochId: 6, StartSlot: 600, LengthInSlots: 100,
	}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}
	ls.publishSnapshotsLocked()
	id := gledger.PoolId(gledger.NewBlake2b224(pending))
	pledge := func(got any) uint64 {
		t.Helper()
		wrapped, ok := got.([]any)
		require.True(t, ok)
		require.Len(t, wrapped, 1)
		result, ok := wrapped[0].(map[gledger.PoolId]stakePoolParams)
		require.True(t, ok)
		require.Contains(t, result, id)
		return result[id].Pledge
	}

	got, err := view.Query(t.Context(), stakePoolParamsQuery(pending), 0)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_000), pledge(got))

	live, err := ls.Query(
		t.Context(), stakePoolParamsQuery(pending), QueryPoint{},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2_000_000), pledge(live))
}
