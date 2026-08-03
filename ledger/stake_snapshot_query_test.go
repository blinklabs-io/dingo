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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixtures captured off the wire from cardano-cli 11.0.0.0
// `query stake-snapshot` against cardano-node 11.0.1 (devnet). See dingo
// issue #2917. The query payloads are LSQ MsgQuery (mini-protocol 7); the
// result fixtures are the CBOR carried inside the tag-24 Serialised wrapper
// of the MsgResult that cardano-node returns.
const (
	ssQueryOnePoolHex = "82038200820082068209821481" +
		"d9010281581c728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2"
	ssQueryAllPoolsHex = "82038200820082068209821480"

	// [ {728e…: [mark 1e12, set 0, go 0]}, markTotal 2e12, setTotal 1, goTotal 1 ]
	ssResultOnePoolInnerHex = "84a1581c728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2" +
		"831b000000e8d4a5100000001b000001d1a94a20000101"

	// [ {728e…: [1e12, 1e12, 0], ccfa…: [1e12, 1e12, 0]}, 2e12, 2e12, 1 ]
	ssResultAllPoolsInnerHex = "84a2" +
		"581c728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2" +
		"831b000000e8d4a510001b000000e8d4a5100000" +
		"581cccfa09b0c1f3fe9650a11b4d23d5c461df76f6ff10eb95018940984f" +
		"831b000000e8d4a510001b000000e8d4a5100000" +
		"1b000001d1a94a20001b000001d1a94a200001"

	ssPool1Hex = "728ea45cc4888f97d1c3233956fd6abf9362854d880efd6e577807f2"
	ssPool2Hex = "ccfa09b0c1f3fe9650a11b4d23d5c461df76f6ff10eb95018940984f"

	oneTrillion = uint64(1_000_000_000_000)
	twoTrillion = uint64(2_000_000_000_000)
)

// blockQueryFromHex decodes a captured LSQ MsgQuery payload and returns the
// inner *BlockQuery, exactly the value dingo's LSQ server hands to
// LedgerState.Query.
func blockQueryFromHex(t *testing.T, payloadHex string) *olocalstatequery.BlockQuery {
	t.Helper()
	msg, err := olocalstatequery.NewMsgFromCbor(
		olocalstatequery.MessageTypeQuery,
		mustHex(t, payloadHex),
	)
	require.NoError(t, err, "captured stake-snapshot query must decode (issue #2917)")
	msgQuery, ok := msg.(*olocalstatequery.MsgQuery)
	require.True(t, ok)
	blockQuery, ok := msgQuery.Query.Query.(*olocalstatequery.BlockQuery)
	require.True(t, ok)
	return blockQuery
}

func markSnapshot(
	t *testing.T,
	epoch uint64,
	poolHex string,
	stake uint64,
) *models.PoolStakeSnapshot {
	t.Helper()
	return &models.PoolStakeSnapshot{
		Epoch:          epoch,
		SnapshotType:   "mark",
		PoolKeyHash:    mustHex(t, poolHex),
		TotalStake:     types.Uint64(stake),
		DelegatorCount: 1,
		CapturedSlot:   epoch * 100,
	}
}

func epochSummary(epoch, total uint64) *models.EpochSummary {
	return &models.EpochSummary{
		Epoch:            epoch,
		TotalActiveStake: types.Uint64(total),
		SnapshotReady:    true,
	}
}

// serialisedInner runs the query through LedgerState.Query and returns the
// bytes inside the tag-24 (CBOR-in-CBOR) Serialised wrapper, asserting the
// GetCBOR result shape along the way.
func serialisedInner(t *testing.T, ls *LedgerState, payloadHex string) []byte {
	t.Helper()
	result, err := ls.Query(blockQueryFromHex(t, payloadHex))
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok, "expected []any MsgResult wire form")
	require.Len(t, outer, 1)
	tag, ok := outer[0].(cbor.Tag)
	require.True(t, ok, "GetCBOR result must be a CBOR tag")
	require.Equal(t, uint64(cbor.CborTagCbor), tag.Number, "expected tag 24 (CBOR-in-CBOR)")
	inner, ok := tag.Content.([]byte)
	require.True(t, ok, "tag-24 content must be a byte string")
	return inner
}

// TestQueryStakeSnapshotSpecificPool reproduces the exact single-pool
// scenario captured from cardano-node and asserts dingo emits byte-identical
// serialised CBOR. Reproduces and guards the fix for issue #2917.
func TestQueryStakeSnapshotSpecificPool(t *testing.T) {
	db := newTestDB(t)
	meta := db.Metadata()
	// Only pool1 has a current-epoch (mark) snapshot; set/go are empty.
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{markSnapshot(t, 2, ssPool1Hex, oneTrillion)},
		nil,
	))
	// Only the mark total exists (2e12, both pools). The set/go epochs have
	// no data, so their totals are 0 and must be reported as the NonZero
	// minimum 1 -- matching cardano-node -- giving totals [2e12, 1, 1].
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, twoTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}})

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	require.Equal(t,
		ssResultOnePoolInnerHex,
		hex.EncodeToString(inner),
		"serialised stake-snapshot must match cardano-node bytes",
	)
}

// TestQueryStakeSnapshotAllPools reproduces the all-pools scenario captured
// from cardano-node and asserts byte-identical serialised CBOR, including the
// canonical (sorted) pool-map key order.
func TestQueryStakeSnapshotAllPools(t *testing.T) {
	db := newTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			// mark (current epoch 2)
			markSnapshot(t, 2, ssPool1Hex, oneTrillion),
			markSnapshot(t, 2, ssPool2Hex, oneTrillion),
			// set (epoch 1)
			markSnapshot(t, 1, ssPool1Hex, oneTrillion),
			markSnapshot(t, 1, ssPool2Hex, oneTrillion),
			// go (epoch 0) intentionally absent -> per-pool go = 0
		},
		nil,
	))
	// mark and set totals are 2e12; the go epoch has no data, so goTotal is
	// clamped to the NonZero minimum 1, giving totals [2e12, 2e12, 1].
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, twoTrillion), nil))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(1, twoTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}})

	inner := serialisedInner(t, ls, ssQueryAllPoolsHex)
	require.Equal(t,
		ssResultAllPoolsInnerHex,
		hex.EncodeToString(inner),
		"serialised all-pools stake-snapshot must match cardano-node bytes",
	)
}

// TestQueryStakeSnapshotEarlyEpochsNonZeroTotals covers the genesis case:
// at epoch 0 the set/go epochs would underflow, so they must resolve to zero
// stake without querying a bogus wrapped-around epoch, and the set/go totals
// must be reported as the NonZero minimum 1 rather than 0 (cardano clients
// decode the totals as NonZero). This mirrors cardano-node 11.0.1 on a fresh
// devnet, which reports total set/go = 1 while every pool's set/go stake is 0.
func TestQueryStakeSnapshotEarlyEpochsNonZeroTotals(t *testing.T) {
	db := newTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{markSnapshot(t, 0, ssPool1Hex, oneTrillion)},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(0, oneTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 0}})

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	// [ {728e…: [mark 1e12, set 0, go 0]}, markTotal 1e12, setTotal 1, goTotal 1 ]
	var decoded []any
	_, err := cbor.Decode(inner, &decoded)
	require.NoError(t, err)
	require.Len(t, decoded, 4)
	require.EqualValues(t, oneTrillion, decoded[1], "markTotal")
	require.EqualValues(t, 1, decoded[2], "setTotal must be NonZero (1) at epoch 0")
	require.EqualValues(t, 1, decoded[3], "goTotal must be NonZero (1) at epoch 0")
	// The per-pool set/go stakes remain plain zero (not clamped).
	pools, ok := decoded[0].(map[any]any)
	require.True(t, ok)
	for _, v := range pools {
		perPool, ok := v.([]any)
		require.True(t, ok)
		require.Len(t, perPool, 3)
		require.EqualValues(t, 0, perPool[1], "per-pool set stake stays 0")
		require.EqualValues(t, 0, perPool[2], "per-pool go stake stays 0")
	}
}

// TestQueryStakeSnapshotAllPoolsIncludesRetiredPool covers the all-pools path
// building the pool set from the union of the mark/set/go snapshots: a pool
// that has retired keeps historical set/go stake and must still be reported
// even though it has no current-epoch (mark) snapshot.
func TestQueryStakeSnapshotAllPoolsIncludesRetiredPool(t *testing.T) {
	db := newTestDB(t)
	meta := db.Metadata()
	// pool1 is active (mark@epoch 2). pool2 retired: it only appears in the
	// previous epoch's snapshot, which is the current epoch's "set".
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, 2, ssPool1Hex, oneTrillion),
			markSnapshot(t, 1, ssPool2Hex, oneTrillion),
		},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, twoTrillion), nil))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(1, oneTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}})

	inner := serialisedInner(t, ls, ssQueryAllPoolsHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	require.Len(t, result.PoolSnapshots, 2, "both pools must be reported")

	pool1 := ledger.NewBlake2b224(mustHex(t, ssPool1Hex))
	pool2 := ledger.NewBlake2b224(mustHex(t, ssPool2Hex))
	require.Contains(t, result.PoolSnapshots, pool1)
	retired, ok := result.PoolSnapshots[pool2]
	require.True(t, ok, "retired pool with only set/go stake must be reported")
	assert.EqualValues(t, 0, retired.StakeMark, "retired pool has no mark stake")
	assert.EqualValues(t, oneTrillion, retired.StakeSet, "retired pool keeps its set stake")
	assert.EqualValues(t, 0, retired.StakeGo)
}

// consensusAtVersion builds a consensus snapshot pinned to a protocol major
// version, so stake-snapshot tests can exercise the PV11 zero-pool filtering.
func consensusAtVersion(epoch uint64, major uint) *consensusSnapshot {
	return &consensusSnapshot{
		currentEpoch: models.Epoch{EpochId: epoch},
		currentPParams: &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: major,
			},
		},
	}
}

// TestQueryStakeSnapshotNonexistentPoolPV10 verifies that below PV11 an
// explicitly requested pool with no stake is still returned, all-zero,
// matching pre-PV11 cardano-ledger GetStakeSnapshots semantics.
func TestQueryStakeSnapshotNonexistentPoolPV10(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.consensus.Store(consensusAtVersion(2, 10))

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	require.Len(t, result.PoolSnapshots, 1,
		"below PV11 an explicitly requested pool is returned even with zero stake")
	snap, ok := result.PoolSnapshots[ledger.NewBlake2b224(mustHex(t, ssPool1Hex))]
	require.True(t, ok)
	assert.EqualValues(t, 0, snap.StakeMark)
	assert.EqualValues(t, 0, snap.StakeSet)
	assert.EqualValues(t, 0, snap.StakeGo)
}

// TestQueryStakeSnapshotNonexistentPoolPV11 verifies that at PV11 an
// explicitly requested pool whose mark/set/go are all zero is omitted
// (cardano-ledger issue 5581).
func TestQueryStakeSnapshotNonexistentPoolPV11(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.consensus.Store(consensusAtVersion(2, 11))

	inner := serialisedInner(t, ls, ssQueryOnePoolHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	assert.Empty(t, result.PoolSnapshots,
		"PV11 omits all-zero pools even when explicitly requested")
	// Totals are still reported, clamped to the NonZero minimum.
	assert.EqualValues(t, 1, result.TotalStakeMark)
}

// TestQueryStakeSnapshotAllPoolsPV11OmitsZeroStake verifies that at PV11 an
// all-pools query drops a pool whose snapshots are all zero while keeping
// pools that carry stake.
func TestQueryStakeSnapshotAllPoolsPV11OmitsZeroStake(t *testing.T) {
	db := newTestDB(t)
	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			markSnapshot(t, 2, ssPool1Hex, oneTrillion), // has stake
			markSnapshot(t, 2, ssPool2Hex, 0),           // explicit zero-stake row
		},
		nil,
	))
	require.NoError(t, meta.SaveEpochSummary(epochSummary(2, oneTrillion), nil))
	ls := &LedgerState{db: db}
	ls.consensus.Store(consensusAtVersion(2, 11))

	inner := serialisedInner(t, ls, ssQueryAllPoolsHex)
	var result olocalstatequery.StakeSnapshotsResult
	_, err := cbor.Decode(inner, &result)
	require.NoError(t, err)
	require.Len(t, result.PoolSnapshots, 1)
	assert.Contains(t, result.PoolSnapshots,
		ledger.NewBlake2b224(mustHex(t, ssPool1Hex)), "pool with stake is kept")
	assert.NotContains(t, result.PoolSnapshots,
		ledger.NewBlake2b224(mustHex(t, ssPool2Hex)), "all-zero pool is omitted")
}
