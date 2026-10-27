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
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stakeDistribution2Query wraps the leaf query the way the wire delivers it.
// GetStakeDistribution2 has no pool filter, unlike GetPoolDistr2.
func stakeDistribution2Query() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyStakeDistribution2Query{},
		},
	}
}

func seedStakeDistribution2Pools(
	t *testing.T,
	ls *LedgerState,
) (poolA, poolB lcommon.PoolKeyHash, vrfA, vrfB []byte) {
	t.Helper()
	vrfA, vrfB = make([]byte, 32), make([]byte, 32)
	for i := range vrfA {
		vrfA[i], vrfB[i] = 0xAA, 0xBB
	}
	idA, idB := make([]byte, 28), make([]byte, 28)
	for i := range idA {
		idA[i], idB[i] = 0x11, 0x22
	}
	const snapshotEpoch = 0
	poolA = seedPoolDistr2Fixture(t, ls.db, idA, vrfA, 3_000_000, snapshotEpoch)
	poolB = seedPoolDistr2Fixture(t, ls.db, idB, vrfB, 1_000_000, snapshotEpoch)
	return poolA, poolB, vrfA, vrfB
}

// TestQueryShelleyStakeDistribution2_ReportsTheLeaderElectionDistribution
// covers the query cardano-cli uses from node-to-client version 21, where
// GetStakeDistribution2 replaces GetStakeDistribution. It is the same pool
// distribution GetPoolDistr2 reports with no filter, and it must decode into
// the client's own StakeDistribution2Result.
func TestQueryShelleyStakeDistribution2_ReportsTheLeaderElectionDistribution(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	poolA, poolB, vrfA, vrfB := seedStakeDistribution2Pools(t, ls)

	result, err := ls.Query(t.Context(), stakeDistribution2Query(), QueryPoint{})
	require.NoError(t, err)
	encoded, err := cbor.Encode(&result)
	require.NoError(t, err)
	var decoded olocalstatequery.StakeDistribution2Result
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err, "the reply must decode as a StakeDistribution2Result")
	distr := decoded.Distribution

	assert.Equal(t, uint64(4_000_000), distr.TotalActiveStake)
	require.Len(t, distr.Pools, 2)
	entryA := distr.Pools[lcommon.PoolId(poolA)]
	entryB := distr.Pools[lcommon.PoolId(poolB)]
	assert.Equal(t, uint64(3_000_000), entryA.TotalPoolStake)
	assert.Equal(t, uint64(1_000_000), entryB.TotalPoolStake)
	assert.Equal(t, vrfA, entryA.VrfHash[:])
	assert.Equal(t, vrfB, entryB.VrfHash[:])
	require.NotNil(t, entryA.StakeFraction)
	require.NotNil(t, entryB.StakeFraction)
	sum := new(big.Rat).Add(entryA.StakeFraction.Rat, entryB.StakeFraction.Rat)
	assert.Zero(t, sum.Cmp(big.NewRat(1, 1)), "fractions must sum to one, got %s", sum)
}

// TestQueryShelleyStakeDistribution2_ViaGetCBOR covers the query wrapped in
// the GetCBOR combinator, whose tag-24 content is the bare PoolDistr
// ([pool map, total]) a node wraps, with no era result wrapper around it.
func TestQueryShelleyStakeDistribution2_ViaGetCBOR(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	poolA, _, _, _ := seedStakeDistribution2Pools(t, ls)

	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCborQuery{
				Query: &olocalstatequery.ShelleyStakeDistribution2Query{},
			},
		},
	}
	result, err := ls.Query(t.Context(), query, QueryPoint{})
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)
	tag, ok := arr[0].(cbor.Tag)
	require.True(t, ok, "expected a tag-24 CBOR.Tag, got %T", arr[0])
	content, ok := tag.Content.([]byte)
	require.True(t, ok, "tag content must be raw CBOR bytes")

	var decoded olocalstatequery.PoolDistribution
	_, err = cbor.Decode(content, &decoded)
	require.NoError(t, err, "tag-24 content must decode as a PoolDistribution")
	entry, ok := decoded.Pools[lcommon.PoolId(poolA)]
	require.True(t, ok, "pool missing from the GetCBOR-wrapped distribution")
	assert.Equal(t, uint64(3_000_000), entry.TotalPoolStake)
	assert.Equal(t, uint64(4_000_000), decoded.TotalActiveStake)
}

// newHalfDelegatedLedger seeds two pools holding 1_000_000 each in the mark
// snapshot of a network whose circulation (8_000_000 maximum supply minus
// 4_000_000 reserves) is twice what the pools hold delegated between them.
func newHalfDelegatedLedger(
	t *testing.T,
	db *database.Database,
) (*LedgerState, lcommon.PoolKeyHash, lcommon.PoolKeyHash) {
	t.Helper()
	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0xAA), repeatedBytes(32, 0x01),
		1_000_000, snapshotEpoch,
	)
	pkhB := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0xBB), repeatedBytes(32, 0x02),
		1_000_000, snapshotEpoch,
	)
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 8000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	require.NoError(t, db.Metadata().SetNetworkState(0, 4_000_000, 1, nil))
	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cfg
	seedEpochs(t, ls, map[uint64]uint64{0: 0})
	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 1, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))
	return ls, pkhA, pkhB
}

// TestQueryShelleyStakeDistribution2_FractionIsOverCirculatingSupply pins the
// denominator of each pool's StakeFraction to circulating supply, as
// cardano-node's poolsByTotalStakeFraction does, rather than to the sum of
// delegated stake. Per-pool stake and the reply's total are unchanged.
func TestQueryShelleyStakeDistribution2_FractionIsOverCirculatingSupply(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls, pkhA, pkhB := newHalfDelegatedLedger(t, db)

	result, err := ls.Query(t.Context(), stakeDistribution2Query(), QueryPoint{})
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

	require.Len(t, distr.Pools, 2)
	for _, pkh := range []lcommon.PoolKeyHash{pkhA, pkhB} {
		entry, ok := distr.Pools[lcommon.PoolId(pkh)]
		require.True(t, ok)
		require.NotNil(t, entry.StakeFraction)
		assert.Zero(t, entry.StakeFraction.Cmp(big.NewRat(1, 4)),
			"GetStakeDistribution2 fraction must be over circulating supply, got %s",
			entry.StakeFraction.Rat)
		assert.Equal(t, uint64(1_000_000), entry.TotalPoolStake)
	}
	assert.Equal(t, uint64(2_000_000), distr.TotalActiveStake)
}

// TestQueryShelleyPoolDistr2_FractionStaysOverDelegatedStake pins that the
// same half-delegated network still reports GetPoolDistr2 fractions over the
// sum of delegated stake, which is the reference behavior for that query.
func TestQueryShelleyPoolDistr2_FractionStaysOverDelegatedStake(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls, pkhA, pkhB := newHalfDelegatedLedger(t, db)

	result, err := ls.Query(t.Context(), poolDistr2Query(), QueryPoint{})
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

	require.Len(t, distr.Pools, 2)
	for _, pkh := range []lcommon.PoolKeyHash{pkhA, pkhB} {
		entry, ok := distr.Pools[lcommon.PoolId(pkh)]
		require.True(t, ok)
		assert.Zero(t, entry.StakeFraction.Cmp(big.NewRat(1, 2)),
			"GetPoolDistr2 fraction must stay over delegated stake, got %s",
			entry.StakeFraction.Rat)
	}
	assert.Equal(t, uint64(2_000_000), distr.TotalActiveStake)
}
