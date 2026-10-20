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
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func poolDistrQuery(pools ...lcommon.PoolKeyHash) *localstatequery.BlockQuery {
	var sets []cbor.SetType[lcommon.PoolId]
	if pools != nil {
		ids := make([]lcommon.PoolId, 0, len(pools))
		for _, pool := range pools {
			ids = append(ids, lcommon.PoolId(pool))
		}
		sets = []cbor.SetType[lcommon.PoolId]{cbor.NewSetType(ids, false)}
	}
	return &localstatequery.BlockQuery{Query: &localstatequery.ShelleyQuery{
		Query: &localstatequery.ShelleyPoolDistrQuery{
			Type:  localstatequery.QueryTypeShelleyPoolDistr,
			Pools: sets,
		},
	}}
}

func decodePoolDistrResult(t *testing.T, result any) localstatequery.PoolDistrResult {
	t.Helper()
	encoded, err := cbor.Encode(&result)
	require.NoError(t, err)
	var decoded localstatequery.PoolDistrResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	return decoded
}

func TestQueryShelleyPoolDistrMatchesLegacyClientResult(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	vrfA := make([]byte, 32)
	vrfB := make([]byte, 32)
	for i := range vrfA {
		vrfA[i] = 0xAA
		vrfB[i] = 0xBB
	}
	poolA := make([]byte, 28)
	poolB := make([]byte, 28)
	for i := range poolA {
		poolA[i] = 0x11
		poolB[i] = 0x22
	}
	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)
	pkhB := seedPoolDistr2Fixture(t, db, poolB, vrfB, 1_000_000, snapshotEpoch)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(t.Context(), poolDistrQuery(), QueryPoint{})
	require.NoError(t, err)
	distr := decodePoolDistrResult(t, result)
	require.Len(t, distr.Results, 2)
	entryA, ok := distr.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	require.Equal(t, vrfA, entryA.VrfHash[:])
	require.NotNil(t, entryA.StakeFraction)
	require.Equal(t, 0, entryA.StakeFraction.Cmp(big.NewRat(3, 4)))
	entryB, ok := distr.Results[lcommon.PoolId(pkhB)]
	require.True(t, ok)
	require.Equal(t, vrfB, entryB.VrfHash[:])
	require.NotNil(t, entryB.StakeFraction)
	require.Equal(t, 0, entryB.StakeFraction.Cmp(big.NewRat(1, 4)))

	filteredResult, err := ls.Query(t.Context(), poolDistrQuery(pkhA), QueryPoint{})
	require.NoError(t, err)
	filtered := decodePoolDistrResult(t, filteredResult)
	require.Len(t, filtered.Results, 1)
	filteredA, ok := filtered.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	require.Equal(t, 0, filteredA.StakeFraction.Cmp(big.NewRat(3, 4)),
		"filtering must not renormalize stake fractions")

	emptyFilter := make([]lcommon.PoolKeyHash, 0)
	emptyResult, err := ls.Query(t.Context(), poolDistrQuery(emptyFilter...), QueryPoint{})
	require.NoError(t, err)
	require.Empty(t, decodePoolDistrResult(t, emptyResult).Results,
		"an explicit empty set selects no pools")
}
