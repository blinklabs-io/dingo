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
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// protocolParamsQuery wraps GetCurrentProtocolParams the way the wire
// delivers it, matching poolDistr2Query/stakeDistributionQuery in the
// neighboring query test files.
func protocolParamsQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCurrentProtocolParamsQuery{},
		},
	}
}

// conwayPParamsWithCostModels builds a Conway pparams value with every
// cbor.Rat-bearing field populated, not just CostModels -- blinklabs-io/dingo#3825's
// PR review (wolf31o2): a fixture that only sets CostModels type-asserts fine
// but is not actually encodable, since cbor.Rat.MarshalCBOR panics on the nil
// *big.Rat a zero-value cbor.Rat (or a nil *cbor.Rat pointer field) carries,
// and PoolVotingThresholds/DRepVotingThresholds's value-typed cbor.Rat fields
// are always encoded (never skippable as CBOR null the way a nil *cbor.Rat
// pointer field is). This is what real cardano-node protocol-parameter data
// always has populated, so an end-to-end wire test should encode a value
// shaped like the real thing, not a partial struct that happens to satisfy a
// type assertion.
func conwayPParamsWithCostModels(
	costModels map[uint][]int64,
) *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	ratPtr := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	return &conway.ConwayProtocolParameters{
		CostModels:                 costModels,
		A0:                         ratPtr(3, 10),
		Rho:                        ratPtr(3, 1000),
		Tau:                        ratPtr(1, 5),
		MinFeeRefScriptCostPerByte: ratPtr(15, 1),
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  ratPtr(577, 10000),
			StepPrice: ratPtr(721, 10000000),
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    rat(51, 100),
			CommitteeNormal:       rat(51, 100),
			CommitteeNoConfidence: rat(51, 100),
			HardForkInitiation:    rat(51, 100),
			PpSecurityGroup:       rat(51, 100),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    rat(67, 100),
			CommitteeNormal:       rat(67, 100),
			CommitteeNoConfidence: rat(60, 100),
			UpdateToConstitution:  rat(75, 100),
			HardForkInitiation:    rat(60, 100),
			PpNetworkGroup:        rat(67, 100),
			PpEconomicGroup:       rat(67, 100),
			PpTechnicalGroup:      rat(67, 100),
			PpGovGroup:            rat(75, 100),
			TreasuryWithdrawal:    rat(67, 100),
		},
	}
}

// TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault covers the
// actual code path this session found responsible for blinklabs-io/dingo#3825:
// HardForkBabbage fabricates a PlutusV2 cost model whenever the previous
// era's params don't have one -- real for any Alonzo genesis, since the
// AlonzoGenesisCostModels format predates PlutusV2 entirely and never has a
// slot for it.
func TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault(
	t *testing.T,
) {
	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.True(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent covers a pparams
// value that already carries a real (non-fabricated) PlutusV2 entry before
// the transition -- HardForkBabbage's own guard (`if _, hasV2 :=
// ret.CostModels[1]; !hasV2`) leaves it untouched, so nothing was injected.
func TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent(t *testing.T) {
	realV2 := []int64{9, 9, 9}
	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: realV2},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.False(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault covers
// a hypothetical newly-added key 1 whose value does not match
// eras.DefaultPlutusV2CostModel -- only the exact known fabricated value
// counts as synthetic, not "any new key 1."
func TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault(
	t *testing.T,
) {
	before := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: {999}},
	}

	assert.False(t, injectedSyntheticV2CostModel(before, after))
}

// TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal covers
// the query-boundary filter: when synthetic is true, the returned value
// omits PlutusV2 while every other key survives, and the original pparams
// (still reachable from internal validation state) is never mutated.
func TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal(
	t *testing.T,
) {
	original := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 1, 1},
			1: {2, 2, 2},
			2: {3, 3, 3},
		},
	}

	filtered := withoutSyntheticV2CostModel(original, true, nil)

	fp, ok := filtered.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, fp.CostModels, uint(1))
	assert.Equal(t, []int64{1, 1, 1}, fp.CostModels[0])
	assert.Equal(t, []int64{3, 3, 3}, fp.CostModels[2])

	// The original, still reachable from ls.currentPParams / the published
	// snapshot for internal script validation, must be untouched.
	assert.Contains(t, original.CostModels, uint(1))
	assert.Equal(t, []int64{2, 2, 2}, original.CostModels[1])
}

// TestWithoutSyntheticV2CostModel_CoversEveryEraType covers
// blinklabs-io/dingo#3825's PR review: the filter's type switch must handle
// every era type ShelleyCurrentProtocolParamsQuery can actually return
// (Alonzo, Babbage, Conway, Dijkstra), not just Conway -- a regression in
// any branch would otherwise pass the suite silently.
func TestWithoutSyntheticV2CostModel_CoversEveryEraType(t *testing.T) {
	costModels := map[uint][]int64{0: {1}, 1: {2}, 2: {3}}

	t.Run("Alonzo", func(t *testing.T) {
		pp := &alonzo.AlonzoProtocolParameters{CostModels: cloneMap(costModels)}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*alonzo.AlonzoProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Babbage", func(t *testing.T) {
		pp := &babbage.BabbageProtocolParameters{
			CostModels: cloneMap(costModels),
		}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*babbage.BabbageProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Conway", func(t *testing.T) {
		pp := &conway.ConwayProtocolParameters{CostModels: cloneMap(costModels)}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*conway.ConwayProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Dijkstra", func(t *testing.T) {
		pp := &dijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: conway.ConwayProtocolParameters{
				CostModels: cloneMap(costModels),
			},
		}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*dijkstra.DijkstraProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
}

func cloneMap(m map[uint][]int64) map[uint][]int64 {
	out := make(map[uint][]int64, len(m))
	for k, v := range m {
		out[k] = append([]int64(nil), v...)
	}
	return out
}

// TestWithoutSyntheticV2CostModel_NilPointerDoesNotPanic covers
// blinklabs-io/dingo#3825's PR review: a concrete-typed nil pointer
// (lcommon.ProtocolParameters holding e.g. a nil *conway.ConwayProtocolParameters)
// still matches its type's case in the switch, so each case must guard
// against nil before dereferencing rather than panicking.
func TestWithoutSyntheticV2CostModel_NilPointerDoesNotPanic(t *testing.T) {
	var nilConway *conway.ConwayProtocolParameters
	var pp lcommon.ProtocolParameters = nilConway

	require.NotPanics(t, func() {
		got := withoutSyntheticV2CostModel(pp, true, nil)
		assert.Equal(t, pp, got)
	})
}

// TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic covers the common
// case: once real data has been observed (or none was ever fabricated),
// the filter must return the value unchanged, identical pointer included,
// so a caller reading it sees the exact same struct internal validation
// uses.
func TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1}, 1: {2}, 2: {3}},
	}

	got := withoutSyntheticV2CostModel(pp, false, nil)

	assert.Same(t, pp, got)
}

// unknownProtocolParameters is a lcommon.ProtocolParameters implementation
// the withoutSyntheticV2CostModel switch has no case for -- standing in for
// a future era type this switch hasn't been taught yet.
type unknownProtocolParameters struct {
	lcommon.ProtocolParameters
}

// TestWithoutSyntheticV2CostModel_UnknownTypeLogsAndReturnsUnfiltered covers
// blinklabs-io/dingo#3825's PR review (wolf31o2): a protocol-parameters type
// the switch doesn't recognize falls to the default branch, which -- unlike
// every other branch -- returns pp unfiltered even though synthetic is true.
// That silently reintroduces #3825 for whatever type this is; the least this
// path can do is log so the gap is observable instead of invisible.
func TestWithoutSyntheticV2CostModel_UnknownTypeLogsAndReturnsUnfiltered(
	t *testing.T,
) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	pp := &unknownProtocolParameters{}

	got := withoutSyntheticV2CostModel(pp, true, logger)

	assert.Same(t, pp, got,
		"an unrecognized type must still be returned, unfiltered")
	assert.Contains(
		t,
		buf.String(),
		"does not recognize this protocol-parameters type",
	)
}

// TestExtractRawCostModels_CoversDijkstra covers blinklabs-io/dingo#3825's PR
// review (wolf31o2): extractRawCostModels' type switch lacked a Dijkstra
// case (falling to its own default: return nil), asymmetric with
// withoutSyntheticV2CostModel, which does handle Dijkstra -- meaning
// injectedSyntheticV2CostModel (built on extractRawCostModels) could never
// detect a Dijkstra-era injection even though the filter it feeds covers
// that era.
func TestExtractRawCostModels_CoversDijkstra(t *testing.T) {
	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			CostModels: map[uint][]int64{0: {1}, 1: {2}},
		},
	}

	got := extractRawCostModels(pp)

	assert.Equal(t, map[uint][]int64{0: {1}, 1: {2}}, got)
}

// TestExtractRawCostModels_NilPointerDoesNotPanic covers blinklabs-io/dingo#3825's
// PR review (Cubic): a concrete-typed nil pointer (lcommon.ProtocolParameters
// holding e.g. a nil *dijkstra.DijkstraProtocolParameters) still matches its
// type's case in the switch, so every case must guard against nil before
// dereferencing rather than panicking -- mirroring the guard
// withoutSyntheticV2CostModel already has for the identical hazard.
func TestExtractRawCostModels_NilPointerDoesNotPanic(t *testing.T) {
	cases := []struct {
		name string
		pp   lcommon.ProtocolParameters
	}{
		{"Alonzo", (*alonzo.AlonzoProtocolParameters)(nil)},
		{"Babbage", (*babbage.BabbageProtocolParameters)(nil)},
		{"Conway", (*conway.ConwayProtocolParameters)(nil)},
		{"Dijkstra", (*dijkstra.DijkstraProtocolParameters)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got := extractRawCostModels(tc.pp)
				assert.Nil(t, got)
			})
		})
	}
}

// TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel is the
// end-to-end regression test for blinklabs-io/dingo#3825: confirmed against
// a real cardano-node's raw wire bytes (captured via a temporary diagnostic,
// decoded with the real client-side type, independent of any display-layer
// bug) that on a chain which has never received a real PlutusV2
// cost-model update, a real cardano-node's GetCurrentProtocolParams reply
// has no PlutusV2 entry at all -- while Dingo's internal state always
// carries HardForkBabbage's fabricated one, needed for real script
// validation. The LocalStateQuery reply must match the real node's
// observable behavior; internal validation must not be affected.
func TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel(
	t *testing.T,
) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery())
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.NotContains(t, pp.CostModels, uint(1),
		"the reply must omit the synthetic PlutusV2 cost model")
	assert.Contains(t, pp.CostModels, uint(0))
	assert.Contains(t, pp.CostModels, uint(2))

	// This is a wire-level regression test, not just a type-assertion check:
	// encode what the reply actually contains and decode it back with the
	// real client-side type, matching the raw-CBOR verification this issue's
	// original diagnosis relied on independent of any display-layer bug.
	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	var decoded conway.ConwayProtocolParameters
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.NotContains(
		t,
		decoded.CostModels,
		uint(1),
		"the encoded wire bytes must not carry the synthetic PlutusV2 cost model",
	)

	// Internal validation state must be completely unaffected by the query.
	internal, ok := ls.currentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"internal state must keep the default for real script validation")
}

// TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel covers the
// other half: once real governance data has cleared the synthetic marker
// (LedgerState.syntheticV2CostModel == false), the reply must include
// whatever is actually in CostModels -- including a value that happens to
// equal the known synthetic default, since real governance re-affirming
// that exact value is still real data, not still a guess.
func TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel(
	t *testing.T,
) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = conwayPParamsWithCostModels(map[uint][]int64{
		0: {1, 1, 1},
		1: eras.DefaultPlutusV2CostModel,
		2: {3, 3, 3},
	})
	ls.syntheticV2CostModel = false
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery())
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.Contains(t, pp.CostModels, uint(1))
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1])

	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	var decoded conway.ConwayProtocolParameters
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.Equal(
		t,
		eras.DefaultPlutusV2CostModel,
		decoded.CostModels[1],
		"real data equal to the known default must still round-trip on the wire",
	)
}

// TestSyntheticV2CostModelPersistence_RoundTripsAcrossRestart covers
// blinklabs-io/dingo#3825's PR review: LedgerState.syntheticV2CostModel must
// survive a restart via persistSyntheticV2CostModel/loadSyntheticV2CostModel,
// not silently reconstruct as false (the zero value) regardless of the
// chain's real history.
func TestSyntheticV2CostModelPersistence_RoundTripsAcrossRestart(t *testing.T) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))

	// Not yet persisted: a fresh database reads back false, same as an
	// explicit false would.
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel)

	require.NoError(t, ls.persistSyntheticV2CostModel(true, nil))
	// Simulate a restart: a fresh in-memory value, restored from the same
	// database.
	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.True(t, ls.syntheticV2CostModel,
		"restored value must survive the simulated restart")

	require.NoError(t, ls.persistSyntheticV2CostModel(false, nil))
	ls.syntheticV2CostModel = true
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel,
		"a later persisted false must also survive the simulated restart")
}

// TestResolveSyntheticV2CostModel_BootstrapsFromValueWhenMarkerAbsent covers
// blinklabs-io/dingo#3825's PR review (wolf31o2): a database that predates
// this marker (or one that was reset by
// database.RecomputeSyntheticV2CostModelMarkerAfterTruncate) must not
// silently behave as "not synthetic" -- it must compare the current PlutusV2
// cost model directly against the known synthetic default instead.
func TestResolveSyntheticV2CostModel_BootstrapsFromValueWhenMarkerAbsent(
	t *testing.T,
) {
	stillSynthetic := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	}
	assert.True(t, resolveSyntheticV2CostModel("", stillSynthetic),
		"an absent marker with the exact synthetic default present must"+
			" resolve to still-synthetic")

	realData := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: {9, 9, 9}},
	}
	assert.False(t, resolveSyntheticV2CostModel("", realData),
		"an absent marker with a value that differs from the synthetic"+
			" default must resolve to real, not synthetic")

	noV2 := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	assert.False(t, resolveSyntheticV2CostModel("", noV2),
		"an absent marker with no PlutusV2 key at all must resolve to"+
			" not-synthetic")

	assert.False(t, resolveSyntheticV2CostModel("", nil),
		"an absent marker with nil pparams must resolve to not-synthetic")
}

// TestResolveSyntheticV2CostModel_ExplicitValueWins covers the common case:
// an explicitly persisted marker value is trusted directly, regardless of
// what pp happens to contain.
func TestResolveSyntheticV2CostModel_ExplicitValueWins(t *testing.T) {
	realData := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{1: eras.DefaultPlutusV2CostModel},
	}
	assert.True(t, resolveSyntheticV2CostModel("true", realData))
	assert.False(t, resolveSyntheticV2CostModel("false", realData))
}

// TestMarkRealV2CostModelObserved_KeepsEarliestConfirmationAcrossMultipleUpdates
// covers blinklabs-io/dingo#3825's PR review (Cubic): a chain that enacts
// more than one real PlutusV2 cost-model update over its life must not have
// its cleared-epoch marker overwritten by the later update -- doing so would
// make RecomputeSyntheticV2CostModelMarkerAfterTruncate incorrectly reset
// the marker to synthetic on a rollback that crosses back past only the
// LATEST update but not an EARLIER one, even though the earlier real value
// still survives on the truncated chain.
func TestMarkRealV2CostModelObserved_KeepsEarliestConfirmationAcrossMultipleUpdates(
	t *testing.T,
) {
	ls, db := newExpiryRollbackTestLedger(t, false, 0)

	// First real update confirmed at epoch 5 (slot 500).
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.markRealV2CostModelObserved(5, txn)
	}))

	// A second real update (e.g. a later governance-enacted cost-model
	// change) confirmed at epoch 10 (slot 1000) must not overwrite the
	// epoch-5 confirmation.
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.markRealV2CostModelObserved(10, txn)
	}))

	clearedEpoch, cleared, err := database.SyntheticV2CostModelClearedEpoch(
		db, nil,
	)
	require.NoError(t, err)
	require.True(t, cleared)
	require.Equal(
		t,
		uint64(5),
		clearedEpoch,
		"the earliest confirmation must be kept, not overwritten by the later one",
	)

	// Roll back to slot 700 (epoch 7): after the first real update, before
	// the second. The surviving chain's PlutusV2 cost model is still real
	// (from the first update), so the marker must NOT be reset to synthetic.
	require.NoError(
		t,
		database.RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 700),
	)

	value, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "false", value,
		"the surviving chain still has real data from the first update"+
			" and must not be reported as synthetic")
}

// TestTransitionToEraFrom_PersistsSyntheticMarkerInSameTransactionAsPParams
// covers blinklabs-io/dingo#3825's PR review (CodeRabbit round): the
// synthetic-cost-model marker must be written in the SAME database
// transaction as the pparams update it describes, not committed
// separately afterward. If they were in different transactions, a crash
// between the two commits could leave a stale marker on restart. This is
// proven here by rolling the transaction back entirely: since both writes
// share one transaction, rollback must undo both together, and a fresh
// read must see neither.
func TestTransitionToEraFrom_PersistsSyntheticMarkerInSameTransactionAsPParams(
	t *testing.T,
) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}

	txn := db.Transaction(true)
	result, err := ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		1,
		100,
		prev,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel)
	require.NoError(t, txn.Rollback())

	// Nothing must be durable: neither the pparams write nor the marker,
	// since they shared one now-rolled-back transaction.
	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.False(t, ls.syntheticV2CostModel,
		"a rolled-back transaction must not leave the marker persisted")
	rolledBackPParams, err := db.GetPParams(
		1, eras.BabbageEraDesc.Id, eras.DecodePParamsBabbage, nil,
	)
	require.NoError(t, err)
	assert.Nil(t, rolledBackPParams,
		"a rolled-back transaction must not leave the pparams write persisted")

	// The same sequence, committed instead, must persist both together.
	txn = db.Transaction(true)
	result, err = ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		1,
		100,
		prev,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel)
	require.NoError(t, txn.Commit())

	ls.syntheticV2CostModel = false
	ls.loadSyntheticV2CostModel()
	assert.True(t, ls.syntheticV2CostModel,
		"a committed transaction must persist the marker")
	committedPParams, err := db.GetPParams(
		1, eras.BabbageEraDesc.Id, eras.DecodePParamsBabbage, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, committedPParams,
		"a committed transaction must persist the pparams write")
}
