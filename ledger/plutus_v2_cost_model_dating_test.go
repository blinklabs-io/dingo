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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPParamsForEpoch_PlutusV2CostModelDatedToRealEnactment reproduces
// blinklabs-io/dingo#4127: on Preview, the Babbage era transition happens at
// epoch 3, but PlutusV2 is not enacted on-chain until epoch 9, still under
// protocol major 7. HardForkBabbage fabricates a PlutusV2 cost model default
// at the era transition so internal script validation always has one to work
// with (see LedgerState.syntheticV2CostModel, blinklabs-io/dingo#3825), but
// that fabricated value must not leak into the persisted, per-epoch pparams
// row that historical/reporting readers resolve -- including the unmodified
// internal/koiosparity comparison, which reads the pparams table directly
// and reported this exact divergence for epochs 3-8 and none other.
//
// Only the epoch-9 row, written from the real on-chain protocol-parameter
// update, may carry a PlutusV2 cost model.
func TestGetPParamsForEpoch_PlutusV2CostModelDatedToRealEnactment(t *testing.T) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	alonzoPParams := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}

	// Babbage era transition at epoch 3, mirroring Preview.
	txn := db.Transaction(true)
	result, err := ls.transitionToEraFrom(
		txn,
		eras.BabbageEraDesc.Id,
		3,      // startEpoch
		259200, // addedSlot (arbitrary, post-transition slot)
		alonzoPParams,
		eras.AlonzoEraDesc.Id,
	)
	require.NoError(t, err)
	require.True(t, result.InjectedSyntheticV2CostModel,
		"test setup: the era transition must be the one that fabricates"+
			" the PlutusV2 default, or this test proves nothing")
	require.NoError(t, txn.Commit())

	// Epoch 9's real on-chain protocol-parameter update introduces PlutusV2
	// for real, the classic Shelley-style path Preview actually used (still
	// PV7, no CIP-1694 governance yet).
	realV2 := []int64{9, 9, 9}
	enactedParams := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: realV2},
	}
	enactedCbor, err := cbor.Encode(enactedParams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		enactedCbor, 777600, 9, eras.BabbageEraDesc.Id, nil,
	))

	for epoch := uint64(3); epoch <= 8; epoch++ {
		pp, err := ls.GetPParamsForEpoch(epoch, eras.BabbageEraDesc)
		require.NoError(t, err)
		require.NotNil(t, pp)
		babbagePP, ok := pp.(*babbage.BabbageProtocolParameters)
		require.True(t, ok)
		_, hasV2 := babbagePP.CostModels[1]
		assert.Falsef(t, hasV2,
			"epoch %d must not carry a PlutusV2 cost model six epochs"+
				" before Preview enacted it",
			epoch,
		)
	}

	for _, epoch := range []uint64{9, 10} {
		pp, err := ls.GetPParamsForEpoch(epoch, eras.BabbageEraDesc)
		require.NoError(t, err)
		require.NotNil(t, pp)
		babbagePP, ok := pp.(*babbage.BabbageProtocolParameters)
		require.True(t, ok)
		v2, hasV2 := babbagePP.CostModels[1]
		require.Truef(t, hasV2,
			"epoch %d must carry the real, on-chain-enacted PlutusV2 cost model",
			epoch,
		)
		assert.Equal(t, realV2, v2)
	}
}

// TestWithDefaultV2CostModelIfMissing_AddsDefaultWhenSyntheticAndAbsent
// covers withDefaultV2CostModelIfMissing, the inverse of
// withoutSyntheticV2CostModel added for blinklabs-io/dingo#4127: restoring
// HardForkBabbage's fabricated PlutusV2 default into pparams reloaded from a
// persisted row that (correctly, per the dating fix) no longer carries it.
func TestWithDefaultV2CostModelIfMissing_AddsDefaultWhenSyntheticAndAbsent(
	t *testing.T,
) {
	pp := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	got := withDefaultV2CostModelIfMissing(pp, true, nil)
	babbagePP, ok := got.(*babbage.BabbageProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, eras.DefaultPlutusV2CostModel, babbagePP.CostModels[1])
	// The original must not be mutated.
	_, hasV2 := pp.CostModels[1]
	assert.False(t, hasV2)
}

func TestWithDefaultV2CostModelIfMissing_NoOpWhenNotSynthetic(t *testing.T) {
	pp := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	got := withDefaultV2CostModelIfMissing(pp, false, nil)
	assert.Same(t, lcommon.ProtocolParameters(pp), got)
}

func TestWithDefaultV2CostModelIfMissing_NoOpWhenAlreadyPresent(t *testing.T) {
	realV2 := []int64{9, 9, 9}
	pp := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{1: realV2},
	}
	got := withDefaultV2CostModelIfMissing(pp, true, nil)
	babbagePP, ok := got.(*babbage.BabbageProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, realV2, babbagePP.CostModels[1],
		"a real value already present must never be overwritten by the"+
			" fabricated default")
}

func TestWithDefaultV2CostModelIfMissing_NilPointerDoesNotPanic(t *testing.T) {
	var pp *babbage.BabbageProtocolParameters
	assert.NotPanics(t, func() {
		withDefaultV2CostModelIfMissing(pp, true, nil)
	})
}

func TestWithDefaultV2CostModelIfMissing_NilInterfaceDoesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		got := withDefaultV2CostModelIfMissing(nil, true, nil)
		assert.Nil(t, got)
	})
}

// TestLoadSyntheticV2CostModel_RestoresDefaultAcrossSimulatedRestart proves
// the restart-fidelity half of the blinklabs-io/dingo#4127 fix: a persisted
// pparams row inside the synthetic window no longer carries the fabricated
// PlutusV2 default (that is the fix), but ls.currentPParams reloaded from
// that row at startup must still carry it, exactly as a continuously
// running process would, so internal script validation does not become
// stricter purely because the process restarted.
func TestLoadSyntheticV2CostModel_RestoresDefaultAcrossSimulatedRestart(
	t *testing.T,
) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	// Simulate the persisted row transitionToEraFrom now writes: the
	// synthetic marker is true, but the pparams value itself has no
	// PlutusV2 cost model, matching what a historical/reporting reader
	// resolves for an epoch inside the window.
	require.NoError(t, ls.persistSyntheticV2CostModel(true, nil))
	ls.currentPParams = &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}

	ls.loadSyntheticV2CostModel()

	assert.True(t, ls.syntheticV2CostModel)
	conwayPP, ok := ls.currentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, eras.DefaultPlutusV2CostModel, conwayPP.CostModels[1],
		"internal validation must see the same fabricated default across"+
			" a restart that lands inside the synthetic window")

	// Sanity: database.SyntheticV2CostModelSyncKey (not this package's
	// concern) round-trips independently of pparams content.
	value, err := db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	assert.Equal(t, "true", value)
}
