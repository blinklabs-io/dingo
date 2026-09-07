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
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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

// seedBabbageSyntheticWindow puts a ledger inside HardForkBabbage's synthetic
// PlutusV2 window as blinklabs-io/dingo#4127 leaves it: the persisted pparams
// row for the era-transition epoch no longer carries the fabricated default,
// while the in-memory ls.currentPParams a continuously running process holds
// still does.
func seedBabbageSyntheticWindow(t *testing.T, ls *LedgerState) {
	t.Helper()
	const (
		epochId       = uint64(3)
		startSlot     = uint64(5)
		slotLength    = uint(1_000)
		lengthInSlots = uint(432_000)
	)
	require.NoError(t, ls.db.SetEpoch(
		startSlot,
		epochId,
		nil,
		nil,
		nil,
		nil,
		eras.BabbageEraDesc.Id,
		slotLength,
		lengthInSlots,
		nil,
	))
	stripped := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	strippedCbor, err := cbor.Encode(stripped)
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		strippedCbor, startSlot, epochId, eras.BabbageEraDesc.Id, nil,
	))

	ls.currentEpoch = models.Epoch{
		EpochId:       epochId,
		StartSlot:     startSlot,
		SlotLength:    slotLength,
		LengthInSlots: lengthInSlots,
		EraId:         eras.BabbageEraDesc.Id,
	}
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentEra = eras.BabbageEraDesc
	ls.currentPParams = &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 2, 3},
			1: eras.DefaultPlutusV2CostModel,
		},
	}
	ls.publishSnapshotsLocked()
}

// TestRollbackInsideSyntheticWindowKeepsFabricatedV2CostModel covers the
// rollback member of the reload class blinklabs-io/dingo#4127's write-side
// filter changes. rollbackChainAndStateDeferred reloads currentPParams from
// the persisted row exactly as startup does; before the filter that row still
// carried HardForkBabbage's fabricated PlutusV2 default, so the reload was
// harmless. Now it does not, so a rollback landing inside the synthetic window
// silently drops the cost model internal script validation needs -- the same
// defect loadSyntheticV2CostModel's restore exists to prevent, on the sibling
// path.
func TestRollbackInsideSyntheticWindowKeepsFabricatedV2CostModel(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	seedBabbageSyntheticWindow(t, ls)
	require.NoError(t, ls.db.SetSyncState(
		database.SyntheticV2CostModelSyncKey, "true", nil,
	))
	ls.syntheticV2CostModel = true

	require.NoError(
		t,
		ls.rollbackChainAndStateDeferred(fixture.ancestorTip.Point, nil),
	)

	assert.True(t, ls.syntheticV2CostModel)
	pp, ok := ls.currentPParams.(*babbage.BabbageProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1],
		"a rollback inside the synthetic window must leave internal script"+
			" validation the same fabricated default it had before")
}

// TestDeepRollbackPastRealV2EnactmentReopensSyntheticWindow covers the second
// consumer of the persisted row's PlutusV2 content that
// blinklabs-io/dingo#4127's write-side filter invalidates.
//
// RecomputeSyntheticV2CostModelMarkerAfterTruncate deliberately DELETES the
// boolean marker rather than forcing it to "true" when a rollback crosses back
// before the epoch real PlutusV2 data was confirmed, leaving
// resolveSyntheticV2CostModel's absent-marker fallback to re-derive the answer
// from the surviving pparams row. That fallback was written when such a row
// always carried the fabricated default. With the row filtered it carries no
// PlutusV2 cost model at all, which the fallback read as "not synthetic" --
// durably, since the marker is now absent and a later restart re-derives the
// same wrong answer.
func TestDeepRollbackPastRealV2EnactmentReopensSyntheticWindow(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	seedBabbageSyntheticWindow(t, ls)
	require.NoError(t, ls.db.SetSyncState(
		database.SyntheticV2CostModelSyncKey, "false", nil,
	))
	require.NoError(t, database.SetSyntheticV2CostModelClearedEpoch(
		ls.db, nil, 9,
	))
	ls.syntheticV2CostModel = false

	require.NoError(
		t,
		ls.rollbackChainAndStateDeferred(fixture.ancestorTip.Point, nil),
	)

	value, err := ls.db.GetSyncState(
		database.SyntheticV2CostModelSyncKey, nil,
	)
	require.NoError(t, err)
	require.Empty(t, value,
		"test setup: this rollback must be the one that deletes the marker,"+
			" or the absent-marker fallback is never exercised")
	assert.True(t, ls.syntheticV2CostModel,
		"a rollback back inside the synthetic window must re-derive as"+
			" synthetic, not as real data")
	pp, ok := ls.currentPParams.(*babbage.BabbageProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1],
		"re-deriving the open window must also restore the fabricated"+
			" default internal script validation needs")
}

// newBabbageBoundaryLedger builds the minimum LedgerState
// applyBoundaryEraTransitions needs to run an Alonzo -> Babbage hop.
func newBabbageBoundaryLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	const shelleyGenesisJSON = `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 86400,
		"slotLength": 1,
		"protocolParams": {"protocolVersion": {"major": 2, "minor": 0}},
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("42", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))
	db := newTestDB(t)
	ls := &LedgerState{
		db:         db,
		currentEra: eras.AlonzoEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	return ls, db
}

// TestApplyBoundaryEraTransitions_DoesNotDateV2CostModelToEraTransition covers
// the second pparams write an era boundary performs: after the per-hop write
// transitionToEraFrom does, applyBoundaryEraTransitions persists the combined
// result of every hop under the NEW epoch's id.
//
// That second row is the one a historical reader actually resolves for the
// transition epoch, because GetPParams selects the newest row at or before the
// requested epoch within the era -- on Preview, the `epoch 3 / era 5` row from
// blinklabs-io/dingo#4127's report, not the `epoch 2 / era 5` row the hop
// wrote. Since splitEraTransitionsForRollover puts every transition after the
// rollover, this is the path every real era boundary takes.
func TestApplyBoundaryEraTransitions_DoesNotDateV2CostModelToEraTransition(
	t *testing.T,
) {
	ls, db := newBabbageBoundaryLedger(t)

	snapshotEpoch := models.Epoch{
		EpochId:       2,
		StartSlot:     172_800,
		SlotLength:    1_000,
		LengthInSlots: 86_400,
		EraId:         eras.AlonzoEraDesc.Id,
	}
	newEpoch := models.Epoch{
		EpochId:       3,
		StartSlot:     259_200,
		SlotLength:    1_000,
		LengthInSlots: 86_400,
		EraId:         eras.AlonzoEraDesc.Id,
		// Pre-seeded so the post-Byron nonce reseed branch is not the
		// subject of this test.
		Nonce: bytes.Repeat([]byte{0x11}, 32),
	}
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch: newEpoch,
		NewCurrentEra:   eras.AlonzoEraDesc,
		NewCurrentPParams: &alonzo.AlonzoProtocolParameters{
			CostModels: map[uint][]int64{0: {1, 2, 3}},
		},
	}

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		results, err := ls.applyBoundaryEraTransitions(
			txn,
			snapshotEpoch,
			[]uint{eras.BabbageEraDesc.Id},
			rolloverResult,
		)
		if err != nil {
			return err
		}
		require.Len(t, results, 1)
		require.True(t, results[0].InjectedSyntheticV2CostModel,
			"test setup: this hop must be the one that fabricates the"+
				" PlutusV2 default, or this test proves nothing")
		return nil
	}))

	// Both rows the boundary wrote -- the hop's own (epoch 2) and the
	// combined one (epoch 3) -- must be free of the fabricated default.
	for _, epoch := range []uint64{2, 3} {
		pp, err := ls.GetPParamsForEpoch(epoch, eras.BabbageEraDesc)
		require.NoError(t, err)
		babbagePP, ok := pp.(*babbage.BabbageProtocolParameters)
		require.True(t, ok)
		_, hasV2 := babbagePP.CostModels[1]
		assert.Falsef(t, hasV2,
			"the persisted row a historical reader resolves for epoch %d"+
				" must not carry the fabricated PlutusV2 default",
			epoch,
		)
	}

	// The in-memory value internal script validation reads keeps it.
	inMemory, ok := rolloverResult.NewCurrentPParams.(*babbage.BabbageProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, eras.DefaultPlutusV2CostModel, inMemory.CostModels[1],
		"filtering the persisted row must not disturb the parameters"+
			" internal script validation reads")
}
