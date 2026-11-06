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

package database

import (
	"context"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPParams_PicksRowMatchingRequestedEra pins the previous-era
// pparams lookup performed by ledger.LedgerState.computePParams at
// every era boundary. Before the fix, the eras-DevNet log showed it
// failing at every inter-era walkback (epoch 1 / Allegra, epoch 2 /
// Mary, epoch 3 / Alonzo) with "cbor: cannot unmarshal CBOR array into
// Go value of type [shelley/mary/alonzo].ProtocolParameters (cannot
// decode CBOR array to struct with different number of elements)".
//
// Reproduction:
//
//   - The epoch-rollover path writes pparams once via the
//     ComputeAndApplyPParamUpdates code path (era = old era) and a
//     second time via ledger.transitionToEra (era = new era), both at
//     the same `startEpoch` value, the OLD epoch's id. Without the
//     era filter, the metadata plugin's GetPParams returned "the most
//     recent row at epoch <= X ordered by epoch DESC, id DESC LIMIT
//     1" — so the row that won the read was the LATEST inserted row,
//     which is the new-era-shape one.
//   - When ledger walks epochCache backwards looking for the previous
//     era's pparams, it picks `prevEra.DecodePParamsFunc` based on the
//     cache entry's recorded EraId. Without the filter the read
//     returned CBOR for a *different* era's struct shape and the
//     decode failed.
//
// Field counts of the relevant pparams structs (per gouroboros v0.166.1):
//
//	shelley.ShelleyProtocolParameters:        17 fields
//	allegra.AllegraProtocolParameters: alias = shelley
//	mary.MaryProtocolParameters:              18 fields  (+ MinPoolCost)
//	alonzo.AlonzoProtocolParameters:          27 fields  (+ Plutus)
//
// The fix folds an era_id filter into GetPParams itself: the SQL
// `WHERE era_id = ?` makes the returned row match the era the caller
// has chosen its decoder for. The two scenarios below seed both rows
// at the same epoch and confirm the filter picks the right one.
func TestGetPParams_PicksRowMatchingRequestedEra(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	txn := db.Transaction(context.Background(), true)

	// Step 1: simulate the old era's epoch-rollover write. At the
	// boundary between Allegra (epoch 1) and Mary (epoch 2), the
	// rollover code writes pparams with the Allegra (old) era id. The
	// shape is Allegra's = Shelley's, 17 fields.
	allegraPP := &shelley.ShelleyProtocolParameters{
		MinFeeA:          44,
		MinFeeB:          155381,
		MaxBlockBodySize: 65536,
		MaxTxSize:        16384,
		ProtocolMajor:    3, // Allegra
	}
	allegraCbor, err := cbor.Encode(allegraPP)
	require.NoError(t, err)
	const boundaryEpoch uint64 = 1
	const boundarySlot uint64 = 75
	require.NoError(t, db.SetPParams(
		allegraCbor, boundarySlot, boundaryEpoch,
		ledger.EraIdAllegra, txn,
	))

	// Step 2: simulate ledger.transitionToEra writing the new era's
	// pparams at the SAME `startEpoch`. The caller in state.go passes
	// `snapshotEpoch.EpochId` (the old epoch id), then transitionToEra
	// stores the post-hard-fork new-era-shape pparams against that old
	// epoch number. For the Allegra→Mary transition the shape is Mary's
	// = 18 fields.
	maryPP := &mary.MaryProtocolParameters{
		MinFeeA:          44,
		MinFeeB:          155381,
		MaxBlockBodySize: 65536,
		MaxTxSize:        16384,
		ProtocolMajor:    4, // Mary
		MinPoolCost:      340000000,
	}
	maryCbor, err := cbor.Encode(maryPP)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		maryCbor, boundarySlot+1, boundaryEpoch,
		ledger.EraIdMary, txn,
	))
	require.NoError(t, txn.Commit())

	// Step 3: emulate ledger.computePParams's previous-era walkback.
	// It found epochCache[i] with EraId == EraIdAllegra at EpochId ==
	// boundaryEpoch and asks the database for that epoch's pparams,
	// passing the Allegra decoder.
	allegraDecode := func(data []byte) (lcommon.ProtocolParameters, error) {
		var pp shelley.ShelleyProtocolParameters // = Allegra alias
		if _, err := cbor.Decode(data, &pp); err != nil {
			return nil, err
		}
		return &pp, nil
	}
	got, err := db.GetPParams(
		boundaryEpoch, ledger.EraIdAllegra, allegraDecode, nil,
	)
	require.NoError(
		t, err,
		"GetPParams must return CBOR matching the era the caller "+
			"intends to decode for. Without the era filter the read "+
			"returned the latest insert at this epoch — here the "+
			"Mary-shape row stored by transitionToEra — and the "+
			"Allegra decoder failed on element count.",
	)
	allegraGot, ok := got.(*shelley.ShelleyProtocolParameters)
	require.Truef(
		t, ok,
		"expected *shelley.ShelleyProtocolParameters (Allegra), got %T",
		got,
	)
	require.Equal(
		t, uint(3), allegraGot.ProtocolMajor,
		"the row returned must be the Allegra-shape one (ProtocolMajor "+
			"= 3); receiving anything else means GetPParams returned a "+
			"different era's row and the caller silently decoded into "+
			"the wrong struct, leaving fields zero-valued",
	)
}

// TestComputeAndApplyPParamUpdates_ReportsPlutusV2CostModelWritten covers
// the case where, on a network that forks
// into Babbage before receiving a real PlutusV2 cost model, that model can
// arrive through this classic Shelley-style update system rather than
// CIP-1694 governance (as it did on real mainnet, well before Conway
// governance existed). The caller needs the same real-write provenance
// signal here that governance.EnactProposal provides for the Conway/
// Dijkstra path, derived from hasPlutusV2CostModelFunc against the enacted
// update itself -- not from comparing the merged result's value before and
// after.
func TestComputeAndApplyPParamUpdates_ReportsPlutusV2CostModelWritten(
	t *testing.T,
) {
	t.Parallel()

	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(context.Background(), true)
	defer txn.Commit() //nolint:errcheck

	updateCbor, err := cbor.Encode(map[uint64]any{
		18: map[uint][]int64{1: {205665, 812, 1}},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01}, updateCbor, 300, 3, txn,
	))

	currentPParams := &alonzo.AlonzoProtocolParameters{
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		CostModels:         map[uint][]int64{0: {1, 2, 3}},
		// From protocol version 9 a cost-model update is not checked for
		// its parameter count, so the short fixture model is enactable.
		ProtocolMajor: 9,
	}
	decodeFunc := func(data []byte) (any, error) {
		var update alonzo.AlonzoProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		return current, nil
	}
	hasPlutusV2CostModelFunc := func(u any) bool {
		upd, ok := u.(alonzo.AlonzoProtocolParameterUpdate)
		if !ok {
			return false
		}
		_, ok = upd.CostModels[1]
		return ok
	}

	_, plutusV2CostModelWritten, err := db.ComputeAndApplyPParamUpdates(
		context.Background(),
		400,
		4,
		2,
		1,
		currentPParams,
		decodeFunc,
		updateFunc,
		hasPlutusV2CostModelFunc,
		txn,
	)
	require.NoError(t, err)
	assert.True(t, plutusV2CostModelWritten,
		"the enacted update explicitly specified CostModels[1]")
}

// TestComputeAndApplyPParamUpdates_FalseWhenUpdateDoesNotWritePlutusV2CostModel
// covers the negative case: an enacted update that touches an unrelated
// field must not report PlutusV2CostModelWritten, even though the merged
// result may still carry a PlutusV2 cost model unchanged from before.
func TestComputeAndApplyPParamUpdates_FalseWhenUpdateDoesNotWritePlutusV2CostModel(
	t *testing.T,
) {
	t.Parallel()

	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(context.Background(), true)
	defer txn.Commit() //nolint:errcheck

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(map[uint64]any{0: minFeeA})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01}, updateCbor, 300, 3, txn,
	))

	currentPParams := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: {205665, 812, 1}},
	}
	decodeFunc := func(data []byte) (any, error) {
		var update alonzo.AlonzoProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		return current, nil
	}
	hasPlutusV2CostModelFunc := func(u any) bool {
		upd, ok := u.(alonzo.AlonzoProtocolParameterUpdate)
		if !ok {
			return false
		}
		_, ok = upd.CostModels[1]
		return ok
	}

	_, plutusV2CostModelWritten, err := db.ComputeAndApplyPParamUpdates(
		context.Background(),
		400,
		4,
		2,
		1,
		currentPParams,
		decodeFunc,
		updateFunc,
		hasPlutusV2CostModelFunc,
		txn,
	)
	require.NoError(t, err)
	assert.False(t, plutusV2CostModelWritten,
		"this update never touched CostModels[1], even though the merged"+
			" result still carries one unchanged")
}

func TestComputeAndApplyPParamUpdates_NilTxnCommitsWrite(
	t *testing.T,
) {
	t.Parallel()

	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(map[uint64]any{0: minFeeA})
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, db.SetPParamUpdate(
			[]byte{byte(i)}, updateCbor, uint64(300+i), 3, nil,
		))
	}

	currentPParams := &shelley.ShelleyProtocolParameters{
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		MinFeeA:            44,
	}
	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		tmp := *current.(*shelley.ShelleyProtocolParameters)
		tmp.MinFeeA = *update.(shelley.ShelleyProtocolParameterUpdate).MinFeeA
		return &tmp, nil
	}

	result, _, err := db.ComputeAndApplyPParamUpdates(
		context.Background(),
		400, 4, 2, 5,
		currentPParams,
		decodeFunc, updateFunc,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint(100),
		result.(*shelley.ShelleyProtocolParameters).MinFeeA,
	)

	stored, err := db.GetPParams(
		4,
		2,
		func(data []byte) (lcommon.ProtocolParameters, error) {
			var params shelley.ShelleyProtocolParameters
			_, err := cbor.Decode(data, &params)
			return &params, err
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(
		t,
		uint(100),
		stored.(*shelley.ShelleyProtocolParameters).MinFeeA,
	)
}

func TestApplyPParamUpdates_NilTxnCommitsWrite(t *testing.T) {
	t.Parallel()

	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(map[uint64]any{0: minFeeA})
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, db.SetPParamUpdate(
			[]byte{byte(i)}, updateCbor, uint64(300+i), 3, nil,
		))
	}

	currentPParams := lcommon.ProtocolParameters(
		&shelley.ShelleyProtocolParameters{
			// Block sizes the votedFuturePParams guard accepts.
			MaxBlockBodySize:   65536,
			MaxTxSize:          16384,
			MaxBlockHeaderSize: 1100,
			MinFeeA:            44,
		},
	)
	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		tmp := *current.(*shelley.ShelleyProtocolParameters)
		tmp.MinFeeA = *update.(shelley.ShelleyProtocolParameterUpdate).MinFeeA
		return &tmp, nil
	}

	require.NoError(t, db.ApplyPParamUpdates(
		context.Background(),
		400, 4, 2, 5,
		&currentPParams,
		decodeFunc, updateFunc,
		nil,
	))
	require.Equal(
		t, uint(100),
		currentPParams.(*shelley.ShelleyProtocolParameters).MinFeeA,
	)

	stored, err := db.GetPParams(
		4,
		2,
		func(data []byte) (lcommon.ProtocolParameters, error) {
			var params shelley.ShelleyProtocolParameters
			_, err := cbor.Decode(data, &params)
			return &params, err
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(
		t,
		uint(100),
		stored.(*shelley.ShelleyProtocolParameters).MinFeeA,
	)
}

// shelleyCloneFunc encodes+decodes a Shelley pparams set, mirroring the
// era clone the ledger passes to ForecastPParamUpdates so the update
// function never mutates the caller's original.
func shelleyCloneFunc(
	pp lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	data, err := cbor.Encode(pp)
	if err != nil {
		return nil, err
	}
	var ret shelley.ShelleyProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func shelleyForecastFuncs() (
	func([]byte) (any, error),
	func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
) {
	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		pp, ok := current.(*shelley.ShelleyProtocolParameters)
		if !ok {
			return nil, assert.AnError
		}
		u, ok := update.(shelley.ShelleyProtocolParameterUpdate)
		if !ok {
			return nil, assert.AnError
		}
		pp.Update(&u)
		return pp, nil
	}
	return decodeFunc, updateFunc
}

// TestForecastPParamUpdates_QuorumMetNoPersist verifies the pure forecast
// applies a quorum-meeting update WITHOUT persisting a pparams row and
// WITHOUT mutating the caller's currentPParams.
func TestForecastPParamUpdates_QuorumMetNoPersist(t *testing.T) {
	t.Parallel()

	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	newMinFeeA := uint(100)
	updateCbor, err := cbor.Encode(map[uint64]any{0: newMinFeeA})
	require.NoError(t, err)
	// Two unique genesis keys submitted in epoch 3 (enacted for epoch 4).
	for _, gk := range [][]byte{{0x01}, {0x02}} {
		require.NoError(
			t,
			db.SetPParamUpdate(gk, updateCbor, 300, 3, nil),
		)
	}

	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}
	decodeFunc, updateFunc := shelleyForecastFuncs()

	result, err := db.ForecastPParamUpdates(
		4, // target epoch
		2, // quorum met (2 unique)
		currentPParams,
		decodeFunc,
		updateFunc,
		shelleyCloneFunc,
		nil,
	)
	require.NoError(t, err)
	resPP, ok := result.(*shelley.ShelleyProtocolParameters)
	require.True(t, ok)
	assert.Equal(
		t,
		uint(100),
		resPP.MinFeeA,
		"forecast should reflect the enacted update",
	)
	// Caller's original must be untouched.
	assert.Equal(
		t,
		uint(44),
		currentPParams.MinFeeA,
		"forecast must not mutate the caller's currentPParams",
	)
	// No pparams row must have been persisted for the target epoch.
	stored, err := db.GetPParams(
		4,
		2,
		func(data []byte) (lcommon.ProtocolParameters, error) {
			var params shelley.ShelleyProtocolParameters
			if _, decErr := cbor.Decode(data, &params); decErr != nil {
				return nil, decErr
			}
			return &params, nil
		},
		nil,
	)
	require.NoError(t, err)
	assert.Nil(t, stored, "forecast must not persist a pparams row")
}

// TestForecastPParamUpdates_QuorumNotMet verifies the forecast returns the
// caller's params unchanged when quorum is not met.
func TestForecastPParamUpdates_QuorumNotMet(t *testing.T) {
	t.Parallel()

	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	newMinFeeA := uint(100)
	updateCbor, err := cbor.Encode(map[uint64]any{0: newMinFeeA})
	require.NoError(t, err)
	require.NoError(
		t,
		db.SetPParamUpdate([]byte{0x01}, updateCbor, 300, 3, nil),
	)

	currentPParams := &shelley.ShelleyProtocolParameters{MinFeeA: 44}
	decodeFunc, updateFunc := shelleyForecastFuncs()

	result, err := db.ForecastPParamUpdates(
		4,
		5, // quorum NOT met (only 1 unique)
		currentPParams,
		decodeFunc,
		updateFunc,
		shelleyCloneFunc,
		nil,
	)
	require.NoError(t, err)
	assert.Same(
		t,
		currentPParams,
		result,
		"should return the original pointer unchanged when quorum not met",
	)
}

// TestPParamEnactmentPendingShortCircuitsTheWriter pins the read-only probe
// that lets the txn == nil path avoid taking the single metadata writer.
// Deciding whether anything will be enacted is a pure read, so only a
// quorum-met proposal should report pending and go on to open a write
// transaction.
func TestPParamEnactmentPendingShortCircuitsTheWriter(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(map[uint64]any{0: minFeeA})
	require.NoError(t, err)

	// Epoch 0 has no submission epoch at all.
	pending, err := db.pparamEnactmentPending(context.Background(), 0, 1)
	require.NoError(t, err)
	require.False(t, pending)

	// Nothing recorded for the submission epoch.
	pending, err = db.pparamEnactmentPending(context.Background(), 4, 3)
	require.NoError(t, err)
	require.False(t, pending)

	txn := db.Transaction(context.Background(), true)
	for i, gk := range [][]byte{
		{0x01, 0x02, 0x03},
		{0x04, 0x05, 0x06},
	} {
		require.NoError(t, db.SetPParamUpdate(
			gk, updateCbor, uint64(300+i), 3, txn,
		))
	}
	require.NoError(t, txn.Commit())

	// Two unique proposals against a quorum of three: still nothing to enact,
	// so the writer must not be taken.
	pending, err = db.pparamEnactmentPending(context.Background(), 4, 3)
	require.NoError(t, err)
	require.False(t, pending)

	txn = db.Transaction(context.Background(), true)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x07, 0x08, 0x09}, updateCbor, 302, 3, txn,
	))
	require.NoError(t, txn.Commit())

	// Quorum met: an enactment will be written, so the writer is warranted.
	pending, err = db.pparamEnactmentPending(context.Background(), 4, 3)
	require.NoError(t, err)
	require.True(t, pending)
}
