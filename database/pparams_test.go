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

package database

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeAndApplyPParamUpdates_QuorumNotMet(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 3 pparam updates from 3 different genesis keys submitted in
	// epoch 3 (so they would be enacted for epoch 4).
	genesisKeys := [][]byte{
		{0x01, 0x02, 0x03},
		{0x04, 0x05, 0x06},
		{0x07, 0x08, 0x09},
	}
	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(
		&shelley.ShelleyProtocolParameterUpdate{
			MinFeeA: &minFeeA,
		},
	)
	require.NoError(t, err)

	for i, gk := range genesisKeys {
		err := db.SetPParamUpdate(
			gk,
			updateCbor,
			uint64(300+i), // slot
			3,             // submission epoch (enacted for epoch 4)
			txn,
		)
		require.NoError(t, err)
	}

	// Set current pparams so we have something to start with
	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
	}
	currentPParamsCbor, err := cbor.Encode(currentPParams)
	require.NoError(t, err)
	err = db.SetPParams(currentPParamsCbor, 0, 3, 2, txn)
	require.NoError(t, err)

	// Decode and update functions
	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		// For test: just return current unchanged to
		// verify the update is skipped
		return current, nil
	}

	// Try to apply with quorum = 5 (only 3 proposals, below
	// quorum)
	result, _, err := db.ComputeAndApplyPParamUpdates(
		400, // slot
		4,   // epoch
		2,   // era
		5,   // quorum - 5 required, only 3 present
		currentPParams,
		decodeFunc,
		updateFunc,
		nil,
		txn,
	)
	require.NoError(t, err)
	// Should return current params unchanged since quorum not met
	assert.Equal(
		t,
		currentPParams,
		result,
		"params should be unchanged when quorum not met",
	)
}

func TestComputeAndApplyPParamUpdates_QuorumMet(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 5 pparam updates from 5 different genesis keys submitted in
	// epoch 3 (so they are enacted for epoch 4).
	genesisKeys := [][]byte{
		{0x01}, {0x02}, {0x03}, {0x04}, {0x05},
	}
	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(
		&shelley.ShelleyProtocolParameterUpdate{
			MinFeeA: &minFeeA,
		},
	)
	require.NoError(t, err)

	for i, gk := range genesisKeys {
		err := db.SetPParamUpdate(
			gk,
			updateCbor,
			uint64(300+i),
			3, // submission epoch (enacted for epoch 4)
			txn,
		)
		require.NoError(t, err)
	}

	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
	}
	currentPParamsCbor, err := cbor.Encode(currentPParams)
	require.NoError(t, err)
	err = db.SetPParams(currentPParamsCbor, 0, 3, 2, txn)
	require.NoError(t, err)

	updateApplied := false
	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		updateApplied = true
		return current, nil
	}

	// Apply with quorum = 5 (exactly 5 proposals, meets quorum)
	_, _, err = db.ComputeAndApplyPParamUpdates(
		400,
		4,
		2,
		5, // quorum met
		currentPParams,
		decodeFunc,
		updateFunc,
		nil,
		txn,
	)
	require.NoError(t, err)
	assert.True(
		t,
		updateApplied,
		"update should be applied when quorum is met",
	)

	stored, err := db.GetPParams(
		4,
		2, // matches the era passed to SetPParams above
		func(data []byte) (lcommon.ProtocolParameters, error) {
			var params shelley.ShelleyProtocolParameters
			_, err := cbor.Decode(data, &params)
			if err != nil {
				return nil, err
			}
			return &params, nil
		},
		txn,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
}

// TestComputeAndApplyPParamUpdates_ReportsPlutusV2CostModelWritten covers
// blinklabs-io/dingo#3825's PR review (wolf31o2): on a network that forks
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
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	updateCbor, err := cbor.Encode(
		&alonzo.AlonzoProtocolParameterUpdate{
			CostModels: map[uint][]int64{1: {205665, 812, 1}},
		},
	)
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01}, updateCbor, 300, 3, txn,
	))

	currentPParams := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
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
		400, 4, 2, 1,
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
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(
		&alonzo.AlonzoProtocolParameterUpdate{
			MinFeeA: &minFeeA,
		},
	)
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
		400, 4, 2, 1,
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
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(
		&shelley.ShelleyProtocolParameterUpdate{
			MinFeeA: &minFeeA,
		},
	)
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, db.SetPParamUpdate(
			[]byte{byte(i)}, updateCbor, uint64(300+i), 3, nil,
		))
	}

	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
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
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	minFeeA := uint(100)
	updateCbor, err := cbor.Encode(
		&shelley.ShelleyProtocolParameterUpdate{
			MinFeeA: &minFeeA,
		},
	)
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, db.SetPParamUpdate(
			[]byte{byte(i)}, updateCbor, uint64(300+i), 3, nil,
		))
	}

	currentPParams := lcommon.ProtocolParameters(
		&shelley.ShelleyProtocolParameters{
			MinFeeA: 44,
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

func TestComputeAndApplyPParamUpdates_FiltersEpoch(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Enacting for epoch 4 uses proposals submitted in epoch 3. Store 5
	// such proposals (meet quorum) plus 3 decoy proposals submitted in
	// epoch 2 (enacted for epoch 3, a different boundary). Querying for the
	// submission epoch 3 surfaces the epoch-2 decoys via the OR epoch-1
	// clause, so the filter must exclude them; only the 5 epoch-3 proposals
	// should count toward quorum.
	for i := range 3 {
		err := db.SetPParamUpdate(
			[]byte{byte(i)},
			[]byte{0x80}, // minimal CBOR
			uint64(200+i),
			2, // submission epoch 2 (decoy; enacted for epoch 3)
			txn,
		)
		require.NoError(t, err)
	}
	for i := range 5 {
		innerMinFeeA := uint(100)
		updateCbor, innerErr := cbor.Encode(
			&shelley.ShelleyProtocolParameterUpdate{
				MinFeeA: &innerMinFeeA,
			},
		)
		require.NoError(t, innerErr)
		err := db.SetPParamUpdate(
			[]byte{byte(10 + i)},
			updateCbor,
			uint64(300+i),
			3, // submission epoch 3 (enacted for epoch 4)
			txn,
		)
		require.NoError(t, err)
	}

	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
	}
	currentPParamsCbor, err := cbor.Encode(currentPParams)
	require.NoError(t, err)
	err = db.SetPParams(currentPParamsCbor, 0, 3, 2, txn)
	require.NoError(t, err)

	updateApplied := false
	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		updateApplied = true
		return current, nil
	}

	// Quorum = 5: submission epoch 3 has 5 proposals (meets quorum) and is
	// what enacts for target epoch 4; the epoch-2 decoys are excluded.
	_, _, err = db.ComputeAndApplyPParamUpdates(
		400,
		4,
		2,
		5, // quorum
		currentPParams,
		decodeFunc,
		updateFunc,
		nil,
		txn,
	)
	require.NoError(t, err)
	assert.True(
		t,
		updateApplied,
		"update should be applied: submission epoch 3 has 5 proposals meeting quorum",
	)
}

func TestComputeAndApplyPParamUpdates_NoUpdates(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
	}

	decodeFunc := func(data []byte) (any, error) {
		return nil, nil
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		t.Fatal("update function should not be called")
		return current, nil
	}

	result, _, err := db.ComputeAndApplyPParamUpdates(
		400, 4, 2, 5,
		currentPParams,
		decodeFunc, updateFunc,
		nil,
		txn,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		currentPParams,
		result,
		"should return current params when no updates exist",
	)
}

func TestComputeAndApplyPParamUpdates_DuplicateGenesis(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 5 updates but from only 2 unique genesis keys
	// (duplicates should not count toward quorum), submitted in epoch 3
	// (enacted for epoch 4).
	genesisKeys := [][]byte{
		{0x01}, {0x02}, {0x01}, {0x02}, {0x01},
	}
	for i, gk := range genesisKeys {
		innerMinFeeA := uint(100)
		updateCbor, innerErr := cbor.Encode(
			&shelley.ShelleyProtocolParameterUpdate{
				MinFeeA: &innerMinFeeA,
			},
		)
		require.NoError(t, innerErr)
		err := db.SetPParamUpdate(
			gk,
			updateCbor,
			uint64(300+i),
			3, // submission epoch (enacted for epoch 4)
			txn,
		)
		require.NoError(t, err)
	}

	currentPParams := &shelley.ShelleyProtocolParameters{
		MinFeeA: 44,
	}
	currentPParamsCbor, err := cbor.Encode(currentPParams)
	require.NoError(t, err)
	err = db.SetPParams(currentPParamsCbor, 0, 3, 2, txn)
	require.NoError(t, err)

	decodeFunc := func(data []byte) (any, error) {
		var update shelley.ShelleyProtocolParameterUpdate
		_, err := cbor.Decode(data, &update)
		return update, err
	}
	updateFunc := func(
		current lcommon.ProtocolParameters,
		update any,
	) (lcommon.ProtocolParameters, error) {
		t.Fatal(
			"update function should not be called " +
				"with duplicate genesis keys",
		)
		return current, nil
	}

	// Only 2 unique genesis keys, quorum is 5
	result, _, err := db.ComputeAndApplyPParamUpdates(
		400, 4, 2, 5,
		currentPParams,
		decodeFunc, updateFunc,
		nil,
		txn,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		currentPParams,
		result,
		"should not apply: only 2 unique genesis keys, need 5",
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
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	newMinFeeA := uint(100)
	updateCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameterUpdate{
		MinFeeA: &newMinFeeA,
	})
	require.NoError(t, err)
	// Two unique genesis keys submitted in epoch 3 (enacted for epoch 4).
	for _, gk := range [][]byte{{0x01}, {0x02}} {
		require.NoError(
			t,
			db.SetPParamUpdate(gk, updateCbor, 300, 3, nil),
		)
	}

	currentPParams := &shelley.ShelleyProtocolParameters{MinFeeA: 44}
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
	config := &Config{DataDir: ""}
	db, err := newTestDatabase(t, config)
	require.NoError(t, err)
	defer db.Close()

	newMinFeeA := uint(100)
	updateCbor, err := cbor.Encode(&shelley.ShelleyProtocolParameterUpdate{
		MinFeeA: &newMinFeeA,
	})
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
