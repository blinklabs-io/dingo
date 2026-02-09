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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeAndApplyPParamUpdates_QuorumNotMet(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := New(config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 3 pparam updates from 3 different genesis keys
	// targeting epoch 4
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
			4,             // epoch
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
	result, err := db.ComputeAndApplyPParamUpdates(
		400, // slot
		4,   // epoch
		2,   // era
		5,   // quorum - 5 required, only 3 present
		currentPParams,
		decodeFunc,
		updateFunc,
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
	db, err := New(config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 5 pparam updates from 5 different genesis keys
	// targeting epoch 4
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
			4, // epoch
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
	_, err = db.ComputeAndApplyPParamUpdates(
		400,
		4,
		2,
		5, // quorum met
		currentPParams,
		decodeFunc,
		updateFunc,
		txn,
	)
	require.NoError(t, err)
	assert.True(
		t,
		updateApplied,
		"update should be applied when quorum is met",
	)
}

func TestComputeAndApplyPParamUpdates_FiltersEpoch(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := New(config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 3 updates for epoch 3 and 5 updates for epoch 4.
	// When querying for epoch 4, GetPParamUpdates returns
	// both (epoch=4 OR epoch=3). The quorum check should only
	// count epoch 4 records.
	for i := 0; i < 3; i++ {
		err := db.SetPParamUpdate(
			[]byte{byte(i)},
			[]byte{0x80}, // minimal CBOR
			uint64(200+i),
			3, // epoch 3
			txn,
		)
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
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
			4, // epoch 4
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

	// Quorum = 5: epoch 4 has 5 proposals (meets quorum),
	// epoch 3 has 3 (below quorum). Only epoch 4 should count.
	_, err = db.ComputeAndApplyPParamUpdates(
		400,
		4,
		2,
		5, // quorum
		currentPParams,
		decodeFunc,
		updateFunc,
		txn,
	)
	require.NoError(t, err)
	assert.True(
		t,
		updateApplied,
		"update should be applied: epoch 4 has 5 proposals meeting quorum",
	)
}

func TestComputeAndApplyPParamUpdates_NoUpdates(
	t *testing.T,
) {
	config := &Config{DataDir: ""}
	db, err := New(config)
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

	result, err := db.ComputeAndApplyPParamUpdates(
		400, 4, 2, 5,
		currentPParams,
		decodeFunc, updateFunc,
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
	db, err := New(config)
	require.NoError(t, err)
	defer db.Close()

	txn := db.Transaction(true)
	defer txn.Commit() //nolint:errcheck

	// Store 5 updates but from only 2 unique genesis keys
	// (duplicates should not count toward quorum)
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
			4,
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
	result, err := db.ComputeAndApplyPParamUpdates(
		400, 4, 2, 5,
		currentPParams,
		decodeFunc, updateFunc,
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
