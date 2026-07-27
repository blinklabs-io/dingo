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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	txn := db.Transaction(true)

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

// TestGetPParams_MaryToAlonzo_PicksMaryRow is the same scenario at the
// Mary→Alonzo boundary — where the field count actually diverges
// enough that an unfiltered query manifested as a hard decode error
// rather than silent zeroing. Without filtering by era, GetPParams
// would return the Alonzo-shape row (27 fields) and the Mary decoder
// fail on element count.
func TestGetPParams_MaryToAlonzo_PicksMaryRow(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	txn := db.Transaction(true)

	// Mary epoch-rollover write at boundary epoch 2.
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
	const boundaryEpoch uint64 = 2
	const boundarySlot uint64 = 150
	require.NoError(t, db.SetPParams(
		maryCbor, boundarySlot, boundaryEpoch,
		ledger.EraIdMary, txn,
	))

	// Alonzo transitionToEra write at the same epoch, 27-field shape.
	alonzoCbor := encodeAlonzoLikeFixedFieldCount(t, 27)
	require.NoError(t, db.SetPParams(
		alonzoCbor, boundarySlot+1, boundaryEpoch,
		ledger.EraIdAlonzo, txn,
	))
	require.NoError(t, txn.Commit())

	// Mary decoder over the row claimed to belong to Mary. With the
	// bug the most-recent insert at this epoch is the 27-element Alonzo
	// row — the 18-field Mary struct decode fails on element count.
	maryDecode := func(data []byte) (lcommon.ProtocolParameters, error) {
		var pp mary.MaryProtocolParameters
		if _, err := cbor.Decode(data, &pp); err != nil {
			return nil, err
		}
		return &pp, nil
	}
	got, err := db.GetPParams(
		boundaryEpoch, ledger.EraIdMary, maryDecode, nil,
	)
	require.NoErrorf(
		t, err,
		"Mary walkback must NOT receive Alonzo CBOR — that's the exact "+
			"decode failure the eras-DevNet log shows. err=%v", err,
	)
	maryGot, ok := got.(*mary.MaryProtocolParameters)
	require.Truef(
		t, ok,
		"expected *mary.MaryProtocolParameters, got %T", got,
	)
	require.Equal(t, uint(4), maryGot.ProtocolMajor)
	require.Equal(t, uint64(340000000), maryGot.MinPoolCost)
}

// encodeAlonzoLikeFixedFieldCount produces a CBOR array of `n` integers
// — the test only needs a CBOR blob whose top-level array has the same
// length as Alonzo's pparams struct so the Mary-shape decoder rejects
// it on element count. Building a real alonzo.AlonzoProtocolParameters
// would drag in costmodel/exunit fixtures the test doesn't care about.
func encodeAlonzoLikeFixedFieldCount(t *testing.T, n int) []byte {
	t.Helper()
	arr := make([]int, n)
	for i := range arr {
		arr[i] = i
	}
	out, err := cbor.Encode(arr)
	require.NoError(t, err)
	return out
}
