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

package eras

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Slots used by the tests below: the transaction is applied inside the era
// forecast horizon but its TTL falls past it, which is the shape of a real
// preview transaction (block slot 699109, TTL 785381, horizon 777600) that
// wedged `dingo load`.
const (
	testAppliedSlot     = 699_109
	testHorizonSlot     = 777_600
	testPastHorizonSlot = 785_381
)

// pastHorizonLedgerState resolves slots to times only inside the era forecast
// horizon, mirroring ledger.LedgerState.SlotToTime once the current era is
// bounded by its safe zone.
type pastHorizonLedgerState struct {
	*mockLedgerState
	horizonSlot     uint64
	slotToTimeCalls int
}

func newPastHorizonLedgerState() *pastHorizonLedgerState {
	return &pastHorizonLedgerState{
		mockLedgerState: newMockLedgerState(),
		horizonSlot:     testHorizonSlot,
	}
}

func (s *pastHorizonLedgerState) SlotToTime(
	slot uint64,
) (time.Time, error) {
	s.slotToTimeCalls++
	if slot >= s.horizonSlot {
		return time.Time{}, hardfork.ErrPastHorizon
	}
	// #nosec G115 -- test slots are small
	return time.Unix(int64(slot), 0), nil
}

// withoutBabbageUtxoValidationRules drops the gouroboros phase-1 rule set so a
// test can exercise the dingo-side script handling in isolation, mirroring
// withoutConwayUtxoValidationRules.
func withoutBabbageUtxoValidationRules(t *testing.T) {
	t.Helper()

	orig := babbageUtxoValidationRules
	babbageUtxoValidationRules = nil
	t.Cleanup(func() {
		babbageUtxoValidationRules = orig
	})
}

func withoutAlonzoUtxoValidationRules(t *testing.T) {
	t.Helper()

	orig := alonzoUtxoValidationRules
	alonzoUtxoValidationRules = nil
	t.Cleanup(func() {
		alonzoUtxoValidationRules = orig
	})
}

// newTestTxCbor builds transaction CBOR with a single input, a fee, and a TTL.
func newTestTxCbor(
	t *testing.T,
	ttl uint64,
	witnessSet map[uint]any,
) []byte {
	t.Helper()

	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	bodyMap := map[uint]any{
		0: []any{
			[]any{inputHash, uint64(0)},
		},
		2: uint64(200_000),
		3: ttl,
	}
	txCbor, err := cbor.Encode(
		[]any{bodyMap, witnessSet, true, nil},
	)
	require.NoError(t, err)
	return txCbor
}

// redeemerWitnessSet is a witness set carrying one spend redeemer, which is
// what makes a transaction require Plutus evaluation.
func redeemerWitnessSet() map[uint]any {
	return map[uint]any{
		5: []any{
			[]any{
				uint64(0), // tag: spend
				uint64(0), // index
				uint64(42),
				[]any{uint64(1_000), uint64(2_000)},
			},
		},
	}
}

// A transaction with no redeemers runs no Plutus script, so no script context
// may be built for it: building one translates its TTL to wall-clock time,
// which fails past the era forecast horizon and rejects a canonical block.
func TestValidateTxBabbageSkipsScriptContextWithoutRedeemers(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)
	require.False(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Zero(
		t,
		ls.slotToTimeCalls,
		"no slot/time translation may happen for a redeemerless transaction",
	)
}

// The gate must not weaken the horizon for transactions that do run scripts:
// those still translate their validity interval, and a past-horizon TTL is a
// genuine translation failure (cardano-ledger's TimeTranslationPastHorizon).
func TestValidateTxBabbageKeepsHorizonForRedeemerTx(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, redeemerWitnessSet()),
	)
	require.NoError(t, err)
	require.True(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)
	assert.Positive(t, ls.slotToTimeCalls)
}

// A redeemerless transaction whose TTL is inside the horizon behaves the same
// either way, so the gate cannot be hiding a translation that used to succeed.
func TestValidateTxBabbageWithoutRedeemersInsideHorizon(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot+100, map[uint]any{}),
	)
	require.NoError(t, err)

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	require.NoError(t, ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	))
}

func TestValidateTxAlonzoSkipsScriptContextWithoutRedeemers(t *testing.T) {
	withoutAlonzoUtxoValidationRules(t)

	tx, err := alonzo.NewAlonzoTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)
	require.False(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxAlonzo(
		tx,
		testAppliedSlot,
		ls,
		&alonzo.AlonzoProtocolParameters{MaxTxSize: 16_384},
	)
	require.NoError(t, err)
	assert.Zero(t, ls.slotToTimeCalls)
}

func TestEvaluateTxBabbageSkipsScriptContextWithoutRedeemers(t *testing.T) {
	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	_, exUnits, redeemerExUnits, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{}, exUnits)
	assert.Empty(t, redeemerExUnits)
	assert.Zero(t, ls.slotToTimeCalls)
}

// EvaluateTxConway (also used for Dijkstra) builds the V3 context up front, so
// it needs the same gate: estimating a redeemerless transaction's execution
// units must not depend on translating its TTL.
func TestEvaluateTxConwaySkipsScriptContextWithoutRedeemers(t *testing.T) {
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{inputHash, uint64(0)},
			},
		},
		2: uint64(200_000),
		3: uint64(testPastHorizonSlot),
	}
	txCbor, err := cbor.Encode(
		[]any{bodyMap, map[uint]any{}, true, nil},
	)
	require.NoError(t, err)
	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.False(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	_, exUnits, redeemerExUnits, err := EvaluateTxConway(
		tx,
		ls,
		&conway.ConwayProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{}, exUnits)
	assert.Empty(t, redeemerExUnits)
	assert.Zero(t, ls.slotToTimeCalls)
}

func TestTxHasRedeemers(t *testing.T) {
	withRedeemer, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot, redeemerWitnessSet()),
	)
	require.NoError(t, err)
	assert.True(t, txHasRedeemers(withRedeemer))

	withoutRedeemer, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot, map[uint]any{}),
	)
	require.NoError(t, err)
	assert.False(t, txHasRedeemers(withoutRedeemer))
}
