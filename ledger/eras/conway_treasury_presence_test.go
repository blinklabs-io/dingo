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

package eras

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConwayFeaturesRuleDistinguishesAbsentFromDeclaredZeroTreasury drives the
// production Conway rule -- resolved out of the composed conwayUtxoValidationRules
// slice, not called bare -- with real decoded Conway transactions.
//
// A declared current treasury value of zero is an assertion about the treasury
// and is a Conway-only feature; an absent key 21 is not. The pinned gouroboros
// release stores key 21 in an int64 with omitempty and returns a non-nil zero
// for both, so a Sign() > 0 test collapses the two states and lets a
// transaction declare a zero treasury alongside a needed PlutusV1/V2 script.
func TestConwayFeaturesRuleDistinguishesAbsentFromDeclaredZeroTreasury(t *testing.T) {
	for _, tc := range []struct {
		name          string
		script        lcommon.Script
		plutusVersion string
	}{
		{
			name:          "PlutusV1",
			script:        lcommon.PlutusV1Script{0x05},
			plutusVersion: "PlutusV1",
		},
		{
			name:          "PlutusV2",
			script:        lcommon.PlutusV2Script{0x06},
			plutusVersion: "PlutusV2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := conwayFeaturesRule(t)

			// Absent key 21: accepted.
			absentTx, absentInput := decodeConwayTreasuryTx(t, tc.script, nil)
			require.NoError(t, rule(
				absentTx,
				0,
				newConwayTreasuryLedgerState(t, absentInput, tc.script),
				&conway.ConwayProtocolParameters{},
			))

			// Declared zero: rejected, same as any other declared value.
			zero := int64(0)
			zeroTx, zeroInput := decodeConwayTreasuryTx(t, tc.script, &zero)
			var zeroErr conway.CurrentTreasuryValueWithPlutusV1V2Error
			require.ErrorAs(t, rule(
				zeroTx,
				0,
				newConwayTreasuryLedgerState(t, zeroInput, tc.script),
				&conway.ConwayProtocolParameters{},
			), &zeroErr)
			assert.Equal(t, tc.plutusVersion, zeroErr.PlutusVersion)

			// Declared non-zero: rejected.
			nonZero := int64(42)
			nonZeroTx, nonZeroInput := decodeConwayTreasuryTx(
				t,
				tc.script,
				&nonZero,
			)
			var nonZeroErr conway.CurrentTreasuryValueWithPlutusV1V2Error
			require.ErrorAs(t, rule(
				nonZeroTx,
				0,
				newConwayTreasuryLedgerState(t, nonZeroInput, tc.script),
				&conway.ConwayProtocolParameters{},
			), &nonZeroErr)
			assert.Equal(t, tc.plutusVersion, nonZeroErr.PlutusVersion)
		})
	}
}

func newConwayTreasuryLedgerState(
	t *testing.T,
	input shelley.ShelleyTransactionInput,
	s lcommon.Script,
) *mockLedgerState {
	t.Helper()
	ls := newMockLedgerState()
	ls.addUtxo(input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestScriptAddress(t, s),
		scriptRef:  s,
	})
	return ls
}

// decodeConwayTreasuryTx builds a Conway transaction by encoding a
// transaction-body map and decoding it, so key 21's presence comes from real
// CBOR rather than from a Go field value.
func decodeConwayTreasuryTx(
	t *testing.T,
	plutusScript lcommon.Script,
	treasuryValue *int64,
) (*conway.ConwayTransaction, shelley.ShelleyTransactionInput) {
	t.Helper()
	input := shelley.ShelleyTransactionInput{
		TxId:        lcommon.Blake2b256{0x83},
		OutputIndex: 0,
	}
	bodyFields := map[uint]any{
		0: cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{input},
			true,
		),
	}
	if treasuryValue != nil {
		bodyFields[21] = *treasuryValue
	}
	bodyCbor, err := cbor.Encode(bodyFields)
	require.NoError(t, err)
	var body conway.ConwayTransactionBody
	require.NoError(t, body.UnmarshalCBOR(bodyCbor))

	witnesses := conway.ConwayTransactionWitnessSet{}
	switch script := plutusScript.(type) {
	case lcommon.PlutusV1Script:
		witnesses.WsPlutusV1Scripts = cbor.NewSetType(
			[]lcommon.PlutusV1Script{script},
			true,
		)
	case lcommon.PlutusV2Script:
		witnesses.WsPlutusV2Scripts = cbor.NewSetType(
			[]lcommon.PlutusV2Script{script},
			true,
		)
	default:
		t.Fatalf("unexpected script type %T", plutusScript)
	}
	return &conway.ConwayTransaction{
		Body:       body,
		WitnessSet: witnesses,
		TxIsValid:  true,
	}, input
}
