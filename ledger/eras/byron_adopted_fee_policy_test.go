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
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// feePolicyUpdate returns the update that adopts a fee policy of summand
// lovelace and multiplier lovelace per byte.
func feePolicyUpdate(
	summandLovelace, multiplierNano int64,
) byron.ByronUpdateProposalBlockVersionMod {
	return byron.ByronUpdateProposalBlockVersionMod{
		TxFeePolicy: []byron.ByronTxFeePolicy{{
			SummandNano:    big.NewInt(summandLovelace * byronFeePolicyScale),
			MultiplierNano: big.NewInt(multiplierNano),
		}},
	}
}

// TestValidateTxByron_AdoptedFeePolicyChangesRequiredFee covers #4419: the
// minimum fee is the summand plus the per-byte multiplier of the policy the
// caller passes, whichever of the two an adopted update changed. Genesis is
// 155,381 + 43.946 * 200 = 164,171 for a 200-byte transaction.
func TestValidateTxByron_AdoptedFeePolicyChangesRequiredFee(t *testing.T) {
	t.Parallel()
	genesis, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)
	const size = 200
	input := newTestInput(0x01, 0)
	newLS := func() *mockLedgerState {
		ls := newMockLedgerState()
		ls.byronFeeSummand = 155_381_000_000_000
		ls.byronFeeMultiplier = 43_946_000_000
		ls.byronMaxTxSize = 4_096
		ls.addUtxo(input, newTestOutput(10_000_000))
		return ls
	}
	tx := func(fee uint64) *testByronTx {
		return &testByronTx{
			inputs: []lcommon.TransactionInput{input},
			outputs: []lcommon.TransactionOutput{
				newTestOutput(10_000_000 - fee),
			},
			cbor: make([]byte, size),
		}
	}
	const genesisFee = uint64(164_171)
	require.NoError(t, ValidateTxByron(tx(genesisFee), 0, newLS(), nil))
	var genesisErr FeeTooLowByronError
	require.ErrorAs(
		t, ValidateTxByron(tx(genesisFee-1), 0, newLS(), nil), &genesisErr,
	)
	require.Equal(t, big.NewInt(int64(genesisFee)), genesisErr.Required)

	tests := []struct {
		name            string
		summand         int64
		multiplierNano  int64
		required        uint64
		genesisAccepts  bool
		adoptedAccepts  bool
		probeFeeAtBound uint64
	}{
		{
			"raise summand only",
			200_000,
			43_946_000_000,
			208_790,
			true,
			false,
			genesisFee,
		},
		{
			"lower summand only",
			100_000,
			43_946_000_000,
			108_790,
			false,
			true,
			108_790,
		},
		{
			"raise multiplier only",
			155_381,
			100_000_000_000,
			175_381,
			true,
			false,
			genesisFee,
		},
		{
			"lower multiplier only",
			155_381,
			10_000_000_000,
			157_381,
			false,
			true,
			157_381,
		},
		{
			"raise both",
			300_000,
			200_000_000_000,
			340_000,
			true,
			false,
			genesisFee,
		},
		{"lower both", 50_000, 5_000_000_000, 51_000, false, true, 51_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			adopted, err := genesis.ApplyUpdate(
				feePolicyUpdate(test.summand, test.multiplierNano),
			)
			require.NoError(t, err)
			require.NoError(
				t,
				ValidateTxByron(tx(test.required), 0, newLS(), adopted),
				"the adopted minimum is enough",
			)
			var feeErr FeeTooLowByronError
			require.ErrorAs(
				t,
				ValidateTxByron(tx(test.required-1), 0, newLS(), adopted),
				&feeErr,
				"one below the adopted minimum",
			)
			assert.Equal(
				t,
				new(big.Int).SetUint64(test.required),
				feeErr.Required,
			)
			// The same fee is judged by the passed policy, not by genesis.
			genesisVerdict := ValidateTxByron(
				tx(test.probeFeeAtBound),
				0,
				newLS(),
				nil,
			)
			adoptedVerdict := ValidateTxByron(
				tx(test.probeFeeAtBound),
				0,
				newLS(),
				adopted,
			)
			assert.Equal(t, test.genesisAccepts, genesisVerdict == nil)
			assert.Equal(t, test.adoptedAccepts, adoptedVerdict == nil)
		})
	}
}

// TestByronFeePolicySuccessiveAdoptionsUseLatest covers #4419: each adopted
// update replaces the whole policy, so the latest one alone sets the fee.
func TestByronFeePolicySuccessiveAdoptionsUseLatest(t *testing.T) {
	t.Parallel()
	genesis, err := NewByronProtocolParametersFromGenesis(mainnetByronGenesis())
	require.NoError(t, err)
	first, err := genesis.ApplyUpdate(feePolicyUpdate(200_000, 100_000_000_000))
	require.NoError(t, err)
	second, err := first.ApplyUpdate(feePolicyUpdate(120_000, 20_000_000_000))
	require.NoError(t, err)
	// A later update of another parameter keeps the latest policy.
	third, err := second.ApplyUpdate(byron.ByronUpdateProposalBlockVersionMod{
		MaxTxSize: []*big.Int{big.NewInt(8_192)},
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name   string
		params *ByronProtocolParameters
		want   int64
	}{
		{"genesis", genesis, 164_171},
		{"first", first, 220_000},
		{"second", second, 124_000},
		{"unrelated update afterwards", third, 124_000},
	} {
		required, err := test.params.MinFee(200)
		require.NoError(t, err, test.name)
		assert.Equal(t, big.NewInt(test.want), required, test.name)
	}
}

// TestByronGenesisFeePolicyNormalizationUnlikeAdopted pins the two decodings
// #4419 keeps apart: genesis truncates the summand to a whole lovelace, while
// an adopted on-chain policy rounds it half to even.
func TestByronGenesisFeePolicyNormalizationUnlikeAdopted(t *testing.T) {
	t.Parallel()
	genesisJSON := mainnetByronGenesis()
	genesisJSON.BlockVersionData.TxFeePolicy.Summand = 155_381_999_999_999
	genesis, err := NewByronProtocolParametersFromGenesis(genesisJSON)
	require.NoError(t, err)
	assert.Equal(t, uint64(155_381), genesis.TxFeeSummand)

	adopted, err := genesis.ApplyUpdate(
		byron.ByronUpdateProposalBlockVersionMod{
			TxFeePolicy: []byron.ByronTxFeePolicy{{
				SummandNano:    big.NewInt(155_381_999_999_999),
				MultiplierNano: big.NewInt(43_946_000_000),
			}},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(155_382), adopted.TxFeeSummand)
}

// TestValidateTxByron_RedeemOnlyExemptionUnderAdoptedPolicy covers #4419: the
// redeem-only zero-fee exception holds however high the adopted policy sets
// the minimum, and a transaction with any ordinary input owes that minimum.
func TestValidateTxByron_RedeemOnlyExemptionUnderAdoptedPolicy(t *testing.T) {
	t.Parallel()
	genesis, err := byronGenesisTestParams(
		155_381_000_000_000, 43_946_000_000, 0,
	)
	require.NoError(t, err)
	adopted, err := genesis.ApplyUpdate(
		feePolicyUpdate(500_000, 200_000_000_000),
	)
	require.NoError(t, err)

	ls := newRedeemFeeLedgerState()
	redeemOnly := buildByronRedeemTx(t, ls, byronRedeemTxCase{
		keys:            []byronRedeemKey{newByronRedeemKey(t, 0xA0)},
		fee:             0,
		validSignatures: true,
	})
	require.NoError(t, ValidateTxByron(redeemOnly, 0, ls, adopted))

	ls = newRedeemFeeLedgerState()
	ordinary := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0xC1)
	mixed := buildByronRedeemTx(t, ls, byronRedeemTxCase{
		keys: []byronRedeemKey{
			newByronRedeemKey(t, 0xA0),
			newByronRedeemKey(t, 0xA1),
		},
		ordinary:        map[int]lcommon.Address{1: ordinary},
		fee:             0,
		validSignatures: true,
	})
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, ValidateTxByron(mixed, 0, ls, adopted), &feeErr)
	want, err := adopted.MinFee(TxSizeForFee(mixed))
	require.NoError(t, err)
	assert.Equal(t, want, feeErr.Required)
}

// TestValidateTxByron_UnknownAdoptionSkipsSizeAndFeeRules covers the trusted
// start whose adopted parameters cannot be established: the fee and
// transaction size rules do not run against genesis values, while the same
// parameters marked known still enforce them.
func TestValidateTxByron_UnknownAdoptionSkipsSizeAndFeeRules(t *testing.T) {
	t.Parallel()
	params, err := byronGenesisTestParams(
		155_381_000_000_000, 43_946_000_000, 100,
	)
	require.NoError(t, err)
	input := newTestInput(0x01, 0)
	newLS := func() *mockLedgerState {
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(1_000_000))
		return ls
	}
	tx := &testByronTx{
		inputs:  []lcommon.TransactionInput{input},
		outputs: []lcommon.TransactionOutput{newTestOutput(1_000_000)},
		cbor:    make([]byte, 200),
	}
	var feeErr FeeTooLowByronError
	require.ErrorAs(t, ValidateTxByron(tx, 0, newLS(), params), &feeErr)
	var sizeErr TxTooLargeByronError
	require.ErrorAs(t, ValidateTxByron(tx, 0, newLS(), params), &sizeErr)

	unknown := params.Clone()
	unknown.AdoptionUnknown = true
	require.NoError(t, ValidateTxByron(tx, 0, newLS(), unknown))
}
