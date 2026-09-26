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
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// validateBabbageWithRule keeps the test at Dingo's era entry point while
// isolating one upstream UTxO predicate, so unrelated rules cannot mask its
// result.
func validateBabbageWithRule(
	t *testing.T,
	ruleID lcommon.UtxoValidationRuleId,
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *babbage.BabbageProtocolParameters,
) error {
	t.Helper()
	original := babbageUtxoValidationRules
	defer func() { babbageUtxoValidationRules = original }()
	index := resolveUtxoValidationSkipIndex(
		babbage.UtxoValidationRuleDescriptors(),
		babbage.UtxoValidationRules,
		ruleID,
	)
	for _, rule := range original {
		if rule.index == index {
			babbageUtxoValidationRules = []indexedUtxoValidationRule{rule}
			return ValidateTxBabbage(tx, 0, ls, pp)
		}
	}
	t.Fatalf("Dingo Babbage validation omitted rule %q", ruleID)
	return nil
}

func validateAlonzoWithRule(
	t *testing.T,
	ruleID lcommon.UtxoValidationRuleId,
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *alonzo.AlonzoProtocolParameters,
) error {
	t.Helper()
	original := alonzoUtxoValidationRules
	defer func() { alonzoUtxoValidationRules = original }()
	index := resolveUtxoValidationSkipIndex(
		alonzo.UtxoValidationRuleDescriptors(),
		alonzo.UtxoValidationRules,
		ruleID,
	)
	for _, rule := range original {
		if rule.index == index {
			alonzoUtxoValidationRules = []indexedUtxoValidationRule{rule}
			return ValidateTxAlonzo(tx, 0, ls, pp)
		}
	}
	t.Fatalf("Dingo Alonzo validation omitted rule %q", ruleID)
	return nil
}

// validateDijkstraWithRule executes an upstream rule through Dingo's
// production Dijkstra validator, retaining any phase wrapper on the rule.
func validateDijkstraWithRule(
	t *testing.T,
	ruleID lcommon.UtxoValidationRuleId,
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *gdijkstra.DijkstraProtocolParameters,
) error {
	t.Helper()
	original := dijkstraPhase1UtxoValidationRules
	defer func() { dijkstraPhase1UtxoValidationRules = original }()
	index := resolveUtxoValidationSkipIndex(
		gdijkstra.UtxoValidationRuleDescriptors(),
		gdijkstra.UtxoValidationRules,
		ruleID,
	)
	for _, rule := range original {
		if rule.index == index {
			dijkstraPhase1UtxoValidationRules = []indexedUtxoValidationRule{rule}
			return ValidateTxDijkstra(tx, 0, ls, pp)
		}
	}
	t.Fatalf("Dingo Dijkstra validation omitted rule %q", ruleID)
	return nil
}

func validateConwayWithRule(
	t *testing.T,
	ruleID lcommon.UtxoValidationRuleId,
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp *conway.ConwayProtocolParameters,
) error {
	t.Helper()
	original := conwayUtxoValidationRules
	originalPhase1 := conwayPhase1UtxoValidationRules
	defer func() {
		conwayUtxoValidationRules = original
		conwayPhase1UtxoValidationRules = originalPhase1
	}()
	index := resolveUtxoValidationSkipIndex(
		conway.UtxoValidationRuleDescriptors(),
		conway.UtxoValidationRules,
		ruleID,
	)
	for _, rule := range original {
		if rule.index == index {
			conwayUtxoValidationRules = []indexedUtxoValidationRule{rule}
			conwayPhase1UtxoValidationRules = conwayUtxoValidationRules
			return ValidateTxConway(tx, 0, ls, pp)
		}
	}
	t.Fatalf("Dingo Conway validation omitted rule %q", ruleID)
	return nil
}

func babbageRedeemers() alonzo.AlonzoRedeemers {
	return alonzo.AlonzoRedeemers{
		Redeemers: []alonzo.AlonzoRedeemer{{
			Tag: lcommon.RedeemerTagSpend,
		}},
	}
}

func TestValidateTxBabbageCollateralKeyLockOnlyWhenPhase2Runs(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	plutus := lcommon.PlutusV1Script{0x01, 0x02}
	scriptAddress := newTestScriptAddress(t, plutus)
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, testAddressOutput{
		testOutput: newTestOutput(10_000_000),
		addr:       scriptAddress,
	})
	protocolParams := &babbage.BabbageProtocolParameters{}

	noPhase2 := &babbage.BabbageTransaction{
		Body: babbage.BabbageTransactionBody{
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{input},
				false,
			),
		},
		TxIsValid: true,
	}
	require.NoError(t, validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralKeyLocked,
		noPhase2,
		state,
		protocolParams,
	))

	phase2 := *noPhase2
	phase2.WitnessSet = babbage.BabbageTransactionWitnessSet{
		WsRedeemers: alonzo.AlonzoRedeemers{
			Redeemers: []alonzo.AlonzoRedeemer{{
				Tag: lcommon.RedeemerTagSpend,
			}},
		},
	}
	err := validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralKeyLocked,
		&phase2,
		state,
		protocolParams,
	)
	require.ErrorContains(t, err, "collateral input must be key-locked")
}

func TestValidateTxDijkstraSkipsGovRulesForInvalidTransactions(t *testing.T) {
	rewardAccount, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	action := &lcommon.HardForkInitiationGovAction{}
	action.ProtocolVersion.Major = gdijkstra.MinProtocolVersionDijkstra
	action.ProtocolVersion.Minor = 2
	proposal := gdijkstra.DijkstraProposalProcedure{
		PPRewardAccount: rewardAccount,
		PPGovAction: gdijkstra.DijkstraGovAction{
			Type:   uint(lcommon.GovActionTypeHardForkInitiation),
			Action: action,
		},
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxProposalProcedures: []gdijkstra.DijkstraProposalProcedure{proposal},
		},
		TxIsValid: false,
	}
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	protocolParams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}

	// 12.2 cannot follow the enacted 12.0 version under the GOV predicate.
	// Dijkstra's reference transition skips that semantic predicate when the
	// transaction is phase-2-invalid, while always-run UTXOW rules still run.
	require.NoError(t, validateDijkstraWithRule(
		t,
		lcommon.UtxoValidationRuleHardForkCanFollow,
		tx,
		state,
		protocolParams,
	))
	tx.TxIsValid = true
	var badVersion conway.BadHardForkProtocolVersionError
	err = validateDijkstraWithRule(
		t,
		lcommon.UtxoValidationRuleHardForkCanFollow,
		tx,
		state,
		protocolParams,
	)
	require.ErrorAs(t, err, &badVersion)
}

type minPoolMarginTestLedgerState struct {
	*mockLedgerState
	floor *big.Rat
}

func (s *minPoolMarginTestLedgerState) MinPoolMargin() *big.Rat {
	return s.floor
}

func TestValidateTxDijkstraPoolMarginFollowsTransactionValidity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		declaredValid bool
		wantPoolError bool
	}{
		{name: "valid transaction enforces margin", declaredValid: true, wantPoolError: true},
		{name: "invalid transaction skips entity margin", declaredValid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &minPoolMarginTestLedgerState{
				mockLedgerState: newMockLedgerState(),
				floor:           big.NewRat(150, 10_000),
			}
			state.skipPhase2Validation = true
			cert := &lcommon.PoolRegistrationCertificate{
				Margin: lcommon.GenesisRat{Rat: big.NewRat(1, 1000)},
			}
			tx := &gdijkstra.DijkstraTransaction{
				Body: gdijkstra.DijkstraTransactionBody{
					TxCertificates: []lcommon.CertificateWrapper{{
						Type:        uint(lcommon.CertificateTypePoolRegistration),
						Certificate: cert,
					}},
				},
				TxIsValid: tc.declaredValid,
			}
			params := &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: gdijkstra.MinProtocolVersionDijkstra,
					},
				},
			}

			err := ValidateTxDijkstra(tx, 0, state, params)
			if tc.wantPoolError {
				require.ErrorContains(t, err, "below minimum pool margin")
			} else if err != nil {
				require.NotContains(t, err.Error(), "below minimum pool margin")
			}
		})
	}
}

func TestValidateTxDijkstraCollateralReturnRejectsPointerStakeAddress(t *testing.T) {
	for _, addressType := range []uint8{
		lcommon.AddressTypeKeyPointer,
		lcommon.AddressTypeScriptPointer,
	} {
		t.Run(fmt.Sprintf("address type %d", addressType), func(t *testing.T) {
			rawAddress := append([]byte{addressType << 4}, make([]byte, 28)...)
			rawAddress = append(rawAddress, 0, 0, 0)
			wire, err := cbor.Encode(map[uint]any{0: rawAddress, 1: uint64(0)})
			require.NoError(t, err)
			var output babbage.BabbageTransactionOutput
			_, err = cbor.Decode(wire, &output)
			require.NoError(t, err)
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			tx := &gdijkstra.DijkstraTransaction{
				Body: gdijkstra.DijkstraTransactionBody{
					TxCollateralReturn: &gdijkstra.DijkstraTransactionOutput{
						Output: output,
					},
				},
				TxIsValid: true,
			}
			var pointerError *lcommon.PtrPresentInCollateralReturn
			err = validateDijkstraWithRule(
				t,
				lcommon.UtxoValidationRulePtrPresentInCollateralReturn,
				tx,
				state,
				&gdijkstra.DijkstraProtocolParameters{},
			)
			require.ErrorAs(t, err, &pointerError)
			require.EqualValues(t, 22, pointerError.Type)
		})
	}

	state := newMockLedgerState()
	state.skipPhase2Validation = true
	output := babbage.BabbageTransactionOutput{
		OutputAddress: newTestKeyAddress(t),
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1_000_000},
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxCollateralReturn: &gdijkstra.DijkstraTransactionOutput{Output: output},
		},
		TxIsValid: true,
	}
	require.NoError(t, validateDijkstraWithRule(
		t,
		lcommon.UtxoValidationRulePtrPresentInCollateralReturn,
		tx,
		state,
		&gdijkstra.DijkstraProtocolParameters{},
	))
}

func TestValidateTxAllErasRejectExtraneousRedeemerForKeyInput(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"6666666666666666666666666666666666666666666666666666666666666666",
		0,
	)
	for _, tc := range []struct {
		name     string
		validate func(*testing.T, lcommon.Transaction, *mockLedgerState) error
		tx       lcommon.Transaction
	}{
		{
			name: "Alonzo",
			tx: &alonzo.AlonzoTransaction{
				Body: alonzo.AlonzoTransactionBody{
					TxInputs: shelley.NewShelleyTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
				},
				WitnessSet: alonzo.AlonzoTransactionWitnessSet{WsRedeemers: babbageRedeemers()},
				TxIsValid:  false,
			},
			validate: func(t *testing.T, tx lcommon.Transaction, state *mockLedgerState) error {
				return validateAlonzoWithRule(t, lcommon.UtxoValidationRuleExtraneousRedeemers, tx, state, &alonzo.AlonzoProtocolParameters{})
			},
		},
		{
			name: "Babbage",
			tx: &babbage.BabbageTransaction{
				Body: babbage.BabbageTransactionBody{
					TxInputs: shelley.NewShelleyTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
				},
				WitnessSet: babbage.BabbageTransactionWitnessSet{WsRedeemers: babbageRedeemers()},
				TxIsValid:  false,
			},
			validate: func(t *testing.T, tx lcommon.Transaction, state *mockLedgerState) error {
				return validateBabbageWithRule(t, lcommon.UtxoValidationRuleExtraneousRedeemers, tx, state, &babbage.BabbageProtocolParameters{})
			},
		},
		{
			name: "Conway",
			tx: &conway.ConwayTransaction{
				Body: conway.ConwayTransactionBody{
					TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
				},
				WitnessSet: conway.ConwayTransactionWitnessSet{WsRedeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
				}}},
				TxIsValid: false,
			},
			validate: func(t *testing.T, tx lcommon.Transaction, state *mockLedgerState) error {
				return validateConwayWithRule(t, lcommon.UtxoValidationRuleExtraneousRedeemers, tx, state, &conway.ConwayProtocolParameters{})
			},
		},
		{
			name: "Dijkstra",
			tx: &gdijkstra.DijkstraTransaction{
				Body: gdijkstra.DijkstraTransactionBody{
					TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
				},
				WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{WsRedeemers: gdijkstra.DijkstraRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
				}}},
				TxIsValid: false,
			},
			validate: func(t *testing.T, tx lcommon.Transaction, state *mockLedgerState) error {
				return validateDijkstraWithRule(t, lcommon.UtxoValidationRuleExtraneousRedeemers, tx, state, &gdijkstra.DijkstraProtocolParameters{})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			state.addUtxo(input, newTestOutput(1_000_000))
			err := tc.validate(t, tc.tx, state)
			require.Error(t, err)
			require.Contains(t, err.Error(), "redeemer")
		})
	}
}

func TestValidateTxBabbageCollateralUsesNetBalanceAndExactThreshold(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, newTestOutput(10_000_000))
	tx := &babbage.BabbageTransaction{
		Body: babbage.BabbageTransactionBody{
			TxFee:        1_000_000,
			TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
			TxCollateralReturn: &babbage.BabbageTransactionOutput{
				OutputAmount: mary.MaryTransactionOutputValue{Amount: 9_000_000},
			},
		},
		WitnessSet: babbage.BabbageTransactionWitnessSet{WsRedeemers: babbageRedeemers()},
		TxIsValid:  false,
	}
	var insufficient alonzo.InsufficientCollateralError
	err := validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleInsufficientCollateral,
		tx,
		state,
		&babbage.BabbageProtocolParameters{CollateralPercentage: 150},
	)
	require.ErrorAs(t, err, &insufficient, "collateral return must reduce collateral to the net amount")

	for _, tc := range []struct {
		name    string
		amount  uint64
		wantErr bool
	}{
		{name: "one below rounded-up requirement", amount: 151, wantErr: true},
		{name: "exact rounded-up requirement", amount: 152},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			state.addUtxo(input, newTestOutput(tc.amount))
			boundaryTx := &babbage.BabbageTransaction{
				Body: babbage.BabbageTransactionBody{
					TxFee:        101,
					TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
				},
				WitnessSet: babbage.BabbageTransactionWitnessSet{WsRedeemers: babbageRedeemers()},
				TxIsValid:  false,
			}
			err := validateBabbageWithRule(
				t,
				lcommon.UtxoValidationRuleInsufficientCollateral,
				boundaryTx,
				state,
				&babbage.BabbageProtocolParameters{CollateralPercentage: 150},
			)
			if tc.wantErr {
				require.ErrorAs(t, err, &insufficient)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxAlonzoCollateralThresholdUsesCeiling(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	for _, tc := range []struct {
		name    string
		fee     uint64
		amount  uint64
		wantErr bool
	}{
		{name: "one below rounded-up requirement", fee: 101, amount: 151, wantErr: true},
		{name: "exact rounded-up requirement", fee: 101, amount: 152},
		{name: "exact percentage multiple", fee: 100, amount: 150},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			state.addUtxo(input, newTestOutput(tc.amount))
			tx := &alonzo.AlonzoTransaction{
				Body: alonzo.AlonzoTransactionBody{
					TxFee:        tc.fee,
					TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
				},
				WitnessSet: alonzo.AlonzoTransactionWitnessSet{
					WsRedeemers: babbageRedeemers(),
				},
				TxIsValid: false,
			}
			err := validateAlonzoWithRule(
				t,
				lcommon.UtxoValidationRuleInsufficientCollateral,
				tx,
				state,
				&alonzo.AlonzoProtocolParameters{CollateralPercentage: 150},
			)
			var insufficient alonzo.InsufficientCollateralError
			if tc.wantErr {
				require.ErrorAs(t, err, &insufficient)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxConwayCollateralUsesNetBalanceAndExactThreshold(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	for _, tc := range []struct {
		name             string
		returnAmount     uint64
		wantInsufficient bool
	}{
		{name: "one below net threshold", returnAmount: 8_499_999, wantInsufficient: true},
		{name: "exact net threshold", returnAmount: 8_499_998},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			state.addUtxo(input, newTestOutput(10_000_000))
			tx := &conway.ConwayTransaction{
				Body: conway.ConwayTransactionBody{
					TxFee:        1_000_001,
					TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
					TxCollateralReturn: &babbage.BabbageTransactionOutput{
						OutputAddress: newTestKeyAddress(t),
						OutputAmount:  mary.MaryTransactionOutputValue{Amount: tc.returnAmount},
					},
				},
				WitnessSet: conway.ConwayTransactionWitnessSet{
					WsRedeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
						{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
					}},
				},
				TxIsValid: false,
			}
			err := validateConwayWithRule(
				t,
				lcommon.UtxoValidationRuleInsufficientCollateral,
				tx,
				state,
				&conway.ConwayProtocolParameters{CollateralPercentage: 150},
			)
			var insufficient alonzo.InsufficientCollateralError
			if tc.wantInsufficient {
				require.ErrorAs(t, err, &insufficient)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxDijkstraCollateralUsesNetBalanceAndExactThreshold(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	for _, tc := range []struct {
		name             string
		returnAmount     uint64
		wantInsufficient bool
	}{
		{name: "one below net threshold", returnAmount: 8_499_999, wantInsufficient: true},
		{name: "exact net threshold", returnAmount: 8_499_998},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			state.addUtxo(input, newTestOutput(10_000_000))
			tx := &gdijkstra.DijkstraTransaction{
				Body: gdijkstra.DijkstraTransactionBody{
					TxFee:        1_000_001,
					TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
					TxCollateralReturn: &gdijkstra.DijkstraTransactionOutput{
						Output: babbage.BabbageTransactionOutput{
							OutputAddress: newTestKeyAddress(t),
							OutputAmount:  mary.MaryTransactionOutputValue{Amount: tc.returnAmount},
						},
					},
				},
				WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
					WsRedeemers: gdijkstra.DijkstraRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
						{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
					}},
				},
				TxIsValid: false,
			}
			params := &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{CollateralPercentage: 150},
			}
			err := validateDijkstraWithRule(
				t,
				lcommon.UtxoValidationRuleInsufficientCollateral,
				tx,
				state,
				params,
			)
			var insufficient alonzo.InsufficientCollateralError
			if tc.wantInsufficient {
				require.ErrorAs(t, err, &insufficient)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxBabbageCollateralReturnCannotCreateAssets(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	policy := lcommon.Blake2b224Hash([]byte("policy"))
	assetName := cbor.NewByteString([]byte("token"))
	returnAssets := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
		policy: {assetName: big.NewInt(1)},
	})
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, newTestOutput(2_000_000))
	tx := &babbage.BabbageTransaction{
		Body: babbage.BabbageTransactionBody{
			TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
			TxCollateralReturn: &babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: 1_000_000,
					Assets: &returnAssets,
				},
			},
		},
		WitnessSet: babbage.BabbageTransactionWitnessSet{WsRedeemers: babbageRedeemers()},
		TxIsValid:  false,
	}
	var collateralAssetError alonzo.CollateralContainsNonAdaError
	err := validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralContainsNonAda,
		tx,
		state,
		&babbage.BabbageProtocolParameters{},
	)
	require.ErrorAs(t, err, &collateralAssetError)

	inputAssets := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
		policy: {assetName: big.NewInt(1)},
	})
	state.addUtxo(input, mary.MaryTransactionOutput{
		OutputAmount: mary.MaryTransactionOutputValue{
			Amount: 2_000_000,
			Assets: &inputAssets,
		},
	})
	require.NoError(t, validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralContainsNonAda,
		tx,
		state,
		&babbage.BabbageProtocolParameters{},
	))
}

func TestValidateTxConwayCollateralReturnCannotCreateAssets(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	policy := lcommon.Blake2b224Hash([]byte("policy"))
	assetName := cbor.NewByteString([]byte("token"))
	returnAssets := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
		policy: {assetName: big.NewInt(1)},
	})
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, newTestOutput(2_000_000))
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxCollateral: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
			TxCollateralReturn: &babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: 1_000_000,
					Assets: &returnAssets,
				},
			},
		},
		WitnessSet: conway.ConwayTransactionWitnessSet{
			WsRedeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
				{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
			}},
		},
		TxIsValid: false,
	}
	var assetError alonzo.CollateralContainsNonAdaError
	err := validateConwayWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralContainsNonAda,
		tx,
		state,
		&conway.ConwayProtocolParameters{},
	)
	require.ErrorAs(t, err, &assetError)

	inputAssets := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
		policy: {assetName: big.NewInt(1)},
	})
	state.addUtxo(input, mary.MaryTransactionOutput{
		OutputAmount: mary.MaryTransactionOutputValue{
			Amount: 2_000_000,
			Assets: &inputAssets,
		},
	})
	require.NoError(t, validateConwayWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralContainsNonAda,
		tx,
		state,
		&conway.ConwayProtocolParameters{},
	))
}

func TestValidateTxBabbageCollateralReturnOutputPredicates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ruleID lcommon.UtxoValidationRuleId
		output babbage.BabbageTransactionOutput
		state  func() *mockLedgerState
		pp     *babbage.BabbageProtocolParameters
	}{
		{
			name:   "minimum ada",
			ruleID: lcommon.UtxoValidationRuleOutputTooSmall,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
			},
			state: func() *mockLedgerState { return newMockLedgerState() },
			pp:    &babbage.BabbageProtocolParameters{AdaPerUtxoByte: 1},
		},
		{
			name:   "maximum value size",
			ruleID: lcommon.UtxoValidationRuleOutputTooBig,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1},
			},
			state: func() *mockLedgerState { return newMockLedgerState() },
			pp:    &babbage.BabbageProtocolParameters{MaxValueSize: 0},
		},
		{
			name:   "byron address attributes",
			ruleID: lcommon.UtxoValidationRuleOutputBootAddrAttrsTooBig,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: func() lcommon.Address {
					addr, err := lcommon.NewByronAddressFromParts(
						lcommon.ByronAddressTypePubkey,
						make([]byte, lcommon.AddressHashSize),
						lcommon.ByronAddressAttributes{Payload: make([]byte, 100)},
					)
					require.NoError(t, err)
					return addr
				}(),
				OutputAmount: mary.MaryTransactionOutputValue{Amount: 1_000_000},
			},
			state: func() *mockLedgerState { return newMockLedgerState() },
			pp:    &babbage.BabbageProtocolParameters{},
		},
		{
			name:   "network identifier",
			ruleID: lcommon.UtxoValidationRuleWrongNetwork,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1_000_000},
			},
			state: func() *mockLedgerState {
				state := newMockLedgerState()
				state.networkId = uint(lcommon.AddressNetworkMainnet)
				return state
			},
			pp: &babbage.BabbageProtocolParameters{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state()
			state.skipPhase2Validation = true
			tx := &babbage.BabbageTransaction{
				Body: babbage.BabbageTransactionBody{
					TxCollateralReturn: &tc.output,
				},
				TxIsValid: true,
			}
			require.Error(t, validateBabbageWithRule(
				t,
				tc.ruleID,
				tx,
				state,
				tc.pp,
			))
		})
	}
}

func TestValidateTxConwayCollateralReturnOutputPredicates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ruleID lcommon.UtxoValidationRuleId
		output babbage.BabbageTransactionOutput
		state  func() *mockLedgerState
		pp     *conway.ConwayProtocolParameters
	}{
		{
			name:   "minimum ada",
			ruleID: lcommon.UtxoValidationRuleOutputTooSmall,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
			},
			state: func() *mockLedgerState { return newMockLedgerState() },
			pp:    &conway.ConwayProtocolParameters{AdaPerUtxoByte: 1},
		},
		{
			name:   "maximum value size",
			ruleID: lcommon.UtxoValidationRuleOutputTooBig,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1},
			},
			state: func() *mockLedgerState { return newMockLedgerState() },
			pp:    &conway.ConwayProtocolParameters{MaxValueSize: 0},
		},
		{
			name:   "byron address attributes",
			ruleID: lcommon.UtxoValidationRuleOutputBootAddrAttrsTooBig,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: func() lcommon.Address {
					addr, err := lcommon.NewByronAddressFromParts(
						lcommon.ByronAddressTypePubkey,
						make([]byte, lcommon.AddressHashSize),
						lcommon.ByronAddressAttributes{Payload: make([]byte, 100)},
					)
					require.NoError(t, err)
					return addr
				}(),
				OutputAmount: mary.MaryTransactionOutputValue{Amount: 1_000_000},
			},
			state: func() *mockLedgerState { return newMockLedgerState() },
			pp:    &conway.ConwayProtocolParameters{},
		},
		{
			name:   "network identifier",
			ruleID: lcommon.UtxoValidationRuleWrongNetwork,
			output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1_000_000},
			},
			state: func() *mockLedgerState {
				state := newMockLedgerState()
				state.networkId = uint(lcommon.AddressNetworkMainnet)
				return state
			},
			pp: &conway.ConwayProtocolParameters{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state()
			state.skipPhase2Validation = true
			tx := &conway.ConwayTransaction{
				Body: conway.ConwayTransactionBody{
					TxCollateralReturn: &tc.output,
				},
				TxIsValid: true,
			}
			require.Error(t, validateConwayWithRule(
				t,
				tc.ruleID,
				tx,
				state,
				tc.pp,
			))
		})
	}
}

func TestValidateTxBabbageTotalCollateralDistinguishesAbsentAndZero(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, newTestOutput(5_000_000))
	makeTx := func(body babbage.BabbageTransactionBody) *babbage.BabbageTransaction {
		body.TxCollateral = cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false)
		body.TxCollateralReturn = &babbage.BabbageTransactionOutput{
			OutputAddress: newTestKeyAddress(t),
			OutputAmount:  mary.MaryTransactionOutputValue{Amount: 3_000_000},
		}
		return &babbage.BabbageTransaction{
			Body:       body,
			WitnessSet: babbage.BabbageTransactionWitnessSet{WsRedeemers: babbageRedeemers()},
			TxIsValid:  false,
		}
	}
	var explicitZero babbage.BabbageTransactionBody
	bodyCBOR, err := cbor.Encode(map[uint]any{17: uint64(0)})
	require.NoError(t, err)
	_, err = cbor.Decode(bodyCBOR, &explicitZero)
	require.NoError(t, err)
	var incorrect babbage.IncorrectTotalCollateralFieldError
	err = validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		makeTx(explicitZero),
		state,
		&babbage.BabbageProtocolParameters{},
	)
	require.ErrorAs(t, err, &incorrect)

	zeroBalance := makeTx(explicitZero)
	zeroBalance.Body.TxCollateralReturn.OutputAmount.Amount = 5_000_000
	require.NoError(t, validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		zeroBalance,
		state,
		&babbage.BabbageProtocolParameters{},
	))

	var absent babbage.BabbageTransactionBody
	require.NoError(t, validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		makeTx(absent),
		state,
		&babbage.BabbageProtocolParameters{},
	))
}

func TestValidateTxConwayTotalCollateralDistinguishesAbsentAndZero(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, newTestOutput(5_000_000))
	makeTx := func(body conway.ConwayTransactionBody) *conway.ConwayTransaction {
		body.TxCollateral = cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false)
		body.TxCollateralReturn = &babbage.BabbageTransactionOutput{
			OutputAddress: newTestKeyAddress(t),
			OutputAmount:  mary.MaryTransactionOutputValue{Amount: 3_000_000},
		}
		return &conway.ConwayTransaction{
			Body:       body,
			WitnessSet: conway.ConwayTransactionWitnessSet{WsRedeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{{Tag: lcommon.RedeemerTagSpend, Index: 0}: {}}}},
			TxIsValid:  false,
		}
	}
	decodeBody := func(totalCollateral any, include bool) conway.ConwayTransactionBody {
		fields := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
		if include {
			fields[17] = totalCollateral
		}
		encoded, err := cbor.Encode(fields)
		require.NoError(t, err)
		var body conway.ConwayTransactionBody
		_, err = cbor.Decode(encoded, &body)
		require.NoError(t, err)
		return body
	}

	var incorrect babbage.IncorrectTotalCollateralFieldError
	err := validateConwayWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		makeTx(decodeBody(uint64(0), true)),
		state,
		&conway.ConwayProtocolParameters{},
	)
	require.ErrorAs(t, err, &incorrect)

	zeroBalance := makeTx(decodeBody(uint64(0), true))
	zeroBalance.Body.TxCollateralReturn.OutputAmount.Amount = 5_000_000
	require.NoError(t, validateConwayWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		zeroBalance,
		state,
		&conway.ConwayProtocolParameters{},
	))

	require.NoError(t, validateConwayWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		makeTx(decodeBody(nil, false)),
		state,
		&conway.ConwayProtocolParameters{},
	))
}

func TestValidateTxDijkstraTotalCollateralPreservesZeroPresence(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	decodeBody := func(includeTotalCollateral bool) gdijkstra.DijkstraTransactionBody {
		fields := map[uint]any{
			0:  []any{},
			1:  []any{},
			2:  uint64(0),
			13: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
		}
		if includeTotalCollateral {
			fields[17] = uint64(0)
		}
		encoded, err := cbor.Encode(fields)
		require.NoError(t, err)
		var body gdijkstra.DijkstraTransactionBody
		_, err = cbor.Decode(encoded, &body)
		require.NoError(t, err)
		return body
	}
	newTx := func(includeTotalCollateral bool) *gdijkstra.DijkstraTransaction {
		body := decodeBody(includeTotalCollateral)
		child := gdijkstra.DijkstraSubTransaction{
			WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
				WsRedeemers: gdijkstra.DijkstraRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
				}},
			},
		}
		body.TxSubTransactions = cbor.NewSetType(
			[]gdijkstra.DijkstraSubTransaction{child},
			false,
		)
		return &gdijkstra.DijkstraTransaction{Body: body, TxIsValid: false}
	}
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	state.addUtxo(input, newTestOutput(5_000_000))
	params := &gdijkstra.DijkstraProtocolParameters{}
	var incorrect babbage.IncorrectTotalCollateralFieldError
	err := validateDijkstraWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		newTx(true),
		state,
		params,
	)
	require.ErrorAs(t, err, &incorrect)

	// An absent key 17 carries no equality assertion, even though the decoded
	// numeric field and the explicitly encoded zero above both read as zero.
	require.NoError(t, validateDijkstraWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		newTx(false),
		state,
		params,
	))

	zeroInputState := newMockLedgerState()
	zeroInputState.skipPhase2Validation = true
	zeroInputState.addUtxo(input, newTestOutput(0))
	require.NoError(t, validateDijkstraWithRule(
		t,
		lcommon.UtxoValidationRuleCollateralEqBalance,
		newTx(true),
		zeroInputState,
		params,
	))
}

func TestConwayTransactionDecoderRejectsPresentEmptyWitnessFields(t *testing.T) {
	body := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
	absentWitnesses := map[uint]any{}
	encode := func(witnesses map[uint]any) []byte {
		t.Helper()
		encoded, err := cbor.Encode([]any{body, witnesses, true, nil})
		require.NoError(t, err)
		return encoded
	}
	_, err := conway.NewConwayTransactionFromCbor(encode(absentWitnesses))
	require.NoError(t, err, "absent witness fields remain valid")

	for field := uint(0); field <= 7; field++ {
		t.Run(fmt.Sprintf("field %d", field), func(t *testing.T) {
			value := any([]any{})
			if field == 5 {
				value = map[uint]any{}
			}
			_, err := conway.NewConwayTransactionFromCbor(encode(map[uint]any{
				field: value,
			}))
			require.Error(t, err, "a present empty field must be rejected")
		})
	}
}

func TestDijkstraTransactionDecoderRejectsEmptyWitnessAndBodyFields(t *testing.T) {
	baseBody := func() map[uint]any {
		return map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
	}
	encode := func(body, witnesses map[uint]any) []byte {
		t.Helper()
		encoded, err := cbor.Encode([]any{body, witnesses, nil})
		require.NoError(t, err)
		return encoded
	}
	_, err := gdijkstra.NewDijkstraTransactionFromCbor(encode(baseBody(), map[uint]any{}))
	require.NoError(t, err, "absent witness fields remain valid")

	for field := uint(0); field <= 8; field++ {
		t.Run(fmt.Sprintf("witness field %d", field), func(t *testing.T) {
			value := any([]any{})
			if field == 5 {
				value = map[uint]any{}
			}
			_, err := gdijkstra.NewDijkstraTransactionFromCbor(encode(
				baseBody(),
				map[uint]any{field: value},
			))
			require.Error(t, err, "a present empty field or invalid key 8 must be rejected")
		})
	}

	for missing := uint(0); missing <= 2; missing++ {
		t.Run(fmt.Sprintf("missing required top-level body key %d", missing), func(t *testing.T) {
			body := baseBody()
			delete(body, missing)
			_, err := gdijkstra.NewDijkstraTransactionFromCbor(encode(body, map[uint]any{}))
			require.Error(t, err)
		})
	}

	for _, field := range []uint{4, 5, 9, 13, 14, 18, 20} {
		t.Run(fmt.Sprintf("empty guarded top-level body key %d", field), func(t *testing.T) {
			body := baseBody()
			value := any([]any{})
			if field == 5 || field == 9 || field == 14 {
				value = map[uint]any{}
			}
			body[field] = value
			_, err := gdijkstra.NewDijkstraTransactionFromCbor(encode(body, map[uint]any{}))
			require.Error(t, err)
		})
	}

	for missing := uint(0); missing <= 1; missing++ {
		t.Run(fmt.Sprintf("missing required subtransaction body key %d", missing), func(t *testing.T) {
			subBody := map[uint]any{0: []any{}, 1: []any{}}
			delete(subBody, missing)
			body := baseBody()
			body[23] = []any{[]any{subBody, map[uint]any{}, nil}}
			_, err := gdijkstra.NewDijkstraTransactionFromCbor(encode(body, map[uint]any{}))
			require.Error(t, err)
		})
	}

	for _, field := range []uint{4, 5, 18, 20} {
		t.Run(fmt.Sprintf("empty guarded subtransaction body key %d", field), func(t *testing.T) {
			value := any([]any{})
			if field == 5 {
				value = map[uint]any{}
			}
			subBody := map[uint]any{0: []any{}, 1: []any{}, field: value}
			body := baseBody()
			body[23] = []any{[]any{subBody, map[uint]any{}, nil}}
			_, err := gdijkstra.NewDijkstraTransactionFromCbor(encode(body, map[uint]any{}))
			require.Error(t, err)
		})
	}

	validSubBody := map[uint]any{0: []any{}, 1: []any{}}
	validBody := baseBody()
	validBody[23] = []any{[]any{validSubBody, map[uint]any{}, nil}}
	_, err = gdijkstra.NewDijkstraTransactionFromCbor(encode(validBody, map[uint]any{}))
	require.NoError(t, err, "present empty output sequences are allowed at both levels")
}

func TestConwayTransactionDecoderRequiresBodyFieldsAndAllowsEmptyOutputs(t *testing.T) {
	for missing := uint(0); missing <= 2; missing++ {
		t.Run(fmt.Sprintf("missing required body key %d", missing), func(t *testing.T) {
			body := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
			delete(body, missing)
			raw, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
			require.NoError(t, err)
			_, err = conway.NewConwayTransactionFromCbor(raw)
			require.Error(t, err)
		})
	}
	body := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
	raw, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	_, err = conway.NewConwayTransactionFromCbor(raw)
	require.NoError(t, err, "a present empty output sequence is allowed")
	for _, field := range []uint{4, 5, 9, 13, 14, 18, 20} {
		t.Run(fmt.Sprintf("empty guarded body key %d", field), func(t *testing.T) {
			body := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
			value := any([]any{})
			if field == 5 || field == 9 {
				value = map[uint]any{}
			}
			body[field] = value
			raw, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
			require.NoError(t, err)
			_, err = conway.NewConwayTransactionFromCbor(raw)
			require.Error(t, err)
		})
	}
}

func TestProductionBlocksPreserveOrderedInvalidTransactionIndexes(t *testing.T) {
	type transactionBlock interface {
		Transactions() []lcommon.Transaction
	}
	for _, tc := range []struct {
		name     string
		newBlock func([]uint) transactionBlock
	}{
		{
			name: "Alonzo",
			newBlock: func(indexes []uint) transactionBlock {
				return &alonzo.AlonzoBlock{
					TransactionBodies:      make([]alonzo.AlonzoTransactionBody, 2),
					TransactionWitnessSets: make([]alonzo.AlonzoTransactionWitnessSet, 2),
					InvalidTransactions:    indexes,
				}
			},
		},
		{
			name: "Babbage",
			newBlock: func(indexes []uint) transactionBlock {
				return &babbage.BabbageBlock{
					TransactionBodies:      make([]babbage.BabbageTransactionBody, 2),
					TransactionWitnessSets: make([]babbage.BabbageTransactionWitnessSet, 2),
					InvalidTransactions:    indexes,
				}
			},
		},
		{
			name: "Conway",
			newBlock: func(indexes []uint) transactionBlock {
				return &conway.ConwayBlock{
					TransactionBodies:      make([]conway.ConwayTransactionBody, 2),
					TransactionWitnessSets: make([]conway.ConwayTransactionWitnessSet, 2),
					InvalidTransactions:    indexes,
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, flags := range []struct {
				name     string
				indexes  []uint
				validity []bool
			}{
				{name: "duplicate index", indexes: []uint{0, 0}, validity: []bool{false, false}},
				{name: "descending indexes", indexes: []uint{1, 0}, validity: []bool{true, false}},
			} {
				t.Run(flags.name, func(t *testing.T) {
					transactions := tc.newBlock(flags.indexes).Transactions()
					require.Len(t, transactions, len(flags.validity))
					for i, tx := range transactions {
						require.Equal(t, flags.validity[i], tx.IsValid(), "transaction %d", i)
					}
				})
			}
		})
	}

	body := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(1)}
	witnesses := []any{map[uint]any{}, map[uint]any{}}
	metadata := map[uint]any{}
	raw, err := cbor.Encode([]any{nil, []any{body, body}, witnesses, metadata, []any{uint64(2)}})
	require.NoError(t, err)
	verifyConfig := lcommon.VerifyConfig{SkipBodyHashValidation: true}
	_, err = alonzo.NewAlonzoBlockFromCbor(raw, verifyConfig)
	require.ErrorContains(t, err, "outside transaction list")
	_, err = babbage.NewBabbageBlockFromCbor(raw, verifyConfig)
	require.ErrorContains(t, err, "outside transaction list")
	_, err = conway.NewConwayBlockFromCbor(raw, verifyConfig)
	require.ErrorContains(t, err, "outside transaction list")
}

func TestBabbageTransactionDecoderRejectsNegativeDeclaredExUnits(t *testing.T) {
	datumCbor, err := cbor.Encode(uint64(42))
	require.NoError(t, err)
	var datum lcommon.Datum
	_, err = cbor.Decode(datumCbor, &datum)
	require.NoError(t, err)
	for _, valid := range []bool{true, false} {
		for _, exUnits := range [][2]int64{{-1, 0}, {0, -1}, {0, 0}} {
			t.Run(fmt.Sprintf("isValid=%t memory=%d steps=%d", valid, exUnits[0], exUnits[1]), func(t *testing.T) {
				body := map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}
				witnesses := map[uint]any{
					5: []any{[]any{
						uint64(lcommon.RedeemerTagSpend),
						uint64(0),
						datum,
						[]any{exUnits[0], exUnits[1]},
					}},
				}
				raw, err := cbor.Encode([]any{body, witnesses, valid, nil})
				require.NoError(t, err)
				_, err = babbage.NewBabbageTransactionFromCbor(raw)
				if exUnits[0] < 0 || exUnits[1] < 0 {
					require.ErrorContains(t, err, "negative integer")
				} else {
					require.NoError(t, err, "a zero declaration remains valid")
				}
			})
		}
	}
}

func babbageOutputWithDatumHash(
	t *testing.T,
	address lcommon.Address,
	amount uint64,
	datumHash lcommon.Blake2b256,
) babbage.BabbageTransactionOutput {
	t.Helper()
	raw, err := cbor.Encode(map[uint]any{
		0: address,
		1: amount,
		2: []any{uint64(babbage.DatumOptionTypeHash), datumHash},
	})
	require.NoError(t, err)
	var output babbage.BabbageTransactionOutput
	_, err = cbor.Decode(raw, &output)
	require.NoError(t, err)
	return output
}

func testDatum(t *testing.T) (lcommon.Datum, lcommon.Blake2b256) {
	t.Helper()
	datumBytes, err := cbor.Encode(uint64(42))
	require.NoError(t, err)
	var datum lcommon.Datum
	_, err = cbor.Decode(datumBytes, &datum)
	require.NoError(t, err)
	return datum, datum.Hash()
}

func TestValidateTxConwaySupplementalDatumSources(t *testing.T) {
	datum, datumHash := testDatum(t)
	keyAddress := newTestKeyAddress(t)
	outputWithHash := func(address lcommon.Address) babbage.BabbageTransactionOutput {
		return babbageOutputWithDatumHash(t, address, 1_000_000, datumHash)
	}

	validate := func(
		t *testing.T,
		tx *conway.ConwayTransaction,
		state *mockLedgerState,
	) error {
		state.skipPhase2Validation = true
		return validateConwayWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			tx,
			state,
			&conway.ConwayProtocolParameters{},
		)
	}

	t.Run("key input does not justify a supplemental datum", func(t *testing.T) {
		input := shelley.NewShelleyTransactionInput(
			"1111111111111111111111111111111111111111111111111111111111111111",
			0,
		)
		state := newMockLedgerState()
		state.addUtxo(input, outputWithHash(keyAddress))
		tx := &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: conway.ConwayTransactionWitnessSet{
				WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, true),
			},
			TxIsValid: true,
		}
		var notAllowed lcommon.NotAllowedSupplementalDatumsError
		require.ErrorAs(t, validate(t, tx, state), &notAllowed)
	})

	t.Run("collateral return justifies its datum hash", func(t *testing.T) {
		tx := &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxCollateralReturn: ptrTo(outputWithHash(keyAddress)),
			},
			WitnessSet: conway.ConwayTransactionWitnessSet{
				WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, true),
			},
			TxIsValid: true,
		}
		require.NoError(t, validate(t, tx, newMockLedgerState()))
	})

	t.Run("ordinary output justifies its datum hash", func(t *testing.T) {
		output := outputWithHash(keyAddress)
		tx := &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxOutputs: []babbage.BabbageTransactionOutput{output},
			},
			WitnessSet: conway.ConwayTransactionWitnessSet{
				WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, true),
			},
			TxIsValid: true,
		}
		require.NoError(t, validate(t, tx, newMockLedgerState()))
	})

	t.Run("reference input justifies its datum hash", func(t *testing.T) {
		input := shelley.NewShelleyTransactionInput(
			"5555555555555555555555555555555555555555555555555555555555555555",
			0,
		)
		state := newMockLedgerState()
		state.addUtxo(input, outputWithHash(keyAddress))
		tx := &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxReferenceInputs: cbor.NewSetType([]shelley.ShelleyTransactionInput{input}, false),
			},
			WitnessSet: conway.ConwayTransactionWitnessSet{
				WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, true),
			},
			TxIsValid: true,
		}
		require.NoError(t, validate(t, tx, state))
	})

	t.Run("missing V1 spending datum fails on invalid transaction", func(t *testing.T) {
		input := shelley.NewShelleyTransactionInput(
			"2222222222222222222222222222222222222222222222222222222222222222",
			0,
		)
		script := lcommon.PlutusV1Script{0x41, 0x01}
		state := newMockLedgerState()
		state.addUtxo(input, outputWithHash(newTestScriptAddress(t, script)))
		tx := &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: conway.ConwayTransactionWitnessSet{
				WsPlutusV1Scripts: cbor.NewSetType([]lcommon.PlutusV1Script{script}, true),
				WsRedeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
				}},
			},
			TxIsValid: false,
		}
		var missing lcommon.MissingDatumForSpendingScriptError
		require.ErrorAs(t, validate(t, tx, state), &missing)

		tx.WitnessSet.WsPlutusData = cbor.NewSetType([]lcommon.Datum{datum}, true)
		require.NoError(t, validate(t, tx, state))
	})

	t.Run("native-script input does not justify a supplemental datum", func(t *testing.T) {
		input := shelley.NewShelleyTransactionInput(
			"3333333333333333333333333333333333333333333333333333333333333333",
			0,
		)
		nativeBytes, err := cbor.Encode(lcommon.NativeScriptAll{Type: 1})
		require.NoError(t, err)
		var native lcommon.NativeScript
		require.NoError(t, native.UnmarshalCBOR(nativeBytes))
		state := newMockLedgerState()
		state.addUtxo(input, outputWithHash(newTestScriptAddress(t, native)))
		tx := &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: conway.ConwayTransactionWitnessSet{
				WsNativeScripts: cbor.NewSetType([]lcommon.NativeScript{native}, true),
				WsPlutusData:    cbor.NewSetType([]lcommon.Datum{datum}, true),
			},
			TxIsValid: false,
		}
		var notAllowed lcommon.NotAllowedSupplementalDatumsError
		require.ErrorAs(t, validate(t, tx, state), &notAllowed)
	})

}

func TestValidateTxConwayRequiredSpendingDatumIgnoresValidityFlag(t *testing.T) {
	datum, datumHash := testDatum(t)
	input := shelley.NewShelleyTransactionInput(
		"7777777777777777777777777777777777777777777777777777777777777777",
		0,
	)
	for _, version := range []string{"V1", "V2"} {
		for _, valid := range []bool{true, false} {
			t.Run(fmt.Sprintf("Plutus %s isValid=%t", version, valid), func(t *testing.T) {
				var script lcommon.Script
				switch version {
				case "V1":
					script = lcommon.PlutusV1Script{0x41, 0x01}
				case "V2":
					script = lcommon.PlutusV2Script{0x41, 0x01}
				}
				scriptAddress := newTestScriptAddress(t, script)
				state := newMockLedgerState()
				state.skipPhase2Validation = true
				state.addUtxo(input, babbageOutputWithDatumHash(t, scriptAddress, 1_000_000, datumHash))
				witnesses := conway.ConwayTransactionWitnessSet{
					WsRedeemers: conway.ConwayRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
						{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
					}},
				}
				switch version {
				case "V1":
					witnesses.WsPlutusV1Scripts = cbor.NewSetType([]lcommon.PlutusV1Script{script.(lcommon.PlutusV1Script)}, true)
				case "V2":
					witnesses.WsPlutusV2Scripts = cbor.NewSetType([]lcommon.PlutusV2Script{script.(lcommon.PlutusV2Script)}, true)
				}
				tx := &conway.ConwayTransaction{
					Body: conway.ConwayTransactionBody{
						TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
					},
					WitnessSet: witnesses,
					TxIsValid:  valid,
				}
				var missing lcommon.MissingDatumForSpendingScriptError
				err := validateConwayWithRule(
					t,
					lcommon.UtxoValidationRuleSupplementalDatums,
					tx,
					state,
					&conway.ConwayProtocolParameters{},
				)
				require.ErrorAs(t, err, &missing)
				tx.WitnessSet.WsPlutusData = cbor.NewSetType([]lcommon.Datum{datum}, true)
				require.NoError(t, validateConwayWithRule(
					t,
					lcommon.UtxoValidationRuleSupplementalDatums,
					tx,
					state,
					&conway.ConwayProtocolParameters{},
				))
			})
		}
	}
}

func TestValidateTxDijkstraRequiredSpendingDatumsAtTopAndNestedLevels(t *testing.T) {
	datum, datumHash := testDatum(t)
	input := shelley.NewShelleyTransactionInput(
		"8888888888888888888888888888888888888888888888888888888888888888",
		0,
	)
	for _, version := range []string{"V1", "V2"} {
		var script lcommon.Script
		switch version {
		case "V1":
			script = lcommon.PlutusV1Script{0x41, 0x01}
		case "V2":
			script = lcommon.PlutusV2Script{0x41, 0x01}
		}
		address := newTestScriptAddress(t, script)
		newWitnesses := func() gdijkstra.DijkstraTransactionWitnessSet {
			witnesses := gdijkstra.DijkstraTransactionWitnessSet{
				WsRedeemers: gdijkstra.DijkstraRedeemers{Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {},
				}},
			}
			switch version {
			case "V1":
				witnesses.WsPlutusV1Scripts = cbor.NewSetType([]lcommon.PlutusV1Script{script.(lcommon.PlutusV1Script)}, true)
			case "V2":
				witnesses.WsPlutusV2Scripts = cbor.NewSetType([]lcommon.PlutusV2Script{script.(lcommon.PlutusV2Script)}, true)
			}
			return witnesses
		}

		for _, valid := range []bool{true, false} {
			t.Run(fmt.Sprintf("top-level Plutus %s isValid=%t", version, valid), func(t *testing.T) {
				state := newMockLedgerState()
				state.skipPhase2Validation = true
				state.addUtxo(input, babbageOutputWithDatumHash(t, address, 1_000_000, datumHash))
				tx := &gdijkstra.DijkstraTransaction{
					Body: gdijkstra.DijkstraTransactionBody{
						TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
					},
					WitnessSet: newWitnesses(),
					TxIsValid:  valid,
				}
				var missing lcommon.MissingDatumForSpendingScriptError
				err := validateDijkstraWithRule(
					t,
					lcommon.UtxoValidationRuleSupplementalDatums,
					tx,
					state,
					&gdijkstra.DijkstraProtocolParameters{},
				)
				require.ErrorAs(t, err, &missing)
				tx.WitnessSet.WsPlutusData = cbor.NewSetType([]lcommon.Datum{datum}, true)
				require.NoError(t, validateDijkstraWithRule(
					t,
					lcommon.UtxoValidationRuleSupplementalDatums,
					tx,
					state,
					&gdijkstra.DijkstraProtocolParameters{},
				))
			})
		}

		t.Run("nested Plutus "+version, func(t *testing.T) {
			state := newMockLedgerState()
			state.skipPhase2Validation = true
			state.addUtxo(input, babbageOutputWithDatumHash(t, address, 1_000_000, datumHash))
			child := gdijkstra.DijkstraSubTransaction{
				Body: gdijkstra.DijkstraSubTransactionBody{
					TxInputs: conway.NewConwayTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
				},
				WitnessSet: newWitnesses(),
			}
			tx := &gdijkstra.DijkstraTransaction{
				Body: gdijkstra.DijkstraTransactionBody{
					TxSubTransactions: cbor.NewSetType([]gdijkstra.DijkstraSubTransaction{child}, false),
				},
				TxIsValid: false,
			}
			var missing lcommon.MissingDatumForSpendingScriptError
			err := validateDijkstraWithRule(
				t,
				lcommon.UtxoValidationRuleSupplementalDatums,
				tx,
				state,
				&gdijkstra.DijkstraProtocolParameters{},
			)
			require.ErrorAs(t, err, &missing)
			child.WitnessSet.WsPlutusData = cbor.NewSetType([]lcommon.Datum{datum}, true)
			tx.Body.TxSubTransactions = cbor.NewSetType([]gdijkstra.DijkstraSubTransaction{child}, false)
			require.NoError(t, validateDijkstraWithRule(
				t,
				lcommon.UtxoValidationRuleSupplementalDatums,
				tx,
				state,
				&gdijkstra.DijkstraProtocolParameters{},
			))
		})
	}
}

func TestValidateTxBabbageCollateralReturnDatumHashJustifiesWitnessDatum(t *testing.T) {
	datum, datumHash := testDatum(t)
	output := babbageOutputWithDatumHash(
		t,
		newTestKeyAddress(t),
		1_000_000,
		datumHash,
	)
	state := newMockLedgerState()
	state.skipPhase2Validation = true
	tx := &babbage.BabbageTransaction{
		Body: babbage.BabbageTransactionBody{
			TxCollateralReturn: &output,
		},
		WitnessSet: babbage.BabbageTransactionWitnessSet{
			WsPlutusData: alonzo.PlutusDataList{Items: []lcommon.Datum{datum}},
		},
		TxIsValid: true,
	}
	require.NoError(t, validateBabbageWithRule(
		t,
		lcommon.UtxoValidationRuleSupplementalDatums,
		tx,
		state,
		&babbage.BabbageProtocolParameters{},
	))
}

func TestValidateTxAlonzoAndBabbageDatumSetRules(t *testing.T) {
	datum, datumHash := testDatum(t)
	input := shelley.NewShelleyTransactionInput(
		"4444444444444444444444444444444444444444444444444444444444444444",
		0,
	)
	script := lcommon.PlutusV1Script{0x41, 0x01}

	t.Run("Alonzo unrelated datum and required spending datum", func(t *testing.T) {
		keyState := newMockLedgerState()
		keyState.addUtxo(input, alonzo.AlonzoTransactionOutput{
			OutputAddress:   newTestKeyAddress(t),
			OutputAmount:    mary.MaryTransactionOutputValue{Amount: 1_000_000},
			OutputDatumHash: &datumHash,
		})
		unrelated := &alonzo.AlonzoTransaction{
			Body: alonzo.AlonzoTransactionBody{
				TxInputs: shelley.NewShelleyTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: alonzo.AlonzoTransactionWitnessSet{
				WsPlutusData: alonzo.PlutusDataList{Items: []lcommon.Datum{datum}},
			},
			TxIsValid: true,
		}
		var notAllowed lcommon.NotAllowedSupplementalDatumsError
		err := validateAlonzoWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			unrelated,
			keyState,
			&alonzo.AlonzoProtocolParameters{
				CostModels: map[uint][]int64{
					0: syntheticFullCostModel(t, lang.LanguageVersionV1),
				},
			},
		)
		require.ErrorAs(t, err, &notAllowed)

		scriptState := newMockLedgerState()
		scriptState.addUtxo(input, alonzo.AlonzoTransactionOutput{
			OutputAddress:   newTestScriptAddress(t, script),
			OutputAmount:    mary.MaryTransactionOutputValue{Amount: 1_000_000},
			OutputDatumHash: &datumHash,
		})
		spending := &alonzo.AlonzoTransaction{
			Body: alonzo.AlonzoTransactionBody{
				TxInputs: shelley.NewShelleyTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: alonzo.AlonzoTransactionWitnessSet{
				WsPlutusV1Scripts: []lcommon.PlutusV1Script{script},
				WsRedeemers:       babbageRedeemers(),
			},
			TxIsValid: false,
		}
		var missing lcommon.MissingDatumForSpendingScriptError
		err = validateAlonzoWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			spending,
			scriptState,
			&alonzo.AlonzoProtocolParameters{
				CostModels: map[uint][]int64{
					0: syntheticFullCostModel(t, lang.LanguageVersionV1),
				},
			},
		)
		require.ErrorAs(t, err, &missing)
		spending.WitnessSet.WsPlutusData = alonzo.PlutusDataList{Items: []lcommon.Datum{datum}}
		require.NoError(t, validateAlonzoWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			spending,
			scriptState,
			&alonzo.AlonzoProtocolParameters{
				CostModels: map[uint][]int64{
					0: syntheticFullCostModel(t, lang.LanguageVersionV1),
				},
			},
		))
	})

	t.Run("Babbage unrelated datum and required spending datum", func(t *testing.T) {
		keyState := newMockLedgerState()
		keyState.addUtxo(input, babbageOutputWithDatumHash(
			t,
			newTestKeyAddress(t),
			1_000_000,
			datumHash,
		))
		unrelated := &babbage.BabbageTransaction{
			Body: babbage.BabbageTransactionBody{
				TxInputs: shelley.NewShelleyTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: babbage.BabbageTransactionWitnessSet{
				WsPlutusData: alonzo.PlutusDataList{Items: []lcommon.Datum{datum}},
			},
			TxIsValid: true,
		}
		var notAllowed lcommon.NotAllowedSupplementalDatumsError
		err := validateBabbageWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			unrelated,
			keyState,
			&babbage.BabbageProtocolParameters{
				CostModels: map[uint][]int64{
					0: syntheticFullCostModel(t, lang.LanguageVersionV1),
				},
			},
		)
		require.ErrorAs(t, err, &notAllowed)

		scriptState := newMockLedgerState()
		scriptState.addUtxo(input, babbageOutputWithDatumHash(
			t,
			newTestScriptAddress(t, script),
			1_000_000,
			datumHash,
		))
		spending := &babbage.BabbageTransaction{
			Body: babbage.BabbageTransactionBody{
				TxInputs: shelley.NewShelleyTransactionInputSet([]shelley.ShelleyTransactionInput{input}),
			},
			WitnessSet: babbage.BabbageTransactionWitnessSet{
				WsPlutusV1Scripts: []lcommon.PlutusV1Script{script},
				WsRedeemers:       babbageRedeemers(),
			},
			TxIsValid: false,
		}
		var missing lcommon.MissingDatumForSpendingScriptError
		err = validateBabbageWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			spending,
			scriptState,
			&babbage.BabbageProtocolParameters{
				CostModels: map[uint][]int64{
					0: syntheticFullCostModel(t, lang.LanguageVersionV1),
				},
			},
		)
		require.ErrorAs(t, err, &missing)
		spending.WitnessSet.WsPlutusData = alonzo.PlutusDataList{Items: []lcommon.Datum{datum}}
		require.NoError(t, validateBabbageWithRule(
			t,
			lcommon.UtxoValidationRuleSupplementalDatums,
			spending,
			scriptState,
			&babbage.BabbageProtocolParameters{
				CostModels: map[uint][]int64{
					0: syntheticFullCostModel(t, lang.LanguageVersionV1),
				},
			},
		))
	})
}

func ptrTo[T any](value T) *T { return &value }
