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

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/builtin"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

const conwayExplicitStakeAmount = int64(2_000_000)

// TestConwayTxInfoV3UsesProtocolVersionForExplicitStakeAmounts verifies the
// PV9/PV10 TxInfo boundary with a V3 script that serialises the translated
// deposit/refund option. Each transaction and script is evaluated at each
// protocol version to ensure the active version controls the context.
// Not t.Parallel: this test swaps the package-level Conway validation rules.
func TestConwayTxInfoV3UsesProtocolVersionForExplicitStakeAmounts(
	t *testing.T,
) {
	withoutConwayUtxoValidationRules(t)
	for _, certificateCase := range []struct {
		name        string
		certificate func(lcommon.Credential) lcommon.Certificate
	}{
		{
			name: "registration deposit",
			certificate: func(credential lcommon.Credential) lcommon.Certificate {
				return &lcommon.RegistrationCertificate{
					CertType:        uint(lcommon.CertificateTypeRegistration),
					StakeCredential: credential,
					Amount:          conwayExplicitStakeAmount,
				}
			},
		},
		{
			name: "deregistration refund",
			certificate: func(credential lcommon.Credential) lcommon.Certificate {
				return &lcommon.DeregistrationCertificate{
					CertType:        uint(lcommon.CertificateTypeDeregistration),
					StakeCredential: credential,
					Amount:          conwayExplicitStakeAmount,
				}
			},
		},
	} {
		t.Run(certificateCase.name, func(t *testing.T) {
			for _, scriptExpectation := range []struct {
				name   string
				major  uint
				option data.PlutusData
			}{
				{
					name:   "script expects PV9 Nothing",
					major:  lcommon.ProtocolVersionConway,
					option: data.NewConstr(1),
				},
				{
					name:  "script expects PV10 Just amount",
					major: lcommon.ProtocolVersionPlomin,
					option: data.NewConstr(
						0,
						data.NewInteger(big.NewInt(conwayExplicitStakeAmount)),
					),
				},
			} {
				t.Run(scriptExpectation.name, func(t *testing.T) {
					plutusScript := lcommon.PlutusV3Script(
						conwayV3StakeAmountObserver(
							t,
							scriptExpectation.option,
						),
					)
					certificate := certificateCase.certificate(
						lcommon.Credential{
							CredType:   lcommon.CredentialTypeScriptHash,
							Credential: plutusScript.Hash(),
						},
					)
					tx := conwayTxInfoCertificateTx(
						plutusScript,
						certificate,
					)

					for _, activeVersion := range []struct {
						name  string
						major uint
					}{
						{name: "PV9", major: lcommon.ProtocolVersionConway},
						{name: "PV10", major: lcommon.ProtocolVersionPlomin},
						{name: "PV11", major: lcommon.ProtocolVersionVanRossem},
					} {
						t.Run(activeVersion.name, func(t *testing.T) {
							pp := &conway.ConwayProtocolParameters{
								ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
									Major: activeVersion.major,
								},
								CostModels: map[uint][]int64{
									2: defaultMachineCostModel(
										t,
										lang.LanguageVersionV3,
									),
								},
								MaxTxExUnits: lcommon.ExUnits{
									Memory: 10_000_000,
									Steps:  100_000_000,
								},
							}
							shouldPass := activeVersion.major >= lcommon.ProtocolVersionPlomin
							if scriptExpectation.major == lcommon.ProtocolVersionConway {
								shouldPass = activeVersion.major == lcommon.ProtocolVersionConway
							}
							if shouldPass {
								require.NoError(t, ValidateTxConway(
									tx, 0, newMockLedgerState(), pp,
								))
								_, _, _, err := EvaluateTxConway(
									tx, newMockLedgerState(), pp,
								)
								require.NoError(t, err)
								return
							}
							err := ValidateTxConway(
								tx, 0, newMockLedgerState(), pp,
							)
							require.Error(t, err)
							_, _, _, err = EvaluateTxConway(
								tx, newMockLedgerState(), pp,
							)
							require.Error(t, err)
						})
					}
				})
			}
		})
	}
}

func conwayTxInfoCertificateTx(
	plutusScript lcommon.PlutusV3Script,
	certificate lcommon.Certificate,
) *mockConwayFeeTxV3 {
	return &mockConwayFeeTxV3{
		mockConwayFeeTx: mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType: txTypeAlonzo,
				fee:    big.NewInt(0),
				witnesses: &mockWitnessSet{
					plutusV3Scripts: []lcommon.PlutusV3Script{plutusScript},
					redeemers: &mockRedeemers{entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagCert,
								Index: 0,
							},
							val: lcommon.RedeemerValue{
								Data: lcommon.Datum{Data: data.NewConstr(0)},
								ExUnits: lcommon.ExUnits{
									Memory: 5_000_000,
									Steps:  50_000_000,
								},
							},
						},
					}},
				},
			},
			certificates: []lcommon.Certificate{certificate},
		},
	}
}

func conwayV3StakeAmountObserver(
	t *testing.T,
	expectedOption data.PlutusData,
) []byte {
	t.Helper()
	expected, err := data.Encode(expectedOption)
	require.NoError(t, err)
	context := syn.Term[syn.DeBruijn](&syn.Var[syn.DeBruijn]{Name: 1})
	contextFields := conwayV3SndPair(conwayV3UnConstrData(context))
	txInfo := conwayV3HeadList(contextFields)
	txInfoFields := conwayV3SndPair(conwayV3UnConstrData(txInfo))
	certificatesData := conwayV3HeadList(conwayV3TailList(txInfoFields, 5))
	certificates := conwayV3UnListData(certificatesData)
	certificate := conwayV3HeadList(certificates)
	certificateFields := conwayV3SndPair(conwayV3UnConstrData(certificate))
	amountOption := conwayV3HeadList(conwayV3TailList(certificateFields, 1))
	serializedOption := conwayV3Apply(builtin.SerialiseData, amountOption)
	equal := conwayV3Apply(
		builtin.EqualsByteString,
		serializedOption,
		&syn.Constant{Con: &syn.ByteString{Inner: expected}},
	)
	result := conwayV3Apply(
		builtin.IfThenElse,
		equal,
		&syn.Delay[syn.DeBruijn]{Term: &syn.Constant{Con: &syn.Unit{}}},
		&syn.Delay[syn.DeBruijn]{Term: &syn.Error{}},
	)
	result = &syn.Force[syn.DeBruijn]{Term: result}
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: result},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	return scriptBytes
}

func conwayV3UnConstrData(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.UnConstrData, term)
}

func conwayV3SndPair(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.SndPair, term)
}

func conwayV3HeadList(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.HeadList, term)
}

func conwayV3UnListData(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.UnListData, term)
}

func conwayV3TailList(
	term syn.Term[syn.DeBruijn],
	count int,
) syn.Term[syn.DeBruijn] {
	for range count {
		term = conwayV3Apply(builtin.TailList, term)
	}
	return term
}

func conwayV3Apply(
	function builtin.DefaultFunction,
	args ...syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	var term syn.Term[syn.DeBruijn] = &syn.Builtin{DefaultFunction: function}
	for range function.ForceCount() {
		term = &syn.Force[syn.DeBruijn]{Term: term}
	}
	for _, arg := range args {
		term = &syn.Apply[syn.DeBruijn]{Function: term, Argument: arg}
	}
	return term
}
