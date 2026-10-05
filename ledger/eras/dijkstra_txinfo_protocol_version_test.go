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
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// TestValidateTxDijkstraV3ExposesExplicitStakeAmounts is the Dijkstra
// counterpart of the Conway protocol-version test: at the Dijkstra protocol
// version a PlutusV3 script sees the explicit registration deposit and
// deregistration refund as Just amount, not Nothing.
// Not t.Parallel: this test swaps the package-level Dijkstra rule list.
func TestValidateTxDijkstraV3ExposesExplicitStakeAmounts(t *testing.T) {
	original := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = nil
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = original })

	pp := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				2: defaultMachineCostModel(t, lang.LanguageVersionV3),
			},
			MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
		},
	}
	explicit := data.NewConstr(
		0,
		data.NewInteger(big.NewInt(conwayExplicitStakeAmount)),
	)
	for _, certificateCase := range []struct {
		name        string
		certificate func(lcommon.Credential) lcommon.CertificateWrapper
	}{
		{
			name: "registration deposit",
			certificate: func(c lcommon.Credential) lcommon.CertificateWrapper {
				return lcommon.CertificateWrapper{
					Type: uint(lcommon.CertificateTypeRegistration),
					Certificate: &lcommon.RegistrationCertificate{
						CertType:        uint(lcommon.CertificateTypeRegistration),
						StakeCredential: c,
						Amount:          conwayExplicitStakeAmount,
					},
				}
			},
		},
		{
			name: "deregistration refund",
			certificate: func(c lcommon.Credential) lcommon.CertificateWrapper {
				return lcommon.CertificateWrapper{
					Type: uint(lcommon.CertificateTypeDeregistration),
					Certificate: &lcommon.DeregistrationCertificate{
						CertType:        uint(lcommon.CertificateTypeDeregistration),
						StakeCredential: c,
						Amount:          conwayExplicitStakeAmount,
					},
				}
			},
		},
	} {
		for _, scriptExpectation := range []struct {
			name   string
			option data.PlutusData
			passes bool
		}{
			{name: "script expects Just amount", option: explicit, passes: true},
			{name: "script expects Nothing", option: data.NewConstr(1)},
		} {
			t.Run(certificateCase.name+"/"+scriptExpectation.name, func(t *testing.T) {
				plutusScript := lcommon.PlutusV3Script(
					conwayV3StakeAmountObserver(t, scriptExpectation.option),
				)
				tx := &gdijkstra.DijkstraTransaction{
					Body: gdijkstra.DijkstraTransactionBody{
						TxCertificates: []lcommon.CertificateWrapper{
							certificateCase.certificate(lcommon.Credential{
								CredType:   lcommon.CredentialTypeScriptHash,
								Credential: plutusScript.Hash(),
							}),
						},
					},
					WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
						WsPlutusV3Scripts: cbor.NewSetType(
							[]lcommon.PlutusV3Script{plutusScript}, false,
						),
						WsRedeemers: gdijkstra.DijkstraRedeemers{
							Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
								{Tag: lcommon.RedeemerTagCert, Index: 0}: {
									Data: lcommon.Datum{Data: data.NewConstr(0)},
									ExUnits: lcommon.ExUnits{
										Memory: 5_000_000,
										Steps:  50_000_000,
									},
								},
							},
						},
					},
					TxIsValid: true,
				}
				err := ValidateTxDijkstra(tx, 0, newMockLedgerState(), pp)
				if scriptExpectation.passes {
					require.NoError(t, err)
					return
				}
				var scriptErr conway.PlutusScriptFailedError
				require.ErrorAs(t, err, &scriptErr)
			})
		}
	}
}
