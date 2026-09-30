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
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// TestByronTxOutInPlutusContext pins the per-era treatment of a Byron TxOut
// in a Plutus script context through the production validators. Alonzo drops
// a Byron input or output from a V1 TxInfo. Babbage and Conway reject it for
// every language version with ByronTxOutInContext, and that context error
// must surface for an invalid transaction too rather than count as an
// expected phase-2 failure.
// Not t.Parallel: this test temporarily replaces package-level rule slices.
func TestByronTxOutInPlutusContext(t *testing.T) {
	originalAlonzoRules := alonzoUtxoValidationRules
	alonzoUtxoValidationRules = nil
	t.Cleanup(func() { alonzoUtxoValidationRules = originalAlonzoRules })
	originalBabbageRules := babbageUtxoValidationRules
	babbageUtxoValidationRules = nil
	t.Cleanup(func() { babbageUtxoValidationRules = originalBabbageRules })
	withoutConwayUtxoValidationRules(t)

	byronAddr := newTestByronAddress(t, lcommon.ByronAddressTypePubkey, 0x42)
	maxTxExUnits := lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000}
	modelIndex := map[lang.LanguageVersion]uint{
		lang.LanguageVersionV1: 0,
		lang.LanguageVersionV2: 1,
		lang.LanguageVersionV3: 2,
	}

	type validateFunc func(
		t *testing.T,
		tx *mockProducedValidityTx,
		ls *mockLedgerState,
		version lang.LanguageVersion,
	) error
	validators := []struct {
		era       string
		versions  []lang.LanguageVersion
		validate  validateFunc
		rejectAny bool
	}{
		{
			era:      "Alonzo",
			versions: []lang.LanguageVersion{lang.LanguageVersionV1},
			validate: func(t *testing.T, tx *mockProducedValidityTx, ls *mockLedgerState, version lang.LanguageVersion) error {
				return ValidateTxAlonzo(tx, 0, ls, &alonzo.AlonzoProtocolParameters{
					ProtocolMajor: 5,
					CostModels: map[uint][]int64{
						0: defaultMachineCostModel(t, version),
					},
					MaxTxExUnits: maxTxExUnits,
				})
			},
		},
		{
			era: "Babbage",
			versions: []lang.LanguageVersion{
				lang.LanguageVersionV1, lang.LanguageVersionV2,
			},
			rejectAny: true,
			validate: func(t *testing.T, tx *mockProducedValidityTx, ls *mockLedgerState, _ lang.LanguageVersion) error {
				return ValidateTxBabbage(tx, 0, ls, &babbage.BabbageProtocolParameters{
					ProtocolMajor: 7,
					CostModels: map[uint][]int64{
						0: defaultMachineCostModel(t, lang.LanguageVersionV1),
						1: defaultMachineCostModel(t, lang.LanguageVersionV2),
					},
					MaxTxExUnits: maxTxExUnits,
				})
			},
		},
		{
			era: "Conway",
			versions: []lang.LanguageVersion{
				lang.LanguageVersionV1,
				lang.LanguageVersionV2,
				lang.LanguageVersionV3,
			},
			rejectAny: true,
			validate: func(t *testing.T, tx *mockProducedValidityTx, ls *mockLedgerState, version lang.LanguageVersion) error {
				return ValidateTxConway(tx, 0, ls, &conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: lcommon.ProtocolVersionVanRossem,
					},
					CostModels: map[uint][]int64{
						modelIndex[version]: defaultMachineCostModel(t, version),
					},
					MaxTxExUnits: maxTxExUnits,
				})
			},
		},
	}

	for _, v := range validators {
		for _, version := range v.versions {
			for _, placement := range []string{"input", "output"} {
				for _, valid := range []bool{true, false} {
					name := v.era + "/" + fmt.Sprintf("V%d", modelIndex[version]+1) + "/" + placement
					if !valid {
						name += "/invalid"
					}
					t.Run(name, func(t *testing.T) {
						// The script succeeds exactly when the context holds
						// no output, which is the case only when a Byron
						// output is dropped from the context.
						outputs := []lcommon.TransactionOutput{newTestOutput(1_000_000)}
						script := txInfoOutputCountScript(t, version, 1, false)
						if placement == "output" {
							outputs = []lcommon.TransactionOutput{
								newTestOutputWithAddress(1_000_000, byronAddr),
							}
							script = txInfoOutputCountScript(t, version, 1, true)
						}
						tx, input := newContextMintTx(t, script, valid, outputs, nil, nil)
						ls := newMockLedgerState()
						spent := lcommon.TransactionOutput(newTestOutput(10_000_000))
						if placement == "input" {
							spent = newTestOutputWithAddress(10_000_000, byronAddr)
						}
						ls.addUtxo(input, spent)

						if v.era == "Alonzo" {
							// The Alonzo validator reads each redeemer's
							// budget through Value rather than Iter.
							redeemers := tx.Witnesses().Redeemers().(*mockRedeemers)
							redeemers.valueOverride = &lcommon.RedeemerValue{
								ExUnits: lcommon.ExUnits{Memory: 5_000_000, Steps: 50_000_000},
							}
						}
						err := v.validate(t, tx, ls, version)
						if v.rejectAny {
							var ctxErr conway.ScriptContextConstructionError
							require.ErrorAs(t, err, &ctxErr)
							require.ErrorContains(t, err, "Byron TxOut")
							return
						}
						if valid {
							require.NoError(t, err)
						} else {
							require.ErrorContains(
								t, err,
								"transaction declared invalid but Plutus scripts succeeded",
							)
						}
					})
				}
			}
		}
	}
}
