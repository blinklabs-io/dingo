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

package ledger

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/builtin"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

const blockExplicitStakeAmount = int64(2_000_000)

// errBlockTxInfoEvaluated stops block application once the Plutus evaluation
// has passed, so a passing case is distinguishable from a failing one without
// depending on certificate state mutation.
var errBlockTxInfoEvaluated = errors.New("plutus evaluation passed")

func blockV3CertificateBlock(
	t *testing.T,
	plutusScript lcommon.PlutusV3Script,
	certificate lcommon.CertificateWrapper,
	major uint,
	slot uint64,
) (*conway.ConwayBlock, *database.BlockIngestionResult) {
	t.Helper()
	tx := conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{certificate},
		},
		WitnessSet: conway.ConwayTransactionWitnessSet{
			WsPlutusV3Scripts: cbor.NewSetType(
				[]lcommon.PlutusV3Script{plutusScript}, false,
			),
			WsRedeemers: conway.ConwayRedeemers{
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
	txCbor, err := cbor.Encode(&tx)
	require.NoError(t, err)
	decoded, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{},
		TransactionBodies: []conway.ConwayTransactionBody{
			decoded.Body,
		},
		TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{
			decoded.WitnessSet,
		},
	}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.ProtoVersion.Major = uint64(major)
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	var txHash [32]byte
	copy(txHash[:], block.Transactions()[0].Hash().Bytes())
	return block, &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHash: {BlockSlot: slot, ByteLength: uint32(len(txCbor))}, // #nosec G115
		},
	}
}

// TestLedgerProcessBlockConwayV3TxInfoFollowsBlockProtocolVersion applies a
// block whose only transaction carries a Plutus V3 script that serialises the
// explicit deposit or refund option from its own certificate. The protocol
// version handed to block application decides whether that option is Nothing
// (PV9) or Just the amount (PV10 and later). Live apply and replay share the
// same call, differing only in the reachesTip argument.
//
// The era validator is replaced by a wrapper that records the protocol
// parameters block application supplied and runs the real Conway evaluation
// with them. Phase-1 rules are not exercised: they need a signed, balanced
// transaction that says nothing about the TxInfo version boundary.
func TestLedgerProcessBlockConwayV3TxInfoFollowsBlockProtocolVersion(
	t *testing.T,
) {
	t.Parallel()

	explicit := data.NewConstr(
		0,
		data.NewInteger(big.NewInt(blockExplicitStakeAmount)),
	)
	nothing := data.NewConstr(1)
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
						Amount:          blockExplicitStakeAmount,
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
						Amount:          blockExplicitStakeAmount,
					},
				}
			},
		},
	} {
		for _, scriptExpectation := range []struct {
			name   string
			option data.PlutusData
			// passesAt reports whether the script passes at a major version.
			passesAt func(major uint) bool
		}{
			{
				name:   "script expects Nothing",
				option: nothing,
				passesAt: func(major uint) bool {
					return major == lcommon.ProtocolVersionConway
				},
			},
			{
				name:   "script expects Just amount",
				option: explicit,
				passesAt: func(major uint) bool {
					return major >= lcommon.ProtocolVersionPlomin
				},
			},
		} {
			for _, major := range []uint{
				lcommon.ProtocolVersionConway,
				lcommon.ProtocolVersionPlomin,
				lcommon.ProtocolVersionVanRossem,
			} {
				for _, reachesTip := range []bool{false, true} {
					name := fmt.Sprintf(
						"%s/%s/PV%d",
						certificateCase.name,
						scriptExpectation.name,
						major,
					)
					if reachesTip {
						name += "/live"
					} else {
						name += "/replay"
					}
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						plutusScript := lcommon.PlutusV3Script(
							blockV3StakeAmountObserver(
								t, scriptExpectation.option,
							),
						)
						certificate := certificateCase.certificate(
							lcommon.Credential{
								CredType:   lcommon.CredentialTypeScriptHash,
								Credential: plutusScript.Hash(),
							},
						)
						const slot = uint64(10)
						block, offsets := blockV3CertificateBlock(
							t, plutusScript, certificate, major, slot,
						)
						pparams := &conway.ConwayProtocolParameters{
							ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
								Major: major,
							},
							CostModels: map[uint][]int64{
								2: blockV3MachineCostModel(
									t, lang.LanguageVersionV3,
								),
							},
							MaxBlockBodySize:   100_000,
							MaxBlockHeaderSize: 100_000,
							MaxTxExUnits: lcommon.ExUnits{
								Memory: 10_000_000, Steps: 100_000_000,
							},
							MaxBlockExUnits: lcommon.ExUnits{
								Memory: 50_000_000, Steps: 500_000_000,
							},
						}
						var gotMajor uint
						called := false
						testEra := eras.ConwayEraDesc
						testEra.ValidateTxFunc = func(
							tx lcommon.Transaction,
							_ uint64,
							view lcommon.LedgerState,
							pp lcommon.ProtocolParameters,
						) error {
							called = true
							conwayPP, ok := pp.(*conway.ConwayProtocolParameters)
							require.True(t, ok)
							gotMajor = conwayPP.ProtocolVersion.Major
							_, _, _, err := eras.EvaluateTxConway(tx, view, pp)
							if err != nil {
								return err
							}
							return errBlockTxInfoEvaluated
						}
						ls := newRequiredDatumLedger(t, newTestDB(t), testEra)
						err := applyRequiredDatumBlock(
							ls, block, slot, reachesTip, offsets,
							testEra, pparams,
						)
						require.True(t, called, "validator not reached: %v", err)
						require.Equal(t, major, gotMajor)
						if scriptExpectation.passesAt(major) {
							require.ErrorIs(t, err, errBlockTxInfoEvaluated)
							return
						}
						require.Error(t, err)
						require.NotErrorIs(t, err, errBlockTxInfoEvaluated)
					})
				}
			}
		}
	}
}

var blockMachineCostMachineCosts = map[string][2]int64{
	"cekStartupCost": {100, 100},
	"cekVarCost":     {16000, 100},
	"cekConstCost":   {16000, 100},
	"cekLamCost":     {16000, 100},
	"cekDelayCost":   {16000, 100},
	"cekForceCost":   {16000, 100},
	"cekApplyCost":   {16000, 100},
	"cekBuiltinCost": {16000, 100},
	"cekConstrCost":  {16000, 100},
	"cekCaseCost":    {16000, 100},
}

// blockV3MachineCostModel returns a complete cost model for the given
// version that reproduces plutigo's real DefaultMachineCosts for every CEK
// machine-step parameter, and a placeholder for every builtin-function cost
// parameter. A test whose script never invokes an actual Plutus builtin
// function (only constants/lambdas/application, no e.g. addInteger) gets
// the exact same evaluated cost plutigo's empty-cost-model fallback used to
// silently produce -- matching known-good, externally-verified reference
// numbers -- while still supplying requiredCostModel a complete,
// non-fallback-triggering list. A script that does invoke a builtin
// function needs its own model with real values for that builtin, since
// this helper's builtin-cost entries are placeholders.
func blockV3MachineCostModel(
	t testing.TB,
	version lang.LanguageVersion,
) []int64 {
	t.Helper()
	names := lang.GetParamNamesForVersion(version)
	model := make([]int64, len(names))
	for i, name := range names {
		prefix, suffix, ok := strings.Cut(name, "-")
		if costs, isMachineCost := blockMachineCostMachineCosts[prefix]; ok &&
			isMachineCost {
			switch suffix {
			case "exBudgetCPU":
				model[i] = costs[0]
				continue
			case "exBudgetMemory":
				model[i] = costs[1]
				continue
			}
		}
		// Builtin-function cost parameter: callers use scripts that never
		// invoke an actual builtin, so this value doesn't affect the
		// evaluated cost -- any valid placeholder works.
		model[i] = 100
	}
	return model
}

func blockV3StakeAmountObserver(
	t *testing.T,
	expectedOption data.PlutusData,
) []byte {
	t.Helper()
	expected, err := data.Encode(expectedOption)
	require.NoError(t, err)
	context := syn.Term[syn.DeBruijn](&syn.Var[syn.DeBruijn]{Name: 1})
	contextFields := blockV3SndPair(blockV3UnConstrData(context))
	txInfo := blockV3HeadList(contextFields)
	txInfoFields := blockV3SndPair(blockV3UnConstrData(txInfo))
	certificatesData := blockV3HeadList(blockV3TailList(txInfoFields, 5))
	certificates := blockV3UnListData(certificatesData)
	certificate := blockV3HeadList(certificates)
	certificateFields := blockV3SndPair(blockV3UnConstrData(certificate))
	amountOption := blockV3HeadList(blockV3TailList(certificateFields, 1))
	serializedOption := blockV3Apply(builtin.SerialiseData, amountOption)
	equal := blockV3Apply(
		builtin.EqualsByteString,
		serializedOption,
		&syn.Constant{Con: &syn.ByteString{Inner: expected}},
	)
	result := blockV3Apply(
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

func blockV3UnConstrData(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return blockV3Apply(builtin.UnConstrData, term)
}

func blockV3SndPair(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return blockV3Apply(builtin.SndPair, term)
}

func blockV3HeadList(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return blockV3Apply(builtin.HeadList, term)
}

func blockV3UnListData(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return blockV3Apply(builtin.UnListData, term)
}

func blockV3TailList(
	term syn.Term[syn.DeBruijn],
	count int,
) syn.Term[syn.DeBruijn] {
	for range count {
		term = blockV3Apply(builtin.TailList, term)
	}
	return term
}

func blockV3Apply(
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
