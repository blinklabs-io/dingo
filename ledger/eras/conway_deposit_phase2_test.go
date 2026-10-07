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
	"crypto/ed25519"
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

const (
	phase2DepositScriptInput = 1_000_000_000
	phase2DepositFee         = 2_100_000
	phase2DepositCollateral  = 3_200_000
	phase2DepositGovAction   = 100_000_000
	phase2DepositDRepRefund  = 2_000_000
)

// phase2DepositLedgerState reports one registered DRep and treats every reward
// account as registered, which mockLedgerState does not model.
type phase2DepositLedgerState struct {
	*mockLedgerState
	drep            *lcommon.DRepRegistration
	stakeRegistered bool
}

func (s phase2DepositLedgerState) DRepRegistration(
	cred lcommon.Credential,
) (*lcommon.DRepRegistration, error) {
	if s.drep != nil && s.drep.Credential.CredType == cred.CredType &&
		s.drep.Credential.Credential == cred.Credential {
		return s.drep, nil
	}
	return nil, nil
}

func (s phase2DepositLedgerState) IsStakeCredentialRegistered(
	_ lcommon.Credential,
) bool {
	return s.stakeRegistered
}

func (s phase2DepositLedgerState) IsRewardAccountRegistered(
	_ lcommon.Credential,
) bool {
	return true
}

// phase2DepositFailingScript is a PlutusV3 validator that takes the script
// context and evaluates to error.
func phase2DepositFailingScript(t *testing.T) lcommon.PlutusV3Script {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: [3]uint32{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: &syn.Error{}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flat)
	require.NoError(t, err)
	return lcommon.PlutusV3Script(scriptBytes)
}

type phase2DepositShape struct {
	isValid   bool
	certs     []lcommon.CertificateWrapper
	proposals []conway.ConwayProposalProcedure
	// delta is added to the script input to give the single output amount:
	// negative for deposits the transaction pays, positive for refunds it
	// claims.
	delta int64
}

// newPhase2DepositTx builds a transaction with a script whose outcome matches
// shape.isValid, valid collateral, and a body balanced under shape amounts.
func newPhase2DepositTx(
	t *testing.T,
	shape phase2DepositShape,
) (*conway.ConwayTransaction, *mockLedgerState, *conway.ConwayProtocolParameters, lcommon.Credential) {
	t.Helper()
	plutusScript := phase2DepositFailingScript(t)
	if shape.isValid {
		flat, err := syn.Encode(
			&syn.Program[syn.DeBruijn]{
				Version: [3]uint32{1, 1, 0},
				Term: &syn.Lambda[syn.DeBruijn]{
					Body: &syn.Constant{Con: &syn.Unit{}},
				},
			},
		)
		require.NoError(t, err)
		scriptBytes, err := cbor.Encode(flat)
		require.NoError(t, err)
		plutusScript = lcommon.PlutusV3Script(scriptBytes)
	}
	scriptInput := shelley.NewShelleyTransactionInput(
		"f228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
		0,
	)
	collateralInput := shelley.NewShelleyTransactionInput(
		"f328b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee99",
		0,
	)
	datum, datumHash := referenceOverlapDatum(t)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x4c
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyHash := lcommon.Blake2b224Hash(publicKey)
	collateralAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		keyHash[:],
		nil,
	)
	require.NoError(t, err)
	state := newMockLedgerState()
	state.networkId = uint(lcommon.AddressNetworkTestnet)
	state.addUtxo(scriptInput, referenceOverlapScriptOutput{
		testAddressScriptOutput: testAddressScriptOutput{
			testOutput: newTestOutput(phase2DepositScriptInput),
			addr:       newTestScriptAddress(t, plutusScript),
		},
		datumHash: &datumHash,
	})
	state.addUtxo(collateralInput, testAddressOutput{
		testOutput: newTestOutput(phase2DepositCollateral),
		addr:       collateralAddress,
	})
	params := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
		},
		KeyDeposit:                 depositTestKeyDeposit,
		DRepDeposit:                depositTestDRepDeposit,
		GovActionDeposit:           phase2DepositGovAction,
		MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(1, 1)},
		MaxTxSize:                  16_384,
		MaxValueSize:               5_000,
		MaxTxExUnits: lcommon.ExUnits{
			Steps:  1_000_000,
			Memory: 1_000_000,
		},
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1)},
		},
		CostModels: map[uint][]int64{
			2: defaultMachineCostModel(t, lang.LanguageVersionV3),
		},
	}
	outputAmount := int64(
		phase2DepositScriptInput-phase2DepositFee,
	) + shape.delta
	require.Positive(t, outputAmount)
	tx := &conway.ConwayTransaction{
		TxIsValid: shape.isValid,
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{scriptInput},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: newTestKeyAddress(t),
				// #nosec G115 -- checked positive above
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: uint64(outputAmount),
				},
			}},
			TxFee: phase2DepositFee,
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{collateralInput},
				false,
			),
			TxTotalCollateral:    phase2DepositCollateral,
			TxCertificates:       shape.certs,
			TxProposalProcedures: shape.proposals,
		},
		WitnessSet: conway.ConwayTransactionWitnessSet{
			WsPlutusData: cbor.NewSetType([]lcommon.Datum{datum}, false),
			WsPlutusV3Scripts: cbor.NewSetType(
				[]lcommon.PlutusV3Script{plutusScript},
				false,
			),
			WsRedeemers: conway.ConwayRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						Data: datum,
						ExUnits: lcommon.ExUnits{
							Steps:  1_000_000,
							Memory: 1_000_000,
						},
					},
				},
			},
		},
	}
	redeemersCbor, err := cbor.Encode(tx.WitnessSet.WsRedeemers.Redeemers)
	require.NoError(t, err)
	tx.WitnessSet.WsRedeemers.SetCbor(redeemersCbor)
	datumsCbor, err := cbor.Encode(tx.WitnessSet.WsPlutusData.Items())
	require.NoError(t, err)
	tx.WitnessSet.WsPlutusData.SetCbor(datumsCbor)
	langViewsCbor, err := lcommon.EncodeLangViews(
		map[uint]struct{}{2: {}},
		params.CostModels,
	)
	require.NoError(t, err)
	scriptData := append(redeemersCbor, datumsCbor...)
	scriptData = append(scriptData, langViewsCbor...)
	scriptDataHash := lcommon.Blake2b256Hash(scriptData)
	tx.Body.TxScriptDataHash = &scriptDataHash
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	txHash := tx.Hash()
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, txHash[:]),
		}},
		false,
	)
	cred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: keyHash,
	}
	return tx, state, params, cred
}

// An isValid=false Conway transaction whose script genuinely fails, so the
// local phase-2 verdict agrees with the flag, must still balance on the
// deposits and refunds the protocol parameters and ledger state require. A
// body balanced on a supplied amount instead is rejected for value
// conservation; the same body balanced on the required amount is accepted.
func TestValidateTxConwayPhase2InvalidDepositAccounting(t *testing.T) {
	t.Parallel()
	keyCred := func(t *testing.T) lcommon.Credential {
		_, _, _, cred := newPhase2DepositTx(t, phase2DepositShape{})
		return cred
	}
	rewardAddr := func(t *testing.T, cred lcommon.Credential) lcommon.Address {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey,
			lcommon.AddressNetworkTestnet,
			nil,
			cred.Credential[:],
		)
		require.NoError(t, err)
		return addr
	}
	type variant struct {
		name  string
		shape func(t *testing.T, amount uint64) phase2DepositShape
		// required and understated are the amounts the transaction balances
		// on; for a refund, understated is an overstated claim.
		required, understated uint64
		drepDeposit           *uint64
	}
	registered := uint64(phase2DepositDRepRefund)
	variants := []variant{
		{
			name:        "stake registration deposit",
			required:    depositTestKeyDeposit,
			understated: 1_000_000,
			shape: func(t *testing.T, amount uint64) phase2DepositShape {
				// #nosec G115 -- small test constants
				return phase2DepositShape{
					certs: []lcommon.CertificateWrapper{{
						Type: uint(lcommon.CertificateTypeRegistration),
						Certificate: &lcommon.RegistrationCertificate{
							CertType: uint(
								lcommon.CertificateTypeRegistration,
							),
							StakeCredential: keyCred(t),
							Amount:          int64(amount),
						},
					}},
					delta: -int64(amount),
				}
			},
		},
		{
			name:        "drep registration deposit",
			required:    depositTestDRepDeposit,
			understated: 1_000_000,
			shape: func(t *testing.T, amount uint64) phase2DepositShape {
				// #nosec G115 -- small test constants
				return phase2DepositShape{
					certs: []lcommon.CertificateWrapper{{
						Type: uint(lcommon.CertificateTypeRegistrationDrep),
						Certificate: &lcommon.RegistrationDrepCertificate{
							CertType: uint(
								lcommon.CertificateTypeRegistrationDrep,
							),
							DrepCredential: keyCred(t),
							Amount:         int64(amount),
						},
					}},
					delta: -int64(amount),
				}
			},
		},
		{
			name:        "drep deregistration refund",
			required:    phase2DepositDRepRefund,
			understated: phase2DepositDRepRefund + 1_000_000,
			drepDeposit: &registered,
			shape: func(t *testing.T, amount uint64) phase2DepositShape {
				// #nosec G115 -- small test constants
				return phase2DepositShape{
					certs: []lcommon.CertificateWrapper{{
						Type: uint(lcommon.CertificateTypeDeregistrationDrep),
						Certificate: &lcommon.DeregistrationDrepCertificate{
							CertType: uint(
								lcommon.CertificateTypeDeregistrationDrep,
							),
							DrepCredential: keyCred(t),
							Amount:         int64(amount),
						},
					}},
					delta: int64(amount),
				}
			},
		},
		{
			name:        "proposal deposit",
			required:    phase2DepositGovAction,
			understated: 1_000_000,
			shape: func(t *testing.T, amount uint64) phase2DepositShape {
				// #nosec G115 -- small test constants
				return phase2DepositShape{
					proposals: []conway.ConwayProposalProcedure{{
						PPDeposit:       amount,
						PPRewardAccount: rewardAddr(t, keyCred(t)),
						PPGovAction: conway.ConwayGovAction{
							Type: uint(lcommon.GovActionTypeInfo),
							Action: &lcommon.InfoGovAction{
								Type: uint(lcommon.GovActionTypeInfo),
							},
						},
					}},
					delta: -int64(amount),
				}
			},
		},
	}
	for _, v := range variants {
		run := func(t *testing.T, amount uint64, isValid bool) error {
			shape := v.shape(t, amount)
			shape.isValid = isValid
			tx, base, params, cred := newPhase2DepositTx(t, shape)
			ls := phase2DepositLedgerState{
				mockLedgerState: base,
				stakeRegistered: len(shape.proposals) > 0,
			}
			if v.drepDeposit != nil {
				ls.drep = &lcommon.DRepRegistration{
					Credential: cred,
					Deposit:    v.drepDeposit,
				}
			}
			return ValidateTxConway(tx, 0, ls, params)
		}
		t.Run(v.name+" understated", func(t *testing.T) {
			t.Parallel()
			err := run(t, v.understated, false)
			var notConserved shelley.ValueNotConservedUtxoError
			require.ErrorAs(t, err, &notConserved)
			shortfall := new(big.Int).Sub(
				notConserved.Produced, notConserved.Consumed,
			)
			shortfall.Abs(shortfall)
			// #nosec G115 -- small test constants
			want := big.NewInt(int64(v.required) - int64(v.understated))
			want.Abs(want)
			require.Zero(t, want.Cmp(shortfall),
				"imbalance %s, want %s", shortfall, want)
		})
		t.Run(v.name+" required", func(t *testing.T) {
			t.Parallel()
			for _, isValid := range []bool{false, true} {
				t.Run(
					fmt.Sprint(isValid),
					func(t *testing.T) { t.Parallel(); require.NoError(t, run(t, v.required, isValid)) },
				)
			}
		})
	}
}
