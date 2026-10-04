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
	"crypto/ed25519"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// dijkstraScopingState wraps the fixture ledger state with the committee
// credential and pool-margin queries the governance and entity rules read, so
// those rules have state to reject against when they run.
type dijkstraScopingState struct {
	*taggedCommitteeLedgerState
	floor *big.Rat
}

func (s *dijkstraScopingState) MinPoolMargin() *big.Rat { return s.floor }

// dijkstraScopingFailingV3Script is a Plutus V3 script that fails when applied
// to its single script-context argument.
func dijkstraScopingFailingV3Script(t *testing.T) lcommon.Script {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: &syn.Error{}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flat)
	require.NoError(t, err)
	return lcommon.PlutusV3Script(scriptBytes)
}

func dijkstraScopingSignerHash() [28]byte {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 0x77
	publicKey := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	return lcommon.Blake2b224Hash(publicKey)
}

// dijkstraScopingPoolCert builds a pool registration at a 0.1% margin through
// the wire decoder, which is the only way to populate the certificate's
// reward-account metadata that its encoder requires.
func dijkstraScopingPoolCert(
	t *testing.T,
	operator [28]byte,
) *lcommon.PoolRegistrationCertificate {
	t.Helper()
	rewardAccount := append([]byte{0xe0}, operator[:]...)
	raw, err := cbor.Encode([]any{
		uint64(lcommon.CertificateTypePoolRegistration),
		operator[:],
		make([]byte, 32),
		uint64(0),
		uint64(0),
		cbor.Rat{Rat: big.NewRat(1, 1000)},
		rewardAccount,
		[]any{operator[:]},
		[]any{},
		nil,
	})
	require.NoError(t, err)
	var cert lcommon.PoolRegistrationCertificate
	_, err = cbor.Decode(raw, &cert)
	require.NoError(t, err)
	return &cert
}

// newDijkstraScopingTx returns a transaction that passes every always-run rule
// when declared invalid (its Plutus script fails, as the flag declares), with
// the given semantic content added to its body and re-signed.
func newDijkstraScopingTx(
	t *testing.T,
	valid bool,
	mutate func(*gdijkstra.DijkstraTransactionBody),
) (*gdijkstra.DijkstraTransaction, lcommon.LedgerState, *gdijkstra.DijkstraProtocolParameters) {
	t.Helper()
	tx, base, params := newDijkstraReferenceOverlapScriptTx(
		t,
		dijkstraScopingFailingV3Script(t),
		valid,
	)
	// Proposals and votes are rejected beside Plutus V1/V2 scripts for either
	// declaration, and V3 rejects a reference input that is also spent.
	tx.Body.TxReferenceInputs = cbor.SetType[shelley.ShelleyTransactionInput]{}
	params.GovActionDeposit = 1_000
	mutate(&tx.Body)
	// Value conservation is an always-run rule that counts proposal deposits,
	// so pay for them from the fixture's change output at the
	// protocol-parameter deposit, which is what the rule charges.
	for range tx.Body.TxProposalProcedures {
		output, ok := tx.Body.TxOutputs[0].Output.(babbage.BabbageTransactionOutput)
		require.True(t, ok)
		output.OutputAmount.Amount -= uint64(params.GovActionDeposit)
		tx.Body.TxOutputs[0].Output = output
	}
	refreshDijkstraTransactionBodyAndSignature(t, tx)
	state := &dijkstraScopingState{
		taggedCommitteeLedgerState: &taggedCommitteeLedgerState{
			mockLedgerState: base,
			available:       true,
		},
		floor: big.NewRat(150, 10_000),
	}
	return tx, state, params
}

func dijkstraScopingProposal(
	t *testing.T,
	deposit uint64,
	action lcommon.GovAction,
	actionType lcommon.GovActionType,
) gdijkstra.DijkstraProposalProcedure {
	t.Helper()
	rewardAccount, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	return gdijkstra.DijkstraProposalProcedure{
		PPDeposit:       deposit,
		PPRewardAccount: rewardAccount,
		PPGovAction: gdijkstra.DijkstraGovAction{
			Type:   uint(actionType),
			Action: action,
		},
	}
}

// TestValidateTxDijkstraScopesSemanticRulesToDeclaredValidity drives the
// production entry point with the same semantic defect declared valid and
// declared invalid. Governance and entity predicates must reject only the
// valid declaration; the invalid one applies the collateral path alone.
func TestValidateTxDijkstraScopesSemanticRulesToDeclaredValidity(t *testing.T) {
	t.Parallel()
	hardFork := func(major, minor uint) lcommon.GovAction {
		action := &lcommon.HardForkInitiationGovAction{
			Type: uint(lcommon.GovActionTypeHardForkInitiation),
		}
		action.ProtocolVersion.Major = major
		action.ProtocolVersion.Minor = minor
		return action
	}
	// Required signers must still be witnessed for an invalid transaction, so
	// the voter and pool operator use the fixture's signing key.
	signer := dijkstraScopingSignerHash()
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *gdijkstra.DijkstraTransactionBody)
		check  func(*testing.T, error)
	}{
		{
			name: "hard fork that cannot follow",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				b.TxProposalProcedures = []gdijkstra.DijkstraProposalProcedure{
					dijkstraScopingProposal(
						t, 1_000,
						hardFork(gdijkstra.MinProtocolVersionDijkstra, 2),
						lcommon.GovActionTypeHardForkInitiation,
					),
				}
			},
			check: func(t *testing.T, err error) {
				var e conway.BadHardForkProtocolVersionError
				require.ErrorAs(t, err, &e)
			},
		},
		{
			name: "mismatched proposal deposit",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				b.TxProposalProcedures = []gdijkstra.DijkstraProposalProcedure{
					dijkstraScopingProposal(
						t, 999,
						&lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)},
						lcommon.GovActionTypeInfo,
					),
				}
			},
			check: func(t *testing.T, err error) {
				var e conway.ProposalDepositIncorrectError
				require.ErrorAs(t, err, &e)
			},
		},
		{
			name: "irrelevant proposal ancestry",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				action := hardFork(gdijkstra.MinProtocolVersionDijkstra, 0)
				hf := action.(*lcommon.HardForkInitiationGovAction)
				hf.ActionId = &lcommon.GovActionId{
					TransactionId: lcommon.Blake2b256{0xa1},
				}
				b.TxProposalProcedures = []gdijkstra.DijkstraProposalProcedure{
					dijkstraScopingProposal(
						t, 1_000, action,
						lcommon.GovActionTypeHardForkInitiation,
					),
				}
			},
			check: func(t *testing.T, err error) {
				var e conway.InvalidGovActionAncestorError
				require.ErrorAs(t, err, &e)
			},
		},
		{
			name: "vote from unknown committee hot credential",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				b.TxVotingProcedures = lcommon.VotingProcedures{
					&lcommon.Voter{
						Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
						Hash: signer,
					}: {},
				}
			},
			check: func(t *testing.T, err error) {
				var e conway.UnknownVoterError
				require.ErrorAs(t, err, &e)
			},
		},
		{
			// The reference checks the declared treasury value inside the
			// phase-2-valid branch of LEDGER. Upstream lists the rule as
			// always-run and gates it internally, so only behaviour pins it.
			name: "current treasury value mismatch",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				b.TxCurrentTreasuryValue = 5
			},
			check: func(t *testing.T, err error) {
				var e lcommon.CurrentTreasuryValueMismatchError
				require.ErrorAs(t, err, &e)
			},
		},
		{
			name: "pool registration below the margin floor",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				b.TxCertificates = []lcommon.CertificateWrapper{{
					Type:        uint(lcommon.CertificateTypePoolRegistration),
					Certificate: dijkstraScopingPoolCert(t, signer),
				}}
			},
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "below minimum pool margin")
			},
		},
		{
			name: "committee hot-key authorization for a non-member",
			mutate: func(t *testing.T, b *gdijkstra.DijkstraTransactionBody) {
				b.TxCertificates = []lcommon.CertificateWrapper{{
					Type: uint(lcommon.CertificateTypeAuthCommitteeHot),
					Certificate: &lcommon.AuthCommitteeHotCertificate{
						CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
						ColdCredential: lcommon.Credential{
							CredType:   lcommon.CredentialTypeAddrKeyHash,
							Credential: signer,
						},
						HotCredential: lcommon.Credential{
							CredType:   lcommon.CredentialTypeAddrKeyHash,
							Credential: lcommon.Blake2b224{0xc3},
						},
					},
				}}
			},
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "not a CC member")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			apply := func(b *gdijkstra.DijkstraTransactionBody) {
				tc.mutate(t, b)
			}

			validTx, validState, validParams := newDijkstraScopingTx(
				t, true, apply,
			)
			tc.check(
				t,
				ValidateTxDijkstra(validTx, 0, validState, validParams),
			)

			invalidTx, invalidState, invalidParams := newDijkstraScopingTx(
				t, false, apply,
			)
			require.NoError(
				t,
				ValidateTxDijkstra(invalidTx, 0, invalidState, invalidParams),
			)
		})
	}
}

// TestValidateTxDijkstraInvalidTransactionStillFailsAlwaysRunRules proves the
// always-run rules are not skipped for a declared-invalid transaction: the same
// fixture that passes untouched is rejected once its collateral, redeemer or
// witness requirement is broken.
func TestValidateTxDijkstraInvalidTransactionStillFailsAlwaysRunRules(
	t *testing.T,
) {
	t.Parallel()
	tx, state, params := newDijkstraScopingTx(
		t, false, func(*gdijkstra.DijkstraTransactionBody) {},
	)
	require.NoError(t, ValidateTxDijkstra(tx, 0, state, params))

	for _, tc := range []struct {
		name    string
		mutate  func(*gdijkstra.DijkstraTransaction)
		wantErr string
	}{
		{
			name: "missing collateral",
			mutate: func(tx *gdijkstra.DijkstraTransaction) {
				tx.Body.TxCollateral = cbor.SetType[shelley.ShelleyTransactionInput]{}
				tx.Body.TxTotalCollateral = 0
			},
			wantErr: "collateral",
		},
		{
			name: "missing redeemer",
			mutate: func(tx *gdijkstra.DijkstraTransaction) {
				tx.WitnessSet.WsRedeemers = gdijkstra.DijkstraRedeemers{}
			},
			wantErr: "redeemer",
		},
		{
			name: "missing vkey witness",
			mutate: func(tx *gdijkstra.DijkstraTransaction) {
				tx.WitnessSet.VkeyWitnesses = cbor.SetType[lcommon.VkeyWitness]{}
			},
			wantErr: "witness",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx, state, params := newDijkstraScopingTx(
				t, false, func(*gdijkstra.DijkstraTransactionBody) {},
			)
			tc.mutate(tx)
			err := ValidateTxDijkstra(tx, 0, state, params)
			require.Error(t, err)
			require.Contains(t, strings.ToLower(err.Error()), tc.wantErr)
		})
	}
}

// TestValidateTxDijkstraMalformedProposalFailsRegardlessOfValidity pins the
// structural control: wire data that cannot decode is rejected before the
// declared validity flag is consulted, and a decoded proposal that sets the
// protocol version through ParameterChange is rejected for both declarations.
func TestValidateTxDijkstraMalformedProposalFailsRegardlessOfValidity(
	t *testing.T,
) {
	t.Parallel()
	for _, valid := range []bool{true, false} {
		t.Run(fmt.Sprintf("is_valid=%t", valid), func(t *testing.T) {
			t.Parallel()
			// A proposal procedure is a four-element array; a truncated one
			// is not a well-formed proposal whatever the transaction's flag.
			body := map[uint]any{
				0:  []any{},
				1:  []any{},
				2:  uint64(0),
				20: []any{[]any{uint64(1_000)}},
			}
			raw, err := cbor.Encode([]any{body, map[uint]any{}, valid, nil})
			require.NoError(t, err)
			_, err = gdijkstra.NewDijkstraTransactionFromCbor(raw)
			require.Error(t, err)

			tx, state, params := newDijkstraScopingTx(
				t, valid, func(b *gdijkstra.DijkstraTransactionBody) {
					b.TxProposalProcedures = []gdijkstra.DijkstraProposalProcedure{
						dijkstraScopingProposal(
							t, 1_000,
							&gdijkstra.DijkstraParameterChangeGovAction{
								ParamUpdate: gdijkstra.DijkstraProtocolParameterUpdate{
									ProtocolVersion: &lcommon.ProtocolParametersProtocolVersion{
										Major: gdijkstra.MinProtocolVersionDijkstra,
									},
								},
							},
							lcommon.GovActionTypeParameterChange,
						),
					}
				},
			)
			err = ValidateTxDijkstra(tx, 0, state, params)
			var protocolVersionErr ParameterChangeProtocolVersionError
			require.ErrorAs(t, err, &protocolVersionErr)
		})
	}
}

type dijkstraRulePhase uint8

const (
	dijkstraRuleAlways dijkstraRulePhase = iota
	dijkstraRulePhase2Valid
)

// dijkstraRulePhases is Dingo's own statement of which upstream rules run for
// every transaction and which only for phase-2-valid ones. It is deliberately
// independent of the upstream table: a descriptor added or reclassified
// upstream must be reviewed here before it can reach the phase-1 list.
var dijkstraRulePhases = map[lcommon.UtxoValidationRuleId]dijkstraRulePhase{
	lcommon.UtxoValidationRuleCurrentTreasuryValue:         dijkstraRuleAlways,
	lcommon.UtxoValidationRuleMetadata:                     dijkstraRuleAlways,
	lcommon.UtxoValidationRuleProposalProcedures:           dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleGovActionWellFormedness:      dijkstraRuleAlways,
	lcommon.UtxoValidationRuleHardForkCanFollow:            dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleProposalAncestry:             dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleProposalDeposit:              dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleProposalNetworkIds:           dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleProposalReturnAccounts:       dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleProposalReturnAddressShape:   dijkstraRuleAlways,
	lcommon.UtxoValidationRuleEmptyTreasuryWithdrawals:     dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleBootstrapAllowedGovActions:   dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleIsValidFlag:                  dijkstraRuleAlways,
	lcommon.UtxoValidationRuleRequiredVKeyWitnesses:        dijkstraRuleAlways,
	lcommon.UtxoValidationRuleCollateralVKeyWitnesses:      dijkstraRuleAlways,
	lcommon.UtxoValidationRuleCollateralKeyLocked:          dijkstraRuleAlways,
	lcommon.UtxoValidationRuleRedeemerAndScriptWitnesses:   dijkstraRuleAlways,
	lcommon.UtxoValidationRuleSignatures:                   dijkstraRuleAlways,
	lcommon.UtxoValidationRuleCostModelsPresent:            dijkstraRuleAlways,
	lcommon.UtxoValidationRuleScriptDataHash:               dijkstraRuleAlways,
	lcommon.UtxoValidationRuleInlineDatumsWithPlutusV1:     dijkstraRuleAlways,
	lcommon.UtxoValidationRuleConwayFeaturesWithPlutusV1V2: dijkstraRuleAlways,
	lcommon.UtxoValidationRuleOutsideValidityInterval:      dijkstraRuleAlways,
	lcommon.UtxoValidationRuleOutsideForecast:              dijkstraRuleAlways,
	lcommon.UtxoValidationRuleInputSetEmpty:                dijkstraRuleAlways,
	lcommon.UtxoValidationRuleNoDuplicateInputs:            dijkstraRuleAlways,
	lcommon.UtxoValidationRuleFeeTooSmall:                  dijkstraRuleAlways,
	lcommon.UtxoValidationRuleInsufficientCollateral:       dijkstraRuleAlways,
	lcommon.UtxoValidationRuleCollateralContainsNonAda:     dijkstraRuleAlways,
	lcommon.UtxoValidationRuleCollateralEqBalance:          dijkstraRuleAlways,
	lcommon.UtxoValidationRulePtrPresentInCollateralReturn: dijkstraRuleAlways,
	lcommon.UtxoValidationRuleNoCollateralInputs:           dijkstraRuleAlways,
	lcommon.UtxoValidationRuleBadInputs:                    dijkstraRuleAlways,
	lcommon.UtxoValidationRuleScriptWitnesses:              dijkstraRuleAlways,
	lcommon.UtxoValidationRuleRequiredRedeemers:            dijkstraRuleAlways,
	lcommon.UtxoValidationRuleBatchWithdrawals:             dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleAccountBalanceIntervals:      dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleValueNotConserved:            dijkstraRuleAlways,
	lcommon.UtxoValidationRuleOutputTooSmall:               dijkstraRuleAlways,
	lcommon.UtxoValidationRuleOutputTooBig:                 dijkstraRuleAlways,
	lcommon.UtxoValidationRuleOutputBootAddrAttrsTooBig:    dijkstraRuleAlways,
	lcommon.UtxoValidationRuleWrongNetwork:                 dijkstraRuleAlways,
	lcommon.UtxoValidationRuleWrongNetworkWithdrawal:       dijkstraRuleAlways,
	lcommon.UtxoValidationRuleTransactionNetworkId:         dijkstraRuleAlways,
	lcommon.UtxoValidationRuleMaxTxSize:                    dijkstraRuleAlways,
	lcommon.UtxoValidationRuleExUnitsTooBig:                dijkstraRuleAlways,
	lcommon.UtxoValidationRuleTooManyCollateralInputs:      dijkstraRuleAlways,
	lcommon.UtxoValidationRuleSupplementalDatums:           dijkstraRuleAlways,
	lcommon.UtxoValidationRuleExtraneousRedeemers:          dijkstraRuleAlways,
	lcommon.UtxoValidationRuleMalformedReferenceScripts:    dijkstraRuleAlways,
	lcommon.UtxoValidationRulePlutusScripts:                dijkstraRuleAlways,
	lcommon.UtxoValidationRuleNativeScripts:                dijkstraRuleAlways,
	lcommon.UtxoValidationRuleDelegation:                   dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleWithdrawals:                  dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleCertificateDeposits:          dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleCommitteeCertificates:        dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleUnknownVoters:                dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleUnknownGovActionIds:          dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleVotingOnExpiredGovAction:     dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleBootstrapVotingRestrictions:  dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleStakePoolVotingRestrictions:  dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleCCVotingRestrictions:         dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleUnelectedCommitteeVoters:     dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRuleRefScriptSizePerTx:           dijkstraRulePhase2Valid,
	lcommon.UtxoValidationRulePoolCertificates:             dijkstraRulePhase2Valid,
}

// TestDijkstraPhase1RulesHaveExplicitPhaseClassification fails when an
// upstream descriptor has no Dingo-side phase, when the upstream rule at a
// descriptor's position is wrapped differently from its classification, or
// when a rule Dingo places in its phase-1 list for a phase-2-valid Id does not
// ignore a declared-invalid transaction.
func TestDijkstraPhase1RulesHaveExplicitPhaseClassification(t *testing.T) {
	t.Parallel()
	descriptors := gdijkstra.UtxoValidationRuleDescriptors()
	rules := gdijkstra.UtxoValidationRules
	require.Len(t, rules, len(descriptors))

	seen := make(map[lcommon.UtxoValidationRuleId]struct{}, len(descriptors))
	phaseByIndex := make(map[int]dijkstraRulePhase, len(descriptors))
	for i, descriptor := range descriptors {
		want, ok := dijkstraRulePhases[descriptor.Id]
		require.Truef(t, ok, "rule %q has no phase classification", descriptor.Id)
		seen[descriptor.Id] = struct{}{}
		phaseByIndex[i] = want
		// ComposeUtxoValidationRules replaces a phase-2-valid rule with an
		// anonymous wrapper; an always-run rule keeps its own function.
		wrapped := strings.Contains(
			utxoValidationRuleName(rules[i]),
			"ComposeUtxoValidationRules",
		)
		require.Equalf(
			t,
			want == dijkstraRulePhase2Valid,
			wrapped,
			"rule %q classified %d disagrees with upstream wrapping",
			descriptor.Id,
			want,
		)
	}
	for id := range dijkstraRulePhases {
		_, ok := seen[id]
		require.Truef(t, ok, "classified rule %q is not an upstream descriptor", id)
	}

	invalid := &gdijkstra.DijkstraTransaction{TxIsValid: false}
	for _, rule := range dijkstraPhase1ValidationRules() {
		if phaseByIndex[rule.index] != dijkstraRulePhase2Valid {
			continue
		}
		require.NoErrorf(
			t,
			rule.validationFunc(invalid, 0, nil, nil),
			"phase-2-valid rule %q ran for a declared-invalid transaction",
			descriptors[rule.index].Id,
		)
	}
}
