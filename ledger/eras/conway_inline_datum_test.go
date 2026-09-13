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
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// conway.UtxoValidateInlineDatumsWithPlutusV1 is the upstream Conway
// inline-datums-with-plutus-v1 UTXO rule. Dingo installs it unmodified, so
// nothing else in this repository pins its behavior. The cases below cover
// the three properties the rule has to get right, none of which
// internal/test/conformance reaches:
//
//   - an inline datum is disqualifying on a consumed input, on a reference
//     input, and on a produced output, whenever a PlutusV1 script is needed;
//   - a reference script is never disqualifying, and neither is the mere
//     presence of reference inputs;
//   - only a *needed* PlutusV1 script constrains the transaction, so a V1
//     script that is merely reachable is ignored.
//
// The needed-not-available distinction is the fix from gouroboros #1980.

// newBabbageInlineDatumOutput builds a Babbage output carrying an inline datum
// at the given address, by round-tripping CBOR rather than asserting a concrete
// era type, so the output reports Datum() the way a decoded block output does.
func newBabbageInlineDatumOutput(
	t *testing.T,
	addr lcommon.Address,
) lcommon.TransactionOutput {
	t.Helper()
	datumCbor, err := cbor.Encode(data.NewConstr(0))
	require.NoError(t, err)
	datumOptionCbor, err := cbor.Encode([]any{
		babbage.DatumOptionTypeData,
		cbor.Tag{Number: 24, Content: datumCbor},
	})
	require.NoError(t, err)
	addressCbor, err := cbor.Encode(addr)
	require.NoError(t, err)
	amountCbor, err := cbor.Encode(uint64(1_000_000))
	require.NoError(t, err)
	outputCbor, err := cbor.Encode(map[uint]cbor.RawMessage{
		0: addressCbor,
		1: amountCbor,
		2: datumOptionCbor,
	})
	require.NoError(t, err)
	var output babbage.BabbageTransactionOutput
	_, err = cbor.Decode(outputCbor, &output)
	require.NoError(t, err)
	return &output
}

// TestConwayInlineDatumRuleRejectsUsedPlutusV1Script is the base rejection: the
// spent input is guarded by a PlutusV1 script and carries an inline datum.
func TestConwayInlineDatumRuleRejectsUsedPlutusV1Script(t *testing.T) {
	input := newTestInput(0x03, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x01, 0x02})
	scriptAddr := newTestScriptAddress(t, plutusScript)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs: []lcommon.TransactionInput{input},
	}
	ls := newMockLedgerState()
	ls.addUtxo(input, newBabbageInlineDatumOutput(t, scriptAddr))

	var inlineDatumErr lcommon.InlineDatumsNotSupportedError
	require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	), &inlineDatumErr)
}

// TestConwayInlineDatumRuleRejectsDatumOnOutput covers an inline datum on one
// of the transaction's own outputs rather than on a consumed input. The
// PlutusV1 script context has to represent that output too, so scanning only
// the consumed inputs misses it.
func TestConwayInlineDatumRuleRejectsDatumOnOutput(t *testing.T) {
	scriptInput := newTestInput(0x21, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x03, 0x04})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs: []lcommon.TransactionInput{scriptInput},
		outputs: []lcommon.TransactionOutput{
			newBabbageInlineDatumOutput(t, newTestKeyAddress(t)),
		},
	}
	ls := newMockLedgerState()
	// The spent UTxO carries no datum; only the new output does.
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       scriptAddr,
		scriptRef:  plutusScript,
	})

	var inlineDatumErr lcommon.InlineDatumsNotSupportedError
	require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	), &inlineDatumErr)
}

// TestConwayInlineDatumRuleRejectsDatumOnReferenceInput covers an inline datum
// reachable only through a reference input.
func TestConwayInlineDatumRuleRejectsDatumOnReferenceInput(t *testing.T) {
	scriptInput := newTestInput(0x31, 0)
	refInput := newTestInput(0x32, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x05, 0x06})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs:          []lcommon.TransactionInput{scriptInput},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       scriptAddr,
		scriptRef:  plutusScript,
	})
	ls.addUtxo(refInput, newBabbageInlineDatumOutput(t, newTestKeyAddress(t)))

	var inlineDatumErr lcommon.InlineDatumsNotSupportedError
	require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	), &inlineDatumErr)
}

// TestConwayInlineDatumRuleRejectsNonSpendingPlutusV1 pins that the
// needed-script scan is not limited to spending purposes. In each case the
// PlutusV1 script is required by a non-spending purpose and the inline datum
// sits on an unrelated key-locked input. cardano-ledger maps the V1 context
// over every spent input once any V1 script runs, so these transactions are
// invalid.
func TestConwayInlineDatumRuleRejectsNonSpendingPlutusV1(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*testing.T, *mockConwayFeeTx, lcommon.PlutusV1Script)
	}{
		{name: "Minting", apply: applyMintPurpose},
		{name: "Certifying", apply: applyCertPurpose},
		{name: "Rewarding", apply: applyWithdrawalPurpose},
		{name: "Voting", apply: applyVotingPurpose},
		{name: "Proposing", apply: applyProposalPurpose},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyInput := newTestInput(0x41, 0)
			plutusScript := lcommon.PlutusV1Script([]byte{0x07, 0x08})
			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					witnesses: &mockWitnessSet{
						plutusV1Scripts: []lcommon.PlutusV1Script{
							plutusScript,
						},
					},
				},
				inputs: []lcommon.TransactionInput{keyInput},
			}
			tc.apply(t, tx, plutusScript)
			ls := newMockLedgerState()
			// Key-locked input, so no spending purpose needs a script.
			ls.addUtxo(
				keyInput,
				newBabbageInlineDatumOutput(t, newTestKeyAddress(t)),
			)

			var inlineDatumErr lcommon.InlineDatumsNotSupportedError
			require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			), &inlineDatumErr)
		})
	}
}

func applyMintPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(s.Hash()): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	tx.assetMint = &assetMint
}

func applyCertPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	tx.certificates = []lcommon.Certificate{
		&lcommon.StakeDeregistrationCertificate{
			StakeCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeScriptHash,
				Credential: lcommon.Blake2b224(s.Hash()),
			},
		},
	}
}

func applyWithdrawalPurpose(
	t *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	addr := newTestScriptStakeAddress(t, s)
	tx.withdrawals = map[*lcommon.Address]*big.Int{
		&addr: big.NewInt(1),
	}
}

func applyVotingPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	voter := lcommon.Voter{
		Type: lcommon.VoterTypeDRepScriptHash,
		Hash: s.Hash(),
	}
	tx.votingProcedures = lcommon.VotingProcedures{
		&voter: nil,
	}
}

func applyProposalPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	tx.proposalProcedures = []lcommon.ProposalProcedure{
		conway.ConwayProposalProcedure{
			PPGovAction: conway.ConwayGovAction{
				Type: uint(lcommon.GovActionTypeParameterChange),
				Action: &conway.ConwayParameterChangeGovAction{
					PolicyHash: s.Hash().Bytes(),
				},
			},
		},
	}
}

// TestConwayInlineDatumRuleIgnoresUnusedPlutusV1ReferenceScript is the case
// gouroboros #1980 fixed. An unrelated PlutusV1 reference script sits on a
// spent UTxO and another spent UTxO carries an inline datum, but no script
// purpose needs the V1 script, so the transaction is valid.
//
// Before #1980 the rule gated on *available* scripts and rejected this shape,
// which turned an ordinary transaction into a permanent validation failure.
// dingo #3240 asserted that rejection against the then-current pin; the
// assertion is inverted here because the upstream rule now gates on needed
// scripts.
func TestConwayInlineDatumRuleIgnoresUnusedPlutusV1ReferenceScript(
	t *testing.T,
) {
	inlineInput := newTestInput(0x01, 0)
	scriptInput := newTestInput(0x02, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x01, 0x02})
	addr := newTestKeyAddress(t)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{witnesses: &mockWitnessSet{}},
		inputs:    []lcommon.TransactionInput{inlineInput, scriptInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(inlineInput, newBabbageInlineDatumOutput(t, addr))
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       addr,
		scriptRef:  plutusScript,
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleAcceptsUnusedPlutusV1WithNeededPlutusV2 is the
// accept path for a transaction that does need a Plutus script, so the
// needed-script scan runs on a non-empty set.
//
// The spending input is a PlutusV2 script address, so there is a real script
// purpose. A PlutusV1 script is also reachable -- in the witness set and as a
// reference script on a reference input -- but no purpose needs it, and an
// inline datum is present. Only the *needed* script is PlutusV2, so the
// transaction is valid. An implementation that scans available scripts instead
// of needed ones rejects this.
func TestConwayInlineDatumRuleAcceptsUnusedPlutusV1WithNeededPlutusV2(
	t *testing.T,
) {
	v2Input := newTestInput(0x11, 0)
	refInput := newTestInput(0x12, 0)
	v2Script := lcommon.PlutusV2Script([]byte{0x0a, 0x0b})
	unusedV1 := lcommon.PlutusV1Script([]byte{0x01, 0x02})

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{unusedV1},
				plutusV2Scripts: []lcommon.PlutusV2Script{v2Script},
			},
		},
		inputs:          []lcommon.TransactionInput{v2Input},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	// The spent UTxO carries the inline datum and is guarded by PlutusV2.
	ls.addUtxo(
		v2Input,
		newBabbageInlineDatumOutput(t, newTestScriptAddress(t, v2Script)),
	)
	// The unused PlutusV1 script is only reachable, never required.
	ls.addUtxo(refInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
		scriptRef:  unusedV1,
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleAllowsReferenceScriptOnOutput pins that a reference
// script on a produced output is not rejected. Conway's transTxOutV1 shadows
// Babbage's and drops the ReferenceScriptsNotSupported branch, checking only
// the inline datum, so a needed PlutusV1 script coexists legitimately with a
// produced output carrying a reference script.
func TestConwayInlineDatumRuleAllowsReferenceScriptOnOutput(t *testing.T) {
	input := newTestInput(0x51, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x0f, 0x10})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs: []lcommon.TransactionInput{input},
		outputs: []lcommon.TransactionOutput{
			testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  plutusScript,
			},
		},
	}
	ls := newMockLedgerState()
	// No inline datum anywhere; the needed V1 script is the spending one.
	ls.addUtxo(input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       scriptAddr,
		scriptRef:  plutusScript,
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleAllowsReferenceInputsWithNeededPlutusV1 pins that
// the mere presence of a reference input is not disqualifying. cardano-ledger's
// Babbage-era V1 instance rejects any reference input, but the Conway vector
// "UTXOS/can use reference scripts" expects success, so the Conway rule must
// look at inline datums only.
func TestConwayInlineDatumRuleAllowsReferenceInputsWithNeededPlutusV1(
	t *testing.T,
) {
	scriptInput := newTestInput(0x61, 0)
	refInput := newTestInput(0x62, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x0b, 0x0c})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs:          []lcommon.TransactionInput{scriptInput},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(2_000_000),
		addr:       scriptAddr,
	})
	ls.addUtxo(refInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleSkipsUnresolvableInput pins the rule's contract for
// an input that is not in the ledger state: defer to UtxoValidateBadInputsUtxo,
// which reports it with the right error, rather than becoming a second source
// of input-resolution failures.
//
// The transaction would otherwise be rejected: its first input is a PlutusV1
// script address carrying an inline datum, so a rule that resolved what it
// could and carried on would return InlineDatumsNotSupportedError here.
func TestConwayInlineDatumRuleSkipsUnresolvableInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing func(*mockConwayFeeTx, lcommon.TransactionInput)
	}{
		{
			name: "Input",
			missing: func(
				tx *mockConwayFeeTx,
				in lcommon.TransactionInput,
			) {
				tx.inputs = append(tx.inputs, in)
			},
		},
		{
			name: "ReferenceInput",
			missing: func(
				tx *mockConwayFeeTx,
				in lcommon.TransactionInput,
			) {
				tx.referenceInputs = append(tx.referenceInputs, in)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scriptInput := newTestInput(0x71, 0)
			missingInput := newTestInput(0x72, 0)
			plutusScript := lcommon.PlutusV1Script([]byte{0x0d, 0x0e})
			scriptAddr := newTestScriptAddress(t, plutusScript)

			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					witnesses: &mockWitnessSet{
						plutusV1Scripts: []lcommon.PlutusV1Script{
							plutusScript,
						},
					},
				},
				inputs: []lcommon.TransactionInput{scriptInput},
			}
			tc.missing(tx, missingInput)
			ls := newMockLedgerState()
			ls.addUtxo(
				scriptInput,
				newBabbageInlineDatumOutput(t, scriptAddr),
			)
			// missingInput is deliberately absent from the ledger state.

			require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			))
		})
	}
}
