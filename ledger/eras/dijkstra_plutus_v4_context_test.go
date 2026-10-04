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
	"bytes"
	"crypto/ed25519"
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/builtin"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

type v4Term = syn.Term[syn.DeBruijn]

func v4Apply(fn builtin.DefaultFunction, args ...v4Term) v4Term {
	var term v4Term = &syn.Builtin{DefaultFunction: fn}
	forces := 0
	switch fn {
	case builtin.SndPair, builtin.FstPair:
		forces = 2
	case builtin.HeadList, builtin.TailList, builtin.IfThenElse:
		forces = 1
	}
	for range forces {
		term = &syn.Force[syn.DeBruijn]{Term: term}
	}
	for _, arg := range args {
		term = &syn.Apply[syn.DeBruijn]{Function: term, Argument: arg}
	}
	return term
}

func v4Field(list v4Term, index int) v4Term {
	for range index {
		list = v4Apply(builtin.TailList, list)
	}
	return v4Apply(builtin.HeadList, list)
}

func v4ConstrFields(term v4Term) v4Term {
	return v4Apply(builtin.SndPair, v4Apply(builtin.UnConstrData, term))
}

// v4ContextScript builds a PlutusV4 script that succeeds only when cond,
// evaluated against the ScriptContext, is true.
func v4ContextScript(
	t *testing.T,
	cond func(ctx v4Term) v4Term,
) lcommon.PlutusV4Script {
	t.Helper()
	ctx := v4Term(&syn.Var[syn.DeBruijn]{Name: 1})
	body := &syn.Force[syn.DeBruijn]{Term: v4Apply(
		builtin.IfThenElse,
		cond(ctx),
		&syn.Delay[syn.DeBruijn]{Term: &syn.Constant{Con: &syn.Unit{}}},
		&syn.Delay[syn.DeBruijn]{Term: &syn.Error{}},
	)}
	flat, err := syn.Encode(&syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: body},
	})
	require.NoError(t, err)
	wrapper, err := cbor.Encode(flat)
	require.NoError(t, err)
	return lcommon.PlutusV4Script(wrapper)
}

// v4MaybeScript succeeds when the Maybe selected by sel has the given
// constructor tag: 0 is Just, 1 is Nothing. It compares the tag rather than
// the whole value, because Just carries a field and so never equals a bare
// Constr 0.
func v4MaybeScript(
	t *testing.T,
	sel func(ctx v4Term) v4Term,
	tag int64,
) lcommon.PlutusV4Script {
	t.Helper()
	return v4ContextScript(t, func(ctx v4Term) v4Term {
		return v4Apply(
			builtin.EqualsInteger,
			v4Apply(builtin.FstPair, v4Apply(builtin.UnConstrData, sel(ctx))),
			&syn.Constant{Con: &syn.Integer{Inner: big.NewInt(tag)}},
		)
	})
}

func v4TxInfoSubTxIx(ctx v4Term) v4Term {
	txInfo := v4Field(v4ConstrFields(ctx), 0)
	return v4Field(v4ConstrFields(txInfo), 1)
}

func v4GuardingTopTxInfo(ctx v4Term) v4Term {
	scriptInfo := v4Field(v4ConstrFields(ctx), 2)
	return v4Field(v4ConstrFields(scriptInfo), 1)
}

// v4RequiredGuardsOrderScript succeeds only when the script-visible
// required-guards map lists a script credential (Constr 1) first and a key
// credential (Constr 0) second. The reference orders script entries before
// key entries, which is the reverse of the credential type numbers.
func v4RequiredGuardsOrderScript(t *testing.T) lcommon.PlutusV4Script {
	t.Helper()
	return v4ContextScript(t, func(ctx v4Term) v4Term {
		txInfo := v4Field(v4ConstrFields(ctx), 0)
		guards := v4Field(v4ConstrFields(txInfo), 12)
		entries := v4Apply(builtin.UnMapData, guards)
		tagIs := func(entry v4Term, tag int64) v4Term {
			credential := v4Apply(builtin.FstPair, entry)
			return v4Apply(
				builtin.EqualsInteger,
				v4Apply(
					builtin.FstPair,
					v4Apply(builtin.UnConstrData, credential),
				),
				&syn.Constant{Con: &syn.Integer{Inner: big.NewInt(tag)}},
			)
		}
		return &syn.Force[syn.DeBruijn]{Term: v4Apply(
			builtin.IfThenElse,
			tagIs(v4Apply(builtin.HeadList, entries), 1),
			&syn.Delay[syn.DeBruijn]{Term: tagIs(v4Field(entries, 1), 0)},
			&syn.Delay[syn.DeBruijn]{Term: v4BoolConstant(false)},
		)}
	})
}

func v4BoolConstant(b bool) v4Term {
	return &syn.Constant{Con: &syn.Bool{Inner: b}}
}

func v4ScriptCredential(s lcommon.Script) lcommon.Credential {
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: lcommon.Blake2b224(s.Hash()),
	}
}

type v4ContextFixture struct {
	tx     *gdijkstra.DijkstraTransaction
	state  *mockLedgerState
	params *gdijkstra.DijkstraProtocolParameters
	key    ed25519.PrivateKey
}

// v4ContextSpec selects which guarding scripts the transaction carries.
type v4ContextSpec struct {
	topScript    lcommon.PlutusV4Script
	childScripts []lcommon.PlutusV4Script
	// required is encoded into field 24 of the top-level body.
	required gdijkstra.DijkstraRequiredTopLevelGuards
	// childRequired is encoded into field 24 of the first child body.
	childRequired gdijkstra.DijkstraRequiredTopLevelGuards
	topGuards     []lcommon.Credential
}

func v4GuardingWitnesses(
	t *testing.T,
	params *gdijkstra.DijkstraProtocolParameters,
	index uint32,
) (gdijkstra.DijkstraTransactionWitnessSet, lcommon.Blake2b256) {
	t.Helper()
	redeemers := map[lcommon.RedeemerKey]lcommon.RedeemerValue{
		{Tag: lcommon.RedeemerTagGuarding, Index: index}: {
			Data: lcommon.Datum{Data: data.NewInteger(big.NewInt(0))},
			ExUnits: lcommon.ExUnits{
				Steps:  5_000_000,
				Memory: 5_000_000,
			},
		},
	}
	wits := gdijkstra.DijkstraTransactionWitnessSet{
		WsRedeemers: gdijkstra.DijkstraRedeemers{Redeemers: redeemers},
	}
	redeemersCbor, err := cbor.Encode(redeemers)
	require.NoError(t, err)
	wits.WsRedeemers.SetCbor(redeemersCbor)
	langViews, err := lcommon.EncodeLangViews(
		map[uint]struct{}{3: {}},
		params.CostModels,
	)
	require.NoError(t, err)
	return wits, lcommon.Blake2b256Hash(append(redeemersCbor, langViews...))
}

func newV4ContextFixture(
	t *testing.T,
	spec v4ContextSpec,
) v4ContextFixture {
	t.Helper()
	const balance = uint64(100_000_000)
	params := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxTxSize:                  16_384,
			MaxValueSize:               5_000,
			MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(1, 1)},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  50_000_000,
				Memory: 50_000_000,
			},
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
			ExecutionCosts: lcommon.ExUnitPrice{
				MemPrice:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				StepPrice: &cbor.Rat{Rat: big.NewRat(1, 1)},
			},
			CostModels: map[uint][]int64{
				3: defaultMachineCostModel(t, lang.LanguageVersionV4),
			},
		},
		MaxRefScriptSizePerTx: 100_000,
		RefScriptCostStride:   25_600,
		RefScriptCostMultiplier: &cbor.Rat{
			Rat: big.NewRat(6, 5),
		},
	}
	seed := bytes.Repeat([]byte{0x77}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	keyAddress := v4KeyAddress(t, privateKey)
	state := newMockLedgerState()
	output := func(amount uint64) gdijkstra.DijkstraTransactionOutput {
		return gdijkstra.DijkstraTransactionOutput{
			Output: babbage.BabbageTransactionOutput{
				OutputAddress: newTestKeyAddress(t),
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: amount},
			},
		}
	}
	inputAt := func(prefix string) shelley.ShelleyTransactionInput {
		return shelley.NewShelleyTransactionInput(
			prefix+"28b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee11",
			0,
		)
	}
	redeemerCount := len(spec.childScripts)
	if spec.topScript != nil {
		redeemerCount++
	}
	// Plutus V4 scripts are only accepted from reference scripts.
	refInput := func(
		prefix string,
		script lcommon.PlutusV4Script,
	) cbor.SetType[shelley.ShelleyTransactionInput] {
		input := inputAt(prefix)
		state.addUtxo(input, testAddressScriptOutput{
			testOutput: newTestOutput(5_000_000),
			addr:       newTestKeyAddress(t),
			scriptRef:  script,
		})
		return cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{input},
			false,
		)
	}
	fee := uint64(10_000_000*redeemerCount + 1_000)
	topInput := inputAt("a2")
	state.addUtxo(topInput, newTestOutputWithAddress(balance, keyAddress))
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{topInput},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{
				output(balance - fee),
			},
			TxFee:                    fee,
			TxRequiredTopLevelGuards: spec.required,
		},
		TxIsValid: true,
	}
	if redeemerCount > 0 {
		collateral := inputAt("e2")
		collateralAmount := uint64(30_000_000)
		state.addUtxo(collateral, newTestOutputWithAddress(
			collateralAmount,
			keyAddress,
		))
		tx.Body.TxCollateral = cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{collateral},
			false,
		)
		tx.Body.TxTotalCollateral = collateralAmount
	}
	guards := append([]lcommon.Credential(nil), spec.topGuards...)
	if spec.topScript != nil {
		guards = append(guards, v4ScriptCredential(spec.topScript))
		wits, hash := v4GuardingWitnesses(
			t,
			params,
			uint32(len(spec.topGuards)),
		)
		tx.WitnessSet = wits
		tx.Body.TxReferenceInputs = refInput("c2", spec.topScript)
		tx.Body.TxScriptDataHash = &hash
	}
	if len(guards) > 0 {
		tx.Body.TxGuards = &gdijkstra.DijkstraGuards{Credentials: guards}
	}
	children := make(
		[]gdijkstra.DijkstraSubTransaction,
		0,
		len(spec.childScripts),
	)
	for idx, script := range spec.childScripts {
		childInput := inputAt(string([]byte{'b', byte('0' + idx)}))
		state.addUtxo(childInput, newTestOutputWithAddress(balance, keyAddress))
		var childRequired gdijkstra.DijkstraRequiredTopLevelGuards
		if idx == 0 {
			childRequired = spec.childRequired
		}
		wits, hash := v4GuardingWitnesses(t, params, 0)
		child := gdijkstra.DijkstraSubTransaction{
			Body: gdijkstra.DijkstraSubTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{childInput},
				),
				TxOutputs: []gdijkstra.DijkstraTransactionOutput{
					output(balance),
				},
				TxRequiredTopLevelGuards: childRequired,
				TxScriptDataHash:         &hash,
				TxReferenceInputs: refInput(
					string([]byte{'d', byte('0' + idx)}),
					script,
				),
				TxGuards: &gdijkstra.DijkstraGuards{
					Credentials: []lcommon.Credential{
						v4ScriptCredential(script),
					},
				},
			},
			WitnessSet: wits,
		}
		childCbor, err := cbor.Encode(child.Body)
		require.NoError(t, err)
		child.Body.SetCbor(childCbor)
		childHash := lcommon.Blake2b256Hash(childCbor)
		child.WitnessSet.VkeyWitnesses = cbor.NewSetType(
			[]lcommon.VkeyWitness{{
				Vkey:      privateKey.Public().(ed25519.PublicKey),
				Signature: ed25519.Sign(privateKey, childHash[:]),
			}},
			false,
		)
		children = append(children, child)
	}
	if len(children) > 0 {
		tx.Body.TxSubTransactions = cbor.NewSetType(children, false)
	}
	return v4ContextFixture{
		tx:     tx,
		state:  state,
		params: params,
		key:    privateKey,
	}
}

// wire encodes the fixture transaction and decodes it again through the
// production decoder, so validation sees what a node receives from a peer or
// reads from a block.
func (f v4ContextFixture) wire(
	t *testing.T,
	spec v4ContextSpec,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	tx := f.tx
	bodyCbor, err := cbor.Encode(tx.Body)
	require.NoError(t, err)
	tx.Body.SetCbor(bodyCbor)
	{
		hash := tx.Hash()
		tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
			[]lcommon.VkeyWitness{{
				Vkey:      f.key.Public().(ed25519.PublicKey),
				Signature: ed25519.Sign(f.key, hash[:]),
			}},
			false,
		)
	}
	txCbor, err := cbor.Encode(tx)
	require.NoError(t, err)
	decoded, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return decoded
}

func requireV4ScriptFailure(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	_, ok := errors.AsType[conway.PlutusScriptFailedError](err)
	require.True(t, ok, "expected a Plutus script failure, got: %v", err)
}

func TestValidateTxDijkstraPlutusV4TopLevelContextOmitsSubTxIxAndTopTxInfo(
	t *testing.T,
) {
	t.Parallel()
	for _, test := range []struct {
		name string
		sel  func(ctx v4Term) v4Term
	}{
		{"txInfoSubTxIx", v4TxInfoSubTxIx},
		{"guarding TopTxInfo", v4GuardingTopTxInfo},
	} {
		t.Run(test.name+" is Nothing", func(t *testing.T) {
			t.Parallel()
			spec := v4ContextSpec{topScript: v4MaybeScript(t, test.sel, 1)}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			require.NoError(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
			)
		})
		t.Run(test.name+" is not Just", func(t *testing.T) {
			t.Parallel()
			spec := v4ContextSpec{topScript: v4MaybeScript(t, test.sel, 0)}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			requireV4ScriptFailure(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
			)
		})
	}
}

func TestValidateTxDijkstraPlutusV4ChildContextOmitsSubTxIxAndTopTxInfo(
	t *testing.T,
) {
	t.Parallel()
	for _, test := range []struct {
		name string
		sel  func(ctx v4Term) v4Term
	}{
		{"txInfoSubTxIx", v4TxInfoSubTxIx},
		{"guarding TopTxInfo", v4GuardingTopTxInfo},
	} {
		t.Run(test.name+" is Nothing for every child", func(t *testing.T) {
			t.Parallel()
			nothing := v4MaybeScript(t, test.sel, 1)
			spec := v4ContextSpec{
				childScripts: []lcommon.PlutusV4Script{nothing},
			}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			require.NoError(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
			)
		})
		t.Run(test.name+" is not Just for a child", func(t *testing.T) {
			t.Parallel()
			spec := v4ContextSpec{
				childScripts: []lcommon.PlutusV4Script{
					v4MaybeScript(t, test.sel, 0),
				},
			}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			requireV4ScriptFailure(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
			)
		})
	}
}

func v4KeyAddress(t *testing.T, key ed25519.PrivateKey) lcommon.Address {
	t.Helper()
	hash := lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey))
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		hash[:],
		nil,
	)
	require.NoError(t, err)
	return addr
}

func v4KeyCredential(key ed25519.PrivateKey) lcommon.Credential {
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.Blake2b224Hash(key.Public().(ed25519.PublicKey)),
	}
}

// v4RequiredGuards returns a required_top_level_guards map holding the
// script's own credential and the fixture key's credential.
func v4RequiredGuards(
	script lcommon.PlutusV4Script,
) (gdijkstra.DijkstraRequiredTopLevelGuards, lcommon.Credential) {
	key := v4KeyCredential(ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{0x77}, ed25519.SeedSize),
	))
	scriptCredential := v4ScriptCredential(script)
	return gdijkstra.DijkstraRequiredTopLevelGuards{
		&key:              nil,
		&scriptCredential: {Data: data.NewInteger(big.NewInt(7))},
	}, key
}

func TestValidateTxDijkstraRequiredTopLevelGuardsTopLevelBody(t *testing.T) {
	t.Parallel()
	script := v4RequiredGuardsOrderScript(t)
	required, key := v4RequiredGuards(script)

	t.Run(
		"satisfied guards reach the script in reference order",
		func(t *testing.T) {
			t.Parallel()
			spec := v4ContextSpec{
				topScript: script,
				required:  required,
				topGuards: []lcommon.Credential{key},
			}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			require.Len(t, tx.Body.TxRequiredTopLevelGuards, 2)
			require.NoError(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
			)
		},
	)
	t.Run("unsatisfied key guard is rejected", func(t *testing.T) {
		t.Parallel()
		spec := v4ContextSpec{topScript: script, required: required}
		fixture := newV4ContextFixture(t, spec)
		tx := fixture.wire(t, spec)
		err := ValidateTxDijkstra(tx, 0, fixture.state, fixture.params)
		var missing *gdijkstra.MissingRequiredGuards
		require.ErrorAs(t, err, &missing)
		require.Equal(t, []lcommon.Credential{key}, missing.Guards)
	})
}

func TestValidateTxDijkstraRequiredTopLevelGuardsChildBody(t *testing.T) {
	t.Parallel()
	childScript := v4MaybeScript(t, v4TxInfoSubTxIx, 1)
	_, key := v4RequiredGuards(childScript)
	required := gdijkstra.DijkstraRequiredTopLevelGuards{&key: nil}

	t.Run(
		"child requirement satisfied by the top-level guard set",
		func(t *testing.T) {
			t.Parallel()
			spec := v4ContextSpec{
				childScripts:  []lcommon.PlutusV4Script{childScript},
				childRequired: required,
				topGuards:     []lcommon.Credential{key},
			}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			require.Len(
				t,
				tx.Body.TxSubTransactions.Items()[0].Body.TxRequiredTopLevelGuards,
				1,
			)
			require.NoError(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
			)
		},
	)
	t.Run(
		"child requirement missing from the top-level guard set",
		func(t *testing.T) {
			t.Parallel()
			spec := v4ContextSpec{
				childScripts:  []lcommon.PlutusV4Script{childScript},
				childRequired: required,
			}
			fixture := newV4ContextFixture(t, spec)
			tx := fixture.wire(t, spec)
			var missing *gdijkstra.MissingRequiredGuards
			require.ErrorAs(
				t,
				ValidateTxDijkstra(tx, 0, fixture.state, fixture.params),
				&missing,
			)
			require.Equal(t, []lcommon.Credential{key}, missing.Guards)
		},
	)
}

// withTopLevelField24 returns the transaction CBOR with body key 24 replaced
// by the given raw value.
func withTopLevelField24(
	t *testing.T,
	txCbor []byte,
	field cbor.RawMessage,
) []byte {
	t.Helper()
	var parts []cbor.RawMessage
	_, err := cbor.Decode(txCbor, &parts)
	require.NoError(t, err)
	require.Len(t, parts, 3)
	var body map[uint]cbor.RawMessage
	_, err = cbor.Decode(parts[0], &body)
	require.NoError(t, err)
	body[24] = field
	parts[0], err = cbor.Encode(body)
	require.NoError(t, err)
	out, err := cbor.Encode(parts)
	require.NoError(t, err)
	return out
}

func TestDijkstraTopLevelRequiredGuardsWireDecode(t *testing.T) {
	t.Parallel()
	spec := v4ContextSpec{}
	fixture := newV4ContextFixture(t, spec)
	tx := fixture.wire(t, spec)
	txCbor := tx.Cbor()
	guard := bytes.Repeat([]byte{0x42}, 28)
	credential := append([]byte{0x82, 0x01, 0x58, 0x1c}, guard...)
	entry := func(datum ...byte) cbor.RawMessage {
		return append(append([]byte{0xa1}, credential...), datum...)
	}

	t.Run("valid entry decodes", func(t *testing.T) {
		t.Parallel()
		decoded, err := gdijkstra.NewDijkstraTransactionFromCbor(
			withTopLevelField24(t, txCbor, entry(0xf6)),
		)
		require.NoError(t, err)
		require.Len(t, decoded.Body.TxRequiredTopLevelGuards, 1)
	})
	for _, test := range []struct {
		name    string
		field   cbor.RawMessage
		wantErr string
	}{
		{"explicitly empty", cbor.RawMessage{0xa0}, "must not be empty"},
		{"not a map", cbor.RawMessage{0x01}, ""},
		{"datum is not Plutus data", entry(0xf5), "decode required guard datum"},
		{"datum is CBOR undefined", entry(0xf7), "decode required guard datum"},
		{
			"unknown credential type",
			append(
				append([]byte{0xa1, 0x82, 0x02, 0x58, 0x1c}, guard...),
				0xf6,
			),
			"",
		},
	} {
		t.Run("malformed: "+test.name, func(t *testing.T) {
			t.Parallel()
			decoded, err := gdijkstra.NewDijkstraTransactionFromCbor(
				withTopLevelField24(t, txCbor, test.field),
			)
			require.Error(t, err)
			require.Nil(t, decoded)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}
