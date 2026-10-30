// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"bytes"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

func TestConwayNativeScriptRejectsDijkstraConstructor(t *testing.T) {
	raw, err := cbor.Encode(common.NativeScriptRequireGuard{
		Type: 6,
		Credential: common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: common.CredentialHash{0x01},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("native_script", raw); err == nil {
		t.Fatal("accepted a Dijkstra native script constructor in Conway")
	}
	script, err := cbor.Encode([]any{
		common.ScriptRefTypeNativeScript,
		cbor.RawMessage(raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("script", script); err == nil {
		t.Fatal("accepted a Dijkstra native script constructor in a Conway script")
	}
}

func TestConwayRedeemerRejectsDijkstraTag(t *testing.T) {
	raw, err := cbor.Encode([]any{
		uint8(6),
		uint32(0),
		cbor.RawMessage{0x00},
		[]uint64{0, 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("redeemer", raw); err == nil {
		t.Fatal("accepted a Dijkstra redeemer tag in Conway")
	}
}

func TestConwayCorpusShapeConstraints(t *testing.T) {
	tests := []struct {
		name string
		rule string
		raw  []byte
	}{
		{name: "bignum over 64 bytes", rule: "big_int", raw: append([]byte{0xc2, 0x58, 0x41}, make([]byte, 65)...)},
		{name: "execution memory not integer", rule: "ex_units", raw: []byte{0x82, 0xf6, 0x00}},
		{name: "native script threshold not integer", rule: "native_script", raw: []byte{0x83, 0x03, 0xf6, 0x80}},
		{name: "redeemer tag not integer", rule: "redeemer", raw: []byte{0x84, 0xf6, 0x00, 0x00, 0x82, 0x00, 0x00}},
		{name: "redeemer map key tag not integer", rule: "redeemers", raw: []byte{0xa1, 0x82, 0xf6, 0x00, 0x82, 0x00, 0x82, 0x00, 0x00}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := reencode(test.rule, test.raw); err == nil {
				t.Fatalf("accepted invalid %s CBOR", test.rule)
			}
		})
	}
}

func TestConwayHeaderBodyRejectsIndefiniteVRFResult(t *testing.T) {
	fields := []cbor.RawMessage{
		{0x00}, {0x00}, {0xf6}, {0x40}, {0x40},
		{0x9f, 0x40, 0x40, 0xff}, {0x00}, {0x40}, {0x80}, {0x82, 0x00, 0x00},
	}
	raw, err := cbor.Encode(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateConwayHeaderBodyCBOR(raw); err == nil {
		t.Fatal("accepted an indefinite header-body VRF result")
	}
}

func TestConwayVotingProcedureRejectsIndefiniteArray(t *testing.T) {
	if _, err := reencode("voting_procedure", []byte{0x9f, 0x01, 0xf6, 0xff}); err == nil {
		t.Fatal("accepted an indefinite Conway voting procedure")
	}
}

func TestConwayAuxiliaryDataEnforcesMetadataValueLimit(t *testing.T) {
	for _, test := range []struct {
		name     string
		value    any
		wantFail bool
	}{
		{name: "text at limit", value: strings.Repeat("x", 64)},
		{name: "text above limit", value: strings.Repeat("x", 65), wantFail: true},
		{name: "bytes at limit", value: make([]byte, 64)},
		{name: "bytes above limit", value: make([]byte, 65), wantFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := cbor.Encode(map[uint]any{0: test.value})
			if err != nil {
				t.Fatal(err)
			}
			fields, err := cbor.Encode(map[uint]cbor.RawMessage{0: metadata})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := cbor.Encode(cbor.RawTag{
				Number:  cbor.CborTagMap,
				Content: fields,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = reencode("auxiliary_data", raw)
			if test.wantFail && err == nil {
				t.Fatal("accepted metadata value over the 64-byte limit")
			}
			if !test.wantFail && err != nil {
				t.Fatalf("rejected metadata value at the limit: %v", err)
			}
		})
	}
}

func TestConwayDataRequiresTag24WrappedPlutusData(t *testing.T) {
	inner := []byte{0x01}
	wrapper, err := cbor.Encode(cbor.WrappedCbor(inner))
	if err != nil {
		t.Fatal(err)
	}
	got, err := reencode("data", wrapper)
	if err != nil {
		t.Fatalf("rejected tag 24 wrapping valid Plutus data: %v", err)
	}
	want, err := cbor.Encode(cbor.WrappedCbor(inner))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("re-encoded data = %x, want %x", got, want)
	}

	plainBytes, err := cbor.Encode(inner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("data", plainBytes); err == nil {
		t.Fatal("accepted Plutus data without the required tag 24 wrapper")
	}
	if _, err := reencode("data", []byte{0xd8, 0x18, 0x5f, 0x41, 0x80, 0xff}); err == nil {
		t.Fatal("accepted an indefinite byte string around tag 24 Plutus data")
	}
}

func TestConwayPoolMetadataRejectsIndefiniteStrings(t *testing.T) {
	hash := append([]byte{0x58, 0x20}, make([]byte, 32)...)
	validFields := []cbor.RawMessage{{0x60}, hash}
	valid, err := cbor.Encode(validFields)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("pool_metadata", valid); err != nil {
		t.Fatalf("rejected definite pool metadata: %v", err)
	}

	indefiniteArray := append([]byte{0x9f}, validFields[0]...)
	indefiniteArray = append(indefiniteArray, hash...)
	indefiniteArray = append(indefiniteArray, 0xff)
	if _, err := reencode("pool_metadata", indefiniteArray); err != nil {
		t.Fatalf("rejected a valid indefinite outer array: %v", err)
	}

	indefiniteURL := append([]byte{0x82, 0x7f, 0x61, 'a', 0xff}, hash...)
	if _, err := reencode("pool_metadata", indefiniteURL); err == nil {
		t.Fatal("accepted an indefinite pool metadata URL")
	}
	indefiniteHash := append([]byte{0x82, 0x60, 0x5f, 0x58, 0x20}, make([]byte, 32)...)
	indefiniteHash = append(indefiniteHash, 0xff)
	if _, err := reencode("pool_metadata", indefiniteHash); err == nil {
		t.Fatal("accepted an indefinite pool metadata hash")
	}
}

func TestConwayMultiAssetRejectsIndefiniteStrings(t *testing.T) {
	policyID := append([]byte{0x58, 0x1c}, make([]byte, 28)...)
	valid := append([]byte{0xa1}, policyID...)
	valid = append(valid, 0xa1, 0x41, 'a', 0x01)
	indefinite := append([]byte{0xa1}, policyID...)
	indefinite = append(indefinite, 0xa1, 0x5f, 0x41, 'a', 0xff, 0x01)

	for _, rule := range []string{"mint", "%multiasset<positive_coin>"} {
		t.Run(rule, func(t *testing.T) {
			t.Parallel()
			if _, err := reencode(rule, valid); err != nil {
				t.Fatalf("rejected definite asset name: %v", err)
			}
			if _, err := reencode(rule, indefinite); err == nil {
				t.Fatal("accepted an indefinite asset-name byte string")
			}
		})
	}
}

func TestConwayMetadataRejectsDuplicateLabels(t *testing.T) {
	if _, err := reencode("metadata", []byte{0xa2, 0x01, 0x80, 0x01, 0x80}); err == nil {
		t.Fatal("accepted duplicate top-level metadata labels")
	}
}

func TestConwayTransactionBodyRejectsNullFieldValues(t *testing.T) {
	if _, err := reencode("transaction_body", []byte{0xa1, 0x03, 0xf6}); err == nil {
		t.Fatal("accepted a null transaction-body field value")
	}
}

func TestConwayBlockRejectsNullMapAndArrayFields(t *testing.T) {
	if err := validateConwayBlockCBOR([]byte{0x85, 0x80, 0x80, 0x80, 0xf6, 0x80}); err == nil {
		t.Fatal("accepted null for the block auxiliary-data map")
	}
	if err := validateConwayBlockCBOR([]byte{0x85, 0x80, 0x80, 0x80, 0xa0, 0xf6}); err == nil {
		t.Fatal("accepted null for the block invalid-transaction array")
	}
	if err := validateConwayBlockCBOR([]byte{0x84, 0x80, 0x80, 0x80, 0xa0}); err != nil {
		t.Fatalf("rejected a block without optional invalid-transaction indices: %v", err)
	}
}

func TestConwayBlockRejectsUnorderedInvalidTransactionIndexes(t *testing.T) {
	for _, indexes := range [][]uint64{{0, 0}, {1, 0}} {
		raw, err := cbor.Encode([]any{[]any{}, []any{}, []any{}, map[uint]any{}, indexes})
		if err != nil {
			t.Fatal(err)
		}
		if err := validateConwayBlockCBOR(raw); err == nil {
			t.Fatalf("accepted invalid transaction indexes %v", indexes)
		}
	}
}

func TestConwayOperationalCertRejectsIndefiniteByteStrings(t *testing.T) {
	valid, err := cbor.Encode([]any{
		make([]byte, 32), uint64(0), uint64(0), make([]byte, 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("operational_cert", valid); err != nil {
		t.Fatalf("rejected definite operational certificate: %v", err)
	}

	indefinite := append([]byte{0x84, 0x5f, 0x58, 0x20}, make([]byte, 32)...)
	indefinite = append(indefinite, 0xff, 0x00, 0x00, 0x5f, 0x58, 0x40)
	indefinite = append(indefinite, make([]byte, 64)...)
	indefinite = append(indefinite, 0xff, 0xff)
	if _, err := reencode("operational_cert", indefinite); err == nil {
		t.Fatal("accepted indefinite operational certificate byte strings")
	}
}

func TestConwayConstitutionRejectsIndefiniteScriptHash(t *testing.T) {
	indefiniteHash := append([]byte{0x82, 0x82, 0x60, 0x58, 0x20}, make([]byte, 32)...)
	indefiniteHash = append(indefiniteHash, 0xff, 0x5f, 0x58, 0x1c)
	indefiniteHash = append(indefiniteHash, make([]byte, 28)...)
	indefiniteHash = append(indefiniteHash, 0xff)
	if err := validateConwayConstitutionCBOR(indefiniteHash); err == nil {
		t.Fatal("accepted an indefinite constitution script hash")
	}

	validNull := []byte{0x82, 0x82, 0x60, 0x58, 0x20}
	validNull = append(validNull, make([]byte, 32)...)
	validNull = append(validNull, 0xf6)
	if err := validateConwayConstitutionCBOR(validNull); err != nil {
		t.Fatalf("rejected a constitution with no script hash: %v", err)
	}
}

func TestConwayConstitutionRejectsIndefiniteOuterArray(t *testing.T) {
	anchor, err := cbor.Encode(common.GovAnchor{
		Url:      "https://example.com",
		DataHash: [32]byte{},
	})
	if err != nil {
		t.Fatal(err)
	}
	valid, err := cbor.Encode([]cbor.RawMessage{anchor, {0xf6}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("constitution", valid); err != nil {
		t.Fatalf("rejected a definite constitution array: %v", err)
	}

	indefinite := append([]byte{0x9f}, valid[1:]...)
	indefinite = append(indefinite, 0xff)
	if _, err := reencode("constitution", indefinite); err == nil {
		t.Fatal("accepted an indefinite constitution array")
	}
}

func TestConwayMetadatumEnforcesMetadataValueLimit(t *testing.T) {
	for _, test := range []struct {
		name     string
		value    any
		wantFail bool
	}{
		{name: "text at limit", value: strings.Repeat("x", 64)},
		{name: "text above limit", value: strings.Repeat("x", 65), wantFail: true},
		{name: "bytes at limit", value: make([]byte, 64)},
		{name: "bytes above limit", value: make([]byte, 65), wantFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := cbor.Encode(test.value)
			if err != nil {
				t.Fatal(err)
			}
			_, err = reencode("metadatum", raw)
			if test.wantFail && err == nil {
				t.Fatal("accepted metadata value over the 64-byte limit")
			}
			if !test.wantFail && err != nil {
				t.Fatalf("rejected metadata value at the limit: %v", err)
			}
		})
	}
}

func TestConwayCostModelsRejectNullParameters(t *testing.T) {
	if _, err := reencode("cost_models", []byte{0xa1, 0x03, 0x82, 0xf6, 0x00}); err == nil {
		t.Fatal("accepted null as a cost-model integer")
	}
	if _, err := reencode("cost_models", []byte{0xa1, 0x03, 0x82, 0x01, 0x00}); err != nil {
		t.Fatalf("rejected integer cost-model parameters: %v", err)
	}
}

func TestConwayWithdrawalsRejectNullAmounts(t *testing.T) {
	address := make([]byte, 29)
	address[0] = 0xe0
	key, err := cbor.Encode(cbor.NewByteString(address))
	if err != nil {
		t.Fatal(err)
	}
	invalid := append([]byte{0xa1}, key...)
	invalid = append(invalid, 0xf6)
	if _, err := reencode("withdrawals", invalid); err == nil {
		t.Fatal("accepted null as a withdrawal amount")
	}
	valid := append([]byte{0xa1}, key...)
	valid = append(valid, 0x00)
	if _, err := reencode("withdrawals", valid); err != nil {
		t.Fatalf("rejected an unsigned withdrawal amount: %v", err)
	}
}

func TestConwayWithdrawalsUseLedgerMapOrder(t *testing.T) {
	hash := bytes.Repeat([]byte{0x33}, common.AddressHashSize)
	testnetScript, err := common.NewAddressFromParts(
		common.AddressTypeNoneScript,
		common.AddressNetworkTestnet,
		nil,
		hash,
	)
	if err != nil {
		t.Fatal(err)
	}
	testnetKey, err := common.NewAddressFromParts(
		common.AddressTypeNoneKey,
		common.AddressNetworkTestnet,
		nil,
		hash,
	)
	if err != nil {
		t.Fatal(err)
	}
	mainnetKey, err := common.NewAddressFromParts(
		common.AddressTypeNoneKey,
		common.AddressNetworkMainnet,
		nil,
		hash,
	)
	if err != nil {
		t.Fatal(err)
	}
	values := map[*common.Address]uint64{
		&mainnetKey:    3,
		&testnetKey:    2,
		&testnetScript: 1,
	}
	raw, err := cbor.Encode(values)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := reencodeWithdrawals(raw)
	if err != nil {
		t.Fatal(err)
	}
	addresses := []*common.Address{&testnetScript, &testnetKey, &mainnetKey}
	amounts := []uint64{1, 2, 3}
	expected := []byte{0xa3}
	for i, address := range addresses {
		encodedAddress, err := cbor.Encode(address)
		if err != nil {
			t.Fatal(err)
		}
		encodedAmount, err := cbor.Encode(amounts[i])
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, encodedAddress...)
		expected = append(expected, encodedAmount...)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("ledger-ordered withdrawal map = %x, want %x", actual, expected)
	}
	if bytes.Equal(actual, raw) {
		t.Fatal("input already used ledger ordering; fixture does not distinguish encoders")
	}
}

func TestConwayProtocolVersionUsesSpecifiedIntegerWidths(t *testing.T) {
	for _, test := range []struct {
		name     string
		rule     string
		raw      []byte
		wantFail bool
	}{
		{name: "maximum minor version", rule: "protocol_version", raw: []byte{0x82, 0x00, 0x1a, 0xff, 0xff, 0xff, 0xff}},
		{name: "indefinite version array", rule: "protocol_version", raw: []byte{0x9f, 0x00, 0x01, 0xff}},
		{name: "major version limit", rule: "major_protocol_version", raw: []byte{0x0d}},
		{name: "major version above limit", rule: "major_protocol_version", raw: []byte{0x0e}, wantFail: true},
		{name: "protocol major above limit", rule: "protocol_version", raw: []byte{0x82, 0x0e, 0x00}, wantFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := reencode(test.rule, test.raw)
			if test.wantFail && err == nil {
				t.Fatal("accepted an out-of-range major protocol version")
			}
			if !test.wantFail && err != nil {
				t.Fatalf("rejected a protocol version in range: %v", err)
			}
		})
	}
}

func TestConwayAuxiliaryDataMapRejectsNullScript(t *testing.T) {
	content, err := cbor.Encode(map[uint][]any{2: {nil}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cbor.Encode(cbor.RawTag{Number: cbor.CborTagMap, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("auxiliary_data_map", raw); err == nil {
		t.Fatal("accepted null as an auxiliary script")
	}
}

func TestConwayTransactionOutputMapsRequireAddressAndValue(t *testing.T) {
	address := make([]byte, 29)
	address[0] = 0x61
	raw, err := cbor.Encode(map[uint]any{0: cbor.NewByteString(address)})
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"transaction_output", "babbage_transaction_output"} {
		t.Run(rule, func(t *testing.T) {
			if _, err := reencode(rule, raw); err == nil {
				t.Fatal("accepted a map output without its required value field")
			}
		})
	}
}

func TestConwayProposalProceduresRejectDuplicateElements(t *testing.T) {
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	proposal, err := cbor.Encode([]any{
		uint64(0),
		cbor.NewByteString(rewardAccount),
		[]any{uint8(6)},
		[]any{"", cbor.NewByteString(make([]byte, 32))},
	})
	if err != nil {
		t.Fatal(err)
	}
	setContent, err := cbor.Encode([]cbor.RawMessage{proposal, proposal})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cbor.Encode(cbor.RawTag{Number: cbor.CborTagSet, Content: setContent})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("proposal_procedures", raw); err == nil {
		t.Fatal("accepted duplicate Conway proposal procedures")
	}
}

func TestConwayCertificateSetsCanonicalizePoolOwners(t *testing.T) {
	operator := make([]byte, 28)
	vrfKeyHash := make([]byte, 32)
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	owner := make([]byte, 28)
	poolCertificate, err := cbor.Encode([]any{
		uint64(common.CertificateTypePoolRegistration),
		cbor.NewByteString(operator),
		cbor.NewByteString(vrfKeyHash),
		uint64(1),
		uint64(1),
		common.NewGenesisRat(1, 2),
		cbor.NewByteString(rewardAccount),
		[]any{cbor.NewByteString(owner)},
		[]any{},
		nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	bareSet, err := cbor.Encode([]cbor.RawMessage{poolCertificate})
	if err != nil {
		t.Fatal(err)
	}

	got, err := reencode("certificates", bareSet)
	if err != nil {
		t.Fatalf("re-encode certificate set: %v", err)
	}
	canonicalCertificate, err := reencode("certificate", poolCertificate)
	if err != nil {
		t.Fatalf("re-encode pool registration certificate: %v", err)
	}
	items, err := cbor.Encode([]cbor.RawMessage{canonicalCertificate})
	if err != nil {
		t.Fatal(err)
	}
	want, err := cbor.Encode(cbor.RawTag{
		Number:  cbor.CborTagSet,
		Content: items,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("re-encoded certificate set = %x, want %x", got, want)
	}
}

func TestConwayProposalProcedureSetsUseTag258(t *testing.T) {
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	anchorHash := make([]byte, 32)
	proposal, err := cbor.Encode([]any{
		uint64(0),
		cbor.NewByteString(rewardAccount),
		[]any{uint64(common.GovActionTypeInfo)},
		[]any{"", cbor.NewByteString(anchorHash)},
	})
	if err != nil {
		t.Fatal(err)
	}
	bareSet, err := cbor.Encode([]cbor.RawMessage{proposal})
	if err != nil {
		t.Fatal(err)
	}
	canonicalProposal, err := reencode("proposal_procedure", proposal)
	if err != nil {
		t.Fatalf("re-encode proposal procedure: %v", err)
	}
	items, err := cbor.Encode([]cbor.RawMessage{canonicalProposal})
	if err != nil {
		t.Fatal(err)
	}
	want, err := cbor.Encode(cbor.RawTag{Number: cbor.CborTagSet, Content: items})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reencode("proposal_procedures", bareSet)
	if err != nil {
		t.Fatalf("re-encode proposal-procedure set: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("re-encoded proposal-procedure set = %x, want %x", got, want)
	}
}

func TestBabbageTransactionOutputUsesLedgerEncoding(t *testing.T) {
	address := make([]byte, 29)
	address[0] = 0x61
	datumHash := make([]byte, 32)
	for _, test := range []struct {
		name          string
		fields        map[uint]any
		want          []any
		wantMapOutput bool
	}{
		{
			name:   "map without datum re-encodes as legacy array",
			fields: map[uint]any{0: cbor.NewByteString(address), 1: uint64(5)},
			want:   []any{cbor.NewByteString(address), uint64(5)},
		},
		{
			name: "datum hash map re-encodes as legacy array",
			fields: map[uint]any{
				0: cbor.NewByteString(address),
				1: uint64(5),
				2: []any{uint64(babbage.DatumOptionTypeHash), cbor.NewByteString(datumHash)},
			},
			want: []any{
				cbor.NewByteString(address),
				uint64(5),
				cbor.NewByteString(datumHash),
			},
		},
		{
			name: "inline datum remains a map",
			fields: map[uint]any{
				0: cbor.NewByteString(address),
				1: uint64(5),
				2: []any{uint64(babbage.DatumOptionTypeData), cbor.Tag{Number: cbor.CborTagCbor, Content: []byte{0x01}}},
			},
			wantMapOutput: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := cbor.Encode(test.fields)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reencode("babbage_transaction_output", raw)
			if err != nil {
				t.Fatalf("re-encode output: %v", err)
			}
			if test.wantMapOutput {
				if len(got) == 0 || got[0]>>5 != 5 {
					t.Fatalf("re-encoded output = %x, want a map", got)
				}
				return
			}
			want, err := cbor.Encode(test.want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("re-encoded output = %x, want %x", got, want)
			}
		})
	}
}

func TestConwayHardForkActionRejectsOversizedProtocolVersion(t *testing.T) {
	raw, err := cbor.Encode([]any{
		uint8(1),
		nil,
		[]uint64{4, 77_120_077_821_055},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reencode("gov_action", raw); err == nil {
		t.Fatal("accepted a hard-fork action with a minor version above uint32")
	}
}
