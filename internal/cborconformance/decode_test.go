// Copyright 2026 Blink Labs Software

package cborconformance

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
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
