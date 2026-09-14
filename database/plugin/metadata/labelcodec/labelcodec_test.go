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

package labelcodec

import (
	"encoding/hex"
	"errors"
	"math/big"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func TestExtractFromMetadatumRejectsNonIntegerTopLevelKey(t *testing.T) {
	_, err := extractFromMetadatum(lcommon.MetaMap{
		Pairs: []lcommon.MetaPair{
			{
				Key:   lcommon.MetaText{Value: "721"},
				Value: lcommon.MetaText{Value: "hello"},
			},
		},
	})
	if err == nil || err.Error() != "metadata is not an integer-keyed map" {
		t.Fatalf("expected integer-keyed map error, got %v", err)
	}
}

func TestExtractFromMetadatumRejectsNilIntegerLabel(t *testing.T) {
	_, err := extractFromMetadatum(lcommon.MetaMap{
		Pairs: []lcommon.MetaPair{
			{
				Key:   lcommon.MetaInt{},
				Value: lcommon.MetaText{Value: "hello"},
			},
		},
	})
	if err == nil ||
		err.Error() != "invalid metadata label: nil integer value" {
		t.Fatalf("expected nil integer label error, got %v", err)
	}
}

func TestExtractFromMetadatumAcceptsValidIntegerKey(t *testing.T) {
	entries, err := extractFromMetadatum(lcommon.MetaMap{
		Pairs: []lcommon.MetaPair{
			{
				Key: lcommon.MetaInt{
					Value: new(big.Int).SetUint64(721),
				},
				Value: lcommon.MetaText{Value: "hello"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 || entries[0].Label != 721 {
		t.Fatalf("unexpected entries: %#v", entries)
	}
}

func TestMetadatumToJSONValueRejectsCollidingMapKeys(t *testing.T) {
	md := lcommon.MetaMap{
		Pairs: []lcommon.MetaPair{
			{
				Key:   lcommon.MetaInt{Value: big.NewInt(1)},
				Value: lcommon.MetaText{Value: "integer key"},
			},
			{
				Key:   lcommon.MetaText{Value: "1"},
				Value: lcommon.MetaText{Value: "text key"},
			},
		},
	}

	_, err := metadatumToJSONValue(md)
	if err == nil {
		t.Fatal("expected colliding metadata keys to be rejected")
	}
	if !errors.Is(err, ErrJSONUnavailable) || err.Error() != `metadata JSON representation unavailable: metadata map keys "1" collide` {
		t.Fatalf("unexpected collision error: %v", err)
	}
}

func TestMetadatumToJSONValueRejectsAllKeyStringCollisions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  lcommon.TransactionMetadatum
		text string
	}{
		{name: "bytes", key: lcommon.MetaBytes{Value: []byte{0xab}}, text: "ab"},
		{name: "list", key: lcommon.MetaList{Items: []lcommon.TransactionMetadatum{lcommon.MetaInt{Value: big.NewInt(1)}}}, text: "[1]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, pairs := range [][]lcommon.MetaPair{
				{{Key: tc.key, Value: lcommon.MetaText{Value: "a"}}, {Key: lcommon.MetaText{Value: tc.text}, Value: lcommon.MetaText{Value: "b"}}},
				{{Key: lcommon.MetaText{Value: tc.text}, Value: lcommon.MetaText{Value: "b"}}, {Key: tc.key, Value: lcommon.MetaText{Value: "a"}}},
			} {
				_, err := metadatumToJSONValue(lcommon.MetaMap{Pairs: pairs})
				if !errors.Is(err, ErrJSONUnavailable) {
					t.Fatalf("expected unavailable JSON, got %v", err)
				}
			}
		})
	}
	value := lcommon.MetaList{Items: []lcommon.TransactionMetadatum{lcommon.MetaMap{Pairs: []lcommon.MetaPair{
		{Key: lcommon.MetaInt{Value: big.NewInt(1)}, Value: lcommon.MetaText{Value: "a"}},
		{Key: lcommon.MetaText{Value: "1"}, Value: lcommon.MetaText{Value: "b"}},
	}}}}
	_, err := metadatumToJSONValue(value)
	if !errors.Is(err, ErrJSONUnavailable) {
		t.Fatalf("expected nested collision to propagate, got %v", err)
	}
}

func TestEntriesFromCBORRejectsEncodedKeyCollisions(t *testing.T) {
	for _, encoded := range []string{
		"a11902d1a20163696e7461316474657874",
		"a11902d1a2613164746578740163696e74",
	} {
		metadataCbor, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := EntriesFromCBOR(metadataCbor)
		if err != nil || len(entries) != 1 {
			t.Fatalf("expected encoded key collision to remain indexed: %s: %v", encoded, err)
		}
		if !errors.Is(entries[0].JSONError, ErrJSONUnavailable) || len(entries[0].CborValue) == 0 || entries[0].JsonValue != "" {
			t.Fatalf("expected unavailable JSON with raw CBOR: %#v", entries[0])
		}
		rawValue, err := RawValue(metadataCbor, 721)
		if err != nil || string(rawValue) != string(entries[0].CborValue) {
			t.Fatalf("expected raw label retrieval, got %x (%v)", rawValue, err)
		}
	}
}

func TestEncodeAndExtractKeepsAmbiguousLabelAndCBOR(t *testing.T) {
	md := lcommon.MetaMap{Pairs: []lcommon.MetaPair{{
		Key: lcommon.MetaInt{Value: big.NewInt(721)},
		Value: lcommon.MetaMap{Pairs: []lcommon.MetaPair{
			{Key: lcommon.MetaInt{Value: big.NewInt(1)}, Value: lcommon.MetaText{Value: "integer"}},
			{Key: lcommon.MetaText{Value: "1"}, Value: lcommon.MetaText{Value: "text"}},
		}},
	}}}
	metadataCbor, entries, err := EncodeAndExtract(md)
	if err != nil || len(metadataCbor) == 0 || len(entries) != 1 {
		t.Fatalf("expected metadata to remain indexable: cbor=%x entries=%#v err=%v", metadataCbor, entries, err)
	}
	if entries[0].Label != 721 || entries[0].JsonValue != "" || len(entries[0].CborValue) == 0 || !errors.Is(entries[0].JSONError, ErrJSONUnavailable) {
		t.Fatalf("unexpected ambiguous entry: %#v", entries[0])
	}
}

func TestDuplicateMetadataLabelRejectedInBothExtractionPaths(t *testing.T) {
	metadataCbor, err := hex.DecodeString("a2016161016162")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EntriesFromCBOR(metadataCbor); err == nil || !strings.Contains(err.Error(), "duplicate metadata label") {
		t.Fatalf("expected duplicate encoded label rejection, got %v", err)
	}
	md := lcommon.MetaMap{Pairs: []lcommon.MetaPair{
		{Key: lcommon.MetaInt{Value: big.NewInt(1)}, Value: lcommon.MetaText{Value: "a"}},
		{Key: lcommon.MetaInt{Value: big.NewInt(1)}, Value: lcommon.MetaText{Value: "b"}},
	}}
	if _, err := extractFromMetadatum(md); err == nil || !strings.Contains(err.Error(), "duplicate metadata label") {
		t.Fatalf("expected duplicate fallback label rejection, got %v", err)
	}
	if _, _, err := EncodeAndExtract(md); err == nil {
		t.Fatal("expected encoded duplicate label rejection")
	}
}

func TestDuplicateNestedEncodedKeyIsUnavailableInJSON(t *testing.T) {
	metadataCbor, err := hex.DecodeString("a11902d1a2016161016162")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := EntriesFromCBOR(metadataCbor)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected duplicate nested key to remain indexed: %v", err)
	}
	if !errors.Is(entries[0].JSONError, ErrJSONUnavailable) || len(entries[0].CborValue) == 0 {
		t.Fatalf("expected unavailable JSON with raw CBOR: %#v", entries[0])
	}
}
