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
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func FuzzExtractFromCborRawValues(f *testing.F) {
	f.Add([]byte(nil))
	addMetadatumSeed(f, lcommon.MetaMap{
		Pairs: []lcommon.MetaPair{
			{
				Key:   lcommon.MetaInt{Value: big.NewInt(721)},
				Value: lcommon.MetaText{Value: "hello"},
			},
		},
	})
	addMetadatumSeed(f, lcommon.MetaMap{
		Pairs: []lcommon.MetaPair{
			{
				Key: lcommon.MetaInt{
					Value: new(big.Int).SetUint64(math.MaxUint64),
				},
				Value: lcommon.MetaList{
					Items: []lcommon.TransactionMetadatum{
						lcommon.MetaBytes{Value: []byte{0x00, 0x01, 0xff}},
						lcommon.MetaInt{Value: big.NewInt(-1)},
						lcommon.MetaMap{
							Pairs: []lcommon.MetaPair{
								{
									Key:   lcommon.MetaText{Value: "nested"},
									Value: lcommon.MetaText{Value: "value"},
								},
							},
						},
					},
				},
			},
		},
	})

	f.Fuzz(func(t *testing.T, metadataCbor []byte) {
		if len(metadataCbor) > 64*1024 {
			t.Skip("metadata corpus input is too large for fast fuzzing")
		}

		entries, err := extractFromCbor(metadataCbor)
		if err != nil {
			return
		}

		for i, entry := range entries {
			if i > 0 && entries[i-1].Label >= entry.Label {
				t.Fatalf(
					"entries not strictly sorted by label: %d before %d",
					entries[i-1].Label,
					entry.Label,
				)
			}

			jsonValue, cborValue, err := RawValues(metadataCbor, entry.Label)
			if err != nil && !errors.Is(err, ErrJSONUnavailable) {
				t.Fatalf("RawValues(label=%d): %v", entry.Label, err)
			}
			if string(jsonValue) != entry.JsonValue {
				t.Fatalf(
					"RawValues JSON for label %d = %s, want %s",
					entry.Label,
					jsonValue,
					entry.JsonValue,
				)
			}
			if !bytes.Equal(cborValue, entry.CborValue) {
				t.Fatalf(
					"RawValues CBOR for label %d = %x, want %x",
					entry.Label,
					cborValue,
					entry.CborValue,
				)
			}
		}
	})
}

func addMetadatumSeed(f *testing.F, metadata lcommon.TransactionMetadatum) {
	metadataCbor, err := metadatumCbor(metadata)
	if err != nil {
		f.Fatalf("metadatumCbor seed: %v", err)
	}
	f.Add(metadataCbor)
}

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

// decodeVector is an independently encoded metadata map. The expected values
// are written out by hand from the CBOR bytes, not derived from the codec.
type decodeVector struct {
	name    string
	encoded string
	want    []decodeWant
}

type decodeWant struct {
	label uint64
	cbor  string
	// json is empty when the JSON projection must be unavailable.
	json string
}

func decodeVectors() []decodeVector {
	const (
		repLabel = "016161" // 1: "a"
		// 721 with keys integer 1 and text "1" colliding in JSON.
		collideIntFirst  = "a20163696e7461316474657874"
		collideTextFirst = "a2613164746578740163696e74"
	)
	repWant := decodeWant{label: 1, cbor: "6161", json: `"a"`}
	return []decodeVector{
		{
			name:    "collision value first, representable label second",
			encoded: "a21902d1" + collideIntFirst + repLabel,
			want: []decodeWant{
				repWant,
				{label: 721, cbor: collideIntFirst},
			},
		},
		{
			name:    "collision value second, representable label first",
			encoded: "a2" + repLabel + "1902d1" + collideIntFirst,
			want: []decodeWant{
				repWant,
				{label: 721, cbor: collideIntFirst},
			},
		},
		{
			name:    "collision keys reversed, label order reversed",
			encoded: "a21902d1" + collideTextFirst + repLabel,
			want: []decodeWant{
				repWant,
				{label: 721, cbor: collideTextFirst},
			},
		},
		{
			name:    "collision keys reversed, label order ascending",
			encoded: "a2" + repLabel + "1902d1" + collideTextFirst,
			want: []decodeWant{
				repWant,
				{label: 721, cbor: collideTextFirst},
			},
		},
		{
			name:    "non-colliding nested keys ascending",
			encoded: "a1" + "02" + "a2" + "6161" + "01" + "6162" + "02",
			want: []decodeWant{
				{label: 2, cbor: "a2616101616202", json: `{"a":1,"b":2}`},
			},
		},
		{
			name:    "non-colliding nested keys descending",
			encoded: "a1" + "02" + "a2" + "6162" + "02" + "6161" + "01",
			want: []decodeWant{
				{label: 2, cbor: "a2616202616101", json: `{"a":1,"b":2}`},
			},
		},
		{
			name:    "definite labels in descending order",
			encoded: "a3" + "1902d1" + "6130" + "02" + "6162" + "01" + "6161",
			want: []decodeWant{
				{label: 1, cbor: "6161", json: `"a"`},
				{label: 2, cbor: "6162", json: `"b"`},
				{label: 721, cbor: "6130", json: `"0"`},
			},
		},
	}
}

func TestDecodeIsDeterministicAcrossRepeatsAndKeyOrders(t *testing.T) {
	t.Parallel()
	const repeats = 64
	for _, vec := range decodeVectors() {
		t.Run(vec.name, func(t *testing.T) {
			t.Parallel()
			metadataCbor, err := hex.DecodeString(vec.encoded)
			if err != nil {
				t.Fatal(err)
			}
			for i := range repeats {
				entries, err := EntriesFromCBOR(metadataCbor)
				if err != nil {
					t.Fatalf("decode %d: %v", i, err)
				}
				if len(entries) != len(vec.want) {
					t.Fatalf(
						"decode %d: got %d entries, want %d",
						i, len(entries), len(vec.want),
					)
				}
				for j, want := range vec.want {
					got := entries[j]
					if got.Label != want.label ||
						hex.EncodeToString(got.CborValue) != want.cbor {
						t.Fatalf(
							"decode %d entry %d: label=%d cbor=%x, want label=%d cbor=%s",
							i, j, got.Label, got.CborValue, want.label, want.cbor,
						)
					}
					if want.json == "" {
						if !errors.Is(got.JSONError, ErrJSONUnavailable) ||
							got.JsonValue != "" {
							t.Fatalf(
								"decode %d label %d: want unavailable JSON, got %q (%v)",
								i, want.label, got.JsonValue, got.JSONError,
							)
						}
					} else if got.JSONError != nil || got.JsonValue != want.json {
						t.Fatalf(
							"decode %d label %d: JSON %q (%v), want %q",
							i, want.label, got.JsonValue, got.JSONError, want.json,
						)
					}

					jsonValue, rawCbor, err := RawValues(metadataCbor, want.label)
					if string(jsonValue) != got.JsonValue ||
						!bytes.Equal(rawCbor, got.CborValue) ||
						errors.Is(err, ErrJSONUnavailable) != (want.json == "") {
						t.Fatalf(
							"decode %d label %d: RawValues disagrees with EntriesFromCBOR: %q %x %v",
							i, want.label, jsonValue, rawCbor, err,
						)
					}
					onlyCbor, err := RawValue(metadataCbor, want.label)
					if err != nil || !bytes.Equal(onlyCbor, got.CborValue) {
						t.Fatalf(
							"decode %d label %d: RawValue = %x (%v)",
							i, want.label, onlyCbor, err,
						)
					}
				}
			}
		})
	}
}

// TestDecodeOutcomesMatchAcrossLabelOrders compares encodings that differ only
// in the order of top-level labels.
func TestDecodeOutcomesMatchAcrossLabelOrders(t *testing.T) {
	t.Parallel()
	vectors := decodeVectors()
	byName := make(map[string]decodeVector, len(vectors))
	for _, v := range vectors {
		byName[v.name] = v
	}
	pairs := [][2]string{
		{
			"collision value first, representable label second",
			"collision value second, representable label first",
		},
		{
			"collision keys reversed, label order reversed",
			"collision keys reversed, label order ascending",
		},
	}
	for _, pair := range pairs {
		first, second := byName[pair[0]], byName[pair[1]]
		a, err := hex.DecodeString(first.encoded)
		if err != nil {
			t.Fatal(err)
		}
		b, err := hex.DecodeString(second.encoded)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(a, b) {
			t.Fatalf("vectors %q and %q must encode differently", pair[0], pair[1])
		}
		entriesA, errA := EntriesFromCBOR(a)
		entriesB, errB := EntriesFromCBOR(b)
		if errA != nil || errB != nil || len(entriesA) != len(entriesB) {
			t.Fatalf("decode: %v, %v", errA, errB)
		}
		for i := range entriesA {
			if entriesA[i].Label != entriesB[i].Label ||
				entriesA[i].JsonValue != entriesB[i].JsonValue ||
				!bytes.Equal(entriesA[i].CborValue, entriesB[i].CborValue) ||
				errors.Is(entriesA[i].JSONError, ErrJSONUnavailable) !=
					errors.Is(entriesB[i].JSONError, ErrJSONUnavailable) {
				t.Fatalf("entry %d differs: %#v vs %#v", i, entriesA[i], entriesB[i])
			}
		}
	}
}

func TestInvalidTopLevelLabelsRejectedInEveryOrder(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ encoded, wantErr string }{
		"duplicate then distinct": {"a3" + "016161" + "016162" + "026163", "duplicate metadata label"},
		"distinct then duplicate": {"a3" + "026163" + "016161" + "016162", "duplicate metadata label"},
		"duplicate big label":     {"a2" + "1902d1" + "6161" + "1902d1" + "6162", "duplicate metadata label"},
		"negative label first":    {"a2" + "20" + "6161" + "01" + "6162", "negative metadata label"},
		"negative label last":     {"a2" + "01" + "6162" + "20" + "6161", "negative metadata label"},
		"text label":              {"a1" + "6131" + "6161", "not an integer-keyed map"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			metadataCbor, err := hex.DecodeString(tc.encoded)
			if err != nil {
				t.Fatal(err)
			}
			for range 16 {
				entries, err := EntriesFromCBOR(metadataCbor)
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("EntriesFromCBOR(%x) = %#v, %v; want error containing %q", metadataCbor, entries, err, tc.wantErr)
				}
				if _, _, err := RawValues(metadataCbor, 1); err == nil {
					t.Fatalf("RawValues accepted %x", metadataCbor)
				}
				if _, err := RawValue(metadataCbor, 1); err == nil {
					t.Fatalf("RawValue accepted %x", metadataCbor)
				}
			}
		})
	}
}
