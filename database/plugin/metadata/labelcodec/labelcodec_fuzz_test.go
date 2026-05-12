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
	"math"
	"math/big"
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
			if err != nil {
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
