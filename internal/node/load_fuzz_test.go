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

package node

import (
	"bytes"
	"testing"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	fxcbor "github.com/fxamacker/cbor/v2"
)

func FuzzCborArrayHeaderLen(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{0x80})
	f.Add([]byte{0x98, 0x02})
	f.Add([]byte{0x9f, 0xff})
	f.Add([]byte{0xa0})

	f.Fuzz(func(t *testing.T, data []byte) {
		headerLen, err := cborArrayHeaderLen(data)
		if err != nil {
			return
		}
		if headerLen < 1 || headerLen > 9 {
			t.Fatalf("header length = %d, want 1..9", headerLen)
		}
		if headerLen > len(data) {
			t.Fatalf("header length %d exceeds input length %d", headerLen, len(data))
		}
		if data[0]&gcbor.CborTypeMask != gcbor.CborTypeArray {
			t.Fatalf("accepted non-array CBOR major type: 0x%x", data[0])
		}
	})
}

func FuzzExtractHeaderCbor(f *testing.F) {
	addBlockSeed(f, []byte{0x01}, []byte{0x02})
	addBlockSeed(f, []byte{0x82, 0x00, 0x01}, []byte{0x80})
	f.Add([]byte(nil))
	f.Add([]byte{0x80})
	f.Add([]byte{0x81, 0x01})

	f.Fuzz(func(t *testing.T, blockCbor []byte) {
		if len(blockCbor) > 64*1024 {
			t.Skip("block CBOR input is too large for fast fuzzing")
		}

		header, err := extractHeaderCbor(blockCbor)
		if err != nil {
			return
		}
		if len(header) == 0 {
			t.Fatalf("extractHeaderCbor returned an empty header")
		}

		headerLen, err := cborArrayHeaderLen(blockCbor)
		if err != nil {
			t.Fatalf("cborArrayHeaderLen rejected block accepted by extractHeaderCbor: %v", err)
		}
		if !bytes.HasPrefix(blockCbor[headerLen:], header) {
			t.Fatalf("extracted header %x is not the first element after array header", header)
		}
	})
}

func FuzzExtractInvalidTxIndices(f *testing.F) {
	addInvalidTxSeed(f, []uint{})
	addInvalidTxSeed(f, []uint{0, 1, 42})
	f.Add([]byte(nil))
	f.Add([]byte{0x80})

	f.Fuzz(func(t *testing.T, blockCbor []byte) {
		if len(blockCbor) > 64*1024 {
			t.Skip("block CBOR input is too large for fast fuzzing")
		}

		indices, err := extractInvalidTxIndices(blockCbor)
		if err != nil {
			return
		}
		for idx := range indices {
			if idx < 0 {
				t.Fatalf("invalid transaction index is negative: %d", idx)
			}
		}
	})
}

func FuzzTxBodyMapValueRange(f *testing.F) {
	addTxBodyMapSeed(f, map[uint64]any{
		0: uint64(1),
		7: []byte{0xde, 0xad, 0xbe, 0xef},
	}, 7)
	addTxBodyMapSeed(f, map[uint64]any{
		2: []uint64{1, 2, 3},
	}, 2)
	f.Add([]byte(nil), uint32(0), uint64(0))
	f.Add([]byte{0xa0}, uint32(0), uint64(0))

	f.Fuzz(func(t *testing.T, bodyCbor []byte, bodyOffset uint32, key uint64) {
		if len(bodyCbor) > 64*1024 {
			t.Skip("tx body CBOR input is too large for fast fuzzing")
		}

		valueOffset, valueLen, found, err := txBodyMapValueRange(bodyCbor, bodyOffset, key)
		if err != nil || !found {
			return
		}
		if valueOffset < bodyOffset {
			t.Fatalf("value offset %d precedes body offset %d", valueOffset, bodyOffset)
		}
		relativeOffset := uint64(valueOffset - bodyOffset)
		end := relativeOffset + uint64(valueLen)
		if end > uint64(len(bodyCbor)) {
			t.Fatalf(
				"value range [%d:%d] exceeds tx body length %d",
				relativeOffset,
				end,
				len(bodyCbor),
			)
		}
	})
}

func addBlockSeed(f *testing.F, header, body []byte) {
	blockCbor, err := fxcbor.Marshal([]gcbor.RawMessage{
		gcbor.RawMessage(header),
		gcbor.RawMessage(body),
	})
	if err != nil {
		f.Fatalf("marshal block seed: %v", err)
	}
	f.Add(blockCbor)
}

func addInvalidTxSeed(f *testing.F, invalidTxs []uint) {
	blockCbor, err := gcbor.Encode([]any{
		uint64(0),
		[]any{},
		[]any{},
		map[uint64]any{},
		invalidTxs,
	})
	if err != nil {
		f.Fatalf("marshal invalid tx seed: %v", err)
	}
	f.Add(blockCbor)
}

func addTxBodyMapSeed(f *testing.F, body map[uint64]any, key uint64) {
	bodyCbor, err := gcbor.Encode(body)
	if err != nil {
		f.Fatalf("marshal tx body seed: %v", err)
	}
	f.Add(bodyCbor, uint32(12), key)
}
