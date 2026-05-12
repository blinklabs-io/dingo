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

package database

import (
	"bytes"
	"math"
	"testing"
)

func FuzzCborOffsetRoundTrip(f *testing.F) {
	f.Add(uint64(0), make([]byte, 32), uint32(0), uint32(0))
	f.Add(
		uint64(12345678),
		bytes.Repeat([]byte{0xab}, 32),
		uint32(1024),
		uint32(256),
	)
	f.Add(
		uint64(math.MaxUint64),
		bytes.Repeat([]byte{0xff}, 32),
		uint32(math.MaxUint32),
		uint32(math.MaxUint32),
	)

	f.Fuzz(func(
		t *testing.T,
		blockSlot uint64,
		blockHashBytes []byte,
		byteOffset uint32,
		byteLength uint32,
	) {
		var blockHash [32]byte
		copy(blockHash[:], blockHashBytes)

		original := CborOffset{
			BlockSlot:  blockSlot,
			BlockHash:  blockHash,
			ByteOffset: byteOffset,
			ByteLength: byteLength,
		}
		decoded, err := DecodeCborOffset(original.Encode())
		if err != nil {
			t.Fatalf("DecodeCborOffset(encoded): %v", err)
		}
		if *decoded != original {
			t.Fatalf("decoded offset = %#v, want %#v", *decoded, original)
		}
	})
}

func FuzzDecodeCborOffset(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("DOFF"))
	f.Add((&CborOffset{
		BlockSlot:  42,
		BlockHash:  [32]byte{0x01, 0x02, 0x03},
		ByteOffset: 12,
		ByteLength: 34,
	}).Encode())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeCborOffset(data)
		if err != nil {
			if IsUtxoOffsetStorage(data) || IsTxOffsetStorage(data) {
				t.Fatalf("storage detector accepted data DecodeCborOffset rejected: %v", err)
			}
			return
		}

		if !IsUtxoOffsetStorage(data) {
			t.Fatalf("IsUtxoOffsetStorage returned false for decoded offset")
		}
		if !IsTxOffsetStorage(data) {
			t.Fatalf("IsTxOffsetStorage returned false for decoded offset")
		}
		if !bytes.Equal(decoded.Encode(), data) {
			t.Fatalf("DecodeCborOffset re-encoded to %x, want %x", decoded.Encode(), data)
		}
	})
}

func FuzzDecodeTxCborParts(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("DTXP"))
	f.Add((&TxCborParts{
		BlockSlot:      42,
		BlockHash:      [32]byte{0x01, 0x02, 0x03},
		BodyOffset:     1,
		BodyLength:     2,
		WitnessOffset:  3,
		WitnessLength:  4,
		MetadataOffset: 5,
		MetadataLength: 6,
		IsValid:        true,
	}).Encode())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeTxCborParts(data)
		if err != nil {
			if IsTxCborPartsStorage(data) {
				t.Fatalf("storage detector accepted data DecodeTxCborParts rejected: %v", err)
			}
			return
		}

		if !IsTxCborPartsStorage(data) {
			t.Fatalf("IsTxCborPartsStorage returned false for decoded tx parts")
		}

		roundTrip, err := DecodeTxCborParts(decoded.Encode())
		if err != nil {
			t.Fatalf("DecodeTxCborParts(decoded.Encode()): %v", err)
		}
		if *roundTrip != *decoded {
			t.Fatalf("round-trip tx parts = %#v, want %#v", *roundTrip, *decoded)
		}
	})
}
