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

package ledgerstate

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
)

func FuzzDecodeTxInFromBytes(f *testing.F) {
	f.Add([]byte(nil))
	f.Add(make([]byte, 34))
	addTxInArraySeed(f, bytes.Repeat([]byte{0xab}, 32), 7)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64*1024 {
			t.Skip("TxIn input is too large for fast fuzzing")
		}

		txHash, outputIndex, err := decodeTxInFromBytes(data)
		if err != nil {
			return
		}
		if len(data) == 34 {
			if !bytes.Equal(txHash, data[:32]) {
				t.Fatalf("binary TxIn hash = %x, want %x", txHash, data[:32])
			}
			expectedIndex := uint32(binary.BigEndian.Uint16(data[32:34]))
			if outputIndex != expectedIndex {
				t.Fatalf("binary TxIn index = %d, want %d", outputIndex, expectedIndex)
			}
		}
	})
}

func FuzzUnwrapCborBytes(f *testing.F) {
	addByteStringSeed(f, []byte(nil))
	addByteStringSeed(f, []byte{0xde, 0xad, 0xbe, 0xef})
	f.Add([]byte{0xd8, 0x18, 0x42, 0x01, 0x02})
	f.Add([]byte(nil))

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64*1024 {
			t.Skip("CBOR input is too large for fast fuzzing")
		}

		unwrapped, err := unwrapCborBytes(cbor.RawMessage(raw))
		if err != nil {
			return
		}
		if len(unwrapped) > 64*1024 {
			t.Fatalf("unwrapCborBytes returned %d bytes, want <= 65536", len(unwrapped))
		}

		encoded, err := cbor.Encode([]byte(unwrapped))
		if err != nil {
			t.Fatalf("cbor.Encode(unwrapped): %v", err)
		}
		roundTrip, err := unwrapCborBytes(cbor.RawMessage(encoded))
		if err != nil {
			t.Fatalf("unwrapCborBytes(re-encoded unwrapped): %v", err)
		}
		if !bytes.Equal(roundTrip, unwrapped) {
			t.Fatalf("re-encoded unwrap = %x, want %x", roundTrip, unwrapped)
		}
	})
}

func addTxInArraySeed(f *testing.F, txHash []byte, outputIndex uint32) {
	data, err := cbor.Encode([]any{txHash, outputIndex})
	if err != nil {
		f.Fatalf("marshal TxIn seed: %v", err)
	}
	f.Add(data)
}

func addByteStringSeed(f *testing.F, data []byte) {
	encoded, err := cbor.Encode(data)
	if err != nil {
		f.Fatalf("marshal byte string seed: %v", err)
	}
	f.Add(encoded)
}
