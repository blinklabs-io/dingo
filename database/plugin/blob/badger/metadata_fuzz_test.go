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

package badger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
)

func FuzzCompactBlockMetadataRoundTrip(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint64(0), []byte(nil))
	f.Add(uint64(42), uint64(7), uint64(99), bytes.Repeat([]byte{0xab}, 32))
	f.Add(uint64(1), uint64(2), uint64(3), bytes.Repeat([]byte{0xcd}, 33))

	f.Fuzz(func(t *testing.T, id uint64, typeValue uint64, height uint64, prevHash []byte) {
		if len(prevHash) > blockMetadataPrevHashMaxLen {
			return
		}

		metadata := types.BlockMetadata{
			ID:       id,
			Type:     uint(typeValue),
			Height:   height,
			PrevHash: append([]byte(nil), prevHash...),
		}
		dst := make([]byte, 32+len(prevHash))
		err := marshalBlockMetadataInto(dst, metadata)
		if err != nil {
			t.Fatalf("marshalBlockMetadataInto: %v", err)
		}

		decoded, err := unmarshalBlockMetadata(dst)
		if err != nil {
			t.Fatalf("unmarshalBlockMetadata(compact): %v", err)
		}
		if decoded.ID != metadata.ID ||
			decoded.Type != metadata.Type ||
			decoded.Height != metadata.Height ||
			!bytes.Equal(decoded.PrevHash, metadata.PrevHash) {
			t.Fatalf("decoded metadata = %#v, want %#v", decoded, metadata)
		}
	})
}

func FuzzUnmarshalBlockMetadata(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("DBM1"))
	seed := make([]byte, 32)
	if err := marshalBlockMetadataInto(seed, types.BlockMetadata{ID: 1}); err != nil {
		f.Fatalf("marshal compact metadata seed: %v", err)
	}
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64*1024 {
			t.Skip("metadata input is too large for fast fuzzing")
		}

		metadata, err := unmarshalBlockMetadata(data)
		if err != nil {
			return
		}
		if len(metadata.PrevHash) > blockMetadataPrevHashMaxLen {
			t.Fatalf("metadata prev hash length = %d, want <= %d",
				len(metadata.PrevHash),
				blockMetadataPrevHashMaxLen,
			)
		}
	})
}
