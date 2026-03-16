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
	"github.com/blinklabs-io/gouroboros/cbor"
)

func TestMarshalBlockMetadataRoundTrip(t *testing.T) {
	expected := types.BlockMetadata{
		ID:       42,
		Type:     7,
		Height:   99,
		PrevHash: bytes.Repeat([]byte{0xab}, 32),
	}

	encoded := make([]byte, 32+len(expected.PrevHash))
	if err := marshalBlockMetadataInto(encoded, expected); err != nil {
		t.Fatalf("marshalBlockMetadataInto failed: %v", err)
	}
	if len(encoded) != 64 {
		t.Fatalf("expected encoded length 64, got %d", len(encoded))
	}

	decoded, err := unmarshalBlockMetadata(encoded)
	if err != nil {
		t.Fatalf("unmarshalBlockMetadata failed: %v", err)
	}
	if decoded.ID != expected.ID {
		t.Fatalf("expected ID %d, got %d", expected.ID, decoded.ID)
	}
	if decoded.Type != expected.Type {
		t.Fatalf("expected Type %d, got %d", expected.Type, decoded.Type)
	}
	if decoded.Height != expected.Height {
		t.Fatalf("expected Height %d, got %d", expected.Height, decoded.Height)
	}
	if !bytes.Equal(decoded.PrevHash, expected.PrevHash) {
		t.Fatalf("expected PrevHash %x, got %x", expected.PrevHash, decoded.PrevHash)
	}
}

func TestUnmarshalBlockMetadataLegacyCbor(t *testing.T) {
	expected := types.BlockMetadata{
		ID:       11,
		Type:     3,
		Height:   77,
		PrevHash: bytes.Repeat([]byte{0xcd}, 32),
	}

	encoded, err := cbor.Encode(expected)
	if err != nil {
		t.Fatalf("legacy cbor encode failed: %v", err)
	}

	decoded, err := unmarshalBlockMetadata(encoded)
	if err != nil {
		t.Fatalf("unmarshalBlockMetadata failed: %v", err)
	}
	if decoded.ID != expected.ID {
		t.Fatalf("expected ID %d, got %d", expected.ID, decoded.ID)
	}
	if decoded.Type != expected.Type {
		t.Fatalf("expected Type %d, got %d", expected.Type, decoded.Type)
	}
	if decoded.Height != expected.Height {
		t.Fatalf("expected Height %d, got %d", expected.Height, decoded.Height)
	}
	if !bytes.Equal(decoded.PrevHash, expected.PrevHash) {
		t.Fatalf("expected PrevHash %x, got %x", expected.PrevHash, decoded.PrevHash)
	}
}
