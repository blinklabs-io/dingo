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

package types

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// BlockImmutableMagic prefixes the value stored at a bp key once the block
// body has been promoted to the ImmutableDB chunk files. Like the tombstone
// magic it cannot begin a valid block CBOR (blocks start with an array
// header byte).
var BlockImmutableMagic = [4]byte{'D', 'I', 'M', '1'}

// BlockImmutableMarkerSize is the encoded size of an ImmutableMarker.
const BlockImmutableMarkerSize = 4 + 8 + 8 + 4 + 4

// ImmutableMarker locates a promoted block's wrapped bytes in a chunk file.
type ImmutableMarker struct {
	Chunk  uint64
	Offset uint64
	Size   uint32
	// CRC is the CRC-32 (IEEE) of the wrapped bytes.
	CRC uint32
}

// ErrBadImmutableMarker reports a marker that is the wrong length.
var ErrBadImmutableMarker = errors.New("malformed immutable block marker")

// Encode returns the marker's stored form.
func (m ImmutableMarker) Encode() []byte {
	out := make([]byte, BlockImmutableMarkerSize)
	copy(out, BlockImmutableMagic[:])
	binary.BigEndian.PutUint64(out[4:], m.Chunk)
	binary.BigEndian.PutUint64(out[12:], m.Offset)
	binary.BigEndian.PutUint32(out[20:], m.Size)
	binary.BigEndian.PutUint32(out[24:], m.CRC)
	return out
}

// IsImmutableMarker reports whether data is a promoted-block marker. It tests
// the magic prefix alone, so no body is ever misread as one nor a marker as a
// body.
func IsImmutableMarker(data []byte) bool {
	return bytes.HasPrefix(data, BlockImmutableMagic[:])
}

// DecodeImmutableMarker parses a marker value.
func DecodeImmutableMarker(data []byte) (ImmutableMarker, error) {
	if !IsImmutableMarker(data) || len(data) != BlockImmutableMarkerSize {
		return ImmutableMarker{}, ErrBadImmutableMarker
	}
	return ImmutableMarker{
		Chunk:  binary.BigEndian.Uint64(data[4:]),
		Offset: binary.BigEndian.Uint64(data[12:]),
		Size:   binary.BigEndian.Uint32(data[20:]),
		CRC:    binary.BigEndian.Uint32(data[24:]),
	}, nil
}

// RawValueItem is implemented by blob iterator items that can return the
// stored value without resolving an immutable marker, so a caller that only
// classifies a block (retained, expired) reads no block body.
type RawValueItem interface {
	RawValueCopy(dst []byte) ([]byte, error)
}
