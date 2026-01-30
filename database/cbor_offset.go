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
	"encoding/binary"
	"fmt"
)

// CborOffsetSize is the size in bytes of an encoded CborOffset.
// Layout: BlockSlot (8) + BlockHash (32) + ByteOffset (4) + ByteLength (4) = 48
const CborOffsetSize = 48

// CborOffset represents a reference to CBOR data within a block.
// Instead of storing duplicate CBOR data, we store an offset reference
// that points to the CBOR within the block's raw data.
type CborOffset struct {
	BlockSlot  uint64   // Slot number of the block containing the CBOR
	BlockHash  [32]byte // Hash of the block containing the CBOR
	ByteOffset uint32   // Byte offset within the block's CBOR data
	ByteLength uint32   // Length of the CBOR data in bytes
}

// Encode serializes the CborOffset to a 48-byte big-endian encoded slice.
// Layout:
//   - bytes 0-7:   BlockSlot (big-endian uint64)
//   - bytes 8-39:  BlockHash (32 bytes)
//   - bytes 40-43: ByteOffset (big-endian uint32)
//   - bytes 44-47: ByteLength (big-endian uint32)
func (c *CborOffset) Encode() []byte {
	buf := make([]byte, CborOffsetSize)

	// Encode BlockSlot (bytes 0-7)
	binary.BigEndian.PutUint64(buf[0:8], c.BlockSlot)

	// Copy BlockHash (bytes 8-39)
	copy(buf[8:40], c.BlockHash[:])

	// Encode ByteOffset (bytes 40-43)
	binary.BigEndian.PutUint32(buf[40:44], c.ByteOffset)

	// Encode ByteLength (bytes 44-47)
	binary.BigEndian.PutUint32(buf[44:48], c.ByteLength)

	return buf
}

// DecodeCborOffset deserializes a 48-byte big-endian encoded slice into a CborOffset.
// Returns an error if the input data is not exactly 48 bytes.
func DecodeCborOffset(data []byte) (*CborOffset, error) {
	if len(data) != CborOffsetSize {
		return nil, fmt.Errorf(
			"invalid CborOffset data size: expected %d bytes, got %d",
			CborOffsetSize,
			len(data),
		)
	}

	offset := &CborOffset{}

	// Decode BlockSlot (bytes 0-7)
	offset.BlockSlot = binary.BigEndian.Uint64(data[0:8])

	// Copy BlockHash (bytes 8-39)
	copy(offset.BlockHash[:], data[8:40])

	// Decode ByteOffset (bytes 40-43)
	offset.ByteOffset = binary.BigEndian.Uint32(data[40:44])

	// Decode ByteLength (bytes 44-47)
	offset.ByteLength = binary.BigEndian.Uint32(data[44:48])

	return offset, nil
}
