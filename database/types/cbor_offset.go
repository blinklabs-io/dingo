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
	"encoding/binary"
	"errors"
	"fmt"
)

// CborOffsetMagic is a 4-byte prefix that identifies offset-based storage.
// This prevents misidentifying legacy CBOR that happens to be the same size.
// Using a fixed-size array makes it immutable (unlike a slice).
var CborOffsetMagic = [4]byte{'D', 'O', 'F', 'F'} // "DOFF" = Dingo OFFset

// CborOffsetSize is the size in bytes of an encoded CborOffset.
// Layout: Magic (4) + BlockSlot (8) + BlockHash (32) + ByteOffset (4) + ByteLength (4) = 52
const CborOffsetSize = 52

// CborOffset represents a reference to CBOR data within a block.
// Instead of storing duplicate CBOR data, we store an offset reference
// that points to the CBOR within the block's raw data.
type CborOffset struct {
	BlockSlot  uint64   // Slot number of the block containing the CBOR
	BlockHash  [32]byte // Hash of the block containing the CBOR
	ByteOffset uint32   // Byte offset within the block's CBOR data
	ByteLength uint32   // Length of the CBOR data in bytes
}

// Encode serializes the CborOffset to a 52-byte big-endian encoded slice.
// Layout:
//   - bytes 0-3:   Magic "DOFF" (identifies offset storage)
//   - bytes 4-11:  BlockSlot (big-endian uint64)
//   - bytes 12-43: BlockHash (32 bytes)
//   - bytes 44-47: ByteOffset (big-endian uint32)
//   - bytes 48-51: ByteLength (big-endian uint32)
func (c *CborOffset) Encode() []byte {
	return c.encode(CborOffsetMagic)
}

// EndorserOffsetMagic is the prefix of an offset into the transaction list of
// a Leios endorser block, as the LeiosDB stores it, rather than into a chain
// block. BlockSlot and BlockHash name the endorser block.
var EndorserOffsetMagic = [4]byte{'D', 'L', 'E', 'B'} // "DLEB" = Dingo LEios Block

// EncodeEndorser serializes the CborOffset with EndorserOffsetMagic. Its
// offsets are positions in the CBOR encoding of the endorser block's raw
// transaction list.
func (c *CborOffset) EncodeEndorser() []byte {
	return c.encode(EndorserOffsetMagic)
}

// IsEndorserOffset reports whether data is an encoded endorser-block offset.
func IsEndorserOffset(data []byte) bool {
	return len(data) == CborOffsetSize &&
		[4]byte(data[0:4]) == EndorserOffsetMagic
}

// DecodeEndorserOffset deserializes an endorser-block offset.
func DecodeEndorserOffset(data []byte) (*CborOffset, error) {
	if !IsEndorserOffset(data) {
		return nil, errors.New("invalid endorser offset: expected DLEB")
	}
	var plain [CborOffsetSize]byte
	copy(plain[:], data)
	copy(plain[0:4], CborOffsetMagic[:])
	return DecodeCborOffset(plain[:])
}

func (c *CborOffset) encode(magic [4]byte) []byte {
	buf := make([]byte, CborOffsetSize)

	// Magic prefix (bytes 0-3)
	copy(buf[0:4], magic[:])

	// Encode BlockSlot (bytes 4-11)
	binary.BigEndian.PutUint64(buf[4:12], c.BlockSlot)

	// Copy BlockHash (bytes 12-43)
	copy(buf[12:44], c.BlockHash[:])

	// Encode ByteOffset (bytes 44-47)
	binary.BigEndian.PutUint32(buf[44:48], c.ByteOffset)

	// Encode ByteLength (bytes 48-51)
	binary.BigEndian.PutUint32(buf[48:52], c.ByteLength)

	return buf
}

// DecodeCborOffset deserializes a 52-byte big-endian encoded slice into a CborOffset.
// Returns an error if the input data is not exactly 52 bytes or has wrong magic.
func DecodeCborOffset(data []byte) (*CborOffset, error) {
	if len(data) != CborOffsetSize {
		return nil, fmt.Errorf(
			"invalid CborOffset data size: expected %d bytes, got %d",
			CborOffsetSize,
			len(data),
		)
	}

	// Verify magic prefix
	if data[0] != CborOffsetMagic[0] || data[1] != CborOffsetMagic[1] ||
		data[2] != CborOffsetMagic[2] || data[3] != CborOffsetMagic[3] {
		return nil, errors.New("invalid CborOffset magic: expected DOFF")
	}

	offset := &CborOffset{}

	// Decode BlockSlot (bytes 4-11)
	offset.BlockSlot = binary.BigEndian.Uint64(data[4:12])

	// Copy BlockHash (bytes 12-43)
	copy(offset.BlockHash[:], data[12:44])

	// Decode ByteOffset (bytes 44-47)
	offset.ByteOffset = binary.BigEndian.Uint32(data[44:48])

	// Decode ByteLength (bytes 48-51)
	offset.ByteLength = binary.BigEndian.Uint32(data[48:52])

	return offset, nil
}

// ExpiredPayloadMagic is the prefix of the 52-byte value a payload column
// holds when the block that carried its bytes had expired, with no archive to
// fetch it from, when the column was converted. It names the block and holds
// no bytes: a read fails with the expired block named.
var ExpiredPayloadMagic = [4]byte{'D', 'E', 'X', 'P'} // "DEXP" = Dingo EXPired

// EncodeExpiredPayload returns the column value for a payload of the expired
// block at slot and hash.
func EncodeExpiredPayload(slot uint64, hash []byte) []byte {
	offset := CborOffset{BlockSlot: slot}
	copy(offset.BlockHash[:], hash)
	buf := offset.encode(ExpiredPayloadMagic)
	return buf
}

// IsExpiredPayload reports whether data is an expired-payload marker.
func IsExpiredPayload(data []byte) bool {
	return len(data) == CborOffsetSize &&
		[4]byte(data[0:4]) == ExpiredPayloadMagic
}

// DecodeExpiredPayload returns the expired block an expired-payload marker
// names.
func DecodeExpiredPayload(data []byte) (*HistoryExpiredError, error) {
	if !IsExpiredPayload(data) {
		return nil, errors.New("invalid expired payload marker: expected DEXP")
	}
	return &HistoryExpiredError{
		Slot: binary.BigEndian.Uint64(data[4:12]),
		Hash: append([]byte(nil), data[12:44]...),
	}, nil
}
