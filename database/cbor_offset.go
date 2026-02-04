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
	"errors"
	"fmt"
)

// offsetMagic is a 4-byte prefix that identifies offset-based storage.
// This prevents misidentifying legacy CBOR that happens to be the same size.
// Using a fixed-size array makes it immutable (unlike a slice).
var offsetMagic = [4]byte{'D', 'O', 'F', 'F'} // "DOFF" = Dingo OFFset

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
	buf := make([]byte, CborOffsetSize)

	// Magic prefix (bytes 0-3)
	copy(buf[0:4], offsetMagic[:])

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
	if data[0] != offsetMagic[0] || data[1] != offsetMagic[1] ||
		data[2] != offsetMagic[2] || data[3] != offsetMagic[3] {
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

// EncodeUtxoOffset encodes a CborOffset for UTxO storage.
// Returns a 52-byte encoded offset with magic prefix.
func EncodeUtxoOffset(offset *CborOffset) []byte {
	return offset.Encode()
}

// IsUtxoOffsetStorage checks if the data is offset-based UTxO storage.
// Returns true if data has the correct size and magic prefix.
func IsUtxoOffsetStorage(data []byte) bool {
	if len(data) != CborOffsetSize {
		return false
	}
	return data[0] == offsetMagic[0] && data[1] == offsetMagic[1] &&
		data[2] == offsetMagic[2] && data[3] == offsetMagic[3]
}

// DecodeUtxoOffset decodes offset-based UTxO storage data.
// Returns an error if the data is not exactly 52 bytes or has wrong magic.
func DecodeUtxoOffset(data []byte) (*CborOffset, error) {
	return DecodeCborOffset(data)
}

// EncodeTxOffset encodes a CborOffset for transaction storage.
// Returns a 52-byte encoded offset with magic prefix.
func EncodeTxOffset(offset *CborOffset) []byte {
	return offset.Encode()
}

// IsTxOffsetStorage checks if the data is offset-based transaction storage.
// Returns true if data has the correct size and magic prefix.
func IsTxOffsetStorage(data []byte) bool {
	if len(data) != CborOffsetSize {
		return false
	}
	return data[0] == offsetMagic[0] && data[1] == offsetMagic[1] &&
		data[2] == offsetMagic[2] && data[3] == offsetMagic[3]
}

// DecodeTxOffset decodes offset-based transaction storage data.
// Returns an error if the data is not exactly 52 bytes or has wrong magic.
func DecodeTxOffset(data []byte) (*CborOffset, error) {
	return DecodeCborOffset(data)
}

// txPartsMagic identifies TxCborParts storage (complete transaction components).
var txPartsMagic = [4]byte{'D', 'T', 'X', 'P'} // "DTXP" = Dingo TX Parts

// TxCborPartsSize is the size in bytes of an encoded TxCborParts.
// Layout: Magic (4) + BlockSlot (8) + BlockHash (32) +
//
//	BodyOffset (4) + BodyLength (4) +
//	WitnessOffset (4) + WitnessLength (4) +
//	MetadataOffset (4) + MetadataLength (4) +
//	IsValid (1) = 69
const TxCborPartsSize = 69

// TxCborParts stores byte offsets for all 4 components of a Cardano transaction.
// This enables byte-perfect reconstruction of standalone transaction CBOR
// from the source block.
//
// A complete standalone transaction has the CBOR structure:
//
//	[body, witness, is_valid, metadata]
//
// However, in Cardano blocks, these components are stored in separate arrays:
//
//	[header, [bodies...], [witnesses...], {metadata_map}, [invalid_txs]]
//
// TxCborParts stores the location of each component so they can be extracted
// and reassembled into a complete transaction.
type TxCborParts struct {
	BlockSlot      uint64   // Slot number of the block containing the transaction
	BlockHash      [32]byte // Hash of the block containing the transaction
	BodyOffset     uint32   // Byte offset of transaction body within block CBOR
	BodyLength     uint32   // Length of transaction body in bytes
	WitnessOffset  uint32   // Byte offset of witness set within block CBOR
	WitnessLength  uint32   // Length of witness set in bytes
	MetadataOffset uint32   // Byte offset of metadata within block CBOR (0 if none)
	MetadataLength uint32   // Length of metadata in bytes (0 if none)
	IsValid        bool     // True if transaction is valid (not in invalid_txs)
}

// Encode serializes TxCborParts to a 69-byte big-endian encoded slice.
// Layout:
//   - bytes 0-3:   Magic "DTXP"
//   - bytes 4-11:  BlockSlot (big-endian uint64)
//   - bytes 12-43: BlockHash (32 bytes)
//   - bytes 44-47: BodyOffset (big-endian uint32)
//   - bytes 48-51: BodyLength (big-endian uint32)
//   - bytes 52-55: WitnessOffset (big-endian uint32)
//   - bytes 56-59: WitnessLength (big-endian uint32)
//   - bytes 60-63: MetadataOffset (big-endian uint32)
//   - bytes 64-67: MetadataLength (big-endian uint32)
//   - byte 68:     IsValid (0 = false, 1 = true)
func (t *TxCborParts) Encode() []byte {
	buf := make([]byte, TxCborPartsSize)

	// Magic prefix (bytes 0-3)
	copy(buf[0:4], txPartsMagic[:])

	// Encode BlockSlot (bytes 4-11)
	binary.BigEndian.PutUint64(buf[4:12], t.BlockSlot)

	// Copy BlockHash (bytes 12-43)
	copy(buf[12:44], t.BlockHash[:])

	// Encode body offset/length (bytes 44-51)
	binary.BigEndian.PutUint32(buf[44:48], t.BodyOffset)
	binary.BigEndian.PutUint32(buf[48:52], t.BodyLength)

	// Encode witness offset/length (bytes 52-59)
	binary.BigEndian.PutUint32(buf[52:56], t.WitnessOffset)
	binary.BigEndian.PutUint32(buf[56:60], t.WitnessLength)

	// Encode metadata offset/length (bytes 60-67)
	binary.BigEndian.PutUint32(buf[60:64], t.MetadataOffset)
	binary.BigEndian.PutUint32(buf[64:68], t.MetadataLength)

	// Encode IsValid (byte 68)
	if t.IsValid {
		buf[68] = 1
	} else {
		buf[68] = 0
	}

	return buf
}

// DecodeTxCborParts deserializes a 69-byte big-endian encoded slice into TxCborParts.
// Returns an error if the input data is not exactly 69 bytes or has wrong magic.
func DecodeTxCborParts(data []byte) (*TxCborParts, error) {
	if len(data) != TxCborPartsSize {
		return nil, fmt.Errorf(
			"invalid TxCborParts data size: expected %d bytes, got %d",
			TxCborPartsSize,
			len(data),
		)
	}

	// Verify magic prefix
	if data[0] != txPartsMagic[0] || data[1] != txPartsMagic[1] ||
		data[2] != txPartsMagic[2] || data[3] != txPartsMagic[3] {
		return nil, errors.New("invalid TxCborParts magic: expected DTXP")
	}

	parts := &TxCborParts{}

	// Decode BlockSlot (bytes 4-11)
	parts.BlockSlot = binary.BigEndian.Uint64(data[4:12])

	// Copy BlockHash (bytes 12-43)
	copy(parts.BlockHash[:], data[12:44])

	// Decode body offset/length (bytes 44-51)
	parts.BodyOffset = binary.BigEndian.Uint32(data[44:48])
	parts.BodyLength = binary.BigEndian.Uint32(data[48:52])

	// Decode witness offset/length (bytes 52-59)
	parts.WitnessOffset = binary.BigEndian.Uint32(data[52:56])
	parts.WitnessLength = binary.BigEndian.Uint32(data[56:60])

	// Decode metadata offset/length (bytes 60-67)
	parts.MetadataOffset = binary.BigEndian.Uint32(data[60:64])
	parts.MetadataLength = binary.BigEndian.Uint32(data[64:68])

	// Decode IsValid (byte 68)
	parts.IsValid = data[68] != 0

	return parts, nil
}

// IsTxCborPartsStorage checks if the data is TxCborParts storage.
// Returns true if data has the correct size and magic prefix.
func IsTxCborPartsStorage(data []byte) bool {
	if len(data) != TxCborPartsSize {
		return false
	}
	return data[0] == txPartsMagic[0] && data[1] == txPartsMagic[1] &&
		data[2] == txPartsMagic[2] && data[3] == txPartsMagic[3]
}

// HasMetadata returns true if the transaction has metadata.
func (t *TxCborParts) HasMetadata() bool {
	return t.MetadataLength > 0
}

// ReassembleTxCbor extracts all transaction components from the block CBOR
// and reassembles them into a complete standalone transaction CBOR.
//
// The returned CBOR has the structure: [body, witness, is_valid, metadata]
// where metadata is null if the transaction has no auxiliary data.
//
// This produces byte-perfect output when the original transaction had this
// exact structure. Note that Cardano blocks store components in separate
// arrays, so the reassembled CBOR may differ from tx.Cbor() which may use
// a different encoding order or structure.
func (t *TxCborParts) ReassembleTxCbor(blockCbor []byte) ([]byte, error) {
	// Safe conversion: Cardano blocks are limited to ~90KB, well within uint32 range.
	blockLen := uint32(len(blockCbor)) // #nosec G115: bounded by Cardano protocol limits

	// Validate body bounds (overflow-safe: check offset first, then remaining space)
	if t.BodyOffset > blockLen || t.BodyLength > blockLen-t.BodyOffset {
		return nil, fmt.Errorf(
			"body range [%d:%d] exceeds block size %d",
			t.BodyOffset, t.BodyOffset+t.BodyLength, blockLen,
		)
	}

	// Validate witness bounds (overflow-safe)
	if t.WitnessOffset > blockLen || t.WitnessLength > blockLen-t.WitnessOffset {
		return nil, fmt.Errorf(
			"witness range [%d:%d] exceeds block size %d",
			t.WitnessOffset, t.WitnessOffset+t.WitnessLength, blockLen,
		)
	}

	// Validate metadata bounds if present (overflow-safe)
	if t.HasMetadata() && (t.MetadataOffset > blockLen || t.MetadataLength > blockLen-t.MetadataOffset) {
		return nil, fmt.Errorf(
			"metadata range [%d:%d] exceeds block size %d",
			t.MetadataOffset, t.MetadataOffset+t.MetadataLength, blockLen,
		)
	}

	// Extract components
	bodyCbor := blockCbor[t.BodyOffset : t.BodyOffset+t.BodyLength]
	witnessCbor := blockCbor[t.WitnessOffset : t.WitnessOffset+t.WitnessLength]

	var metadataCbor []byte
	if t.HasMetadata() {
		metadataCbor = blockCbor[t.MetadataOffset : t.MetadataOffset+t.MetadataLength]
	}

	// Build the complete transaction CBOR array: [body, witness, is_valid, metadata]
	// Using CBOR indefinite-length array encoding for flexibility
	return buildTxCborArray(bodyCbor, witnessCbor, t.IsValid, metadataCbor), nil
}

// buildTxCborArray constructs a CBOR array containing the transaction components.
// Format: [body, witness, is_valid, metadata_or_null]
func buildTxCborArray(body, witness []byte, isValid bool, metadata []byte) []byte {
	// Calculate total size needed
	// CBOR array header (1 byte for 4-element array: 0x84)
	// + body bytes + witness bytes
	// + is_valid (1 byte: 0xF5 for true, 0xF4 for false)
	// + metadata bytes (or 0xF6 for null)
	size := 1 + len(body) + len(witness) + 1
	if len(metadata) > 0 {
		size += len(metadata)
	} else {
		size++ // null takes 1 byte
	}

	result := make([]byte, 0, size)

	// CBOR array header: 4-element array
	result = append(result, 0x84)

	// Body (already CBOR-encoded)
	result = append(result, body...)

	// Witness (already CBOR-encoded)
	result = append(result, witness...)

	// is_valid boolean
	if isValid {
		result = append(result, 0xf5) // CBOR true
	} else {
		result = append(result, 0xf4) // CBOR false
	}

	// Metadata or null
	if len(metadata) > 0 {
		result = append(result, metadata...)
	} else {
		result = append(result, 0xf6) // CBOR null
	}

	return result
}
