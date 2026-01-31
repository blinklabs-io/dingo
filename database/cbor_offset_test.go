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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCborOffsetEncodeDecode(t *testing.T) {
	testCases := []struct {
		name       string
		blockSlot  uint64
		blockHash  [32]byte
		byteOffset uint32
		byteLength uint32
	}{
		{
			name:       "zero values",
			blockSlot:  0,
			blockHash:  [32]byte{},
			byteOffset: 0,
			byteLength: 0,
		},
		{
			name:      "typical values",
			blockSlot: 12345678,
			blockHash: [32]byte{
				0x01,
				0x02,
				0x03,
				0x04,
				0x05,
				0x06,
				0x07,
				0x08,
				0x09,
				0x0a,
				0x0b,
				0x0c,
				0x0d,
				0x0e,
				0x0f,
				0x10,
				0x11,
				0x12,
				0x13,
				0x14,
				0x15,
				0x16,
				0x17,
				0x18,
				0x19,
				0x1a,
				0x1b,
				0x1c,
				0x1d,
				0x1e,
				0x1f,
				0x20,
			},
			byteOffset: 1024,
			byteLength: 256,
		},
		{
			name:      "max uint64 slot",
			blockSlot: math.MaxUint64,
			blockHash: [32]byte{
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
				0xff,
			},
			byteOffset: math.MaxUint32,
			byteLength: math.MaxUint32,
		},
		{
			name:      "large slot number",
			blockSlot: 1000000000000,
			blockHash: [32]byte{
				0xab,
				0xcd,
				0xef,
				0x12,
				0x34,
				0x56,
				0x78,
				0x9a,
				0xbc,
				0xde,
				0xf0,
				0x11,
				0x22,
				0x33,
				0x44,
				0x55,
				0x66,
				0x77,
				0x88,
				0x99,
				0xaa,
				0xbb,
				0xcc,
				0xdd,
				0xee,
				0xff,
				0x00,
				0x11,
				0x22,
				0x33,
				0x44,
				0x55,
			},
			byteOffset: 500000,
			byteLength: 100000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := CborOffset{
				BlockSlot:  tc.blockSlot,
				BlockHash:  tc.blockHash,
				ByteOffset: tc.byteOffset,
				ByteLength: tc.byteLength,
			}

			// Test encode
			encoded := original.Encode()
			require.Len(
				t,
				encoded,
				CborOffsetSize,
				"encoded size should be %d bytes",
				CborOffsetSize,
			)

			// Test decode
			decoded, err := DecodeCborOffset(encoded)
			require.NoError(t, err, "decode should not error")
			require.NotNil(t, decoded, "decoded should not be nil")

			// Verify round-trip
			assert.Equal(
				t,
				original.BlockSlot,
				decoded.BlockSlot,
				"BlockSlot mismatch",
			)
			assert.Equal(
				t,
				original.BlockHash,
				decoded.BlockHash,
				"BlockHash mismatch",
			)
			assert.Equal(
				t,
				original.ByteOffset,
				decoded.ByteOffset,
				"ByteOffset mismatch",
			)
			assert.Equal(
				t,
				original.ByteLength,
				decoded.ByteLength,
				"ByteLength mismatch",
			)
		})
	}
}

func TestCborOffsetEncodeFormat(t *testing.T) {
	// Test that encoding is big-endian and in the expected format
	offset := CborOffset{
		BlockSlot: 0x0102030405060708, // 8 bytes
		BlockHash: [32]byte{
			0x10,
			0x11,
			0x12,
			0x13,
			0x14,
			0x15,
			0x16,
			0x17,
			0x18,
			0x19,
			0x1a,
			0x1b,
			0x1c,
			0x1d,
			0x1e,
			0x1f,
			0x20,
			0x21,
			0x22,
			0x23,
			0x24,
			0x25,
			0x26,
			0x27,
			0x28,
			0x29,
			0x2a,
			0x2b,
			0x2c,
			0x2d,
			0x2e,
			0x2f,
		},
		ByteOffset: 0x30313233, // 4 bytes
		ByteLength: 0x40414243, // 4 bytes
	}

	encoded := offset.Encode()

	// Verify Magic prefix bytes 0-3
	assert.True(
		t,
		bytes.Equal(encoded[0:4], offsetMagic[:]),
		"Magic prefix mismatch",
	)

	// Verify BlockSlot bytes 4-11 (big-endian)
	expectedSlotBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	assert.True(
		t,
		bytes.Equal(encoded[4:12], expectedSlotBytes),
		"BlockSlot encoding mismatch",
	)

	// Verify BlockHash bytes 12-43
	expectedHashBytes := []byte{
		0x10,
		0x11,
		0x12,
		0x13,
		0x14,
		0x15,
		0x16,
		0x17,
		0x18,
		0x19,
		0x1a,
		0x1b,
		0x1c,
		0x1d,
		0x1e,
		0x1f,
		0x20,
		0x21,
		0x22,
		0x23,
		0x24,
		0x25,
		0x26,
		0x27,
		0x28,
		0x29,
		0x2a,
		0x2b,
		0x2c,
		0x2d,
		0x2e,
		0x2f,
	}
	assert.True(
		t,
		bytes.Equal(encoded[12:44], expectedHashBytes),
		"BlockHash encoding mismatch",
	)

	// Verify ByteOffset bytes 44-47 (big-endian)
	expectedOffsetBytes := []byte{0x30, 0x31, 0x32, 0x33}
	assert.True(
		t,
		bytes.Equal(encoded[44:48], expectedOffsetBytes),
		"ByteOffset encoding mismatch",
	)

	// Verify ByteLength bytes 48-51 (big-endian)
	expectedLengthBytes := []byte{0x40, 0x41, 0x42, 0x43}
	assert.True(
		t,
		bytes.Equal(encoded[48:52], expectedLengthBytes),
		"ByteLength encoding mismatch",
	)
}

func TestDecodeCborOffsetInvalidSize(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "too short - 1 byte",
			data: []byte{0x01},
		},
		{
			name: "too short - 51 bytes",
			data: make([]byte, 51),
		},
		{
			name: "too long - 53 bytes",
			data: make([]byte, 53),
		},
		{
			name: "too long - 100 bytes",
			data: make([]byte, 100),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := DecodeCborOffset(tc.data)
			assert.Error(t, err, "decode should return error for invalid size")
			assert.Nil(t, decoded, "decoded should be nil on error")
			assert.Contains(
				t,
				err.Error(),
				"52",
				"error message should mention expected size",
			)
		})
	}
}

func TestDecodeCborOffsetInvalidMagic(t *testing.T) {
	// Valid size but wrong magic
	data := make([]byte, CborOffsetSize)
	data[0] = 'X' // Wrong magic
	data[1] = 'O'
	data[2] = 'F'
	data[3] = 'F'

	decoded, err := DecodeCborOffset(data)
	assert.Error(t, err, "decode should return error for invalid magic")
	assert.Nil(t, decoded, "decoded should be nil on error")
	assert.Contains(
		t,
		err.Error(),
		"magic",
		"error message should mention magic",
	)
}

func TestCborOffsetSizeConstant(t *testing.T) {
	// Verify the constant matches expected value
	// Layout: Magic (4) + BlockSlot (8) + BlockHash (32) + ByteOffset (4) + ByteLength (4) = 52
	assert.Equal(t, 52, CborOffsetSize, "CborOffsetSize should be 52")

	// Verify encoded size matches constant
	offset := CborOffset{}
	encoded := offset.Encode()
	assert.Len(
		t,
		encoded,
		CborOffsetSize,
		"encoded size should match CborOffsetSize constant",
	)
}

func TestEncodeUtxoOffset(t *testing.T) {
	offset := &CborOffset{
		BlockSlot:  12345,
		BlockHash:  [32]byte{0x01, 0x02, 0x03},
		ByteOffset: 100,
		ByteLength: 200,
	}

	encoded := EncodeUtxoOffset(offset)
	assert.Len(t, encoded, CborOffsetSize, "encoded should be 52 bytes")

	// Verify magic prefix
	assert.True(t, bytes.Equal(encoded[0:4], offsetMagic[:]), "should have DOFF magic prefix")

	// Verify it decodes correctly
	decoded, err := DecodeUtxoOffset(encoded)
	require.NoError(t, err)
	assert.Equal(t, offset.BlockSlot, decoded.BlockSlot)
	assert.Equal(t, offset.BlockHash, decoded.BlockHash)
	assert.Equal(t, offset.ByteOffset, decoded.ByteOffset)
	assert.Equal(t, offset.ByteLength, decoded.ByteLength)
}

func TestIsUtxoOffsetStorage(t *testing.T) {
	// Create a valid offset-encoded data
	validOffset := make([]byte, CborOffsetSize)
	copy(validOffset[0:4], offsetMagic[:])

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid 52 bytes with magic",
			data:     validOffset,
			expected: true,
		},
		{
			name:     "52 bytes but wrong magic",
			data:     make([]byte, 52), // zeros, no DOFF magic
			expected: false,
		},
		{
			name:     "too short - 51 bytes",
			data:     make([]byte, 51),
			expected: false,
		},
		{
			name:     "too long - 53 bytes",
			data:     make([]byte, 53),
			expected: false,
		},
		{
			name:     "empty",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "typical CBOR size",
			data:     make([]byte, 100),
			expected: false,
		},
		{
			name:     "48 bytes (legacy size) should not match",
			data:     make([]byte, 48),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsUtxoOffsetStorage(tc.data)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestDecodeUtxoOffsetError(t *testing.T) {
	// Invalid sizes should error
	_, err := DecodeUtxoOffset(make([]byte, 51))
	assert.Error(t, err)

	_, err = DecodeUtxoOffset(make([]byte, 53))
	assert.Error(t, err)

	// Valid size but wrong magic should error
	wrongMagic := make([]byte, 52)
	_, err = DecodeUtxoOffset(wrongMagic)
	assert.Error(t, err)

	// Valid size with correct magic should succeed
	validData := make([]byte, 52)
	copy(validData[0:4], offsetMagic[:])
	_, err = DecodeUtxoOffset(validData)
	assert.NoError(t, err)
}

func TestEncodeTxOffset(t *testing.T) {
	offset := &CborOffset{
		BlockSlot:  12345,
		BlockHash:  [32]byte{0x01, 0x02, 0x03},
		ByteOffset: 100,
		ByteLength: 200,
	}

	encoded := EncodeTxOffset(offset)
	assert.Len(t, encoded, CborOffsetSize, "encoded should be 52 bytes")

	// Verify magic prefix
	assert.True(t, bytes.Equal(encoded[0:4], offsetMagic[:]), "should have DOFF magic prefix")

	// Verify it decodes correctly
	decoded, err := DecodeTxOffset(encoded)
	require.NoError(t, err)
	assert.Equal(t, offset.BlockSlot, decoded.BlockSlot)
	assert.Equal(t, offset.BlockHash, decoded.BlockHash)
	assert.Equal(t, offset.ByteOffset, decoded.ByteOffset)
	assert.Equal(t, offset.ByteLength, decoded.ByteLength)
}

func TestIsTxOffsetStorage(t *testing.T) {
	// Create a valid offset-encoded data
	validOffset := make([]byte, CborOffsetSize)
	copy(validOffset[0:4], offsetMagic[:])

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid 52 bytes with magic",
			data:     validOffset,
			expected: true,
		},
		{
			name:     "52 bytes but wrong magic",
			data:     make([]byte, 52), // zeros, no DOFF magic
			expected: false,
		},
		{
			name:     "too short - 51 bytes",
			data:     make([]byte, 51),
			expected: false,
		},
		{
			name:     "too long - 53 bytes",
			data:     make([]byte, 53),
			expected: false,
		},
		{
			name:     "empty",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "typical CBOR size",
			data:     make([]byte, 100),
			expected: false,
		},
		{
			name:     "48 bytes (legacy size) should not match",
			data:     make([]byte, 48),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsTxOffsetStorage(tc.data)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestDecodeTxOffsetError(t *testing.T) {
	// Invalid sizes should error
	_, err := DecodeTxOffset(make([]byte, 51))
	assert.Error(t, err)

	_, err = DecodeTxOffset(make([]byte, 53))
	assert.Error(t, err)

	// Valid size but wrong magic should error
	wrongMagic := make([]byte, 52)
	_, err = DecodeTxOffset(wrongMagic)
	assert.Error(t, err)

	// Valid size with correct magic should succeed
	validData := make([]byte, 52)
	copy(validData[0:4], offsetMagic[:])
	_, err = DecodeTxOffset(validData)
	assert.NoError(t, err)
}

func TestTxCborPartsEncodeDecode(t *testing.T) {
	testCases := []struct {
		name           string
		blockSlot      uint64
		blockHash      [32]byte
		bodyOffset     uint32
		bodyLength     uint32
		witnessOffset  uint32
		witnessLength  uint32
		metadataOffset uint32
		metadataLength uint32
		isValid        bool
	}{
		{
			name:           "zero values",
			blockSlot:      0,
			blockHash:      [32]byte{},
			bodyOffset:     0,
			bodyLength:     0,
			witnessOffset:  0,
			witnessLength:  0,
			metadataOffset: 0,
			metadataLength: 0,
			isValid:        false,
		},
		{
			name:           "typical valid transaction",
			blockSlot:      12345678,
			blockHash:      [32]byte{0x01, 0x02, 0x03, 0x04},
			bodyOffset:     100,
			bodyLength:     500,
			witnessOffset:  1000,
			witnessLength:  200,
			metadataOffset: 2000,
			metadataLength: 50,
			isValid:        true,
		},
		{
			name:           "invalid transaction no metadata",
			blockSlot:      99999,
			blockHash:      [32]byte{0xff, 0xee, 0xdd},
			bodyOffset:     200,
			bodyLength:     300,
			witnessOffset:  800,
			witnessLength:  150,
			metadataOffset: 0,
			metadataLength: 0,
			isValid:        false,
		},
		{
			name:      "max values",
			blockSlot: math.MaxUint64,
			blockHash: [32]byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
			bodyOffset:     math.MaxUint32,
			bodyLength:     math.MaxUint32,
			witnessOffset:  math.MaxUint32,
			witnessLength:  math.MaxUint32,
			metadataOffset: math.MaxUint32,
			metadataLength: math.MaxUint32,
			isValid:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := TxCborParts{
				BlockSlot:      tc.blockSlot,
				BlockHash:      tc.blockHash,
				BodyOffset:     tc.bodyOffset,
				BodyLength:     tc.bodyLength,
				WitnessOffset:  tc.witnessOffset,
				WitnessLength:  tc.witnessLength,
				MetadataOffset: tc.metadataOffset,
				MetadataLength: tc.metadataLength,
				IsValid:        tc.isValid,
			}

			// Test encode
			encoded := original.Encode()
			require.Len(
				t,
				encoded,
				TxCborPartsSize,
				"encoded size should be %d bytes",
				TxCborPartsSize,
			)

			// Verify magic prefix
			assert.True(
				t,
				bytes.Equal(encoded[0:4], txPartsMagic[:]),
				"should have DTXP magic prefix",
			)

			// Test decode
			decoded, err := DecodeTxCborParts(encoded)
			require.NoError(t, err, "decode should not error")
			require.NotNil(t, decoded, "decoded should not be nil")

			// Verify round-trip
			assert.Equal(t, original.BlockSlot, decoded.BlockSlot, "BlockSlot mismatch")
			assert.Equal(t, original.BlockHash, decoded.BlockHash, "BlockHash mismatch")
			assert.Equal(t, original.BodyOffset, decoded.BodyOffset, "BodyOffset mismatch")
			assert.Equal(t, original.BodyLength, decoded.BodyLength, "BodyLength mismatch")
			assert.Equal(t, original.WitnessOffset, decoded.WitnessOffset, "WitnessOffset mismatch")
			assert.Equal(t, original.WitnessLength, decoded.WitnessLength, "WitnessLength mismatch")
			assert.Equal(t, original.MetadataOffset, decoded.MetadataOffset, "MetadataOffset mismatch")
			assert.Equal(t, original.MetadataLength, decoded.MetadataLength, "MetadataLength mismatch")
			assert.Equal(t, original.IsValid, decoded.IsValid, "IsValid mismatch")
		})
	}
}

func TestTxCborPartsSizeConstant(t *testing.T) {
	// Verify the constant matches expected value
	// Layout: Magic (4) + BlockSlot (8) + BlockHash (32) +
	//         BodyOffset (4) + BodyLength (4) +
	//         WitnessOffset (4) + WitnessLength (4) +
	//         MetadataOffset (4) + MetadataLength (4) +
	//         IsValid (1) = 69
	assert.Equal(t, 69, TxCborPartsSize, "TxCborPartsSize should be 69")

	// Verify encoded size matches constant
	parts := TxCborParts{}
	encoded := parts.Encode()
	assert.Len(
		t,
		encoded,
		TxCborPartsSize,
		"encoded size should match TxCborPartsSize constant",
	)
}

func TestDecodeTxCborPartsInvalidSize(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{name: "empty data", data: []byte{}},
		{name: "too short - 1 byte", data: []byte{0x01}},
		{name: "too short - 68 bytes", data: make([]byte, 68)},
		{name: "too long - 70 bytes", data: make([]byte, 70)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := DecodeTxCborParts(tc.data)
			assert.Error(t, err, "decode should return error for invalid size")
			assert.Nil(t, decoded, "decoded should be nil on error")
			assert.Contains(t, err.Error(), "69", "error message should mention expected size")
		})
	}
}

func TestDecodeTxCborPartsInvalidMagic(t *testing.T) {
	// Valid size but wrong magic
	data := make([]byte, TxCborPartsSize)
	data[0] = 'X' // Wrong magic
	data[1] = 'T'
	data[2] = 'X'
	data[3] = 'P'

	decoded, err := DecodeTxCborParts(data)
	assert.Error(t, err, "decode should return error for invalid magic")
	assert.Nil(t, decoded, "decoded should be nil on error")
	assert.Contains(t, err.Error(), "magic", "error message should mention magic")
}

func TestIsTxCborPartsStorage(t *testing.T) {
	// Create valid TxCborParts data
	validData := make([]byte, TxCborPartsSize)
	copy(validData[0:4], txPartsMagic[:])

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{name: "valid 69 bytes with magic", data: validData, expected: true},
		{name: "69 bytes but wrong magic", data: make([]byte, 69), expected: false},
		{name: "too short - 68 bytes", data: make([]byte, 68), expected: false},
		{name: "too long - 70 bytes", data: make([]byte, 70), expected: false},
		{name: "empty", data: []byte{}, expected: false},
		{name: "52 bytes (CborOffset size)", data: make([]byte, 52), expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsTxCborPartsStorage(tc.data)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestTxCborPartsHasMetadata(t *testing.T) {
	// With metadata
	withMeta := TxCborParts{MetadataLength: 100}
	assert.True(t, withMeta.HasMetadata())

	// Without metadata
	noMeta := TxCborParts{MetadataLength: 0}
	assert.False(t, noMeta.HasMetadata())
}

func TestTxCborPartsReassembleTxCbor(t *testing.T) {
	// Create a mock block CBOR with embedded components
	// Body: simple CBOR map {0: 1}
	bodyCbor := []byte{0xa1, 0x00, 0x01}
	// Witness: simple CBOR array [1, 2]
	witnessCbor := []byte{0x82, 0x01, 0x02}
	// Metadata: simple CBOR map {1: "test"}
	metadataCbor := []byte{0xa1, 0x01, 0x64, 0x74, 0x65, 0x73, 0x74}

	// Build mock block with components at different offsets
	blockCbor := make([]byte, 100)
	copy(blockCbor[10:], bodyCbor)     // body at offset 10
	copy(blockCbor[30:], witnessCbor)  // witness at offset 30
	copy(blockCbor[50:], metadataCbor) // metadata at offset 50

	t.Run("valid transaction with metadata", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset:     10,
			BodyLength:     uint32(len(bodyCbor)),
			WitnessOffset:  30,
			WitnessLength:  uint32(len(witnessCbor)),
			MetadataOffset: 50,
			MetadataLength: uint32(len(metadataCbor)),
			IsValid:        true,
		}

		result, err := parts.ReassembleTxCbor(blockCbor)
		require.NoError(t, err)

		// Verify structure: 0x84 (4-element array) + body + witness + true + metadata
		assert.Equal(t, byte(0x84), result[0], "should be 4-element array")
		// Body should follow array header
		assert.True(t, bytes.Equal(result[1:1+len(bodyCbor)], bodyCbor), "body mismatch")
		// Witness should follow body
		witnessStart := 1 + len(bodyCbor)
		assert.True(t, bytes.Equal(result[witnessStart:witnessStart+len(witnessCbor)], witnessCbor), "witness mismatch")
		// IsValid (true = 0xf5) should follow witness
		isValidIdx := witnessStart + len(witnessCbor)
		assert.Equal(t, byte(0xf5), result[isValidIdx], "is_valid should be true (0xf5)")
		// Metadata should follow is_valid
		metaStart := isValidIdx + 1
		assert.True(t, bytes.Equal(result[metaStart:], metadataCbor), "metadata mismatch")
	})

	t.Run("invalid transaction without metadata", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset:     10,
			BodyLength:     uint32(len(bodyCbor)),
			WitnessOffset:  30,
			WitnessLength:  uint32(len(witnessCbor)),
			MetadataOffset: 0,
			MetadataLength: 0,
			IsValid:        false,
		}

		result, err := parts.ReassembleTxCbor(blockCbor)
		require.NoError(t, err)

		// Verify structure: 0x84 + body + witness + false + null
		assert.Equal(t, byte(0x84), result[0], "should be 4-element array")
		// IsValid (false = 0xf4)
		isValidIdx := 1 + len(bodyCbor) + len(witnessCbor)
		assert.Equal(t, byte(0xf4), result[isValidIdx], "is_valid should be false (0xf4)")
		// Null (0xf6) for no metadata
		assert.Equal(t, byte(0xf6), result[isValidIdx+1], "metadata should be null (0xf6)")
	})

	t.Run("body out of bounds", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset: 90,
			BodyLength: 20, // Would exceed block size
		}

		_, err := parts.ReassembleTxCbor(blockCbor)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "body")
	})

	t.Run("witness out of bounds", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset:    10,
			BodyLength:    3,
			WitnessOffset: 95,
			WitnessLength: 10, // Would exceed block size
		}

		_, err := parts.ReassembleTxCbor(blockCbor)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "witness")
	})

	t.Run("metadata out of bounds", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset:     10,
			BodyLength:     3,
			WitnessOffset:  30,
			WitnessLength:  3,
			MetadataOffset: 98,
			MetadataLength: 10, // Would exceed block size
		}

		_, err := parts.ReassembleTxCbor(blockCbor)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "metadata")
	})

	t.Run("uint32 overflow in body bounds check", func(t *testing.T) {
		// Test that overflow in offset+length is caught
		// 0xFFFFFFF0 + 0x20 = 0x10 (wraps), which is < 100
		// Without overflow protection, this would pass the check but panic on slice
		parts := TxCborParts{
			BodyOffset: 0xFFFFFFF0,
			BodyLength: 0x20,
		}

		_, err := parts.ReassembleTxCbor(blockCbor)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "body")
	})

	t.Run("uint32 overflow in witness bounds check", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset:    10,
			BodyLength:    3,
			WitnessOffset: 0xFFFFFFE0,
			WitnessLength: 0x30,
		}

		_, err := parts.ReassembleTxCbor(blockCbor)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "witness")
	})

	t.Run("uint32 overflow in metadata bounds check", func(t *testing.T) {
		parts := TxCborParts{
			BodyOffset:     10,
			BodyLength:     3,
			WitnessOffset:  30,
			WitnessLength:  3,
			MetadataOffset: 0xFFFFFF00,
			MetadataLength: 0x200,
		}

		_, err := parts.ReassembleTxCbor(blockCbor)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "metadata")
	})
}
