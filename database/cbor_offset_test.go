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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCborOffsetEncodeFormat(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

func TestTxCborPartsSizeConstant(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
			assert.Contains(
				t,
				err.Error(),
				"69",
				"error message should mention expected size",
			)
		})
	}
}

func TestDecodeTxCborPartsInvalidMagic(t *testing.T) {
	t.Parallel()

	// Valid size but wrong magic
	data := make([]byte, TxCborPartsSize)
	data[0] = 'X' // Wrong magic
	data[1] = 'T'
	data[2] = 'X'
	data[3] = 'P'

	decoded, err := DecodeTxCborParts(data)
	assert.Error(t, err, "decode should return error for invalid magic")
	assert.Nil(t, decoded, "decoded should be nil on error")
	assert.Contains(
		t,
		err.Error(),
		"magic",
		"error message should mention magic",
	)
}

// TestDecodeTxCborPartsRejectsNoncanonicalIsValid proves a validity byte
// other than the canonical 0/1 that Encode ever produces is rejected rather
// than silently coerced to true by a `!= 0` check. IsTxCborPartsStorage
// deliberately still recognizes the data as DTXP-shaped (format, not
// content, validation): a UTxO-recovery caller must reach this decode
// error rather than take a not-DTXP-shaped fallback path that would
// silently treat the corrupted record as simply absent.
func TestDecodeTxCborPartsRejectsNoncanonicalIsValid(t *testing.T) {
	t.Parallel()

	for _, isValidByte := range []byte{2, 0x7f, 0x80, 0xfe, 0xff} {
		t.Run(
			fmt.Sprintf("byte value %d", isValidByte),
			func(t *testing.T) {
				data := (&TxCborParts{IsValid: true}).Encode()
				data[68] = isValidByte

				decoded, err := DecodeTxCborParts(data)
				assert.Error(
					t,
					err,
					"decode should reject a noncanonical IsValid byte",
				)
				assert.Nil(t, decoded, "decoded should be nil on error")
				assert.True(
					t,
					IsTxCborPartsStorage(data),
					"format recognition must still see this as DTXP-shaped "+
						"so a recovery caller reaches the decode error "+
						"above instead of a not-DTXP-shaped fallback path",
				)
			},
		)
	}
}

func TestIsTxCborPartsStorage(t *testing.T) {
	t.Parallel()

	// Create valid TxCborParts data
	validData := make([]byte, TxCborPartsSize)
	copy(validData[0:4], txPartsMagic[:])

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{name: "valid 69 bytes with magic", data: validData, expected: true},
		{
			name:     "69 bytes but wrong magic",
			data:     make([]byte, 69),
			expected: false,
		},
		{name: "too short - 68 bytes", data: make([]byte, 68), expected: false},
		{name: "too long - 70 bytes", data: make([]byte, 70), expected: false},
		{name: "empty", data: []byte{}, expected: false},
		{
			name:     "52 bytes (CborOffset size)",
			data:     make([]byte, 52),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsTxCborPartsStorage(tc.data)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestTxCborPartsHasMetadata(t *testing.T) {
	t.Parallel()

	// With metadata
	withMeta := TxCborParts{MetadataLength: 100}
	assert.True(t, withMeta.HasMetadata())

	// Without metadata
	noMeta := TxCborParts{MetadataLength: 0}
	assert.False(t, noMeta.HasMetadata())
}

func TestTxCborPartsReassembleTxCbor(t *testing.T) {
	t.Parallel()

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
		assert.True(
			t,
			bytes.Equal(result[1:1+len(bodyCbor)], bodyCbor),
			"body mismatch",
		)
		// Witness should follow body
		witnessStart := 1 + len(bodyCbor)
		assert.True(
			t,
			bytes.Equal(
				result[witnessStart:witnessStart+len(witnessCbor)],
				witnessCbor,
			),
			"witness mismatch",
		)
		// IsValid (true = 0xf5) should follow witness
		isValidIdx := witnessStart + len(witnessCbor)
		assert.Equal(
			t,
			byte(0xf5),
			result[isValidIdx],
			"is_valid should be true (0xf5)",
		)
		// Metadata should follow is_valid
		metaStart := isValidIdx + 1
		assert.True(
			t,
			bytes.Equal(result[metaStart:], metadataCbor),
			"metadata mismatch",
		)
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
		assert.Equal(
			t,
			byte(0xf4),
			result[isValidIdx],
			"is_valid should be false (0xf4)",
		)
		// Null (0xf6) for no metadata
		assert.Equal(
			t,
			byte(0xf6),
			result[isValidIdx+1],
			"metadata should be null (0xf6)",
		)
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
