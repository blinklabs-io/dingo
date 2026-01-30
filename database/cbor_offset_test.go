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

	// Verify BlockSlot bytes 0-7 (big-endian)
	expectedSlotBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	assert.True(
		t,
		bytes.Equal(encoded[0:8], expectedSlotBytes),
		"BlockSlot encoding mismatch",
	)

	// Verify BlockHash bytes 8-39
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
		bytes.Equal(encoded[8:40], expectedHashBytes),
		"BlockHash encoding mismatch",
	)

	// Verify ByteOffset bytes 40-43 (big-endian)
	expectedOffsetBytes := []byte{0x30, 0x31, 0x32, 0x33}
	assert.True(
		t,
		bytes.Equal(encoded[40:44], expectedOffsetBytes),
		"ByteOffset encoding mismatch",
	)

	// Verify ByteLength bytes 44-47 (big-endian)
	expectedLengthBytes := []byte{0x40, 0x41, 0x42, 0x43}
	assert.True(
		t,
		bytes.Equal(encoded[44:48], expectedLengthBytes),
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
			name: "too short - 47 bytes",
			data: make([]byte, 47),
		},
		{
			name: "too long - 49 bytes",
			data: make([]byte, 49),
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
				"48",
				"error message should mention expected size",
			)
		})
	}
}

func TestCborOffsetSizeConstant(t *testing.T) {
	// Verify the constant matches expected value
	assert.Equal(t, 48, CborOffsetSize, "CborOffsetSize should be 48")

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
