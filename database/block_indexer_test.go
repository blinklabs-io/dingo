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
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockIndexerComputeOffsets(t *testing.T) {
	// Load blocks from immutable test data
	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err, "failed to open immutable database")

	// Get an iterator starting from the beginning
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err, "failed to create block iterator")
	defer iter.Close()

	// Test offset computation on multiple blocks
	blocksWithTx := 0
	maxBlocksToTest := 500

	for i := 0; i < maxBlocksToTest; i++ {
		immBlock, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error reading block %d: %s", i, err)
		}
		if immBlock == nil {
			break // End of chain
		}

		// Parse the block
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		if err != nil {
			// Skip blocks that can't be parsed (e.g., Byron EBB)
			continue
		}

		// Skip blocks without transactions
		txs := block.Transactions()
		if len(txs) == 0 {
			continue
		}

		blocksWithTx++

		// Create indexer and compute offsets
		indexer := NewBlockIndexer(immBlock.Slot, immBlock.Hash)
		result, err := indexer.ComputeOffsets(immBlock.Cbor, block)
		require.NoError(t, err, "failed to compute offsets for block %d (slot %d)",
			i, immBlock.Slot)

		// Verify we got transaction offsets
		assert.Equal(t, len(txs), len(result.TxOffsets),
			"transaction count mismatch for block %d (slot %d)", i, immBlock.Slot)

		// Verify each transaction offset
		for _, tx := range txs {
			txHash := tx.Hash()
			var txHashArray [32]byte
			copy(txHashArray[:], txHash.Bytes())

			offset, ok := result.TxOffsets[txHashArray]
			assert.True(t, ok, "missing offset for tx %x in block %d", txHash[:8], i)

			if ok {
				// Verify offset is within block bounds
				end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
				assert.LessOrEqual(t, end, uint64(len(immBlock.Cbor)),
					"tx offset out of bounds for tx %x in block %d", txHash[:8], i)

				// Verify block slot and hash match
				assert.Equal(t, immBlock.Slot, offset.BlockSlot,
					"block slot mismatch in offset")
				var expectedHash [32]byte
				copy(expectedHash[:], immBlock.Hash)
				assert.Equal(t, expectedHash, offset.BlockHash,
					"block hash mismatch in offset")
			}
		}

		// Verify UTxO offsets
		for _, tx := range txs {
			produced := tx.Produced()
			for _, utxo := range produced {
				txHash := tx.Hash()
				var txHashArray [32]byte
				copy(txHashArray[:], txHash.Bytes())

				ref := UtxoRef{
					TxId:      txHashArray,
					OutputIdx: utxo.Id.Index(),
				}

				offset, ok := result.UtxoOffsets[ref]
				if ok {
					// Verify offset is within block bounds
					end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
					assert.LessOrEqual(t, end, uint64(len(immBlock.Cbor)),
						"utxo offset out of bounds for ref %x:%d in block %d",
						txHash[:8], utxo.Id.Index(), i)

					// Verify extracted bytes match the UTxO CBOR
					if end <= uint64(len(immBlock.Cbor)) {
						extracted := immBlock.Cbor[offset.ByteOffset : offset.ByteOffset+offset.ByteLength]
						expectedCbor := utxo.Output.Cbor()
						if len(expectedCbor) > 0 {
							assert.Equal(t, expectedCbor, extracted,
								"extracted CBOR mismatch for utxo %x:%d in block %d",
								txHash[:8], utxo.Id.Index(), i)
						}
					}
				}
			}
		}

		// Stop after testing some blocks with transactions
		if blocksWithTx >= 20 {
			break
		}
	}

	assert.Greater(t, blocksWithTx, 0, "no blocks with transactions were tested")
	t.Logf("Successfully tested %d blocks with transactions", blocksWithTx)
}

func TestBlockIndexerEmptyBlock(t *testing.T) {
	// Test with a block that has no transactions
	indexer := NewBlockIndexer(0, make([]byte, 32))

	// Test nil block returns error
	result, err := indexer.ComputeOffsets([]byte{}, nil)
	assert.Error(t, err, "expected error for nil block")
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "block cannot be nil")
}

// findCborInBytes searches for a CBOR byte sequence within a larger byte slice.
// Returns the offset if found, -1 otherwise.
func findCborInBytes(data, target []byte) int {
	if len(target) == 0 || len(data) < len(target) {
		return -1
	}

	for i := 0; i <= len(data)-len(target); i++ {
		match := true
		for j := 0; j < len(target); j++ {
			if data[i+j] != target[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func TestFindCborInBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		target   []byte
		expected int
	}{
		{
			name:     "found at start",
			data:     []byte{0x01, 0x02, 0x03, 0x04, 0x05},
			target:   []byte{0x01, 0x02},
			expected: 0,
		},
		{
			name:     "found in middle",
			data:     []byte{0x01, 0x02, 0x03, 0x04, 0x05},
			target:   []byte{0x02, 0x03},
			expected: 1,
		},
		{
			name:     "found at end",
			data:     []byte{0x01, 0x02, 0x03, 0x04, 0x05},
			target:   []byte{0x04, 0x05},
			expected: 3,
		},
		{
			name:     "not found",
			data:     []byte{0x01, 0x02, 0x03, 0x04, 0x05},
			target:   []byte{0x06, 0x07},
			expected: -1,
		},
		{
			name:     "empty target",
			data:     []byte{0x01, 0x02, 0x03},
			target:   []byte{},
			expected: -1,
		},
		{
			name:     "target larger than data",
			data:     []byte{0x01, 0x02},
			target:   []byte{0x01, 0x02, 0x03},
			expected: -1,
		},
		{
			name:     "exact match",
			data:     []byte{0x01, 0x02, 0x03},
			target:   []byte{0x01, 0x02, 0x03},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findCborInBytes(tt.data, tt.target)
			assert.Equal(t, tt.expected, result)
		})
	}
}
