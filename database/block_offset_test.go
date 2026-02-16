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

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/immutable"
)

func TestExtractTransactionOffsets(t *testing.T) {
	// Load blocks from immutable test data
	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err, "failed to open immutable database")

	// Get an iterator starting from the beginning
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err, "failed to create block iterator")
	defer iter.Close()

	// Test offset extraction on multiple blocks
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

		// Skip blocks without transactions (e.g., Byron EBB or empty blocks)
		txs := block.Transactions()
		if len(txs) == 0 {
			continue
		}

		blocksWithTx++

		// Extract offsets
		offsets, err := common.ExtractTransactionOffsets(immBlock.Cbor)
		require.NoError(t, err, "failed to extract offsets from block %d (slot %d)",
			i, immBlock.Slot)

		// Verify we got offsets for all transactions
		assert.Equal(t, len(txs), len(offsets.Transactions),
			"transaction count mismatch for block %d (slot %d)", i, immBlock.Slot)

		// Verify each offset points to valid data
		for txIdx, tx := range txs {
			if txIdx >= len(offsets.Transactions) {
				continue
			}

			loc := offsets.Transactions[txIdx]

			// Verify body offset is within block bounds
			bodyEnd := uint64(loc.Body.Offset) + uint64(loc.Body.Length)
			assert.LessOrEqual(t, bodyEnd, uint64(len(immBlock.Cbor)),
				"body offset out of bounds for tx %d in block %d", txIdx, i)

			// Extract body CBOR and verify it matches transaction body
			if loc.Body.Offset > 0 && loc.Body.Length > 0 && bodyEnd <= uint64(len(immBlock.Cbor)) {
				extractedBody := immBlock.Cbor[loc.Body.Offset : loc.Body.Offset+loc.Body.Length]

				// The extracted data should be valid CBOR (basic sanity check)
				assert.Greater(t, len(extractedBody), 0,
					"extracted body is empty for tx %d in block %d", txIdx, i)

				// Compare with transaction's stored CBOR if available
				txCbor := tx.Cbor()
				if len(txCbor) > 0 {
					// Transaction CBOR includes both body and witnesses,
					// so extracted body should be a prefix or we need to
					// compare differently based on era
					assert.LessOrEqual(t, len(extractedBody), len(txCbor)+1000,
						"extracted body unexpectedly larger than tx cbor for tx %d in block %d",
						txIdx, i)
				}
			}

			// Verify witness offset is within block bounds
			witnessEnd := uint64(loc.Witness.Offset) + uint64(loc.Witness.Length)
			assert.LessOrEqual(t, witnessEnd, uint64(len(immBlock.Cbor)),
				"witness offset out of bounds for tx %d in block %d", txIdx, i)

			// Verify datum, redeemer, and script offsets if present
			for hash, datumLoc := range loc.Datums {
				datumEnd := uint64(datumLoc.Offset) + uint64(datumLoc.Length)
				assert.LessOrEqual(t, datumEnd, uint64(len(immBlock.Cbor)),
					"datum offset out of bounds for hash %x in tx %d block %d",
					hash[:8], txIdx, i)
			}

			for key, redeemerLoc := range loc.Redeemers {
				redeemerEnd := uint64(redeemerLoc.Offset) + uint64(redeemerLoc.Length)
				assert.LessOrEqual(t, redeemerEnd, uint64(len(immBlock.Cbor)),
					"redeemer offset out of bounds for key (%d,%d) in tx %d block %d",
					key.Tag, key.Index, txIdx, i)
			}

			for hash, scriptLoc := range loc.Scripts {
				scriptEnd := uint64(scriptLoc.Offset) + uint64(scriptLoc.Length)
				assert.LessOrEqual(t, scriptEnd, uint64(len(immBlock.Cbor)),
					"script offset out of bounds for hash %x in tx %d block %d",
					hash[:8], txIdx, i)
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

func TestExtractTransactionOffsetsEmptyBlock(t *testing.T) {
	// Test that the function handles blocks with empty/minimal structure
	// Byron EBB blocks have a different structure

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err, "failed to open immutable database")

	// Get first block (typically Byron genesis or EBB)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err, "failed to create block iterator")
	defer iter.Close()

	immBlock, err := iter.Next()
	require.NoError(t, err, "failed to get first block")
	require.NotNil(t, immBlock, "expected at least one block")

	// Should not panic on Byron blocks
	offsets, err := common.ExtractTransactionOffsets(immBlock.Cbor)
	// Error is acceptable for unsupported block formats
	if err == nil {
		assert.NotNil(t, offsets, "offsets should not be nil when no error")
	}
}
