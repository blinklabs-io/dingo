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

package ledger

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestUtxoStorageAndRetrieval tests that UTxOs from regular blocks are stored
// and retrieved correctly using the offset-based storage system.
func TestUtxoStorageAndRetrieval(t *testing.T) {
	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "utxo_storage_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	// Create database
	dbConfig := &database.Config{
		DataDir:        tmpDir,
		Logger:         logger,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}
	db, err := database.New(dbConfig)
	require.NoError(t, err)
	defer db.Close()

	// Load blocks from immutable testdata
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)

	// Start from genesis and process a few blocks
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: nil})
	require.NoError(t, err)
	defer iter.Close()

	var storedUtxos []struct {
		txId      []byte
		outputIdx uint32
		slot      uint64
	}

	blocksProcessed := 0
	maxBlocks := 10 // Process a few blocks to find some UTxOs

	for blocksProcessed < maxBlocks {
		immBlock, err := iter.Next()
		if err != nil {
			// io.EOF or equivalent signals end of iteration
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				break
			}
			t.Fatalf("unexpected iterator error: %v", err)
		}
		if immBlock == nil {
			break
		}

		// Decode block using gouroboros
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)

		point := ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		}

		t.Logf("Processing block at slot %d with %d transactions",
			point.Slot, len(block.Transactions()))

		// Skip blocks with no transactions
		if len(block.Transactions()) == 0 {
			blocksProcessed++
			continue
		}

		// First, store the block
		txn := db.Transaction(true)
		err = txn.Do(func(txn *database.Txn) error {
			// Store block CBOR
			blockRecord := models.Block{
				Slot:     point.Slot,
				Hash:     point.Hash,
				Number:   block.BlockNumber(),
				Type:     uint(block.Type()),
				PrevHash: block.PrevHash().Bytes(),
				Cbor:     block.Cbor(),
			}
			if err := db.BlockCreate(blockRecord, txn); err != nil {
				return err
			}

			// Compute offsets - offsets MUST be available
			indexer := database.NewBlockIndexer(point.Slot, point.Hash)
			offsets, err := indexer.ComputeOffsets(block.Cbor(), block)
			if err != nil {
				return fmt.Errorf(
					"compute offsets for block %d: %w",
					point.Slot,
					err,
				)
			}

			// Process each transaction
			for txIdx, tx := range block.Transactions() {
				txHash := tx.Hash()
				var txHashArray [32]byte
				copy(txHashArray[:], txHash.Bytes())

				t.Logf("  TX %d: %s with %d outputs",
					txIdx, txHash.String(), len(tx.Outputs()))

				// Verify offsets exist for this transaction
				if txOff, ok := offsets.TxOffsets[txHashArray]; ok {
					t.Logf("    TX offset: slot=%d, offset=%d, length=%d",
						txOff.BlockSlot, txOff.ByteOffset, txOff.ByteLength)
				} else {
					return fmt.Errorf("TX offset not found for %s", txHash.String())
				}

				// Store the transaction - offsets MUST be available
				err := db.SetTransaction(
					tx,
					point,
					uint32(txIdx),
					0,
					nil,
					nil,
					&database.BlockIngestionResult{
						TxOffsets:   offsets.TxOffsets,
						UtxoOffsets: offsets.UtxoOffsets,
					},
					txn,
				)
				if err != nil {
					return err
				}

				// Track outputs for later verification
				for _, utxo := range tx.Produced() {
					txId := utxo.Id.Id().Bytes()
					outputIdx := utxo.Id.Index()

					// Verify offset was computed
					ref := database.UtxoRef{
						TxId:      txHashArray,
						OutputIdx: outputIdx,
					}
					if utxoOff, ok := offsets.UtxoOffsets[ref]; ok {
						t.Logf(
							"    Output %d offset: slot=%d, offset=%d, length=%d",
							outputIdx,
							utxoOff.BlockSlot,
							utxoOff.ByteOffset,
							utxoOff.ByteLength,
						)
					} else {
						return fmt.Errorf("output %d offset not found", outputIdx)
					}

					storedUtxos = append(storedUtxos, struct {
						txId      []byte
						outputIdx uint32
						slot      uint64
					}{
						txId:      txId,
						outputIdx: outputIdx,
						slot:      point.Slot,
					})
				}
			}

			return nil
		})
		require.NoError(t, err)

		blocksProcessed++
	}

	t.Logf(
		"\n=== Stored %d UTxOs from %d blocks ===\n",
		len(storedUtxos),
		blocksProcessed,
	)

	// Now try to retrieve each stored UTxO
	var retrievalErrors int
	var metadataErrors int
	var blobErrors int
	var successCount int

	for _, utxoRef := range storedUtxos {
		txn := db.Transaction(false)

		// Step 1: Check if metadata exists
		metaTxn := txn.Metadata()
		utxoMeta, err := db.Metadata().
			GetUtxo(utxoRef.txId, utxoRef.outputIdx, metaTxn)
		if err != nil {
			t.Logf("Metadata error for %s#%d: %v",
				hex.EncodeToString(utxoRef.txId[:8]), utxoRef.outputIdx, err)
			metadataErrors++
			txn.Release()
			continue
		}
		if utxoMeta == nil {
			t.Logf(
				"Metadata MISSING for %s#%d (slot %d)",
				hex.EncodeToString(
					utxoRef.txId[:8],
				),
				utxoRef.outputIdx,
				utxoRef.slot,
			)
			metadataErrors++
			txn.Release()
			continue
		}

		// Step 2: Check if blob data exists
		blob := db.Blob()
		blobTxn := txn.Blob()
		blobData, err := blob.GetUtxo(blobTxn, utxoRef.txId, utxoRef.outputIdx)
		if err != nil {
			t.Logf("Blob error for %s#%d: %v",
				hex.EncodeToString(utxoRef.txId[:8]), utxoRef.outputIdx, err)
			blobErrors++
			txn.Release()
			continue
		}

		// Step 3: Check blob data type
		if database.IsUtxoOffsetStorage(blobData) {
			// Decode offset
			offset, err := database.DecodeUtxoOffset(blobData)
			if err != nil {
				t.Logf(
					"Offset decode error for %s#%d: %v",
					hex.EncodeToString(
						utxoRef.txId[:8],
					),
					utxoRef.outputIdx,
					err,
				)
				blobErrors++
				txn.Release()
				continue
			}

			// Try to get block CBOR
			blockCbor, _, err := blob.GetBlock(
				blobTxn,
				offset.BlockSlot,
				offset.BlockHash[:],
			)
			if err != nil {
				t.Logf(
					"Block retrieval error for %s#%d: slot=%d, hash=%x, err=%v",
					hex.EncodeToString(utxoRef.txId[:8]),
					utxoRef.outputIdx,
					offset.BlockSlot,
					offset.BlockHash[:8],
					err,
				)
				blobErrors++
				txn.Release()
				continue
			}

			// Extract UTxO CBOR
			end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
			if end > uint64(len(blockCbor)) {
				t.Logf(
					"Offset out of bounds for %s#%d: offset=%d, length=%d, block_size=%d",
					hex.EncodeToString(utxoRef.txId[:8]),
					utxoRef.outputIdx,
					offset.ByteOffset,
					offset.ByteLength,
					len(blockCbor),
				)
				blobErrors++
				txn.Release()
				continue
			}

			// Success!
			successCount++
		} else {
			// Raw CBOR storage
			if len(blobData) > 0 {
				successCount++
			} else {
				t.Logf("Empty blob data for %s#%d",
					hex.EncodeToString(utxoRef.txId[:8]), utxoRef.outputIdx)
				blobErrors++
			}
		}

		txn.Release()
	}

	retrievalErrors = metadataErrors + blobErrors

	t.Logf("\n=== RESULTS ===")
	t.Logf("Total UTxOs: %d", len(storedUtxos))
	t.Logf("Successful retrievals: %d", successCount)
	t.Logf("Metadata errors: %d", metadataErrors)
	t.Logf("Blob errors: %d", blobErrors)
	t.Logf("Total retrieval errors: %d", retrievalErrors)

	// Require all UTxOs to be retrievable
	require.Equal(t, 0, retrievalErrors, "Some UTxOs could not be retrieved")
	require.Equal(
		t,
		len(storedUtxos),
		successCount,
		"Not all UTxOs were retrieved successfully",
	)
}

// TestUtxoByRefAfterSetTransaction verifies that UtxoByRef works immediately
// after SetTransaction within the same transaction.
func TestUtxoByRefAfterSetTransaction(t *testing.T) {
	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "utxo_byref_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	// Create database
	dbConfig := &database.Config{
		DataDir:        tmpDir,
		Logger:         logger,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	}
	db, err := database.New(dbConfig)
	require.NoError(t, err)
	defer db.Close()

	// Load blocks from immutable testdata
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)

	// Start from genesis
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: nil})
	require.NoError(t, err)
	defer iter.Close()

	// Find a block with transactions
	var block lcommon.Block
	var blockCbor []byte
	for {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			t.Skip("No blocks with transactions found")
		}

		block, err = ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)

		if len(block.Transactions()) > 0 {
			blockCbor = immBlock.Cbor
			break
		}
	}

	point := ocommon.Point{
		Slot: block.SlotNumber(),
		Hash: block.Hash().Bytes(),
	}

	t.Logf(
		"Using block at slot %d with %d transactions",
		point.Slot,
		len(block.Transactions()),
	)

	// Store block and verify UTxO retrieval in same transaction
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Store block CBOR
		blockRecord := models.Block{
			Slot:     point.Slot,
			Hash:     point.Hash,
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     blockCbor,
		}
		if err := db.BlockCreate(blockRecord, txn); err != nil {
			return err
		}

		// Compute offsets - offsets MUST be available
		indexer := database.NewBlockIndexer(point.Slot, point.Hash)
		offsets, err := indexer.ComputeOffsets(blockCbor, block)
		if err != nil {
			return fmt.Errorf("compute offsets: %w", err)
		}

		// Process first transaction - offsets MUST be available
		tx := block.Transactions()[0]
		err = db.SetTransaction(
			tx,
			point,
			0,
			0,
			nil,
			nil,
			&database.BlockIngestionResult{
				TxOffsets:   offsets.TxOffsets,
				UtxoOffsets: offsets.UtxoOffsets,
			},
			txn,
		)
		if err != nil {
			return err
		}

		// Try to retrieve UTxOs immediately (within same transaction)
		for _, utxo := range tx.Produced() {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			t.Logf("Attempting to retrieve %s#%d within same transaction...",
				hex.EncodeToString(txId[:8]), outputIdx)

			retrieved, err := db.UtxoByRef(txId, outputIdx, txn)
			if err != nil {
				t.Errorf("Failed to retrieve %s#%d: %v",
					hex.EncodeToString(txId[:8]), outputIdx, err)
				continue
			}

			t.Logf("Successfully retrieved %s#%d: CBOR len=%d",
				hex.EncodeToString(txId[:8]), outputIdx, len(retrieved.Cbor))
		}

		return nil
	})
	require.NoError(t, err)
}
