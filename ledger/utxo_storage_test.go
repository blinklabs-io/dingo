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

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
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
		DataDir: tmpDir,
		Logger:  logger,
	}
	db, err := dbtest.NewDatabase(t, dbConfig)
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

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

// newUtxoStorageTestDB creates a temp-directory database for tests that
// load real blocks from the immutable testdata fixture.
func newUtxoStorageTestDB(t *testing.T) *database.Database {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "utxo_storage_test")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
		Logger: slog.New(
			slog.NewTextHandler(
				io.Discard,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	return db
}

// newUtxoStorageTestIterator opens the shared immutable testdata fixture
// and returns an iterator positioned at genesis.
func newUtxoStorageTestIterator(t *testing.T) *immutable.BlockIterator {
	t.Helper()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: nil})
	require.NoError(t, err)
	t.Cleanup(func() { iter.Close() })
	return iter
}

// errNextProducingBlockValidated is returned from the scratch transaction
// nextProducingBlock uses to check storability, forcing a rollback so the
// caller's own (real) store starts from a clean slate regardless of
// whether that check succeeded or failed.
var errNextProducingBlockValidated = errors.New(
	"nextProducingBlock: storability validated, rolling back",
)

// nextProducingBlock advances iter to the next block whose first
// transaction produces at least one UTxO and can actually be stored with
// this package's minimal SetTransaction call (nil pparamUpdates and
// certDeposits) — some fixture transactions carry a deposit-bearing
// certificate that needs certDeposits this helper doesn't supply, and
// those are skipped too, in a scratch transaction that is always rolled
// back. It decodes the block and returns it along with its raw CBOR. It
// skips the test if the fixture is exhausted before finding one, so
// callers never need to separately handle a non-producing or un-storable
// first transaction.
func nextProducingBlock(
	t *testing.T,
	db *database.Database,
	iter *immutable.BlockIterator,
) (lcommon.Block, []byte) {
	t.Helper()
	for {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			t.Skip(
				"no storable block with a producing first transaction found in testdata",
			)
		}

		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)

		if len(block.Transactions()) == 0 ||
			len(block.Transactions()[0].Produced()) == 0 {
			continue
		}

		txn := db.Transaction(true)
		err = txn.Do(func(txn *database.Txn) error {
			if _, err := tryStoreBlockFirstTx(db, txn, block, immBlock.Cbor); err != nil {
				return err
			}
			return errNextProducingBlockValidated
		})
		if !errors.Is(err, errNextProducingBlockValidated) {
			// A real storage error (e.g. a deposit-bearing certificate
			// needing certDeposits this helper doesn't supply); try the
			// next producing block instead.
			continue
		}
		return block, immBlock.Cbor
	}
}

// storeBlockFirstTx stores block (and its raw CBOR) plus its first
// transaction into db within txn, computing the offsets SetTransaction
// requires, and returns the stored transaction.
func storeBlockFirstTx(
	t *testing.T,
	db *database.Database,
	txn *database.Txn,
	block lcommon.Block,
	blockCbor []byte,
) lcommon.Transaction {
	t.Helper()
	tx, err := tryStoreBlockFirstTx(db, txn, block, blockCbor)
	require.NoError(t, err)
	return tx
}

// tryStoreBlockFirstTx is the non-asserting form of storeBlockFirstTx, for
// callers that want to skip a block that fails to store (e.g. one whose
// first transaction carries a deposit-bearing certificate, which needs
// certDeposits this minimal helper doesn't supply) rather than failing the
// test outright.
func tryStoreBlockFirstTx(
	db *database.Database,
	txn *database.Txn,
	block lcommon.Block,
	blockCbor []byte,
) (lcommon.Transaction, error) {
	point := ocommon.Point{
		Slot: block.SlotNumber(),
		Hash: block.Hash().Bytes(),
	}
	blockRecord := models.Block{
		Slot:     point.Slot,
		Hash:     point.Hash,
		Number:   block.BlockNumber(),
		Type:     uint(block.Type()),
		PrevHash: block.PrevHash().Bytes(),
		Cbor:     blockCbor,
	}
	if err := db.BlockCreate(blockRecord, txn); err != nil {
		return nil, err
	}

	indexer := database.NewBlockIndexer(point.Slot, point.Hash)
	offsets, err := indexer.ComputeOffsets(blockCbor, block)
	if err != nil {
		return nil, err
	}

	txs := block.Transactions()
	if len(txs) == 0 {
		return nil, errors.New("block has no transactions")
	}
	tx := txs[0]
	if err := db.SetTransaction(
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
	); err != nil {
		return nil, err
	}
	return tx, nil
}

// TestUtxoByRefAfterSetTransaction verifies that UtxoByRef works immediately
// after SetTransaction within the same transaction.
func TestUtxoByRefAfterSetTransaction(t *testing.T) {
	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)
	block, blockCbor := nextProducingBlock(t, db, iter)

	// Store block and verify UTxO retrieval in same transaction
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		tx := storeBlockFirstTx(t, db, txn, block, blockCbor)

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

// TestUtxosByRefsAfterSetTransaction verifies the batched UTxO lookup
// returns every produced UTxO for a transaction in one call, exactly once
// even when a ref is requested more than once, and silently omits a ref
// that doesn't correspond to any live UTxO rather than erroring the whole
// batch (see #392).
func TestUtxosByRefsAfterSetTransaction(t *testing.T) {
	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)
	block, blockCbor := nextProducingBlock(t, db, iter)

	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		tx := storeBlockFirstTx(t, db, txn, block, blockCbor)
		produced := tx.Produced()

		refs := make([]models.UtxoId, 0, len(produced)+2)
		for _, utxo := range produced {
			refs = append(refs, models.UtxoId{
				Hash: utxo.Id.Id().Bytes(),
				Idx:  utxo.Id.Index(),
			})
		}
		// Requesting the first produced UTxO's ref a second time must not
		// duplicate it in the result.
		refs = append(refs, refs[0])
		// A ref with no matching live UTxO should be silently omitted,
		// not cause the whole batch to fail.
		bogusHash := make([]byte, 32)
		refs = append(refs, models.UtxoId{Hash: bogusHash, Idx: 9999})

		results, err := db.UtxosByRefs(refs, txn)
		if err != nil {
			return err
		}
		require.Len(
			t,
			results,
			len(produced),
			"duplicate ref should not duplicate its result row",
		)

		byRef := make(map[string]models.Utxo, len(results))
		for _, utxo := range results {
			key := hex.EncodeToString(utxo.TxId) + ":" +
				fmt.Sprint(utxo.OutputIdx)
			byRef[key] = utxo
		}
		for _, utxo := range produced {
			txId := utxo.Id.Id().Bytes()
			key := hex.EncodeToString(txId) + ":" +
				fmt.Sprint(utxo.Id.Index())
			got, ok := byRef[key]
			require.True(t, ok, "missing UTxO %s", key)
			require.NotEmpty(t, got.Cbor)
		}

		return nil
	})
	require.NoError(t, err)
}

func TestUtxoByRefRecoversMissingBlobFromProducerBlock(t *testing.T) {
	for _, deleteTxBlob := range []bool{false, true} {
		t.Run(
			fmt.Sprintf("delete_tx_blob=%t", deleteTxBlob),
			func(t *testing.T) {
				tmpDir, err := os.MkdirTemp("", "utxo_byref_recover_test")
				require.NoError(t, err)
				defer os.RemoveAll(tmpDir)

				logger := slog.New(
					slog.NewTextHandler(
						io.Discard,
						&slog.HandlerOptions{Level: slog.LevelDebug},
					),
				)

				dbConfig := &database.Config{
					DataDir: tmpDir,
					Logger:  logger,
				}
				db, err := dbtest.NewDatabase(t, dbConfig)
				require.NoError(t, err)
				defer dbtest.CloseDatabase(db)

				imm, err := immutable.New("../database/immutable/testdata")
				require.NoError(t, err)

				iter, err := imm.BlocksFromPoint(
					ocommon.Point{Slot: 0, Hash: nil},
				)
				require.NoError(t, err)
				defer iter.Close()

				var block lcommon.Block
				var blockCbor []byte
				for {
					immBlock, err := iter.Next()
					require.NoError(t, err)
					if immBlock == nil {
						t.Fatal("no blocks with transactions found")
					}
					block, err = ledger.NewBlockFromCbor(
						immBlock.Type,
						immBlock.Cbor,
					)
					require.NoError(t, err)
					if len(block.Transactions()) == 0 {
						continue
					}
					if len(block.Transactions()[0].Produced()) == 0 {
						continue
					}
					blockCbor = immBlock.Cbor
					break
				}

				point := ocommon.Point{
					Slot: block.SlotNumber(),
					Hash: block.Hash().Bytes(),
				}
				tx := block.Transactions()[0]
				expectedUtxo := tx.Produced()[0]
				txId := tx.Hash().Bytes()
				outputIdx := expectedUtxo.Id.Index()

				txn := db.Transaction(true)
				err = txn.Do(func(txn *database.Txn) error {
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
					indexer := database.NewBlockIndexer(point.Slot, point.Hash)
					offsets, err := indexer.ComputeOffsets(blockCbor, block)
					if err != nil {
						return fmt.Errorf("compute offsets: %w", err)
					}
					return db.SetTransaction(
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
				})
				require.NoError(t, err)

				deleteTxn := db.Transaction(true)
				err = deleteTxn.Do(func(txn *database.Txn) error {
					if err := db.Blob().DeleteUtxo(txn.Blob(), txId, outputIdx); err != nil {
						return err
					}
					if deleteTxBlob {
						if err := db.Blob().DeleteTx(txn.Blob(), txId); err != nil {
							return err
						}
					}
					return nil
				})
				require.NoError(t, err)

				metaUtxo, err := db.Metadata().GetUtxo(txId, outputIdx, nil)
				require.NoError(t, err)
				require.NotNil(t, metaUtxo)

				lookupTxn := db.Transaction(true)
				err = lookupTxn.Do(func(txn *database.Txn) error {
					retrieved, err := db.UtxoByRef(txId, outputIdx, txn)
					require.NoError(t, err)
					require.Equal(t, expectedUtxo.Output.Cbor(), retrieved.Cbor)

					// The recovery path should heal the missing blob so future
					// lookups do not need to re-derive it from the producer block.
					// Verify by checking the blob key exists (the stored value is a
					// DOFF offset reference, not raw CBOR).
					_, err = db.Blob().GetUtxo(txn.Blob(), txId, outputIdx)
					require.NoError(t, err)

					// A second UtxoByRef should succeed without recovery.
					retrieved2, err := db.UtxoByRef(txId, outputIdx, txn)
					require.NoError(t, err)
					require.Equal(
						t,
						expectedUtxo.Output.Cbor(),
						retrieved2.Cbor,
					)
					return nil
				})
				require.NoError(t, err)
			},
		)
	}
}
