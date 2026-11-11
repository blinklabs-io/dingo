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
	"context"
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
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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

		txn := db.Transaction(context.Background(), true)
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
	if err := db.SetTransaction(context.Background(),
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
		txn, 0,
	); err != nil {
		return nil, err
	}
	return tx, nil
}

// TestUtxosByRefsAfterSetTransaction verifies the batched UTxO lookup
// returns every produced UTxO for a transaction in one call, exactly once
// even when a ref is requested more than once, and silently omits a ref
// that doesn't correspond to any live UTxO rather than erroring the whole
// batch.
func TestUtxosByRefsAfterSetTransaction(t *testing.T) {
	t.Parallel()

	db := newUtxoStorageTestDB(t)
	iter := newUtxoStorageTestIterator(t)
	block, blockCbor := nextProducingBlock(t, db, iter)

	txn := db.Transaction(context.Background(), true)
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

		results, err := db.UtxosByRefs(context.Background(), refs, txn)
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
	t.Parallel()

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

				txn := db.Transaction(context.Background(), true)
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
					return db.SetTransaction(context.Background(),
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
						txn, 0,
					)
				})
				require.NoError(t, err)

				deleteTxn := db.Transaction(context.Background(), true)
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

				lookupTxn := db.Transaction(context.Background(), true)
				err = lookupTxn.Do(func(txn *database.Txn) error {
					retrieved, err := db.UtxoByRef(context.Background(), txId, outputIdx, txn)
					require.NoError(t, err)
					require.Equal(t, expectedUtxo.Output.Cbor(), retrieved.Cbor)

					// The recovery path should heal the missing blob so future
					// lookups do not need to re-derive it from the producer block.
					// Verify by checking the blob key exists (the stored value is a
					// DOFF offset reference, not raw CBOR).
					_, err = db.Blob().GetUtxo(txn.Blob(), txId, outputIdx)
					require.NoError(t, err)

					// A second UtxoByRef should succeed without recovery.
					retrieved2, err := db.UtxoByRef(context.Background(), txId, outputIdx, txn)
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
