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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestSetGapBlockTransactionPersistsPositionedCertificates verifies that the
// gap-block ingestion path writes the same certificate provenance consumed by
// transaction hydration and account history readers. Both transactions share
// a slot so the history query must use block_index as its tie-breaker, and the
// second write is repeated to cover the certificate upsert path.
func TestSetGapBlockTransactionPersistsPositionedCertificates(t *testing.T) {
	db := openTestDB(t)

	stakeKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x31}, 28))
	credential := lcommon.Credential{
		CredType:   0,
		Credential: stakeKey,
	}

	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)

	buildTransaction := func(seed byte, poolSeed byte) lcommon.Transaction {
		t.Helper()
		input, inputErr := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{seed + 0x10}, 32),
			0,
		)
		require.NoError(t, inputErr)
		poolKey := lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{poolSeed}, 28)),
		)
		builder := mockledger.NewTransactionBuilder()
		builder.WithId(bytes.Repeat([]byte{seed}, 32))
		builder.WithInputs(input)
		builder.WithOutputs(output)
		builder.WithCertificates(&lcommon.StakeDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeDelegation),
			StakeCredential: &credential,
			PoolKeyHash:     poolKey,
		})
		transaction, buildErr := builder.Build()
		require.NoError(t, buildErr)
		return transaction
	}

	const slot = uint64(42_000)
	point := ocommon.Point{
		Slot: slot,
		Hash: bytes.Repeat([]byte{0xA1}, 32),
	}
	late := buildTransaction(0x41, 0x51)
	early := buildTransaction(0x42, 0x52)
	seedInputs := make([]models.Utxo, 0, 2)
	for _, transaction := range []lcommon.Transaction{late, early} {
		input := transaction.Consumed()[0]
		seedInputs = append(seedInputs, models.Utxo{
			TxId:      input.Id().Bytes(),
			OutputIdx: input.Index(),
			AddedSlot: 1,
			Amount:    1_000_000,
		})
	}
	seedTxn := db.MetadataTxn(true)
	require.NoError(t, seedTxn.Do(func(txn *Txn) error {
		return db.Metadata().ImportUtxos(seedInputs, txn.Metadata())
	}))
	seedTxn.Release()

	gapOffsets := func(transaction lcommon.Transaction) *BlockIngestionResult {
		t.Helper()
		var blockHash [32]byte
		copy(blockHash[:], point.Hash)
		var txHash [32]byte
		copy(txHash[:], transaction.Hash().Bytes())
		return &BlockIngestionResult{
			TxOffsets: map[[32]byte]CborOffset{
				txHash: {
					BlockSlot:  slot,
					BlockHash:  blockHash,
					ByteLength: 1,
				},
			},
			UtxoOffsets: map[UtxoRef]CborOffset{
				{TxId: txHash, OutputIdx: 0}: {
					BlockSlot:  slot,
					BlockHash:  blockHash,
					ByteLength: 1,
				},
			},
		}
	}

	// Deliberately ingest the higher block index first. The reader's ordering
	// must come from persisted transaction/certificate positions, not insertion
	// order or SQLite row IDs.
	require.NoError(t, db.SetGapBlockTransaction(
		late, point, 5, nil, gapOffsets(late), nil,
	))
	require.NoError(t, db.SetGapBlockTransaction(
		early, point, 2, nil, gapOffsets(early), nil,
	))
	// Reprocessing the same gap transaction must not duplicate its certificate.
	require.NoError(t, db.SetGapBlockTransaction(
		late, point, 5, nil, gapOffsets(late), nil,
	))

	gotTx, err := db.Metadata().GetTransactionByHash(late.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, gotTx)
	require.Len(t, gotTx.Certificates, 1)
	require.Equal(t, uint(0), gotTx.Certificates[0].CertIndex)
	require.Equal(t, uint(lcommon.CertificateTypeStakeDelegation), gotTx.Certificates[0].CertType)

	history, err := db.GetAccountDelegationHistoryByCredential(
		0, stakeKey.Bytes(), 0, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, uint32(2), history[0].BlockIndex)
	require.Equal(t, uint32(5), history[1].BlockIndex)
	require.Equal(t, uint32(0), history[0].CertIndex)
	require.Equal(t, uint32(0), history[1].CertIndex)
	require.Equal(t, slot, history[0].AddedSlot)
	require.Equal(t, slot, history[1].AddedSlot)
}

type gapRollbackCandidate struct {
	consumerBlock  models.Block
	consumerPoint  ocommon.Point
	consumerTx     lcommon.Transaction
	producerBlocks []models.Block
}

// gapProducerTx identifies one of the transactions that produced an
// input consumed by a gap-block test candidate. It keeps the producer
// tx and its block point so the test can feed both halves of the
// produce→consume pair through SetGapBlockTransaction.
type gapProducerTx struct {
	block models.Block
	point ocommon.Point
	tx    lcommon.Transaction
}

type gapConsumeCandidate struct {
	consumerBlock models.Block
	consumerPoint ocommon.Point
	consumerTx    lcommon.Transaction
	producers     []gapProducerTx
}

type batchedCrossBlockSpendCandidate struct {
	producerBlock models.Block
	producerPoint ocommon.Point
	producerTx    lcommon.Transaction
	producerIdx   uint32
	consumerBlock models.Block
	consumerPoint ocommon.Point
	consumerTx    lcommon.Transaction
	consumerIdx   uint32
	input         lcommon.TransactionInput
}

func TestSetGapBlockTransactionRestoresConsumedInputsOnRollback(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findGapRollbackCandidate(t)
	// The fixture contains three stake registrations and three pool
	// registrations. Preserve their protocol deposits instead of recording
	// an authoritative zero for the first certificate only.
	certificateDeposits := map[int]uint64{
		0: 2_000_000,
		1: 500_000_000,
		3: 2_000_000,
		4: 500_000_000,
		6: 2_000_000,
		7: 500_000_000,
	}
	for _, block := range candidate.producerBlocks {
		storeBlockOffsetsOnly(t, db, block)
	}
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	require.NoError(
		t,
		db.SetGapBlockTransaction(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			certificateDeposits,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected gap-consumed input %s to be present for rollback",
			input.String(),
		)
		require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
		require.Equal(
			t,
			candidate.consumerTx.Hash().Bytes(),
			[]byte(utxo.SpentAtTxId),
		)
	}

	txn := db.MetadataTxn(true)
	require.NoError(
		t,
		txn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteTransactionsAfterSlot(
				candidate.consumerPoint.Slot-1,
				txn.Metadata(),
			)
		}),
	)
	txn.Release()

	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxo(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected rollback to restore gap-consumed input %s",
			input.String(),
		)
		require.Zero(t, utxo.DeletedSlot)
		require.Nil(t, utxo.SpentAtTxId)
	}
}

func TestSetTransactionRecoversMissingConsumedInputsFromBlob(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findGapRollbackCandidateWithoutCertificates(t)
	for _, block := range candidate.producerBlocks {
		storeBlockOffsetsOnly(t, db, block)
	}
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	require.NoError(
		t,
		db.SetTransaction(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected SetTransaction to recover consumed input %s",
			input.String(),
		)
		require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
		require.Equal(
			t,
			candidate.consumerTx.Hash().Bytes(),
			[]byte(utxo.SpentAtTxId),
		)
	}
}

func TestSetTransactionBatchedSpendsPreviousBlockOutputInSameBatch(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findBatchedCrossBlockSpendCandidate(t)
	storeBlockOffsetsOnly(t, db, candidate.producerBlock)
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	acc := db.NewBatchAccumulator()
	txn := db.Transaction(true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(
		t,
		db.SetTransactionBatched(
			candidate.producerTx,
			candidate.producerPoint,
			candidate.producerIdx,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.producerBlock),
			acc,
			txn,
		),
	)
	require.NoError(
		t,
		db.SetTransactionBatched(
			candidate.consumerTx,
			candidate.consumerPoint,
			candidate.consumerIdx,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			acc,
			txn,
		),
	)
	require.NoError(t, db.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())

	utxo, err := db.Metadata().GetUtxoIncludingSpent(
		candidate.input.Id().Bytes(),
		candidate.input.Index(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, utxo)
	require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
	require.Equal(
		t,
		candidate.consumerTx.Hash().Bytes(),
		[]byte(utxo.SpentAtTxId),
	)
}

func findGapRollbackCandidate(t *testing.T) gapRollbackCandidate {
	return findGapRollbackCandidateMatching(t, false)
}

func findGapRollbackCandidateWithoutCertificates(
	t *testing.T,
) gapRollbackCandidate {
	return findGapRollbackCandidateMatching(t, true)
}

func findGapRollbackCandidateMatching(
	t *testing.T,
	requireNoCertificates bool,
) gapRollbackCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	seenOutputs := make(map[string]models.Block)
	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(
			immBlock.Type,
			immBlock.Cbor,
		)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		for _, tx := range block.Transactions() {
			if requireNoCertificates && len(tx.Certificates()) > 0 {
				for _, utxo := range tx.Produced() {
					seenOutputs[inputRefKey(
						utxo.Id.Id().Bytes(),
						utxo.Id.Index(),
					)] = blockModel
				}
				continue
			}
			consumed := tx.Consumed()
			if len(consumed) > 0 {
				producerByPoint := make(map[string]models.Block)
				allEarlier := true
				for _, input := range consumed {
					producerBlock, ok := seenOutputs[inputRefKey(
						input.Id().Bytes(),
						input.Index(),
					)]
					if !ok || producerBlock.Slot >= blockModel.Slot {
						allEarlier = false
						break
					}
					pointKey := fmt.Sprintf(
						"%d:%x",
						producerBlock.Slot,
						producerBlock.Hash,
					)
					producerByPoint[pointKey] = producerBlock
				}
				if allEarlier {
					producerBlocks := make(
						[]models.Block,
						0,
						len(producerByPoint),
					)
					for _, producerBlock := range producerByPoint {
						producerBlocks = append(
							producerBlocks,
							producerBlock,
						)
					}
					return gapRollbackCandidate{
						consumerBlock: blockModel,
						consumerPoint: ocommon.Point{
							Slot: blockModel.Slot,
							Hash: blockModel.Hash,
						},
						consumerTx:     tx,
						producerBlocks: producerBlocks,
					}
				}
			}
			for _, utxo := range tx.Produced() {
				seenOutputs[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = blockModel
			}
		}
	}

	t.Fatal("failed to find rollback candidate in immutable testdata")
	return gapRollbackCandidate{}
}

func storeBlockOffsetsOnly(t *testing.T, db *Database, block models.Block) {
	t.Helper()

	offsets := mustBlockOffsets(t, block)
	txn := db.Transaction(true)
	require.NoError(
		t,
		txn.Do(func(txn *Txn) error {
			if err := db.BlockCreate(block, txn); err != nil {
				return err
			}
			blob := txn.DB().Blob()
			for txHash, offset := range offsets.TxOffsets {
				if err := blob.SetTx(
					txn.Blob(),
					txHash[:],
					EncodeTxOffset(&offset),
				); err != nil {
					return err
				}
			}
			for ref, offset := range offsets.UtxoOffsets {
				if err := blob.SetUtxo(
					txn.Blob(),
					ref.TxId[:],
					ref.OutputIdx,
					EncodeUtxoOffset(&offset),
				); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	txn.Release()
}

func mustBlockOffsets(
	t *testing.T,
	block models.Block,
) *BlockIngestionResult {
	t.Helper()

	decodedBlock, err := gledger.NewBlockFromCbor(
		block.Type,
		block.Cbor,
	)
	require.NoError(t, err)
	indexer := NewBlockIndexer(block.Slot, block.Hash)
	offsets, err := indexer.ComputeOffsets(block.Cbor, decodedBlock)
	require.NoError(t, err)
	return &BlockIngestionResult{
		TxOffsets:   offsets.TxOffsets,
		UtxoOffsets: offsets.UtxoOffsets,
	}
}

func inputRefKey(txId []byte, outputIdx uint32) string {
	return fmt.Sprintf("%x:%d", txId, outputIdx)
}

// findGapConsumeCandidate walks the immutable testdata and returns a
// produce-then-consume pair where both halves are separately
// addressable transactions: the producer tx(s) that created the
// consumed outputs plus the consumer tx that later spent them in a
// different (later) block. Useful for exercising the full gap-block
// ingestion path through two SetGapBlockTransaction calls.
func findGapConsumeCandidate(t *testing.T) gapConsumeCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type producedEntry struct {
		block models.Block
		point ocommon.Point
		tx    lcommon.Transaction
	}
	produced := make(map[string]producedEntry)

	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		blockPoint := ocommon.Point{
			Slot: blockModel.Slot,
			Hash: blockModel.Hash,
		}
		for _, tx := range block.Transactions() {
			consumed := tx.Consumed()
			if len(consumed) > 0 {
				producerByKey := make(
					map[string]gapProducerTx,
				)
				allFound := true
				for _, input := range consumed {
					key := inputRefKey(
						input.Id().Bytes(),
						input.Index(),
					)
					entry, ok := produced[key]
					if !ok || entry.block.Slot >= blockModel.Slot {
						allFound = false
						break
					}
					pKey := fmt.Sprintf(
						"%d:%x:%x",
						entry.block.Slot,
						entry.block.Hash,
						entry.tx.Hash().Bytes(),
					)
					producerByKey[pKey] = gapProducerTx{
						block: entry.block,
						point: entry.point,
						tx:    entry.tx,
					}
				}
				if allFound {
					producers := make(
						[]gapProducerTx,
						0,
						len(producerByKey),
					)
					for _, p := range producerByKey {
						producers = append(producers, p)
					}
					return gapConsumeCandidate{
						consumerBlock: blockModel,
						consumerPoint: blockPoint,
						consumerTx:    tx,
						producers:     producers,
					}
				}
			}
			for _, utxo := range tx.Produced() {
				produced[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = producedEntry{
					block: blockModel,
					point: blockPoint,
					tx:    tx,
				}
			}
		}
	}

	t.Fatal("failed to find gap consume candidate in immutable testdata")
	return gapConsumeCandidate{}
}

// findGapConsumeCandidateWithoutCertificates is the cert-free variant
// of findGapConsumeCandidate. It walks the immutable testdata for the
// same produce-then-consume pair, but skips any transaction (in either
// position) that carries certificates so callers can drive the pair
// through SetTransaction with no certDeposits map.
func findGapConsumeCandidateWithoutCertificates(
	t *testing.T,
) gapConsumeCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type producedEntry struct {
		block models.Block
		point ocommon.Point
		tx    lcommon.Transaction
	}
	produced := make(map[string]producedEntry)

	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		blockPoint := ocommon.Point{
			Slot: blockModel.Slot,
			Hash: blockModel.Hash,
		}
		for _, tx := range block.Transactions() {
			if len(tx.Certificates()) > 0 {
				// A producer with certs would carry deposit-bearing
				// state we don't want to fixture-up; drop it from
				// both sides by not tracking its outputs and not
				// matching it as a consumer.
				continue
			}
			consumed := tx.Consumed()
			if len(consumed) > 0 {
				producerByKey := make(map[string]gapProducerTx)
				allFound := true
				for _, input := range consumed {
					key := inputRefKey(
						input.Id().Bytes(),
						input.Index(),
					)
					entry, ok := produced[key]
					if !ok || entry.block.Slot >= blockModel.Slot {
						allFound = false
						break
					}
					pKey := fmt.Sprintf(
						"%d:%x:%x",
						entry.block.Slot,
						entry.block.Hash,
						entry.tx.Hash().Bytes(),
					)
					producerByKey[pKey] = gapProducerTx{
						block: entry.block,
						point: entry.point,
						tx:    entry.tx,
					}
				}
				if allFound {
					producers := make(
						[]gapProducerTx,
						0,
						len(producerByKey),
					)
					for _, p := range producerByKey {
						producers = append(producers, p)
					}
					return gapConsumeCandidate{
						consumerBlock: blockModel,
						consumerPoint: blockPoint,
						consumerTx:    tx,
						producers:     producers,
					}
				}
			}
			for _, utxo := range tx.Produced() {
				produced[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = producedEntry{
					block: blockModel,
					point: blockPoint,
					tx:    tx,
				}
			}
		}
	}

	t.Fatal(
		"failed to find cert-free gap consume candidate in immutable testdata",
	)
	return gapConsumeCandidate{}
}

func findBatchedCrossBlockSpendCandidate(
	t *testing.T,
) batchedCrossBlockSpendCandidate {
	t.Helper()

	imm, err := immutable.New("immutable/testdata")
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type producedEntry struct {
		block models.Block
		point ocommon.Point
		tx    lcommon.Transaction
		idx   uint32
	}
	prevProduced := make(map[string]producedEntry)

	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blockModel := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Number:   block.BlockNumber(),
			Type:     uint(block.Type()),
			PrevHash: block.PrevHash().Bytes(),
			Cbor:     block.Cbor(),
		}
		blockPoint := ocommon.Point{
			Slot: blockModel.Slot,
			Hash: blockModel.Hash,
		}

		for idx, tx := range block.Transactions() {
			if len(tx.Certificates()) > 0 {
				continue
			}
			for _, input := range tx.Consumed() {
				entry, ok := prevProduced[inputRefKey(
					input.Id().Bytes(),
					input.Index(),
				)]
				if !ok {
					continue
				}
				return batchedCrossBlockSpendCandidate{
					producerBlock: entry.block,
					producerPoint: entry.point,
					producerTx:    entry.tx,
					producerIdx:   entry.idx,
					consumerBlock: blockModel,
					consumerPoint: blockPoint,
					consumerTx:    tx,
					consumerIdx:   uint32(idx),
					input:         input,
				}
			}
		}

		prevProduced = make(map[string]producedEntry)
		for idx, tx := range block.Transactions() {
			if len(tx.Certificates()) > 0 {
				continue
			}
			for _, utxo := range tx.Produced() {
				prevProduced[inputRefKey(
					utxo.Id.Id().Bytes(),
					utxo.Id.Index(),
				)] = producedEntry{
					block: blockModel,
					point: blockPoint,
					tx:    tx,
					idx:   uint32(idx),
				}
			}
		}
	}

	t.Fatal(
		"failed to find previous-block spend candidate in immutable testdata",
	)
	return batchedCrossBlockSpendCandidate{}
}

// TestSetGapBlockTransactionSpendsLiveProducedInputs verifies that when
// a gap-block transaction consumes a UTxO produced by an earlier
// gap-block transaction (both ingested through SetGapBlockTransaction),
// the produced UTxO row is marked as spent with both deleted_slot and
// spent_at_tx_id populated — matching the normal SetTransaction path.
//
// Regression test for a bug where ensureGapConsumedUtxos unconditionally
// skipped existing UTxO rows, leaving outputs produced by earlier gap
// blocks live forever even after a later gap block consumed them.
func TestSetGapBlockTransactionSpendsLiveProducedInputs(t *testing.T) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close()

	candidate := findGapConsumeCandidate(t)
	certificateDeposits := map[int]uint64{
		0: 2_000_000,
		1: 500_000_000,
		3: 2_000_000,
		4: 500_000_000,
		6: 2_000_000,
		7: 500_000_000,
	}

	// Store blob offsets for all producer blocks plus the consumer
	// block so SetGapBlockTransaction can look up and persist
	// produced UTxO offset references.
	storedBlocks := make(map[string]struct{})
	for _, producer := range candidate.producers {
		key := fmt.Sprintf("%d:%x", producer.block.Slot, producer.block.Hash)
		if _, ok := storedBlocks[key]; ok {
			continue
		}
		storedBlocks[key] = struct{}{}
		storeBlockOffsetsOnly(t, db, producer.block)
	}
	consumerKey := fmt.Sprintf(
		"%d:%x",
		candidate.consumerBlock.Slot,
		candidate.consumerBlock.Hash,
	)
	if _, ok := storedBlocks[consumerKey]; !ok {
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
	}

	// Seed each producer tx's own consumed inputs as already-spent
	// rows (without a SpentAtTxId, to avoid needing a parent
	// transaction row for the Inputs FK) so the producer's
	// gap-block ingestion does not try to recover them from a blob
	// store that lacks the predecessor blocks. This mimics the
	// Mithril snapshot state where inputs from before the gap
	// window are already recorded as spent.
	//
	// Contract for the gap path: AddedSlot = DeletedSlot = seedSlot
	// with seedSlot != point.Slot drives ensureGapConsumedUtxos into
	// its "already spent by a different tx" branch (SpentAtTxId == nil
	// AND DeletedSlot != 0 AND DeletedSlot != point.Slot), so the
	// existing row is left untouched. Future changes to the gap-path
	// branch conditions must keep this skip path reachable or update
	// this seed accordingly.
	seedSlot := uint64(1)
	for _, producer := range candidate.producers {
		seeded := make([]models.Utxo, 0, len(producer.tx.Consumed()))
		for _, in := range producer.tx.Consumed() {
			seeded = append(seeded, models.Utxo{
				TxId:        in.Id().Bytes(),
				OutputIdx:   in.Index(),
				AddedSlot:   seedSlot,
				DeletedSlot: seedSlot,
			})
		}
		if len(seeded) > 0 {
			txn := db.MetadataTxn(true)
			require.NoError(
				t,
				txn.Do(func(txn *Txn) error {
					return db.Metadata().ImportUtxos(
						seeded,
						txn.Metadata(),
					)
				}),
			)
			txn.Release()
		}
	}

	// Ingest every producer tx through the gap path first, so the
	// consumed inputs exist as live UTxO rows when the consumer tx
	// arrives.
	for _, producer := range candidate.producers {
		require.NoError(
			t,
			db.SetGapBlockTransaction(
				producer.tx,
				producer.point,
				0,
				nil,
				mustBlockOffsets(t, producer.block),
				nil,
			),
		)
	}

	// Sanity: every consumed input is live before the consumer runs.
	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"producer gap tx did not persist input %s",
			input.String(),
		)
		require.Zero(
			t,
			utxo.DeletedSlot,
			"input %s should be live before consumer gap tx runs",
			input.String(),
		)
		require.Nil(
			t,
			utxo.SpentAtTxId,
			"input %s should be unspent before consumer gap tx runs",
			input.String(),
		)
	}

	// Now ingest the consumer gap tx. This must mark the produced
	// UTxOs as spent rather than silently skipping them.
	require.NoError(
		t,
		db.SetGapBlockTransaction(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			certificateDeposits,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	spenderTxHash := candidate.consumerTx.Hash().Bytes()
	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"expected consumed input %s to remain in metadata",
			input.String(),
		)
		require.Equal(
			t,
			candidate.consumerPoint.Slot,
			utxo.DeletedSlot,
			"input %s deleted_slot not set to consumer slot",
			input.String(),
		)
		require.Equal(
			t,
			spenderTxHash,
			[]byte(utxo.SpentAtTxId),
			"input %s spent_at_tx_id not set to consumer tx hash",
			input.String(),
		)
	}
}
