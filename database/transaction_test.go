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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

type mockBlobStore struct {
	deleteTxErrs map[string]error
	commitErrs   []error
	deleteTxnIDs []int
	txns         []*mockBlobTxn
	utxoData     map[string][]byte
}

type mockBlobTxn struct {
	id            int
	commitErr     error
	commitCount   int
	rollbackCount int
}

func (m *mockBlobStore) Close() error {
	return nil
}

func (m *mockBlobStore) DiskSize() (int64, error) {
	return 0, nil
}

func (m *mockBlobStore) NewTransaction(bool) types.Txn {
	txn := &mockBlobTxn{id: len(m.txns) + 1}
	if idx := len(m.txns); idx < len(m.commitErrs) {
		txn.commitErr = m.commitErrs[idx]
	}
	m.txns = append(m.txns, txn)
	return txn
}

func (m *mockBlobStore) Get(types.Txn, []byte) ([]byte, error) {
	return nil, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) Set(types.Txn, []byte, []byte) error {
	return nil
}

func (m *mockBlobStore) Delete(types.Txn, []byte) error {
	return nil
}

func (m *mockBlobStore) NewIterator(
	types.Txn,
	types.BlobIteratorOptions,
) types.BlobIterator {
	return nil
}

func (m *mockBlobStore) GetCommitTimestamp() (int64, error) {
	return 0, nil
}

func (m *mockBlobStore) SetCommitTimestamp(int64, types.Txn) error {
	return nil
}

func (m *mockBlobStore) SetBlock(
	types.Txn,
	uint64,
	[]byte,
	[]byte,
	uint64,
	uint,
	uint64,
	[]byte,
) error {
	return nil
}

func (m *mockBlobStore) GetBlock(
	types.Txn,
	uint64,
	[]byte,
) ([]byte, types.BlockMetadata, error) {
	return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) DeleteBlock(types.Txn, uint64, []byte, uint64) error {
	return nil
}

func (m *mockBlobStore) GetBlockURL(
	context.Context,
	types.Txn,
	ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return types.SignedURL{}, types.BlockMetadata{}, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) SetUtxo(types.Txn, []byte, uint32, []byte) error {
	return nil
}

func (m *mockBlobStore) GetUtxo(txn types.Txn, txId []byte, outputIdx uint32) ([]byte, error) {
	if m.utxoData != nil {
		if data, ok := m.utxoData[fmt.Sprintf("%x:%d", txId, outputIdx)]; ok {
			return data, nil
		}
	}
	return nil, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) DeleteUtxo(types.Txn, []byte, uint32) error {
	return nil
}

func (m *mockBlobStore) SetTx(types.Txn, []byte, []byte) error {
	return nil
}

func (m *mockBlobStore) GetTx(types.Txn, []byte) ([]byte, error) {
	return nil, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) DeleteTx(txn types.Txn, txHash []byte) error {
	mockTxn, ok := txn.(*mockBlobTxn)
	if !ok {
		return types.ErrTxnWrongType
	}
	m.deleteTxnIDs = append(m.deleteTxnIDs, mockTxn.id)
	if err, ok := m.deleteTxErrs[string(txHash)]; ok {
		return err
	}
	return nil
}

func (m *mockBlobTxn) Commit() error {
	m.commitCount++
	return m.commitErr
}

func (m *mockBlobTxn) Rollback() error {
	m.rollbackCount++
	return nil
}

func TestDeleteTxBlobsUsesCallerBlobTxn(t *testing.T) {
	t.Parallel()

	store := &mockBlobStore{}
	db := &Database{
		blob: store,
		logger: slog.New(
			slog.NewJSONHandler(
				io.Discard,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	}
	txn := db.Transaction(true)

	txHashes := [][]byte{{0x01}, {0x02}, {0x03}}
	require.NoError(t, deleteTxBlobs(db, txHashes, txn))
	require.Len(t, store.txns, 1)
	require.Equal(t, []int{1, 1, 1}, store.deleteTxnIDs)
	require.Zero(t, store.txns[0].commitCount)
	require.Zero(t, store.txns[0].rollbackCount)

	require.NoError(t, txn.Rollback())
}

func TestDeleteTxBlobsCountsFailedBatchCommit(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	store := &mockBlobStore{commitErrs: []error{errors.New("commit failed")}}
	db := &Database{
		blob: store,
		logger: slog.New(
			slog.NewJSONHandler(
				&logs,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	}

	txHashes := [][]byte{{0x01}, {0x02}, {0x03}}
	require.NoError(t, deleteTxBlobs(db, txHashes, nil))
	require.Len(t, store.txns, 1)
	require.Equal(t, 1, store.txns[0].commitCount)
	require.Contains(t, logs.String(), "\"failed\":3")
	require.Contains(t, logs.String(), "\"total\":3")
}

func TestRecoverConsumedUtxoLegacyRawCborWithoutProducerBlockFails(t *testing.T) {
	db, err := New(&Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	origBlob := db.Blob()
	store := &mockBlobStore{utxoData: make(map[string][]byte)}
	db.SetBlobStore(store)
	if origBlob != nil {
		require.NoError(t, origBlob.Close())
	}

	txId := bytes.Repeat([]byte{0xAB}, 32)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	rawOutput, err := cbor.Encode(output)
	require.NoError(t, err)
	store.utxoData[fmt.Sprintf("%x:%d", txId, 0)] = rawOutput

	txn := db.Transaction(true)
	defer txn.Release()

	_, err = db.recoverConsumedUtxo(dbtestutil.NewMockInput(txId, 0), txn)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUtxoNotFound)
}

// TestSetTransactionRecoveryPopulatesProducerFK verifies that when
// ensureTransactionConsumedUtxos has to recover a missing UTxO row for
// a consumed input, the recovered row carries the producer transaction
// FK. Without this FK, SetUtxosNotDeletedAfterSlot would reanimate the
// row during a rollback but joins on utxo.transaction_id and GORM
// Preload("Outputs") through the producer Transaction would silently
// drop it.
func TestSetTransactionRecoveryPopulatesProducerFK(t *testing.T) {
	// Intentionally not t.Parallel(): database.New() writes to plugin
	// option destination pointers via SetPluginOption, which the race
	// detector flags when two tests build a Database concurrently.
	db, err := New(&Config{
		DataDir:        t.TempDir(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)

	// Persist each producer half: the block + blob offsets, plus a
	// metadata Transaction row with its produced UTxOs. We bypass the
	// db.SetTransaction wrapper because the producer's own inputs are
	// not part of this fixture and would force a recovery cascade. We
	// only need the producer's Transaction row and its produced UTxO
	// rows to exist so the consumer's recovery has a real FK target.
	for _, p := range candidate.producers {
		storeBlockOffsetsOnly(t, db, p.block)
		metaTxn := db.MetadataTxn(true)
		producer := p
		require.NoError(
			t,
			metaTxn.Do(func(txn *Txn) error {
				return db.Metadata().SetGapBlockTransaction(
					producer.tx,
					producer.point,
					0,
					txn.Metadata(),
				)
			}),
		)
		metaTxn.Release()
	}

	// Snapshot each producer transaction's primary key so we can
	// later assert the recovered UTxOs' FK matches.
	producerIDByHash := make(map[string]uint, len(candidate.producers))
	for _, p := range candidate.producers {
		producerHash := p.tx.Hash().Bytes()
		got, err := db.Metadata().GetTransactionByHash(producerHash, nil)
		require.NoError(t, err)
		require.NotNil(
			t,
			got,
			"producer transaction row must be persisted",
		)
		require.NotZero(t, got.ID)
		producerIDByHash[fmt.Sprintf("%x", producerHash)] = got.ID
	}

	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	// Force the recovery path by deleting the metadata Utxo rows for
	// the inputs the consumer is about to spend. The blob store still
	// has their offset references, and the metadata Transaction row
	// for each producer remains intact.
	refs := make([]models.UtxoId, 0, len(candidate.consumerTx.Consumed()))
	for _, input := range candidate.consumerTx.Consumed() {
		refs = append(refs, models.UtxoId{
			Hash: input.Id().Bytes(),
			Idx:  input.Index(),
		})
	}
	metaTxn := db.MetadataTxn(true)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteUtxos(refs, txn.Metadata())
		}),
	)
	metaTxn.Release()
	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.Nil(
			t,
			utxo,
			"setup: utxo %s must be deleted to exercise recovery",
			input.String(),
		)
	}

	// SetTransaction on the consumer triggers
	// ensureTransactionConsumedUtxos -> recoverConsumedUtxo for each
	// missing input.
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

	// Each recovered UTxO row must carry the producer transaction FK
	// pointing at the right Transaction.ID.
	for _, input := range candidate.consumerTx.Consumed() {
		producerHash := input.Id().Bytes()
		expectedID, ok := producerIDByHash[fmt.Sprintf("%x", producerHash)]
		require.True(
			t,
			ok,
			"setup: producer hash %x missing from snapshot",
			producerHash,
		)
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			producerHash,
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"recovered UTxO for %s must be present",
			input.String(),
		)
		require.NotNil(
			t,
			utxo.TransactionID,
			"recovered UTxO for %s must carry producer FK so that "+
				"rollback reanimation keeps it visible to joins on "+
				"utxo.transaction_id and Preload(\"Outputs\")",
			input.String(),
		)
		require.Equal(t, expectedID, *utxo.TransactionID)
	}

	// Stronger end-to-end check: rollback past the consumer slot and
	// confirm the producer Transaction's preloaded Outputs include
	// each reanimated row.
	rollbackTxn := db.MetadataTxn(true)
	require.NoError(
		t,
		rollbackTxn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteTransactionsAfterSlot(
				candidate.consumerPoint.Slot-1,
				txn.Metadata(),
			)
		}),
	)
	rollbackTxn.Release()
	rollbackTxn = db.MetadataTxn(true)
	require.NoError(
		t,
		rollbackTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetUtxosNotDeletedAfterSlot(
				candidate.consumerPoint.Slot-1,
				txn.Metadata(),
			)
		}),
	)
	rollbackTxn.Release()
	for _, input := range candidate.consumerTx.Consumed() {
		producer, err := db.Metadata().GetTransactionByHash(
			input.Id().Bytes(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, producer)
		found := false
		for _, out := range producer.Outputs {
			if out.OutputIdx == input.Index() {
				found = true
				break
			}
		}
		require.True(
			t,
			found,
			"after rollback, recovered UTxO for %s must be "+
				"reachable from producer Transaction.Outputs preload",
			input.String(),
		)
	}
}
