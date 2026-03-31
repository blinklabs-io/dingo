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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type mockBlobStore struct {
	deleteTxErrs map[string]error
	commitErrs   []error
	deleteTxnIDs []int
	txns         []*mockBlobTxn
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
	return nil, nil
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
	return nil, types.BlockMetadata{}, nil
}

func (m *mockBlobStore) DeleteBlock(types.Txn, uint64, []byte, uint64) error {
	return nil
}

func (m *mockBlobStore) GetBlockURL(
	context.Context,
	types.Txn,
	ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return types.SignedURL{}, types.BlockMetadata{}, nil
}

func (m *mockBlobStore) SetUtxo(types.Txn, []byte, uint32, []byte) error {
	return nil
}

func (m *mockBlobStore) GetUtxo(types.Txn, []byte, uint32) ([]byte, error) {
	return nil, nil
}

func (m *mockBlobStore) DeleteUtxo(types.Txn, []byte, uint32) error {
	return nil
}

func (m *mockBlobStore) SetTx(types.Txn, []byte, []byte) error {
	return nil
}

func (m *mockBlobStore) GetTx(types.Txn, []byte) ([]byte, error) {
	return nil, nil
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
