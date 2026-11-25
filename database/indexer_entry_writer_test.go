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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// entryWriterBound is how long a write through an IndexerDB's transaction may
// take. On a metadata store with one writer, an entry write outside that
// transaction waits behind it and never returns, so a regression shows as a
// failure after this bound rather than a hung test.
const entryWriterBound = 20 * time.Second

// withinBound runs fn and fails the test if it does not return in time.
func withinBound(t *testing.T, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(entryWriterBound):
		t.Fatal("the entry write waited behind its own metadata transaction")
	}
}

func TestIndexerDBRollbackDeletesEntriesInAMetadataOnlyWriteTransaction(
	t *testing.T,
) {
	t.Parallel()
	fixture, _, candidate := indexedConsumer(t)
	txHash := candidate.consumerTx.Hash().Bytes()
	offsets := indexerOffsets(t, fixture.indexer)
	produced := candidate.consumerTx.Produced()
	require.NotEmpty(t, produced)

	txn := NewMetadataOnlyTxn(context.Background(), fixture.indexer, true)
	defer txn.Release()
	utxos := make([]models.Utxo, 0, len(produced))
	for idx := range produced {
		utxos = append(utxos, models.Utxo{
			TxId:      txHash,
			OutputIdx: uint32(idx), // #nosec G115
		})
	}
	t.Run("utxo", func(t *testing.T) {
		withinBound(t, func() error {
			return deleteUtxoBlobs(fixture.indexer, utxos, txn)
		})
	})
	t.Run("transaction", func(t *testing.T) {
		withinBound(t, func() error {
			return deleteTxBlobs(fixture.indexer, [][]byte{txHash}, txn)
		})
	})
	require.NoError(t, txn.Commit())

	for idx := range produced {
		_, ok, err := offsets.GetUtxoOffset(nil, txHash, uint32(idx)) // #nosec G115
		require.NoError(t, err)
		require.False(t, ok, "output %d keeps its offset", idx)
	}
	_, ok, err := offsets.GetTransactionOffset(nil, txHash)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestIndexerDBRepairsAnOffsetThroughItsMetadataWriteTransaction(
	t *testing.T,
) {
	t.Parallel()
	fixture, _, candidate := indexedConsumer(t)
	txHash := candidate.consumerTx.Hash().Bytes()
	offsets := indexerOffsets(t, fixture.indexer)
	data, ok, err := offsets.GetUtxoOffset(nil, txHash, 0)
	require.NoError(t, err)
	require.True(t, ok)
	offset, err := DecodeUtxoOffset(data)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		// handle returns the transaction the repair writes through.
		handle func(txn *Txn) (*Txn, func())
	}{
		{"metadata-only write transaction", func(txn *Txn) (*Txn, func()) {
			return txn, func() {}
		}},
		{"recovery handle", func(txn *Txn) (*Txn, func()) {
			return txn.withBlobForRecovery()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, offsets.DeleteUtxoOffset(nil, txHash, 0))
			txn := NewMetadataOnlyTxn(
				context.Background(), fixture.indexer, true,
			)
			defer txn.Release()
			handle, cleanup := tc.handle(txn)
			withinBound(t, func() error {
				return repairUtxoBlob(
					fixture.indexer, handle, txHash, 0, offset,
				)
			})
			cleanup()
			require.NoError(t, txn.Commit())

			repaired, ok, err := offsets.GetUtxoOffset(nil, txHash, 0)
			require.NoError(t, err)
			require.True(t, ok, "the repair commits with the transaction")
			require.Equal(t, data, repaired)
		})
	}
}

// failingOffsets fails every SetUtxoOffset call after performing it, so the
// write has happened when the caller sees the error.
type failingOffsets struct {
	metadata.IndexerOffsetStore
	sets atomic.Int32
}

var errInjectedOffsetWrite = errors.New("injected offset write failure")

func (f *failingOffsets) SetUtxoOffset(
	txn types.Txn, txId []byte, outputIdx uint32, data []byte,
) error {
	f.sets.Add(1)
	if err := f.IndexerOffsetStore.SetUtxoOffset(
		txn, txId, outputIdx, data,
	); err != nil {
		return err
	}
	return errInjectedOffsetWrite
}

// MaterializeBlockUtxos rewrites the entries of an IndexerDB in one
// transaction: a failure after a rewrite leaves every entry an offset, and a
// clean run leaves none.
func TestIndexerDBMaterializesBlockUtxosInOneTransaction(t *testing.T) {
	t.Parallel()
	fixture, _, candidate := indexedConsumer(t)
	txHash := candidate.consumerTx.Hash().Bytes()
	produced := candidate.consumerTx.Produced()
	require.NotEmpty(t, produced)
	real := fixture.indexer.Blob().(*indexerBlobStore)
	offsets := indexerOffsets(t, fixture.indexer)
	slot := candidate.consumerPoint.Slot
	hash := candidate.consumerBlock.Hash

	failing := &failingOffsets{IndexerOffsetStore: real.offsets}
	_, drain := fixture.indexer.SetBlobStore(
		&indexerBlobStore{blocks: real.blocks, offsets: failing},
	)
	drain()
	ctx, cancel := context.WithTimeout(context.Background(), entryWriterBound)
	defer cancel()
	_, err := fixture.indexer.MaterializeBlockUtxos(ctx, slot, hash)
	require.ErrorIs(t, err, errInjectedOffsetWrite)
	require.Positive(t, failing.sets.Load(),
		"the failure came after a rewrite")
	for idx := range produced {
		data, ok, err := offsets.GetUtxoOffset(nil, txHash, uint32(idx)) // #nosec G115
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, IsUtxoOffsetStorage(data),
			"output %d was rewritten outside the failed transaction", idx)
	}

	_, drain = fixture.indexer.SetBlobStore(real)
	drain()
	n, err := fixture.indexer.MaterializeBlockUtxos(ctx, slot, hash)
	require.NoError(t, err)
	require.Equal(t, len(produced), n)
	for idx := range produced {
		data, ok, err := offsets.GetUtxoOffset(nil, txHash, uint32(idx)) // #nosec G115
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, IsUtxoOffsetStorage(data))
	}
}
